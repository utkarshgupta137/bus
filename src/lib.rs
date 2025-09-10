//! Bus provides a lock-free, bounded, single-producer, multi-consumer, broadcast channel.
//!
//! It uses a circular buffer and atomic instructions to implement a lock-free single-producer,
//! multi-consumer channel. The interface is similar to that of the `std::sync::mpsc` channels,
//! except that multiple consumers (readers of the channel) can be produced, whereas only a single
//! sender can exist. Furthermore, in contrast to most multi-consumer FIFO queues, bus is
//! *broadcast*; every send goes to every consumer.
//!
//! I haven't seen this particular implementation in literature (some extra bookkeeping is
//! necessary to allow multiple consumers), but a lot of related reading can be found in Ross
//! Bencina's blog post ["Some notes on lock-free and wait-free
//! algorithms"](http://www.rossbencina.com/code/lockfree).
//!
//! Bus achieves broadcast by cloning the element in question, which is why `T` must implement
//! `Clone`. However, Bus is clever about only cloning when necessary. Specifically, the last
//! consumer to see a given value will move it instead of cloning, which means no cloning is
//! happening for the single-consumer case. For cases where cloning is expensive, `Arc` should be
//! used instead.
//!
//! # Examples
//!
//! Single-send, multi-consumer example
//!
//! ```rust
//! use bus::Bus;
//! let mut bus = Bus::new(10);
//! let mut rx1 = bus.add_rx();
//! let mut rx2 = bus.add_rx();
//!
//! bus.broadcast("Hello");
//! assert_eq!(rx1.recv(), Ok("Hello"));
//! assert_eq!(rx2.recv(), Ok("Hello"));
//! ```
//!
//! Multi-send, multi-consumer example
//!
//! ```rust
//! # if cfg!(miri) { return } // Miri is too slow
//! use bus::Bus;
//! use std::thread;
//!
//! let mut bus = Bus::new(10);
//! let mut rx1 = bus.add_rx();
//! let mut rx2 = bus.add_rx();
//!
//! // start a thread that sends 1..100
//! let j = thread::spawn(move || {
//!     for i in 1..100 {
//!         bus.broadcast(i);
//!     }
//! });
//!
//! // every value should be received by both receivers
//! for i in 1..100 {
//!     // rx1
//!     assert_eq!(rx1.recv(), Ok(i));
//!     // and rx2
//!     assert_eq!(rx2.recv(), Ok(i));
//! }
//!
//! j.join().unwrap();
//! ```
//!
//! Many-to-many channel using a dispatcher
//!
//! ```rust
//! use bus::Bus;
//!
//! use std::thread;
//! use std::sync::mpsc;
//!
//! // set up fan-in
//! let (tx1, mix_rx) = mpsc::sync_channel(100);
//! let tx2 = tx1.clone();
//! // set up fan-out
//! let mut mix_tx = Bus::new(100);
//! let mut rx1 = mix_tx.add_rx();
//! let mut rx2 = mix_tx.add_rx();
//! // start dispatcher
//! thread::spawn(move || {
//!     for m in mix_rx.iter() {
//!         mix_tx.broadcast(m);
//!     }
//! });
//!
//! // sends on tx1 are received ...
//! tx1.send("Hello").unwrap();
//!
//! // ... by both receiver rx1 ...
//! assert_eq!(rx1.recv(), Ok("Hello"));
//! // ... and receiver rx2
//! assert_eq!(rx2.recv(), Ok("Hello"));
//!
//! // same with sends on tx2
//! tx2.send("world").unwrap();
//! assert_eq!(rx1.recv(), Ok("world"));
//! assert_eq!(rx2.recv(), Ok("world"));
//! ```

use std::cell::UnsafeCell;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{RecvError, RecvTimeoutError, TryRecvError};
use std::time::{Duration, Instant};

use crossbeam_utils::CachePadded;

struct SeatState<T> {
    val: Option<T>,
}

struct MutSeatState<T>(UnsafeCell<SeatState<T>>);
unsafe impl<T> Sync for MutSeatState<T> {}
impl<T> Deref for MutSeatState<T> {
    type Target = UnsafeCell<SeatState<T>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> fmt::Debug for MutSeatState<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("MutSeatState").field(&self.0).finish()
    }
}

/// A Seat is a single location in the circular buffer.
/// Each Seat knows how many readers are expected to access it, as well as how many have. The
/// producer will never modify a seat's state unless all readers for a particular seat have either
/// called `.take()` on it, or have left (see `Bus.rleft`).
///
/// The producer walks the seats of the ring in order, and will always only modify the seat at
/// `tail + 1` once all readers have finished with the seat at `head + 2`. A reader will never
/// access a seat unless it is between the reader's `head` and the producer's `tail`. Together,
/// these properties ensure that a Seat is either accessed only by readers, or by only the
/// producer.
///
/// The `read` attribute is used to ensure that readers see the most recent write to the seat when
/// they access it. This is done using `Ordering::Acquire` and `Ordering::Release`.
struct Seat<T> {
    state: MutSeatState<T>,
}

impl<T> fmt::Debug for Seat<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Seat").field("state", &self.state).finish()
    }
}

impl<T> Default for Seat<T> {
    fn default() -> Self {
        Self {
            state: MutSeatState(UnsafeCell::new(SeatState { val: None })),
        }
    }
}

/// `BusInner` encapsulates data that both the writer and the readers need to access. The tail is
/// only ever modified by the producer, and read by the consumers. The length of the bus is
/// instantiated when the bus is created, and is never modified.
struct BusInner<T> {
    tail: CachePadded<AtomicUsize>,
    ring: Box<[Seat<T>]>,
    mask: usize,
    closed: AtomicBool,
}

impl<T> fmt::Debug for BusInner<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BusInner")
            .field("len", &self.ring.len())
            .field("tail", &self.tail)
            .field("closed", &self.closed)
            .finish()
    }
}

/// `Bus` is the main interconnect for broadcast messages. It can be used to send broadcast
/// messages, or to connect additional consumers. When the `Bus` is dropped, receivers will
/// continue receiving any outstanding broadcast messages they would have received if the bus were
/// not dropped. After all those messages have been received, any subsequent receive call on a
/// receiver will return a disconnected error.
pub struct Bus<T> {
    state: Arc<BusInner<T>>,

    // current number of readers
    readers: usize,
}

impl<T> fmt::Debug for Bus<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bus")
            .field("state", &self.state)
            .field("readers", &self.readers)
            .finish()
    }
}

impl<T> Bus<T> {
    /// Allocates a new `Bus`.
    ///
    /// The provided length should be sufficient to absorb temporary peaks in the data flow, and is
    /// thus workflow-dependent. Bus performance degrades somewhat when the queue is full, so it is
    /// generally better to set this high than low unless you are pressed for memory.
    #[must_use]
    pub fn new(len: usize) -> Self {
        assert!(len.is_power_of_two(), "size must be a power of 2.");
        let inner = Arc::new(BusInner {
            tail: CachePadded::new(AtomicUsize::new(0)),
            ring: (0..len).map(|_| Seat::default()).collect(),
            mask: len - 1,
            closed: AtomicBool::new(false),
        });

        Self {
            state: inner,
            readers: 0,
        }
    }

    /// Attempts to place the given value on the bus.
    ///
    /// If the bus is full, the behavior depends on `block`. If false, the value given is returned
    /// in an `Err()`. Otherwise, the current thread will be parked until there is space in the bus
    /// again, and the broadcast will be tried again until it succeeds.
    ///
    /// Note that broadcasts will succeed even if there are no consumers!
    fn broadcast_inner(&mut self, val: T) -> Result<(), T> {
        let tail = self.state.tail.load(Ordering::Relaxed);

        unsafe { &mut *(&self.state.ring[tail]).state.get() }.val = Some(val);

        // now tell readers that they can read
        self.state
            .tail
            .store((tail + 1) & self.state.mask, Ordering::Release);

        Ok(())
    }

    /// Attempt to broadcast the given value to all consumers, but does not block if full.
    ///
    /// Note that, in contrast to regular channels, a bus is *not* considered closed if there are
    /// no consumers, and thus broadcasts will continue to succeed. Thus, a successful broadcast
    /// occurs as long as there is room on the internal bus to store the value, or some older value
    /// has been received by all consumers. Note that a return value of `Err` means that the data
    /// will never be received (by any consumer), but a return value of Ok does not mean that the
    /// data will be received by a given consumer. It is possible for a receiver to hang up
    /// immediately after this function returns Ok.
    ///
    /// This method will never block the current thread.
    ///
    /// ```rust
    /// use bus::Bus;
    /// let mut tx = Bus::new(1);
    /// let mut rx = tx.add_rx();
    /// assert_eq!(tx.try_broadcast("Hello"), Ok(()));
    /// assert_eq!(tx.try_broadcast("world"), Err("world"));
    /// ```
    #[allow(clippy::missing_errors_doc)]
    pub fn try_broadcast(&mut self, val: T) -> Result<(), T> {
        self.broadcast_inner(val)
    }

    /// Broadcasts a value on the bus to all consumers.
    ///
    /// This function will block until space in the internal buffer becomes available.
    ///
    /// Note that a successful send does not guarantee that the receiver will ever see the data if
    /// there is a buffer on this channel. Items may be enqueued in the internal buffer for the
    /// receiver to receive at a later time. Furthermore, in contrast to regular channels, a bus is
    /// *not* considered closed if there are no consumers, and thus broadcasts will continue to
    /// succeed.
    pub fn broadcast(&mut self, mut val: T) {
        loop {
            match self.broadcast_inner(val) {
                Ok(()) => break,
                Err(ret) => {
                    val = ret;
                }
            }
        }
    }

    /// Add a new consumer to this bus.
    ///
    /// The new consumer will receive all *future* broadcasts on this bus.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use bus::Bus;
    /// use std::sync::mpsc::TryRecvError;
    ///
    /// let mut bus = Bus::new(10);
    /// let mut rx1 = bus.add_rx();
    ///
    /// bus.broadcast("Hello");
    ///
    /// // consumer present during broadcast sees update
    /// assert_eq!(rx1.recv(), Ok("Hello"));
    ///
    /// // new consumer does *not* see broadcast
    /// let mut rx2 = bus.add_rx();
    /// assert_eq!(rx2.try_recv(), Err(TryRecvError::Empty));
    ///
    /// // both consumers see new broadcast
    /// bus.broadcast("world");
    /// assert_eq!(rx1.recv(), Ok("world"));
    /// assert_eq!(rx2.recv(), Ok("world"));
    /// ```
    pub fn add_rx(&mut self) -> BusReader<T> {
        self.readers += 1;

        BusReader {
            bus: Arc::clone(&self.state),
            head: self.state.tail.load(Ordering::Relaxed),
        }
    }

    /// Returns the number of active consumers currently attached to this bus.
    ///
    /// It is not guaranteed that a sent message will reach this number of consumers, as active
    /// consumers may never call `recv` or `try_recv` again before dropping.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use bus::Bus;
    ///
    /// let mut bus = Bus::<u8>::new(10);
    /// assert_eq!(bus.rx_count(), 0);
    ///
    /// let rx1 = bus.add_rx();
    /// assert_eq!(bus.rx_count(), 1);
    ///
    /// drop(rx1);
    /// assert_eq!(bus.rx_count(), 0);
    /// ```
    #[must_use]
    pub const fn rx_count(&self) -> usize {
        self.readers
    }
}

impl<T> Drop for Bus<T> {
    fn drop(&mut self) {
        self.state.closed.store(true, Ordering::Relaxed);
        // Acquire/Release .tail to ensure other threads see new .closed
        self.state.tail.fetch_add(0, Ordering::AcqRel);
    }
}

/// A `BusReader` is a single consumer of `Bus` broadcasts. It will see every new value that is
/// passed to `.broadcast()` (or successful calls to `.try_broadcast()`) on the `Bus` that it was
/// created from.
///
/// Dropping a `BusReader` is perfectly safe, and will unblock the writer if it was waiting for
/// that read to see a particular update.
///
/// ```rust
/// use bus::Bus;
/// let mut tx = Bus::new(1);
/// let mut r1 = tx.add_rx();
/// let r2 = tx.add_rx();
/// assert_eq!(tx.try_broadcast(true), Ok(()));
/// assert_eq!(r1.recv(), Ok(true));
///
/// // the bus does not have room for another broadcast
/// // since it knows r2 has not yet read the first broadcast
/// assert_eq!(tx.try_broadcast(true), Err(true));
///
/// // dropping r2 tells the producer that there is a free slot
/// // (i.e., it has been read by everyone)
/// drop(r2);
/// assert_eq!(tx.try_broadcast(true), Ok(()));
/// ```
pub struct BusReader<T> {
    bus: Arc<BusInner<T>>,
    head: usize,
}

impl<T> fmt::Debug for BusReader<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BusReader")
            .field("bus", &self.bus)
            .field("head", &self.head)
            .finish()
    }
}

impl<T: Clone + Sync> BusReader<T> {
    /// Attempts to read a broadcast from the bus.
    ///
    /// If the bus is empty, the behavior depends on `block`. If false,
    /// `Err(mpsc::RecvTimeoutError::Timeout)` is returned. Otherwise, the current thread will be
    /// parked until there is another broadcast on the bus, at which point the receive will be
    /// performed.
    fn recv_inner(&mut self) -> Result<T, TryRecvError> {
        let head = self.head;
        let tail = self.bus.tail.load(Ordering::Acquire);
        if tail == head {
            // buffer is empty, check whether it's closed.
            // relaxed is fine since Bus.drop does an acquire/release on tail
            if self.bus.closed.load(Ordering::Relaxed) {
                // the bus is closed, and we didn't miss anything!
                return Err(TryRecvError::Disconnected);
            }

            return Err(TryRecvError::Empty);
        }

        let ret = unsafe { &*(&self.bus.ring[head]).state.get() }
            .val
            .clone()
            .expect("seat that should be occupied was empty");

        // safe because mask is read-only
        self.head = (head + 1) & self.bus.mask;
        Ok(ret)
    }

    /// Attempts to return a pending broadcast on this receiver without blocking.
    ///
    /// This method will never block the caller in order to wait for data to become available.
    /// Instead, this will always return immediately with a possible option of pending data on the
    /// channel.
    ///
    /// If the corresponding bus has been dropped, and all broadcasts have been received, this
    /// method will return with a disconnected error.
    ///
    /// This method is useful for a flavor of "optimistic check" before deciding to block on a
    /// receiver.
    ///
    /// ```rust
    /// use bus::Bus;
    /// use std::thread;
    ///
    /// let mut tx = Bus::new(10);
    /// let mut rx = tx.add_rx();
    ///
    /// // spawn a thread that will broadcast at some point
    /// let j = thread::spawn(move || {
    ///     tx.broadcast(true);
    /// });
    ///
    /// loop {
    ///     match rx.try_recv() {
    ///         Ok(val) => {
    ///             assert_eq!(val, true);
    ///             break;
    ///         }
    ///         Err(..) => {
    ///             // maybe we can do other useful work here
    ///             // or we can just busy-loop
    ///             thread::yield_now()
    ///         },
    ///     }
    /// }
    ///
    /// j.join().unwrap();
    /// ```
    #[allow(clippy::missing_errors_doc)]
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.recv_inner()
    }

    /// Read another broadcast message from the bus, and block if none are available.
    ///
    /// This function will always block the current thread if there is no data available and it's
    /// possible for more broadcasts to be sent. Once a broadcast is sent on the corresponding
    /// `Bus`, then this receiver will wake up and return that message.
    ///
    /// If the corresponding `Bus` has been dropped, or it is dropped while this call is blocking,
    /// this call will wake up and return `Err` to indicate that no more messages can ever be
    /// received on this channel. However, since channels are buffered, messages sent before the
    /// disconnect will still be properly received.
    #[allow(clippy::missing_errors_doc)]
    pub fn recv(&mut self) -> Result<T, RecvError> {
        loop {
            match self.recv_inner() {
                Ok(val) => return Ok(val),
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => {
                    return Err(RecvError);
                }
            }
        }
    }

    /// Attempts to wait for a value from the bus, returning an error if the corresponding channel
    /// has hung up, or if it waits more than `timeout`.
    ///
    /// This function will always block the current thread if there is no data available and it's
    /// possible for more broadcasts to be sent. Once a message is sent on the corresponding `Bus`,
    /// then this receiver will wake up and return that message.
    ///
    /// If the corresponding `Bus` has been dropped, or it is dropped while this call is blocking,
    /// this call will wake up and return Err to indicate that no more messages can ever be
    /// received on this channel. However, since channels are buffered, messages sent before the
    /// disconnect will still be properly received.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use bus::Bus;
    /// use std::sync::mpsc::RecvTimeoutError;
    /// use std::time::Duration;
    ///
    /// let mut tx = Bus::<bool>::new(10);
    /// let mut rx = tx.add_rx();
    ///
    /// let timeout = Duration::from_millis(100);
    /// assert_eq!(Err(RecvTimeoutError::Timeout), rx.recv_timeout(timeout));
    /// ```
    #[allow(clippy::missing_errors_doc)]
    pub fn recv_timeout(&mut self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        let start = Instant::now();
        loop {
            match self.recv_inner() {
                Ok(val) => return Ok(val),
                Err(TryRecvError::Empty) => {
                    if start.elapsed() >= timeout {
                        return Err(RecvTimeoutError::Timeout);
                    }
                }
                Err(TryRecvError::Disconnected) => {
                    return Err(RecvTimeoutError::Disconnected);
                }
            }
        }
    }
}
