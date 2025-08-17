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
    max: usize,
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
    read: CachePadded<AtomicUsize>,
    state: MutSeatState<T>,
}

impl<T> fmt::Debug for Seat<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Seat")
            .field("read", &self.read)
            .field("state", &self.state)
            .finish()
    }
}

impl<T: Clone + Sync> Seat<T> {
    /// take is used by a reader to extract a copy of the value stored on this seat. only readers
    /// that were created strictly before the time this seat was last written to by the producer
    /// are allowed to call this method, and they may each only call it once.
    fn take(&self) -> T {
        // the writer will only modify this element when .read hits .max - writer.rleft[i]. we can
        // be sure that this is not currently the case (which means it's safe for us to read)
        // because:
        //
        //  - .max is set to the number of readers at the time when the write happens
        //  - any joining readers will start at a later seat
        //  - so, at most .max readers will call .take() on this seat this time around the buffer
        //  - a reader must leave either *before* or *after* a call to recv. there are two cases:
        //
        //    - it leaves before, rleft is decremented, but .take is not called
        //    - it leaves after, .take is called, but head has been incremented, so rleft will be
        //      decremented for the *next* seat, not this one
        //
        //    so, either .take is called, and .read is incremented, or writer.rleft is incremented.
        //    thus, for a writer to modify this element, *all* readers at the time of the previous
        //    write to this seat must have either called .take or have left.
        //  - since we are one of those readers, this cannot be true, so it's safe for us to assume
        //    that there is no concurrent writer for this seat
        let state = unsafe { &*self.state.get() };

        // NOTE
        // we must extract the value *before* we decrement the number of remaining items otherwise,
        // the object might be replaced by the time we read it!
        let v = state
            .val
            .clone()
            .expect("seat that should be occupied was empty");

        self.read.fetch_add(1, Ordering::Relaxed);

        v
    }
}

impl<T> Default for Seat<T> {
    fn default() -> Self {
        Self {
            read: CachePadded::new(AtomicUsize::new(0)),
            state: MutSeatState(UnsafeCell::new(SeatState { max: 0, val: None })),
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
            .field("ring", &self.ring)
            .field("len", &(self.mask + 1))
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

    /// Get the expected number of reads for the given seat. This number will always be
    /// conservative, in that fewer reads may be fine. Specifically, `.rleft` may not be
    /// sufficiently up-to-date to account for all readers that have left.
    #[inline]
    fn expected(&mut self, at: usize) -> usize {
        // since only the producer will modify the ring, and &mut self guarantees that *we* are the
        // producer, no-one is modifying the ring. Multiple read-only borrows are safe, and so the
        // cast below is safe.
        unsafe { &*self.state.ring[at].state.get() }.max
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

        // we want to check if the next element over is free to ensure that we always leave one
        // empty space between the head and the tail. This is necessary so that readers can
        // distinguish between an empty and a full list. If the fence seat is free, the seat at
        // tail must also be free, which is simple enough to show by induction (exercise for the
        // reader).
        let fence = (tail + 1) & self.state.mask;

        let fence_read = self.state.ring[fence].read.load(Ordering::Relaxed);

        // is the fence block now free?
        if fence_read == self.expected(fence) {
            // yes! go ahead and write!
        } else {
            // no, and blocking isn't allowed, so return an error
            return Err(val);
        }

        // next one over is free, we have a free seat!
        let readers = self.readers;
        {
            let next = &self.state.ring[tail];
            // we are the only writer, so no-one else can be writing. however, since we're
            // mutating state, we also need for there to be no readers for this to be safe. the
            // argument for why this is the case is roughly an inverse of the argument for why
            // the unsafe block in Seat.take() is safe.  basically, since
            //
            //   .read + .rleft == .max
            //
            // we know all readers at the time of the seat's previous write have accessed this
            // seat. we also know that no other readers will access that seat (they must have
            // started at later seats). thus, we are the only thread accessing this seat, and
            // so we can safely access it as mutable.
            let state = unsafe { &mut *next.state.get() };
            state.max = readers;
            state.val = Some(val);
            next.read.store(0, Ordering::Relaxed);
        }
        // now tell readers that they can read
        self.state.tail.store(fence, Ordering::Release);

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
            closed: false,
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
    closed: bool,
}

impl<T> fmt::Debug for BusReader<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BusReader")
            .field("bus", &self.bus)
            .field("head", &self.head)
            .field("closed", &self.closed)
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
        if self.closed {
            return Err(TryRecvError::Disconnected);
        }

        let tail = self.bus.tail.load(Ordering::Acquire);
        if tail == self.head {
            // buffer is empty, check whether it's closed.
            // relaxed is fine since Bus.drop does an acquire/release on tail
            if self.bus.closed.load(Ordering::Relaxed) {
                // the bus is closed, and we didn't miss anything!
                self.closed = true;
                return Err(TryRecvError::Disconnected);
            }

            return Err(TryRecvError::Empty);
        }

        let head = self.head;
        let ret = self.bus.ring[head].take();

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
