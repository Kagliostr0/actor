package ringbuffer

import (
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
)

const (
	// minSize is the smallest backing array a RingBuffer will allocate. Two
	// slots keep the power-of-two mask meaningful and make a zero or negative
	// size harmless.
	minSize = int64(2)

	// maxSize is the largest size New will round up to; it keeps the
	// power-of-two rounding from overflowing int64.
	maxSize = int64(1) << 62
)

// buffer is a backing array together with the mask that maps an absolute
// cursor onto one of its slots. Both fields are immutable once the buffer has
// been published through RingBuffer.content: growth allocates a new buffer and
// swaps the pointer instead of mutating these fields, so a reader that has
// loaded the pointer always sees a consistent (items, mask) pair.
type buffer[T any] struct {
	items []T
	mask  int64 // len(items)-1; len(items) is always a power of two
}

// RingBuffer is a growable multi-producer, single-consumer queue.
//
// Any number of goroutines may call Push concurrently. Pop and PopN must be
// called from exactly one goroutine — for an actor, the scheduler goroutine
// that drains its inbox. That single-consumer invariant is what makes the
// design safe: the head cursor has exactly one writer, so the consumer never
// coordinates with anybody on the common path.
//
// Push never blocks waiting for room and never drops an item: a full backing
// array is replaced by one twice the size.
//
// # Synchronization
//
// The cursors are absolute, ever-increasing indices; a slot is found by
// masking. Which is protected by what:
//
//   - mu serializes producers with each other and guards growth. The consumer
//     never acquires it.
//   - tail is the next free absolute index. Written only by producers, only
//     while holding mu. Read by the grow path.
//   - head is the next absolute index to pop. Written only by the consumer.
//     Producers load it to decide whether the buffer is full; a stale value is
//     safe because head only ever grows, so the producer's view of "how full"
//     errs on the side of full.
//   - length is the queued item count, maintained with atomic adds by both
//     sides. A producer's increment is what publishes a written slot to the
//     consumer.
//   - content is the backing array. Written only by a producer that is growing
//     the buffer (holding mu); loaded atomically by everyone.
//   - growing and popping are the handshake that keeps a grow from touching
//     slots the consumer is clearing. See grow.
type RingBuffer[T any] struct {
	content atomic.Pointer[buffer[T]]

	head   atomic.Int64
	tail   atomic.Int64
	length atomic.Int64

	mu sync.Mutex

	growing atomic.Bool
	popping atomic.Bool
}

// New returns an empty RingBuffer with room for size items. size is rounded up
// to a power of two and clamped to a minimum of 2, so zero and negative values
// are safe. The buffer grows on demand, so size is a starting point and not a
// limit; pick the batch size you expect to drain to avoid growth at runtime.
func New[T any](size int64) *RingBuffer[T] {
	size = roundUpPow2(size)
	rb := &RingBuffer[T]{}
	rb.content.Store(&buffer[T]{items: make([]T, size), mask: size - 1})
	return rb
}

// Push appends item to the back of the queue and always reports true. It is
// safe to call from any number of goroutines.
//
// Push never blocks waiting for room and never drops an item: when the backing
// array is full it is replaced by one of twice the size, so the only bound is
// memory. The bool result exists so callers can stay source-compatible with a
// bounded implementation that refuses items instead of growing.
func (rb *RingBuffer[T]) Push(item T) bool {
	rb.mu.Lock()
	b := rb.content.Load()
	tail := rb.tail.Load()
	if tail-rb.head.Load() > b.mask { // == cap: no free slot
		b = rb.grow(b, tail)
	}
	b.items[tail&b.mask] = item
	rb.tail.Store(tail + 1)
	// Publishes the slot written above: the consumer only reads a slot after
	// it has observed this increment.
	rb.length.Add(1)
	rb.mu.Unlock()
	return true
}

// Pop removes and returns the item at the front of the queue. It reports false,
// with the zero value of T, when the queue is empty.
//
// Pop must be called by the single consumer goroutine, the same one that calls
// PopN. Calling it from two goroutines at once corrupts the queue.
func (rb *RingBuffer[T]) Pop() (T, bool) {
	var zero T
	if rb.length.Load() == 0 {
		return zero, false
	}

	rb.enterConsumer()
	b := rb.content.Load()
	head := rb.head.Load()
	slot := head & b.mask
	item := b.items[slot]
	// Clear before publishing the slot as free, or a producer could fill it
	// and have its item wiped. Clearing at all is what keeps a drained inbox
	// from pinning the objects it used to hold.
	b.items[slot] = zero
	rb.length.Add(-1)
	rb.head.Store(head + 1)
	rb.leaveConsumer()

	return item, true
}

// PopN removes up to n items from the front of the queue and returns them in
// FIFO order. It reports false when n is not positive or the queue is empty.
// The returned slice is freshly allocated and never aliases the backing array,
// so the caller may hold on to it while producers keep pushing.
//
// PopN must be called by the single consumer goroutine, the same one that calls
// Pop. Draining a batch in one call is what lets a scheduler amortize its
// wake-up over many messages.
func (rb *RingBuffer[T]) PopN(n int64) ([]T, bool) {
	if n <= 0 {
		return nil, false
	}
	queued := rb.length.Load()
	if queued == 0 {
		return nil, false
	}
	n = min(n, queued)
	// Allocate before entering the critical section: a producer that wants to
	// grow the buffer waits for the consumer to leave it. Reading length early
	// is safe because we are the only consumer, so the queue can only have
	// grown by the time we get there.
	items := make([]T, n)

	rb.enterConsumer()
	b := rb.content.Load()
	head := rb.head.Load()
	start := head & b.mask
	// The batch is contiguous up to the end of the array, then wraps.
	first := min(n, b.mask+1-start)
	copy(items, b.items[start:start+first])
	clear(b.items[start : start+first])
	if rest := n - first; rest > 0 {
		copy(items[first:], b.items[:rest])
		clear(b.items[:rest])
	}
	rb.length.Add(-n)
	rb.head.Store(head + n)
	rb.leaveConsumer()

	return items, true
}

// Len returns the number of queued items.
//
// It is advisory: a producer may push, or the consumer may pop, the instant it
// returns. Use it to size a PopN batch or to report a metric, never as an
// invariant.
func (rb *RingBuffer[T]) Len() int64 {
	return rb.length.Load()
}

// grow replaces old with a backing array of twice the size, copies the live
// items into it in FIFO order and returns it. The caller must hold mu, which
// keeps other producers out.
//
// Keeping the consumer out is the job of the growing/popping handshake, which
// is Dekker's algorithm over two sequentially consistent flags: the consumer
// sets popping before it reads growing, and grow sets growing before it reads
// popping, so at least one side sees the other's flag. Either the consumer
// stands down and waits, or grow waits here until the consumer has left its
// critical section. Without it, grow could read a slot while the consumer is
// clearing it — a data race — and could copy an item that was already popped
// back into the new array.
func (rb *RingBuffer[T]) grow(old *buffer[T], tail int64) *buffer[T] {
	rb.growing.Store(true)
	defer rb.growing.Store(false)
	for rb.popping.Load() {
		runtime.Gosched()
	}

	// The consumer may have drained while we waited, in which case there is
	// room again and doubling would only waste memory.
	head := rb.head.Load()
	if tail-head <= old.mask {
		return old
	}

	size := (old.mask + 1) * 2
	items := make([]T, size)
	mask := size - 1
	for i := head; i < tail; i++ {
		items[i&mask] = old.items[i&old.mask]
	}
	b := &buffer[T]{items: items, mask: mask}
	rb.content.Store(b)
	return b
}

// enterConsumer opens the consumer's critical section, standing down while a
// producer is growing the buffer. See grow for why the handshake is needed and
// why it cannot deadlock: the consumer clears popping before it waits, so the
// producer always gets to finish.
func (rb *RingBuffer[T]) enterConsumer() {
	for {
		rb.popping.Store(true)
		if !rb.growing.Load() {
			return
		}
		rb.popping.Store(false)
		for rb.growing.Load() {
			runtime.Gosched()
		}
	}
}

// leaveConsumer closes the consumer's critical section, releasing any producer
// waiting to grow the buffer.
func (rb *RingBuffer[T]) leaveConsumer() {
	rb.popping.Store(false)
}

// roundUpPow2 returns size rounded up to a power of two, clamped to
// [minSize, maxSize].
func roundUpPow2(size int64) int64 {
	if size <= minSize {
		return minSize
	}
	if size >= maxSize {
		return maxSize
	}
	if size&(size-1) == 0 {
		return size
	}
	return int64(1) << bits.Len64(uint64(size-1))
}
