// Package ringbuffer provides the lock-free multi-producer, single-consumer
// queue that backs an actor's inbox.
//
// Many goroutines may push into a [RingBuffer]; exactly one drains it, one item
// at a time with [RingBuffer.Pop] or a whole batch at a time with
// [RingBuffer.PopN]. Pushing never blocks and never drops an item: a full
// buffer is replaced by one twice the size.
//
// This package is deliberately unaware of actors and imports nothing outside
// the standard library.
package ringbuffer
