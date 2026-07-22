package ringbuffer

import (
	"runtime"
	"sync"
	"testing"
)

func TestNewRoundsSizeUpToPowerOfTwo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		size int64
		want int64
	}{
		{size: -1, want: 2},
		{size: 0, want: 2},
		{size: 1, want: 2},
		{size: 2, want: 2},
		{size: 3, want: 4},
		{size: 4, want: 4},
		{size: 5, want: 8},
		{size: 1000, want: 1024},
		{size: 1024, want: 1024},
	}
	for _, tt := range tests {
		rb := New[int](tt.size)
		if got := rb.content.Load().mask + 1; got != tt.want {
			t.Errorf("New(%d) capacity = %d, want %d", tt.size, got, tt.want)
		}
	}
}

func TestPushPopSingle(t *testing.T) {
	t.Parallel()

	rb := New[int](4)
	for i := 1; i <= 3; i++ {
		if !rb.Push(i) {
			t.Fatalf("Push(%d) = false, want true", i)
		}
	}
	for i := 1; i <= 3; i++ {
		got, ok := rb.Pop()
		if !ok {
			t.Fatalf("Pop() #%d = _, false; want true", i)
		}
		if got != i {
			t.Fatalf("Pop() #%d = %d, want %d (FIFO)", i, got, i)
		}
	}
	if got, ok := rb.Pop(); ok || got != 0 {
		t.Fatalf("Pop() on empty = (%d, %v), want (0, false)", got, ok)
	}
}

func TestPopClearsSlot(t *testing.T) {
	t.Parallel()

	rb := New[*int](2)
	v := 42
	rb.Push(&v)
	if _, ok := rb.Pop(); !ok {
		t.Fatal("Pop() = _, false; want true")
	}
	if got := rb.content.Load().items[0]; got != nil {
		t.Errorf("slot after Pop = %v, want nil (popped items must not stay reachable)", got)
	}

	rb.Push(&v)
	if _, ok := rb.PopN(1); !ok {
		t.Fatal("PopN(1) = _, false; want true")
	}
	if got := rb.content.Load().items[1]; got != nil {
		t.Errorf("slot after PopN = %v, want nil (popped items must not stay reachable)", got)
	}
}

func TestPopN(t *testing.T) {
	t.Parallel()

	rb := New[int](16)
	for i := 0; i < 10; i++ {
		rb.Push(i)
	}

	want := 0
	for batch := 0; batch < 2; batch++ {
		items, ok := rb.PopN(4)
		if !ok {
			t.Fatalf("PopN(4) #%d = _, false; want true", batch+1)
		}
		if len(items) != 4 {
			t.Fatalf("PopN(4) #%d returned %d items, want 4", batch+1, len(items))
		}
		for _, got := range items {
			if got != want {
				t.Fatalf("PopN(4) #%d = %d, want %d (FIFO)", batch+1, got, want)
			}
			want++
		}
	}

	items, ok := rb.PopN(10)
	if !ok {
		t.Fatal("PopN(10) = _, false; want true")
	}
	if len(items) != 2 {
		t.Fatalf("PopN(10) returned %d items, want the 2 remaining", len(items))
	}
	if items[0] != 8 || items[1] != 9 {
		t.Fatalf("PopN(10) = %v, want [8 9]", items)
	}

	if items, ok := rb.PopN(1); ok || items != nil {
		t.Fatalf("PopN on empty = (%v, %v), want (nil, false)", items, ok)
	}
	if items, ok := rb.PopN(0); ok || items != nil {
		t.Fatalf("PopN(0) = (%v, %v), want (nil, false)", items, ok)
	}
	if items, ok := rb.PopN(-1); ok || items != nil {
		t.Fatalf("PopN(-1) = (%v, %v), want (nil, false)", items, ok)
	}
}

// TestPopNWrapsAround drives head past the end of the backing array so PopN has
// to stitch the batch together from the tail and the head of the array.
func TestPopNWrapsAround(t *testing.T) {
	t.Parallel()

	rb := New[int](4)
	for i := 1; i <= 3; i++ {
		rb.Push(i)
	}
	if _, ok := rb.PopN(2); !ok {
		t.Fatal("PopN(2) = _, false; want true")
	}
	for i := 4; i <= 6; i++ {
		rb.Push(i)
	}

	items, ok := rb.PopN(4)
	if !ok {
		t.Fatal("PopN(4) = _, false; want true")
	}
	want := []int{3, 4, 5, 6}
	if len(items) != len(want) {
		t.Fatalf("PopN(4) = %v, want %v", items, want)
	}
	for i := range want {
		if items[i] != want[i] {
			t.Fatalf("PopN(4) = %v, want %v", items, want)
		}
	}
	if rb.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", rb.Len())
	}
}

// TestGrow starts from the smallest possible buffer and pushes far past its
// capacity: nothing may be dropped or reordered by the doubling.
func TestGrow(t *testing.T) {
	t.Parallel()

	const n = 10000
	rb := New[int](2)
	for i := 0; i < n; i++ {
		if !rb.Push(i) {
			t.Fatalf("Push(%d) = false, want true", i)
		}
	}
	if rb.Len() != n {
		t.Fatalf("Len() = %d, want %d", rb.Len(), n)
	}
	if got := rb.content.Load().mask + 1; got < n {
		t.Fatalf("capacity = %d, want at least %d", got, n)
	}

	for i := 0; i < n; i++ {
		got, ok := rb.Pop()
		if !ok {
			t.Fatalf("Pop() #%d = _, false; want true", i)
		}
		if got != i {
			t.Fatalf("Pop() #%d = %d, want %d (FIFO across growth)", i, got, i)
		}
	}
	if _, ok := rb.Pop(); ok {
		t.Fatal("Pop() on drained buffer = _, true; want false")
	}
}

// TestGrowWithPopN is TestGrow drained in batches, which exercises the wrapping
// copy against a buffer that changed size underneath it.
func TestGrowWithPopN(t *testing.T) {
	t.Parallel()

	const n = 10000
	rb := New[int](2)
	for i := 0; i < n; i++ {
		rb.Push(i)
	}

	want := 0
	for want < n {
		items, ok := rb.PopN(64)
		if !ok {
			t.Fatalf("PopN(64) = _, false; want true after %d items", want)
		}
		for _, got := range items {
			if got != want {
				t.Fatalf("PopN = %d, want %d (FIFO across growth)", got, want)
			}
			want++
		}
	}
	if rb.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", rb.Len())
	}
}

func TestLenTracks(t *testing.T) {
	t.Parallel()

	rb := New[int](4)
	if rb.Len() != 0 {
		t.Fatalf("Len() on a new buffer = %d, want 0", rb.Len())
	}
	for i := 1; i <= 6; i++ {
		rb.Push(i)
		if rb.Len() != int64(i) {
			t.Fatalf("Len() after %d pushes = %d, want %d", i, rb.Len(), i)
		}
	}
	for i := 5; i >= 3; i-- {
		if _, ok := rb.Pop(); !ok {
			t.Fatal("Pop() = _, false; want true")
		}
		if rb.Len() != int64(i) {
			t.Fatalf("Len() after Pop = %d, want %d", rb.Len(), i)
		}
	}
	if _, ok := rb.PopN(2); !ok {
		t.Fatal("PopN(2) = _, false; want true")
	}
	if rb.Len() != 1 {
		t.Fatalf("Len() after PopN(2) = %d, want 1", rb.Len())
	}
	rb.PopN(10)
	if rb.Len() != 0 {
		t.Fatalf("Len() after draining = %d, want 0", rb.Len())
	}
}

const (
	producers   = 8
	perProducer = 10000
	totalPushes = producers * perProducer
)

// value encodes which producer sent an item and its position in that producer's
// stream, so a drained batch can be checked for loss, duplication and per
// producer ordering.
func value(producer, seq int) int { return producer*perProducer + seq }

// checkDrained asserts that items holds every value exactly once and that each
// producer's values arrived in the order it pushed them.
func checkDrained(t *testing.T, items []int) {
	t.Helper()

	if len(items) != totalPushes {
		t.Fatalf("drained %d items, want %d", len(items), totalPushes)
	}
	seen := make([]bool, totalPushes)
	next := make([]int, producers)
	for _, v := range items {
		if v < 0 || v >= totalPushes {
			t.Fatalf("drained bogus value %d", v)
		}
		if seen[v] {
			t.Fatalf("value %d drained twice", v)
		}
		seen[v] = true

		producer, seq := v/perProducer, v%perProducer
		if seq != next[producer] {
			t.Fatalf("producer %d: got seq %d, want %d (FIFO per producer)", producer, seq, next[producer])
		}
		next[producer]++
	}
	for v, ok := range seen {
		if !ok {
			t.Fatalf("value %d was lost", v)
		}
	}
}

// TestConcurrentPush runs the real workload: many producers pushing while one
// consumer drains. The buffer starts at the minimum size so the consumer races
// the growth path over and over.
func TestConcurrentPush(t *testing.T) {
	rb := New[int](2)

	var wg sync.WaitGroup
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		go func(producer int) {
			defer wg.Done()
			for seq := 0; seq < perProducer; seq++ {
				if !rb.Push(value(producer, seq)) {
					t.Errorf("Push = false, want true")
					return
				}
			}
		}(p)
	}

	drained := make([]int, 0, totalPushes)
	for len(drained) < totalPushes {
		items, ok := rb.PopN(256)
		if !ok {
			runtime.Gosched()
			continue
		}
		drained = append(drained, items...)
	}
	wg.Wait()

	checkDrained(t, drained)
	if rb.Len() != 0 {
		t.Fatalf("Len() after draining = %d, want 0", rb.Len())
	}
}

// TestConcurrentPushThenDrain lets the producers race each other with no
// consumer at all, so the buffer has to absorb every item by growing.
func TestConcurrentPushThenDrain(t *testing.T) {
	rb := New[int](2)

	var wg sync.WaitGroup
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		go func(producer int) {
			defer wg.Done()
			for seq := 0; seq < perProducer; seq++ {
				rb.Push(value(producer, seq))
			}
		}(p)
	}
	wg.Wait()

	if rb.Len() != totalPushes {
		t.Fatalf("Len() = %d, want %d", rb.Len(), totalPushes)
	}
	drained := make([]int, 0, totalPushes)
	for {
		item, ok := rb.Pop()
		if !ok {
			break
		}
		drained = append(drained, item)
	}

	checkDrained(t, drained)
}

// TestConcurrentPushSingleItemPop covers the same race as TestConcurrentPush
// with the one-at-a-time consumer, whose critical section is shorter and hits
// the handshake with different timing.
func TestConcurrentPushSingleItemPop(t *testing.T) {
	rb := New[int](2)

	var wg sync.WaitGroup
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		go func(producer int) {
			defer wg.Done()
			for seq := 0; seq < perProducer; seq++ {
				rb.Push(value(producer, seq))
			}
		}(p)
	}

	drained := make([]int, 0, totalPushes)
	for len(drained) < totalPushes {
		item, ok := rb.Pop()
		if !ok {
			runtime.Gosched()
			continue
		}
		drained = append(drained, item)
	}
	wg.Wait()

	checkDrained(t, drained)
}

const benchBatch = 1 << 12

// BenchmarkPush measures a push into a buffer that is drained often enough
// never to grow, which is the steady state of a healthy inbox.
func BenchmarkPush(b *testing.B) {
	rb := New[int](benchBatch)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Push(i)
		if i&(benchBatch-1) == benchBatch-1 {
			b.StopTimer()
			rb.PopN(benchBatch)
			b.StartTimer()
		}
	}
}

// BenchmarkPushParallel measures the multi-producer path — every producer
// contends for the same mutex — while a single consumer drains in batches, as
// the scheduler goroutine would.
func BenchmarkPushParallel(b *testing.B) {
	rb := New[int](benchBatch)
	done := make(chan struct{})
	var consumer sync.WaitGroup

	consumer.Add(1)
	go func() {
		defer consumer.Done()
		for {
			select {
			case <-done:
				return
			default:
				if _, ok := rb.PopN(benchBatch); !ok {
					runtime.Gosched()
				}
			}
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rb.Push(1)
		}
	})
	b.StopTimer()

	close(done)
	consumer.Wait()
}

// BenchmarkPopN measures draining one full batch; the refill is excluded from
// the timer, so ns/op is the cost of a single PopN call.
func BenchmarkPopN(b *testing.B) {
	rb := New[int](benchBatch)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for j := 0; j < benchBatch; j++ {
			rb.Push(j)
		}
		b.StartTimer()

		if _, ok := rb.PopN(benchBatch); !ok {
			b.Fatal("PopN = _, false; want true")
		}
	}
}
