package actor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// testActor records the kind of each message it receives so tests can assert
// the order in which lifecycle and user messages were handled.
type testActor struct {
	recorded []string
}

// Compile-time assertion that *testActor satisfies the Receiver interface.
var _ Receiver = (*testActor)(nil)

func (a *testActor) Receive(c *Context) {
	switch c.Message().(type) {
	case Initialized:
		a.recorded = append(a.recorded, "initialized")
	case Started:
		a.recorded = append(a.recorded, "started")
	case Stopped:
		a.recorded = append(a.recorded, "stopped")
	default:
		a.recorded = append(a.recorded, "user")
	}
}

func TestReceiverInterfaceSatisfied(t *testing.T) {
	// The compile-time assertion above does the real work; this test just
	// exercises the interface value so the assertion is not dead code.
	var r Receiver = &testActor{}
	assert.NotNil(t, r)
}

func TestProducerReturnsFreshInstance(t *testing.T) {
	var producer Producer = func() Receiver { return &testActor{} }

	first := producer()
	second := producer()

	assert.NotSame(t, first, second,
		"each Producer call must return a brand-new Receiver (fresh state on restart)")
}

func TestLifecycleTypeSwitch(t *testing.T) {
	a := &testActor{}
	ctx := newContext(&Engine{}, NewPID("local", "worker-1"))

	// Hand-built deliveries in the order the engine guarantees:
	// Initialized -> Started -> user messages -> Stopped.
	deliveries := []any{
		Initialized{},
		Started{},
		"user-message",
		Stopped{},
	}
	for _, msg := range deliveries {
		a.Receive(ctx.withEnvelope(Envelope{Msg: msg}))
	}

	assert.Equal(t,
		[]string{"initialized", "started", "user", "stopped"},
		a.recorded,
		"lifecycle types must be distinguishable in a type switch inside Receive")
}
