package actor

// Receiver is the contract every actor implements: a single method that
// handles one delivered message. An actor is nothing more than a Receiver
// plus the private state its methods close over.
//
// All messages — user messages and the engine's lifecycle messages
// (Initialized, Started, Stopped) — arrive through this one method, so an
// actor sets up, tears down, and does its work in a single type switch.
// There are no special hooks.
//
// Receive is always called from the actor's own goroutine, one call at a
// time. Implementations may therefore keep un-synchronized private state.
// The *Context must not be retained after Receive returns.
type Receiver interface {
	Receive(*Context)
}

// Producer creates a brand-new Receiver. The engine calls it once when the
// actor is spawned and again on every restart, so each incarnation of an
// actor starts from a fresh zero value.
//
// Because restarts get a whole new Receiver — not a reset old one — an actor
// may keep un-synchronized private state without defensive copying: whatever
// an incarnation accumulated dies with it, and its replacement starts clean.
//
// A Producer must return a distinct instance on every call. Sharing one
// Receiver across incarnations would leak state across restarts and defeat
// the supervision model.
type Producer func() Receiver
