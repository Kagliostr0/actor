package actor

// This file defines the lifecycle messages the engine injects into every
// actor's mailbox. They are ordinary messages: they travel the same inbox as
// user messages and are handled in the same type switch inside Receive, so
// setup and teardown use the same code path as everything else.
//
// The engine guarantees this delivery order for one incarnation of an actor:
//
//	Initialized -> Started -> user messages -> Stopped
//
// A restart produces a fresh Initialized/Started pair delivered to a new
// Receiver instance (see Producer); the previous incarnation has already
// received its Stopped.
//
// All three types are empty structs on purpose: they carry no data, cost no
// allocation, and are matched purely by type in a switch.

// Initialized is the first message an actor ever receives. The engine
// delivers it before the actor's PID is added to the registry, so the actor
// is not reachable yet: nobody can have sent it anything, and this is the
// only message it can possibly be holding.
//
// Use it to allocate state: open resources, initialize fields, build caches.
//
// It is safe to inspect the Context (own PID, parent, engine), but sending
// to oneself or expecting replies from others is premature — the actor
// cannot be looked up until Started.
type Initialized struct{}

// Started is delivered after the actor's PID has been added to the registry.
// The actor is now registered and reachable, and user messages may follow
// immediately — possibly from senders that were waiting for the PID to
// appear.
//
// Use it for work that requires reachability: announcing oneself to peers,
// subscribing to the event stream, or kicking off initial requests to other
// actors.
type Started struct{}

// Stopped is the last message an actor ever receives. The engine delivers it
// after the inbox has been drained and the actor's PID has been removed from
// the registry. No further messages will arrive for this incarnation.
//
// Use it to release resources: close files, flush buffers, deregister from
// external systems.
//
// Sending from here is not guaranteed to be delivered: the actor is already
// unregistered, and other actors may be shutting down too.
type Stopped struct{}
