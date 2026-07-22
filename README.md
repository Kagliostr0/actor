# actor

A small, fast, in-process actor framework for Go.

**Status: proof of concept under construction.** The API below is the target
contract; it is being built one issue at a time. See the
[open issues](https://github.com/lsproule/actor/issues) for the build order.

## Mental model

An actor is a goroutine that owns some private state and a mailbox. You never
touch that state directly — you send it a message, addressed by a `*PID`. The
engine routes the message into the actor's inbox, and the actor processes
messages one at a time. No locks, no shared memory, no data races.

- **PID** — the address of an actor (`address` + `id`).
- **Inbox** — a lock-free MPSC ring buffer plus a scheduler goroutine.
- **Engine** — spawns actors, routes messages, supervises restarts, and owns
  the event stream.
- **Context** — what an actor sees while handling a message: the message, who
  sent it, its own PID, its parent, and its children.

## The API we are building toward

```go
package main

import (
	"fmt"
	"time"

	"github.com/lsproule/actor/actor"
)

type ping struct{ from string }
type pong struct{}

type ponger struct{}

func newPonger() actor.Receiver { return &ponger{} }

func (p *ponger) Receive(c *actor.Context) {
	switch msg := c.Message().(type) {
	case actor.Initialized:
		// allocate resources; not yet reachable
	case actor.Started:
		fmt.Println("ponger started", c.PID())
	case actor.Stopped:
		fmt.Println("ponger stopped")
	case ping:
		fmt.Println("ping from", msg.from)
		c.Respond(pong{})
	}
}

func main() {
	e, err := actor.NewEngine(actor.NewEngineConfig())
	if err != nil {
		panic(err)
	}

	pid := e.Spawn(newPonger, "ponger",
		actor.WithID("1"),
		actor.WithInboxSize(1024),
	)

	e.Send(pid, ping{from: "main"})

	resp, err := e.Request(pid, ping{from: "main"}, time.Second).Result()
	fmt.Println(resp, err)

	<-e.Poison(pid).Done()
}
```

## Layout

| package       | contents                                                        |
| ------------- | --------------------------------------------------------------- |
| `actor/`      | PID, Context, Engine, inbox, process registry, supervision, events |
| `ringbuffer/` | lock-free MPSC ring buffer backing the inbox                    |
| `examples/`   | runnable demos                                                  |
| `bench/`      | throughput, spawn cost, and request-latency benchmarks          |

## Development

```sh
make test   # go test -race ./...
make vet
make bench
make cover
```

## License

MIT — see [LICENSE](LICENSE).
