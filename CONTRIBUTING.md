# Contributing

This repo is being built issue by issue. A few ground rules keep the pieces
fitting together.

- **One issue per PR.** Reference it in the PR title (`#7: process registry`).
- **Respect the scope section of the issue.** If an issue says a feature belongs
  to a later issue, do not implement it — a half-built version of issue #13 in
  issue #8's PR is worse than no version.
- **`make test` must pass**, and it runs with `-race`. Concurrency bugs in an
  actor runtime are not "flaky tests".
- **`gofmt -l .` must be empty** and `go vet ./...` must be clean. CI enforces both.
- **No new dependencies in `actor/` or `ringbuffer/`.** The core is stdlib-only.
  `github.com/stretchr/testify` is allowed in `_test.go` files.
- **Exported symbols get GoDoc comments.** The doc comment is part of the API.
- **Keep the hot path allocation-free** where you reasonably can. If you add an
  allocation per message, say so in the PR and justify it.
