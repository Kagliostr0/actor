.PHONY: test bench vet lint cover build

build:
	go build ./...

test:
	go test -race -count=1 ./...

bench:
	go test -run=^$$ -bench=. -benchmem ./...

vet:
	go vet ./...

lint: vet
	gofmt -l .

cover:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out
