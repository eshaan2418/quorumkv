NPROC := $(shell getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)
J ?= $(shell n=$(NPROC); h=$$((n / 2)); [ $$h -lt 2 ] && h=2; echo $$h)
export GOMAXPROCS ?= $(J)

.PHONY: build test test-short lint bench chaos demo clean

build:
	go build -o bin/ ./cmd/...

test:
	go test -race -count=1 ./...

test-short:
	go test -race -short ./...

lint:
	gofmt -l . | tee /dev/stderr | (! read)
	go vet ./...
	golangci-lint run ./...

bench: build
	./scripts/bench.sh

# Multi-process chaos campaign: safe read modes must always be linearizable;
# unsafe local reads are run as a control to show the checker catches them.
chaos: build
	./scripts/chaos.sh

demo: build
	./scripts/demo.sh

clean:
	rm -rf bin
