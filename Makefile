SHELL := /bin/sh

GO := go
MODULE_DIR := store
PROTO_FILES := store/wal/pb/wal.proto store/snapshot/pb/snapshot.proto
GO_FILES := $(shell find $(MODULE_DIR) -name '*.go' -not -path '*/vendor/*')

.PHONY: build test test-race vet fmt fmt-check mod-check check generate generate-check

build:
	$(GO) -C $(MODULE_DIR) build ./...

test:
	$(GO) -C $(MODULE_DIR) test ./... -count=1 -timeout 5m

test-race:
	$(GO) -C $(MODULE_DIR) test -race ./... -count=1 -timeout 10m

vet:
	$(GO) -C $(MODULE_DIR) vet ./...

fmt:
	gofmt -w $(GO_FILES)

fmt-check:
	@files="$$(gofmt -l $(GO_FILES))"; \
	if [ -n "$$files" ]; then \
		echo "The following Go files need formatting:"; \
		echo "$$files"; \
		exit 1; \
	fi

mod-check:
	$(GO) -C $(MODULE_DIR) mod verify
	$(GO) -C $(MODULE_DIR) mod tidy -diff

check: fmt-check mod-check build vet test

generate:
	@command -v protoc >/dev/null || { echo "protoc is required"; exit 1; }
	@command -v protoc-gen-go >/dev/null || { echo "protoc-gen-go is required"; exit 1; }
	protoc --go_out=. --go_opt=paths=source_relative $(PROTO_FILES)

generate-check: generate
	git diff --exit-code -- store/wal/pb/wal.pb.go store/snapshot/pb/snapshot.pb.go
