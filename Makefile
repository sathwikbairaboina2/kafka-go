GO_IMAGE ?= golang:1.26
DOCKER_GO = docker run --rm -v "$(CURDIR):/src" -v kafka-go-gomod:/go/pkg/mod \
	-v kafka-go-gobuild:/root/.cache/go-build -w /src $(GO_IMAGE)

.PHONY: test check fuzz build compat demo bench

test:
	$(DOCKER_GO) go test -race -count=1 ./...

check:
	$(DOCKER_GO) sh scripts/check.sh

fuzz:
	$(DOCKER_GO) go test -run '^$$' -fuzz '^FuzzReader$$' -fuzztime 15s ./internal/protocol
	$(DOCKER_GO) go test -run '^$$' -fuzz '^FuzzParseBatch$$' -fuzztime 15s ./internal/record
	$(DOCKER_GO) go test -run '^$$' -fuzz '^FuzzRecoverSegment$$' -fuzztime 15s ./internal/storage

build:
	docker build -t kafka-go/kgod:dev .

compat:
	bash scripts/compat-kcat.sh

demo:
	bash scripts/demo.sh

bench:
	bash scripts/bench.sh
