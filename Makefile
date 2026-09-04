.PHONY: lint test build

lint:
	go vet ./...
	@files=$$(gofmt -l . 2>/dev/null); if [ -n "$$files" ]; then echo "ERROR: gofmt drift detected, run 'gofmt -w .'"; echo "$$files"; exit 1; fi

test:
	go test ./...

build:
	go build -trimpath -o bin/taskrunner ./cmd/taskrunner
