.PHONY: hooks commit-lint test-commit-lint lint test build e2e

lint:
	go vet ./...
	@files=$$(gofmt -l . 2>/dev/null); if [ -n "$$files" ]; then echo "ERROR: gofmt drift detected, run 'gofmt -w .'"; echo "$$files"; exit 1; fi

test:
	go test ./...

build:
	go build -trimpath -o bin/taskrunner ./cmd/taskrunner

e2e:
	scripts/e2e/e2e.sh

# RR-GIT-01: self-contained commit checks; installation is worktree-scoped.
hooks:
	sh scripts/install-git-hooks.sh

commit-lint:
	python3 scripts/commit-policy.py pending

test-commit-lint:
	python3 scripts/test-commit-policy.py
