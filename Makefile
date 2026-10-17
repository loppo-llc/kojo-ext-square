VERSION := $(shell sed -n 's/.*"version": *"\([^"]*\)".*/\1/p' kojo-package.json | head -1)
LDFLAGS := -s -w -X main.version=$(VERSION)
PLATFORMS := darwin-arm64 darwin-amd64 linux-amd64 linux-arm64 windows-amd64

.PHONY: all build test clean

all: test build

test:
	go vet ./...
	go test ./...

# kojo installs a package by cloning it, so the binaries are committed.
# -trimpath, -buildvcs=false and CGO_ENABLED=0 keep them reproducible and static.
build:
	@for p in $(PLATFORMS); do \
		os=$${p%-*}; arch=$${p#*-}; ext=; \
		[ "$$os" = windows ] && ext=.exe; \
		echo "build $$os/$$arch"; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch go build -trimpath -buildvcs=false -ldflags "$(LDFLAGS)" \
			-o bin/$$p/kojo-ext-square$$ext . || exit 1; \
	done

clean:
	rm -rf bin
