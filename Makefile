all: test build

test:
	go test -v ./...

format:
	go fmt ./...

deps:
	go mod tidy

build:
	go build -mod=vendor -o bin/aion

release:
	go build -mod=vendor -ldflags "-s -w" -o bin/aion
