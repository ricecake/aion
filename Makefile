all: test build

test:
	go test -v ./...

format:
	go fmt ./...

deps:
	go mod tidy

build:
	go build -o bin/aion

release:
	go build -ldflags "-s -w" -o bin/aion
