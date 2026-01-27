.PHONY: build run clean proto test

# Build the binary
build:
	go build -o bin/events-bridge ./cmd/events-bridge

# Run the service
run: build
	./bin/events-bridge

# Run with debug logging
run-debug: build
	./bin/events-bridge --debug

# Clean build artifacts
clean:
	rm -rf bin/

# Generate protobuf code (requires buf)
proto:
	cd api/events-bridge/v1 && buf generate

# Run tests
test:
	go test -v ./...

# Format code
fmt:
	go fmt ./...

# Lint code
lint:
	golangci-lint run

# Install dependencies
deps:
	go mod download
	go mod tidy
