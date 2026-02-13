FROM golang:1.25 AS builder
ENV CGO_ENABLED=0

WORKDIR /src/log-events-consumer

# Copy go.mod and go.sum first for better layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN go build -o log-events-consumer

FROM alpine:3.19
LABEL authors="Qubic Integration"

# Install ca-certificates for HTTPS connections
RUN apk --no-cache add ca-certificates

# Create non-root user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

# Copy executable from build stage
COPY --from=builder /src/log-events-consumer/log-events-consumer .

# Change ownership to non-root user
RUN chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser

ENTRYPOINT ["./log-events-consumer"]