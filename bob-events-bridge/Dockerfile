FROM golang:1.25 AS builder
ENV CGO_ENABLED=0

WORKDIR /src
COPY . /src

RUN go build -o "./bin/events-bridge" "./cmd/events-bridge"

# We don't need golang to run binaries, just use alpine.
FROM alpine
COPY --from=builder /src/bin/events-bridge /app/events-bridge

EXPOSE 8000
EXPOSE 8001
EXPOSE 8002

WORKDIR /app

ENTRYPOINT ["./events-bridge"]