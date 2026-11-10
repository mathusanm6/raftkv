# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/raftkv ./cmd/raftkv
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/shardedkv ./cmd/shardedkv
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/loadgen ./cmd/loadgen

# Runtime stage for raftkv
FROM alpine:3.20 AS raftkv
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /bin/raftkv /usr/local/bin/raftkv
EXPOSE 9000 9001 8080
ENTRYPOINT ["/usr/local/bin/raftkv"]

# Runtime stage for shardedkv
FROM alpine:3.20 AS shardedkv
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /bin/shardedkv /usr/local/bin/shardedkv
EXPOSE 9000 8080
ENTRYPOINT ["/usr/local/bin/shardedkv"]

# Runtime stage for loadgen
FROM alpine:3.20 AS loadgen
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /bin/loadgen /usr/local/bin/loadgen
ENTRYPOINT ["/usr/local/bin/loadgen"]
