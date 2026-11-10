# Runtime stage for raftkv
FROM alpine:3.20 AS raftkv
RUN apk add --no-cache ca-certificates tzdata
COPY bin/linux/raftkv /usr/local/bin/raftkv
EXPOSE 9000 9001 8080
ENTRYPOINT ["/usr/local/bin/raftkv"]

# Runtime stage for shardedkv
FROM alpine:3.20 AS shardedkv
RUN apk add --no-cache ca-certificates tzdata
COPY bin/linux/shardedkv /usr/local/bin/shardedkv
EXPOSE 9000 8080
ENTRYPOINT ["/usr/local/bin/shardedkv"]

# Runtime stage for loadgen
FROM alpine:3.20 AS loadgen
RUN apk add --no-cache ca-certificates tzdata
COPY bin/linux/loadgen /usr/local/bin/loadgen
ENTRYPOINT ["/usr/local/bin/loadgen"]
