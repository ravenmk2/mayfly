FROM alpine:3

RUN apk add --no-cache ca-certificates

COPY mayfly /usr/local/bin/mayfly

ENTRYPOINT ["mayfly"]
