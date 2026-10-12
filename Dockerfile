# Runtime image for the goreleaser pipeline: the release workflow builds the
# platform binary first and drops it at the build-context root as "mayfly",
# so this Dockerfile must only COPY that one artifact.
FROM alpine:3

RUN apk add --no-cache ca-certificates \
    && adduser -D -u 10001 mayfly \
    && mkdir -p /data \
    && chown mayfly:mayfly /data

COPY mayfly /usr/local/bin/mayfly

# 1883 mqtt, 8883 mqtts, 8083 ws, 8084 wss, 8080 console/REST/metrics
EXPOSE 1883 8883 8083 8084 8080

# All mutable state lives under the working directory (./data) by default.
WORKDIR /data
VOLUME ["/data"]

USER mayfly

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:8080/healthz || exit 1

ENTRYPOINT ["mayfly"]
