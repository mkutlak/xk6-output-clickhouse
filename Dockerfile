FROM --platform=$BUILDPLATFORM golang:1.25-alpine3.23 AS builder
ARG TARGETOS
ARG TARGETARCH
ARG XK6_VERSION=v1.3.4
ARG MODULE_NAME=github.com/mkutlak/xk6-output-clickhouse
WORKDIR /build

RUN apk --no-cache add git ca-certificates

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go install go.k6.io/xk6/cmd/xk6@${XK6_VERSION}

COPY go.mod go.sum ./

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    xk6 build \
    --with ${MODULE_NAME}=. \
    --output /build/k6

FROM alpine:3.23
LABEL org.opencontainers.image.title="xk6-output-clickhouse" \
      org.opencontainers.image.description="k6 with ClickHouse output extension" \
      org.opencontainers.image.source="https://github.com/mkutlak/xk6-output-clickhouse"

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -g 12345 k6 && \
    adduser -D -u 12345 -G k6 k6

COPY --from=builder /build/k6 /usr/bin/k6
USER 12345
WORKDIR /home/k6
ENTRYPOINT ["k6"]
