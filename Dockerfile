FROM --platform=$BUILDPLATFORM golang:1.25-alpine3.23 AS builder
WORKDIR $GOPATH/src/go.k6.io/k6
RUN apk --no-cache add git && \
    go install go.k6.io/xk6/cmd/xk6@latest
COPY . .
RUN xk6 build --with github.com/mkutlak/xk6-output-clickhouse=. --output /tmp/k6

FROM alpine:3.23
RUN apk add --no-cache --no-scripts ca-certificates && \
    update-ca-certificates 2>/dev/null || true && \
    adduser -D -u 12345 -g 12345 k6

COPY --from=builder /tmp/k6 /usr/bin/k6

USER 12345
WORKDIR /home/k6
ENTRYPOINT ["k6"]
