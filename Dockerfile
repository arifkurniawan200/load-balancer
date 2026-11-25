# syntax=docker/dockerfile:1
FROM golang:1.24-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/lb ./cmd/lb && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/backend ./cmd/backend

FROM alpine:3.20

RUN apk add --no-cache ca-certificates wget && \
    addgroup -S app && adduser -S -G app app

COPY --from=builder /out/lb /usr/local/bin/lb
COPY --from=builder /out/backend /usr/local/bin/backend
COPY config.docker.yaml /etc/lb/config.yaml

USER app
EXPOSE 8080 8081

ENTRYPOINT ["/usr/local/bin/lb"]
CMD ["-config", "/etc/lb/config.yaml"]
