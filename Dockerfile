# --- Server binary (requires CGO for DuckDB) ---
FROM golang:1.24-alpine AS builder-server

RUN apk add --no-cache gcc musl-dev

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -ldflags="-s -w" -o metadata-api ./cmd/server

# --- Converter binary (pure Go, no CGO) ---
FROM golang:1.24-alpine AS builder-convert

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o convert ./cmd/convert

# --- Runtime: server ---
FROM alpine:3.21 AS server

RUN apk add --no-cache ca-certificates libstdc++

WORKDIR /app
COPY --from=builder-server /app/metadata-api .

EXPOSE 8080

ENTRYPOINT ["/app/metadata-api"]

# --- Runtime: converter ---
FROM alpine:3.21 AS converter

RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder-convert /app/convert .

ENTRYPOINT ["/app/convert"]
