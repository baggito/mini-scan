FROM golang:1.24-alpine AS builder

# Required for CGO (SQLite driver)
RUN apk add --no-cache gcc musl-dev

WORKDIR /app

# Download dependencies first (better caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY . .
RUN CGO_ENABLED=1 go build -o processor ./cmd/processor

# Runtime stage
FROM alpine:latest

# Create data directory for SQLite
RUN mkdir -p /data

COPY --from=builder /app/processor /processor

CMD ["/processor"]
