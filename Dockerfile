FROM golang:1.23-alpine AS builder

ARG CMD

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /service ./cmd/${CMD}

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /service /service
ENTRYPOINT ["/service"]
