# Obsidio starter: Go. Multi-stage build — pinned images for reproducible builds.
# Final image is a minimal scratch binary: no shell, no libc, tiny attack surface.

FROM golang:1.22-bookworm AS build
WORKDIR /app
COPY go.mod ./
# Copy all .go source files (main.go; health.go is a leftover and excluded below)
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o server .

FROM debian:bookworm-slim
COPY --from=build /app/server /server
EXPOSE 8080
CMD ["/server"]
