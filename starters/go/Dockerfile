# Obsidio starter: Go. Multi-stage build — pinned images for reproducible builds.
# Final image is a minimal scratch binary: no shell, no libc, tiny attack surface.

FROM golang:1.22-bookworm AS build
WORKDIR /app
COPY go.mod ./
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o server .

FROM scratch
COPY --from=build /app/server /server
COPY priceDB.json /priceDB.json
EXPOSE 8080
CMD ["/server"]
