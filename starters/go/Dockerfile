# Obsidio starter: Go. Multi-stage build, pinned images, small final image.
FROM golang:1.22-bookworm AS build
WORKDIR /app
COPY go.mod ./
COPY main.go ./
RUN CGO_ENABLED=0 go build -o server main.go

FROM debian:bookworm-slim
COPY --from=build /app/server /server
EXPOSE 8080
CMD ["/server"]
