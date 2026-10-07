FROM golang:1.27.1-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -buildvcs=false -trimpath -o /out/broker ./cmd/broker \
 && CGO_ENABLED=0 go build -buildvcs=false -trimpath -o /out/admin ./cmd/admin \
 && CGO_ENABLED=0 go build -buildvcs=false -trimpath -o /out/producer ./cmd/producer \
 && CGO_ENABLED=0 go build -buildvcs=false -trimpath -o /out/consumer ./cmd/consumer \
 && CGO_ENABLED=0 go build -buildvcs=false -trimpath -o /out/demo ./cmd/demo \
 && CGO_ENABLED=0 go build -buildvcs=false -trimpath -o /out/benchmark ./cmd/benchmark \
 && CGO_ENABLED=0 go build -buildvcs=false -trimpath -o /out/replay ./cmd/replay
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/* && useradd -r -u 10001 streamforge \
 && mkdir -p /data && chown streamforge:streamforge /data
COPY --from=build /out/ /usr/local/bin/
USER streamforge
EXPOSE 9001 9101
ENTRYPOINT ["broker"]
