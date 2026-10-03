FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kgod ./cmd/kgod \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kgoctl ./cmd/kgoctl \
 && mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/kgod /out/kgoctl /usr/local/bin/
COPY --from=build --chown=65532:65532 /out/data /data
EXPOSE 9092
ENTRYPOINT ["/usr/local/bin/kgod"]
CMD ["--listen", ":9092", "--data-dir", "/data"]
