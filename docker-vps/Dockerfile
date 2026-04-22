ARG GO_VERSION=1.26.1

FROM golang:${GO_VERSION}-bookworm AS build

WORKDIR /src

COPY relay ./relay
COPY headless/telemost ./headless/telemost

WORKDIR /src/headless/telemost

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ENV CGO_ENABLED=0 \
    GOOS=${TARGETOS} \
    GOARCH=${TARGETARCH} \
    GOFLAGS=-buildvcs=false

RUN mkdir -p /out/work \
    && go build -trimpath -ldflags="-s -w" -o /out/headless-telemost-creator .

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build --chown=65532:65532 /out/work /work
COPY --from=build /out/headless-telemost-creator /usr/local/bin/headless-telemost-creator

WORKDIR /work
USER 65532:65532

ENTRYPOINT ["/usr/local/bin/headless-telemost-creator"]
CMD ["--help"]
