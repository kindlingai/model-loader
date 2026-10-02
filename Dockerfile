# model-loader image, published as ghcr.io/kindlingai/model-loader:<version>
# for linux/amd64 and linux/arm64.
#
# The binary is static (CGO_ENABLED=0) and sits at /usr/local/bin/model-loader,
# so kindling-spark-os can COPY --from this image pinned by digest, the same
# way it takes mentatd. The runtime is alpine rather than distroless because
# deploy/compose wraps the command in /bin/sh to expand env vars.
ARG GO_IMAGE=golang:1.26-alpine
ARG RUNTIME_IMAGE=alpine:3

# Cross-compile on the build platform; Go needs no emulation for this.
FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS build
ARG TARGETOS TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/model-loader ./cmd/model-loader

FROM ${RUNTIME_IMAGE}
LABEL org.opencontainers.image.title="model-loader" \
      org.opencontainers.image.description="Download model files once and copy them to every node on the LAN" \
      org.opencontainers.image.source="https://github.com/kindlingai/model-loader"
COPY --from=build /out/model-loader /usr/local/bin/model-loader
EXPOSE 7762
ENTRYPOINT ["model-loader"]
