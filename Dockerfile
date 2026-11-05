# Build the manager binary
FROM --platform=$BUILDPLATFORM golang:1.27.3 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace
COPY go.mod go.mod
COPY go.sum go.sum
RUN --mount=type=cache,target=/go/pkg \
    go mod download || (sleep 5 && go mod download) || (sleep 10 && go mod download)

COPY cmd/main.go cmd/main.go
COPY api/ api/
COPY internal/ internal/

FROM builder AS manager-builder
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -a -o manager cmd/main.go

FROM gcr.io/distroless/static:nonroot AS manager
WORKDIR /
COPY --from=manager-builder /workspace/manager .
USER 65532:65532

ENTRYPOINT ["/manager"]
