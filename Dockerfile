FROM alpine:latest as certs
RUN apk --update add ca-certificates

FROM golang:1.18-alpine
RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh
RUN apk add gcc
RUN apk add musl-dev
RUN go mod download github.com/open-telemetry/opentelemetry-collector-contrib/pkg/batchpersignal@v0.66.0
COPY docker_builder.yaml docker_builder.yaml
COPY ./loadbalancerhttp ./loadbalancerhttp
COPY ./gcs_exporter ./gcs_exporter
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

RUN go install go.opentelemetry.io/collector/cmd/builder@v0.66.0

USER root

RUN  mkdir /go/pkg/mod/cache/vcs
RUN builder --config docker_builder.yaml --output-path executable
ENTRYPOINT ["executable/otelcol-custom"]
CMD ["--config", "gcs_exporter/example.yaml"]
EXPOSE 4317 55680 55679
