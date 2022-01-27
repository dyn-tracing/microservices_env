FROM alpine:latest as certs
RUN apk --update add ca-certificates

FROM golang:1.17-alpine
RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh
RUN apk add gcc
RUN apk add musl-dev
RUN go get github.com/hashicorp/go-rootcerts@v1.0.2
RUN go get go.opentelemetry.io/collector/internal/obsreportconfig/obsmetrics@v0.43.1
RUN go get go.opentelemetry.io/collector/component
COPY ./custom_opentelemetry_collector ./custom_opentelemetry_collector
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

RUN go install go.opentelemetry.io/collector/cmd/builder@latest

#ARG USER_UID=10001
#USER ${USER_UID}
USER root

RUN  mkdir /go/pkg/mod/cache/vcs
RUN builder --config custom_opentelemetry_collector/docker_builder.yaml
ENTRYPOINT ["/var/folders/vj/smckm_qs4052kd93p4zybytr0000gn/T/otelcol-distribution2816830151"]
CMD ["--config", "custom_opentelemetry_collector/example.yaml"]
EXPOSE 4317 55680 55679
