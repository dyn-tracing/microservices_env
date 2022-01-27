FROM alpine:latest as certs
RUN apk --update add ca-certificates

FROM golang:1.17-alpine
RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh
RUN go get github.com/hashicorp/go-rootcerts@v1.0.2
RUN go get go.opentelemetry.io/collector/consumer/pdata
RUN go get go.opentelemetry.io/collector/internal/obsreportconfig/obsmetrics@v0.43.1
RUN  go get go.opentelemetry.io/collector/config/configmodels                      
COPY ./custom_opentelemetry_collector ./custom_opentelemetry_collector
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
RUN go install go.opentelemetry.io/collector/cmd/builder@v0.37.0

#ARG USER_UID=10001
#USER ${USER_UID}
USER root

RUN  mkdir /go/pkg/mod/cache/vcs
RUN builder --config custom_opentelemetry_collector/docker_builder.yaml
ENTRYPOINT ["tail", "-f", "/dev/null"]
EXPOSE 4317 55680 55679
