FROM alpine:latest as certs
RUN apk --update add ca-certificates

#FROM scratch
FROM golang:1.17-alpine
RUN go install go.opentelemetry.io/collector/cmd/builder@latest

ARG USER_UID=10001
USER ${USER_UID}

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

ENTRYPOINT ["/builder"]
EXPOSE 4317 55680 55679
