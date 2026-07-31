# Run this dockerfile from the top level of the indigo git repository like:
#
#   podman build -f ./cmd/tap/Dockerfile -t tap .

### Compile stage
FROM golang:1.25-alpine3.22@sha256:65b4400aee0927412e9ed791a11893273a49d55df24841f7599660fb80dae464 AS build-env
RUN apk add --no-cache build-base make git

ADD . /dockerbuild
WORKDIR /dockerbuild

# timezone data for alpine builds
RUN GIT_VERSION=$(git describe --tags --long --always) && \
    go build -tags timetzdata -o /tap ./cmd/tap

### Run stage
FROM alpine:3.22@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce

RUN apk add --no-cache --update dumb-init ca-certificates runit
ENTRYPOINT ["dumb-init", "--"]

RUN mkdir -p /data
WORKDIR /data
COPY --from=build-env /tap /

# small things to make golang binaries work well under alpine
ENV GODEBUG=netdns=go
ENV TZ=Etc/UTC

EXPOSE 2480

CMD ["/tap", "run"]

LABEL org.opencontainers.image.source=https://github.com/codebenderhq/indigo
LABEL org.opencontainers.image.description="AT Protocol Sync Utility (tap)"
LABEL org.opencontainers.image.licenses=MIT
