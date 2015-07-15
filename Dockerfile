FROM scratch
MAINTAINER Iron Ops <ops@iron.io>

ADD glock_linux_amd64 ./glock
EXPOSE 45625
ENTRYPOINT ["/glock"]
