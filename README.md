glock
=====

Boom

# Building with Docker

Cross compilation with `gox`:

  go get github.com/mitchellh/gox
  gox -build-toolchain
  gox -osarch="linux/amd64"
  docker build -t iron/glock .

Otherwise, use a Go build container.
