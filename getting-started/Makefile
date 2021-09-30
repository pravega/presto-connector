REPO=prestodb/presto-with-pravega
VERSION=latest

.PHONY: all build-image

all: build-image

build-image:
	docker build -f Dockerfile -t $(REPO):$(VERSION) .
