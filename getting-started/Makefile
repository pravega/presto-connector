REPO=pravega-getting-started/pravega-getting-started
VERSION=latest

.PHONY: all build-image

all: build-image

build-prestodb-image:
	docker build -f Dockerfile-prestodb -t $(REPO):$(VERSION) .


build-trino-image:
	docker build -f Dockerfile-trino -t $(REPO):$(VERSION) .