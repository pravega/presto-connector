#!/bin/bash


echo $HOST_IP

if [ -z "$HOST_IP" ]; then
	echo "please set HOST_IP env variable"
	exit 1
fi

echo "set pravega and schema-registry configuration"
echo "connector.name=pravega
pravega.controller=tcp://$HOST_IP:9090
pravega.schema-registry=http://$HOST_IP:9092
" > etc/pravega.properties

# our plugin is not built in.  download release
echo "download pravega plugin"
curl --silent -O http://192.168.49.1:8000/pravega-presto-connector-0.1.0.tar.gz 2>/dev/null && \
	tar -xzf pravega-presto-connector-0.1.0.tar.gz && \
	mv pravega-presto-connector-0.1.0 pravega-plugin

# and add it to existing prestodb image
echo "build docker image"
make build-image

# cleanup
rm -rf ./pravega-plugin ./pravega-presto-connector-0.1.0.tar.gz
