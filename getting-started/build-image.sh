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
" > etc/catalog/pravega.properties

# our plugin is not built in.  download release
echo "download pravega plugin"

pkg=`curl https://github.com/pravega/presto-connector/packages/1017266 2>/dev/null | \
	 grep -e github-registry.*prestodb-connector-0.1.0.tar.gz\& | \
	 awk -F '"' '{print $2'} | \
	 sed -e 's/\&amp;/\&/g'`
echo $pkg

curl "$pkg" 2>/dev/null --output pravega-plugin.tar.gz && \
	tar -xzf pravega-plugin.tar.gz && \
	mv pravega-presto-connector-0.1.0 pravega-plugin

# and add it to existing prestodb image
echo "build docker image"
make build-image

# cleanup
rm -rf ./pravega-plugin ./pravega-plugin.tar.gz
