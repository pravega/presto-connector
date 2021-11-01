#!/bin/bash

# prestodb or trino
image="${PRODUCT:-trino}"

echo "build for $image, using HOST_IP: '$HOST_IP'"

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
echo "download pravega plugin for $image"

pkg=""
get_pkg() {
  if [ "$image" = "trino" ]; then
    page="1017267"
  else
    page="1017266"
  fi

  pkg=`curl "https://github.com/pravega/presto-connector/packages/$page" 2>/dev/null | \
     grep -e github-registry.*connector.*tar.gz\& | \
     awk -F '"' '{print $2'} | \
     sed -e 's/\&amp;/\&/g'`

  curl "$pkg" 2>/dev/null --output pravega-plugin.tar.gz && \
    tar -xzf pravega-plugin.tar.gz && \
    mv `find . -name *connector* -type d` pravega-plugin
}

get_pkg

if [ "$image" = "trino" ]; then
  make build-trino-image
else
  make build-prestodb-image
fi

# cleanup
rm -rf ./pravega-plugin ./pravega-plugin.tar.gz
