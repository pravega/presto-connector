#!/bin/bash

if [ -z "$HOST_IP" ]; then
	echo "please set HOST_IP env variable"
	exit 1
fi

# build the sample code
echo "building sample code"
cd ./pravega-demo
./gradlew build
cd ..

java -cp pravega-demo/build/demo/pravega-demo-0.1.0.jar:pravega-demo/build/demo/* io.pravega.demo.Demo -controller $HOST_IP:9090 -sr $HOST_IP:9092 -scope demo