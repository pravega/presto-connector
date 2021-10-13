FROM ahanaio/prestodb-sandbox:latest

RUN mkdir /opt/presto-server/plugin/pravega
COPY pravega-plugin/* /opt/presto-server/plugin/pravega/

COPY etc/catalog/pravega.properties /opt/presto-server/etc/catalog

RUN mkdir /opt/presto-server/etc/pravega
COPY etc/pravega/* /opt/presto-server/etc/pravega/
