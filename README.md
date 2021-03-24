# presto-connector
Pravega Presto connector

Presto is a distributed SQL query engine for big data. Presto uses connectors to query storage from different storage sources. This repository contains the code for a connector (the Pravega Presto connector) to query storage from Pravega streams. To learn more about PrestoDB, visit https://prestodb.io

Pravega is an open source distributed storage service implementing Streams. It offers Stream as the main primitive for the foundation of reliable storage systems: a high-performance, durable, elastic, and unlimited append-only byte stream with strict ordering and consistency.  To learn more about Pravega, visit https://pravega.io

See the [User Manual](https://prestodb.github.io/docs/current/) for Presto deployment instructions and end user documentation.

## Requirements

To build and run the Pravega Presto connector, you must meet the following requirements:

* Linux 
* Java 8 Update 151 or higher (8u151+), 64-bit. Both Oracle JDK and OpenJDK are supported.
* Gradle 6.5.1+ (for building)
* Python 2.7+ (for running with the launcher script)
* Pravega version 0.9.0 or higher
* Pravega Schema Registry version 0.2.0 or higher

## Building Pravega Presto connector

Pravega Presto connector is a standard Gradle project. Simply run the following command from the project root directory:

    # [root@lrmk226 ~]# ./gradlew clean build

On the first build, Gradle will download all the dependencies from various locations of the internet and cache them in the local repository (`~/.gradle / caches `), which can take a considerable amount of time.  Subsequent builds will be faster.  

Pravega Presto connector has a set of unit tests that can take several minutes to run. You can run the tests using this command:

    # [root@lrmk226 ~]# ./gradlew test

## Installing Presto

If you haven't already done so, install the Presto server and default connectors on one or more Linux hosts. Instructions for downloading, installing and configuring Presto can be found here: https://prestodb.io/docs/current/installation/deployment.html.

When using the tar.gz Presto bundle downloaded from Maven Central, the Presto installation can be installed in any location. Determine a location with sufficient available storage space. Using the wget command, download the gzip'd tar file from Maven using the link defined in the PrestoDB deployment section, but similar to the steps below:
    
    # [root@lrmk226 ~]# pwd
    /root
    # [root@lrmk226 ~]# wget https://repo1.maven.org/maven2/com/facebook/presto/presto-server/0.248/presto-server-0.248.tar.gz
    # [root@lrmk226 ~]# tar xvzf presto-server-0.248.tar.gz
    # [root@lrmk226 ~]# export PRESTO_HOME=/root/presto-server-0.248
    
Make a directory for the Presto configuration files

    [root@lrmk226 ~]# mkdir $PRESTO_HOME/etc
    
Now follow the directions to create the neccesary configuration files for configuring Presto found in the PrestoDB documentation.

## Installing and Configuring Pravega Connector

The plugin file that gets created during the presto-connector build process is: ./build/distributions/pravega-<VERSION>.tar.gz.  This file can be untar'd in the $PRESTO_ROOT/plugin directory of a valid Presto installation. Like all Presto connectors, the Pravega Presto connector uses a properties files to point to the storage provider (e.g. Pravega controller).  Create a properties file similar to below, but replace the # characters with the appropriate IP address of the Pravega Controller and the Pravega Schema Registry server of your configuration.

    [root@lrmk226 ~]# cd $PRESTO_HOME/plugin
    [root@lrmk226 ~]# ls *.gz
    pravega-presto-connector-0.1.0.tar.gz
    [root@lrmk226 ~]# tar xvfz pravega-presto-connector-0.1.0.tar.gz
    [root@lrmk226 ~]# cat $PRESTO_HOME/etc/catalog/pravega.properties
    connector.name=pravega
    pravega.controller=tcp://##.###.###.###:9090
    pravega.schema-registry=http://##.###.###.###:9092

If you have deployed Presto on more than one host (coordinator and one or more workers), you must download/copy the Pravega connector gzip tar file to each node, and create the configuration properties file on all hosts.

## Running Presto Server

As mentioned in the PrestoDB documentation, use the launcher tool to start the Presto server on each node.

## Running Presto in your IDE

After building Presto for the first time, you can load the project into your IDE and run the server in your IDE. We recommend using [IntelliJ IDEA](http://www.jetbrains.com/idea/). Because Pravega Presto connectoris a standard Gradle project, you can import it into your IDE. In IntelliJ, choose Import Project from the Quick Start box and point it to the root of the source tree.  IntelliJ will identify the *.gradle files and prompt you to confirm.

After opening the project in IntelliJ, double check that the Java SDK is properly configured for the project:

* Open the File menu and select Project Structure
* In the SDKs section, ensure that a 1.8 JDK is selected (create one if none exist)
* In the Project section, ensure the Project language level is set to 8.0 as Presto makes use of several Java 8 language features

Use the following options to create a run configuration that runs the Presto server using the Pravega Presto connector:

* Main Class: 'com.facebook.presto.server.PrestoServer'
* VM Options: '-ea -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -Xmx2G -Dconfig=etc/config.properties -Dcom.sun.xml.bind.v2.bytecode.ClassTailor.noOptimize=true -Dlog.levels-file=etc/log.properties'
* Working directory: '$MODULE_DIR$'
* Use classpath of module: 'pravega.main'
* Add a 'Before Launch' task - Add a gradle task to run the 'jar' task for the 'presto-connector' Gradle project.

Modify the pravega.properties file in etc/catalog as previously described to point to a running Pravega controller, and a running Schema Registry.

## Schema Definitions

Currently, you must manually create schema definitions using a JSON file. In future releases, the 'CREATE TABLE' Presto command will be available.  The JSON configuration files are read at server startup, and should be located in etc/pravega directory.  In the JSON schema example below, "customer" is a stream name in the tpch pravega scope.

    {
        "tableName": "customer",
        "schemaName": "tpch",
        "objectName": "customer",
        "event": [{
            "dataFormat": "json",
            "fields": [
                {
                    "name": "custkey",
                    "mapping": "custkey",
                    "type": "BIGINT"
                },
                {
                    "name": "name",
                    "mapping": "name",
                    "type": "VARCHAR(25)"
                },
                {
                    "name": "address",
                    "mapping": "address",
                    "type": "VARCHAR(40)"
                },
                {
                    "name": "nationkey",
                    "mapping": "nationkey",
                    "type": "BIGINT"
                },
                {
                    "name": "phone",
                    "mapping": "phone",
                    "type": "VARCHAR(15)"
                },
                {
                    "name": "acctbal",
                    "mapping": "acctbal",
                    "type": "DOUBLE"
                },
                {
                    "name": "mktsegment",
                    "mapping": "mktsegment",
                    "type": "VARCHAR(10)"
                },
                {
                    "name": "comment",
                    "mapping": "comment",
                    "type": "VARCHAR(117)"
                }
            ]
        }]
    }

