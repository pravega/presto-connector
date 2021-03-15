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

## Building Presto

Pravega Presto connector is a standard Gradle project. Simply run the following command from the project root directory:

    ./gradlew clean build

On the first build, Gradle will download all the dependencies from various locations of the internet and cache them in the local repository (`~/.gradle / caches `), which can take a considerable amount of time.  Subsequent builds will be faster.  

Pravega Presto connector has a set of unit tests that can take several minutes to run. You can run the tests using this command:

    ./gradlew test

## Installing Presto

Instructions for downloading, installing and configuring Presto can be found here: https://prestodb.io/docs/current/installation/deployment.html.

A typical Presto installation installs the Presto server in /usr/lib. You download (via wget) the gzip'd tar file from Maven using the link defined in the PrestoDB deployment section.  Untar the file while in /usr/lib, which will create a presto-server-<RELEASE> directory.  Create a symbolic link to the code as follows
    
    # [root@lrmk226 ~]# ln -s /usr/lib/presto-server-<RELEASE> /usr/lib/presto
    
Typically, the Presto configuration files are located in /etc/presto, so create this directory, then create another symbolic link in /usr/lib/presto to point to this directory

    [root@lrmk226 ~]# mkdir /etc/presto
    [root@lrmk226 ~]# ln -s /etc/presto /usr/lib/presto/etc
    
Now follow the directions for Configuring Presto found in the PrestoDB documentation.

## Installing and Configuring Pravega Connector

The plugin file that gets created during the build process is: ./build/distributions/pravega-<VERSION>.tar.gz.  This file can be untar'd in the /usr/lib/presto/lib/plugins directory of a running Presto installation. Like all Presto connectors, the Pravega Presto connector uses a properties files to point to the storage provider (e.g. Pravega controller).  Create a properties file similar to below, but replace the # characters with the appropriate IP address of the Pravega Controller and the Pravega Schema Registry server of your configuration.

    [root@lrmk226 ~]# cat /etc/presto/catalog/pravega.properties
    connector.name=pravega
    pravega.controller=tcp://##.###.###.###:9090
    pravega.schema-registry=http://##.###.###.###:9092

If you have deployed Presto on more than one host (coordinator and one or more workers), you must download/copy the Pravega connector to each node, and create the configuration properties file on all hosts.

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
* Working directory: '/root/presto'
* Use classpath of module: 'pravega.main'

The working directory should be manually created and set to where the configuration properties files are located on your host.

    [root@lrmk226 ~]# find /root/presto -ls
    537429121    0 drwxr-xr-x   4 root     root           28 Mar 10 10:40 /root/presto
    537429123    0 drwxr-xr-x   4 root     root           97 Mar  2 17:12 /root/presto/etc
    805328026    0 drwxr-xr-x   2 root     root           32 Mar  3 15:58 /root/presto/etc/catalog
    816318053    4 -rw-r--r--   1 root     root          119 Mar  3 15:58 /root/presto/etc/catalog/pravega.properties
    272367596    0 drwxr-xr-x   2 root     root            6 Mar  2 17:04 /root/presto/etc/ecs
    537429402    4 -rw-r--r--   1 root     root          854 Mar  2 17:10 /root/presto/etc/config.properties
    537429389    4 -rw-r--r--   1 root     root          351 Mar  2 17:11 /root/presto/etc/jvm.config
    537435775    4 -rw-r--r--   1 root     root          378 Mar  2 17:12 /root/presto/etc/log.properties
    575879    0 drwxr-xr-x   3 root     root           17 Mar  2 17:17 /root/presto/var
    268833640    0 drwxr-xr-x   2 root     root           30 Mar 10 10:42 /root/presto/var/log
    272373839   12 -rw-r--r--   1 root     root        11230 Mar 10 10:42 /root/presto/var/log/http-request.log

Create the pravega.properties file as previously described.

## Schema Definitions

Currently, you must manually create schema definitions using a JSON file. In future releases, the 'CREATE TABLE' Presto command will be available.  The JSON configuration files are read at server startup, and should be located in /etc/presto/pravega directory.  An example schema definition file might be:

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

