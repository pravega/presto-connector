# Pravega Connector

## Introduction
This getting started guide will show how you can query events from Pravega

## Steps


### Set HOST_IP
This IP will be used for the pravega stack
```
export HOST_IP=192.168.49.1
```

### Build Image w/ Pravega Plugin
You must first build a presto image that contains the pravega connector plugin.
This script will download a pravega plugin release, and add it to a base presto image.
It will also add the sample schema file(s).

```
sh build-image.sh
```

### Start Services
This includes pravaga, schema registry, and prestodb
```
docker-compose up -d
```

### Running sample/Loading demo data
A sample client is included (source in ./pravega-demo).  It will ingest some examples into the 'demo' pravega scope.  To build + run:
```
sh demo.sh
```

### presto-cli
```
$ docker exec -it `docker ps | grep presto-with-pravega | awk '{print $1}'` presto-cli
presto> 
```

### Exec Queries
```
presto> show catalogs;
 Catalog 
--------- 
 ...  
 pravega 
 ...
```

```
presto> show schemas from pravega;
       Schema       
--------------------
 demo               
 information_schema 
(2 rows)
```

```
presto:demo> show tables;
    Table     
--------------
 inventory    
 sensor       
 transactions 
(3 rows)
```

```
presto:demo> describe sensor;
   Column    |  Type   | Extra | Comment 
-------------+---------+-------+---------
 name        | varchar |       |         
 measurement | bigint  |       |         
 timestamp   | bigint  |       |         
(3 rows)
```

```
presto:demo> select * from sensor where name = 'sensor0' order by timestamp limit 5;
  name   |     measurement      |   timestamp   
---------+----------------------+---------------
 sensor0 | -2394620136288866641 | 1632939825916 
 sensor0 |  6214524158102000915 | 1632939825987 
 sensor0 |  -991013728383871819 | 1632939825988 
 sensor0 | -5923582664463696430 | 1632939825989 
 sensor0 |  4593297834576035790 | 1632939825990 
(5 rows)
```


### Stop Services
```docker-compose down```