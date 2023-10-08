---
{
"title": "Paimon",
"language": "en"
}
---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->


# Paimon

<version since="dev">
</version>

## Instructions for use

1. When data in hdfs,need to put core-site.xml, hdfs-site.xml and hive-site.xml in the conf directory of FE and BE. First read the hadoop configuration file in the conf directory, and then read the related to the environment variable `HADOOP_CONF_DIR` configuration file.
2. The currently adapted version of the payment is 0.4.0

## Create Catalog

Paimon Catalog Currently supports two types of Metastore creation catalogs:
* filesystem(default),Store both metadata and data in the file system.
* hive metastore,It also stores metadata in Hive metastore. Users can access these tables directly from Hive.

### Creating a Catalog Based on FileSystem

> For versions 2.0.1 and earlier, please use the following `Create Catalog based on Hive Metastore`.

#### HDFS
```sql
CREATE CATALOG `paimon_hdfs` PROPERTIES (
    "type" = "paimon",
    "warehouse" = "hdfs://HDFS8000871/user/paimon",
    "dfs.nameservices" = "HDFS8000871",
    "dfs.ha.namenodes.HDFS8000871" = "nn1,nn2",
    "dfs.namenode.rpc-address.HDFS8000871.nn1" = "172.21.0.1:4007",
    "dfs.namenode.rpc-address.HDFS8000871.nn2" = "172.21.0.2:4007",
    "dfs.client.failover.proxy.provider.HDFS8000871" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
    "hadoop.username" = "hadoop"
);

```

#### S3

> Note that.
>
> user need download [paimon-s3-0.4.0-incubating.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/0.4.0-incubating/paimon-s3-0.4.0-incubating.jar)
>
> Place it in directory ${DORIS_HOME}/be/lib/java_extensions/preload-extensions and restart be
>
> Starting from version 2.0.2, this file can be placed in BE's `custom_lib/` directory (if it does not exist, just create it manually) to prevent the file from being lost due to the replacement of the lib directory when upgrading the cluster.

```sql
CREATE CATALOG `paimon_s3` PROPERTIES (
    "type" = "paimon",
    "warehouse" = "s3://paimon-1308700295.cos.ap-beijing.myqcloud.com/paimoncos",
    "s3.endpoint" = "cos.ap-beijing.myqcloud.com",
    "s3.access_key" = "ak",
    "s3.secret_key" = "sk"
);

```

#### OSS

>Note that.
>
> user need download [paimon-oss-0.4.0-incubating.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-oss/0.4.0-incubating/paimon-oss-0.4.0-incubating.jar)
> Place it in directory ${DORIS_HOME}/be/lib/java_extensions/preload-extensions and restart be


```sql
CREATE CATALOG `paimon_oss` PROPERTIES (
    "type" = "paimon",
    "warehouse" = "oss://paimon-zd/paimonoss",
    "oss.endpoint" = "oss-cn-beijing.aliyuncs.com",
    "oss.access_key" = "ak",
    "oss.secret_key" = "sk"
);

```

### Creating a Catalog Based on Hive Metastore

```sql
CREATE CATALOG `paimon_hms` PROPERTIES (
    "type" = "paimon",
    "paimon.catalog.type" = "hms",
    "warehouse" = "hdfs://HDFS8000871/user/zhangdong/paimon2",
    "hive.metastore.uris" = "thrift://172.21.0.44:7004",
    "dfs.nameservices" = "HDFS8000871",
    "dfs.ha.namenodes.HDFS8000871" = "nn1,nn2",
    "dfs.namenode.rpc-address.HDFS8000871.nn1" = "172.21.0.1:4007",
    "dfs.namenode.rpc-address.HDFS8000871.nn2" = "172.21.0.2:4007",
    "dfs.client.failover.proxy.provider.HDFS8000871" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
    "hadoop.username" = "hadoop"
);

```

## Column Type Mapping

Same as that in Hive Catalogs. See the relevant section in [Hive](./hive.md).
