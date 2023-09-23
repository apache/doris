---
{
"title": "Paimon",
"language": "zh-CN"
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

## 使用须知

1. 数据放在hdfs时，需要将 core-site.xml，hdfs-site.xml 和 hive-site.xml  放到 FE 和 BE 的 conf 目录下。优先读取 conf 目录下的 hadoop 配置文件，再读取环境变量 `HADOOP_CONF_DIR` 的相关配置文件。
2. 当前适配的paimon版本为0.4.0

## 创建 Catalog

Paimon Catalog 当前支持两种类型的Metastore创建Catalog:
* filesystem（默认），同时存储元数据和数据在filesystem。
* hive metastore，它还将元数据存储在Hive metastore中。用户可以直接从Hive访问这些表。

### 基于FileSystem创建Catalog

> 2.0.1 及之前版本，请使用后面的 `基于Hive Metastore创建Catalog`。

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

> 注意：
>
> 用户需要手动下载[paimon-s3-0.4.0-incubating.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/0.4.0-incubating/paimon-s3-0.4.0-incubating.jar)

> 放在${DORIS_HOME}/be/lib/java_extensions/preload-extensions目录下并重启be。
>
> 从 2.0.2 版本起，可以将这个文件放置在BE的 `custom_lib/` 目录下（如不存在，手动创建即可），以防止升级集群时因为 lib 目录被替换而导致文件丢失。

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

>注意：
>
> 用户需要手动下载[paimon-oss-0.4.0-incubating.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-oss/0.4.0-incubating/paimon-oss-0.4.0-incubating.jar)
> 放在${DORIS_HOME}/be/lib/java_extensions/preload-extensions目录下并重启be

```sql
CREATE CATALOG `paimon_oss` PROPERTIES (
    "type" = "paimon",
    "warehouse" = "oss://paimon-zd/paimonoss",
    "oss.endpoint" = "oss-cn-beijing.aliyuncs.com",
    "oss.access_key" = "ak",
    "oss.secret_key" = "sk"
);

```

### 基于Hive Metastore创建Catalog

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

## 列类型映射

和 Hive Catalog 基本一致，可参阅 [Hive Catalog](./hive.md) 中 **列类型映射** 一节。

