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

## 使用限制

1. 目前只支持简单字段类型。
2. 目前仅支持 Hive Metastore 类型的 Catalog。所以使用方式和 Hive Catalog 基本一致。后续版本将支持其他类型的 Catalog。

## 创建 Catalog

### 基于Paimon API创建Catalog

使用Paimon API访问元数据的方式，目前只支持Hive服务作为Paimon的Catalog。

- Hive Metastore 作为元数据服务

```sql
CREATE CATALOG `paimon` PROPERTIES (
    "type" = "paimon",
    "hive.metastore.uris" = "thrift://172.16.65.15:7004",
    "dfs.ha.namenodes.HDFS1006531" = "nn2,nn1",
    "dfs.namenode.rpc-address.HDFS1006531.nn2" = "172.16.65.115:4007",
    "dfs.namenode.rpc-address.HDFS1006531.nn1" = "172.16.65.15:4007",
    "dfs.nameservices" = "HDFS1006531",
    "hadoop.username" = "hadoop",
    "warehouse" = "hdfs://HDFS1006531/data/paimon",
    "dfs.client.failover.proxy.provider.HDFS1006531" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
);
```

## 列类型映射

和 Hive Catalog 基本一致，可参阅 [Hive Catalog](./hive.md) 中 **列类型映射** 一节。

