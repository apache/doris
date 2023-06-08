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

## Usage

1. Currently, Doris only supports simple field types.
2. Doris only supports Hive Metastore Catalogs currently. The usage is basically the same as that of Hive Catalogs. More types of Catalogs will be supported in future versions.

## Create Catalog

### Create Catalog Based on Paimon API

Use the Paimon API to access metadata.Currently, only support Hive service as Paimon's Catalog.

- Hive Metastore

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

## Column Type Mapping

Same as that in Hive Catalogs. See the relevant section in [Hive](./hive.md).
