---
{
    "title": "CREATE-CATALOG",
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

## CREATE-CATALOG

### Name

CREATE CATALOG

### Description

该语句用于创建外部数据目录（catalog）

语法：

```sql
CREATE CATALOG [IF NOT EXISTS] catalog_name
    [PROPERTIES ("key"="value", ...)];
```

`PROPERTIES` 为 catalog 的连接信息。其中 "type" 属性必须指定，目前支持：

* hms：Hive MetaStore
* es：Elasticsearch

### Example

1. 新建数据目录 hive

   ```sql
   CREATE CATALOG hive PROPERTIES (
		"type"="hms",
		'hive.metastore.uris' = 'thrift://172.21.0.1:7004',
		'dfs.nameservices'='service1',
		'dfs.ha.namenodes. service1'='nn1,nn2',
		'dfs.namenode.rpc-address.HDFS8000871.nn1'='172.21.0.2:4007',
		'dfs.namenode.rpc-address.HDFS8000871.nn2'='172.21.0.3:4007',
		'dfs.client.failover.proxy.provider.HDFS8000871'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
	);
	```

2. 新建数据目录 es

   ```sql
   ```

### Keywords

CREATE, CATALOG

### Best Practice

