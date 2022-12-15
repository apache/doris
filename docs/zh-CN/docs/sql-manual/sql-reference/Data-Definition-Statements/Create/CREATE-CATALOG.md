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

<version since="1.2">

CREATE CATALOG

</version>

### Description

该语句用于创建外部数据目录（catalog）

语法：

```sql
CREATE CATALOG [IF NOT EXISTS] catalog_name [WITH RESOURCE resource_name];
```

`RESOURCE` 可以通过 [CREATE RESOURCE](../../../sql-reference/Data-Definition-Statements/Create/CREATE-RESOURCE.md) 创建，目前支持三种 Resource，分别连接三种外部数据源：

* hms：Hive MetaStore
* es：Elasticsearch
* jdbc：数据库访问的标准接口(JDBC), 当前只支持`jdbc:mysql`

### Example

1. 新建数据目录 hive

	```sql
	CREATE RESOURCE hms_resource PROPERTIES (
		'type'='hms',
		'hive.metastore.uris' = 'thrift://127.0.0.1:7004',
		'dfs.nameservices'='HANN',
		'dfs.ha.namenodes.HANN'='nn1,nn2',
		'dfs.namenode.rpc-address.HANN.nn1'='nn1_host:rpc_port',
		'dfs.namenode.rpc-address.HANN.nn2'='nn2_host:rpc_port',
		'dfs.client.failover.proxy.provider.HANN'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
	);

	CREATE CATALOG hive WITH RESOURCE hms_resource;
	```

2. 新建数据目录 es

	```sql
	CREATE RESOURCE es_resource PROPERTIES (
		"type"="es",
		"hosts"="http://127.0.0.1:9200"
	);

	CREATE CATALOG es WITH RESOURCE es_resource;
	```

3. 新建数据目录 jdbc

	```sql
	CREATE RESOURCE mysql_resource PROPERTIES (
		"type"="jdbc",
		"user"="root",
		"password"="123456",
		"jdbc_url" = "jdbc:mysql://127.0.0.1:3316/doris_test?useSSL=false",
		"driver_url" = "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/mysql-connector-java-8.0.25.jar",
		"driver_class" = "com.mysql.cj.jdbc.Driver"
	);

	CREATE CATALOG jdbc WITH RESOURCE msyql_resource;
	```

### Keywords

CREATE, CATALOG

### Best Practice

