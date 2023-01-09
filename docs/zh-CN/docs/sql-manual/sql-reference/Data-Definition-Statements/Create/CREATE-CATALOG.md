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
CREATE CATALOG [IF NOT EXISTS] catalog_name
	[WITH RESOURCE resource_name]
	[PROPERTIES ("key"="value", ...)];
```

`RESOURCE` 可以通过 [CREATE RESOURCE](../../../sql-reference/Data-Definition-Statements/Create/CREATE-RESOURCE.md) 创建，目前支持三种 Resource，分别连接三种外部数据源：

* hms：Hive MetaStore
* es：Elasticsearch
* jdbc：数据库访问的标准接口(JDBC), 当前支持 MySQL 和 PostgreSQL

### 创建 catalog

**通过 resource 创建 catalog**

`1.2.0` 以后的版本推荐通过 resource 创建 catalog，多个使用场景可以复用相同的 resource。
```sql
CREATE RESOURCE catalog_resource PROPERTIES (
    'type'='hms|es|jdbc',
    ...
);

// 在 PROERPTIES 中指定的配置，将会覆盖 Resource 中的配置。
CREATE CATALOG catalog_name WITH RESOURCE catalog_resource PROPERTIES(
    'key' = 'value'
)
```

**通过 properties 创建 catalog**

`1.2.0` 版本通过 properties 创建 catalog。
```sql
CREATE CATALOG catalog_name PROPERTIES (
    'type'='hms|es|jdbc',
    ...
);
```

### Example

1. 新建数据目录 hive

	```sql
	-- 1.2.0+ 版本
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

	-- 1.2.0 版本
	CREATE CATALOG hive PROPERTIES (
		'type'='hms',
		'hive.metastore.uris' = 'thrift://127.0.0.1:7004',
		'dfs.nameservices'='HANN',
		'dfs.ha.namenodes.HANN'='nn1,nn2',
		'dfs.namenode.rpc-address.HANN.nn1'='nn1_host:rpc_port',
		'dfs.namenode.rpc-address.HANN.nn2'='nn2_host:rpc_port',
		'dfs.client.failover.proxy.provider.HANN'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
	);
	```

2. 新建数据目录 es

	```sql
	-- 1.2.0+ 版本
	CREATE RESOURCE es_resource PROPERTIES (
		"type"="es",
		"hosts"="http://127.0.0.1:9200"
	);
	CREATE CATALOG es WITH RESOURCE es_resource;

	-- 1.2.0 版本
	CREATE CATALOG es PROPERTIES (
		"type"="es",
		"hosts"="http://127.0.0.1:9200"
	);
	```

3. 新建数据目录 jdbc
	**mysql**

	```sql
	-- 1.2.0+ 版本
	CREATE RESOURCE mysql_resource PROPERTIES (
		"type"="jdbc",
		"user"="root",
		"password"="123456",
		"jdbc_url" = "jdbc:mysql://127.0.0.1:3316/doris_test?useSSL=false",
		"driver_url" = "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/mysql-connector-java-8.0.25.jar",
		"driver_class" = "com.mysql.cj.jdbc.Driver"
	);
	CREATE CATALOG jdbc WITH RESOURCE msyql_resource;

	-- 1.2.0 版本
	CREATE CATALOG jdbc PROPERTIES (
		"type"="jdbc",
		"jdbc.user"="root",
		"jdbc.password"="123456",
		"jdbc.jdbc_url" = "jdbc:mysql://127.0.0.1:3316/doris_test?useSSL=false",
		"jdbc.driver_url" = "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/mysql-connector-java-8.0.25.jar",
		"jdbc.driver_class" = "com.mysql.cj.jdbc.Driver"
	);
	```

	**postgresql**

	```sql
	-- 1.2.0+ 版本
	CREATE RESOURCE pg_resource PROPERTIES (
		"type"="jdbc",
		"user"="postgres",
		"password"="123456",
		"jdbc_url" = "jdbc:postgresql://127.0.0.1:5432/demo",
		"driver_url" = "file:/path/to/postgresql-42.5.1.jar",
		"driver_class" = "org.postgresql.Driver"
	);
	CREATE CATALOG jdbc WITH RESOURCE pg_resource;

	-- 1.2.0 版本
	CREATE CATALOG jdbc PROPERTIES (
		"type"="jdbc",
		"jdbc.user"="postgres",
		"jdbc.password"="123456",
		"jdbc.jdbc_url" = "jdbc:postgresql://127.0.0.1:5432/demo",
		"jdbc.driver_url" = "file:/path/to/postgresql-42.5.1.jar",
		"jdbc.driver_class" = "org.postgresql.Driver"
	);
	```	

### Keywords

CREATE, CATALOG

### Best Practice

