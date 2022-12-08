---
{
    "title": "CREATE-CATALOG",
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

## CREATE-CATALOG

### Name

CREATE CATALOG

### Description

This statement is used to create an external catalog

Syntax:

```sql
CREATE CATALOG [IF NOT EXISTS] catalog_name [WITH RESOURCE resource_name];
```

`RESOURCE` can be created from [CREATE RESOURCE](../../../sql-reference/Data-Definition-Statements/Create/CREATE-RESOURCE.md), current supports：

* hms：Hive MetaStore
* es：Elasticsearch
* jdbc：数据库访问的标准接口(JDBC), 当前只支持`jdbc:mysql`

### Example

1. Create catalog hive

	```sql
	CREATE RESOURCE hms_resource PROPERTIES (
		'type'='hms',
		'hive.metastore.uris' = 'thrift://172.21.0.44:7004',
		'dfs.nameservices'='HDFS8000871',
		'dfs.ha.namenodes.HDFS8000871'='nn1,nn2',
		'dfs.namenode.rpc-address.HDFS8000871.nn1'='172.21.0.32:4007',
		'dfs.namenode.rpc-address.HDFS8000871.nn2'='172.21.0.44:4007',
		'dfs.client.failover.proxy.provider.HDFS8000871'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
	);

	CREATE CATALOG hive WITH RESOURCE hms_resource;
	```

2. Create catalog es

	```sql
	CREATE RESOURCE es_resource PROPERTIES (
		"type"="es",
		"hosts"="http://127.0.0.1:9200"
	);

	CREATE CATALOG es WITH RESOURCE es_resource;
	```

3. Create catalog jdbc

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

