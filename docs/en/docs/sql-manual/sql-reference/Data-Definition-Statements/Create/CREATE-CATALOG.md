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
CREATE CATALOG [IF NOT EXISTS] catalog_name
    [PROPERTIES ("key"="value", ...)];
```

`PROPERTIES` is the connection information for the catalog. The "type" attribute must be specified, currently supports:

* hms：Hive MetaStore
* es：Elasticsearch
* jdbc: Database access standard interface (JDBC), currently only support `jdbc:mysql`

### Example

1. Create catalog hive

   ```sql
   CREATE CATALOG hive PROPERTIES (
		"type"="hms",
		'hive.metastore.uris' = 'thrift://172.21.0.1:7004',
		'dfs.nameservices'='HDFS8000871',
		'dfs.ha.namenodes.HDFS8000871'='nn1,nn2',
		'dfs.namenode.rpc-address.HDFS8000871.nn1'='172.21.0.2:4007',
		'dfs.namenode.rpc-address.HDFS8000871.nn2'='172.21.0.3:4007',
		'dfs.client.failover.proxy.provider.HDFS8000871'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
	);
	```

2. Create catalog es

   ```sql
   CREATE CATALOG es PROPERTIES (
	   "type"="es",
	   "elasticsearch.hosts"="http://127.0.0.1:9200"
   );
   ```

3. Create catalog jdbc

   ```sql
   CREATE CATALOG jdbc PROPERTIES (
		"type"="jdbc",
		"jdbc.user"="root",
		"jdbc.password"="123456",
		"jdbc.jdbc_url" = "jdbc:mysql://127.0.0.1:13396/demo",
		"jdbc.driver_url" = "file:/mnt/disk2/ftw/tools/jar/mysql-connector-java-5.1.47/mysql-connector-java-5.1.47.jar",
		"jdbc.driver_class" = "com.mysql.jdbc.Driver"
	);
   ```

### Keywords

CREATE, CATALOG

### Best Practice

