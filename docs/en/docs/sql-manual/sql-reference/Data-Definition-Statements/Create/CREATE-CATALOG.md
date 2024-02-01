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
CREATE CATALOG [IF NOT EXISTS] catalog_name [comment]
	PROPERTIES ("key"="value", ...);
```

### Example

1. Create catalog hive

	```sql
	CREATE CATALOG hive comment 'hive catalog' PROPERTIES (
		'type'='hms',
		'hive.metastore.uris' = 'thrift://127.0.0.1:7004',
		'dfs.nameservices'='HANN',
		'dfs.ha.namenodes.HANN'='nn1,nn2',
		'dfs.namenode.rpc-address.HANN.nn1'='nn1_host:rpc_port',
		'dfs.namenode.rpc-address.HANN.nn2'='nn2_host:rpc_port',
		'dfs.client.failover.proxy.provider.HANN'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
	);
	```

2. Create catalog es

	```sql
	CREATE CATALOG es PROPERTIES (
		"type"="es",
		"hosts"="http://127.0.0.1:9200"
	);
	```

3. Create catalog jdbc

	**mysql**

	```sql
	CREATE CATALOG jdbc PROPERTIES (
		"type"="jdbc",
		"user"="root",
		"password"="123456",
		"jdbc_url" = "jdbc:mysql://127.0.0.1:3316/doris_test?useSSL=false",
		"driver_url" = "https://doris-community-test-1308700295.cos.ap-hongkong.myqcloud.com/jdbc_driver/mysql-connector-java-8.0.25.jar",
		"driver_class" = "com.mysql.cj.jdbc.Driver"
	);
	```

	**postgresql**

	```sql
	CREATE CATALOG jdbc PROPERTIES (
		"type"="jdbc",
		"user"="postgres",
		"password"="123456",
		"jdbc_url" = "jdbc:postgresql://127.0.0.1:5432/demo",
		"driver_url" = "file:///path/to/postgresql-42.5.1.jar",
		"driver_class" = "org.postgresql.Driver"
	);
	```

	**clickhouse**

	```sql
	CREATE CATALOG jdbc PROPERTIES (
		"type"="jdbc",
		"user"="default",
		"password"="123456",
		"jdbc_url" = "jdbc:clickhouse://127.0.0.1:8123/demo",
		"driver_url" = "file:///path/to/clickhouse-jdbc-0.3.2-patch11-all.jar",
		"driver_class" = "com.clickhouse.jdbc.ClickHouseDriver"
	)
	```

	**oracle**
	```sql
	CREATE CATALOG jdbc PROPERTIES (
		"type"="jdbc",
		"user"="doris",
		"password"="123456",
		"jdbc_url" = "jdbc:oracle:thin:@127.0.0.1:1521:helowin",
		"driver_url" = "file:///path/to/ojdbc8.jar",
		"driver_class" = "oracle.jdbc.driver.OracleDriver"
	);	
	```

	**SQLServer**
	```sql
	CREATE CATALOG sqlserver_catalog PROPERTIES (
		"type"="jdbc",
		"user"="SA",
		"password"="Doris123456",
		"jdbc_url" = "jdbc:sqlserver://localhost:1433;DataBaseName=doris_test",
		"driver_url" = "file:///path/to/mssql-jdbc-11.2.3.jre8.jar",
		"driver_class" = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
	);	
	```

    **SAP HANA**
    ```sql
	CREATE CATALOG saphana_catalog PROPERTIES (
       "type"="jdbc",
       "user"="SYSTEM",
       "password"="SAPHANA",
       "jdbc_url" = "jdbc:sap://localhost:31515/TEST",
       "driver_url" = "file:///path/to/ngdbc.jar",
       "driver_class" = "com.sap.db.jdbc.Driver"
	);
    ```

    **Trino**
    ```sql
	CREATE CATALOG trino_catalog PROPERTIES (
       "type"="jdbc",
       "user"="hadoop",
       "password"="",
       "jdbc_url" = "jdbc:trino://localhost:8080/hive",
       "driver_url" = "file:///path/to/trino-jdbc-389.jar",
       "driver_class" = "io.trino.jdbc.TrinoDriver"
	);
    ```

    **OceanBase**
    ```sql
	CREATE CATALOG oceanbase_catalog PROPERTIES (
       "type"="jdbc",
       "user"="root",
       "password"="",
       "jdbc_url" = "jdbc:oceanbase://localhost:2881/demo",
       "driver_url" = "file:///path/to/oceanbase-client-2.4.2.jar",
       "driver_class" = "com.oceanbase.jdbc.Driver"
	);
    ```

### Keywords

CREATE, CATALOG

### Best Practice

