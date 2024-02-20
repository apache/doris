---
{
    "title": "SQL Dialect",
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

# SQL Dialect

Starting from version 2.1, Doris can support multiple SQL dialects, such as Presto, Trino, Hive, PostgreSQL, Spark, Oracle, Clickhouse, and more. Through this feature, users can directly use the corresponding SQL dialect to query data in Doris, which facilitates users to smoothly migrate their original business to Doris.

> 1. This function is currently an experimental function. If you encounter any problems during use, you are welcome to provide feedback through the mail group, [Github issue](https://github.com/apache/doris/issues), etc. .
>
> 2. This function only supports query statements and does not support other DDL and DML statements including Explain.

## Deploy service

1. Download latest [SQL Transform Tool](https://doris-build-1308700295.cos.ap-beijing.myqcloud.com/transform-doris-tool/transform-doris-tool-1.0.0-bin-x86).
2. On any FE node, start the service through the following command:

	`nohup ./transform-doris-tool-1.0.0-bin-x86 run --host=0.0.0.0 --port=5001 &`
	
	> 1. This service is a stateless service and can be started and stopped at any time.
	>
	> 2. `5001` is the service port and can be arbitrarily specified as an available port.
	>
	> 3. It is recommended to start a separate service on each FE node.

3. Start the Doris cluster (version 2.1 or higher)
4. Set the URL of the SQL Dialect Conversion Service with the following command in Doris:

	`MySQL> set global sql_converter_service_url = "http://127.0.0.1:5001/api/v1/convert"`
	
	> 1. `127.0.0.1:5001` is the deployment node IP and port of the SQL dialect conversion service.
	
## Use SQL dialect

Here we take the Presto/Trino SQL dialect as an example:

1. `set sql_dialect = "presto";`
2. Execute any Presto/Trino SQL syntax for data query.

Currently supported dialect types include:

- `presto`
- `trino`
- `hive`
- `spark`
- `postgres`
- `clickhouse`
- `oracle`

