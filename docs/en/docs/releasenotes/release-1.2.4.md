---
{
    "title": "Release 1.2.4.1",
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


# Behavior Changed

- For `DateV2`/`DatetimeV2` and `DecimalV3` type, in the results of `DESCRIBLE` and `SHOW CREATE TABLE` statements, they will no longer be displayed as `DateV2`/`DatetimeV2` or `DecimalV3`, but directly displayed as `Date`/`Datetime` or `Decimal`.

	- This change is for compatibility with some BI tools. If you want to see the actual type of the column, you can check it with the `DESCRIBE ALL` statement.

- When querying tables in the `information_schema` database, the meta information(database, table, column, etc.) in the external catalog is no longer returned by default.

	- This change avoids the problem that the `information_schema` database cannot be queried due to the connection problem of some external catalog, so as to solve the problem of using some BI tools with Doris. It can be controlled by the FE configuration  `infodb_support_ext_catalog`, and the default value is `false`, that is, the meta information of external catalog will not be returned.

# Improvement

### JDBC Catalog

- Supports connecting to Trino/Presto via JDBC Catalog

​        Refer to: [https://doris.apache.org/docs/dev/lakehouse/multi-catalog/jdbc#trino](https://doris.apache.org/docs/dev/lakehouse/multi-catalog/jdbc#trino)

- JDBC Catalog connects to Clickhouse data source and supports Array type mapping

​        Refer to: [https://doris.apache.org/docs/dev/lakehouse/multi-catalog/jdbc#clickhouse](https://doris.apache.org/docs/dev/lakehouse/multi-catalog/jdbc#clickhouse)

### Spark Load 

- Spark Load supports Resource Manager HA related configuration

​        Refer to: https://github.com/apache/doris/pull/15000

## Bug Fixes

- Fixed several connectivity issues with Hive Catalog.

- Fixed ClassNotFound issues with Hudi Catalog.

- Optimize the connection pool of JDBC Catalog to avoid too many connections.

- Fix the problem that OOM will occur when importing data from another Doris cluster through JDBC Catalog.

- Fixed serveral queries and imports planning issues.

- Fixed several issues with Unique Key Merge-On-Write data model.

- Fix several BDBJE issues and solve the problem of abnormal FE metadata in some cases.

- Fix the problem that the `CREATE VIEW` statement does not support Table Valued Function.

- Fixed several memory statistics issues.

- Fixed several issues reading Parquet/ORC format.

- Fixed several issues with DecimalV3.

- Fixed several issues with SHOW QUERY/LOAD PROFILE.

