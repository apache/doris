---
{
    "title": "CREATE-DATABASE",
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

## CREATE-DATABASE

### Name

CREATE DATABASE

### Description

This statement is used to create a new database (database)

grammar:

```sql
CREATE DATABASE [IF NOT EXISTS] db_name
    [PROPERTIES ("key"="value", ...)];
````

`PROPERTIES` Additional information about the database, which can be defaulted.

- If you create an Iceberg database, you need to provide the following information in properties:

  ```sql
  PROPERTIES (
    "iceberg.database" = "iceberg_db_name",
    "iceberg.hive.metastore.uris" = "thrift://127.0.0.1:9083",
    "iceberg.catalog.type" = "HIVE_CATALOG"
  )
  ````

  illustrate:
  
  - `ceberg.database` : the library name corresponding to Iceberg;
  - `iceberg.hive.metastore.uris` : hive metastore service address;
  - `iceberg.catalog.type`: The default is `HIVE_CATALOG`; currently only `HIVE_CATALOG` is supported, and more Iceberg catalog types will be supported in the future.

- If you want to specify the default replica distribution for tables in db, you need to specify `replication_allocation` (the `replication_allocation` attribute of table will have higher priority than db)

  ```sql
  PROPERTIES (
    "replication_allocation" = "tag.location.default:3"
  )
  ```

### Example

1. Create a new database db_test

   ```sql
   CREATE DATABASE db_test;
   ````

2. Create a new Iceberg database iceberg_test

   ```sql
   CREATE DATABASE `iceberg_test`
   PROPERTIES (
   "iceberg.database" = "doris",
   "iceberg.hive.metastore.uris" = "thrift://127.0.0.1:9083",
   "iceberg.catalog.type" = "HIVE_CATALOG"
   );
   ````

### Keywords

````text
CREATE, DATABASE
````

### Best Practice

