---
{
    "title": "CREATE DATABASE",
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

# CREATE DATABASE

## Description

    This statement is used to create a new database  
    Syntax:  
        CREATE DATABASE [IF NOT EXISTS] db_name  
        [PROPERTIES ("key"="value", ...)] ;

1. PROPERTIES
    Additional information of a database, can be defaulted.
    1) In case of iceberg, the following information needs to be provided in the properties.
    ```
        PROPERTIES (
            "iceberg.database" = "iceberg_db_name",
            "iceberg.hive.metastore.uris" = "thrift://127.0.0.1:9083",
            "iceberg.catalog.type" = "HIVE_CATALOG"
            )

    ```
    `iceberg.database` is the name of the database corresponding to Iceberg.  
    `iceberg.hive.metastore.uris` is the address of the hive metastore service.  
    `iceberg.catalog.type` defaults to `HIVE_CATALOG`. Currently, only `HIVE_CATALOG` is supported, more Iceberg catalog types will be supported later.

## example
   1. Create a new database db_test
        ```
        CREATE DATABASE db_test;
        ```

   2. Create a new Iceberg database iceberg_test
        ```
        CREATE DATABASE `iceberg_test`
        PROPERTIES (
        "iceberg.database" = "doris",
        "iceberg.hive.metastore.uris" = "thrift://127.0.0.1:9083",
        "iceberg.catalog.type" = "HIVE_CATALOG"
        );
        ```

## keyword
CREATE,DATABASE

