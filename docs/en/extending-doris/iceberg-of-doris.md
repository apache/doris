---
{
    "title": "Iceberg of Doris",
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

# Iceberg External Table of Doris

Iceberg External Table of Doris provides Doris with the ability to access Iceberg external tables directly, eliminating the need for cumbersome data import and leveraging Doris' own OLAP capabilities to solve Iceberg table data analysis problems.

 1. support Iceberg data sources to access Doris
 2. Support joint query between Doris and Iceberg data source tables to perform more complex analysis operations

This document introduces how to use this feature and the considerations.

## Glossary

### Noun in Doris

* FE: Frontend, the front-end node of Doris, responsible for metadata management and request access
* BE: Backend, the backend node of Doris, responsible for query execution and data storage

## How to use

### Create Iceberg External Table 

Iceberg tables can be created in Doris in two ways. You do not need to declare the column definitions of the table when creating an external table, Doris can automatically convert them based on the column definitions of the table in Iceberg.

1. Create a separate external table to mount the Iceberg table.  
   The syntax can be viewed in `HELP CREATE TABLE`.

    ```sql
    -- Syntax
    CREATE [EXTERNAL] TABLE table_name 
    ENGINE = ICEBERG
    [COMMENT "comment"]
    PROPERTIES (
    "database" = "iceberg_db_name",
    "table" = "icberg_table_name",
    "hive.metastore.uris" = "thrift://192.168.0.1:9083",
    "catalog.type" = "HIVE_CATALOG"
    );


    -- Example: Mount iceberg_table under iceberg_db in Iceberg 
    CREATE TABLE `t_iceberg` 
    ENGINE = ICEBERG
    PROPERTIES (
    "database" = "iceberg_db",
    "table" = "iceberg_table",
    "hive.metastore.uris" = "thrift://192.168.0.1:9083",
    "catalog.type" = "HIVE_CATALOG"
    );
    ```

2. Create an Iceberg database to mount the corresponding Iceberg database on the remote side, and mount all the tables under the database.  
   You can check the syntax with `HELP CREATE DATABASE`.

    ```sql
    -- Syntax
    CREATE DATABASE db_name 
    ENGINE = ICEBERG
    [COMMENT "comment"]
    PROPERTIES (
    "database" = "iceberg_db_name",
    "hive.metastore.uris" = "thrift://192.168.0.1:9083",
    "catalog.type" = "HIVE_CATALOG"
    );

    -- Example: mount the iceberg_db in Iceberg and mount all tables under that db
    CREATE DATABASE `iceberg_test_db`
    ENGINE = ICEBERG
    PROPERTIES (
    "database" = "iceberg_db",
    "hive.metastore.uris" = "thrift://192.168.0.1:9083",
    "catalog.type" = "HIVE_CATALOG"
    );
    ```

    The progress of the table build in `iceberg_test_db` can be viewed by `HELP SHOW TABLE CREATION`.

#### Parameter Description

- ENGINE needs to be specified as ICEBERG
- PROPERTIES property.
    - `hive.metastore.uris`: Hive Metastore service address
    - `database`: the name of the database to which Iceberg is mounted
    - `table`: the name of the table to which Iceberg is mounted, not required when mounting Iceberg database.
    - `catalog.type`: the catalog method used in Iceberg, the default is `HIVE_CATALOG`, currently only this method is supported, more Iceberg catalog access methods will be supported in the future.

### Show table structure

Show table structure can be viewed by `HELP SHOW CREATE TABLE`.
    
## Data Type Matching

The supported Iceberg column types correspond to Doris in the following table.

|  Iceberg  | Doris  |             Description              |
| :------: | :----: | :-------------------------------: |
|   BOOLEAN  | BOOLEAN  |                         |
|   INTEGER   |  INT  |                       |
|   LONG | BIGINT |              |
|   FLOAT   | FLOAT |  |
|   DOUBLE  | DOUBLE |  |
|   DATE  | DATE |  |
|   TIMESTAMP   |  DATETIME  | Timestamp to Datetime with loss of precision |
|   STRING   |  STRING  |                                   |
|   UUID  | VARCHAR | Use VARCHAR instead | 
|   DECIMAL  | DECIMAL |  |
|   TIME  | - | not supported |
|   FIXED  | - | not supported |
|   BINARY  | - | not supported |
|   STRUCT  | - | not supported |
|   LIST  | - | not supported |
|   MAP  | - | not supported |

**Note:** 
- Iceberg table Schema changes **are not automatically synchronized** and require rebuilding the Iceberg external tables or database in Doris.
- The current default supported version of Iceberg is 0.12.0 and has not been tested in other versions. More versions will be supported in the future.

### Query Usage

Once you have finished building the Iceberg external table in Doris, it is no different from a normal Doris OLAP table except that you cannot use the data models in Doris (rollup, preaggregation, materialized views, etc.)

```sql
select * from t_icebe where k1 > 1000 and k3 = 'term' or k4 like '%doris';
```
