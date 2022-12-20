---
{
    "title": "Doris Hudi external table",
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

# Hudi External Table of Doris

<version deprecated="1.2.0" comment="Please use the Multi-Catalog function to access Hudi">

Hudi External Table of Doris provides Doris with the ability to access hdui external tables directly, eliminating the need for cumbersome data import and leveraging Doris' own OLAP capabilities to solve hudi table data analysis problems.

 1. support hudi data sources for Doris
 2. Support joint query between Doris and hdui data source tables to perform more complex analysis operations

This document introduces how to use this feature and the considerations.

</version>

## Glossary

### Noun in Doris

* FE: Frontend, the front-end node of Doris, responsible for metadata management and request access
* BE: Backend, the backend node of Doris, responsible for query execution and data storage

## How to use

### Create Hudi External Table 

Hudi tables can be created in Doris with or without schema. You do not need to declare the column definitions of the table when creating an external table, Doris can resolve the column definitions of the table in hive metastore when querying the table.

1. Create a separate external table to mount the Hudi table.  
   The syntax can be viewed in `HELP CREATE TABLE`.

    ```sql
    -- Syntax
    CREATE [EXTERNAL] TABLE table_name
    [(column_definition1[, column_definition2, ...])]
    ENGINE = HUDI
    [COMMENT "comment"]
    PROPERTIES (
    "hudi.database" = "hudi_db_in_hive_metastore",
    "hudi.table" = "hudi_table_in_hive_metastore",
    "hudi.hive.metastore.uris" = "thrift://127.0.0.1:9083"
    );


    -- Example: Mount hudi_table_in_hive_metastore under hudi_db_in_hive_metastore in Hive MetaStore 
    CREATE TABLE `t_hudi` 
    ENGINE = HUDI
    PROPERTIES (
    "hudi.database" = "hudi_db_in_hive_metastore",
    "hudi.table" = "hudi_table_in_hive_metastore",
    "hudi.hive.metastore.uris" = "thrift://127.0.0.1:9083"
    );
    
    -- Example：Mount hudi table with schema.
    CREATE TABLE `t_hudi` (
        `id` int NOT NULL COMMENT "id number",
        `name` varchar(10) NOT NULL COMMENT "user name"
    ) ENGINE = HUDI
    PROPERTIES (
    "hudi.database" = "hudi_db_in_hive_metastore",
    "hudi.table" = "hudi_table_in_hive_metastore",
    "hudi.hive.metastore.uris" = "thrift://127.0.0.1:9083"
    );
    ```


#### Parameter Description
- column_definition
  -  When create hudi table without schema(recommended), doris will resolve columns from hive metastore when query.
  -  When create hudi table with schema, the columns must exist in corresponding table in hive metastore.
- ENGINE needs to be specified as HUDI
- PROPERTIES property.
    - `hudi.hive.metastore.uris`: Hive Metastore service address
    - `hudi.database`: the name of the database to which Hudi is mounted
    - `hudi.table`: the name of the table to which Hudi is mounted, not required when mounting Hudi database.

### Show table structure

Show table structure can be viewed by `HELP SHOW CREATE TABLE`.
    


## Data Type Matching

The supported Hudi column types correspond to Doris in the following table.

|  Hudi  | Doris  |             Description              |
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
- The current default supported version of hudi is 0.10.0 and has not been tested in other versions. More versions will be supported in the future.


### Query Usage

Once you have finished building the hdui external table in Doris, it is no different from a normal Doris OLAP table except that you cannot use the data models in Doris (rollup, preaggregation, materialized views, etc.)

```sql
select * from t_hudi where k1 > 1000 and k3 = 'term' or k4 like '%doris';
```

