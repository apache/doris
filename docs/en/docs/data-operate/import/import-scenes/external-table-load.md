---
{
    "title": "Synchronize Data Through External Table",
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

# Synchronize data through external table

Doris can create external tables. Once created, you can query the data of the external table directly with the SELECT statement or import the data of the external table with the `INSERT INTO SELECT` method.

Doris external tables currently support the following data sources.

- MySQL
- Oracle
- PostgreSQL
- SQLServer
- Hive 
- Iceberg
- ElasticSearch

This document describes how to create external tables accessible through the ODBC protocol and how to import data from these external tables.

## Create external table

For a detailed introduction to creating ODBC external tables, please refer to the [CREATE ODBC TABLE](../../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-EXTERNAL-TABLE.md) syntax help manual.

Here is just an example of how to use it.

1. Create an ODBC Resource

   The purpose of ODBC Resource is to manage the connection information of external tables uniformly.

   ```sql
   CREATE EXTERNAL RESOURCE `oracle_test_odbc`
   PROPERTIES (
       "type" = "odbc_catalog",
       "host" = "192.168.0.1",
       "port" = "8086",
       "user" = "oracle",
       "password" = "oracle",
       "database" = "oracle",
       "odbc_type" = "oracle",
       "driver" = "Oracle"
   );
   ````

Here we have created a Resource named `oracle_test_odbc`, whose type is `odbc_catalog`, indicating that this is a Resource used to store ODBC information. `odbc_type` is `oracle`, indicating that this OBDC Resource is used to connect to the Oracle database. For other types of resources, see the [resource management](../../../advanced/resource.md) documentation for details.

2. Create an external table

```sql
CREATE EXTERNAL TABLE `ext_oracle_demo` (
  `k1` decimal(9, 3) NOT NULL COMMENT "",
  `k2` char(10) NOT NULL COMMENT "",
  `k3` datetime NOT NULL COMMENT "",
  `k5` varchar(20) NOT NULL COMMENT "",
  `k6` double NOT NULL COMMENT ""
) ENGINE=ODBC
COMMENT "ODBC"
PROPERTIES (
    "odbc_catalog_resource" = "oracle_test_odbc",
    "database" = "oracle",
    "table" = "baseall"
);
````

Here we create an `ext_oracle_demo` external table and reference the `oracle_test_odbc` Resource created earlier

## Import Data

1. Create the Doris table

   Here we create a Doris table with the same column information as the external table `ext_oracle_demo` created in the previous step:

   ```sql
   CREATE TABLE `doris_oralce_tbl` (
     `k1` decimal(9, 3) NOT NULL COMMENT "",
     `k2` char(10) NOT NULL COMMENT "",
     `k3` datetime NOT NULL COMMENT "",
     `k5` varchar(20) NOT NULL COMMENT "",
     `k6` double NOT NULL COMMENT ""
   )
   COMMENT "Doris Table"
   DISTRIBUTED BY HASH(k1) BUCKETS 2
   PROPERTIES (
       "replication_num" = "1"
   );
   ````

   For detailed instructions on creating Doris tables, see [CREATE-TABLE](../../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md) syntax help.

2. Import data (from `ext_oracle_demo` table to `doris_oralce_tbl` table)

   ```sql
   INSERT INTO doris_oralce_tbl SELECT k1,k2,k3 FROM ext_oracle_demo limit 200
   ````
   
   The INSERT command is a synchronous command, and a successful return indicates that the import was successful.

## Precautions

- It must be ensured that the external data source and the Doris cluster can communicate with each other, including the network between the BE node and the external data source.
- ODBC external tables essentially access the data source through a single ODBC client, so it is not suitable to import a large amount of data at one time. It is recommended to import multiple times in batches.
