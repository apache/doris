---
{
    "title": "CREATE TABLE LIKE",
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

# CREATE TABLE LIKE

## description

Use CREATE TABLE ... LIKE to create an empty table based on the definition of another table, including any column attributes, table partitions and table properties defined in the original table:
Syntax:

```
    CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name LIKE [database.]table_name [WITH ROLLUP (r2,r2,r3,...)] 
```

Explain:
    1. The replicated table structures include Column Definition, Partitions, Table Properties, and so on
    2. The SELECT privilege is required on the original table.
    3. Support to copy external table such as MySQL.
    4. Support to copy OLAP table rollup

## Example
    1. Under the test1 Database, create an empty table with the same table structure as table1, named table2

        CREATE TABLE test1.table2 LIKE test1.table1
    
    2. Under the test2 Database, create an empty table with the same table structure as test1.table1, named table2

        CREATE TABLE test2.table2 LIKE test1.table1

    3. Under the test1 Database, create an empty table with the same table structure as table1, named table2. copy r1 and r2 rollup of table1 simultaneously

        CREATE TABLE test1.table2 LIKE test1.table1 WITH ROLLUP (r1,r2)

    4. Under the test1 Database, create an empty table with the same table structure as table1, named table2. copy all rollup of table1 simultaneously

        CREATE TABLE test1.table2 LIKE test1.table1 WITH ROLLUP

    5. Under the test2 Database, create an empty table with the same table structure as table1, named table2. copy r1 and r2 rollup of table1 simultaneously

        CREATE TABLE test2.table2 LIKE test1.table1 WITH ROLLUP (r1,r2)

    6. Under the test2 Database, create an empty table with the same table structure as table1, named table2. copy all rollup of table1 simultaneously

        CREATE TABLE test2.table2 LIKE test1.table1 WITH ROLLUP
    
    7. Under the test1 Database, create an empty table with the same table structure as MySQL's external table1, called table2

        CREATE TABLE test1.table2 LIKE test1.table1

## keyword

```
    CREATE,TABLE,LIKE

```
