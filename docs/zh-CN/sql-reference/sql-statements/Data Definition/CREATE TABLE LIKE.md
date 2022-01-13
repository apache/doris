---
{
    "title": "CREATE TABLE LIKE",
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

# CREATE TABLE LIKE

## description

该语句用于创建一个表结构和另一张表完全相同的空表，同时也能够可选复制一些rollup。
语法：

```
    CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [database.]table_name LIKE [database.]table_name [WITH ROLLUP (r1,r2,r3,...)]
```

说明:
    1. 复制的表结构包括Column Definition、Partitions、Table Properties等
    2. 用户需要对复制的原表有`SELECT`权限
    3. 支持复制MySQL等外表
    4. 支持复制OLAP Table的rollup

## Example
    1. 在test1库下创建一张表结构和table1相同的空表，表名为table2

        CREATE TABLE test1.table2 LIKE test1.table1
    
    2. 在test2库下创建一张表结构和test1.table1相同的空表，表名为table2

        CREATE TABLE test2.table2 LIKE test1.table1

    3. 在test1库下创建一张表结构和table1相同的空表，表名为table2，同时复制table1的r1，r2两个rollup

        CREATE TABLE test1.table2 LIKE test1.table1 WITH ROLLUP (r1,r2)

    4. 在test1库下创建一张表结构和table1相同的空表，表名为table2，同时复制table1的所有rollup

        CREATE TABLE test1.table2 LIKE test1.table1 WITH ROLLUP

    5. 在test2库下创建一张表结构和test1.table1相同的空表，表名为table2，同时复制table1的r1，r2两个rollup

        CREATE TABLE test2.table2 LIKE test1.table1 WITH ROLLUP (r1,r2)

    6. 在test2库下创建一张表结构和test1.table1相同的空表，表名为table2，同时复制table1的所有rollup

        CREATE TABLE test2.table2 LIKE test1.table1 WITH ROLLUP

    7. 在test1库下创建一张表结构和MySQL外表table1相同的空表，表名为table2

        CREATE TABLE test1.table2 LIKE test1.table1

## keyword

```
    CREATE,TABLE,LIKE

```
