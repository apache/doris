---
{
    "title": "DROP MATERIALIZED VIEW",
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

# DROP MATERIALIZED VIEW

## description
    该语句用于删除物化视图。同步语法

语法:

    ```
    DROP MATERIALIZED VIEW [IF EXISTS] mv_name ON table_name
    ```

1. IF EXISTS
	如果物化视图不存在，不要抛出错误。如果不声明此关键字，物化视图不存在则报错。
	
2. mv_name
	待删除的物化视图的名称。必填项。
	
3. table_name
	待删除的物化视图所属的表名。必填项。
	
## example

表结构为

```
mysql> desc all_type_table all;
+----------------+-------+----------+------+-------+---------+-------+
| IndexName      | Field | Type     | Null | Key   | Default | Extra |
+----------------+-------+----------+------+-------+---------+-------+
| all_type_table | k1    | TINYINT  | Yes  | true  | N/A     |       |
|                | k2    | SMALLINT | Yes  | false | N/A     | NONE  |
|                | k3    | INT      | Yes  | false | N/A     | NONE  |
|                | k4    | BIGINT   | Yes  | false | N/A     | NONE  |
|                | k5    | LARGEINT | Yes  | false | N/A     | NONE  |
|                | k6    | FLOAT    | Yes  | false | N/A     | NONE  |
|                | k7    | DOUBLE   | Yes  | false | N/A     | NONE  |
|                |       |          |      |       |         |       |
| k1_sumk2       | k1    | TINYINT  | Yes  | true  | N/A     |       |
|                | k2    | SMALLINT | Yes  | false | N/A     | SUM   |
+----------------+-------+----------+------+-------+---------+-------+
```

1. 删除表 all_type_table 的名为 k1_sumk2 的物化视图

	```
	drop materialized view k1_sumk2 on all_type_table;
	```
	物化视图被删除后的表结构
	
	```
	+----------------+-------+----------+------+-------+---------+-------+
| IndexName      | Field | Type     | Null | Key   | Default | Extra |
+----------------+-------+----------+------+-------+---------+-------+
| all_type_table | k1    | TINYINT  | Yes  | true  | N/A     |       |
|                | k2    | SMALLINT | Yes  | false | N/A     | NONE  |
|                | k3    | INT      | Yes  | false | N/A     | NONE  |
|                | k4    | BIGINT   | Yes  | false | N/A     | NONE  |
|                | k5    | LARGEINT | Yes  | false | N/A     | NONE  |
|                | k6    | FLOAT    | Yes  | false | N/A     | NONE  |
|                | k7    | DOUBLE   | Yes  | false | N/A     | NONE  |
+----------------+-------+----------+------+-------+---------+-------+
	```
	
2. 删除表 all_type_table 中一个不存在的物化视图

	```
	drop materialized view k1_k2 on all_type_table;
	ERROR 1064 (HY000): errCode = 2, detailMessage = Materialized view [k1_k2] does not exist in table [all_type_table]
	```
	删除请求直接报错

3. 删除表 all_type_table 中的物化视图 k1_k2，不存在不报错。

	```
	drop materialized view if exists k1_k2 on all_type_table;
Query OK, 0 rows affected (0.00 sec)
	```
	
	存在则删除，不存在则不报错。

## keyword
  DROP, MATERIALIZED, VIEW	
