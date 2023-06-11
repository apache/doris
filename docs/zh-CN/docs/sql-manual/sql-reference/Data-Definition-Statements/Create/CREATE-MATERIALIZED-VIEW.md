---
{
    "title": "CREATE-MATERIALIZED-VIEW",
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

## CREATE-MATERIALIZED-VIEW

### Name

CREATE MATERIALIZED VIEW

### Description

该语句用于创建物化视图。物化视图分为单表物化视图和多表物化视图，原理差异比较大，下面分两个部分分别介绍。

### 单表物化视图

该操作为异步操作，提交成功后，需通过 [SHOW ALTER TABLE MATERIALIZED VIEW](../../Show-Statements/SHOW-ALTER-TABLE-MATERIALIZED-VIEW.md) 查看作业进度。在显示 FINISHED 后既可通过 `desc [table_name] all` 命令来查看物化视图的 schema 了。

语法：

```sql
CREATE MATERIALIZED VIEW [MV name] as [query]
[PROPERTIES ("key" = "value")]
```

说明：

- `MV name`：物化视图的名称，必填项。相同表的物化视图名称不可重复。

- `query`：用于构建物化视图的查询语句，查询语句的结果既物化视图的数据。目前支持的 query 格式为:

  ```sql
  SELECT select_expr[, select_expr ...]
  FROM [Base view name]
  GROUP BY column_name[, column_name ...]
  ORDER BY column_name[, column_name ...]
  ```

  语法和查询语句语法一致。

  - `select_expr`：物化视图的 schema 中所有的列。  
    - 至少包含一个单列。 
  - `base view name`：物化视图的原始表名，必填项。  
    - 必须是单表，且非子查询
  - `group by`：物化视图的分组列，选填项。 
    - 不填则数据不进行分组。
  - `order by`：物化视图的排序列，选填项。  
    - 排序列的声明顺序必须和 select_expr 中列声明顺序一致。  
    - 如果不声明 order by，则根据规则自动补充排序列。 如果物化视图是聚合类型，则所有的分组列自动补充为排序列。 如果物化视图是非聚合类型，则前 36 个字节自动补充为排序列。
    - 如果自动补充的排序个数小于3个，则前三个作为排序列。 如果 query 中包含分组列的话，则排序列必须和分组列一致。

- properties

  声明物化视图的一些配置，选填项。

  ```text
  PROPERTIES ("key" = "value", "key" = "value" ...)
  ```

  以下几个配置，均可声明在此处：

  ```text
   short_key: 排序列的个数。
   timeout: 物化视图构建的超时时间。
  ```

#### Example

Base 表结构为

```sql
mysql> desc duplicate_table;
+-------+--------+------+------+---------+-------+
| Field | Type   | Null | Key  | Default | Extra |
+-------+--------+------+------+---------+-------+
| k1    | INT    | Yes  | true | N/A     |       |
| k2    | INT    | Yes  | true | N/A     |       |
| k3    | BIGINT | Yes  | true | N/A     |       |
| k4    | BIGINT | Yes  | true | N/A     |       |
+-------+--------+------+------+---------+-------+
```
```sql
create table duplicate_table(
	k1 int null,
	k2 int null,
	k3 bigint null,
	k4 bigint null
)
duplicate key (k1,k2,k3,k4)
distributed BY hash(k4) buckets 3
properties("replication_num" = "1");
```
注意：如果物化视图包含了base表的分区列和分桶列,那么这些列必须作为物化视图中的key列

1. 创建一个仅包含原始表 （k1, k2）列的物化视图

   ```sql
   create materialized view k1_k2 as
   select k2, k1 from duplicate_table;
   ```

   物化视图的 schema 如下图，物化视图仅包含两列 k1, k2 且不带任何聚合

   ```text
   +-----------------+-------+--------+------+------+---------+-------+
   | IndexName       | Field | Type   | Null | Key  | Default | Extra |
   +-----------------+-------+--------+------+------+---------+-------+
   | k2_k1           | k2    | INT    | Yes  | true | N/A     |       |
   |                 | k1    | INT    | Yes  | true | N/A     |       |
   +-----------------+-------+--------+------+------+---------+-------+
   ```

2. 创建一个以 k2 为排序列的物化视图

   ```sql
   create materialized view k2_order as
   select k2, k1 from duplicate_table order by k2;
   ```

   物化视图的 schema 如下图，物化视图仅包含两列 k2, k1，其中 k2 列为排序列，不带任何聚合。

   ```text
   +-----------------+-------+--------+------+-------+---------+-------+
   | IndexName       | Field | Type   | Null | Key   | Default | Extra |
   +-----------------+-------+--------+------+-------+---------+-------+
   | k2_order        | k2    | INT    | Yes  | true  | N/A     |       |
   |                 | k1    | INT    | Yes  | false | N/A     | NONE  |
   +-----------------+-------+--------+------+-------+---------+-------+
   ```

3. 创建一个以 k1, k2 分组，k3 列为 SUM 聚合的物化视图

   ```sql
   create materialized view k1_k2_sumk3 as
   select k1, k2, sum(k3) from duplicate_table group by k1, k2;
   ```

   物化视图的 schema 如下图，物化视图包含两列 k1, k2，sum(k3) 其中 k1, k2 为分组列，sum(k3) 为根据 k1, k2 分组后的 k3 列的求和值。

   由于物化视图没有声明排序列，且物化视图带聚合数据，系统默认补充分组列 k1, k2 为排序列。

   ```text
   +-----------------+-------+--------+------+-------+---------+-------+
   | IndexName       | Field | Type   | Null | Key   | Default | Extra |
   +-----------------+-------+--------+------+-------+---------+-------+
   | k1_k2_sumk3     | k1    | INT    | Yes  | true  | N/A     |       |
   |                 | k2    | INT    | Yes  | true  | N/A     |       |
   |                 | k3    | BIGINT | Yes  | false | N/A     | SUM   |
   +-----------------+-------+--------+------+-------+---------+-------+
   ```

4. 创建一个去除重复行的物化视图

   ```sql
   create materialized view deduplicate as
   select k1, k2, k3, k4 from duplicate_table group by k1, k2, k3, k4;
   ```

   物化视图 schema 如下图，物化视图包含 k1, k2, k3, k4列，且不存在重复行。

   ```text
   +-----------------+-------+--------+------+-------+---------+-------+
   | IndexName       | Field | Type   | Null | Key   | Default | Extra |
   +-----------------+-------+--------+------+-------+---------+-------+
   | deduplicate     | k1    | INT    | Yes  | true  | N/A     |       |
   |                 | k2    | INT    | Yes  | true  | N/A     |       |
   |                 | k3    | BIGINT | Yes  | true  | N/A     |       |
   |                 | k4    | BIGINT | Yes  | true  | N/A     |       |
   +-----------------+-------+--------+------+-------+---------+-------+
   ```

5. 创建一个不声明排序列的非聚合型物化视图

   all_type_table 的 schema 如下

   ```
   +-------+--------------+------+-------+---------+-------+
   | Field | Type         | Null | Key   | Default | Extra |
   +-------+--------------+------+-------+---------+-------+
   | k1    | TINYINT      | Yes  | true  | N/A     |       |
   | k2    | SMALLINT     | Yes  | true  | N/A     |       |
   | k3    | INT          | Yes  | true  | N/A     |       |
   | k4    | BIGINT       | Yes  | true  | N/A     |       |
   | k5    | DECIMAL(9,0) | Yes  | true  | N/A     |       |
   | k6    | DOUBLE       | Yes  | false | N/A     | NONE  |
   | k7    | VARCHAR(20)  | Yes  | false | N/A     | NONE  |
   +-------+--------------+------+-------+---------+-------+
   ```

   物化视图包含 k3, k4, k5, k6, k7 列，且不声明排序列，则创建语句如下：

   ```sql
   create materialized view mv_1 as
   select k3, k4, k5, k6, k7 from all_type_table;
   ```

   系统默认补充的排序列为 k3, k4, k5 三列。这三列类型的字节数之和为 4(INT) + 8(BIGINT) + 16(DECIMAL) = 28 < 36。所以补充的是这三列作为排序列。 物化视图的 schema 如下，可以看到其中 k3, k4, k5 列的 key 字段为 true，也就是排序列。k6, k7 列的 key 字段为 false，也就是非排序列。

   ```sql
   +----------------+-------+--------------+------+-------+---------+-------+
   | IndexName      | Field | Type         | Null | Key   | Default | Extra |
   +----------------+-------+--------------+------+-------+---------+-------+
   | mv_1           | k3    | INT          | Yes  | true  | N/A     |       |
   |                | k4    | BIGINT       | Yes  | true  | N/A     |       |
   |                | k5    | DECIMAL(9,0) | Yes  | true  | N/A     |       |
   |                | k6    | DOUBLE       | Yes  | false | N/A     | NONE  |
   |                | k7    | VARCHAR(20)  | Yes  | false | N/A     | NONE  |
   +----------------+-------+--------------+------+-------+---------+-------+
   ```

### 多表物化视图

该操作为异步操作，提交成功后，需通过 [SHOW MTMV JOB](../../Show-Statements/SHOW-MTMV-JOB.md)和[SHOW MTMV TASK](../../Show-Statements/SHOW-MTMV-TASK.md) 查看作业进度。

语法：

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [database.]mvName 
build_mv 
[mv_refersh_info] 
[mv_keys] 
[distribution] 
[properties] 
AS 
query_stmt
```

build_mv
```sql
BUILD IMMEDIATE | DEFERRED

IMMEDIATE：物化视图创建成功后立即刷新。
DEFERRED：物化视图创建成功后不进行刷新。可以通过手动调用[刷新命令](../../Utility-Statements/REFRESH-MATERIALIZED-VIEW.md)或通过mv_refersh_info设置定时任务触发刷新。
```

mv_refersh_info
```sql
REFRESH [refresh_method] [refresh_trigger]

NEVER REFRESH
```

refresh_method
```sql
FAST | COMPLETE | FORCE

COMPLETE：全量刷新
目前仅支持COMPLETE
```

refresh_trigger
```sql
ON DEMAND | COMMIT

START WITH start_time

START WITH start_time NEXT interval time_unit

NEXT interval time_unit

ON DEMAND：按需刷新
COMMIT：暂未支持
定时刷新：需要指定刷新开始时间和刷新间隔 START('yyyy-MM-dd hh:mm:ss') EVERY (interval n day/hour/minute/second)。刷新间隔仅支持：DAY、HOUR、MINUTE 以及 SECOND。
```

mv_keys
```sql
物化视图默认使用 DUPLICATE KEY 模型，因此括号里指定的为排序列
KEY(col1, col2, ...)
```

distribution 同[CREATE TABLE](CREATE-TABLE.md)

properties 同[CREATE TABLE](CREATE-TABLE.md)

query_stmt
```sql
SELECT select_expr[, select_expr ...]
[GROUP BY column_name[, column_name ...]]
[ORDER BY column_name[, column_name ...]]

select_expr:必填，查询语句中所有的列，也即物化视图的 schema 中所有的列,该参数支持如下值:
    单列:形如 SELECT a, b, c FROM table_a
    聚合列：形如 SELECT sum(a) as b FROM table_a
GROUP BY：选填，构建物化视图查询语句的分组列。如不指定该参数，则默认不对数据进行分组。
ORDER BY：选填，构建物化视图查询语句的排序列。
```

#### Example

Base 表结构为

```sql
mysql> desc duplicate_table;
+-------+--------+------+------+---------+-------+
| Field | Type   | Null | Key  | Default | Extra |
+-------+--------+------+------+---------+-------+
| k1    | INT    | Yes  | true | N/A     |       |
| k2    | INT    | Yes  | true | N/A     |       |
| k3    | BIGINT | Yes  | true | N/A     |       |
| k4    | BIGINT | Yes  | true | N/A     |       |
+-------+--------+------+------+---------+-------+
```
```sql
create table duplicate_table(
	k1 int null,
	k2 int null,
	k3 bigint null,
	k4 bigint null
)
duplicate key (k1,k2,k3,k4)
distributed BY hash(k4) buckets 3
properties("replication_num" = "1");

create table duplicate_table2(
	k1 int null,
	k5 int null
)
duplicate key (k1)
distributed BY hash(k5) buckets 3
properties("replication_num" = "2");
```

1. 创建一个仅包含原始表 （k1, k2）列的物化视图，立即刷新，后续手动刷新

   ```sql
   create materialized view k1_k2 BUILD IMMEDIATE NEVER REFRESH 
   DISTRIBUTED BY HASH(k1) 
   BUCKETS 6 
   properties("replication_num" = "1")
   as
   select k1, k2 from duplicate_table;
   ```

2. 创建一个仅包含原始表 （k1, k2）列的物化视图，不立即刷新，后续手动刷新

   ```sql
   create materialized view k1_k2 BUILD DEFERRED NEVER REFRESH 
   DISTRIBUTED BY HASH(k1) 
   BUCKETS 6 
   properties("replication_num" = "1")
   as
   select k1, k2 from duplicate_table;
   ```

3. 创建一个仅包含原始表 （k1, k2）列的物化视图，不立即刷新，后续定时刷新

   ```sql
   create materialized view k1_k2 BUILD DEFERRED REFRESH COMPLETE START WITH "2023-05-16 21:33:00" NEXT 6 MINUTE 
   DISTRIBUTED BY HASH(k1) 
   BUCKETS 6 
   properties("replication_num" = "1")
   as
   select k1, k2 from duplicate_table;
   ```

4. 创建一个多表关联的物化视图

   ```sql
   create materialized view multi BUILD DEFERRED REFRESH COMPLETE START WITH "2023-05-16 21:33:00" NEXT 6 MINUTE 
   DISTRIBUTED BY HASH(k1) 
   BUCKETS 6 
   properties("replication_num" = "1")
   as
   select a.k1, a.k2, b.k5 from duplicate_table a join duplicate_table2 b on a.k1=b.k1;
   ```

5. 创建一个聚合的聚合的物化视图

   ```sql
   create materialized view aggr BUILD DEFERRED REFRESH COMPLETE START WITH "2023-05-16 21:33:00" NEXT 6 MINUTE 
   DISTRIBUTED BY HASH(k1) 
   BUCKETS 6 
   properties("replication_num" = "1")
   as
   select k1, sum(k2) as ksum from duplicate_table group by k1;
   ```


### Keywords

    CREATE, MATERIALIZED, VIEW

### Best Practice

