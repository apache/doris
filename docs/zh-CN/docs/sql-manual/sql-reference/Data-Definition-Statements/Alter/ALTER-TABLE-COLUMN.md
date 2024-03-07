---
{
    "title": "ALTER-TABLE-COLUMN",
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

## Name

ALTER TABLE COLUMN

### Description

该语句用于对已有 table 进行 Schema change 操作。schema change 是异步的，任务提交成功则返回，之后可使用[SHOW ALTER TABLE COLUMN](../../Show-Statements/SHOW-ALTER.md) 命令查看进度。

:::tip
Doris 在建表之后有物化索引的概念，在建表成功后为 base 表，物化索引为 base index，基于 base 表可以创建 rollup index。其中 base index 和 rollup index 都是物化索引，在进行 schema change 操作时如果不指定 rollup_index_name 默认基于 base 表进行操作。
Doris 在 1.2.0 支持了 light schema change 轻量表结构变更，对于值列的加减操作，可以更快地，同步地完成。可以在建表时手动指定 "light_schema_change" = 'true'，2.0.0 及之后版本该参数默认开启。
:::

### 语法：

```sql
ALTER TABLE [database.]table alter_clause;
```

schema change 的 alter_clause 支持如下几种修改方式：

**1. 添加列，向指定的 index 位置进行列添加**

**语法**

```sql
ALTER TABLE [database.]table table_name ADD COLUMN column_name column_type [KEY | agg_type] [DEFAULT "default_value"]
[AFTER column_name|FIRST]
[TO rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```
  
**Example**
  
1. 向 example_db.my_table 的 key_1 后添加一个 key 列 new_col (非聚合模型)

  ```sql
  ALTER TABLE example_db.my_table
  ADD COLUMN new_col INT KEY DEFAULT "0" AFTER key_1;
  ```

2. 向 example_db.my_table 的 value_1 后添加一个 value 列 new_col (非聚合模型)

  ```sql
  ALTER TABLE example_db.my_table
  ADD COLUMN new_col INT DEFAULT "0" AFTER value_1;
  ```

3. 向 example_db.my_table 的 key_1 后添加一个 key 列 new_col (聚合模型)

  ```sql
  ALTER TABLE example_db.my_table
  ADD COLUMN new_col INT KEY DEFAULT "0" AFTER key_1;
  ```

4. 向 example_db.my_table 的 value_1 后添加一个 value 列 new_col SUM 聚合类型 (聚合模型)

  ```sql
  ALTER TABLE example_db.my_table   
  ADD COLUMN new_col INT SUM DEFAULT "0" AFTER value_1; 
  ```

5. 将 new_col 添加到 example_db.my_table 表的首列位置 (非聚合模型)

  ```sql
  ALTER TABLE example_db.my_table
  ADD COLUMN new_col INT KEY DEFAULT "0" FIRST;
  ```
  
:::tip
  - 聚合模型如果增加 value 列，需要指定 agg_type
  - 非聚合模型（如 DUPLICATE KEY）如果增加key列，需要指定KEY关键字
  - 不能在 rollup index 中增加 base index 中已经存在的列（如有需要，可以重新创建一个 rollup index）
:::

**2. 添加多列，向指定的 index 位置进行多列添加**

**语法**
  
  ```sql
  ALTER TABLE [database.]table table_name ADD COLUMN (column_name1 column_type [KEY | agg_type] DEFAULT "default_value", ...)
  [TO rollup_index_name]
  [PROPERTIES ("key"="value", ...)]
  ```
  
**Example**
  
1. 向 example_db.my_table 中添加多列，new_col 和 new_col2 都是 SUM 聚合类型(聚合模型)

  ```sql
  ALTER TABLE example_db.my_table
  ADD COLUMN (new_col1 INT SUM DEFAULT "0" ,new_col2 INT SUM DEFAULT "0");
  ```

2. 向 example_db.my_table 中添加多列(非聚合模型)，其中 new_col1 为 KEY 列，new_col2 为 value 列

  ```sql
  ALTER TABLE example_db.my_table
  ADD COLUMN (new_col1 INT key DEFAULT "0" , new_col2 INT DEFAULT "0");
  ```
  
:::tip
  
  - 聚合模型如果增加 value 列，需要指定agg_type
  - 聚合模型如果增加key列，需要指定KEY关键字
  - 不能在 rollup index 中增加 base index 中已经存在的列（如有需要，可以重新创建一个 rollup index）
:::

**3. 删除列，从指定 index 中删除一列**

**语法**
  
  ```sql
  ALTER TABLE [database.]table table_name DROP COLUMN column_name
  [FROM rollup_index_name]
  ```
  
**Example**
   
1. 从 example_db.my_table 中删除 col1 列
  
    ```sql
    ALTER TABLE example_db.my_table DROP COLUMN col1;
    ```

:::tip
  - 不能删除分区列
  - 聚合模型不能删除KEY列
  - 如果是从 base index 中删除列，则如果 rollup index 中包含该列，也会被删除
:::

**4. 修改指定列类型以及列位置**

**语法**
  
```sql
ALTER TABLE [database.]table table_name MODIFY COLUMN column_name column_type [KEY | agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]
[AFTER column_name|FIRST]
[FROM rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

**Example**
  
1. 修改 example_db.my_table 的 key 列 col1 的类型为 BIGINT，并移动到 col2 列后面。

  ```sql
  ALTER TABLE example_db.my_table 
  MODIFY COLUMN col1 BIGINT KEY DEFAULT "1" AFTER col2;
  ```

  :::tip
  无论是修改 key 列还是 value 列都需要声明完整的 column 信息
  :::

2. 修改 example_db.my_table 的 val1 列最大长度。原 val1 为 (val1 VARCHAR(32) REPLACE DEFAULT "abc")

  ```sql
  ALTER TABLE example_db.my_table 
  MODIFY COLUMN val1 VARCHAR(64) REPLACE DEFAULT "abc";
  ```

  :::tip
  只能修改列的类型，列的其他属性维持原样
  :::

3. 修改 Duplicate key 表 Key 列的某个字段的长度

  ```sql
  ALTER TABLE example_db.my_table 
  MODIFY COLUMN k3 VARCHAR(50) KEY NULL COMMENT 'to 50';
  ```
  
:::tip
  - 聚合模型如果修改 value 列，需要指定 agg_type
  - 非聚合类型如果修改key列，需要指定KEY关键字
  - 只能修改列的类型，列的其他属性维持原样（即其他属性需在语句中按照原属性显式的写出，参见 example 8）
  - 分区列和分桶列不能做任何修改
  - 目前支持以下类型的转换（精度损失由用户保证）
    - TINYINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE 类型向范围更大的数字类型转换
    - TINTINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE/DECIMAL 转换成 VARCHAR
    - VARCHAR 支持修改最大长度
    - VARCHAR/CHAR 转换成 TINTINT/SMALLINT/INT/BIGINT/LARGEINT/FLOAT/DOUBLE
    - VARCHAR/CHAR 转换成 DATE (目前支持"%Y-%m-%d", "%y-%m-%d", "%Y%m%d", "%y%m%d", "%Y/%m/%d, "%y/%m/%d"六种格式化格式)
    - DATETIME 转换成 DATE(仅保留年-月-日信息, 例如: `2019-12-09 21:47:05` <--> `2019-12-09`)
    - DATE 转换成 DATETIME(时分秒自动补零， 例如: `2019-12-09` <--> `2019-12-09 00:00:00`)
    - FLOAT 转换成 DOUBLE
    - INT 转换成 DATE (如果INT类型数据不合法则转换失败，原始数据不变)
    - 除DATE与DATETIME以外都可以转换成STRING，但是STRING不能转换任何其他类型
:::

**5. 对指定表的列进行重新排序**

**语法**
  
  ```sql
  ALTER TABLE [database.]table table_name ORDER BY (column_name1, column_name2, ...)
  [FROM rollup_index_name]
  [PROPERTIES ("key"="value", ...)]
  ```

**Example**
  
1. 调整 example_db.my_table 的 key 列 和 value 列的顺序（非聚合模型）

  ```sql
  CREATE TABLE `my_table`(
  `k_1` INT NULL,
  `k_2` INT NULL,
  `v_1` INT NULL,
  `v_2` varchar NULL,
  `v_3` varchar NULL
  ) ENGINE=OLAP
  DUPLICATE KEY(`k_1`, `k_2`)
  COMMENT 'OLAP'
  DISTRIBUTED BY HASH(`k_1`) BUCKETS 5
  PROPERTIES (
  "replication_allocation" = "tag.location.default: 1"
  );

  ALTER TABLE example_db.my_table ORDER BY (k_2,k_1,v_3,v_2,v_1);

  mysql> desc my_table;
  +-------+------------+------+-------+---------+-------+
  | Field | Type       | Null | Key   | Default | Extra |
  +-------+------------+------+-------+---------+-------+
  | k_2   | INT        | Yes  | true  | NULL    |       |
  | k_1   | INT        | Yes  | true  | NULL    |       |
  | v_3   | VARCHAR(*) | Yes  | false | NULL    | NONE  |
  | v_2   | VARCHAR(*) | Yes  | false | NULL    | NONE  |
  | v_1   | INT        | Yes  | false | NULL    | NONE  |
  +-------+------------+------+-------+---------+-------+
  ```

2. 同时执行添加列和列排序操作

  ```sql
  CREATE TABLE `my_table` (
  `k_1` INT NULL,
  `k_2` INT NULL,
  `v_1` INT NULL,
  `v_2` varchar NULL,
  `v_3` varchar NULL
  ) ENGINE=OLAP
  DUPLICATE KEY(`k_1`, `k_2`)
  COMMENT 'OLAP'
  DISTRIBUTED BY HASH(`k_1`) BUCKETS 5
  PROPERTIES (
  "replication_allocation" = "tag.location.default: 1"
  );

  ALTER TABLE example_db.my_table
  ADD COLUMN col INT DEFAULT "0" AFTER v_1,
  ORDER BY (k_2,k_1,v_3,v_2,v_1,col);

  mysql> desc my_table;
  +-------+------------+------+-------+---------+-------+
  | Field | Type       | Null | Key   | Default | Extra |
  +-------+------------+------+-------+---------+-------+
  | k_2   | INT        | Yes  | true  | NULL    |       |
  | k_1   | INT        | Yes  | true  | NULL    |       |
  | v_3   | VARCHAR(*) | Yes  | false | NULL    | NONE  |
  | v_2   | VARCHAR(*) | Yes  | false | NULL    | NONE  |
  | v_1   | INT        | Yes  | false | NULL    | NONE  |
  | col   | INT        | Yes  | false | 0       | NONE  |
  +-------+------------+------+-------+---------+-------+
  ```
  
:::tip
  - index 中的所有列都要写出来
  - value 列在 key 列之后
  - key 列只能调整 key 列的范围内进行调整，value 列同理
:::

### Keywords

```text
ALTER, TABLE, COLUMN, ALTER TABLE
```

### Best Practice

