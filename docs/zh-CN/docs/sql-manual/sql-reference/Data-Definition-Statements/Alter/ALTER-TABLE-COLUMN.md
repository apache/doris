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

## ALTER-TABLE-COLUMN

### Name

ALTER TABLE COLUMN

### Description

该语句用于对已有 table 进行 Schema change 操作。schema change 是异步的，任务提交成功则返回，之后可使用[SHOW ALTER TABLE COLUMN](../../Show-Statements/SHOW-ALTER.md) 命令查看进度。

语法：

```sql
ALTER TABLE [database.]table alter_clause;
```

schema change 的 alter_clause 支持如下几种修改方式：

1. 向指定 index 的指定位置添加一列

语法：

```sql
ADD COLUMN column_name column_type [KEY | agg_type] [DEFAULT "default_value"]
[AFTER column_name|FIRST]
[TO rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

 注意：

- 聚合模型如果增加 value 列，需要指定 agg_type
- 非聚合模型（如 DUPLICATE KEY）如果增加key列，需要指定KEY关键字
-  不能在 rollup index 中增加 base index 中已经存在的列（如有需要，可以重新创建一个 rollup index）

2. 向指定 index 添加多列

语法：

```sql
ADD COLUMN (column_name1 column_type [KEY | agg_type] DEFAULT "default_value", ...)
[TO rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

注意：

- 聚合模型如果增加 value 列，需要指定agg_type
- 聚合模型如果增加key列，需要指定KEY关键字
- 不能在 rollup index 中增加 base index 中已经存在的列（如有需要，可以重新创建一个 rollup index）

3. 从指定 index 中删除一列

语法：

```sql
DROP COLUMN column_name
[FROM rollup_index_name]
```

注意：

- 不能删除分区列
- 如果是从 base index 中删除列，则如果 rollup index 中包含该列，也会被删除

4. 修改指定 index 的列类型以及列位置

 语法：

```sql
MODIFY COLUMN column_name column_type [KEY | agg_type] [NULL | NOT NULL] [DEFAULT "default_value"]
[AFTER column_name|FIRST]
[FROM rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

注意：

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

5. 对指定 index 的列进行重新排序

语法：

```sql
ORDER BY (column_name1, column_name2, ...)
[FROM rollup_index_name]
[PROPERTIES ("key"="value", ...)]
```

注意：

- index 中的所有列都要写出来
- value 列在 key 列之后

### Example

1. 向 example_rollup_index 的 col1 后添加一个key列 new_col(非聚合模型)

```sql
ALTER TABLE example_db.my_table
ADD COLUMN new_col INT KEY DEFAULT "0" AFTER col1
TO example_rollup_index;
```

2. 向example_rollup_index的col1后添加一个value列new_col(非聚合模型)

```sql
ALTER TABLE example_db.my_table   
ADD COLUMN new_col INT DEFAULT "0" AFTER col1    
TO example_rollup_index;
```

3. 向example_rollup_index的col1后添加一个key列new_col(聚合模型)

```sql
ALTER TABLE example_db.my_table   
ADD COLUMN new_col INT DEFAULT "0" AFTER col1    
TO example_rollup_index;
```

4. 向example_rollup_index的col1后添加一个value列new_col SUM聚合类型(聚合模型)

```sql
ALTER TABLE example_db.my_table   
ADD COLUMN new_col INT SUM DEFAULT "0" AFTER col1    
TO example_rollup_index;
```

5. 向 example_rollup_index 添加多列(聚合模型)

```sql
ALTER TABLE example_db.my_table
ADD COLUMN (col1 INT DEFAULT "1", col2 FLOAT SUM DEFAULT "2.3")
TO example_rollup_index;
```

6. 从 example_rollup_index 删除一列

```sql
ALTER TABLE example_db.my_table
DROP COLUMN col2
FROM example_rollup_index;
```

7. 修改 base index 的 key 列 col1 的类型为 BIGINT，并移动到 col2 列后面。

```sql
ALTER TABLE example_db.my_table 
MODIFY COLUMN col1 BIGINT KEY DEFAULT "1" AFTER col2;
```

注意：无论是修改 key 列还是 value 列都需要声明完整的 column 信息

8. 修改 base index 的 val1 列最大长度。原 val1 为 (val1 VARCHAR(32) REPLACE DEFAULT "abc")

```sql
ALTER TABLE example_db.my_table 
MODIFY COLUMN val1 VARCHAR(64) REPLACE DEFAULT "abc";
```
注意：只能修改列的类型，列的其他属性维持原样

9. 重新排序 example_rollup_index 中的列（设原列顺序为：k1,k2,k3,v1,v2）

```sql
ALTER TABLE example_db.my_table
ORDER BY (k3,k1,k2,v2,v1)
FROM example_rollup_index;
```

10. 同时执行两种操作

```sql
ALTER TABLE example_db.my_table
ADD COLUMN v2 INT MAX DEFAULT "0" AFTER k2 TO example_rollup_index,
ORDER BY (k3,k1,k2,v2,v1) FROM example_rollup_index;
```

11. 修改Duplicate key 表 Key 列的某个字段的长度

```sql
alter table example_tbl modify column k3 varchar(50) key null comment 'to 50'
```



### Keywords

```text
ALTER, TABLE, COLUMN, ALTER TABLE
```

### Best Practice

