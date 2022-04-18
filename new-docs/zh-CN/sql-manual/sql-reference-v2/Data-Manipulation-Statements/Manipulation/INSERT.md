---
{
    "title": "INSERT",
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

## INSERT

### Name

INSERT

### Description

该语句是完成数据插入操作。

```sql
INSERT INTO table_name
    [ PARTITION (p1, ...) ]
    [ WITH LABEL label]
    [ (column [, ...]) ]
    [ [ hint [, ...] ] ]
    { VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }
```

 Parameters

> tablet_name: 导入数据的目的表。可以是 `db_name.table_name` 形式
>
> partitions: 指定待导入的分区，必须是 `table_name` 中存在的分区，多个分区名称用逗号分隔
>
> label: 为 Insert 任务指定一个 label
>
> column_name: 指定的目的列，必须是 `table_name` 中存在的列
>
> expression: 需要赋值给某个列的对应表达式
>
> DEFAULT: 让对应列使用默认值
>
> query: 一个普通查询，查询的结果会写入到目标中
>
> hint: 用于指示 `INSERT` 执行行为的一些指示符。`streaming` 和 默认的非 `streaming` 方式均会使用同步方式完成 `INSERT` 语句执行
>    非 `streaming` 方式在执行完成后会返回一个 label 方便用户通过 `SHOW LOAD` 查询导入的状态

注意：

当前执行 `INSERT` 语句时，对于有不符合目标表格式的数据，默认的行为是过滤，比如字符串超长等。但是对于有要求数据不能够被过滤的业务场景，可以通过设置会话变量 `enable_insert_strict` 为 `true` 来确保当有数据被过滤掉的时候，`INSERT` 不会被执行成功。

### Example

`test` 表包含两个列`c1`, `c2`。

1. 向`test`表中导入一行数据

```sql
INSERT INTO test VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, 2);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT);
INSERT INTO test (c1) VALUES (1);
```

其中第一条、第二条语句是一样的效果。在不指定目标列时，使用表中的列顺序来作为默认的目标列。
第三条、第四条语句表达的意思是一样的，使用`c2`列的默认值，来完成数据导入。

2. 向`test`表中一次性导入多行数据

```sql
INSERT INTO test VALUES (1, 2), (3, 2 + 2);
INSERT INTO test (c1, c2) VALUES (1, 2), (3, 2 * 2);
INSERT INTO test (c1) VALUES (1), (3);
INSERT INTO test (c1, c2) VALUES (1, DEFAULT), (3, DEFAULT);
```

其中第一条、第二条语句效果一样，向`test`表中一次性导入两条数据
第三条、第四条语句效果已知，使用`c2`列的默认值向`test`表中导入两条数据

3. 向 `test` 表中导入一个查询语句结果

```sql
INSERT INTO test SELECT * FROM test2;
INSERT INTO test (c1, c2) SELECT * from test2;
```

4. 向 `test` 表中导入一个查询语句结果，并指定 partition 和 label

```sql
INSERT INTO test PARTITION(p1, p2) WITH LABEL `label1` SELECT * FROM test2;
INSERT INTO test WITH LABEL `label1` (c1, c2) SELECT * from test2;
```

异步的导入其实是，一个同步的导入封装成了异步。填写 streaming 和不填写的**执行效率是一样**的。

由于Doris之前的导入方式都是异步导入方式，为了兼容旧有的使用习惯，不加 streaming 的 `INSERT` 语句依旧会返回一个 label，用户需要通过`SHOW LOAD`命令查看此`label`导入作业的状态。

### Keywords

    INSERT

### Best Practice

