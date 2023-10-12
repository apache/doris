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
> hint: 用于指示 `INSERT` 执行行为的一些指示符。目前 hint 有三个可选值`/*+ STREAMING */`、`/*+ SHUFFLE */`或`/*+ NOSHUFFLE */`
> 1. STREAMING：目前无实际作用，只是为了兼容之前的版本，因此保留。（之前的版本加上这个 hint 会返回 label，现在默认都会返回 label）
> 2. SHUFFLE：当目标表是分区表，开启这个 hint 会进行 repartiiton。
> 3. NOSHUFFLE：即使目标表是分区表，也不会进行 repartiiton，但会做一些其他操作以保证数据正确落到各个分区中。

对于开启了merge-on-write的Unique表，还可以使用insert语句进行部分列更新的操作。要使用insert语句进行部分列更新，需要将会话变量enable_unique_key_partial_update的值设置为true(该变量默认值为false，即默认无法通过insert语句进行部分列更新)。进行部分列更新时，插入的列必须至少包含所有的Key列，同时指定需要更新的列。如果插入行Key列的值在原表中存在，则将更新具有相同key列值那一行的数据。如果插入行Key列的值在原表中不存在，则将向表中插入一条新的数据，此时insert语句中没有指定的列必须有默认值或可以为null，这些缺失列会首先尝试用默认值填充，如果该列没有默认值，则尝试使用null值填充，如果该列不能为null，则本次插入失败。

需要注意的是，控制insert语句是否开启严格模式的会话变量`enable_insert_strict`的默认值为true，即insert语句默认开启严格模式，而在严格模式下进行部分列更新不允许更新不存在的key。所以，在使用insert语句进行部分列更新的时候如果希望能插入不存在的key，需要在`enable_unique_key_partial_update`设置为true的基础上同时将`enable_insert_strict`也设置为true。

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


### Keywords

    INSERT

### Best Practice

1. 查看返回结果

   INSERT 操作是一个同步操作，返回结果即表示操作结束。用户需要根据返回结果的不同，进行对应的处理。

   1. 执行成功，结果集为空

      如果 insert 对应 select 语句的结果集为空，则返回如下：

      ```sql
      mysql> insert into tbl1 select * from empty_tbl;
      Query OK, 0 rows affected (0.02 sec)
      ```

      `Query OK` 表示执行成功。`0 rows affected` 表示没有数据被导入。

   2. 执行成功，结果集不为空

      在结果集不为空的情况下。返回结果分为如下几种情况：

      1. Insert 执行成功并可见：

         ```sql
         mysql> insert into tbl1 select * from tbl2;
         Query OK, 4 rows affected (0.38 sec)
         {'label':'insert_8510c568-9eda-4173-9e36-6adc7d35291c', 'status':'visible', 'txnId':'4005'}
         
         mysql> insert into tbl1 with label my_label1 select * from tbl2;
         Query OK, 4 rows affected (0.38 sec)
         {'label':'my_label1', 'status':'visible', 'txnId':'4005'}
         
         mysql> insert into tbl1 select * from tbl2;
         Query OK, 2 rows affected, 2 warnings (0.31 sec)
         {'label':'insert_f0747f0e-7a35-46e2-affa-13a235f4020d', 'status':'visible', 'txnId':'4005'}
         
         mysql> insert into tbl1 select * from tbl2;
         Query OK, 2 rows affected, 2 warnings (0.31 sec)
         {'label':'insert_f0747f0e-7a35-46e2-affa-13a235f4020d', 'status':'committed', 'txnId':'4005'}
         ```

         `Query OK` 表示执行成功。`4 rows affected` 表示总共有4行数据被导入。`2 warnings` 表示被过滤的行数。

         同时会返回一个 json 串：

         ```json
         {'label':'my_label1', 'status':'visible', 'txnId':'4005'}
         {'label':'insert_f0747f0e-7a35-46e2-affa-13a235f4020d', 'status':'committed', 'txnId':'4005'}
         {'label':'my_label1', 'status':'visible', 'txnId':'4005', 'err':'some other error'}
         ```

         `label` 为用户指定的 label 或自动生成的 label。Label 是该 Insert Into 导入作业的标识。每个导入作业，都有一个在单 database 内部唯一的 Label。

         `status` 表示导入数据是否可见。如果可见，显示 `visible`，如果不可见，显示 `committed`。

         `txnId` 为这个 insert 对应的导入事务的 id。

         `err` 字段会显示一些其他非预期错误。

         当需要查看被过滤的行时，用户可以通过如下语句

         ```sql
         show load where label="xxx";
         ```

         返回结果中的 URL 可以用于查询错误的数据，具体见后面 **查看错误行** 小结。

         **数据不可见是一个临时状态，这批数据最终是一定可见的**

         可以通过如下语句查看这批数据的可见状态：

         ```sql
         show transaction where id=4005;
         ```

         返回结果中的 `TransactionStatus` 列如果为 `visible`，则表述数据可见。

   3. 执行失败

      执行失败表示没有任何数据被成功导入，并返回如下：

      ```sql
      mysql> insert into tbl1 select * from tbl2 where k1 = "a";
      ERROR 1064 (HY000): all partitions have no load data. url: http://10.74.167.16:8042/api/_load_error_log?file=__shard_2/error_log_insert_stmt_ba8bb9e158e4879-ae8de8507c0bf8a2_ba8bb9e158e4879_ae8de8507c0bf8a2
      ```

      其中 `ERROR 1064 (HY000): all partitions have no load data` 显示失败原因。后面的 url 可以用于查询错误的数据：

      ```sql
      show load warnings on "url";
      ```

      可以查看到具体错误行。

2. 超时时间

   <version since="dev"></version>
   INSERT 操作的超时时间由 [会话变量](../../../../advanced/variables.md) `insert_timeout` 控制。默认为4小时。超时则作业会被取消。

3. Label 和原子性

   INSERT 操作同样能够保证导入的原子性，可以参阅 [导入事务和原子性](../../../../data-operate/import/import-scenes/load-atomicity.md) 文档。

   当需要使用 `CTE(Common Table Expressions)` 作为 insert 操作中的查询部分时，必须指定 `WITH LABEL` 和 `column` 部分。

4. 过滤阈值

   与其他导入方式不同，INSERT 操作不能指定过滤阈值（`max_filter_ratio`）。默认的过滤阈值为 1，即素有错误行都可以被忽略。

   对于有要求数据不能够被过滤的业务场景，可以通过设置 [会话变量](../../../../advanced/variables.md) `enable_insert_strict` 为 `true` 来确保当有数据被过滤掉的时候，`INSERT` 不会被执行成功。

5. 性能问题

   不建议使用 `VALUES` 方式进行单行的插入。如果必须这样使用，请将多行数据合并到一个 INSERT 语句中进行批量提交。
