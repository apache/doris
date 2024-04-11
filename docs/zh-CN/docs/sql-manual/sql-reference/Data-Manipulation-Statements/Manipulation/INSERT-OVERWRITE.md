---
{
    "title": "INSERT-OVERWRITE",
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

## INSERT OVERWRITE

### Name

INSERT OVERWRITE

### Description

该语句的功能是重写表或表的某些分区

```sql
INSERT OVERWRITE table table_name
    [ PARTITION (p1, ... | *) ]
    [ WITH LABEL label]
    [ (column [, ...]) ]
    [ [ hint [, ...] ] ]
    { VALUES ( { expression | DEFAULT } [, ...] ) [, ...] | query }
```

 Parameters

> table_name: 需要重写的目的表。这个表必须存在。可以是 `db_name.table_name` 形式
>
> partitions: 需要重写的目标分区，支持两种形式：
>
>> 1. 分区名。必须是 `table_name` 中存在的分区，多个分区名称用逗号分隔。
>> 2. 星号(*)。开启[自动检测分区](#overwrite-auto-detect-partition)功能。写入操作将会自动检测数据所涉及的分区，并覆写这些分区。
>
> label: 为 Insert 任务指定一个 label
>
> column_name: 指定的目的列，必须是 `table_name` 中存在的列
>
> expression: 需要赋值给某个列的对应表达式
>
> DEFAULT: 让对应列使用默认值
>
> query: 一个普通查询，查询的结果会重写到目标中
>
> hint: 用于指示 `INSERT` 执行行为的一些指示符。目前 hint 有三个可选值`/*+ STREAMING */`、`/*+ SHUFFLE */`或`/*+ NOSHUFFLE */`
>
> 1. STREAMING：目前无实际作用，只是为了兼容之前的版本，因此保留。（之前的版本加上这个 hint 会返回 label，现在默认都会返回 label）
> 2. SHUFFLE：当目标表是分区表，开启这个 hint 会进行 repartiiton。
> 3. NOSHUFFLE：即使目标表是分区表，也不会进行 repartiiton，但会做一些其他操作以保证数据正确落到各个分区中。

注意：

1. 在当前版本中，会话变量 `enable_insert_strict` 默认为 `true`，如果执行 `INSERT OVERWRITE` 语句时，对于有不符合目标表格式的数据被过滤掉的话会重写目标表失败（比如重写分区时，不满足所有分区条件的数据会被过滤）。
2. 如果INSERT OVERWRITE的目标表是[AUTO-PARTITION表](../../../../advanced/partition/auto-partition)，若未指定PARTITION（重写整表），那么可以创建新的分区。如果指定了覆写的PARTITION（包括通过 `partition(*)` 语法自动检测并覆盖分区），那么在此过程中，AUTO PARTITION表表现得如同普通分区表一样，不满足现有分区条件的数据将被过滤，而非创建新的分区。
3. INSERT OVERWRITE语句会首先创建一个新表，将需要重写的数据插入到新表中，最后原子性的用新表替换旧表并修改名称。因此，在重写表的过程中，旧表中的数据在重写完毕之前仍然可以正常访问。

### Example

假设有`test` 表。该表包含两个列`c1`, `c2`，两个分区`p1`,`p2`。建表语句如下所示

```sql
CREATE TABLE IF NOT EXISTS test (
  `c1` int NOT NULL DEFAULT "1",
  `c2` int NOT NULL DEFAULT "4"
) ENGINE=OLAP
UNIQUE KEY(`c1`)
PARTITION BY LIST (`c1`)
(
PARTITION p1 VALUES IN ("1","2","3"),# 分区p1只允许1 2 3存在
PARTITION p2 VALUES IN ("4","5","6") # 分区p2只允许1 5 6存在
)
DISTRIBUTED BY HASH(`c1`) BUCKETS 3
PROPERTIES (
  "replication_allocation" = "tag.location.default: 1",
  "in_memory" = "false",
  "storage_format" = "V2"
);
```

#### Overwrite Table

1. VALUES的形式重写`test`表

    ```sql
    # 单行重写
    INSERT OVERWRITE table test VALUES (1, 2);
    INSERT OVERWRITE table test (c1, c2) VALUES (1, 2);
    INSERT OVERWRITE table test (c1, c2) VALUES (1, DEFAULT);
    INSERT OVERWRITE table test (c1) VALUES (1);
    # 多行重写
    INSERT OVERWRITE table test VALUES (1, 2), (3, 2 + 2);
    INSERT OVERWRITE table test (c1, c2) VALUES (1, 2), (3, 2 * 2);
    INSERT OVERWRITE table test (c1, c2) VALUES (1, DEFAULT), (3, DEFAULT);
    INSERT OVERWRITE table test (c1) VALUES (1), (3);
    ```

- 第一条语句和第二条语句的效果一致，重写时如果不指定目标列，会使用表中的列顺序来作为默认的目标列。重写成功后表`test`中只有一行数据。
- 第三条语句和第四条语句的效果一致，没有指定的列`c2`会使用默认值4来完成数据重写。重写成功后表`test`中只有一行数据。
- 第五条语句和第六条语句的效果一致，在语句中可以使用表达式（如`2+2`，`2*2`），执行语句的时候会计算出表达式的结果再重写表`test`。重写成功后表`test`中有两行数据。

- 第七条语句和第八条语句的效果一致，没有指定的列`c2`会使用默认值4来完成数据重写。重写成功后表`test`中有两行数据。

2. 查询语句的形式重写`test`表，表`test2`和表`test`的数据格式需要保持一致，如果不一致会触发数据类型的隐式转换

    ```sql
    INSERT OVERWRITE table test SELECT * FROM test2;
    INSERT OVERWRITE table test (c1, c2) SELECT * from test2;
    ```

- 第一条语句和第二条语句的效果一致，该语句的作用是将数据从表`test2`中取出，使用取出的数据重写表`test`。重写成功后表`test`中的数据和表`test2`中的数据保持一致。

3. 重写 `test` 表并指定label

    ```sql
    INSERT OVERWRITE table test WITH LABEL `label1` SELECT * FROM test2;
    INSERT OVERWRITE table test WITH LABEL `label2` (c1, c2) SELECT * from test2;
    ```

- 使用label会将此任务封装成一个**异步任务**，执行语句之后，相关操作都会异步执行，用户可以通过`SHOW LOAD;`命令查看此`label`导入作业的状态。需要注意的是label具有唯一性。


#### Overwrite Table Partition

使用 INSERT OVERWRITE 重写分区时，实际我们是将如下三步操作封装为一个事务并执行，如果中途失败，已进行的操作将会回滚：
1. 假设指定重写分区 p1，首先创建一个与重写的目标分区结构相同的空临时分区 `pTMP`
2. 向 `pTMP` 中写入数据
3. 使用 `pTMP` 原子替换 `p1` 分区

举例如下：

1. VALUES的形式重写`test`表分区`P1`和`p2`

    ```sql
    # 单行重写
    INSERT OVERWRITE table test PARTITION(p1,p2) VALUES (1, 2);
    INSERT OVERWRITE table test PARTITION(p1,p2) (c1, c2) VALUES (1, 2);
    INSERT OVERWRITE table test PARTITION(p1,p2) (c1, c2) VALUES (1, DEFAULT);
    INSERT OVERWRITE table test PARTITION(p1,p2) (c1) VALUES (1);
    # 多行重写
    INSERT OVERWRITE table test PARTITION(p1,p2) VALUES (1, 2), (4, 2 + 2);
    INSERT OVERWRITE table test PARTITION(p1,p2) (c1, c2) VALUES (1, 2), (4, 2 * 2);
    INSERT OVERWRITE table test PARTITION(p1,p2) (c1, c2) VALUES (1, DEFAULT), (4, DEFAULT);
    INSERT OVERWRITE table test PARTITION(p1,p2) (c1) VALUES (1), (4);
    ```

    以上语句与重写表不同的是，它们都是重写表中的分区。分区可以一次重写一个分区也可以一次重写多个分区。需要注意的是，只有满足对应分区过滤条件的数据才能够重写成功。如果重写的数据中有数据不满足其中任意一个分区，那么本次重写会失败。一个失败的例子如下所示

    ```sql
    INSERT OVERWRITE table test PARTITION(p1,p2) VALUES (7, 2);
    ```

    以上语句重写的数据`c1=7`分区`p1`和`p2`的条件都不满足，因此会重写失败。

2. 查询语句的形式重写`test`表分区`P1`和`p2`，表`test2`和表`test`的数据格式需要保持一致，如果不一致会触发数据类型的隐式转换

    ```sql
    INSERT OVERWRITE table test PARTITION(p1,p2) SELECT * FROM test2;
    INSERT OVERWRITE table test PARTITION(p1,p2) (c1, c2) SELECT * from test2;
    ```

3. 重写 `test` 表分区`P1`和`p2`并指定label

    ```sql
    INSERT OVERWRITE table test PARTITION(p1,p2) WITH LABEL `label3` SELECT * FROM test2;
    INSERT OVERWRITE table test PARTITION(p1,p2) WITH LABEL `label4` (c1, c2) SELECT * from test2;
    ```

#### Overwrite Auto Detect Partition

当 INSERT OVERWRITE 命令指定的 PARTITION 子句为 `PARTITION(*)` 时，此次覆写将会自动检测分区数据所在的分区。例如：

```sql
mysql> create table test(
    -> k0 int null
    -> )
    -> partition by range (k0)
    -> (
    -> PARTITION p10 values less than (10),
    -> PARTITION p100 values less than (100),
    -> PARTITION pMAX values less than (maxvalue)
    -> )
    -> DISTRIBUTED BY HASH(`k0`) BUCKETS 1
    -> properties("replication_num" = "1");
Query OK, 0 rows affected (0.11 sec)

mysql> insert into test values (1), (2), (15), (100), (200);
Query OK, 5 rows affected (0.29 sec)

mysql> select * from test order by k0;
+------+
| k0   |
+------+
|    1 |
|    2 |
|   15 |
|  100 |
|  200 |
+------+
5 rows in set (0.23 sec)

mysql> insert overwrite table test partition(*) values (3), (1234);
Query OK, 2 rows affected (0.24 sec)

mysql> select * from test order by k0;
+------+
| k0   |
+------+
|    3 |
|   15 |
| 1234 |
+------+
3 rows in set (0.20 sec)
```

可以看到，数据 3、1234 所在的分区 `p10` 和 `pMAX` 中的全部数据均被覆写，而 `p100` 分区未发生变化。该操作可以理解为 INSERT OVERWRITE 操作时通过 PARTITION 子句指定覆写特定分区的语法糖，它的实现原理与[指定重写特定分区](#overwrite-table-partition)相同。通过 `PARTITION(*)` 的语法，在覆写大量分区数据时我们可以免于手动填写全部分区名的繁琐。

### Keywords

    INSERT OVERWRITE, OVERWRITE, AUTO DETECT

