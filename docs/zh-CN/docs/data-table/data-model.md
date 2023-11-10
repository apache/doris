---
{
    "title": "数据模型",
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

# 数据模型

本文档主要从逻辑层面，描述 Doris 的数据模型，以帮助用户更好的使用 Doris 应对不同的业务场景。

## 基本概念

在 Doris 中，数据以表（Table）的形式进行逻辑上的描述。
一张表包括行（Row）和列（Column）。Row 即用户的一行数据。Column 用于描述一行数据中不同的字段。

Column 可以分为两大类：Key 和 Value。从业务角度看，Key 和 Value 可以分别对应维度列和指标列。Doris的key列是建表语句中指定的列，建表语句中的关键字'unique key'或'aggregate key'或'duplicate key'后面的列就是 Key 列，除了 Key 列剩下的就是 Value 列。

Doris 的数据模型主要分为3类:

- Aggregate
- Unique
- Duplicate

下面我们分别介绍。

## Aggregate 模型

我们以实际的例子来说明什么是聚合模型，以及如何正确的使用聚合模型。

### 示例1：导入数据聚合

假设业务有如下数据表模式：

| ColumnName      | Type        | AggregationType | Comment    |
|-----------------|-------------|-----------------|------------|
| user_id         | LARGEINT    |                 | 用户id       |
| date            | DATE        |                 | 数据灌入日期     |
| city            | VARCHAR(20) |                 | 用户所在城市     |
| age             | SMALLINT    |                 | 用户年龄       |
| sex             | TINYINT     |                 | 用户性别       |
| last_visit_date | DATETIME    | REPLACE         | 用户最后一次访问时间 |
| cost            | BIGINT      | SUM             | 用户总消费      |
| max_dwell_time  | INT         | MAX             | 用户最大停留时间   |
| min_dwell_time  | INT         | MIN             | 用户最小停留时间   |

如果转换成建表语句则如下（省略建表语句中的 Partition 和 Distribution 信息）

```sql
CREATE DATABASE IF NOT EXISTS example_db;

CREATE TABLE IF NOT EXISTS example_db.example_tbl_agg1
(
    `user_id` LARGEINT NOT NULL COMMENT "用户id",
    `date` DATE NOT NULL COMMENT "数据灌入日期时间",
    `city` VARCHAR(20) COMMENT "用户所在城市",
    `age` SMALLINT COMMENT "用户年龄",
    `sex` TINYINT COMMENT "用户性别",
    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
    `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
)
AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
```

可以看到，这是一个典型的用户信息和访问行为的事实表。
在一般星型模型中，用户信息和访问行为一般分别存放在维度表和事实表中。这里我们为了更加方便的解释 Doris 的数据模型，将两部分信息统一存放在一张表中。

表中的列按照是否设置了 `AggregationType`，分为 Key (维度列) 和 Value（指标列）。没有设置 `AggregationType` 的，如 `user_id`、`date`、`age` ... 等称为 **Key**，而设置了 `AggregationType` 的称为 **Value**。

当我们导入数据时，对于 Key 列相同的行会聚合成一行，而 Value 列会按照设置的 `AggregationType` 进行聚合。 `AggregationType` 目前有以下几种聚合方式和agg_state：

1. SUM：求和，多行的 Value 进行累加。
2. REPLACE：替代，下一批数据中的 Value 会替换之前导入过的行中的 Value。
3. MAX：保留最大值。
4. MIN：保留最小值。
5. REPLACE_IF_NOT_NULL：非空值替换。和 REPLACE 的区别在于对于null值，不做替换。
6. HLL_UNION：HLL 类型的列的聚合方式，通过 HyperLogLog 算法聚合。
7. BITMAP_UNION：BIMTAP 类型的列的聚合方式，进行位图的并集聚合。

如果这几种聚合方式无法满足需求，则可以选择使用agg_state类型。


假设我们有以下导入数据（原始数据）：

| user_id | date       | city | age | sex | last_visit_date     | cost | max_dwell_time | min_dwell_time |
|---------|------------|------|-----|-----|---------------------|------|----------------|----------------|
| 10000   | 2017-10-01 | 北京   | 20  | 0   | 2017-10-01 06:00:00 | 20   | 10             | 10             |
| 10000   | 2017-10-01 | 北京   | 20  | 0   | 2017-10-01 07:00:00 | 15   | 2              | 2              |
| 10001   | 2017-10-01 | 北京   | 30  | 1   | 2017-10-01 17:05:45 | 2    | 22             | 22             |
| 10002   | 2017-10-02 | 上海   | 20  | 1   | 2017-10-02 12:59:12 | 200  | 5              | 5              |
| 10003   | 2017-10-02 | 广州   | 32  | 0   | 2017-10-02 11:20:00 | 30   | 11             | 11             |
| 10004   | 2017-10-01 | 深圳   | 35  | 0   | 2017-10-01 10:00:15 | 100  | 3              | 3              |
| 10004   | 2017-10-03 | 深圳   | 35  | 0   | 2017-10-03 10:20:22 | 11   | 6              | 6              |

通过sql导入数据：

```sql
insert into example_db.example_tbl_agg1 values
(10000,"2017-10-01","北京",20,0,"2017-10-01 06:00:00",20,10,10),
(10000,"2017-10-01","北京",20,0,"2017-10-01 07:00:00",15,2,2),
(10001,"2017-10-01","北京",30,1,"2017-10-01 17:05:45",2,22,22),
(10002,"2017-10-02","上海",20,1,"2017-10-02 12:59:12",200,5,5),
(10003,"2017-10-02","广州",32,0,"2017-10-02 11:20:00",30,11,11),
(10004,"2017-10-01","深圳",35,0,"2017-10-01 10:00:15",100,3,3),
(10004,"2017-10-03","深圳",35,0,"2017-10-03 10:20:22",11,6,6);
```

我们假设这是一张记录用户访问某商品页面行为的表。我们以第一行数据为例，解释如下：

| 数据                  | 说明                  |
|---------------------|---------------------|
| 10000               | 用户id，每个用户唯一识别id     |
| 2017-10-01          | 数据入库时间，精确到日期        |
| 北京                  | 用户所在城市              |
| 20                  | 用户年龄                |
| 0                   | 性别男（1 代表女性）         |
| 2017-10-01 06:00:00 | 用户本次访问该页面的时间，精确到秒   |
| 20                  | 用户本次访问产生的消费         |
| 10                  | 用户本次访问，驻留该页面的时间     |
| 10                  | 用户本次访问，驻留该页面的时间（冗余） |

那么当这批数据正确导入到 Doris 中后，Doris 中最终存储如下：

| user_id | date       | city | age | sex | last_visit_date     | cost | max_dwell_time | min_dwell_time |
|---------|------------|------|-----|-----|---------------------|------|----------------|----------------|
| 10000   | 2017-10-01 | 北京   | 20  | 0   | 2017-10-01 07:00:00 | 35   | 10             | 2              |
| 10001   | 2017-10-01 | 北京   | 30  | 1   | 2017-10-01 17:05:45 | 2    | 22             | 22             |
| 10002   | 2017-10-02 | 上海   | 20  | 1   | 2017-10-02 12:59:12 | 200  | 5              | 5              |
| 10003   | 2017-10-02 | 广州   | 32  | 0   | 2017-10-02 11:20:00 | 30   | 11             | 11             |
| 10004   | 2017-10-01 | 深圳   | 35  | 0   | 2017-10-01 10:00:15 | 100  | 3              | 3              |
| 10004   | 2017-10-03 | 深圳   | 35  | 0   | 2017-10-03 10:20:22 | 11   | 6              | 6              |

可以看到，用户 10000 只剩下了一行**聚合后**的数据。而其余用户的数据和原始数据保持一致。这里先解释下用户 10000 聚合后的数据：

前5列没有变化，从第6列 `last_visit_date` 开始：

- `2017-10-01 07:00:00`：因为 `last_visit_date` 列的聚合方式为 REPLACE，所以 `2017-10-01 07:00:00` 替换了 `2017-10-01 06:00:00` 保存了下来。

  > 注：在同一个导入批次中的数据，对于 REPLACE 这种聚合方式，替换顺序不做保证。如在这个例子中，最终保存下来的，也有可能是 `2017-10-01 06:00:00`。而对于不同导入批次中的数据，可以保证，后一批次的数据会替换前一批次。

- `35`：因为 `cost` 列的聚合类型为 SUM，所以由 20 + 15 累加获得 35。

- `10`：因为 `max_dwell_time` 列的聚合类型为 MAX，所以 10 和 2 取最大值，获得 10。

- `2`：因为 `min_dwell_time` 列的聚合类型为 MIN，所以 10 和 2 取最小值，获得 2。

经过聚合，Doris 中最终只会存储聚合后的数据。换句话说，即明细数据会丢失，用户不能够再查询到聚合前的明细数据了。

### 示例2：保留明细数据

接示例1，我们将表结构修改如下：

| ColumnName      | Type        | AggregationType | Comment     |
|-----------------|-------------|-----------------|-------------|
| user_id         | LARGEINT    |                 | 用户id        |
| date            | DATE        |                 | 数据灌入日期      |
| timestamp       | DATETIME    |                 | 数据灌入时间，精确到秒 |
| city            | VARCHAR(20) |                 | 用户所在城市      |
| age             | SMALLINT    |                 | 用户年龄        |
| sex             | TINYINT     |                 | 用户性别        |
| last_visit_date | DATETIME    | REPLACE         | 用户最后一次访问时间  |
| cost            | BIGINT      | SUM             | 用户总消费       |
| max_dwell_time  | INT         | MAX             | 用户最大停留时间    |
| min_dwell_time  | INT         | MIN             | 用户最小停留时间    |

即增加了一列 `timestamp`，记录精确到秒的数据灌入时间。
同时，将`AGGREGATE KEY`设置为`AGGREGATE KEY(user_id, date, timestamp, city, age, sex)`

```sql
CREATE TABLE IF NOT EXISTS example_db.example_tbl_agg2
(
    `user_id` LARGEINT NOT NULL COMMENT "用户id",
    `date` DATE NOT NULL COMMENT "数据灌入日期时间",
	`timestamp` DATETIME NOT NULL COMMENT "数据灌入日期时间戳",
    `city` VARCHAR(20) COMMENT "用户所在城市",
    `age` SMALLINT COMMENT "用户年龄",
    `sex` TINYINT COMMENT "用户性别",
    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
    `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
)
AGGREGATE KEY(`user_id`, `date`, `timestamp` ,`city`, `age`, `sex`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
```

导入数据如下：

| user_id | date       | timestamp           | city | age | sex | last_visit_date     | cost | max_dwell_time | min_dwell_time |
|---------|------------|---------------------|------|-----|-----|---------------------|------|----------------|----------------|
| 10000   | 2017-10-01 | 2017-10-01 08:00:05 | 北京   | 20  | 0   | 2017-10-01 06:00:00 | 20   | 10             | 10             |
| 10000   | 2017-10-01 | 2017-10-01 09:00:05 | 北京   | 20  | 0   | 2017-10-01 07:00:00 | 15   | 2              | 2              |
| 10001   | 2017-10-01 | 2017-10-01 18:12:10 | 北京   | 30  | 1   | 2017-10-01 17:05:45 | 2    | 22             | 22             |
| 10002   | 2017-10-02 | 2017-10-02 13:10:00 | 上海   | 20  | 1   | 2017-10-02 12:59:12 | 200  | 5              | 5              |
| 10003   | 2017-10-02 | 2017-10-02 13:15:00 | 广州   | 32  | 0   | 2017-10-02 11:20:00 | 30   | 11             | 11             |
| 10004   | 2017-10-01 | 2017-10-01 12:12:48 | 深圳   | 35  | 0   | 2017-10-01 10:00:15 | 100  | 3              | 3              |
| 10004   | 2017-10-03 | 2017-10-03 12:38:20 | 深圳   | 35  | 0   | 2017-10-03 10:20:22 | 11   | 6              | 6              |

通过sql导入数据：

```sql
insert into example_db.example_tbl_agg2 values
(10000,"2017-10-01","2017-10-01 08:00:05","北京",20,0,"2017-10-01 06:00:00",20,10,10),
(10000,"2017-10-01","2017-10-01 09:00:05","北京",20,0,"2017-10-01 07:00:00",15,2,2),
(10001,"2017-10-01","2017-10-01 18:12:10","北京",30,1,"2017-10-01 17:05:45",2,22,22),
(10002,"2017-10-02","2017-10-02 13:10:00","上海",20,1,"2017-10-02 12:59:12",200,5,5),
(10003,"2017-10-02","2017-10-02 13:15:00","广州",32,0,"2017-10-02 11:20:00",30,11,11),
(10004,"2017-10-01","2017-10-01 12:12:48","深圳",35,0,"2017-10-01 10:00:15",100,3,3),
(10004,"2017-10-03","2017-10-03 12:38:20","深圳",35,0,"2017-10-03 10:20:22",11,6,6);
```

那么当这批数据正确导入到 Doris 中后，Doris 中最终存储如下：

| user_id | date       | timestamp           | city | age | sex | last_visit_date     | cost | max_dwell_time | min_dwell_time |
|---------|------------|---------------------|------|-----|-----|---------------------|------|----------------|----------------|
| 10000   | 2017-10-01 | 2017-10-01 08:00:05 | 北京   | 20  | 0   | 2017-10-01 06:00:00 | 20   | 10             | 10             |
| 10000   | 2017-10-01 | 2017-10-01 09:00:05 | 北京   | 20  | 0   | 2017-10-01 07:00:00 | 15   | 2              | 2              |
| 10001   | 2017-10-01 | 2017-10-01 18:12:10 | 北京   | 30  | 1   | 2017-10-01 17:05:45 | 2    | 22             | 22             |
| 10002   | 2017-10-02 | 2017-10-02 13:10:00 | 上海   | 20  | 1   | 2017-10-02 12:59:12 | 200  | 5              | 5              |
| 10003   | 2017-10-02 | 2017-10-02 13:15:00 | 广州   | 32  | 0   | 2017-10-02 11:20:00 | 30   | 11             | 11             |
| 10004   | 2017-10-01 | 2017-10-01 12:12:48 | 深圳   | 35  | 0   | 2017-10-01 10:00:15 | 100  | 3              | 3              |
| 10004   | 2017-10-03 | 2017-10-03 12:38:20 | 深圳   | 35  | 0   | 2017-10-03 10:20:22 | 11   | 6              | 6              |

我们可以看到，存储的数据，和导入数据完全一样，没有发生任何聚合。这是因为，这批数据中，因为加入了 `timestamp` 列，所有行的 Key 都**不完全相同**。也就是说，只要保证导入的数据中，每一行的 Key 都不完全相同，那么即使在聚合模型下，Doris 也可以保存完整的明细数据。

### 示例3：导入数据与已有数据聚合

接示例1。假设现在表中已有数据如下：

| user_id | date       | city | age | sex | last_visit_date     | cost | max_dwell_time | min_dwell_time |
|---------|------------|------|-----|-----|---------------------|------|----------------|----------------|
| 10000   | 2017-10-01 | 北京   | 20  | 0   | 2017-10-01 07:00:00 | 35   | 10             | 2              |
| 10001   | 2017-10-01 | 北京   | 30  | 1   | 2017-10-01 17:05:45 | 2    | 22             | 22             |
| 10002   | 2017-10-02 | 上海   | 20  | 1   | 2017-10-02 12:59:12 | 200  | 5              | 5              |
| 10003   | 2017-10-02 | 广州   | 32  | 0   | 2017-10-02 11:20:00 | 30   | 11             | 11             |
| 10004   | 2017-10-01 | 深圳   | 35  | 0   | 2017-10-01 10:00:15 | 100  | 3              | 3              |
| 10004   | 2017-10-03 | 深圳   | 35  | 0   | 2017-10-03 10:20:22 | 11   | 6              | 6              |

我们再导入一批新的数据：

| user_id | date       | city | age | sex | last_visit_date     | cost | max_dwell_time | min_dwell_time |
|---------|------------|------|-----|-----|---------------------|------|----------------|----------------|
| 10004   | 2017-10-03 | 深圳   | 35  | 0   | 2017-10-03 11:22:00 | 44   | 19             | 19             |
| 10005   | 2017-10-03 | 长沙   | 29  | 1   | 2017-10-03 18:11:02 | 3    | 1              | 1              |

通过sql导入数据：

```sql
insert into example_db.example_tbl_agg1 values
(10004,"2017-10-03","深圳",35,0,"2017-10-03 11:22:00",44,19,19),
(10005,"2017-10-03","长沙",29,1,"2017-10-03 18:11:02",3,1,1);
```

那么当这批数据正确导入到 Doris 中后，Doris 中最终存储如下：

| user_id | date       | city | age | sex | last_visit_date     | cost | max_dwell_time | min_dwell_time |
|---------|------------|------|-----|-----|---------------------|------|----------------|----------------|
| 10000   | 2017-10-01 | 北京   | 20  | 0   | 2017-10-01 07:00:00 | 35   | 10             | 2              |
| 10001   | 2017-10-01 | 北京   | 30  | 1   | 2017-10-01 17:05:45 | 2    | 22             | 22             |
| 10002   | 2017-10-02 | 上海   | 20  | 1   | 2017-10-02 12:59:12 | 200  | 5              | 5              |
| 10003   | 2017-10-02 | 广州   | 32  | 0   | 2017-10-02 11:20:00 | 30   | 11             | 11             |
| 10004   | 2017-10-01 | 深圳   | 35  | 0   | 2017-10-01 10:00:15 | 100  | 3              | 3              |
| 10004   | 2017-10-03 | 深圳   | 35  | 0   | 2017-10-03 11:22:00 | 55   | 19             | 6              |
| 10005   | 2017-10-03 | 长沙   | 29  | 1   | 2017-10-03 18:11:02 | 3    | 1              | 1              |

可以看到，用户 10004 的已有数据和新导入的数据发生了聚合。同时新增了 10005 用户的数据。

数据的聚合，在 Doris 中有如下三个阶段发生：

1. 每一批次数据导入的 ETL 阶段。该阶段会在每一批次导入的数据内部进行聚合。
2. 底层 BE 进行数据 Compaction 的阶段。该阶段，BE 会对已导入的不同批次的数据进行进一步的聚合。
3. 数据查询阶段。在数据查询时，对于查询涉及到的数据，会进行对应的聚合。

数据在不同时间，可能聚合的程度不一致。比如一批数据刚导入时，可能还未与之前已存在的数据进行聚合。但是对于用户而言，用户**只能查询到**聚合后的数据。即不同的聚合程度对于用户查询而言是透明的。用户需始终认为数据以**最终的完成的聚合程度**存在，而**不应假设某些聚合还未发生**。（可参阅**聚合模型的局限性**一节获得更多详情。）

### agg_state

    AGG_STATE不能作为key列使用，建表时需要同时声明聚合函数的签名。
    用户不需要指定长度和默认值。实际存储的数据大小与函数实现有关。

建表

```sql
set enable_agg_state=true;
create table aggstate(
    k1 int null,
    k2 agg_state sum(int),
    k3 agg_state group_concat(string)
)
aggregate key (k1)
distributed BY hash(k1) buckets 3
properties("replication_num" = "1");
```

其中agg_state用于声明数据类型为agg_state，sum/group_concat为聚合函数的签名。
注意agg_state是一种数据类型，同int/array/string

agg_state只能配合[state](../sql-manual/sql-functions/combinators/state.md)
    /[merge](../sql-manual/sql-functions/combinators/merge.md)/[union](../sql-manual/sql-functions/combinators/union.md)函数组合器使用。

agg_state是聚合函数的中间结果，例如，聚合函数sum ， 则agg_state可以表示sum(1,2,3,4,5)的这个中间状态，而不是最终的结果。

agg_state类型需要使用state函数来生成，对于当前的这个表，则为`sum_state`,`group_concat_state`。

```sql
insert into aggstate values(1,sum_state(1),group_concat_state('a'));
insert into aggstate values(1,sum_state(2),group_concat_state('b'));
insert into aggstate values(1,sum_state(3),group_concat_state('c'));
```
此时表只有一行 ( 注意，下面的表只是示意图，不是真的可以select显示出来)

| k1 | k2         | k3                        |               
|----|------------|---------------------------| 
| 1  | sum(1,2,3) | group_concat_state(a,b,c) | 

再插入一条数据

```sql
insert into aggstate values(2,sum_state(4),group_concat_state('d'));
```

此时表的结构为

| k1 | k2         | k3                        |               
|----|------------|---------------------------| 
| 1  | sum(1,2,3) | group_concat_state(a,b,c) | 
| 2  | sum(4)     | group_concat_state(d)     |

我们可以通过merge操作来合并多个state，并且返回最终聚合函数计算的结果

```
mysql> select sum_merge(k2) from aggstate;
+---------------+
| sum_merge(k2) |
+---------------+
|            10 |
+---------------+
```

`sum_merge` 会先把sum(1,2,3) 和 sum(4) 合并成 sum(1,2,3,4) ，并返回计算的结果。
因为group_concat对于顺序有要求，所以结果是不稳定的。

```
mysql> select group_concat_merge(k3) from aggstate;
+------------------------+
| group_concat_merge(k3) |
+------------------------+
| c,b,a,d                |
+------------------------+
```

如果不想要聚合的最终结果，可以使用union来合并多个聚合的中间结果，生成一个新的中间结果。

```sql
insert into aggstate select 3,sum_union(k2),group_concat_union(k3) from aggstate ;
```
此时的表结构为

| k1 | k2           | k3                          |               
|----|--------------|-----------------------------| 
| 1  | sum(1,2,3)   | group_concat_state(a,b,c)   | 
| 2  | sum(4)       | group_concat_state(d)       |
| 3  | sum(1,2,3,4) | group_concat_state(a,b,c,d) |

可以通过查询

```
mysql> select sum_merge(k2) , group_concat_merge(k3)from aggstate;
+---------------+------------------------+
| sum_merge(k2) | group_concat_merge(k3) |
+---------------+------------------------+
|            20 | c,b,a,d,c,b,a,d        |
+---------------+------------------------+

mysql> select sum_merge(k2) , group_concat_merge(k3)from aggstate where k1 != 2;
+---------------+------------------------+
| sum_merge(k2) | group_concat_merge(k3) |
+---------------+------------------------+
|            16 | c,b,a,d,c,b,a          |
+---------------+------------------------+
```

用户可以通过agg_state做出跟细致的聚合函数操作。

注意 agg_state 存在一定的性能开销

## Unique 模型

在某些多维分析场景下，用户更关注的是如何保证 Key 的唯一性，即如何获得 Primary Key 唯一性约束。
因此，我们引入了 Unique 数据模型。在1.2版本之前，该模型本质上是聚合模型的一个特例，也是一种简化的表结构表示方式。
由于聚合模型的实现方式是读时合并（merge on read)，因此在一些聚合查询上性能不佳（参考后续章节[聚合模型的局限性](#聚合模型的局限性)的描述），
在1.2版本我们引入了Unique模型新的实现方式，写时合并（merge on write），通过在写入时做一些额外的工作，实现了最优的查询性能。
写时合并将在未来替换读时合并成为Unique模型的默认实现方式，两者将会短暂的共存一段时间。下面将对两种实现方式分别举例进行说明。

### 读时合并（与聚合模型相同的实现方式）

| ColumnName    | Type         | IsKey | Comment |
|---------------|--------------|-------|---------|
| user_id       | BIGINT       | Yes   | 用户id    |
| username      | VARCHAR(50)  | Yes   | 用户昵称    |
| city          | VARCHAR(20)  | No    | 用户所在城市  |
| age           | SMALLINT     | No    | 用户年龄    |
| sex           | TINYINT      | No    | 用户性别    |
| phone         | LARGEINT     | No    | 用户电话    |
| address       | VARCHAR(500) | No    | 用户住址    |
| register_time | DATETIME     | No    | 用户注册时间  |

这是一个典型的用户基础信息表。这类数据没有聚合需求，只需保证主键唯一性。（这里的主键为 user_id + username）。那么我们的建表语句如下：

```sql
CREATE TABLE IF NOT EXISTS example_db.example_tbl_unique
(
    `user_id` LARGEINT NOT NULL COMMENT "用户id",
    `username` VARCHAR(50) NOT NULL COMMENT "用户昵称",
    `city` VARCHAR(20) COMMENT "用户所在城市",
    `age` SMALLINT COMMENT "用户年龄",
    `sex` TINYINT COMMENT "用户性别",
    `phone` LARGEINT COMMENT "用户电话",
    `address` VARCHAR(500) COMMENT "用户地址",
    `register_time` DATETIME COMMENT "用户注册时间"
)
UNIQUE KEY(`user_id`, `username`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
```

而这个表结构，完全同等于以下使用聚合模型描述的表结构：

| ColumnName    | Type         | AggregationType | Comment |
|---------------|--------------|-----------------|---------|
| user_id       | BIGINT       |                 | 用户id    |
| username      | VARCHAR(50)  |                 | 用户昵称    |
| city          | VARCHAR(20)  | REPLACE         | 用户所在城市  |
| age           | SMALLINT     | REPLACE         | 用户年龄    |
| sex           | TINYINT      | REPLACE         | 用户性别    |
| phone         | LARGEINT     | REPLACE         | 用户电话    |
| address       | VARCHAR(500) | REPLACE         | 用户住址    |
| register_time | DATETIME     | REPLACE         | 用户注册时间  |

及建表语句：

```sql
CREATE TABLE IF NOT EXISTS example_db.example_tbl_agg3
(
    `user_id` LARGEINT NOT NULL COMMENT "用户id",
    `username` VARCHAR(50) NOT NULL COMMENT "用户昵称",
    `city` VARCHAR(20) REPLACE COMMENT "用户所在城市",
    `age` SMALLINT REPLACE COMMENT "用户年龄",
    `sex` TINYINT REPLACE COMMENT "用户性别",
    `phone` LARGEINT REPLACE COMMENT "用户电话",
    `address` VARCHAR(500) REPLACE COMMENT "用户地址",
    `register_time` DATETIME REPLACE COMMENT "用户注册时间"
)
AGGREGATE KEY(`user_id`, `username`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
```

即Unique 模型的读时合并实现完全可以用聚合模型中的 REPLACE 方式替代。其内部的实现方式和数据存储方式也完全一样。这里不再继续举例说明。

<version since="1.2">

### 写时合并

Unique模型的写时合并实现，与聚合模型就是完全不同的两种模型了，查询性能更接近于duplicate模型，在有主键约束需求的场景上相比聚合模型有较大的查询性能优势，尤其是在聚合查询以及需要用索引过滤大量数据的查询中。

在 1.2.0 版本中，作为一个新的feature，写时合并默认关闭，用户可以通过添加下面的property来开启

```
"enable_unique_key_merge_on_write" = "true"
```
> 注意：
> 1. 建议使用1.2.4及以上版本，该版本修复了一些bug和稳定性问题
> 2. 在be.conf中添加配置项：disable_storage_page_cache=false。不添加该配置项可能会对数据导入性能产生较大影响

仍然以上面的表为例，建表语句为

```sql
CREATE TABLE IF NOT EXISTS example_db.example_tbl_unique_merge_on_write
(
    `user_id` LARGEINT NOT NULL COMMENT "用户id",
    `username` VARCHAR(50) NOT NULL COMMENT "用户昵称",
    `city` VARCHAR(20) COMMENT "用户所在城市",
    `age` SMALLINT COMMENT "用户年龄",
    `sex` TINYINT COMMENT "用户性别",
    `phone` LARGEINT COMMENT "用户电话",
    `address` VARCHAR(500) COMMENT "用户地址",
    `register_time` DATETIME COMMENT "用户注册时间"
)
UNIQUE KEY(`user_id`, `username`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"enable_unique_key_merge_on_write" = "true"
);
```

使用这种建表语句建出来的表结构，与聚合模型就完全不同了：

| ColumnName    | Type         | AggregationType | Comment |
|---------------|--------------|-----------------|---------|
| user_id       | BIGINT       |                 | 用户id    |
| username      | VARCHAR(50)  |                 | 用户昵称    |
| city          | VARCHAR(20)  | NONE            | 用户所在城市  |
| age           | SMALLINT     | NONE            | 用户年龄    |
| sex           | TINYINT      | NONE            | 用户性别    |
| phone         | LARGEINT     | NONE            | 用户电话    |
| address       | VARCHAR(500) | NONE            | 用户住址    |
| register_time | DATETIME     | NONE            | 用户注册时间  |

在开启了写时合并选项的Unique表上，数据在导入阶段就会去将被覆盖和被更新的数据进行标记删除，同时将新的数据写入新的文件。在查询的时候，
所有被标记删除的数据都会在文件级别被过滤掉，读取出来的数据就都是最新的数据，消除掉了读时合并中的数据聚合过程，并且能够在很多情况下支持多种谓词的下推。因此在许多场景都能带来比较大的性能提升，尤其是在有聚合查询的情况下。

【注意】
1. 新的Merge-on-write实现默认关闭，且只能在建表时通过指定property的方式打开。
2. 旧的Merge-on-read的实现无法无缝升级到新版本的实现（数据组织方式完全不同），如果需要改为使用写时合并的实现版本，需要手动执行`insert into unique-mow-table select * from source table`.
3. 在Unique模型上独有的delete sign 和 sequence col，在写时合并的新版实现中仍可以正常使用，用法没有变化。

</version>

## Duplicate 模型

在某些多维分析场景下，数据既没有主键，也没有聚合需求。因此，我们引入 Duplicate 数据模型来满足这类需求。举例说明。

| ColumnName | Type          | SortKey | Comment |
|------------|---------------|---------|---------|
| timestamp  | DATETIME      | Yes     | 日志时间    |
| type       | INT           | Yes     | 日志类型    |
| error_code | INT           | Yes     | 错误码     |
| error_msg  | VARCHAR(1024) | No      | 错误详细信息  |
| op_id      | BIGINT        | No      | 负责人id   |
| op_time    | DATETIME      | No      | 处理时间    |

建表语句如下：

```sql
CREATE TABLE IF NOT EXISTS example_db.example_tbl_duplicate
(
    `timestamp` DATETIME NOT NULL COMMENT "日志时间",
    `type` INT NOT NULL COMMENT "日志类型",
    `error_code` INT COMMENT "错误码",
    `error_msg` VARCHAR(1024) COMMENT "错误详细信息",
    `op_id` BIGINT COMMENT "负责人id",
    `op_time` DATETIME COMMENT "处理时间"
)
DUPLICATE KEY(`timestamp`, `type`, `error_code`)
DISTRIBUTED BY HASH(`type`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1"
);
```

这种数据模型区别于 Aggregate 和 Unique 模型。数据完全按照导入文件中的数据进行存储，不会有任何聚合。即使两行数据完全相同，也都会保留。
而在建表语句中指定的 DUPLICATE KEY，只是用来指明底层数据按照那些列进行排序。（更贴切的名称应该为 “Sorted Column”，
这里取名 “DUPLICATE KEY” 只是用以明确表示所用的数据模型。关于 “Sorted Column”的更多解释，可以参阅[前缀索引](./index/index-overview.md)）。在 DUPLICATE KEY 的选择上，我们建议适当的选择前 2-4 列就可以。

这种数据模型适用于既没有聚合需求，又没有主键唯一性约束的原始数据的存储。更多使用场景，可参阅[聚合模型的局限性](#聚合模型的局限性)小节。

<version since="2.0">

### 无排序列 Duplicate 模型

当创建表的时候没有指定Unique、Aggregate或Duplicate时，会默认创建一个Duplicate模型的表，并自动指定排序列。

当用户并没有排序需求的时候，可以通过在表属性中配置：

```
"enable_duplicate_without_keys_by_default" = "true"
```

然后再创建默认模型的时候，就会不再指定排序列，也不会给该表创建前缀索引，以此减少在导入和存储上额外的开销。

建表语句如下：

```sql
CREATE TABLE IF NOT EXISTS example_db.example_tbl_duplicate_without_keys_by_default
(
    `timestamp` DATETIME NOT NULL COMMENT "日志时间",
    `type` INT NOT NULL COMMENT "日志类型",
    `error_code` INT COMMENT "错误码",
    `error_msg` VARCHAR(1024) COMMENT "错误详细信息",
    `op_id` BIGINT COMMENT "负责人id",
    `op_time` DATETIME COMMENT "处理时间"
)
DISTRIBUTED BY HASH(`type`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"enable_duplicate_without_keys_by_default" = "true"
);

MySQL > desc example_tbl_duplicate_without_keys_by_default;
+------------+---------------+------+-------+---------+-------+
| Field      | Type          | Null | Key   | Default | Extra |
+------------+---------------+------+-------+---------+-------+
| timestamp  | DATETIME      | No   | false | NULL    | NONE  |
| type       | INT           | No   | false | NULL    | NONE  |
| error_code | INT           | Yes  | false | NULL    | NONE  |
| error_msg  | VARCHAR(1024) | Yes  | false | NULL    | NONE  |
| op_id      | BIGINT        | Yes  | false | NULL    | NONE  |
| op_time    | DATETIME      | Yes  | false | NULL    | NONE  |
+------------+---------------+------+-------+---------+-------+
6 rows in set (0.01 sec)
```
</version>

## 聚合模型的局限性

这里我们针对 Aggregate 模型，来介绍下聚合模型的局限性。

在聚合模型中，模型对外展现的，是**最终聚合后的**数据。也就是说，任何还未聚合的数据（比如说两个不同导入批次的数据），必须通过某种方式，以保证对外展示的一致性。我们举例说明。

假设表结构如下：

| ColumnName | Type     | AggregationType | Comment |
|------------|----------|-----------------|---------|
| user_id    | LARGEINT |                 | 用户id    |
| date       | DATE     |                 | 数据灌入日期  |
| cost       | BIGINT   | SUM             | 用户总消费   |

假设存储引擎中有如下两个已经导入完成的批次的数据：

**batch 1**

| user_id | date       | cost |
|---------|------------|------|
| 10001   | 2017-11-20 | 50   |
| 10002   | 2017-11-21 | 39   |

**batch 2**

| user_id | date       | cost |
|---------|------------|------|
| 10001   | 2017-11-20 | 1    |
| 10001   | 2017-11-21 | 5    |
| 10003   | 2017-11-22 | 22   |

可以看到，用户 10001 分属在两个导入批次中的数据还没有聚合。但是为了保证用户只能查询到如下最终聚合后的数据：

| user_id | date       | cost |
|---------|------------|------|
| 10001   | 2017-11-20 | 51   |
| 10001   | 2017-11-21 | 5    |
| 10002   | 2017-11-21 | 39   |
| 10003   | 2017-11-22 | 22   |

我们在查询引擎中加入了聚合算子，来保证数据对外的一致性。

另外，在聚合列（Value）上，执行与聚合类型不一致的聚合类查询时，要注意语意。比如我们在如上示例中执行如下查询：

```
SELECT MIN(cost) FROM table;
```

得到的结果是 5，而不是 1。

同时，这种一致性保证，在某些查询中，会极大地降低查询效率。

我们以最基本的 count(*) 查询为例：

```
SELECT COUNT(*) FROM table;
```

在其他数据库中，这类查询都会很快地返回结果。因为在实现上，我们可以通过如“导入时对行进行计数，保存 count 的统计信息”，或者在查询时“仅扫描某一列数据，
获得 count 值”的方式，只需很小的开销，即可获得查询结果。但是在 Doris 的聚合模型中，这种查询的开销**非常大**。

我们以刚才的数据为例：

**batch 1**

| user_id | date       | cost |
|---------|------------|------|
| 10001   | 2017-11-20 | 50   |
| 10002   | 2017-11-21 | 39   |

**batch 2**

| user_id | date       | cost |
|---------|------------|------|
| 10001   | 2017-11-20 | 1    |
| 10001   | 2017-11-21 | 5    |
| 10003   | 2017-11-22 | 22   |

因为最终的聚合结果为：

| user_id | date       | cost |
|---------|------------|------|
| 10001   | 2017-11-20 | 51   |
| 10001   | 2017-11-21 | 5    |
| 10002   | 2017-11-21 | 39   |
| 10003   | 2017-11-22 | 22   |

所以，`select count(*) from table;` 的正确结果应该为 **4**。但如果我们只扫描 `user_id` 这一列，如果加上查询时聚合，最终得到的结果是 **3**（10001, 10002, 10003）。而如果不加查询时聚合，则得到的结果是 **5**（两批次一共5行数据）。可见这两个结果都是不对的。

为了得到正确的结果，我们必须同时读取 `user_id` 和 `date` 这两列的数据，**再加上查询时聚合**，才能返回 **4** 这个正确的结果。也就是说，在 count(*) 查询中，Doris 必须扫描所有的 AGGREGATE KEY 列（这里就是 `user_id` 和 `date`），并且聚合后，才能得到语意正确的结果。当聚合列非常多时，count(*) 查询需要扫描大量的数据。

因此，当业务上有频繁的 count(*) 查询时，我们建议用户通过增加一个**值恒为 1 的，聚合类型为 SUM 的列来模拟 count(\*)**。如刚才的例子中的表结构，我们修改如下：

| ColumnName | Type   | AggregateType | Comment   |
|------------|--------|---------------|-----------|
| user_id    | BIGINT |               | 用户id      |
| date       | DATE   |               | 数据灌入日期    |
| cost       | BIGINT | SUM           | 用户总消费     |
| count      | BIGINT | SUM           | 用于计算count |

增加一个 count 列，并且导入数据中，该列值**恒为 1**。则 `select count(*) from table;` 的结果等价于 `select sum(count) from table;`。
而后者的查询效率将远高于前者。不过这种方式也有使用限制，就是用户需要自行保证，不会重复导入 AGGREGATE KEY 列都相同地行。
否则，`select sum(count) from table;` 只能表述原始导入的行数，而不是 `select count(*) from table;` 的语义。

另一种方式，就是 **将如上的 `count` 列的聚合类型改为 REPLACE，且依然值恒为 1**。那么 `select sum(count) from table;` 和 `select count(*) from table;` 的结果将是一致的。并且这种方式，没有导入重复行的限制。

### Unique模型的写时合并实现

Unique模型的写时合并实现没有聚合模型的局限性，还是以刚才的数据为例，写时合并为每次导入的rowset增加了对应的delete bitmap，来标记哪些数据被覆盖。第一批数据导入后状态如下

**batch 1**

| user_id | date       | cost | delete bit |
|---------|------------|------|------------|
| 10001   | 2017-11-20 | 50   | false      |
| 10002   | 2017-11-21 | 39   | false      |

当第二批数据导入完成后，第一批数据中重复的行就会被标记为已删除，此时两批数据状态如下

**batch 1**

| user_id | date       | cost | delete bit |
|---------|------------|------|------------|
| 10001   | 2017-11-20 | 50   | **true**   |
| 10002   | 2017-11-21 | 39   | false      |

**batch 2**

| user_id | date       | cost | delete bit |
|---------|------------|------|------------|
| 10001   | 2017-11-20 | 1    | false      |
| 10001   | 2017-11-21 | 5    | false      |
| 10003   | 2017-11-22 | 22   | false      |

在查询时，所有在delete bitmap中被标记删除的数据都不会读出来，因此也无需进行做任何数据聚合，上述数据中有效地行数为4行，
查询出的结果也应该是4行，也就可以采取开销最小的方式来获取结果，即前面提到的“仅扫描某一列数据，获得 count 值”的方式。

在测试环境中，count(*) 查询在Unique模型的写时合并实现上的性能，相比聚合模型有10倍以上的提升。

### Duplicate 模型

Duplicate 模型没有聚合模型的这个局限性。因为该模型不涉及聚合语意，在做 count(*) 查询时，任意选择一列查询，即可得到语意正确的结果。

## key 列
Duplicate、Aggregate、Unique 模型，都会在建表指定 key 列，然而实际上是有所区别的：对于 Duplicate 模型，表的key列，
可以认为只是 “排序列”，并非起到唯一标识的作用。而 Aggregate、Unique 模型这种聚合类型的表，key 列是兼顾 “排序列” 和 “唯一标识列”，是真正意义上的“ key 列”。

## 数据模型的选择建议

因为数据模型在建表时就已经确定，且**无法修改**。所以，选择一个合适的数据模型**非常重要**。

1. Aggregate 模型可以通过预聚合，极大地降低聚合查询时所需扫描的数据量和查询的计算量，非常适合有固定模式的报表类查询场景。但是该模型对 count(*) 查询很不友好。同时因为固定了 Value 列上的聚合方式，在进行其他类型的聚合查询时，需要考虑语意正确性。
2. Unique 模型针对需要唯一主键约束的场景，可以保证主键唯一性约束。但是无法利用 ROLLUP 等预聚合带来的查询优势。
   1. 对于聚合查询有较高性能需求的用户，推荐使用自1.2版本加入的写时合并实现。
   2. Unique 模型仅支持整行更新，如果用户既需要唯一主键约束，又需要更新部分列（例如将多张源表导入到一张 doris 表的情形），则可以考虑使用 Aggregate 模型，同时将非主键列的聚合类型设置为 REPLACE_IF_NOT_NULL。具体的用法可以参考[语法手册](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md)
3. Duplicate 适合任意维度的 Ad-hoc 查询。虽然同样无法利用预聚合的特性，但是不受聚合模型的约束，可以发挥列存模型的优势（只读取相关列，而不需要读取所有 Key 列）。
