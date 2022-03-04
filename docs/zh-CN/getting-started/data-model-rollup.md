---
{
    "title": "数据模型、ROLLUP 及前缀索引",
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

# 数据模型、ROLLUP 及前缀索引

本文档主要从逻辑层面，描述 Doris 的数据模型、 ROLLUP 以及前缀索引的概念，以帮助用户更好的使用 Doris 应对不同的业务场景。

## 基本概念

在 Doris 中，数据以表（Table）的形式进行逻辑上的描述。  
一张表包括行（Row）和列（Column）。Row 即用户的一行数据。Column 用于描述一行数据中不同的字段。  

Column 可以分为两大类：Key 和 Value。从业务角度看，Key 和 Value 可以分别对应维度列和指标列。

Doris 的数据模型主要分为3类:

* Aggregate
* Uniq
* Duplicate

下面我们分别介绍。

## Aggregate 模型

我们以实际的例子来说明什么是聚合模型，以及如何正确的使用聚合模型。

### 示例1：导入数据聚合

假设业务有如下数据表模式：

|ColumnName|Type|AggregationType|Comment|
|---|---|---|---|
|user\_id|LARGEINT||用户id|
|date|DATE||数据灌入日期|
|city|VARCHAR(20)||用户所在城市|
|age|SMALLINT||用户年龄|
|sex|TINYINT||用户性别|
|last_visit_date|DATETIME|REPLACE|用户最后一次访问时间|
|cost|BIGINT|SUM|用户总消费|
|max\_dwell\_time|INT|MAX|用户最大停留时间|
|min\_dwell\_time|INT|MIN|用户最小停留时间|

如果转换成建表语句则如下（省略建表语句中的 Partition 和 Distribution 信息）

```
CREATE TABLE IF NOT EXISTS example_db.expamle_tbl
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
... /* 省略 Partition 和 Distribution 信息 */
；
```

可以看到，这是一个典型的用户信息和访问行为的事实表。  
在一般星型模型中，用户信息和访问行为一般分别存放在维度表和事实表中。这里我们为了更加方便的解释 Doris 的数据模型，将两部分信息统一存放在一张表中。

表中的列按照是否设置了 `AggregationType`，分为 Key (维度列) 和 Value（指标列）。没有设置 `AggregationType` 的，如 `user_id`、`date`、`age` ... 等称为 **Key**，而设置了 `AggregationType` 的称为 **Value**。

当我们导入数据时，对于 Key 列相同的行会聚合成一行，而 Value 列会按照设置的 `AggregationType` 进行聚合。 `AggregationType` 目前有以下四种聚合方式：

1. SUM：求和，多行的 Value 进行累加。
2. REPLACE：替代，下一批数据中的 Value 会替换之前导入过的行中的 Value。
3. MAX：保留最大值。
4. MIN：保留最小值。

假设我们有以下导入数据（原始数据）：

|user\_id|date|city|age|sex|last\_visit\_date|cost|max\_dwell\_time|min\_dwell\_time|
|---|---|---|---|---|---|---|---|---|
|10000|2017-10-01|北京|20|0|2017-10-01 06:00:00|20|10|10|
|10000|2017-10-01|北京|20|0|2017-10-01 07:00:00|15|2|2|
|10001|2017-10-01|北京|30|1|2017-10-01 17:05:45|2|22|22|
|10002|2017-10-02|上海|20|1|2017-10-02 12:59:12|200|5|5|
|10003|2017-10-02|广州|32|0|2017-10-02 11:20:00|30|11|11|
|10004|2017-10-01|深圳|35|0|2017-10-01 10:00:15|100|3|3|
|10004|2017-10-03|深圳|35|0|2017-10-03 10:20:22|11|6|6|

我们假设这是一张记录用户访问某商品页面行为的表。我们以第一行数据为例，解释如下：

|数据|说明|
|---|---|
|10000|用户id，每个用户唯一识别id|
|2017-10-01|数据入库时间，精确到日期|
|北京|用户所在城市|
|20|用户年龄|
|0|性别男（1 代表女性）|
|2017-10-01 06:00:00|用户本次访问该页面的时间，精确到秒|
|20|用户本次访问产生的消费|
|10|用户本次访问，驻留该页面的时间|
|10|用户本次访问，驻留该页面的时间（冗余）|

那么当这批数据正确导入到 Doris 中后，Doris 中最终存储如下：

|user\_id|date|city|age|sex|last\_visit\_date|cost|max\_dwell\_time|min\_dwell\_time|
|---|---|---|---|---|---|---|---|---|
|10000|2017-10-01|北京|20|0|2017-10-01 07:00:00|35|10|2|
|10001|2017-10-01|北京|30|1|2017-10-01 17:05:45|2|22|22|
|10002|2017-10-02|上海|20|1|2017-10-02 12:59:12|200|5|5|
|10003|2017-10-02|广州|32|0|2017-10-02 11:20:00|30|11|11|
|10004|2017-10-01|深圳|35|0|2017-10-01 10:00:15|100|3|3|
|10004|2017-10-03|深圳|35|0|2017-10-03 10:20:22|11|6|6|

可以看到，用户 10000 只剩下了一行**聚合后**的数据。而其余用户的数据和原始数据保持一致。这里先解释下用户 10000 聚合后的数据：

前5列没有变化，从第6列 `last_visit_date` 开始：

* `2017-10-01 07:00:00`：因为 `last_visit_date` 列的聚合方式为 REPLACE，所以 `2017-10-01 07:00:00` 替换了 `2017-10-01 06:00:00` 保存了下来。
    > 注：在同一个导入批次中的数据，对于 REPLACE 这种聚合方式，替换顺序不做保证。如在这个例子中，最终保存下来的，也有可能是 `2017-10-01 06:00:00`。而对于不同导入批次中的数据，可以保证，后一批次的数据会替换前一批次。

* `35`：因为 `cost` 列的聚合类型为 SUM，所以由 20 + 15 累加获得 35。
* `10`：因为 `max_dwell_time` 列的聚合类型为 MAX，所以 10 和 2 取最大值，获得 10。
* `2`：因为 `min_dwell_time` 列的聚合类型为 MIN，所以 10 和 2 取最小值，获得 2。

经过聚合，Doris 中最终只会存储聚合后的数据。换句话说，即明细数据会丢失，用户不能够再查询到聚合前的明细数据了。

### 示例2：保留明细数据

接示例1，我们将表结构修改如下：

|ColumnName|Type|AggregationType|Comment|
|---|---|---|---|
|user\_id|LARGEINT||用户id|
|date|DATE||数据灌入日期|
|timestamp|DATETIME||数据灌入时间，精确到秒|
|city|VARCHAR(20)||用户所在城市|
|age|SMALLINT||用户年龄|
|sex|TINYINT||用户性别|
|last\_visit\_date|DATETIME|REPLACE|用户最后一次访问时间|
|cost|BIGINT|SUM|用户总消费|
|max\_dwell\_time|INT|MAX|用户最大停留时间|
|min\_dwell\_time|INT|MIN|用户最小停留时间|

即增加了一列 `timestamp`，记录精确到秒的数据灌入时间。

导入数据如下：

|user_id|date|timestamp|city|age|sex|last\_visit\_date|cost|max\_dwell\_time|min\_dwell\_time|
|---|---|---|---|---|---|---|---|---|---|
|10000|2017-10-01|2017-10-01 08:00:05|北京|20|0|2017-10-01 06:00:00|20|10|10|
|10000|2017-10-01|2017-10-01 09:00:05|北京|20|0|2017-10-01 07:00:00|15|2|2|
|10001|2017-10-01|2017-10-01 18:12:10|北京|30|1|2017-10-01 17:05:45|2|22|22|
|10002|2017-10-02|2017-10-02 13:10:00|上海|20|1|2017-10-02 12:59:12|200|5|5|
|10003|2017-10-02|2017-10-02 13:15:00|广州|32|0|2017-10-02 11:20:00|30|11|11|
|10004|2017-10-01|2017-10-01 12:12:48|深圳|35|0|2017-10-01 10:00:15|100|3|3|
|10004|2017-10-03|2017-10-03 12:38:20|深圳|35|0|2017-10-03 10:20:22|11|6|6|

那么当这批数据正确导入到 Doris 中后，Doris 中最终存储如下：

|user_id|date|timestamp|city|age|sex|last\_visit\_date|cost|max\_dwell\_time|min\_dwell\_time|
|---|---|---|---|---|---|---|---|---|---|
|10000|2017-10-01|2017-10-01 08:00:05|北京|20|0|2017-10-01 06:00:00|20|10|10|
|10000|2017-10-01|2017-10-01 09:00:05|北京|20|0|2017-10-01 07:00:00|15|2|2|
|10001|2017-10-01|2017-10-01 18:12:10|北京|30|1|2017-10-01 17:05:45|2|22|22|
|10002|2017-10-02|2017-10-02 13:10:00|上海|20|1|2017-10-02 12:59:12|200|5|5|
|10003|2017-10-02|2017-10-02 13:15:00|广州|32|0|2017-10-02 11:20:00|30|11|11|
|10004|2017-10-01|2017-10-01 12:12:48|深圳|35|0|2017-10-01 10:00:15|100|3|3|
|10004|2017-10-03|2017-10-03 12:38:20|深圳|35|0|2017-10-03 10:20:22|11|6|6|

我们可以看到，存储的数据，和导入数据完全一样，没有发生任何聚合。这是因为，这批数据中，因为加入了 `timestamp` 列，所有行的 Key 都**不完全相同**。也就是说，只要保证导入的数据中，每一行的 Key 都不完全相同，那么即使在聚合模型下，Doris 也可以保存完整的明细数据。

### 示例3：导入数据与已有数据聚合

接示例1。假设现在表中已有数据如下：

|user_id|date|city|age|sex|last\_visit\_date|cost|max\_dwell\_time|min\_dwell\_time|
|---|---|---|---|---|---|---|---|---|
|10000|2017-10-01|北京|20|0|2017-10-01 07:00:00|35|10|2|
|10001|2017-10-01|北京|30|1|2017-10-01 17:05:45|2|22|22|
|10002|2017-10-02|上海|20|1|2017-10-02 12:59:12|200|5|5|
|10003|2017-10-02|广州|32|0|2017-10-02 11:20:00|30|11|11|
|10004|2017-10-01|深圳|35|0|2017-10-01 10:00:15|100|3|3|
|10004|2017-10-03|深圳|35|0|2017-10-03 10:20:22|11|6|6|

我们再导入一批新的数据：

|user_id|date|city|age|sex|last\_visit\_date|cost|max\_dwell\_time|min\_dwell\_time|
|---|---|---|---|---|---|---|---|---|
|10004|2017-10-03|深圳|35|0|2017-10-03 11:22:00|44|19|19|
|10005|2017-10-03|长沙|29|1|2017-10-03 18:11:02|3|1|1|

那么当这批数据正确导入到 Doris 中后，Doris 中最终存储如下：

|user_id|date|city|age|sex|last\_visit\_date|cost|max\_dwell\_time|min\_dwell\_time|
|---|---|---|---|---|---|---|---|---|
|10000|2017-10-01|北京|20|0|2017-10-01 07:00:00|35|10|2|
|10001|2017-10-01|北京|30|1|2017-10-01 17:05:45|2|22|22|
|10002|2017-10-02|上海|20|1|2017-10-02 12:59:12|200|5|5|
|10003|2017-10-02|广州|32|0|2017-10-02 11:20:00|30|11|11|
|10004|2017-10-01|深圳|35|0|2017-10-01 10:00:15|100|3|3|
|10004|2017-10-03|深圳|35|0|2017-10-03 11:22:00|55|19|6|
|10005|2017-10-03|长沙|29|1|2017-10-03 18:11:02|3|1|1|

可以看到，用户 10004 的已有数据和新导入的数据发生了聚合。同时新增了 10005 用户的数据。

数据的聚合，在 Doris 中有如下三个阶段发生：

1. 每一批次数据导入的 ETL 阶段。该阶段会在每一批次导入的数据内部进行聚合。
2. 底层 BE 进行数据 Compaction 的阶段。该阶段，BE 会对已导入的不同批次的数据进行进一步的聚合。
3. 数据查询阶段。在数据查询时，对于查询涉及到的数据，会进行对应的聚合。

数据在不同时间，可能聚合的程度不一致。比如一批数据刚导入时，可能还未与之前已存在的数据进行聚合。但是对于用户而言，用户**只能查询到**聚合后的数据。即不同的聚合程度对于用户查询而言是透明的。用户需始终认为数据以**最终的完成的聚合程度**存在，而**不应假设某些聚合还未发生**。（可参阅**聚合模型的局限性**一节获得更多详情。）

## Uniq 模型

在某些多维分析场景下，用户更关注的是如何保证 Key 的唯一性，即如何获得 Primary Key 唯一性约束。因此，我们引入了 Uniq 的数据模型。该模型本质上是聚合模型的一个特例，也是一种简化的表结构表示方式。我们举例说明。

|ColumnName|Type|IsKey|Comment|
|---|---|---|---|
|user_id|BIGINT|Yes|用户id|
|username|VARCHAR(50)|Yes|用户昵称|
|city|VARCHAR(20)|No|用户所在城市|
|age|SMALLINT|No|用户年龄|
|sex|TINYINT|No|用户性别|
|phone|LARGEINT|No|用户电话|
|address|VARCHAR(500)|No|用户住址|
|register_time|DATETIME|No|用户注册时间|

这是一个典型的用户基础信息表。这类数据没有聚合需求，只需保证主键唯一性。（这里的主键为 user_id + username）。那么我们的建表语句如下：

```
CREATE TABLE IF NOT EXISTS example_db.expamle_tbl
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
... /* 省略 Partition 和 Distribution 信息 */
；
```

而这个表结构，完全同等于以下使用聚合模型描述的表结构：

|ColumnName|Type|AggregationType|Comment|
|---|---|---|---|
|user_id|BIGINT||用户id|
|username|VARCHAR(50)||用户昵称|
|city|VARCHAR(20)|REPLACE|用户所在城市|
|age|SMALLINT|REPLACE|用户年龄|
|sex|TINYINT|REPLACE|用户性别|
|phone|LARGEINT|REPLACE|用户电话|
|address|VARCHAR(500)|REPLACE|用户住址|
|register_time|DATETIME|REPLACE|用户注册时间|

及建表语句：

```
CREATE TABLE IF NOT EXISTS example_db.expamle_tbl
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
... /* 省略 Partition 和 Distribution 信息 */
；
```

即 Uniq 模型完全可以用聚合模型中的 REPLACE 方式替代。其内部的实现方式和数据存储方式也完全一样。这里不再继续举例说明。

## Duplicate 模型

在某些多维分析场景下，数据既没有主键，也没有聚合需求。因此，我们引入 Duplicate 数据模型来满足这类需求。举例说明。

|ColumnName|Type|SortKey|Comment|
|---|---|---|---|
|timestamp|DATETIME|Yes|日志时间|
|type|INT|Yes|日志类型|
|error_code|INT|Yes|错误码|
|error_msg|VARCHAR(1024)|No|错误详细信息|
|op_id|BIGINT|No|负责人id|
|op_time|DATETIME|No|处理时间|

建表语句如下：

```
CREATE TABLE IF NOT EXISTS example_db.expamle_tbl
(
    `timestamp` DATETIME NOT NULL COMMENT "日志时间",
    `type` INT NOT NULL COMMENT "日志类型",
    `error_code` INT COMMENT "错误码",
    `error_msg` VARCHAR(1024) COMMENT "错误详细信息",
    `op_id` BIGINT COMMENT "负责人id",
    `op_time` DATETIME COMMENT "处理时间"
)
DUPLICATE KEY(`timestamp`, `type`)
... /* 省略 Partition 和 Distribution 信息 */
；
```

这种数据模型区别于 Aggregate 和 Uniq 模型。数据完全按照导入文件中的数据进行存储，不会有任何聚合。即使两行数据完全相同，也都会保留。
而在建表语句中指定的 DUPLICATE KEY，只是用来指明底层数据按照那些列进行排序。（更贴切的名称应该为 “Sorted Column”，这里取名 “DUPLICATE KEY” 只是用以明确表示所用的数据模型。关于 “Sorted Column”的更多解释，可以参阅**前缀索引**小节）。在 DUPLICATE KEY 的选择上，我们建议适当的选择前 2-4 列就可以。

这种数据模型适用于既没有聚合需求，又没有主键唯一性约束的原始数据的存储。更多使用场景，可参阅**聚合模型的局限性**小节。

## ROLLUP

ROLLUP 在多维分析中是“上卷”的意思，即将数据按某种指定的粒度进行进一步聚合。

### 基本概念

在 Doris 中，我们将用户通过建表语句创建出来的表称为 Base 表（Base Table）。Base 表中保存着按用户建表语句指定的方式存储的基础数据。

在 Base 表之上，我们可以创建任意多个 ROLLUP 表。这些 ROLLUP 的数据是基于 Base 表产生的，并且在物理上是**独立存储**的。

ROLLUP 表的基本作用，在于在 Base 表的基础上，获得更粗粒度的聚合数据。

下面我们用示例详细说明在不同数据模型中的 ROLLUP 表及其作用。

#### Aggregate 和 Uniq 模型中的 ROLLUP

因为 Uniq 只是 Aggregate 模型的一个特例，所以这里我们不加以区别。

1. 示例1：获得每个用户的总消费

接**Aggregate 模型**小节的**示例2**，Base 表结构如下：

|ColumnName|Type|AggregationType|Comment|
|---|---|---|---|
|user_id|LARGEINT||用户id|
|date|DATE||数据灌入日期|
|timestamp|DATETIME||数据灌入时间，精确到秒|
|city|VARCHAR(20)||用户所在城市|
|age|SMALLINT||用户年龄|
|sex|TINYINT||用户性别|
|last_visit_date|DATETIME|REPLACE|用户最后一次访问时间|
|cost|BIGINT|SUM|用户总消费|
|max\_dwell\_time|INT|MAX|用户最大停留时间|
|min\_dwell\_time|INT|MIN|用户最小停留时间|

存储的数据如下：

|user_id|date|timestamp|city|age|sex|last\_visit\_date|cost|max\_dwell\_time|min\_dwell\_time|
|---|---|---|---|---|---|---|---|---|---|
|10000|2017-10-01|2017-10-01 08:00:05|北京|20|0|2017-10-01 06:00:00|20|10|10|
|10000|2017-10-01|2017-10-01 09:00:05|北京|20|0|2017-10-01 07:00:00|15|2|2|
|10001|2017-10-01|2017-10-01 18:12:10|北京|30|1|2017-10-01 17:05:45|2|22|22|
|10002|2017-10-02|2017-10-02 13:10:00|上海|20|1|2017-10-02 12:59:12|200|5|5|
|10003|2017-10-02|2017-10-02 13:15:00|广州|32|0|2017-10-02 11:20:00|30|11|11|
|10004|2017-10-01|2017-10-01 12:12:48|深圳|35|0|2017-10-01 10:00:15|100|3|3|
|10004|2017-10-03|2017-10-03 12:38:20|深圳|35|0|2017-10-03 10:20:22|11|6|6|

在此基础上，我们创建一个 ROLLUP：

|ColumnName|
|---|
|user_id|
|cost|

该 ROLLUP 只包含两列：user_id 和 cost。则创建完成后，该 ROLLUP  中存储的数据如下：

|user\_id|cost|
|---|---|
|10000|35|
|10001|2|
|10002|200|
|10003|30|
|10004|111|

可以看到，ROLLUP 中仅保留了每个 user_id，在 cost 列上的 SUM 的结果。那么当我们进行如下查询时:

`SELECT user_id, sum(cost) FROM table GROUP BY user_id;`

Doris 会自动命中这个 ROLLUP     表，从而只需扫描极少的数据量，即可完成这次聚合查询。

2. 示例2：获得不同城市，不同年龄段用户的总消费、最长和最短页面驻留时间

紧接示例1。我们在 Base 表基础之上，再创建一个 ROLLUP：

|ColumnName|Type|AggregationType|Comment|
|---|---|---|---|
|city|VARCHAR(20)||用户所在城市|
|age|SMALLINT||用户年龄|
|cost|BIGINT|SUM|用户总消费|
|max\_dwell\_time|INT|MAX|用户最大停留时间|
|min\_dwell\_time|INT|MIN|用户最小停留时间|

则创建完成后，该 ROLLUP 中存储的数据如下：

|city|age|cost|max\_dwell\_time|min\_dwell\_time|
|---|---|---|---|---|
|北京|20|35|10|2|
|北京|30|2|22|22|
|上海|20|200|5|5|
|广州|32|30|11|11|
|深圳|35|111|6|3|

当我们进行如下这些查询时:

* `SELECT city, age, sum(cost), max(max_dwell_time), min(min_dwell_time) FROM table GROUP BY city, age;`  
* `SELECT city, sum(cost), max(max_dwell_time), min(min_dwell_time) FROM table GROUP BY city;`  
* `SELECT city, age, sum(cost), min(min_dwell_time) FROM table GROUP BY city, age;`  

Doris 会自动命中这个 ROLLUP 表。

#### Duplicate 模型中的 ROLLUP

因为 Duplicate 模型没有聚合的语意。所以该模型中的 ROLLUP，已经失去了“上卷”这一层含义。而仅仅是作为调整列顺序，以命中前缀索引的作用。我们将在接下来的小节中，详细介绍前缀索引，以及如何使用ROLLUP改变前缀索引，以获得更好的查询效率。

### 前缀索引与 ROLLUP

#### 前缀索引

不同于传统的数据库设计，Doris 不支持在任意列上创建索引。Doris 这类 MPP 架构的 OLAP 数据库，通常都是通过提高并发，来处理大量数据的。  
本质上，Doris 的数据存储在类似 SSTable（Sorted String Table）的数据结构中。该结构是一种有序的数据结构，可以按照指定的列进行排序存储。在这种数据结构上，以排序列作为条件进行查找，会非常的高效。

在 Aggregate、Uniq 和 Duplicate 三种数据模型中。底层的数据存储，是按照各自建表语句中，AGGREGATE KEY、UNIQ KEY 和 DUPLICATE KEY 中指定的列进行排序存储的。

而前缀索引，即在排序的基础上，实现的一种根据给定前缀列，快速查询数据的索引方式。

我们将一行数据的前 **36 个字节** 作为这行数据的前缀索引。当遇到 VARCHAR 类型时，前缀索引会直接截断。我们举例说明：

1. 以下表结构的前缀索引为 user_id(8 Bytes) + age(4 Bytes) + message(prefix 20 Bytes)。

|ColumnName|Type|
|---|---|
|user_id|BIGINT|
|age|INT|
|message|VARCHAR(100)|
|max\_dwell\_time|DATETIME|
|min\_dwell\_time|DATETIME|

2. 以下表结构的前缀索引为 user_name(20 Bytes)。即使没有达到 36 个字节，因为遇到 VARCHAR，所以直接截断，不再往后继续。

|ColumnName|Type|
|---|---|
|user_name|VARCHAR(20)|
|age|INT|
|message|VARCHAR(100)|
|max\_dwell\_time|DATETIME|
|min\_dwell\_time|DATETIME|

当我们的查询条件，是**前缀索引的前缀**时，可以极大的加快查询速度。比如在第一个例子中，我们执行如下查询：

`SELECT * FROM table WHERE user_id=1829239 and age=20；`

该查询的效率会**远高于**如下查询：

`SELECT * FROM table WHERE age=20；`

所以在建表时，**正确的选择列顺序，能够极大地提高查询效率**。

#### ROLLUP 调整前缀索引

因为建表时已经指定了列顺序，所以一个表只有一种前缀索引。这对于使用其他不能命中前缀索引的列作为条件进行的查询来说，效率上可能无法满足需求。因此，我们可以通过创建 ROLLUP 来人为的调整列顺序。举例说明。

Base 表结构如下：

|ColumnName|Type|
|---|---|
|user\_id|BIGINT|
|age|INT|
|message|VARCHAR(100)|
|max\_dwell\_time|DATETIME|
|min\_dwell\_time|DATETIME|

我们可以在此基础上创建一个 ROLLUP 表：

|ColumnName|Type|
|---|---|
|age|INT|
|user\_id|BIGINT|
|message|VARCHAR(100)|
|max\_dwell\_time|DATETIME|
|min\_dwell\_time|DATETIME|

可以看到，ROLLUP 和 Base 表的列完全一样，只是将 user_id 和 age 的顺序调换了。那么当我们进行如下查询时：

`SELECT * FROM table where age=20 and message LIKE "%error%";`

会优先选择 ROLLUP 表，因为 ROLLUP 的前缀索引匹配度更高。

### ROLLUP 的几点说明

* ROLLUP 最根本的作用是提高某些查询的查询效率（无论是通过聚合来减少数据量，还是修改列顺序以匹配前缀索引）。因此 ROLLUP 的含义已经超出了 “上卷” 的范围。这也是为什么我们在源代码中，将其命名为 Materialized Index（物化索引）的原因。
* ROLLUP 是附属于 Base 表的，可以看做是 Base 表的一种辅助数据结构。用户可以在 Base 表的基础上，创建或删除 ROLLUP，但是不能在查询中显式的指定查询某 ROLLUP。是否命中 ROLLUP 完全由 Doris 系统自动决定。
* ROLLUP 的数据是独立物理存储的。因此，创建的 ROLLUP 越多，占用的磁盘空间也就越大。同时对导入速度也会有影响（导入的ETL阶段会自动产生所有 ROLLUP 的数据），但是不会降低查询效率（只会更好）。
* ROLLUP 的数据更新与 Base 表是完全同步的。用户无需关心这个问题。
* ROLLUP 中列的聚合方式，与 Base 表完全相同。在创建 ROLLUP 无需指定，也不能修改。
* 查询能否命中 ROLLUP 的一个必要条件（非充分条件）是，查询所涉及的**所有列**（包括 select list 和 where 中的查询条件列等）都存在于该 ROLLUP 的列中。否则，查询只能命中 Base 表。
* 某些类型的查询（如 count(*)）在任何条件下，都无法命中 ROLLUP。具体参见接下来的 **聚合模型的局限性** 一节。
* 可以通过 `EXPLAIN your_sql;` 命令获得查询执行计划，在执行计划中，查看是否命中 ROLLUP。
* 可以通过 `DESC tbl_name ALL;` 语句显示 Base 表和所有已创建完成的 ROLLUP。

在这篇文档中可以查看 [查询如何命中 Rollup](hit-the-rollup)

## 聚合模型的局限性

这里我们针对 Aggregate 模型（包括 Uniq 模型），来介绍下聚合模型的局限性。

在聚合模型中，模型对外展现的，是**最终聚合后的**数据。也就是说，任何还未聚合的数据（比如说两个不同导入批次的数据），必须通过某种方式，以保证对外展示的一致性。我们举例说明。

假设表结构如下：

|ColumnName|Type|AggregationType|Comment|
|---|---|---|---|
|user\_id|LARGEINT||用户id|
|date|DATE||数据灌入日期|
|cost|BIGINT|SUM|用户总消费|

假设存储引擎中有如下两个已经导入完成的批次的数据：

**batch 1**

|user\_id|date|cost|
|---|---|---|
|10001|2017-11-20|50|
|10002|2017-11-21|39|

**batch 2**

|user\_id|date|cost|
|---|---|---|
|10001|2017-11-20|1|
|10001|2017-11-21|5|
|10003|2017-11-22|22|

可以看到，用户 10001 分属在两个导入批次中的数据还没有聚合。但是为了保证用户只能查询到如下最终聚合后的数据：

|user\_id|date|cost|
|---|---|---|
|10001|2017-11-20|51|
|10001|2017-11-21|5|
|10002|2017-11-21|39|
|10003|2017-11-22|22|

我们在查询引擎中加入了聚合算子，来保证数据对外的一致性。

另外，在聚合列（Value）上，执行与聚合类型不一致的聚合类查询时，要注意语意。比如我们在如上示例中执行如下查询：

`SELECT MIN(cost) FROM table;`

得到的结果是 5，而不是 1。

同时，这种一致性保证，在某些查询中，会极大的降低查询效率。

我们以最基本的 count(*) 查询为例：

`SELECT COUNT(*) FROM table;`

在其他数据库中，这类查询都会很快的返回结果。因为在实现上，我们可以通过如“导入时对行进行计数，保存count的统计信息”，或者在查询时“仅扫描某一列数据，获得count值”的方式，只需很小的开销，即可获得查询结果。但是在 Doris 的聚合模型中，这种查询的开销**非常大**。

我们以刚才的数据为例：

**batch 1**

|user\_id|date|cost|
|---|---|---|
|10001|2017-11-20|50|
|10002|2017-11-21|39|

**batch 2**

|user\_id|date|cost|
|---|---|---|
|10001|2017-11-20|1|
|10001|2017-11-21|5|
|10003|2017-11-22|22|

因为最终的聚合结果为：

|user\_id|date|cost|
|---|---|---|
|10001|2017-11-20|51|
|10001|2017-11-21|5|
|10002|2017-11-21|39|
|10003|2017-11-22|22|

所以，`select count(*) from table;` 的正确结果应该为 **4**。但如果我们只扫描 `user_id` 这一列，如果加上查询时聚合，最终得到的结果是 **3**（10001, 10002, 10003）。而如果不加查询时聚合，则得到的结果是 **5**（两批次一共5行数据）。可见这两个结果都是不对的。

为了得到正确的结果，我们必须同时读取 `user_id` 和 `date` 这两列的数据，**再加上查询时聚合**，才能返回 **4** 这个正确的结果。也就是说，在 count(\*) 查询中，Doris 必须扫描所有的 AGGREGATE KEY 列（这里就是 `user_id` 和 `date`），并且聚合后，才能得到语意正确的结果。当聚合列非常多时，count(\*) 查询需要扫描大量的数据。

因此，当业务上有频繁的 count(\*) 查询时，我们建议用户通过增加一个**值恒为 1 的，聚合类型为 SUM 的列来模拟 count(\*)**。如刚才的例子中的表结构，我们修改如下：

|ColumnName|Type|AggregateType|Comment|
|---|---|---|---|
|user\_id|BIGINT||用户id|
|date|DATE||数据灌入日期|
|cost|BIGINT|SUM|用户总消费|
|count|BIGINT|SUM|用于计算count|

增加一个 count 列，并且导入数据中，该列值**恒为 1**。则 `select count(*) from table;` 的结果等价于 `select sum(count) from table;`。而后者的查询效率将远高于前者。不过这种方式也有使用限制，就是用户需要自行保证，不会重复导入 AGGREGATE KEY 列都相同的行。否则，`select sum(count) from table;` 只能表述原始导入的行数，而不是 `select count(*) from table;` 的语义。

另一种方式，就是 **将如上的 `count` 列的聚合类型改为 REPLACE，且依然值恒为 1**。那么 `select sum(count) from table;` 和 `select count(*) from table;` 的结果将是一致的。并且这种方式，没有导入重复行的限制。

### Duplicate 模型

Duplicate 模型没有聚合模型的这个局限性。因为该模型不涉及聚合语意，在做 count(*) 查询时，任意选择一列查询，即可得到语意正确的结果。

## 数据模型的选择建议

因为数据模型在建表时就已经确定，且**无法修改**。所以，选择一个合适的数据模型**非常重要**。

1. Aggregate 模型可以通过预聚合，极大地降低聚合查询时所需扫描的数据量和查询的计算量，非常适合有固定模式的报表类查询场景。但是该模型对 count(*) 查询很不友好。同时因为固定了 Value 列上的聚合方式，在进行其他类型的聚合查询时，需要考虑语意正确性。
2. Uniq 模型针对需要唯一主键约束的场景，可以保证主键唯一性约束。但是无法利用 ROLLUP 等预聚合带来的查询优势（因为本质是 REPLACE，没有 SUM 这种聚合方式）。
3. Duplicate 适合任意维度的 Ad-hoc 查询。虽然同样无法利用预聚合的特性，但是不受聚合模型的约束，可以发挥列存模型的优势（只读取相关列，而不需要读取所有 Key 列）。
