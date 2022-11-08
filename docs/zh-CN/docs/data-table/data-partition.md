---
{
    "title": "数据划分",
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

# 数据划分

本文档主要介绍 Doris 的建表和数据划分，以及建表操作中可能遇到的问题和解决方法。

## 基本概念

在 Doris 中，数据都以表（Table）的形式进行逻辑上的描述。

### Row & Column

一张表包括行（Row）和列（Column）：

- Row：即用户的一行数据；

- Column： 用于描述一行数据中不同的字段。

  Column 可以分为两大类：Key 和 Value。从业务角度看，Key 和 Value 可以分别对应维度列和指标列。从聚合模型的角度来说，Key 列相同的行，会聚合成一行。其中 Value 列的聚合方式由用户在建表时指定。关于更多聚合模型的介绍，可以参阅 [Doris 数据模型](data-model.md)。

### Tablet & Partition

在 Doris 的存储引擎中，用户数据被水平划分为若干个数据分片（Tablet，也称作数据分桶）。每个 Tablet 包含若干数据行。各个 Tablet 之间的数据没有交集，并且在物理上是独立存储的。

多个 Tablet 在逻辑上归属于不同的分区（Partition）。一个 Tablet 只属于一个 Partition。而一个 Partition 包含若干个 Tablet。因为 Tablet 在物理上是独立存储的，所以可以视为 Partition 在物理上也是独立。Tablet 是数据移动、复制等操作的最小物理存储单元。

若干个 Partition 组成一个 Table。Partition 可以视为是逻辑上最小的管理单元。数据的导入与删除，都可以或仅能针对一个 Partition 进行。

## 数据划分

我们以一个建表操作来说明 Doris 的数据划分。

Doris 的建表是一个同步命令，SQL执行完成即返回结果，命令返回成功即表示建表成功。具体建表语法可以参考[CREATE TABLE](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md)，也可以通过 `HELP CREATE TABLE;` 查看更多帮助。

本小节通过一个例子，来介绍 Doris 的建表方式。

```sql
-- Range Partition

CREATE TABLE IF NOT EXISTS example_db.example_range_tbl
(
    `user_id` LARGEINT NOT NULL COMMENT "用户id",
    `date` DATE NOT NULL COMMENT "数据灌入日期时间",
    `timestamp` DATETIME NOT NULL COMMENT "数据灌入的时间戳",
    `city` VARCHAR(20) COMMENT "用户所在城市",
    `age` SMALLINT COMMENT "用户年龄",
    `sex` TINYINT COMMENT "用户性别",
    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
    `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
)
ENGINE=OLAP
AGGREGATE KEY(`user_id`, `date`, `timestamp`, `city`, `age`, `sex`)
PARTITION BY RANGE(`date`)
(
    PARTITION `p201701` VALUES LESS THAN ("2017-02-01"),
    PARTITION `p201702` VALUES LESS THAN ("2017-03-01"),
    PARTITION `p201703` VALUES LESS THAN ("2017-04-01")
)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 16
PROPERTIES
(
    "replication_num" = "3",
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2018-01-01 12:00:00"
);


-- List Partition

CREATE TABLE IF NOT EXISTS example_db.example_list_tbl
(
    `user_id` LARGEINT NOT NULL COMMENT "用户id",
    `date` DATE NOT NULL COMMENT "数据灌入日期时间",
    `timestamp` DATETIME NOT NULL COMMENT "数据灌入的时间戳",
    `city` VARCHAR(20) COMMENT "用户所在城市",
    `age` SMALLINT COMMENT "用户年龄",
    `sex` TINYINT COMMENT "用户性别",
    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
    `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
)
ENGINE=olap
AGGREGATE KEY(`user_id`, `date`, `timestamp`, `city`, `age`, `sex`)
PARTITION BY LIST(`city`)
(
    PARTITION `p_cn` VALUES IN ("Beijing", "Shanghai", "Hong Kong"),
    PARTITION `p_usa` VALUES IN ("New York", "San Francisco"),
    PARTITION `p_jp` VALUES IN ("Tokyo")
)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 16
PROPERTIES
(
    "replication_num" = "3",
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2018-01-01 12:00:00"
);

```

### 列定义

这里我们只以 AGGREGATE KEY 数据模型为例进行说明。更多数据模型参阅 [Doris 数据模型](./data-model.md)。

列的基本类型，可以通过在 mysql-client 中执行 `HELP CREATE TABLE;` 查看。

AGGREGATE KEY 数据模型中，所有没有指定聚合方式（SUM、REPLACE、MAX、MIN）的列视为 Key 列。而其余则为 Value 列。

定义列时，可参照如下建议：

1. Key 列必须在所有 Value 列之前。
2. 尽量选择整型类型。因为整型类型的计算和查找效率远高于字符串。
3. 对于不同长度的整型类型的选择原则，遵循 **够用即可**。
4. 对于 VARCHAR 和 STRING 类型的长度，遵循 **够用即可**。

### 分区和分桶

Doris 支持两层的数据划分。第一层是 Partition，支持 Range 和 List 的划分方式。第二层是 Bucket（Tablet），仅支持 Hash 的划分方式。

也可以仅使用一层分区。使用一层分区时，只支持 Bucket 划分。下面我们来分别介绍下分区以及分桶：

1. **Partition**

   - Partition 列可以指定一列或多列，分区列必须为 KEY 列。多列分区的使用方式在后面 **多列分区** 小结介绍。
   - 不论分区列是什么类型，在写分区值时，都需要加双引号。
   - 分区数量理论上没有上限。
   - 当不使用 Partition 建表时，系统会自动生成一个和表名同名的，全值范围的 Partition。该 Partition 对用户不可见，并且不可删改。
   - 创建分区时**不可添加范围重叠**的分区。

   **Range 分区**

   - 分区列通常为时间列，以方便的管理新旧数据。

   - Partition 支持通过 `VALUES LESS THAN (...)` 仅指定上界，系统会将前一个分区的上界作为该分区的下界，生成一个左闭右开的区间。同时，也支持通过 `VALUES [...)` 指定上下界，生成一个左闭右开的区间。

   - 通过 `VALUES [...)` 同时指定上下界比较容易理解。这里举例说明，当使用 `VALUES LESS THAN (...)` 语句进行分区的增删操作时，分区范围的变化情况：

     - 如上 `example_range_tbl` 示例，当建表完成后，会自动生成如下3个分区：

       ```text
       p201701: [MIN_VALUE,  2017-02-01)
       p201702: [2017-02-01, 2017-03-01)
       p201703: [2017-03-01, 2017-04-01)
       ```

     - 当我们增加一个分区 p201705 VALUES LESS THAN ("2017-06-01")，分区结果如下：

       ```text
       p201701: [MIN_VALUE,  2017-02-01)
       p201702: [2017-02-01, 2017-03-01)
       p201703: [2017-03-01, 2017-04-01)
       p201705: [2017-04-01, 2017-06-01)
       ```

     - 此时我们删除分区 p201703，则分区结果如下：

       ```text
       p201701: [MIN_VALUE,  2017-02-01)
       p201702: [2017-02-01, 2017-03-01)
       p201705: [2017-04-01, 2017-06-01)
       ```

       > 注意到 p201702 和 p201705 的分区范围并没有发生变化，而这两个分区之间，出现了一个空洞：[2017-03-01, 2017-04-01)。即如果导入的数据范围在这个空洞范围内，是无法导入的。

     - 继续删除分区 p201702，分区结果如下：

       ```text
       p201701: [MIN_VALUE,  2017-02-01)
       p201705: [2017-04-01, 2017-06-01)
       ```

       > 空洞范围变为：[2017-02-01, 2017-04-01)

     - 现在增加一个分区 p201702new VALUES LESS THAN ("2017-03-01")，分区结果如下：

       ```text
       p201701:    [MIN_VALUE,  2017-02-01)
       p201702new: [2017-02-01, 2017-03-01)
       p201705:    [2017-04-01, 2017-06-01)
       ```

       > 可以看到空洞范围缩小为：[2017-03-01, 2017-04-01)

     - 现在删除分区 p201701，并添加分区 p201612 VALUES LESS THAN ("2017-01-01")，分区结果如下：

       ```text
       p201612:    [MIN_VALUE,  2017-01-01)
       p201702new: [2017-02-01, 2017-03-01)
       p201705:    [2017-04-01, 2017-06-01) 
       ```

       > 即出现了一个新的空洞：[2017-01-01, 2017-02-01)

   综上，分区的删除不会改变已存在分区的范围。删除分区可能出现空洞。通过 `VALUES LESS THAN` 语句增加分区时，分区的下界紧接上一个分区的上界。

   Range分区除了上述我们看到的单列分区，也支持**多列分区**，示例如下：

   ```text
   PARTITION BY RANGE(`date`, `id`)
   (
       PARTITION `p201701_1000` VALUES LESS THAN ("2017-02-01", "1000"),
       PARTITION `p201702_2000` VALUES LESS THAN ("2017-03-01", "2000"),
       PARTITION `p201703_all`  VALUES LESS THAN ("2017-04-01")
   )
   ```
   
   在以上示例中，我们指定 `date`(DATE 类型) 和 `id`(INT 类型) 作为分区列。以上示例最终得到的分区如下：
   
   ```
   * p201701_1000:    [(MIN_VALUE,  MIN_VALUE), ("2017-02-01", "1000")   )
   * p201702_2000:    [("2017-02-01", "1000"),  ("2017-03-01", "2000")   )
   * p201703_all:     [("2017-03-01", "2000"),  ("2017-04-01", MIN_VALUE)) 
   ```
   
   注意，最后一个分区用户缺省只指定了 `date` 列的分区值，所以 `id` 列的分区值会默认填充 `MIN_VALUE`。当用户插入数据时，分区列值会按照顺序依次比较，最终得到对应的分区。举例如下：
   
   ```
   * 数据  -->  分区
   * 2017-01-01, 200     --> p201701_1000
   * 2017-01-01, 2000    --> p201701_1000
   * 2017-02-01, 100     --> p201701_1000
   * 2017-02-01, 2000    --> p201702_2000
   * 2017-02-15, 5000    --> p201702_2000
   * 2017-03-01, 2000    --> p201703_all
   * 2017-03-10, 1       --> p201703_all
   * 2017-04-01, 1000    --> 无法导入
   * 2017-05-01, 1000    --> 无法导入
   ```

   **List 分区**

   - 分区列支持 `BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DATE, DATETIME, CHAR, VARCHAR` 数据类型，分区值为枚举值。只有当数据为目标分区枚举值其中之一时，才可以命中分区。

   - Partition 支持通过 `VALUES IN (...)` 来指定每个分区包含的枚举值。

   - 下面通过示例说明，进行分区的增删操作时，分区的变化。

     - 如上 `example_list_tbl` 示例，当建表完成后，会自动生成如下3个分区：

       ```text
       p_cn: ("Beijing", "Shanghai", "Hong Kong")
       p_usa: ("New York", "San Francisco")
       p_jp: ("Tokyo")
       ```

     - 当我们增加一个分区 p_uk VALUES IN ("London")，分区结果如下：

       ```text
       p_cn: ("Beijing", "Shanghai", "Hong Kong")
       p_usa: ("New York", "San Francisco")
       p_jp: ("Tokyo")
       p_uk: ("London")
       ```

     - 当我们删除分区 p_jp，分区结果如下：

       ```text
       p_cn: ("Beijing", "Shanghai", "Hong Kong")
       p_usa: ("New York", "San Francisco")
       p_uk: ("London")
       ```

   List分区也支持**多列分区**，示例如下：

   ```text
   PARTITION BY LIST(`id`, `city`)
   (
       PARTITION `p1_city` VALUES IN (("1", "Beijing"), ("1", "Shanghai")),
       PARTITION `p2_city` VALUES IN (("2", "Beijing"), ("2", "Shanghai")),
       PARTITION `p3_city` VALUES IN (("3", "Beijing"), ("3", "Shanghai"))
   )
   ```
   
   在以上示例中，我们指定 `id`(INT 类型) 和 `city`(VARCHAR 类型) 作为分区列。以上示例最终得到的分区如下：
   
   ```
   * p1_city: [("1", "Beijing"), ("1", "Shanghai")]
   * p2_city: [("2", "Beijing"), ("2", "Shanghai")]
   * p3_city: [("3", "Beijing"), ("3", "Shanghai")]
   ```
   
   当用户插入数据时，分区列值会按照顺序依次比较，最终得到对应的分区。举例如下：
   
   ```
   * 数据  --->  分区
   * 1, Beijing     ---> p1_city
   * 1, Shanghai    ---> p1_city
   * 2, Shanghai    ---> p2_city
   * 3, Beijing     ---> p3_city
   * 1, Tianjin     ---> 无法导入
   * 4, Beijing     ---> 无法导入
   ```

2. **Bucket**

   - 如果使用了 Partition，则 `DISTRIBUTED ...` 语句描述的是数据在**各个分区内**的划分规则。如果不使用 Partition，则描述的是对整个表的数据的划分规则。
   - 分桶列可以是多列，Aggregate 和 Unique 模型必须为 Key 列，Duplicate 模型可以是 key 列和 value 列。分桶列可以和 Partition 列相同或不同。
   - 分桶列的选择，是在 **查询吞吐** 和 **查询并发** 之间的一种权衡：
     1. 如果选择多个分桶列，则数据分布更均匀。如果一个查询条件不包含所有分桶列的等值条件，那么该查询会触发所有分桶同时扫描，这样查询的吞吐会增加，单个查询的延迟随之降低。这个方式适合大吞吐低并发的查询场景。
     2. 如果仅选择一个或少数分桶列，则对应的点查询可以仅触发一个分桶扫描。此时，当多个点查询并发时，这些查询有较大的概率分别触发不同的分桶扫描，各个查询之间的IO影响较小（尤其当不同桶分布在不同磁盘上时），所以这种方式适合高并发的点查询场景。
   - 分桶的数量理论上没有上限。

3. **关于 Partition 和 Bucket 的数量和数据量的建议。**

   - 一个表的 Tablet 总数量等于 (Partition num * Bucket num)。
   - 一个表的 Tablet 数量，在不考虑扩容的情况下，推荐略多于整个集群的磁盘数量。
   - 单个 Tablet 的数据量理论上没有上下界，但建议在 1G - 10G 的范围内。如果单个 Tablet 数据量过小，则数据的聚合效果不佳，且元数据管理压力大。如果数据量过大，则不利于副本的迁移、补齐，且会增加 Schema Change 或者 Rollup 操作失败重试的代价（这些操作失败重试的粒度是 Tablet）。
   - 当 Tablet 的数据量原则和数量原则冲突时，建议优先考虑数据量原则。
   - 在建表时，每个分区的 Bucket 数量统一指定。但是在动态增加分区时（`ADD PARTITION`），可以单独指定新分区的 Bucket 数量。可以利用这个功能方便的应对数据缩小或膨胀。
   - 一个 Partition 的 Bucket 数量一旦指定，不可更改。所以在确定 Bucket 数量时，需要预先考虑集群扩容的情况。比如当前只有 3 台 host，每台 host 有 1 块盘。如果 Bucket 的数量只设置为 3 或更小，那么后期即使再增加机器，也不能提高并发度。
   - 举一些例子：假设在有10台BE，每台BE一块磁盘的情况下。如果一个表总大小为 500MB，则可以考虑4-8个分片。5GB：8-16个分片。50GB：32个分片。500GB：建议分区，每个分区大小在 50GB 左右，每个分区16-32个分片。5TB：建议分区，每个分区大小在 50GB 左右，每个分区16-32个分片。

   > 注：表的数据量可以通过 [`SHOW DATA`](../sql-manual/sql-reference/Show-Statements/SHOW-DATA.md) 命令查看，结果除以副本数，即表的数据量。

4. **关于 Random Distribution 的设置以及使用场景。**   
    - 如果 OLAP 表没有更新类型的字段，将表的数据分桶模式设置为 RANDOM，则可以避免严重的数据倾斜(数据在导入表对应的分区的时候，单次导入作业每个 batch 的数据将随机选择一个tablet进行写入)。
    - 当表的分桶模式被设置为RANDOM 时，因为没有分桶列，无法根据分桶列的值仅对几个分桶查询，对表进行查询的时候将对命中分区的全部分桶同时扫描，该设置适合对表数据整体的聚合查询分析而不适合高并发的点查询。
    - 如果 OLAP 表的是 Random Distribution 的数据分布，那么在数据导入的时候可以设置单分片导入模式（将 `load_to_single_tablet` 设置为 true），那么在大数据量的导入的时候，一个任务在将数据写入对应的分区时将只写入一个分片，这样将能提高数据导入的并发度和吞吐量，减少数据导入和 Compaction
    导致的写放大问题，保障集群的稳定性。 

#### 复合分区与单分区

复合分区

- 第一级称为 Partition，即分区。用户可以指定某一维度列作为分区列（当前只支持整型和时间类型的列），并指定每个分区的取值范围。
- 第二级称为 Distribution，即分桶。用户可以指定一个或多个维度列以及桶数对数据进行 HASH 分布 或者不指定分桶列设置成 Random Distribution 对数据进行随机分布。

以下场景推荐使用复合分区

- 有时间维度或类似带有有序值的维度，可以以这类维度列作为分区列。分区粒度可以根据导入频次、分区数据量等进行评估。
- 历史数据删除需求：如有删除历史数据的需求（比如仅保留最近N 天的数据）。使用复合分区，可以通过删除历史分区来达到目的。也可以通过在指定分区内发送 DELETE 语句进行数据删除。
- 解决数据倾斜问题：每个分区可以单独指定分桶数量。如按天分区，当每天的数据量差异很大时，可以通过指定分区的分桶数，合理划分不同分区的数据,分桶列建议选择区分度大的列。

用户也可以不使用复合分区，即使用单分区。则数据只做 HASH 分布。

### PROPERTIES

在建表语句的最后 PROPERTIES 中，关于PROPERTIES中可以设置的相关参数，我们可以查看[CREATE TABLE](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md)中查看详细的介绍。

### ENGINE

本示例中，ENGINE 的类型是 olap，即默认的 ENGINE 类型。在 Doris 中，只有这个 ENGINE 类型是由 Doris 负责数据管理和存储的。其他 ENGINE 类型，如 mysql、broker、es 等等，本质上只是对外部其他数据库或系统中的表的映射，以保证 Doris 可以读取这些数据。而 Doris 本身并不创建、管理和存储任何非 olap ENGINE 类型的表和数据。

### 其他

```text
`IF NOT EXISTS` 表示如果没有创建过该表，则创建。注意这里只判断表名是否存在，而不会判断新建表结构是否与已存在的表结构相同。所以如果存在一个同名但不同构的表，该命令也会返回成功，但并不代表已经创建了新的表和新的结构。
```

## 常见问题

### 建表操作常见问题

1. 如果在较长的建表语句中出现语法错误，可能会出现语法错误提示不全的现象。这里罗列可能的语法错误供手动纠错：

   - 语法结构错误。请仔细阅读 `HELP CREATE TABLE;`，检查相关语法结构。
   - 保留字。当用户自定义名称遇到保留字时，需要用反引号 `` 引起来。建议所有自定义名称使用这个符号引起来。
   - 中文字符或全角字符。非 utf8 编码的中文字符，或隐藏的全角字符（空格，标点等）会导致语法错误。建议使用带有显示不可见字符的文本编辑器进行检查。

2. `Failed to create partition [xxx] . Timeout`

   Doris 建表是按照 Partition 粒度依次创建的。当一个 Partition 创建失败时，可能会报这个错误。即使不使用 Partition，当建表出现问题时，也会报 `Failed to create partition`，因为如前文所述，Doris 会为没有指定 Partition 的表创建一个不可更改的默认的 Partition。

   当遇到这个错误是，通常是 BE 在创建数据分片时遇到了问题。可以参照以下步骤排查：

   - 在 fe.log 中，查找对应时间点的 `Failed to create partition` 日志。在该日志中，会出现一系列类似 `{10001-10010}` 字样的数字对。数字对的第一个数字表示 Backend ID，第二个数字表示 Tablet ID。如上这个数字对，表示 ID 为 10001 的 Backend 上，创建 ID 为 10010 的 Tablet 失败了。
   - 前往对应 Backend 的 be.INFO 日志，查找对应时间段内，tablet id 相关的日志，可以找到错误信息。
   - 以下罗列一些常见的 tablet 创建失败错误，包括但不限于：
     - BE 没有收到相关 task，此时无法在 be.INFO 中找到 tablet id 相关日志或者 BE 创建成功，但汇报失败。以上问题，请参阅 [安装与部署](../install/install-deploy.md) 检查 FE 和 BE 的连通性。
     - 预分配内存失败。可能是表中一行的字节长度超过了 100KB。
     - `Too many open files`。打开的文件句柄数超过了 Linux 系统限制。需修改 Linux 系统的句柄数限制。

   如果创建数据分片时超时，也可以通过在 fe.conf 中设置 `tablet_create_timeout_second=xxx` 以及 `max_create_table_timeout_second=xxx` 来延长超时时间。其中 `tablet_create_timeout_second` 默认是1秒, `max_create_table_timeout_second` 默认是60秒，总体的超时时间为min(tablet_create_timeout_second * replication_num, max_create_table_timeout_second)，具体参数设置可参阅 [FE配置项](../admin-manual/config/fe-config.md) 。

3. 建表命令长时间不返回结果。

   Doris 的建表命令是同步命令。该命令的超时时间目前设置的比较简单，即（tablet num * replication num）秒。如果创建较多的数据分片，并且其中有分片创建失败，则可能导致等待较长超时后，才会返回错误。

   正常情况下，建表语句会在几秒或十几秒内返回。如果超过一分钟，建议直接取消掉这个操作，前往 FE 或 BE 的日志查看相关错误。

## 更多帮助

关于数据划分更多的详细说明，我们可以在[CREATE TABLE](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md)命令手册中查阅，也可以在Mysql客户端下输入 `HELP CREATE TABLE;` 获取更多的帮助信息。
