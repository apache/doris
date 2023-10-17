---
{
"title": "统计信息",
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

# 统计信息

## 统计信息简介

在 SQL 数据库中，查询优化器的质量对系统性能有重要影响。优化器需要依据统计信息来估算查询代价，尤其在等值查询场景中，精确估算基数是非常重要的，基数估算可以帮助优化器选择最优的查询计划，从而提高查询性能。

在执行一个查询时，未经过充分优化的执行计划和经过优化的执行计划可能会导致执行时间上的巨大差异，这个差距可以高达数倍。因此，对于 SQL 查询优化器来说，收集和分析统计信息是非常重要的，只有通过这些统计信息，优化器才能够准确地评估不同执行计划的成本，并选择最佳的执行计划。

Doris 查询优化器使用统计信息来确定查询最有效的执行计划。Doris 维护的统计信息包括表级别的统计信息和列级别的统计信息。

表统计信息：

| 信息                | 描述                       |
| :------------------ | :------------------------- |
| `row_count`         | 表的行数                   |
| `data_size`         | 表的⼤⼩（单位 byte）      |
| `update_rows`       | 收集统计信息后所更新的行数 |
| `healthy`           | 表的健康度                 |
| `update_time`       | 最近更新的时间             |
| `last_analyze_time` | 上次收集统计信息的时间     |

> 表的健康度：表示表统计信息的健康程度。当 `update_rows` 大于等于 `row_count` 时，健康度为 0；当 `update_rows` 小于 `row_count` 时，健康度为 `100 * (1 - update_rows / row_count)` 。

列统计信息：

| 信息            | 描述                       |
| :-------------- | :------------------------- |
| `row_count`     | 列的总行数                 |
| `data_size`     | 列的总⻓度（单位 byte）    |
| `avg_size_byte` | 列的平均⻓度（单位 bytes） |
| `ndv`           | 列 num distinct value      |
| `min`           | 列最小值                   |
| `max`           | 列最⼤值                   |
| `null_count`    | 列 null 个数               |
| `histogram`     | 列直方图                   |

接下来将简单介绍其中出现的直方图等数据结构，以及详细介绍统计信息的收集和维护。

## 直方图简介

直方图（histogram）是一种用于描述数据分布情况的工具，它将数据根据大小分成若干个区间（桶），并使用简单的统计量来表示每个区间中数据的特征。是数据库中的一种重要的统计信息，可以描述列中的数据分布情况。直方图最典型的应用场景是通过估算查询谓词的选择率来帮助优化器选择最优的执行计划。

在 Doris 中，会对每个表具体的列构建一个等高直方图（Equi-height Histogram）。直方图包括一系列桶，​ 其中每个桶的统计量包括桶的上下界、桶包含的元素数量、前面桶所有元素的数量、桶中不同值的个数。具体可以参考 SQL 函数 `histogram` 或者 `hist` 的使用说明。

> 采用等高直方图的分桶方法，每个桶中的数值频次之和都应该接近于总行数的 `1/N`。但是，如果严格遵守等高原则进行分桶，会出现某些值落在桶的边界上的情况，导致同一个值出现在两个不同的桶中。这种情况会对选择率的估算造成干扰。因此，在实现中，Doris 对等高直方图的分桶方法进行了修改：如果将一个值加入到某个桶中导致该桶中的数据频次超过了总行数的 `1/N`，则根据哪种情况更接近 `1/N`，将该值放入该桶或下一个桶中。

## 收集统计信息

### 手动收集

⽤户通过 `ANALYZE` 语句触发手动收集任务，根据提供的参数，收集指定的表或列的统计信息。

列统计信息收集语法：

```SQL
ANALYZE < TABLE | DATABASE table_name | db_name > 
    [ PARTITIONS [(*) | (partition_name [, ...]) | WITH RECENT COUNT ] ]
    [ (column_name [, ...]) ]
    [ [ WITH SYNC ] [ WITH SAMPLE PERCENT | ROWS ] [ WITH SQL ] ]
    [ PROPERTIES ("key" = "value", ...) ];
```

其中：

- table_name: 指定的的目标表。可以是  `db_name.table_name`  形式。
- column_name: 指定的目标列。必须是  `table_name`  中存在的列，多个列名称用逗号分隔。
- sync：同步收集统计信息。收集完后返回。若不指定则异步执行并返回任务 ID。
- incremental：增量收集统计信息。不支持增量收集直方图统计信息。
- period：周期性收集统计信息。单位为秒，指定后会定期收集相应的统计信息。
- sample percent | rows：抽样收集统计信息。可以指定抽样比例或者抽样行数。
- sql：执行sql来收集外表分区列统计信息。默认从元数据收集分区列信息，这样效率比较高但是行数和数据量大小可能不准。用户可以指定使用sql来收集，这样可以收集到准确的分区列信息。

增量收集适合类似时间列这样的单调不减列作为分区的表，或者历史分区数据不会更新的表。

注意：

- 直方图统计信息不支持增量收集。
- 使用增量收集时，必须保证表存量的统计信息可用（即其他历史分区数据不发生变化），否则会导致统计信息有误差。

示例：

- 增量收集 `example_tbl` 表的统计信息，使用以下语法：

```SQL
-- 使用with incremental
mysql> ANALYZE TABLE stats_test.example_tbl WITH INCREMENTAL;
+--------+
| job_id |
+--------+
| 51910  |
+--------+

-- 配置incremental
mysql> ANALYZE TABLE stats_test.example_tbl PROPERTIES("incremental" = "true");
+--------+
| job_id |
+--------+
| 51910  |
+--------+
```

- 增量收集 `example_tbl` 表 `city`, `age`, `sex` 列的统计信息，使用以下语法：

```SQL
mysql> ANALYZE TABLE stats_test.example_tbl(city, age, sex) WITH INCREMENTAL;
+--------+
| job_id |
+--------+
| 51988  |
+--------+
```

#### 抽样收集

在表数据量较大时，系统收集统计信息可能会比较耗时，可以使用抽样收集来提高统计信息收集的速度。根据实际情况指定抽样的比例或者抽样的行数。

示例：

- 抽样收集 `example_tbl` 表的统计信息，使用以下语法：

```SQL
-- 使用with sample rows抽样行数
mysql> ANALYZE TABLE stats_test.example_tbl WITH SAMPLE ROWS 5;
+--------+
| job_id |
+--------+
| 52120  |
+--------+

-- 使用with sample percent抽样比例
mysql> ANALYZE TABLE stats_test.example_tbl WITH SAMPLE PERCENT 50;
+--------+
| job_id |
+--------+
| 52201  |
+--------+

-- 配置sample.row抽样行数
mysql> ANALYZE TABLE stats_test.example_tbl PROPERTIES("sample.rows" = "5");
+--------+
| job_id |
+--------+
| 52279  |
+--------+

-- 配置sample.percent抽样比例
mysql> ANALYZE TABLE stats_test.example_tbl PROPERTIES("sample.percent" = "50");
+--------+
| job_id |
+--------+
| 52282  |
+--------+
```

- 抽样收集 `example_tbl` 表的直方图信息，与普通统计信息类似，使用以下语法：

```SQL
mysql> ANALYZE TABLE stats_test.example_tbl UPDATE HISTOGRAM WITH SAMPLE ROWS 5;
+--------+
| job_id |
+--------+
| 52357  |
+--------+
```

#### 同步收集

一般执行 `ANALYZE` 语句后系统会启动异步任务去收集统计信息并立刻返回统计任务 ID。如果想要等待统计信息收集结束后返会，可以使用同步收集方式。

示例：

- 抽样收集 `example_tbl` 表的统计信息，使用以下语法：

```SQL
-- 使用with sync
mysql> ANALYZE TABLE stats_test.example_tbl WITH SYNC;

-- 配置sync
mysql> ANALYZE TABLE stats_test.example_tbl PROPERTIES("sync" = "true");
```

- 抽样收集 `example_tbl` 表的直方图信息，与普通统计信息类似，使用以下语法：

```SQL
mysql> ANALYZE TABLE stats_test.example_tbl UPDATE HISTOGRAM WITH SYNC;
```

### 自动收集

自动收集是指用户在执行 `ANALYZE` 语句时，指定 `PERIOD` 或者 `AUTO` 关键字或者进行相关配置时，系统后续将自动生成任务，进行统计信息的收集。

#### 周期性收集

周期性收集是指在一定时间间隔内，重新收集表相应的统计信息。

示例：

- 周期性（每隔一天）收集 `example_tbl` 表的统计信息，使用以下语法：

```SQL
-- 使用with period
mysql> ANALYZE TABLE stats_test.example_tbl WITH PERIOD 86400;
+--------+
| job_id |
+--------+
| 52409  |
+--------+

-- 配置period.seconds
mysql> ANALYZE TABLE stats_test.example_tbl PROPERTIES("period.seconds" = "86400");
+--------+
| job_id |
+--------+
| 52535  |
+--------+
```

- 周期性（每隔一天）收集 `example_tbl` 表的直方图信息，与普通统计信息类似，使用以下语法：

```SQL
mysql> ANALYZE TABLE stats_test.example_tbl UPDATE HISTOGRAM WITH PERIOD 86400;
+--------+
| job_id |
+--------+
| 52684  |
+--------+
```

#### 自动收集

表发生变更时可能会导致统计信息“失效”，可能会导致优化器选择错误的执行计划。

导致表统计信息失效的原因包括：

- 新增字段：新增字段无统计信息
- 字段变更：原有统计信息不可用
- 新增分区：新增分区无统计信息
- 分区变更：原有统计信息失效
- 数据变更(插入数据 | 删除数据 | 更改数据)：统计信息有误差

主要涉及的操作包括：

- update：更新数据
- delete：删除数据
- drop：删除分区
- load：导入数据、新增分区
- insert：插入数据、新增分区
- alter：字段变更、分区变更、新增分区

其中库、表、分区、字段删除，内部会自动清除这些无效的统计信息。调整列顺序以及修改列类型不影响。

系统根据表的健康度（参考上文定义）来决定是否需要重新收集统计信息。我们通过设置健康度阈值，当健康度低于某个值时系统将重新收集表对应的统计信息。简单来讲就是对于收集过统计信息的表，如果某一个分区数据变多/变少、或者新增/删除分区，都有可能触发统计信息的自动收集，重新收集后更新表的统计信息和健康度。目前只会收集用户配置了自动收集统计信息的表，其他表不会自动收集统计信息。

示例：

- 自动收集 `example_tbl` 表的统计信息，使用以下语法：

```SQL
-- 使用with auto
mysql> ANALYZE TABLE stats_test.example_tbl WITH AUTO;
+--------+
| job_id |
+--------+
| 52539  |
+--------+

-- 配置automatic
mysql> ANALYZE TABLE stats_test.example_tbl PROPERTIES("automatic" = "true");
+--------+
| job_id |
+--------+
| 52565  |
+--------+
```

### 管理任务

#### 查看统计任务

通过 `SHOW ANALYZE` 来查看统计信息收集任务的信息。

语法如下：

```SQL
SHOW ANALYZE [ table_name | job_id ]
    [ WHERE [ STATE = [ "PENDING" | "RUNNING" | "FINISHED" | "FAILED" ] ] ];
```

其中：

- table_name：表名，指定后可查看该表对应的统计任务信息。可以是  `db_name.table_name`  形式。不指定时返回所有统计任务信息。
- job_id：统计信息任务 ID，执行 `ANALYZE` 非同步收集统计信息时所返回的值。不指定时返回所有统计任务信息。

目前 `SHOW ANALYZE` 会输出 11 列，具体如下：

| 列名                   | 说明         |
| :--------------------- | :----------- |
| `job_id`               | 统计任务 ID  |
| `catalog_name`         | catalog 名称 |
| `db_name`              | 数据库名称   |
| `tbl_name`             | 表名称       |
| `col_name`             | 列名称       |
| `job_type`             | 任务类型     |
| `analysis_type`        | 统计类型     |
| `message`              | 任务信息     |
| `last_exec_time_in_ms` | 上次执行时间 |
| `state`                | 任务状态     |
| `schedule_type`        | 调度方式     |

> 在系统中，统计信息任务包含多个子任务，每个子任务单独收集一列的统计信息。

示例：

- 查看 ID 为 `20038` 的统计任务信息，使用以下语法：

```SQL
mysql> SHOW ANALYZE 20038 
+--------+--------------+----------------------+----------+-----------------------+----------+---------------+---------+----------------------+----------+---------------+
| job_id | catalog_name | db_name              | tbl_name | col_name              | job_type | analysis_type | message | last_exec_time_in_ms | state    | schedule_type |
+--------+--------------+----------------------+----------+-----------------------+----------+---------------+---------+----------------------+----------+---------------+
| 20038  | internal     | default_cluster:test | t3       | [col4,col2,col3,col1] | MANUAL   | FUNDAMENTALS  |         | 2023-06-01 17:22:15  | FINISHED | ONCE          |
+--------+--------------+----------------------+----------+-----------------------+----------+---------------+---------+----------------------+----------+---------------+

```

可通过`SHOW ANALYZE TASK STATUS [job_id]`，查看具体每个列统计信息的收集完成情况。

```
mysql> show analyze task status  20038 ;
+---------+----------+---------+----------------------+----------+
| task_id | col_name | message | last_exec_time_in_ms | state    |
+---------+----------+---------+----------------------+----------+
| 20039   | col4     |         | 2023-06-01 17:22:15  | FINISHED |
| 20040   | col2     |         | 2023-06-01 17:22:15  | FINISHED |
| 20041   | col3     |         | 2023-06-01 17:22:15  | FINISHED |
| 20042   | col1     |         | 2023-06-01 17:22:15  | FINISHED |
+---------+----------+---------+----------------------+----------+


```

- 查看 `example_tbl` 表的的统计任务信息，使用以下语法：

```SQL
mysql> SHOW ANALYZE stats_test.example_tbl;
+--------+--------------+----------------------------+-------------+-----------------+----------+---------------+---------+----------------------+----------+---------------+
| job_id | catalog_name | db_name                    | tbl_name    | col_name        | job_type | analysis_type | message | last_exec_time_in_ms | state    | schedule_type |
+--------+--------------+----------------------------+-------------+-----------------+----------+---------------+---------+----------------------+----------+---------------+
| 68603  | internal     | default_cluster:stats_test | example_tbl |                 | MANUAL   | INDEX         |         | 2023-05-05 17:53:27  | FINISHED | ONCE          |
| 68603  | internal     | default_cluster:stats_test | example_tbl | last_visit_date | MANUAL   | COLUMN        |         | 2023-05-05 17:53:26  | FINISHED | ONCE          |
| 68603  | internal     | default_cluster:stats_test | example_tbl | age             | MANUAL   | COLUMN        |         | 2023-05-05 17:53:27  | FINISHED | ONCE          |
| 68603  | internal     | default_cluster:stats_test | example_tbl | city            | MANUAL   | COLUMN        |         | 2023-05-05 17:53:25  | FINISHED | ONCE          |
| 68603  | internal     | default_cluster:stats_test | example_tbl | cost            | MANUAL   | COLUMN        |         | 2023-05-05 17:53:27  | FINISHED | ONCE          |
| 68603  | internal     | default_cluster:stats_test | example_tbl | min_dwell_time  | MANUAL   | COLUMN        |         | 2023-05-05 17:53:24  | FINISHED | ONCE          |
| 68603  | internal     | default_cluster:stats_test | example_tbl | date            | MANUAL   | COLUMN        |         | 2023-05-05 17:53:27  | FINISHED | ONCE          |
| 68603  | internal     | default_cluster:stats_test | example_tbl | user_id         | MANUAL   | COLUMN        |         | 2023-05-05 17:53:25  | FINISHED | ONCE          |
| 68603  | internal     | default_cluster:stats_test | example_tbl | max_dwell_time  | MANUAL   | COLUMN        |         | 2023-05-05 17:53:26  | FINISHED | ONCE          |
| 68603  | internal     | default_cluster:stats_test | example_tbl | sex             | MANUAL   | COLUMN        |         | 2023-05-05 17:53:26  | FINISHED | ONCE          |
| 68678  | internal     | default_cluster:stats_test | example_tbl | user_id         | MANUAL   | HISTOGRAM     |         | 2023-05-05 18:00:11  | FINISHED | ONCE          |
| 68678  | internal     | default_cluster:stats_test | example_tbl | sex             | MANUAL   | HISTOGRAM     |         | 2023-05-05 18:00:09  | FINISHED | ONCE          |
| 68678  | internal     | default_cluster:stats_test | example_tbl | last_visit_date | MANUAL   | HISTOGRAM     |         | 2023-05-05 18:00:10  | FINISHED | ONCE          |
| 68678  | internal     | default_cluster:stats_test | example_tbl | date            | MANUAL   | HISTOGRAM     |         | 2023-05-05 18:00:10  | FINISHED | ONCE          |
| 68678  | internal     | default_cluster:stats_test | example_tbl | cost            | MANUAL   | HISTOGRAM     |         | 2023-05-05 18:00:10  | FINISHED | ONCE          |
| 68678  | internal     | default_cluster:stats_test | example_tbl | age             | MANUAL   | HISTOGRAM     |         | 2023-05-05 18:00:10  | FINISHED | ONCE          |
| 68678  | internal     | default_cluster:stats_test | example_tbl | min_dwell_time  | MANUAL   | HISTOGRAM     |         | 2023-05-05 18:00:10  | FINISHED | ONCE          |
| 68678  | internal     | default_cluster:stats_test | example_tbl | max_dwell_time  | MANUAL   | HISTOGRAM     |         | 2023-05-05 18:00:09  | FINISHED | ONCE          |
| 68678  | internal     | default_cluster:stats_test | example_tbl |                 | MANUAL   | HISTOGRAM     |         | 2023-05-05 18:00:11  | FINISHED | ONCE          |
| 68678  | internal     | default_cluster:stats_test | example_tbl | city            | MANUAL   | HISTOGRAM     |         | 2023-05-05 18:00:11  | FINISHED | ONCE          |
+--------+--------------+----------------------------+-------------+-----------------+----------+---------------+---------+----------------------+----------+---------------+
```

- 查看所有的统计任务信息，并按照上次完成时间降序，返回前 3 条信息，使用以下语法：

```SQL
mysql> SHOW ANALYZE WHERE state = "FINISHED" ORDER BY last_exec_time_in_ms DESC LIMIT 3;
+--------+--------------+----------------------------+-------------+-----------------+----------+---------------+---------+----------------------+----------+---------------+
| job_id | catalog_name | db_name                    | tbl_name    | col_name        | job_type | analysis_type | message | last_exec_time_in_ms | state    | schedule_type |
+--------+--------------+----------------------------+-------------+-----------------+----------+---------------+---------+----------------------+----------+---------------+
| 68603  | internal     | default_cluster:stats_test | example_tbl | age             | MANUAL   | COLUMN        |         | 2023-05-05 17:53:27  | FINISHED | ONCE          |
| 68603  | internal     | default_cluster:stats_test | example_tbl | sex             | MANUAL   | COLUMN        |         | 2023-05-05 17:53:26  | FINISHED | ONCE          |
| 68603  | internal     | default_cluster:stats_test | example_tbl | last_visit_date | MANUAL   | COLUMN        |         | 2023-05-05 17:53:26  | FINISHED | ONCE          |
+--------+--------------+----------------------------+-------------+-----------------+----------+---------------+---------+----------------------+----------+---------------+
```

#### 终止统计任务

通过 `KILL ANALYZE` 来终止正在运行的统计任务。

语法如下：

```SQL
KILL ANALYZE job_id;
```

其中：

- job_id：统计信息任务 ID。执行 `ANALYZE` 非同步收集统计信息时所返回的值，也可以通过 `SHOW ANALYZE` 语句获取。

示例：

- 终止 ID 为 52357 的统计任务。

```SQL
mysql> KILL ANALYZE 52357;
```

## 查看统计信息

### 表统计信息

> 暂不可用。

通过 `SHOW TABLE STATS` 来查看表的总行数以及统计信息健康度等信息。

语法如下：

```SQL
SHOW TABLE STATS table_name [ PARTITION (partition_name) ];
```

其中：

- table_name: 导入数据的目标表。可以是  `db_name.table_name`  形式。
- partition_name: 指定的目标分区。必须是  `table_name`  中存在的分区，只能指定一个分区。

目前 `SHOW TABLE STATS` 会输出 6 列，具体如下：

| 列名                | 说明                   |
| :------------------ | :--------------------- |
| `row_count`         | 行数                   |
| `update_rows`       | 更新的行数             |
| `data_size`         | 数据大小。单位 byte    |
| `healthy`           | 健康度                 |
| `update_time`       | 更新时间               |
| `last_analyze_time` | 上次收集统计信息的时间 |

示例：

- 查看 `example_tbl` 表的统计信息，使用以下语法：

```SQL
mysql> SHOW TABLE STATS stats_test.example_tbl;
+-----------+-------------+---------+-----------+---------------------+---------------------+
| row_count | update_rows | healthy | data_size | update_time         | last_analyze_time   |
+-----------+-------------+---------+-----------+---------------------+---------------------+
| 8         | 0           | 100     | 6999      | 2023-04-08 15:40:47 | 2023-04-08 17:43:28 |
+-----------+-------------+---------+-----------+---------------------+---------------------+
```

- 查看 `example_tbl` 表 `p_201701` 分区的统计信息，使用以下语法：

```SQL
mysql> SHOW TABLE STATS stats_test.example_tbl PARTITION (p_201701);
+-----------+-------------+---------+-----------+---------------------+---------------------+
| row_count | update_rows | healthy | data_size | update_time         | last_analyze_time   |
+-----------+-------------+---------+-----------+---------------------+---------------------+
| 4         | 0           | 100     | 2805      | 2023-04-08 11:48:02 | 2023-04-08 17:43:27 |
+-----------+-------------+---------+-----------+---------------------+---------------------+
```

### 查看列统计信息

通过 `SHOW COLUMN STATS` 来查看列的不同值数以及 `NULL` 数量等信息。

语法如下：

```SQL
SHOW COLUMN [cached] STATS table_name [ (column_name [, ...]) ] [ PARTITION (partition_name) ];
```

其中：

- cached: 展示当前FE内存缓存中的统计信息。
- table_name: 收集统计信息的目标表。可以是  `db_name.table_name`  形式。
- column_name: 指定的目标列，必须是  `table_name`  中存在的列，多个列名称用逗号分隔。
- partition_name: 指定的目标分区，必须是  `table_name`  中存在的分区，只能指定一个分区。

目前 `SHOW COLUMN STATS` 会输出 10 列，具体如下：

| 列名            | 说明                       |
| :-------------- | :------------------------- |
| `column_name`   | 列名称                     |
| `count`         | 列的总行数                 |
| `ndv`           | 不同值的个数               |
| `num_null`      | 空值的个数                 |
| `data_size`     | 列的总⻓度（单位 bytes）   |
| `avg_size_byte` | 列的平均⻓度（单位 bytes） |
| `min`           | 列最小值                   |
| `max`           | 列最⼤值                   |

示例：

- 查看 `example_tbl` 表所有列的统计信息，使用以下语法：

```SQL
mysql> SHOW COLUMN STATS stats_test.example_tbl;
+-----------------+-------+------+----------+-------------------+-------------------+-----------------------+-----------------------+
| column_name     | count | ndv  | num_null | data_size         | avg_size_byte     | min                   | max                   |
+-----------------+-------+------+----------+-------------------+-------------------+-----------------------+-----------------------+
| date            | 6.0   | 3.0  | 0.0      | 28.0              | 4.0               | '2017-10-01'          | '2017-10-03'          |
| cost            | 6.0   | 6.0  | 0.0      | 56.0              | 8.0               | 2                     | 200                   |
| min_dwell_time  | 6.0   | 6.0  | 0.0      | 28.0              | 4.0               | 2                     | 22                    |
| city            | 6.0   | 4.0  | 0.0      | 54.0              | 7.0               | 'Beijing'             | 'Shenzhen'            |
| user_id         | 6.0   | 5.0  | 0.0      | 112.0             | 16.0              | 10000                 | 10004                 |
| sex             | 6.0   | 2.0  | 0.0      | 7.0               | 1.0               | 0                     | 1                     |
| max_dwell_time  | 6.0   | 6.0  | 0.0      | 28.0              | 4.0               | 3                     | 22                    |
| last_visit_date | 6.0   | 6.0  | 0.0      | 112.0             | 16.0              | '2017-10-01 06:00:00' | '2017-10-03 10:20:22' |
| age             | 6.0   | 4.0  | 0.0      | 14.0              | 2.0               | 20                    | 35                    |
+-----------------+-------+------+----------+-------------------+-------------------+-----------------------+-----------------------+
```

- 查看 `example_tbl` 表 `p_201701` 分区的统计信息，使用以下语法：

```SQL
mysql> SHOW COLUMN STATS stats_test.example_tbl PARTITION (p_201701);
+-----------------+-------+------+----------+--------------------+-------------------+-----------------------+-----------------------+
| column_name     | count | ndv  | num_null | data_size          | avg_size_byte     | min                   | max                   |
+-----------------+-------+------+----------+--------------------+-------------------+-----------------------+-----------------------+
| date            | 3.0   | 1.0  | 0.0      | 16.0               | 4.0               | '2017-10-01'          | '2017-10-01'          |
| cost            | 3.0   | 3.0  | 0.0      | 32.0               | 8.0               | 2                     | 100                   |
| min_dwell_time  | 3.0   | 3.0  | 0.0      | 16.0               | 4.0               | 2                     | 22                    |
| city            | 3.0   | 2.0  | 0.0      | 29.0               | 7.0               | 'Beijing'             | 'Shenzhen'            |
| user_id         | 3.0   | 3.0  | 0.0      | 64.0               | 16.0              | 10000                 | 10004                 |
| sex             | 3.0   | 2.0  | 0.0      | 4.0                | 1.0               | 0                     | 1                     |
| max_dwell_time  | 3.0   | 3.0  | 0.0      | 16.0               | 4.0               | 3                     | 22                    |
| last_visit_date | 3.0   | 3.0  | 0.0      | 64.0               | 16.0              | '2017-10-01 06:00:00' | '2017-10-01 17:05:45' |
| age             | 3.0   | 3.0  | 0.0      | 8.0                | 2.0               | 20                    | 35                    |
+-----------------+-------+------+----------+--------------------+-------------------+-----------------------+-----------------------+
```

- 查看 `example_tbl` 表 `city`, `age`, `sex` 列的统计信息，使用以下语法：

```SQL
mysql> SHOW COLUMN STATS stats_test.example_tbl(city, age, sex);
+-------------+-------+------+----------+-------------------+-------------------+-----------+------------+
| column_name | count | ndv  | num_null | data_size         | avg_size_byte     | min       | max        |
+-------------+-------+------+----------+-------------------+-------------------+-----------+------------+
| city        | 6.0   | 4.0  | 0.0      | 54.0              | 7.0               | 'Beijing' | 'Shenzhen' |
| sex         | 6.0   | 2.0  | 0.0      | 7.0               | 1.0               | 0         | 1          |
| age         | 6.0   | 4.0  | 0.0      | 14.0              | 2.0               | 20        | 35         |
+-------------+-------+------+----------+-------------------+-------------------+-----------+------------+
```

- 查看 `example_tbl` 表 `p_201701` 分区 `city`, `age`, `sex` 列的统计信息，使用以下语法：

```SQL
mysql> SHOW COLUMN STATS stats_test.example_tbl(city, age, sex) PARTITION (p_201701);
+-------------+-------+------+----------+--------------------+-------------------+-----------+------------+
| column_name | count | ndv  | num_null | data_size          | avg_size_byte     | min       | max        |
+-------------+-------+------+----------+--------------------+-------------------+-----------+------------+
| city        | 3.0   | 2.0  | 0.0      | 29.0               | 7.0               | 'Beijing' | 'Shenzhen' |
| sex         | 3.0   | 2.0  | 0.0      | 4.0                | 1.0               | 0         | 1          |
| age         | 3.0   | 3.0  | 0.0      | 8.0                | 2.0               | 20        | 35         |
+-------------+-------+------+----------+--------------------+-------------------+-----------+------------+
```

### 查看列直方图信息

通过 `SHOW COLUMN HISTOGRAM` 来查看直方图每个桶的信息。

语法如下：

```SQL
SHOW COLUMN HISTOGRAM table_name [ (column_name [, ...]) ];
```

其中：

- table_name: 导入数据的目标表。可以是  `db_name.table_name`  形式。
- column_name: 指定的目标列，必须是  `table_name`  中存在的列，多个列名称用逗号分隔。

目前 `SHOW COLUMN HISTOGRAM` 会输出 5 列，其中每个 bucket 又包含 5 个属性，具体如下：

| 列名          | 说明                         |
| :------------ | :--------------------------- |
| `column_name` | 列名称                       |
| `data_type`   | 列的数据类型                 |
| `sample_rate` | 采用比例。默认全量收集时为 1 |
| `num_buckets` | 包含的桶数量                 |
| `buckets`     | 桶的详细信息（Json 格式）    |
| `lower`       | 桶的下界                     |
| `upper`       | 桶的上界                     |
| `count`       | 桶包含的元素数量             |
| `pre_sum`     | 前面桶所有元素的数量         |
| `ndv`         | 桶中不同值的个数             |

示例：

- 查看 `example_tbl` 表所有列的直方图信息，使用以下语法：

```SQL
mysql> SHOW COLUMN HISTOGRAM stats_test.example_tbl;
+-----------------+-------------+-------------+-------------+---------------------------------------------------------------------------------------------------------------+
| column_name     | data_type   | sample_rate | num_buckets | buckets                                                                                                       |
+-----------------+-------------+-------------+-------------+---------------------------------------------------------------------------------------------------------------+
| date            | DATE        | 1.0         | 1           | [{"lower_expr":"2017-10-01","upper_expr":"2017-10-03","count":6.0,"pre_sum":0.0,"ndv":3.0}]                   |
| cost            | BIGINT      | 1.0         | 1           | [{"lower_expr":"2","upper_expr":"200","count":6.0,"pre_sum":0.0,"ndv":6.0}]                                   |
| min_dwell_time  | INT         | 1.0         | 1           | [{"lower_expr":"2","upper_expr":"22","count":6.0,"pre_sum":0.0,"ndv":6.0}]                                    |
| city            | VARCHAR(20) | 1.0         | 1           | [{"lower_expr":"Shanghai","upper_expr":"Shenzhen","count":6.0,"pre_sum":0.0,"ndv":4.0}]                       |
| user_id         | LARGEINT    | 1.0         | 1           | [{"lower_expr":"10000","upper_expr":"10004","count":6.0,"pre_sum":0.0,"ndv":5.0}]                             |
| sex             | TINYINT     | 1.0         | 1           | [{"lower_expr":"0","upper_expr":"1","count":6.0,"pre_sum":0.0,"ndv":2.0}]                                     |
| max_dwell_time  | INT         | 1.0         | 1           | [{"lower_expr":"3","upper_expr":"22","count":6.0,"pre_sum":0.0,"ndv":6.0}]                                    |
| last_visit_date | DATETIME    | 1.0         | 1           | [{"lower_expr":"2017-10-01 06:00:00","upper_expr":"2017-10-03 10:20:22","count":6.0,"pre_sum":0.0,"ndv":6.0}] |
| age             | SMALLINT    | 1.0         | 1           | [{"lower_expr":"20","upper_expr":"35","count":6.0,"pre_sum":0.0,"ndv":4.0}]                                   |
+-----------------+-------------+-------------+-------------+---------------------------------------------------------------------------------------------------------------+
```

- 查看 `example_tbl` 表 `city`, `age`, `sex` 列的直方图信息，使用以下语法：

```SQL
mysql> SHOW COLUMN HISTOGRAM stats_test.example_tbl(city, age, sex);
+-------------+-------------+-------------+-------------+----------------------------------------------------------------------------------------+
| column_name | data_type   | sample_rate | num_buckets | buckets                                                                                |
+-------------+-------------+-------------+-------------+----------------------------------------------------------------------------------------+
| city        | VARCHAR(20) | 1.0         | 1           | [{"lower_expr":"Shanghai","upper_expr":"Shenzhen","count":6.0,"pre_sum":0.0,"ndv":4.0}]|
| sex         | TINYINT     | 1.0         | 1           | [{"lower_expr":"0","upper_expr":"1","count":6.0,"pre_sum":0.0,"ndv":2.0}]              |
| age         | SMALLINT    | 1.0         | 1           | [{"lower_expr":"20","upper_expr":"35","count":6.0,"pre_sum":0.0,"ndv":4.0}]            |
+-------------+-------------+-------------+-------------+----------------------------------------------------------------------------------------+
```

Buckets 说明：

> 每个列的 Buckets 都以 JSON 格式返回，Bucket 从小到大排列，每个 Bucket 包含上下界，元素数量，元素 NDV 以及前面所有桶的元素数量。其中列的元素数量（row_count） = 最后一个桶元素数量（count） + 前面所有桶的元素数量（pre_sum）。以下列的行数为 17。

```JSON
[
    {
        "lower_expr": 2,
        "upper_expr": 7,
        "count": 6,
        "pre_sum": 0,
        "ndv": 6
    },
    {
        "lower_expr": 10,
        "upper_expr": 20,
        "count": 11,
        "pre_sum": 6,
        "ndv": 11
    }
]
```

## 修改统计信息

⽤户通过 `ALTER` 语句修改统计信息，根据提供的参数，修改列相应的统计信息。

```SQL
ALTER TABLE table_name MODIFY COLUMN column_name SET STATS ('stat_name' = 'stat_value', ...) [ PARTITION (partition_name) ];
```

其中：

- table_name: 删除统计信息的目标表。可以是 `db_name.table_name` 形式。
- column_name: 指定的目标列，必须是 `table_name` 中存在的列，每次只能修改一列的统计信息。
- stat_name 和 stat_value: 相应的统计信息名称和统计信息信息的值，多个统计信息逗号分隔。可以修改的统计信息包括 `row_count`, `ndv`, `num_nulls`, `min_value`, `max_value`, `data_size`。
- partition_name: 指定的目标分区。必须是 `table_name` 中存在的分区，多个分区使用逗号分割。

示例：

- 修改 `example_tbl` 表 `age` 列 `row_count` 统计信息，使用以下语法：

```SQL
mysql> ALTER TABLE stats_test.example_tbl MODIFY COLUMN age SET STATS ('row_count'='6001215');
mysql> SHOW COLUMN STATS stats_test.example_tbl(age);
+-------------+-----------+------+----------+-----------+---------------+------+------+
| column_name | count     | ndv  | num_null | data_size | avg_size_byte | min  | max  |
+-------------+-----------+------+----------+-----------+---------------+------+------+
| age         | 6001215.0 | 0.0  | 0.0      | 0.0       | 0.0           | N/A  | N/A  |
+-------------+-----------+------+----------+-----------+---------------+------+------+
```

- 修改 `example_tbl` 表 `age` 列 `row_count`, `num_nulls`, `data_size` 统计信息，使用以下语法：

```SQL
mysql> ALTER TABLE stats_test.example_tbl MODIFY COLUMN age SET STATS ('row_count'='6001215', 'num_nulls'='2023', 'data_size'='600121522');
mysql> SHOW COLUMN STATS stats_test.example_tbl(age);
+-------------+-----------+------+----------+-----------+---------------+------+------+
| column_name | count     | ndv  | num_null | data_size | avg_size_byte | min  | max  |
+-------------+-----------+------+----------+-----------+---------------+------+------+
| age         | 6001215.0 | 0.0  | 2023.0   | 600121522 | 0.0           | N/A  | N/A  |
+-------------+-----------+------+----------+-----------+---------------+------+------+
```

## 删除统计信息

⽤户通过 `DROP` 语句删除统计信息，根据提供的参数，删除指定的表、分区或列的统计信息。删除时会同时删除列统计信息和列直方图信息。

语法：

```SQL
DROP [ EXPIRED ] STATS [ table_name [ (column_name [, ...]) ] ];
```

其中：

- table_name: 要删除统计信息的目标表。可以是  `db_name.table_name`  形式。
- column_name: 指定的目标列。必须是  `table_name`  中存在的列，多个列名称用逗号分隔。
- expired：统计信息清理。不能指定表，会删除系统中无效的统计信息以及过期的统计任务信息。

示例：

- 清理统计信息，使用以下语法：

```SQL
mysql> DROP EXPIRED STATS;
```

- 删除 `example_tbl` 表的统计信息，使用以下语法：

```SQL
mysql> DROP STATS stats_test.example_tbl;
```

- 删除 `example_tbl` 表 `city`, `age`, `sex` 列的统计信息，使用以下语法：

```SQL
mysql> DROP STATS stats_test.example_tbl(city, age, sex);
```

## ANALYZE 配置项

用于根据job id删除自动/周期Analyze作业

```sql
DROP ANALYZE JOB [JOB_ID]
```

## Full auto analyze

用户可以使用选项 `enable_full_auto_analyze` 来决定是否启用Full auto analyze。如果启用，Doris会自动分析除了一些内部数据库（如`information_db`等）之外的所有数据库，并忽略AUTO作业。默认情况下，该选项为true。

## Other ANALYZE configuration item

| conf                                                    | comment                                                                                                                                                                                                                                                                                             | default value                  |
|---------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------|
| statistics_sql_parallel_exec_instance_num               | 控制每个统计信息收集SQL在BE侧的并发实例数/pipeline task num                                                                                                                                                                                                                                                           | 1                              |
| statistics_sql_mem_limit_in_bytes                       | 控制每个统计信息SQL可占用的BE内存                                                                                                                                                                                                                                                                                 | 2L * 1024 * 1024 * 1024 (2GiB) |
| statistics_simultaneously_running_task_num              | 通过`ANALYZE TABLE[DATABASE]`提交异步作业后，可同时analyze的列的数量，所有异步任务共同受到该参数约束                                                                                                                                                                                                                                  | 5                              |
| analyze_task_timeout_in_minutes                         | AnalyzeTask执行超时时间                                                                                                                                                                                                                                                                                   | 12 hours                       |
| full_auto_analyze_start_time/full_auto_analyze_end_time | Full auto analyze 执行时间范围，该时间段之外的时间不会触发full auto analyze                                                                                                                                                                                                                                             | 00:00:00-02:00:00              |                      |
|stats_cache_size| 统计信息缓存的实际内存占用大小高度依赖于数据的特性，因为在不同的数据集和场景中，最大/最小值的平均大小和直方图的桶数量会有很大的差异。此外，JVM版本等因素也会对其产生影响。在这里，我将给出统计信息缓存在包含10_0000个项目时所占用的内存大小。每个项目的最大/最小值的平均长度为32，列名的平均长度为16，并且每个列都有一个具有128个桶的直方图。在这种情况下，统计信息缓存总共占用了911.954833984MiB的内存。如果没有直方图，统计信息缓存总共占用了61.2777404785MiB的内存。强烈不建议分析具有非常大字符串值的列，因为这可能导致FE内存溢出（OOM）。 | 10_0000                        |
|full_auto_analyze_simultaneously_running_task_num| 控制并发执行的full auto analyze                                                                                                                                                                                                                                                                            | 5                              |
|parallel_sync_analyze_task_num| 每次通过`ANALYZE...WITH SYNC`提交同步作业时，可同时analyze的列的数量                                                                                                                                                                                                                                                    | 2                              |

## 常见问题

### ANALYZE WITH SYNC 执行失败：Failed to analyze following columns...

SQL执行时间受`query_timeout`会话变量控制，该变量默认值为300秒，`ANALYZE DATABASE/TABLE`等语句通常耗时较大，很容易超过该时间限制而被cancel，建议根据ANALYZE对象的数据量适当增大`query_timeout`的值。

### ANALYZE提交报错：Stats table not available...

执行ANALYZE时统计数据会被写入到内部表`__internal_schema.column_statistics`中，FE会在执行ANALYZE前检查该表tablet状态，如果存在不可用的tablet则拒绝执行任务。出现该报错请检查BE集群状态。
