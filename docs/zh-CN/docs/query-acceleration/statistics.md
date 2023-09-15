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


通过收集统计信息有助于优化器了解数据分布特性，在进行CBO（基于成本优化）时优化器会利用这些统计信息来计算谓词的选择性，并估算每个执行计划的成本。从而选择更优的计划以大幅提升查询效率。

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

## 收集统计信息

### 使用ANALYZE语句

⽤户通过 `ANALYZE` 语句触发手动收集任务，根据提供的参数，收集指定的表或列的统计信息。

列统计信息收集语法：

```SQL
ANALYZE < TABLE | DATABASE table_name | db_name > 
    [ PARTITIONS [(*) | (partition_name [, ...]) | WITH RECENT COUNT] ]
    [ (column_name [, ...]) ]
    [ [ WITH SYNC ] [ WITH SAMPLE PERCENT | ROWS ]]
    [ PROPERTIES ("key" = "value", ...) ];
```

其中：

- table_name: 指定的的目标表。可以是  `db_name.table_name`  形式。
- partition_name: 指定的目标分区（目前只针对Hive外表）。必须是  `table_name`  中存在的分区，多个列名称用逗号分隔。分区名样例: 单层分区PARTITIONS(`event_date=20230706`)，多层分区PARTITIONS(`nation=CN/city=Beijing`)。PARTITIONS (*)指定所有分区，PARTITIONS WITH RECENT 100指定最新的100个分区。
- column_name: 指定的目标列。必须是  `table_name`  中存在的列，多个列名称用逗号分隔。
- sync：同步收集统计信息。收集完后返回。若不指定则异步执行并返回任务 ID。
- sample percent | rows：抽样收集统计信息。可以指定抽样比例或者抽样行数。


### 自动收集

// TODO


### 管理任务

#### 查看统计任务

通过 `SHOW ANALYZE` 来查看统计信息收集任务的信息。

语法如下：

```SQL
SHOW ANALYZE < table_name | job_id >
    [ WHERE [ STATE = [ "PENDING" | "RUNNING" | "FINISHED" | "FAILED" ] ] ];
```

- table_name：表名，指定后可查看该表对应的统计任务信息。可以是  `db_name.table_name`  形式。不指定时返回所有统计任务信息。
- job_id：统计信息任务 ID，执行 `ANALYZE` 非同步收集统计信息时所返回的值。不指定时返回所有统计任务信息。

输出：

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

#### 查看统计信息

#### 表统计信息


通过 `SHOW TABLE STATS` 表的统计信息收集概况。

语法如下：

```SQL
SHOW TABLE STATS table_name [ PARTITION ];
```

其中：

- table_name: 导入数据的目标表。可以是  `db_name.table_name`  形式。

输出：

| 列名                | 说明                   |
| :------------------ | :--------------------- |
| `row_count`         | 行数                   |
| `update_rows`       | 更新的行数             |
| `data_size`         | 数据大小。单位 byte    |
| `healthy`           | 健康度                 |
| `update_time`       | 更新时间               |
| `last_analyze_time` | 上次收集统计信息的时间 |


#### 查看列统计信息

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


#### 修改统计信息

⽤户可以通过 `ALTER` 语句调整统计信息。

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

#### 删除统计信息

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

#### 删除Analyze Job

用于根据job id删除自动/周期Analyze作业

```sql
DROP ANALYZE JOB [JOB_ID]
```


## 配置项

|conf                                                    | comment                                                                                                                                                                                                                                                                                             | default value                  |
|---------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------|
| statistics_sql_parallel_exec_instance_num               | 控制每个统计信息收集SQL在BE侧的并发实例数/pipeline task num                                                                                                                                                                                                                                                           | 1                              |
| statistics_sql_mem_limit_in_bytes                       | 控制每个统计信息SQL可占用的BE内存                                                                                                                                                                                                                                                                                 | 2L * 1024 * 1024 * 1024 (2GiB) |
| statistics_simultaneously_running_task_num              | 通过`ANALYZE TABLE[DATABASE]`提交异步作业后，可同时analyze的列的数量，所有异步任务共同受到该参数约束                                                                                                                                                                                                                                  | 5                              |
| analyze_task_timeout_in_minutes                         | AnalyzeTask执行超时时间                                                                                                                                                                                                                                                                                   | 12 hours                       |
|stats_cache_size| 统计信息缓存的实际内存占用大小高度依赖于数据的特性，因为在不同的数据集和场景中，最大/最小值的平均大小和直方图的桶数量会有很大的差异。此外，JVM版本等因素也会对其产生影响。下面给出统计信息缓存在包含100000个项目时所占用的内存大小。每个项目的最大/最小值的平均长度为32，列名的平均长度为16，统计信息缓存总共占用了61.2777404785MiB的内存。强烈不建议分析具有非常大字符串值的列，因为这可能导致FE内存溢出。 | 100000                        |

## 常见问题

### ANALYZE WITH SYNC 执行失败：Failed to analyze following columns...

SQL执行时间受`query_timeout`会话变量控制，该变量默认值为300秒，`ANALYZE DATABASE/TABLE`等语句通常耗时较大，很容易超过该时间限制而被cancel，建议根据ANALYZE对象的数据量适当增大`query_timeout`的值。

### ANALYZE提交报错：Stats table not available...

执行ANALYZE时统计数据会被写入到内部表`__internal_schema.column_statistics`中，FE会在执行ANALYZE前检查该表tablet状态，如果存在不可用的tablet则拒绝执行任务。出现该报错请检查BE集群状态。

### 大表ANALYZE失败

由于ANALYZE能够使用的资源受到比较严格的限制，对一些大表的ANALYZE操作有可能超时或者超出BE内存限制。这些情况下，建议使用 `ANALYZE ... WITH SAMPLE...`。此外对于动态分区表的场景，强烈建议使用`ANALYZE ... WITH INCREMENTAL...`，该语句仅增量的处理数据更新的分区，能够避免大量的重复计算从而提高效率。
