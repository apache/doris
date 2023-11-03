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

当前收集列的如下信息：

| 信息            | 描述                       |
| :-------------- | :------------------------- |
| `row_count`     | 总行数                 |
| `data_size`     | 总数据量    |
| `avg_size_byte` | 值的平均⻓度 |
| `ndv`           | 不同值数量      |
| `min`           | 最小值                   |
| `max`           | 最⼤值                   |
| `null_count`    | 空值数量               |

## 收集统计信息

### 使用ANALYZE语句

Doris支持用户通过提交ANALYZE语句来触发统计信息的收集和更新。

语法：

```SQL
ANALYZE < TABLE | DATABASE table_name | db_name > 
    [ (column_name [, ...]) ]
    [ [ WITH SYNC ] [ WITH SAMPLE PERCENT | ROWS ] [ WITH SQL ] ]
    [ PROPERTIES ("key" = "value", ...) ];
```

其中：

- table_name: 指定的的目标表。可以是  `db_name.table_name`  形式。
- column_name: 指定的目标列。必须是  `table_name`  中存在的列，多个列名称用逗号分隔。
- sync：同步收集统计信息。收集完后返回。若不指定则异步执行并返回任务 ID。
- sample percent | rows：抽样收集统计信息。可以指定抽样比例或者抽样行数。
- sql：执行sql来收集外表分区列统计信息。默认从元数据收集分区列信息，这样效率比较高但是行数和数据量大小可能不准。用户可以指定使用sql来收集，这样可以收集到准确的分区列信息。


### 自动收集

在指定的时间段内自动收集满足条件的库表上的统计信息。用户可以通过设置参数`full_auto_analyze_start_time`（默认为00:00:00）和参数`full_auto_analyze_end_time`（默认为23:59:59）来指定自动收集的时间段。

此功能仅对没有统计信息或者统计信息过时的库表进行收集。当一个表的数据更新了20%（该值可通过参数`table_stats_health_threshold`（默认为80）配置）以上时，Doris会认为该表的统计信息已经过时。

对于数据量较大（默认为5GiB）的表，Doris会自动采取采样的方式去收集，以尽可能降低对系统造成的负担并尽快完成收集作业，用户可通过设置FE参数`huge_table_lower_bound_size_in_bytes`来调节此行为。同时对于数据量大于`huge_table_lower_bound_size_in_bytes` * 5 的表，Doris保证其收集时间间隔不小于12小时（该时间可通过FE参数`huge_table_auto_analyze_interval_in_millis`控制）。

自动采样默认采样4194304(2^22)行，但由于实现方式的原因实际采样数可能大于该值。如果希望采样更多的行以获得更准确的数据分布信息，可通过FE参数`huge_table_default_sample_rows`配置。

用户可以通过设置FE全局会话变量`SET GLOBAL enable_full_auto_analyze = false`来关闭本功能。

### 任务管理

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
SHOW TABLE STATS table_name;
```

其中：

- table_name: 导入数据的目标表。可以是  `db_name.table_name`  形式。

输出：

| 列名                | 说明                   |
| :------------------ | :--------------------- |
|`updated_rows`|自上次ANALYZE以来该表的更新行数|
|`query_times`|保留列，后续版本用以记录该表查询次数|
|`row_count`| 行数（不反映命令执行时的准确行数）|
|`updated_time`| 上次更新时间|
|`columns`| 收集过统计信息的列|



#### 查看列统计信息

通过 `SHOW COLUMN STATS` 来查看列的不同值数以及 `NULL` 数量等信息。

语法如下：

```SQL
SHOW COLUMN [cached] STATS table_name [ (column_name [, ...]) ];
```

其中：

- cached: 展示当前FE内存缓存中的统计信息。
- table_name: 收集统计信息的目标表。可以是  `db_name.table_name`  形式。
- column_name: 指定的目标列，必须是  `table_name`  中存在的列，多个列名称用逗号分隔。

#### 修改统计信息

⽤户可以通过 `ALTER` 语句调整统计信息。

```SQL
ALTER TABLE table_name MODIFY COLUMN column_name SET STATS ('stat_name' = 'stat_value', ...) [ PARTITION (partition_name) ];
```

其中：

- table_name: 删除统计信息的目标表。可以是 `db_name.table_name` 形式。
- column_name: 指定的目标列，必须是 `table_name` 中存在的列，每次只能修改一列的统计信息。
- stat_name 和 stat_value: 相应的统计信息名称和统计信息信息的值，多个统计信息逗号分隔。可以修改的统计信息包括 `row_count`, `ndv`, `num_nulls`, `min_value`, `max_value`, `data_size`。

#### 删除统计信息

⽤户通过 `DROP` 语句删除统计信息，根据提供的参数，删除指定的表、分区或列的统计信息。删除时会同时删除列统计信息和列直方图信息。

语法：

```SQL
DROP [ EXPIRED ] STATS [ table_name [ (column_name [, ...]) ] ];
```


#### 删除Analyze Job

用于根据job id删除自动/周期Analyze作业

```sql
DROP ANALYZE JOB [JOB_ID]
```

### 查看自动收集任务执行情况

此命令用于打开自动收集功能后，查看自动收集任务的完成状态。

```sql
SHOW AUTO ANALYZE [table_name]
    [ WHERE [ STATE = [ "PENDING" | "RUNNING" | "FINISHED" | "FAILED" ] ] ];
```

自动收集任务不支持查看每个列的具完成情况及失败原因。默认只保存过去20000个执行完毕的自动收集任务的状态。

## 配置项

|fe conf option                                                    | comment                                                                                                                                                                                                                                                                                             | default value                  |
|---------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------|
| statistics_sql_parallel_exec_instance_num               | 控制每个统计信息收集SQL在BE侧的并发实例数/pipeline task num                                                                                                                                                                                                                                                           | 1                              |
| statistics_sql_mem_limit_in_bytes                       | 控制每个统计信息SQL可占用的BE内存                                                                                                                                                                                                                                                        | 2L * 1024 * 1024 * 1024 (2GiB) |
| statistics_simultaneously_running_task_num              | 通过`ANALYZE TABLE[DATABASE]`提交异步作业后，可同时analyze的列的数量，所有异步任务共同受到该参数约束|5|
| analyze_task_timeout_in_minutes                         | AnalyzeTask执行超时时间                                                                                                                                                                                                                                                                                   | 1hours                       |
|stats_cache_size| 统计信息缓存的实际内存占用大小高度依赖于数据的特性，因为在不同的数据集和场景中，最大/最小值的平均大小和直方图的桶数量会有很大的差异。此外，JVM版本等因素也会对其产生影响。下面给出统计信息缓存在包含100000个项目时所占用的内存大小。每个项目的最大/最小值的平均长度为32，列名的平均长度为16，统计信息缓存总共占用了61.2777404785MiB的内存。强烈不建议分析具有非常大字符串值的列，因为这可能导致FE内存溢出。 | 100000                        |
|analyze_record_limit|控制统计信息作业执行记录的持久化行数|20000|
|huge_table_default_sample_rows|定义开启开启大表自动sample后，对大表的采样行数|4194304|
|huge_table_lower_bound_size_in_bytes|大小超过该值的的表，在自动收集时将会自动通过采样收集统计信息|5368709120|
|huge_table_auto_analyze_interval_in_millis|控制对大表的自动ANALYZE的最小时间间隔，在该时间间隔内大小超过huge_table_lower_bound_size_in_bytes * 5的表仅ANALYZE一次|43200000|
|table_stats_health_threshold|取值在0-100之间，当自上次统计信息收集操作之后，数据更新量达到 (100 - table_stats_health_threshold)% ，认为该表的统计信息已过时|80|

|会话变量|说明|默认值|
|---|---|---|
|full_auto_analyze_start_time|自动统计信息收集开始时间|00:00:00|
|full_auto_analyze_end_time|自动统计信息收集结束时间|23:59:59|
|enable_full_auto_analyze|开启自动收集功能|true|
|insert_merge_item_count|控制INSERT攒批数量|

注意：上面列出的会话变量必须通过`SET GLOBAL`全局设置。


|会话变量|说明|默认值|
|---|---|---|
|analyze_timeout|控制同步ANALYZE超时时间，单位为秒|43200|

## 使用建议

根据我们的测试，在数据量（这里指实际存储占用的空间）为128GiB以下的表上，除自动收集功能执行时间段之外无须改动默认配置。

依据集群配置情况，自动收集任务通常会占用20%左右的CPU资源，因此用户需要根据自己的业务情况，适当调整自动收集功能执行时间段以避开业务高峰期资源抢占。

由于ANALYZE是资源密集型操作，因此最好尽可能不要在业务高峰期执行此类操作，从而避免对业务造成干扰，集群负载较高的情况下，ANALYZE操作也更容易失败。此外，基于相同的原因，我们建议用户避免全量的ANALYZE整库整表。通常来讲，只需要对经常作为谓词条件，JOIN条件，聚合字段以及ID字段的列进行ANALYZE就足够了。如果用户提交的SQL涉及到大量此类操作，并且表上也没有统计信息或者统计信息非常陈旧，那么我们建议：

* 在提交复杂查询之前先对涉及到的表列进行ANALYZE，因为规划不当的复杂查询将占用非常多的系统资源，非荣容易资源耗尽或超时而失败
* 如果用户为Doris配置了周期性数据导入例程，那么建议在导入完毕后，执行ANALYZE从而保证后续查询规划能够利用到最新的统计数据。可以利用Doris已有的作业调度框架自动化完成此类设置
* 当表的数据发生显著变化后，比如新建表并完成数据导入后，ANALYZE对应的表。

## 常见问题

### ANALYZE WITH SYNC 执行失败：Failed to analyze following columns...

SQL执行时间受`query_timeout`会话变量控制，该变量默认值为300秒，`ANALYZE DATABASE/TABLE`等语句通常耗时较大，很容易超过该时间限制而被cancel，建议根据ANALYZE对象的数据量适当增大`query_timeout`的值。

### ANALYZE提交报错：Stats table not available...

执行ANALYZE时统计数据会被写入到内部表`__internal_schema.column_statistics`中，FE会在执行ANALYZE前检查该表tablet状态，如果存在不可用的tablet则拒绝执行任务。出现该报错请检查BE集群状态。

用户可通过`SHOW BACKENDS\G`，确定BE状态是否正常。如果BE状态正常，可使用命令`ADMIN SHOW REPLICA STATUS FROM __internal_schema.[tbl_in_this_db]`，检查该库下tablet状态，确保tablet状态正常。

### 大表ANALYZE失败

由于ANALYZE能够使用的资源受到比较严格的限制，对一些大表的ANALYZE操作有可能超时或者超出BE内存限制。这些情况下，建议使用 `ANALYZE ... WITH SAMPLE...`。
