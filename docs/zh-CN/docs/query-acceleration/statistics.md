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

<br/>


## 1. 收集统计信息

---

### 1.1 使用ANALYZE语句手动收集

Doris支持用户通过提交ANALYZE语句来手动触发统计信息的收集和更新。

语法：

```SQL
ANALYZE < TABLE | DATABASE table_name | db_name > 
    [ (column_name [, ...]) ]
    [ [ WITH SYNC ] [ WITH SAMPLE PERCENT | ROWS ] [ WITH SCAN ] ]
    [ PROPERTIES ("key" = "value", ...) ];
```

其中：

- table_name: 指定的目标表。可以是  `db_name.table_name`  形式。
- column_name: 指定的目标列。必须是  `table_name`  中存在的列，多个列名称用逗号分隔。
- sync：同步收集统计信息。收集完后返回。若不指定则异步执行并返回JOB ID。
- sample percent | rows：抽样收集统计信息。可以指定抽样比例或者抽样行数。
- scan：该配置只针对hive外表的分区列，指定通过扫描外表数据来收集分区列统计信息，这样收集的信息比较准确但执行时间较长。如果不指定，默认会从元数据收集分区列信息，这样效率比较高但是行数和数据量大小可能不准。


下面是一些例子

对一张表按照10%的比例采样收集统计数据：

```sql
ANALYZE TABLE lineitem WITH SAMPLE PERCENT 10;
```

对一张表按采样10万行收集统计数据

```sql
ANALYZE TABLE lineitem WITH SAMPLE ROWS 100000;
```

<br />

### 1.2 自动收集

此功能从2.0.3开始正式支持，默认为全天开启状态。下面对其基本运行逻辑进行阐述，在每次导入事务提交后，Doris将记录本次导入事务更新的表行数用以估算当前已有表的统计数据的健康度（对于没有收集过统计数据的表，其健康度为0）。当表的健康度低于60（可通过参数`table_stats_health_threshold`调节）时，Doris会认为该表的统计信息已经过时，并在之后触发对该表的统计信息收集作业。而对于统计信息健康度高于60的表，则不会重复进行收集。External Catalog中的外表目前还未支持健康度，可以认为外表的健康度一直为0。

统计信息的收集作业本身需要占用一定的系统资源，为了尽可能降低开销，对于数据量较大（默认为5GiB，可通过设置FE参数`huge_table_lower_bound_size_in_bytes`来调节此行为）的表，Doris会自动采取采样的方式去收集，自动采样默认采样4194304(2^22)行，以尽可能降低对系统造成的负担并尽快完成收集作业。如果希望采样更多的行以获得更准确的数据分布信息，可通过调整参数`huge_table_default_sample_rows`增大采样行数。另外对于数据量大于`huge_table_lower_bound_size_in_bytes` * 5 的表，Doris保证其收集时间间隔不小于12小时（该时间可通过调整参数`huge_table_auto_analyze_interval_in_millis`控制）。

如果担心自动收集作业对业务造成干扰，可结合自身需求通过设置参数`full_auto_analyze_start_time`和参数`full_auto_analyze_end_time`指定自动收集作业在业务负载较低的时间段执行。也可以通过设置参数`enable_full_auto_analyze` 为`false`来彻底关闭本功能。

External catalog默认不参与自动收集。因为external catalog往往包含海量历史数据，如果参与自动收集，可能占用过多资源。可以通过设置catalog的property来打开和关闭external catalog的自动收集。

```sql
ALTER CATALOG external_catalog SET PROPERTIES ('enable.auto.analyze'='true'); // 打开自动收集
ALTER CATALOG external_catalog SET PROPERTIES ('enable.auto.analyze'='false'); // 关闭自动收集
```

<br />

## 2. 作业管理

---

### 2.1 查看统计作业

通过 `SHOW ANALYZE` 来查看统计信息收集作业的信息。

语法如下：

```SQL
SHOW [AUTO] ANALYZE < table_name | job_id >
    [ WHERE [ STATE = [ "PENDING" | "RUNNING" | "FINISHED" | "FAILED" ] ] ];
```

- AUTO：仅仅展示自动收集历史作业信息。需要注意的是默认只保存过去20000个执行完毕的自动收集作业的状态。
- table_name：表名，指定后可查看该表对应的统计作业信息。可以是  `db_name.table_name`  形式。不指定时返回所有统计作业信息。
- job_id：统计信息作业 ID，执行 `ANALYZE` 异步收集时得到。不指定id时此命令返回所有统计作业信息。

输出：

| 列名                   | 说明         |
| :--------------------- | :----------- |
| `job_id`               | 统计作业 ID  |
| `catalog_name`         | catalog 名称 |
| `db_name`              | 数据库名称   |
| `tbl_name`             | 表名称       |
| `col_name`             | 列名称列表       |
| `job_type`             | 作业类型     |
| `analysis_type`        | 统计类型     |
| `message`              | 作业信息     |
| `last_exec_time_in_ms` | 上次执行时间 |
| `state`                | 作业状态     |
| `schedule_type`        | 调度方式     |

下面是一个例子：

```sql
mysql> show analyze 245073\G;
*************************** 1. row ***************************
              job_id: 245073
        catalog_name: internal
             db_name: default_cluster:tpch
            tbl_name: lineitem
            col_name: [l_returnflag,l_receiptdate,l_tax,l_shipmode,l_suppkey,l_shipdate,l_commitdate,l_partkey,l_orderkey,l_quantity,l_linestatus,l_comment,l_extendedprice,l_linenumber,l_discount,l_shipinstruct]
            job_type: MANUAL
       analysis_type: FUNDAMENTALS
             message: 
last_exec_time_in_ms: 2023-11-07 11:00:52
               state: FINISHED
            progress: 16 Finished  |  0 Failed  |  0 In Progress  |  16 Total
       schedule_type: ONCE
```

<br/>

### 2.2 查看每列统计信息收集情况

每个收集作业中可以包含一到多个任务，每个任务对应一列的收集。用户可通过如下命令查看具体每列的统计信息收集完成情况。

语法：

```sql
SHOW ANALYZE TASK STATUS [job_id]
```

下面是一个例子：

```
mysql> show analyze task status 20038 ;
+---------+----------+---------+----------------------+----------+
| task_id | col_name | message | last_exec_time_in_ms | state    |
+---------+----------+---------+----------------------+----------+
| 20039   | col4     |         | 2023-06-01 17:22:15  | FINISHED |
| 20040   | col2     |         | 2023-06-01 17:22:15  | FINISHED |
| 20041   | col3     |         | 2023-06-01 17:22:15  | FINISHED |
| 20042   | col1     |         | 2023-06-01 17:22:15  | FINISHED |
+---------+----------+---------+----------------------+----------+


```

<br/>

### 2.3 查看列统计信息

通过 `SHOW COLUMN STATS` 来查看列的各项统计数据。

语法如下：

```SQL
SHOW COLUMN [cached] STATS table_name [ (column_name [, ...]) ];
```

其中：

- cached: 展示当前FE内存缓存中的统计信息。
- table_name: 收集统计信息的目标表。可以是  `db_name.table_name`  形式。
- column_name: 指定的目标列，必须是  `table_name`  中存在的列，多个列名称用逗号分隔。

下面是一个例子：

```sql
mysql> show column stats lineitem(l_tax)\G;
*************************** 1. row ***************************
  column_name: l_tax
        count: 6001215.0
          ndv: 9.0
     num_null: 0.0
    data_size: 4.800972E7
avg_size_byte: 8.0
          min: 0.00
          max: 0.08
       method: FULL
         type: FUNDAMENTALS
      trigger: MANUAL
  query_times: 0
 updated_time: 2023-11-07 11:00:46

```

<br/>

### 2.4 表收集概况

通过 `SHOW TABLE STATS` 查看表的统计信息收集概况。

语法如下：

```SQL
SHOW TABLE STATS table_name;
```

其中：

- table_name: 目标表表名。可以是  `db_name.table_name`  形式。

输出：

| 列名                | 说明                   |
| :------------------ | :--------------------- |
|`updated_rows`|自上次ANALYZE以来该表的更新行数|
|`query_times`|保留列，后续版本用以记录该表查询次数|
|`row_count`| 行数（不反映命令执行时的准确行数）|
|`updated_time`| 上次更新时间|
|`columns`| 收集过统计信息的列|
|`trigger`|触发方式|

下面是一个例子：

```sql
mysql> show table stats lineitem \G;
*************************** 1. row ***************************
updated_rows: 0
 query_times: 0
   row_count: 6001215
updated_time: 2023-11-07
     columns: [l_returnflag, l_receiptdate, l_tax, l_shipmode, l_suppkey, l_shipdate, l_commitdate, l_partkey, l_orderkey, l_quantity, l_linestatus, l_comment, l_extendedprice, l_linenumber, l_discount, l_shipinstruct]
     trigger: MANUAL
```

<br/>

### 2.5 终止统计作业

通过 `KILL ANALYZE` 来终止正在运行的统计作业。

语法如下：

```SQL
KILL ANALYZE job_id;
```

其中：

- job_id：统计信息作业 ID。执行 `ANALYZE` 异步收集统计信息时所返回的值，也可以通过 `SHOW ANALYZE` 语句获取。

示例：

- 终止 ID 为 52357 的统计作业。

```SQL
mysql> KILL ANALYZE 52357;
```

<br/>

## 3. 会话变量及配置项

---

### 3.1 会话变量

|会话变量|说明|默认值|
|---|---|---|
|full_auto_analyze_start_time|自动统计信息收集开始时间|00:00:00|
|full_auto_analyze_end_time|自动统计信息收集结束时间|23:59:59|
|enable_full_auto_analyze|开启自动收集功能|true|
|huge_table_default_sample_rows|对大表的采样行数|4194304|
|huge_table_lower_bound_size_in_bytes|大小超过该值的的表，在自动收集时将会自动通过采样收集统计信息|5368709120|
|huge_table_auto_analyze_interval_in_millis|控制对大表的自动ANALYZE的最小时间间隔，在该时间间隔内大小超过huge_table_lower_bound_size_in_bytes * 5的表仅ANALYZE一次|43200000|
|table_stats_health_threshold|取值在0-100之间，当自上次统计信息收集操作之后，数据更新量达到 (100 - table_stats_health_threshold)% ，认为该表的统计信息已过时|60|
|analyze_timeout|控制ANALYZE超时时间，单位为秒|43200|

<br/>

### 3.2 FE配置项

下面的FE配置项通常情况下，无需关注

|FE配置项|说明|默认值|
|---|---|---|
|analyze_record_limit|控制统计信息作业执行记录的持久化行数|20000|
|stats_cache_size| FE侧统计信息缓存条数 | 500000                        |
| statistics_simultaneously_running_task_num |可同时执行的异步作业数量|3|
| statistics_sql_mem_limit_in_bytes| 控制每个统计信息SQL可占用的BE内存| 2L * 1024 * 1024 * 1024 (2GiB) |

<br/>

## 4. 常见问题

---

### 4.1 ANALYZE提交报错：Stats table not available...

执行ANALYZE时统计数据会被写入到内部表`__internal_schema.column_statistics`中，FE会在执行ANALYZE前检查该表tablet状态，如果存在不可用的tablet则拒绝执行作业。出现该报错请检查BE集群状态。

用户可通过`SHOW BACKENDS\G`，确定BE状态是否正常。如果BE状态正常，可使用命令`ADMIN SHOW REPLICA STATUS FROM __internal_schema.[tbl_in_this_db]`，检查该库下tablet状态，确保tablet状态正常。

<br/>

### 4.2 大表ANALYZE失败

由于ANALYZE能够使用的资源受到比较严格的限制，对一些大表的ANALYZE操作有可能超时或者超出BE内存限制。这些情况下，建议使用 `ANALYZE ... WITH SAMPLE...`。
