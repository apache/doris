---
{
    "title": "数据导出",
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

# 数据导出

异步导出（Export）是 Doris 提供的一种将数据异步导出的功能。该功能可以将用户指定的表或分区的数据，以指定的文件格式，通过 Broker 进程或 S3协议/HDFS协议 导出到远端存储上，如 对象存储 / HDFS 等。

当前，EXPORT支持导出 Doris本地表 / View视图 / 外表，支持导出到 parquet / orc / csv / csv_with_names / csv_with_names_and_types 文件格式。

本文档主要介绍 Export 的基本原理、使用方式、最佳实践以及注意事项。

## 原理

用户提交一个 Export 作业后。Doris 会统计这个作业涉及的所有 Tablet。然后根据`parallelism`参数（由用户指定）对这些 Tablet 进行分组。每个线程负责一组tablets，生成若干个`SELECT INTO OUTFILE`查询计划。该查询计划会读取所包含的 Tablet 上的数据，然后通过 S3协议 / HDFS协议 / Broker 将数据写到远端存储指定的路径中。

总体的执行流程如下:

1. 用户提交一个 Export 作业到 FE。
2. FE会统计要导出的所有Tablets，然后根据`parallelism`参数将所有Tablets分组，每一组再根据`maximum_number_of_export_partitions`参数生成若干个`SELECT INTO OUTFILE`查询计划
3. 根据`parallelism`参数，生成相同个数的`ExportTaskExecutor`，每一个`ExportTaskExecutor`由一个线程负责，线程由FE的Job 调度框架去调度执行。
4. FE的Job调度器会去调度`ExportTaskExecutor`并执行，每一个`ExportTaskExecutor`会串行地去执行由它负责的若干个`SELECT INTO OUTFILE`查询计划。

## 开始导出

Export 的详细用法可参考 [EXPORT](../../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/EXPORT.md) 。


### 导出到HDFS

```sql
EXPORT TABLE db1.tbl1 
PARTITION (p1,p2)
[WHERE [expr]]
TO "hdfs://host/path/to/export/" 
PROPERTIES
(
    "label" = "mylabel",
    "column_separator"=",",
    "columns" = "col1,col2",
    "parallelusm" = "3"
)
WITH BROKER "hdfs"
(
    "username" = "user",
    "password" = "passwd"
);
```

* `label`：本次导出作业的标识。后续可以使用这个标识查看作业状态。
* `column_separator`：列分隔符。默认为 `\t`。支持不可见字符，比如 '\x07'。
* `columns`：要导出的列，使用英文状态逗号隔开，如果不填这个参数默认是导出表的所有列。
* `line_delimiter`：行分隔符。默认为 `\n`。支持不可见字符，比如 '\x07'。
* `parallelusm`：并发3个线程去导出。

### 导出到对象存储

通过s3 协议直接将数据导出到指定的存储.

```sql
EXPORT TABLE test TO "s3://bucket/path/to/export/dir/"
WITH S3 (
    "s3.endpoint" = "http://host",
    "s3.access_key" = "AK",
    "s3.secret_key"="SK",
    "s3.region" = "region"
);
```

- `s3.access_key`/`s3.secret_key`：是您访问对象存储的ACCESS_KEY/SECRET_KEY
- `s3.endpoint`：Endpoint表示对象存储对外服务的访问域名.
- `s3.region`：表示对象存储数据中心所在的地域.


### 查看导出状态

提交作业后，可以通过  [SHOW EXPORT](../../sql-manual/sql-reference/Show-Statements/SHOW-EXPORT.md) 命令查询导出作业状态。结果举例如下：

```sql
mysql> show EXPORT\G;
*************************** 1. row ***************************
     JobId: 14008
     State: FINISHED
  Progress: 100%
  TaskInfo: {"partitions":[],"max_file_size":"","delete_existing_files":"","columns":"","format":"csv","column_separator":"\t","line_delimiter":"\n","db":"default_cluster:demo","tbl":"student4","tablet_num":30}
      Path: hdfs://host/path/to/export/
CreateTime: 2019-06-25 17:08:24
 StartTime: 2019-06-25 17:08:28
FinishTime: 2019-06-25 17:08:34
   Timeout: 3600
  ErrorMsg: NULL
  OutfileInfo: [
  [
    {
      "fileNumber": "1",
      "totalRows": "4",
      "fileSize": "34bytes",
      "url": "file:///127.0.0.1/Users/fangtiewei/tmp_data/export/f1ab7dcc31744152-bbb4cda2f5c88eac_"
    }
  ]
]
1 row in set (0.01 sec)
```

* JobId：作业的唯一 ID
* State：作业状态：
  * PENDING：作业待调度
  * EXPORTING：数据导出中
  * FINISHED：作业成功
  * CANCELLED：作业失败
* Progress：作业进度。该进度以查询计划为单位。假设一共 10 个线程，当前已完成 3 个，则进度为 30%。
* TaskInfo：以 Json 格式展示的作业信息：
  * db：数据库名
  * tbl：表名
  * partitions：指定导出的分区。`空`列表 表示所有分区。
  * column_separator：导出文件的列分隔符。
  * line_delimiter：导出文件的行分隔符。
  * tablet num：涉及的总 Tablet 数量。
  * broker：使用的 broker 的名称。
  * coord num：查询计划的个数。
  * max_file_size：一个导出文件的最大大小。
  * delete_existing_files：是否删除导出目录下已存在的文件及目录。
  * columns：指定需要导出的列名，空值代表导出所有列。
  * format：导出的文件格式
* Path：远端存储上的导出路径。
* CreateTime/StartTime/FinishTime：作业的创建时间、开始调度时间和结束时间。
* Timeout：作业超时时间。单位是秒。该时间从 CreateTime 开始计算。
* ErrorMsg：如果作业出现错误，这里会显示错误原因。
* OutfileInfo：如果作业导出成功，这里会显示具体的`SELECT INTO OUTFILE`结果信息。

### 取消导出任务

<version since="1.2.2"></version>

提交作业后，可以通过  [CANCEL EXPORT](../../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/CANCEL-EXPORT.md) 命令取消导出作业。取消命令举例如下：

```sql
CANCEL EXPORT
FROM example_db
WHERE LABEL like "%example%";
````

## 最佳实践

### 并发导出

一个 Export 作业可以设置`parallelism`参数来并发导出数据。`parallelism`参数实际就是指定执行 EXPORT 作业的线程数量。每一个线程会负责导出表的部分Tablets。

一个 Export 作业的底层执行逻辑实际上是`SELECT INTO OUTFILE`语句，`parallelism`参数设置的每一个线程都会去执行独立的`SELECT INTO OUTFILE`语句。

Export 作业拆分成多个`SELECT INTO OUTFILE`的具体逻辑是：将该表的所有tablets平均的分给所有parallel线程，如：
- num(tablets) = 40, parallelism = 3，则这3个线程各自负责的tablets数量分别为 14，13，13个。
- num(tablets) = 2, parallelism = 3，则Doris会自动将parallelism设置为2，每一个线程负责一个tablets。

当一个线程负责的tablest超过 `maximum_tablets_of_outfile_in_export` 数值（默认为10，可在fe.conf中添加`maximum_tablets_of_outfile_in_export`参数来修改该值）时，该线程就会拆分为多个`SELECT INTO OUTFILE`语句，如：
- 一个线程负责的tablets数量分别为 14，`maximum_tablets_of_outfile_in_export = 10`，则该线程负责两个`SELECT INTO OUTFILE`语句，第一个`SELECT INTO OUTFILE`语句导出10个tablets，第二个`SELECT INTO OUTFILE`语句导出4个tablets，两个`SELECT INTO OUTFILE`语句由该线程串行执行。


当所要导出的数据量很大时，可以考虑适当调大`parallelism`参数来增加并发导出。若机器核数紧张，无法再增加`parallelism` 而导出表的Tablets又较多 时，可以考虑调大`maximum_tablets_of_outfile_in_export`来增加一个`SELECT INTO OUTFILE`语句负责的tablets数量，也可以加快导出速度。

### exec\_mem\_limit

通常一个 Export 作业的查询计划只有 `扫描-导出` 两部分，不涉及需要太多内存的计算逻辑。所以通常 2GB 的默认内存限制可以满足需求。

但在某些场景下，比如一个查询计划，在同一个 BE 上需要扫描的 Tablet 过多，或者 Tablet 的数据版本过多时，可能会导致内存不足。可以调整session变量`exec_mem_limit`来调大内存使用限制。

## 注意事项

* 不建议一次性导出大量数据。一个 Export 作业建议的导出数据量最大在几十 GB。过大的导出会导致更多的垃圾文件和更高的重试成本。
* 如果表数据量过大，建议按照分区导出。
* 在 Export 作业运行过程中，如果 FE 发生重启或切主，则 Export 作业会失败，需要用户重新提交。
* 如果 Export 作业运行失败，已经生成的文件不会被删除，需要用户手动删除。
* Export 作业可以导出 Base 表 / View视图表 / 外表 的数据，不会导出 Rollup Index 的数据。
* Export 作业会扫描数据，占用 IO 资源，可能会影响系统的查询延迟。
* 在使用EXPORT命令时，请确保目标路径是已存在的目录，否则导出可能会失败。
* 在并发导出时，请注意合理地配置线程数量和并行度，以充分利用系统资源并避免性能瓶颈。
* 导出到本地文件时，要注意文件权限和路径，确保有足够的权限进行写操作，并遵循适当的文件系统路径。
* 在导出过程中，可以实时监控进度和性能指标，以便及时发现问题并进行优化调整。
* 导出操作完成后，建议验证导出的数据是否完整和正确，以确保数据的质量和完整性。

## 相关配置

### FE

* `maximum_tablets_of_outfile_in_export`：ExportExecutorTask任务中一个OutFile语句允许的最大tablets数量。

## 更多帮助

关于 EXPORT 使用的更多详细语法及最佳实践，请参阅 [Export](../../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/EXPORT.md) 命令手册，你也可以在 MySql 客户端命令行下输入 `HELP EXPORT` 获取更多帮助信息。

EXPORT 命令底层实现是`SELECT INTO OUTFILE`语句，有关`SELECT INTO OUTFILE`可以参阅[同步导出](./outfile.md) 和 [SELECT INTO OUTFILE](../..//sql-manual/sql-reference/Data-Manipulation-Statements/OUTFILE.md)命令手册。
