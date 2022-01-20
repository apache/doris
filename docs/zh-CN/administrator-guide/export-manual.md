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

数据导出（Export）是 Doris 提供的一种将数据导出的功能。该功能可以将用户指定的表或分区的数据，以文本的格式，通过 Broker 进程导出到远端存储上，如 HDFS/BOS 等。

本文档主要介绍 Export 的基本原理、使用方式、最佳实践以及注意事项。

## 名词解释

* FE：Frontend，Doris 的前端节点。负责元数据管理和请求接入。
* BE：Backend，Doris 的后端节点。负责查询执行和数据存储。
* Broker：Doris 可以通过 Broker 进程对远端存储进行文件操作。
* Tablet：数据分片。一个表会划分成多个数据分片。

## 原理

用户提交一个 Export 作业后。Doris 会统计这个作业涉及的所有 Tablet。然后对这些 Tablet 进行分组，每组生成一个特殊的查询计划。该查询计划会读取所包含的 Tablet 上的数据，然后通过 Broker 将数据写到远端存储指定的路径中，也可以通过S3协议直接导出到支持S3协议的远端存储上。

总体的调度方式如下:

```
+--------+
| Client |
+---+----+
    |  1. Submit Job
    |
+---v--------------------+
| FE                     |
|                        |
| +-------------------+  |
| | ExportPendingTask |  |
| +-------------------+  |
|                        | 2. Generate Tasks
| +--------------------+ |
| | ExportExporingTask | |
| +--------------------+ |
|                        |
| +-----------+          |     +----+   +------+   +---------+
| | QueryPlan +----------------> BE +--->Broker+--->         |
| +-----------+          |     +----+   +------+   | Remote  |
| +-----------+          |     +----+   +------+   | Storage |
| | QueryPlan +----------------> BE +--->Broker+--->         |
| +-----------+          |     +----+   +------+   +---------+
+------------------------+         3. Execute Tasks

```

1. 用户提交一个 Export 作业到 FE。
2. FE 的 Export 调度器会通过两阶段来执行一个 Export 作业：
    1. PENDING：FE 生成 ExportPendingTask，向 BE 发送 snapshot 命令，对所有涉及到的 Tablet 做一个快照。并生成多个查询计划。
    2. EXPORTING：FE 生成 ExportExportingTask，开始执行查询计划。

### 查询计划拆分

Export 作业会生成多个查询计划，每个查询计划负责扫描一部分 Tablet。每个查询计划扫描的 Tablet 个数由 FE 配置参数 `export_tablet_num_per_task` 指定，默认为 5。即假设一共 100 个 Tablet，则会生成 20 个查询计划。用户也可以在提交作业时，通过作业属性 `tablet_num_per_task` 指定这个数值。

一个作业的多个查询计划顺序执行。 

### 查询计划执行

一个查询计划扫描多个分片，将读取的数据以行的形式组织，每 1024 行为一个 batch，调用 Broker 写入到远端存储上。

查询计划遇到错误会整体自动重试 3 次。如果一个查询计划重试 3 次依然失败，则整个作业失败。

Doris 会首先在指定的远端存储的路径中，建立一个名为 `__doris_export_tmp_12345` 的临时目录（其中 `12345` 为作业 id）。导出的数据首先会写入这个临时目录。每个查询计划会生成一个文件，文件名示例：

`export-data-c69fcf2b6db5420f-a96b94c1ff8bccef-1561453713822`

其中 `c69fcf2b6db5420f-a96b94c1ff8bccef` 为查询计划的 query id。`1561453713822` 为文件生成的时间戳。

当所有数据都导出后，Doris 会将这些文件 rename 到用户指定的路径中。

### Broker 参数

Export 需要借助 Broker 进程访问远端存储，不同的 Broker 需要提供不同的参数，具体请参阅 [Broker文档](./broker.md)

## 使用示例

Export 的详细命令可以通过 `HELP EXPORT;` 。举例如下：

```
EXPORT TABLE db1.tbl1 
PARTITION (p1,p2)
[WHERE [expr]]
TO "hdfs://host/path/to/export/" 
PROPERTIES
(
	"label" = "mylabel",
    "column_separator"=",",
    "columns" = "col1,col2",
    "exec_mem_limit"="2147483648",
    "timeout" = "3600"
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
* `exec_mem_limit`： 表示 Export 作业中，一个查询计划在单个 BE 上的内存使用限制。默认 2GB。单位字节。
* `timeout`：作业超时时间。默认 2小时。单位秒。
* `tablet_num_per_task`：每个查询计划分配的最大分片数。默认为 5。

提交作业后，可以通过 `SHOW EXPORT` 命令查询导入作业状态。结果举例如下：

```
     JobId: 14008
     Label: mylabel
     State: FINISHED
  Progress: 100%
  TaskInfo: {"partitions":["*"],"exec mem limit":2147483648,"column separator":",","line delimiter":"\n","tablet num":1,"broker":"hdfs","coord num":1,"db":"default_cluster:db1","tbl":"tbl3"}
      Path: bos://bj-test-cmy/export/
CreateTime: 2019-06-25 17:08:24
 StartTime: 2019-06-25 17:08:28
FinishTime: 2019-06-25 17:08:34
   Timeout: 3600
  ErrorMsg: N/A
```

* JobId：作业的唯一 ID
* Label：自定义作业标识
* State：作业状态：
    * PENDING：作业待调度
    * EXPORTING：数据导出中
    * FINISHED：作业成功
    * CANCELLED：作业失败
* Progress：作业进度。该进度以查询计划为单位。假设一共 10 个查询计划，当前已完成 3 个，则进度为 30%。
* TaskInfo：以 Json 格式展示的作业信息：
    * db：数据库名
    * tbl：表名
    * partitions：指定导出的分区。`*` 表示所有分区。
    * exec mem limit：查询计划内存使用限制。单位字节。
    * column separator：导出文件的列分隔符。
    * line delimiter：导出文件的行分隔符。
    * tablet num：涉及的总 Tablet 数量。
    * broker：使用的 broker 的名称。
    * coord num：查询计划的个数。
* Path：远端存储上的导出路径。
* CreateTime/StartTime/FinishTime：作业的创建时间、开始调度时间和结束时间。
* Timeout：作业超时时间。单位是秒。该时间从 CreateTime 开始计算。
* ErrorMsg：如果作业出现错误，这里会显示错误原因。

## 最佳实践

### 查询计划的拆分

一个 Export 作业有多少查询计划需要执行，取决于总共有多少 Tablet，以及一个查询计划最多可以分配多少个 Tablet。因为多个查询计划是串行执行的，所以如果让一个查询计划处理更多的分片，则可以减少作业的执行时间。但如果查询计划出错（比如调用 Broker 的 RPC 失败，远端存储出现抖动等），过多的 Tablet 会导致一个查询计划的重试成本变高。所以需要合理安排查询计划的个数以及每个查询计划所需要扫描的分片数，在执行时间和执行成功率之间做出平衡。一般建议一个查询计划扫描的数据量在 3-5 GB内（一个表的 Tablet 的大小以及个数可以通过 `SHOW TABLET FROM tbl_name;` 语句查看。）。

### exec\_mem\_limit

通常一个 Export 作业的查询计划只有 `扫描`-`导出` 两部分，不涉及需要太多内存的计算逻辑。所以通常 2GB 的默认内存限制可以满足需求。但在某些场景下，比如一个查询计划，在同一个 BE 上需要扫描的 Tablet 过多，或者 Tablet 的数据版本过多时，可能会导致内存不足。此时需要通过这个参数设置更大的内存，比如 4GB、8GB 等。

## 注意事项

* 不建议一次性导出大量数据。一个 Export 作业建议的导出数据量最大在几十 GB。过大的导出会导致更多的垃圾文件和更高的重试成本。
* 如果表数据量过大，建议按照分区导出。
* 在 Export 作业运行过程中，如果 FE 发生重启或切主，则 Export 作业会失败，需要用户重新提交。
* 如果 Export 作业运行失败，在远端存储中产生的 `__doris_export_tmp_xxx` 临时目录，以及已经生成的文件不会被删除，需要用户手动删除。
* 如果 Export 作业运行成功，在远端存储中产生的 `__doris_export_tmp_xxx` 目录，根据远端存储的文件系统语义，可能会保留，也可能会被清除。比如在百度对象存储（BOS）中，通过 rename 操作将一个目录中的最后一个文件移走后，该目录也会被删除。如果该目录没有被清除，用户可以手动清除。
* 当 Export 运行完成后（成功或失败），FE 发生重启或切主，则 `SHOW EXPORT` 展示的作业的部分信息会丢失，无法查看。
* Export 作业只会导出 Base 表的数据，不会导出 Rollup Index 的数据。
* Export 作业会扫描数据，占用 IO 资源，可能会影响系统的查询延迟。

## 相关配置

### FE

* `export_checker_interval_second`：Export 作业调度器的调度间隔，默认为 5 秒。设置该参数需重启 FE。
* `export_running_job_num_limit`：正在运行的 Export 作业数量限制。如果超过，则作业将等待并处于 PENDING 状态。默认为 5，可以运行时调整。
* `export_task_default_timeout_second`：Export 作业默认超时时间。默认为 2 小时。可以运行时调整。
* `export_tablet_num_per_task`：一个查询计划负责的最大分片数。默认为 5。

