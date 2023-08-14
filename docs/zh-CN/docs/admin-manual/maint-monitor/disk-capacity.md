---
{
    "title": "磁盘空间管理",
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

# 磁盘空间管理

本文档主要介绍和磁盘存储空间有关的系统参数和处理策略。

Doris 的数据磁盘空间如果不加以控制，会因磁盘写满而导致进程挂掉。因此我们监测磁盘的使用率和剩余空间，通过设置不同的警戒水位，来控制 Doris 系统中的各项操作，尽量避免发生磁盘被写满的情况。

## 名词解释

- Data Dir：数据目录，在 BE 配置文件 `be.conf` 的 `storage_root_path` 中指定的各个数据目录。通常一个数据目录对应一个磁盘、因此下文中 **磁盘** 也指代一个数据目录。

## 基本原理

BE 定期（每隔一分钟）会向 FE 汇报一次磁盘使用情况。FE 记录这些统计值，并根据这些统计值，限制不同的操作请求。

在 FE 中分别设置了 **高水位（High Watermark）** 和 **危险水位（Flood Stage）** 两级阈值。危险水位高于高水位。当磁盘使用率高于高水位时，Doris 会限制某些操作的执行（如副本均衡等）。而如果高于危险水位，则会禁止某些操作的执行（如导入）。

同时，在 BE 上也设置了 **危险水位（Flood Stage）**。考虑到 FE 并不能完全及时的检测到 BE 上的磁盘使用情况，以及无法控制某些 BE 自身运行的操作（如 Compaction）。因此 BE 上的危险水位用于 BE 主动拒绝和停止某些操作，达到自我保护的目的。

## FE 参数

**高水位：**

```text
storage_high_watermark_usage_percent 默认 85 (85%)。
storage_min_left_capacity_bytes 默认 2GB。
```

当磁盘空间使用率**大于** `storage_high_watermark_usage_percent`，**或者** 磁盘空间剩余大小**小于** `storage_min_left_capacity_bytes` 时，该磁盘不会再被作为以下操作的目的路径：

- Tablet 均衡操作（Balance）
- Colocation 表数据分片的重分布（Relocation）
- Decommission

**危险水位：**

```text
storage_flood_stage_usage_percent 默认 95 (95%)。
storage_flood_stage_left_capacity_bytes 默认 1GB。
```

当磁盘空间使用率**大于** `storage_flood_stage_usage_percent`，**并且** 磁盘空间剩余大小**小于** `storage_flood_stage_left_capacity_bytes` 时，该磁盘不会再被作为以下操作的目的路径，并禁止某些操作：

- Tablet 均衡操作（Balance）
- Colocation 表数据分片的重分布（Relocation）
- 副本补齐
- 恢复操作（Restore）
- 数据导入（Load/Insert）

## BE 参数

**危险水位：**

```text
storage_flood_stage_usage_percent 默认 90 (90%)。
storage_flood_stage_left_capacity_bytes 默认 1GB。
```

当磁盘空间使用率**大于** `storage_flood_stage_usage_percent`，**并且** 磁盘空间剩余大小**小于** `storage_flood_stage_left_capacity_bytes` 时，该磁盘上的以下操作会被禁止：

- Base/Cumulative Compaction。
- 数据写入。包括各种导入操作。
- Clone Task。通常发生于副本修复或均衡时。
- Push Task。发生在 Hadoop 导入的 Loading 阶段，下载文件。
- Alter Task。Schema Change 或 Rollup 任务。
- Download Task。恢复操作的 Downloading 阶段。

## 磁盘空间释放

当磁盘空间高于高水位甚至危险水位后，很多操作都会被禁止。此时可以尝试通过以下方式减少磁盘使用率，恢复系统。

- 删除表或分区

  通过删除表或分区的方式，能够快速降低磁盘空间使用率，恢复集群。**注意：只有 `DROP` 操作可以达到快速降低磁盘空间使用率的目的，`DELETE` 操作不可以。**

  ```text
  DROP TABLE tbl;
  ALTER TABLE tbl DROP PARTITION p1;
  ```

- 扩容 BE

  扩容后，数据分片会自动均衡到磁盘使用率较低的 BE 节点上。扩容操作会根据数据量及节点数量不同，在数小时或数天后使集群到达均衡状态。

- 修改表或分区的副本

  可以将表或分区的副本数降低。比如默认3副本可以降低为2副本。该方法虽然降低了数据的可靠性，但是能够快速的降低磁盘使用率，使集群恢复正常。该方法通常用于紧急恢复系统。请在恢复后，通过扩容或删除数据等方式，降低磁盘使用率后，将副本数恢复为 3。

  修改副本操作为瞬间生效，后台会自动异步的删除多余的副本。

  ```text
  ALTER TABLE tbl MODIFY PARTITION p1 SET("replication_num" = "2");
  ```

- 删除多余文件

  当 BE 进程已经因为磁盘写满而挂掉并无法启动时（此现象可能因 FE 或 BE 检测不及时而发生）。需要通过删除数据目录下的一些临时文件，保证 BE 进程能够启动。以下目录中的文件可以直接删除：

  - log/：日志目录下的日志文件。
  - snapshot/: 快照目录下的快照文件。
  - trash/：回收站中的文件。

  **这种操作会对 [从 BE 回收站中恢复数据](../data-admin/delete-recover.md) 产生影响。**

  如果BE还能够启动，则可以使用`ADMIN CLEAN TRASH ON(BackendHost:BackendHeartBeatPort);`来主动清理临时文件，会清理 **所有** trash文件和过期snapshot文件，**这将影响从回收站恢复数据的操作** 。

  如果不手动执行`ADMIN CLEAN TRASH`，系统仍将会在几分钟至几十分钟内自动执行清理，这里分为两种情况：

  - 如果磁盘占用未达到 **危险水位(Flood Stage)** 的90%，则会清理过期trash文件和过期snapshot文件，此时会保留一些近期文件而不影响恢复数据。
  - 如果磁盘占用已达到 **危险水位(Flood Stage)** 的90%，则会清理 **所有** trash文件和过期snapshot文件， **此时会影响从回收站恢复数据的操作** 。 自动执行的时间间隔可以通过配置项中的`max_garbage_sweep_interval`和`min_garbage_sweep_interval`更改。

  出现由于缺少trash文件而导致恢复失败的情况时，可能返回如下结果：

  ```text
  {"status": "Fail","msg": "can find tablet path in trash"}
  ```

- 删除数据文件（危险！！！）

  当以上操作都无法释放空间时，需要通过删除数据文件来释放空间。数据文件在指定数据目录的 `data/` 目录下。删除数据分片（Tablet）必须先确保该 Tablet 至少有一个副本是正常的，否则**删除唯一副本会导致数据丢失**。假设我们要删除 id 为 12345 的 Tablet：

  - 找到 Tablet 对应的目录，通常位于 `data/shard_id/tablet_id/` 下。如：

    `data/0/12345/`

  - 记录 tablet id 和 schema hash。其中 schema hash 为上一步目录的下一级目录名。如下为 352781111：

    `data/0/12345/352781111`

  - 删除数据目录：

    `rm -rf data/0/12345/`

  - 删除 Tablet 元数据（具体参考 [Tablet 元数据管理工具](./tablet-meta-tool.md)）

    `./lib/meta_tool --operation=delete_header --root_path=/path/to/root_path --tablet_id=12345 --schema_hash= 352781111`
