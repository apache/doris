---
{
    "title": "Rollup",
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

# Rollup

用户可以通过创建上卷表（Rollup）加速查询。关于 Rollup 的概念和使用方式可以参阅 [数据模型、ROLLUP 及前缀索引](../../getting-started/data-model-rollup.md) 和 [Rollup 与查询](../../getting-started/hit-the-rollup.md) 两篇文档。

本文档主要介绍如何创建 Rollup 作业，以及创建 Rollup 的一些注意事项和常见问题。

## 名词解释

* Base Table：基表。每一个表被创建时，都对应一个基表。基表存储了这个表的完整的数据。Rollup 通常基于基表中的数据创建（也可以通过其他 Rollup 创建）。
* Index：物化索引。Rollup 或 Base Table 都被称为物化索引。
* Transaction：事务。每一个导入任务都是一个事务，每个事务有一个唯一递增的 Transaction ID。

## 原理介绍

创建 Rollup 的基本过程，是通过 Base 表的数据，生成一份新的包含指定列的 Rollup 的数据。其中主要需要进行两部分数据转换，一是已存在的历史数据的转换，二是在 Rollup 执行过程中，新到达的导入数据的转换。

```
+----------+
| Load Job |
+----+-----+
     |
     | Load job generates both base and rollup index data
     |
     |      +------------------+ +---------------+
     |      | Base Index       | | Base Index    |
     +------> New Incoming Data| | History Data  |
     |      +------------------+ +------+--------+
     |                                  |
     |                                  | Convert history data
     |                                  |
     |      +------------------+ +------v--------+
     |      | Rollup Index     | | Rollup Index  |
     +------> New Incoming Data| | History Data  |
            +------------------+ +---------------+
```

在开始转换历史数据之前，Doris 会获取一个最新的 Transaction ID。并等待这个 Transaction ID 之前的所有导入事务完成。这个 Transaction ID 成为分水岭。意思是，Doris 保证在分水岭之后的所有导入任务，都会同时为 Rollup Index 生成数据。这样当历史数据转换完成后，可以保证 Rollup 和 Base 表的数据是齐平的。

## 创建作业

创建 Rollup 的具体语法可以查看帮助 `HELP ALTER TABLE` 中 Rollup 部分的说明。

Rollup 的创建是一个异步过程，作业提交成功后，用户需要通过 `SHOW ALTER TABLE ROLLUP` 命令来查看作业进度。

## 查看作业

`SHOW ALTER TABLE ROLLUP` 可以查看当前正在执行或已经完成的 Rollup 作业。举例如下：

```
          JobId: 20037
      TableName: tbl1
     CreateTime: 2019-08-06 15:38:49
   FinishedTime: N/A
  BaseIndexName: tbl1
RollupIndexName: r1
       RollupId: 20038
  TransactionId: 10034
          State: PENDING
            Msg:
       Progress: N/A
        Timeout: 86400
```

* JobId：每个 Rollup 作业的唯一 ID。
* TableName：Rollup 对应的基表的表名。
* CreateTime：作业创建时间。
* FinishedTime：作业结束时间。如未结束，则显示 "N/A"。
* BaseIndexName：Rollup 对应的源 Index 的名称。
* RollupIndexName：Rollup 的名称。
* RollupId：Rollup 的唯一 ID。
* TransactionId：转换历史数据的分水岭 transaction ID。
* State：作业所在阶段。
    * PENDING：作业在队列中等待被调度。
    * WAITING_TXN：等待分水岭 transaction ID 之前的导入任务完成。
    * RUNNING：历史数据转换中。
    * FINISHED：作业成功。
    * CANCELLED：作业失败。
* Msg：如果作业失败，这里会显示失败信息。
* Progress：作业进度。只有在 RUNNING 状态才会显示进度。进度是以 M/N 的形式显示。其中 N 为 Rollup 的总副本数。M 为已完成历史数据转换的副本数。
* Timeout：作业超时时间。单位秒。

## 取消作业

在作业状态不为 FINISHED 或 CANCELLED 的情况下，可以通过以下命令取消 Rollup 作业：

`CANCEL ALTER TABLE ROLLUP FROM tbl_name;`

## 注意事项

* 一张表在同一时间只能有一个 Rollup 作业在运行。且一个作业中只能创建一个 Rollup。

* Rollup 操作不阻塞导入和查询操作。

* 如果 DELETE 操作，where 条件中的某个 Key 列在某个 Rollup 中不存在，则不允许该  DELETE。

    如果某个 Key 列在某一 Rollup 中不存在，则 DELETE 操作无法对该 Rollup 进行数据删除，从而无法保证 Rollup 表和 Base 表的数据一致性。

* Rollup 的列必须存在于 Base 表中。

    Rollup 的列永远是 Base 表列的子集。不能出现 Base 表中不存在的列。

* 如果 Rollup 中包含 REPLACE 聚合类型的列，则该 Rollup 必须包含所有 Key 列。

    假设 Base 表结构如下：
    
    ```(k1 INT, k2 INT, v1 INT REPLACE, v2 INT SUM)```
    
    如果需要创建的 Rollup 包含 `v1` 列，则必须包含 `k1`, `k2` 列。否则系统无法决定 `v1` 列在 Rollup 中的取值。
    
    注意，Unique 数据模型表中的所有 Value 列都是 REPLACE 聚合类型。
    
* DUPLICATE 数据模型表的 Rollup，可以指定 Rollup 的 DUPLICATE KEY。

    DUPLICATE 数据模型表中的 DUPLICATE KEY 其实就是排序列。Rollup 可以指定自己的排序列，但排序列必须是 Rollup 列顺序的前缀。如果不指定，则系统会检查 Rollup 是否包含了 Base 表的所有排序列，如果没有包含，则会报错。举例：
    
    Base 表结构：`(k1 INT, k2 INT, k3 INT) DUPLICATE KEY(k1, k2)`
    
    则 Rollup 可以为：`(k2 INT, k1 INT) DUPLICATE KEY(k2)` 

* Rollup 不需要包含 Base 表的分区列或分桶列。

## 常见问题

* 一个表可以创建多少 Rollup

    一个表能够创建的 Rollup 个数理论上没有限制，但是过多的 Rollup 会影响导入性能。因为导入时，会同时给所有 Rollup 产生数据。同时 Rollup 会占用物理存储空间。通常一个表的 Rollup 数量在 10 个以内比较合适。
    
* Rollup 创建的速度

    目前 Rollup 创建速度按照最差效率估计约为 10MB/s。保守起见，用户可以根据这个速率来设置作业的超时时间。

* 提交作业报错 `Table xxx is not stable. ...`

    Rollup 只有在表数据完整且非均衡状态下才可以开始。如果表的某些数据分片副本不完整，或者某些副本正在进行均衡操作，则提交会被拒绝。
    
    数据分片副本是否完整，可以通过以下命令查看：
    
    ```ADMIN SHOW REPLICA STATUS FROM tbl WHERE STATUS != "OK";```
    
    如果有返回结果，则说明有副本有问题。通常系统会自动修复这些问题，用户也可以通过以下命令优先修复这个表：
    
    ```ADMIN REPAIR TABLE tbl1;```
    
    用户可以通过以下命令查看是否有正在运行的均衡任务：
    
    ```SHOW PROC "/cluster_balance/pending_tablets";```
    
    可以等待均衡任务完成，或者通过以下命令临时禁止均衡操作：
    
    ```ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");```
    
## 相关配置

### FE 配置

* `alter_table_timeout_second`：作业默认超时时间，86400 秒。

### BE 配置

* `alter_tablet_worker_count`：在 BE 端用于执行历史数据转换的线程数。默认为 3。如果希望加快 Rollup 作业的速度，可以适当调大这个参数后重启 BE。但过多的转换线程可能会导致 IO 压力增加，影响其他操作。该线程和 Schema Change 作业共用。
    
    
    
    
    
     


