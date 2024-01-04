---
{
    "title": "数据副本管理",
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

# 数据副本管理

从 0.9.0 版本开始，Doris 引入了优化后的副本管理策略，同时支持了更为丰富的副本状态查看工具。本文档主要介绍 Doris 数据副本均衡、修复方面的调度策略，以及副本管理的运维方法。帮助用户更方便的掌握和管理集群中的副本状态。

> Colocation 属性的表的副本修复和均衡可以参阅[这里](../../query-acceleration/join-optimization/colocation-join.md)

## 名词解释

1. Tablet：Doris 表的逻辑分片，一个表有多个分片。
2. Replica：分片的副本，默认一个分片有3个副本。
3. Healthy Replica：健康副本，副本所在 Backend 存活，且副本的版本完整。
4. TabletChecker（TC）：是一个常驻的后台线程，用于定期扫描所有的 Tablet，检查这些 Tablet 的状态，并根据检查结果，决定是否将 tablet 发送给 TabletScheduler。
5. TabletScheduler（TS）：是一个常驻的后台线程，用于处理由 TabletChecker 发来的需要修复的 Tablet。同时也会进行集群副本均衡的工作。
6. TabletSchedCtx（TSC）：是一个 tablet 的封装。当 TC 选择一个 tablet 后，会将其封装为一个 TSC，发送给 TS。
7. Storage Medium：存储介质。Doris 支持对分区粒度指定不同的存储介质，包括 SSD 和 HDD。副本调度策略也是针对不同的存储介质分别调度的。

```

              +--------+              +-----------+
              |  Meta  |              |  Backends |
              +---^----+              +------^----+
                  | |                        | 3. Send clone tasks
 1. Check tablets | |                        |
           +--------v------+        +-----------------+
           | TabletChecker +--------> TabletScheduler |
           +---------------+        +-----------------+
                   2. Waiting to be scheduled


```

上图是一个简化的工作流程。


## 副本状态

一个 Tablet 的多个副本，可能因为某些情况导致状态不一致。Doris 会尝试自动修复这些状态不一致的副本，让集群尽快从错误状态中恢复。

**一个 Replica 的健康状态有以下几种：**

1. BAD

    即副本损坏。包括但不限于磁盘故障、BUG等引起的副本不可恢复的损毁状态。
    
2. VERSION\_MISSING

    版本缺失。Doris 中每一批次导入都对应一个数据版本。而一个副本的数据由多个连续的版本组成。而由于导入错误、延迟等原因，可能导致某些副本的数据版本不完整。
    
3. HEALTHY

    健康副本。即数据正常的副本，并且副本所在的 BE 节点状态正常（心跳正常且不处于下线过程中）
    

**一个 Tablet 的健康状态由其所有副本的状态决定，有以下几种：**

1. REPLICA\_MISSING
   
    副本缺失。即存活副本数小于期望副本数。
    
2. VERSION\_INCOMPLETE

    存活副本数大于等于期望副本数，但其中健康副本数小于期望副本数。

3. REPLICA\_RELOCATING

    拥有等于 replication num 的版本完整的存活副本数，但是部分副本所在的 BE 节点处于 unavailable 状态（比如 decommission 中）
    
4. REPLICA\_MISSING\_IN\_CLUSTER

    当使用多 cluster 方式时，健康副本数大于等于期望副本数，但在对应 cluster 内的副本数小于期望副本数。
    
5. REDUNDANT

    副本冗余。健康副本都在对应 cluster 内，但数量大于期望副本数。或者有多余的 unavailable 副本。

6. FORCE\_REDUNDANT

    这是一个特殊状态。只会出现在当已存在副本数大于等于可用节点数，可用节点数大于等于期望副本数，并且存活的副本数小于期望副本数。这种情况下，需要先删除一个副本，以保证有可用节点用于创建新副本。
    
7. COLOCATE\_MISMATCH

    针对 Colocation 属性的表的分片状态。表示分片副本与 Colocation Group 的指定的分布不一致。

8. COLOCATE\_REDUNDANT

    针对 Colocation 属性的表的分片状态。表示 Colocation 表的分片副本冗余。
    
9. HEALTHY

    健康分片，即条件[1-8]都不满足。
    
## 副本修复

TabletChecker 作为常驻的后台进程，会定期检查所有分片的状态。对于非健康状态的分片，将会交给 TabletScheduler 进行调度和修复。修复的实际操作，都由 BE 上的 clone 任务完成。FE 只负责生成这些 clone 任务。

> 注1：副本修复的主要思想是先通过创建或补齐使得分片的副本数达到期望值，然后再删除多余的副本。
> 
> 注2：一个 clone 任务就是完成从一个指定远端 BE 拷贝指定数据到指定目的端 BE 的过程。

针对不同的状态，我们采用不同的修复方式：

1. REPLICA\_MISSING/REPLICA\_RELOCATING

    选择一个低负载的，可用的 BE 节点作为目的端。选择一个健康副本作为源端。clone 任务会从源端拷贝一个完整的副本到目的端。对于副本补齐，我们会直接选择一个可用的 BE 节点，而不考虑存储介质。
    
2. VERSION\_INCOMPLETE

    选择一个相对完整的副本作为目的端。选择一个健康副本作为源端。clone 任务会从源端尝试拷贝缺失的版本到目的端的副本。
    
3. REPLICA\_MISSING\_IN\_CLUSTER

    这种状态处理方式和 REPLICA\_MISSING 相同。
    
4. REDUNDANT

    通常经过副本修复后，分片会有冗余的副本。我们选择一个冗余副本将其删除。冗余副本的选择遵从以下优先级：
    1. 副本所在 BE 已经下线
    2. 副本已损坏
    3. 副本所在 BE 失联或在下线中
    4. 副本处于 CLONE 状态（该状态是 clone 任务执行过程中的一个中间状态）
    5. 副本有版本缺失
    6. 副本所在 cluster 不正确
    7. 副本所在 BE 节点负载高

5. FORCE\_REDUNDANT

    不同于 REDUNDANT，因为此时虽然存活的副本数小于期望副本数，但是因为已经没有额外的可用节点用于创建新的副本了。所以此时必须先删除一个副本，以腾出一个可用节点用于创建新的副本。
    删除副本的顺序同 REDUNDANT。
    
6. COLOCATE\_MISMATCH

    从 Colocation Group 中指定的副本分布 BE 节点中选择一个作为目的节点进行副本补齐。

7. COLOCATE\_REDUNDANT

    删除一个非 Colocation Group 中指定的副本分布 BE 节点上的副本。

Doris 在选择副本节点时，不会将同一个 Tablet 的副本部署在同一个 host 的不同 BE 上。保证了即使同一个 host 上的所有 BE 都挂掉，也不会造成全部副本丢失。

### 调度优先级

TabletScheduler 里等待被调度的分片会根据状态不同，赋予不同的优先级。优先级高的分片将会被优先调度。目前有以下几种优先级。
    
1. VERY\_HIGH

    * REDUNDANT。对于有副本冗余的分片，我们优先处理。虽然逻辑上来讲，副本冗余的紧急程度最低，但是因为这种情况处理起来最快且可以快速释放资源（比如磁盘空间等），所以我们优先处理。
    * FORCE\_REDUNDANT。同上。

2. HIGH

    * REPLICA\_MISSING 且多数副本缺失（比如3副本丢失了2个）
    * VERSION\_INCOMPLETE 且多数副本的版本缺失
    * COLOCATE\_MISMATCH 我们希望 Colocation 表相关的分片能够尽快修复完成。
    * COLOCATE\_REDUNDANT

3. NORMAL

    * REPLICA\_MISSING 但多数存活（比如3副本丢失了1个）
    * VERSION\_INCOMPLETE 但多数副本的版本完整
    * REPLICA\_RELOCATING 且多数副本需要 relocate（比如3副本有2个）

4. LOW

    * REPLICA\_MISSING\_IN\_CLUSTER
    * REPLICA\_RELOCATING 但多数副本 stable

### 手动优先级

系统会自动判断调度优先级。但是有些时候，用户希望某些表或分区的分片能够更快的被修复。因此我们提供一个命令，用户可以指定某个表或分区的分片被优先修复：

`ADMIN REPAIR TABLE tbl [PARTITION (p1, p2, ...)];`

这个命令，告诉 TC，在扫描 Tablet 时，对需要优先修复的表或分区中的有问题的 Tablet，给予 VERY\_HIGH 的优先级。
    
> 注：这个命令只是一个 hint，并不能保证一定能修复成功，并且优先级也会随 TS 的调度而发生变化。并且当 Master FE 切换或重启后，这些信息都会丢失。

可以通过以下命令取消优先级：

`ADMIN CANCEL REPAIR TABLE tbl [PARTITION (p1, p2, ...)];`

### 优先级调度

优先级保证了损坏严重的分片能够优先被修复，提高系统可用性。但是如果高优先级的修复任务一直失败，则会导致低优先级的任务一直得不到调度。因此，我们会根据任务的运行状态，动态的调整任务的优先级，保证所有任务都有机会被调度到。

* 连续5次调度失败（如无法获取资源，无法找到合适的源端或目的端等），则优先级会被下调。
* 持续 30 分钟未被调度，则上调优先级。 
* 同一 tablet 任务的优先级至少间隔 5 分钟才会被调整一次。

同时为了保证初始优先级的权重，我们规定，初始优先级为 VERY\_HIGH 的，最低被下调到 NORMAL。而初始优先级为 LOW 的，最多被上调为 HIGH。这里的优先级调整，也会调整用户手动设置的优先级。

## 副本均衡

Doris 会自动进行集群内的副本均衡。目前支持两种均衡策略，负载/分区。负载均衡适合需要兼顾节点磁盘使用率和节点副本数量的场景；而分区均衡会使每个分区的副本都均匀分布在各个节点，避免热点，适合对分区读写要求比较高的场景。但是，分区均衡不考虑磁盘使用率，使用分区均衡时需要注意磁盘的使用情况。 策略只能在fe启动前配置[tablet_rebalancer_type](../config/fe-config.md)  ，不支持运行时切换。

### 负载均衡

负载均衡的主要思想是，对某些分片，先在低负载的节点上创建一个副本，然后再删除这些分片在高负载节点上的副本。同时，因为不同存储介质的存在，在同一个集群内的不同 BE 节点上，可能存在一种或两种存储介质。我们要求存储介质为 A 的分片在均衡后，尽量依然存储在存储介质 A 中。所以我们根据存储介质，对集群的 BE 节点进行划分。然后针对不同的存储介质的 BE 节点集合，进行负载均衡调度。

同样，副本均衡会保证不会将同一个 Tablet 的副本部署在同一个 host 的 BE 上。

#### BE 节点负载

我们用 ClusterLoadStatistics（CLS）表示一个 cluster 中各个 Backend 的负载均衡情况。TabletScheduler 根据这个统计值，来触发集群均衡。我们当前通过 **磁盘使用率** 和 **副本数量** 两个指标，为每个BE计算一个 loadScore，作为 BE 的负载分数。分数越高，表示该 BE 的负载越重。

磁盘使用率和副本数量各有一个权重系数，分别为 **capacityCoefficient** 和 **replicaNumCoefficient**，其 **和恒为1**。其中 capacityCoefficient 会根据实际磁盘使用率动态调整。当一个 BE 的总体磁盘使用率在 50% 以下，则 capacityCoefficient 值为 0.5，如果磁盘使用率在 75%（可通过 FE 配置项 `capacity_used_percent_high_water` 配置）以上，则值为 1。如果使用率介于 50% ~ 75% 之间，则该权重系数平滑增加，公式为：

`capacityCoefficient= 2 * 磁盘使用率 - 0.5`

该权重系数保证当磁盘使用率过高时，该 Backend 的负载分数会更高，以保证尽快降低这个 BE 的负载。

TabletScheduler 会每隔 20s 更新一次 CLS。

### 分区均衡

分区均衡的主要思想是，将每个分区的在各个 Backend 上的 replica 数量差（即 partition skew），减少到最小。因此只考虑副本个数，不考虑磁盘使用率。
为了尽量少的迁移次数，分区均衡使用二维贪心的策略，优先均衡partition skew最大的分区，均衡分区时会尽量选择，可以使整个 cluster 的在各个 Backend 上的 replica 数量差（即 cluster skew/total skew）减少的方向。

#### skew 统计

skew 统计信息由`ClusterBalanceInfo`表示，其中，`partitionInfoBySkew`以 partition skew 为key排序，便于找到max partition skew；`beByTotalReplicaCount`则是以 Backend 上的所有 replica 个数为key排序。`ClusterBalanceInfo`同样保持在CLS中, 同样 20s 更新一次。

max partition skew 的分区可能有多个，采用随机的方式选择一个分区计算。

### 均衡策略

TabletScheduler 在每轮调度时，都会通过 LoadBalancer 来选择一定数目的健康分片作为 balance 的候选分片。在下一次调度时，会尝试根据这些候选分片，进行均衡调度。

## 资源控制

无论是副本修复还是均衡，都是通过副本在各个 BE 之间拷贝完成的。如果同一台 BE 同一时间执行过多的任务，则会带来不小的 IO 压力。因此，Doris 在调度时控制了每个节点上能够执行的任务数目。最小的资源控制单位是磁盘（即在 be.conf 中指定的一个数据路径）。我们默认为每块磁盘配置两个 slot 用于副本修复。一个 clone 任务会占用源端和目的端各一个 slot。如果 slot 数目为零，则不会再对这块磁盘分配任务。该 slot 个数可以通过 FE 的 `schedule_slot_num_per_path` 参数配置。 

另外，我们默认为每块磁盘提供 2 个单独的 slot 用于均衡任务。目的是防止高负载的节点因为 slot 被修复任务占用，而无法通过均衡释放空间。

## 副本状态查看

副本状态查看主要是查看副本的状态，以及副本修复和均衡任务的运行状态。这些状态大部分都**仅存在于** Master FE 节点中。因此，以下命令需直连到 Master FE 执行。

### 副本状态

1. 全局状态检查

    通过 `SHOW PROC '/cluster_health/tablet_health';` 命令可以查看整个集群的副本状态。

    ```
    +-------+--------------------------------+-----------+------------+-------------------+----------------------+----------------------+--------------+----------------------------+-------------------------+-------------------+---------------------+----------------------+----------------------+------------------+-----------------------------+-----------------+-------------+------------+
    | DbId  | DbName                         | TabletNum | HealthyNum | ReplicaMissingNum | VersionIncompleteNum | ReplicaRelocatingNum | RedundantNum | ReplicaMissingInClusterNum | ReplicaMissingForTagNum | ForceRedundantNum | ColocateMismatchNum | ColocateRedundantNum | NeedFurtherRepairNum | UnrecoverableNum | ReplicaCompactionTooSlowNum | InconsistentNum | OversizeNum | CloningNum |
    +-------+--------------------------------+-----------+------------+-------------------+----------------------+----------------------+--------------+----------------------------+-------------------------+-------------------+---------------------+----------------------+----------------------+------------------+-----------------------------+-----------------+-------------+------------+
    | 10005 | default_cluster:doris_audit_db | 84        | 84         | 0                 | 0                    | 0                    | 0            | 0                          | 0                       | 0                 | 0                   | 0                    | 0                    | 0                | 0                           | 0               | 0           | 0          |
    | 13402 | default_cluster:ssb1           | 709       | 708        | 1                 | 0                    | 0                    | 0            | 0                          | 0                       | 0                 | 0                   | 0                    | 0                    | 0                | 0                           | 0               | 0           | 0          |
    | 10108 | default_cluster:tpch1          | 278       | 278        | 0                 | 0                    | 0                    | 0            | 0                          | 0                       | 0                 | 0                   | 0                    | 0                    | 0                | 0                           | 0               | 0           | 0          |
    | Total | 3                              | 1071      | 1070       | 1                 | 0                    | 0                    | 0            | 0                          | 0                       | 0                 | 0                   | 0                    | 0                    | 0                | 0                           | 0               | 0           | 0          |
    +-------+--------------------------------+-----------+------------+-------------------+----------------------+----------------------+--------------+----------------------------+-------------------------+-------------------+---------------------+----------------------+----------------------+------------------+-----------------------------+-----------------+-------------+------------+
    ```

    其中 `HealthyNum` 列显示了对应的 Database 中，有多少 Tablet 处于健康状态。`ReplicaCompactionTooSlowNum` 列显示了对应的 Database 中，有多少 Tablet的 处于副本版本数过多的状态， `InconsistentNum` 列显示了对应的 Database 中，有多少 Tablet 处于副本不一致的状态。最后一行 `Total` 行对整个集群进行了统计。正常情况下 `TabletNum` 和 `HealthNum` 应该相等。如果不相等，可以进一步查看具体有哪些 Tablet。如上图中，ssb1 数据库有 1 个 Tablet 状态不健康，则可以使用以下命令查看具体是哪一个 Tablet。
    
    `SHOW PROC '/cluster_health/tablet_health/13402';`
    
    其中 `13402` 为对应的 DbId。

   ```
   +-----------------------+--------------------------+--------------------------+------------------+--------------------------------+-----------------------------+-----------------------+-------------------------+--------------------------+--------------------------+----------------------+---------------------------------+---------------------+-----------------+
   | ReplicaMissingTablets | VersionIncompleteTablets | ReplicaRelocatingTablets | RedundantTablets | ReplicaMissingInClusterTablets | ReplicaMissingForTagTablets | ForceRedundantTablets | ColocateMismatchTablets | ColocateRedundantTablets | NeedFurtherRepairTablets | UnrecoverableTablets | ReplicaCompactionTooSlowTablets | InconsistentTablets | OversizeTablets |
   +-----------------------+--------------------------+--------------------------+------------------+--------------------------------+-----------------------------+-----------------------+-------------------------+--------------------------+--------------------------+----------------------+---------------------------------+---------------------+-----------------+
   | 14679                 |                          |                          |                  |                                |                             |                       |                         |                          |                          |                      |                                 |                     |                 |
   +-----------------------+--------------------------+--------------------------+------------------+--------------------------------+-----------------------------+-----------------------+-------------------------+--------------------------+--------------------------+----------------------+---------------------------------+---------------------+-----------------+
   ```

    上图会显示具体的不健康的 Tablet ID（14679），该 Tablet 处于 ReplicaMissing 的状态。后面我们会介绍如何查看一个具体的 Tablet 的各个副本的状态。
    
2. 表（分区）级别状态检查
   
    用户可以通过以下命令查看指定表或分区的副本状态，并可以通过 WHERE 语句对状态进行过滤。如查看表 tbl1 中，分区 p1 和 p2 上状态为 OK 的副本：
    
    `ADMIN SHOW REPLICA STATUS FROM tbl1 PARTITION (p1, p2) WHERE STATUS = "OK";`
    
    ```
    +----------+-----------+-----------+---------+-------------------+--------------------+------------------+------------+------------+-------+--------+--------+
    | TabletId | ReplicaId | BackendId | Version | LastFailedVersion | LastSuccessVersion | CommittedVersion | SchemaHash | VersionNum | IsBad | State  | Status |
    +----------+-----------+-----------+---------+-------------------+--------------------+------------------+------------+------------+-------+--------+--------+
    | 29502429 | 29502432  | 10006     | 2       | -1                | 2                  | 1                | -1         | 2          | false | NORMAL | OK     |
    | 29502429 | 36885996  | 10002     | 2       | -1                | -1                 | 1                | -1         | 2          | false | NORMAL | OK     |
    | 29502429 | 48100551  | 10007     | 2       | -1                | -1                 | 1                | -1         | 2          | false | NORMAL | OK     |
    | 29502433 | 29502434  | 10001     | 2       | -1                | 2                  | 1                | -1         | 2          | false | NORMAL | OK     |
    | 29502433 | 44900737  | 10004     | 2       | -1                | -1                 | 1                | -1         | 2          | false | NORMAL | OK     |
    | 29502433 | 48369135  | 10006     | 2       | -1                | -1                 | 1                | -1         | 2          | false | NORMAL | OK     |
    +----------+-----------+-----------+---------+-------------------+--------------------+------------------+------------+------------+-------+--------+--------+
    ```

    这里会展示所有副本的状态。其中 `IsBad` 列为 `true` 则表示副本已经损坏。而 `Status` 列则会显示另外的其他状态。具体的状态说明，可以通过 `HELP ADMIN SHOW REPLICA STATUS;` 查看帮助。
    
    `ADMIN SHOW REPLICA STATUS` 命令主要用于查看副本的健康状态。用户还可以通过以下命令查看指定表中副本的一些额外信息：
    
    `SHOW TABLETS FROM tbl1;`
    
    ```
    +----------+-----------+-----------+------------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+----------+----------+--------+-------------------------+--------------+----------------------+--------------+----------------------+----------------------+----------------------+
    | TabletId | ReplicaId | BackendId | SchemaHash | Version | VersionHash | LstSuccessVersion | LstSuccessVersionHash | LstFailedVersion | LstFailedVersionHash | LstFailedTime | DataSize | RowCount | State  | LstConsistencyCheckTime | CheckVersion |     CheckVersionHash | VersionCount | PathHash             | MetaUrl              | CompactionStatus     |
    +----------+-----------+-----------+------------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+----------+----------+--------+-------------------------+--------------+----------------------+--------------+----------------------+----------------------+----------------------+
    | 29502429 | 29502432  | 10006     | 1421156361 | 2       | 0           | 2                 | 0                     | -1               | 0                    | N/A           | 784      | 0        | NORMAL | N/A                     | -1           |     -1               | 2            | -5822326203532286804 | url                  | url                  |
    | 29502429 | 36885996  | 10002     | 1421156361 | 2       | 0           | -1                | 0                     | -1               | 0                    | N/A           | 784      | 0        | NORMAL | N/A                     | -1           |     -1               | 2            | -1441285706148429853 | url                  | url                  |
    | 29502429 | 48100551  | 10007     | 1421156361 | 2       | 0           | -1                | 0                     | -1               | 0                    | N/A           | 784      | 0        | NORMAL | N/A                     | -1           |     -1               | 2            | -4784691547051455525 | url                  | url                  |
    +----------+-----------+-----------+------------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+----------+----------+--------+-------------------------+--------------+----------------------+--------------+----------------------+----------------------+----------------------+  
    ```
    
    上图展示了包括副本大小、行数、版本数量、所在数据路径等一些额外的信息。
    
    > 注：这里显示的 `State` 列的内容不代表副本的健康状态，而是副本处于某种任务下的状态，比如 CLONE、SCHEMA\_CHANGE、ROLLUP 等。

    此外，用户也可以通过以下命令，查看指定表或分区的副本分布情况，来检查副本分布是否均匀。
    
    `ADMIN SHOW REPLICA DISTRIBUTION FROM tbl1;`
    
    ```
    +-----------+------------+-------+---------+
    | BackendId | ReplicaNum | Graph | Percent |
    +-----------+------------+-------+---------+
    | 10000     | 7          |       | 7.29 %  |
    | 10001     | 9          |       | 9.38 %  |
    | 10002     | 7          |       | 7.29 %  |
    | 10003     | 7          |       | 7.29 %  |
    | 10004     | 9          |       | 9.38 %  |
    | 10005     | 11         | >     | 11.46 % |
    | 10006     | 18         | >     | 18.75 % |
    | 10007     | 15         | >     | 15.62 % |
    | 10008     | 13         | >     | 13.54 % |
    +-----------+------------+-------+---------+
    ```
    
    这里分别展示了表 tbl1 的副本在各个 BE 节点上的个数、百分比，以及一个简单的图形化显示。
    
4. Tablet 级别状态检查

    当我们要定位到某个具体的 Tablet 时，可以使用如下命令来查看一个具体的 Tablet 的状态。如查看 ID 为 29502553 的 tablet：
    
    `SHOW TABLET 29502553;`

    ```
    +------------------------+-----------+---------------+-----------+----------+----------+-------------+----------+--------+---------------------------------------------------------------------------+
    | DbName                 | TableName | PartitionName | IndexName | DbId     | TableId  | PartitionId | IndexId  | IsSync | DetailCmd                                                                 |
    +------------------------+-----------+---------------+-----------+----------+----------+-------------+----------+--------+---------------------------------------------------------------------------+
    | default_cluster:test   | test      | test          | test      | 29502391 | 29502428 | 29502427    | 29502428 | true   | SHOW PROC '/dbs/29502391/29502428/partitions/29502427/29502428/29502553'; |
    +------------------------+-----------+---------------+-----------+----------+----------+-------------+----------+--------+---------------------------------------------------------------------------+
    ```
    
    上图显示了这个 tablet 所对应的数据库、表、分区、上卷表等信息。用户可以复制 `DetailCmd` 命令中的命令继续执行：
    
    `SHOW PROC '/dbs/29502391/29502428/partitions/29502427/29502428/29502553';`
    
    ```
    +-----------+-----------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+------------+----------+----------+--------+-------+--------------+----------------------+----------+------------------+
    | ReplicaId | BackendId | Version | VersionHash | LstSuccessVersion | LstSuccessVersionHash | LstFailedVersion | LstFailedVersionHash | LstFailedTime | SchemaHash | DataSize | RowCount | State  | IsBad | VersionCount | PathHash             | MetaUrl  | CompactionStatus |
    +-----------+-----------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+------------+----------+----------+--------+-------+--------------+----------------------+----------+------------------+
    | 43734060  | 10004     | 2       | 0           | -1                | 0                     | -1               | 0                    | N/A           | -1         | 784      | 0        | NORMAL | false | 2            | -8566523878520798656 | url      | url              |
    | 29502555  | 10002     | 2       | 0           | 2                 | 0                     | -1               | 0                    | N/A           | -1         | 784      | 0        | NORMAL | false | 2            | 1885826196444191611  | url      | url              |
    | 39279319  | 10007     | 2       | 0           | -1                | 0                     | -1               | 0                    | N/A           | -1         | 784      | 0        | NORMAL | false | 2            | 1656508631294397870  | url      | url              |
    +-----------+-----------+---------+-------------+-------------------+-----------------------+------------------+----------------------+---------------+------------+----------+----------+--------+-------+--------------+----------------------+----------+------------------+ 
    ```

    上图显示了对应 Tablet 的所有副本情况。这里显示的内容和 `SHOW TABLETS FROM tbl1;` 的内容相同。但这里可以清楚的知道，一个具体的 Tablet 的所有副本的状态。

### 副本调度任务

1. 查看等待被调度的任务

    `SHOW PROC '/cluster_balance/pending_tablets';`

    ```
    +----------+--------+-----------------+---------+----------+----------+-------+---------+--------+----------+---------+---------------------+---------------------+---------------------+----------+------+-------------+---------------+---------------------+------------+---------------------+--------+---------------------+-------------------------------+
    | TabletId | Type   | Status          | State   | OrigPrio | DynmPrio | SrcBe | SrcPath | DestBe | DestPath | Timeout | Create              | LstSched            | LstVisit            | Finished | Rate | FailedSched | FailedRunning | LstAdjPrio          | VisibleVer | VisibleVerHash      | CmtVer | CmtVerHash          | ErrMsg                        |
    +----------+--------+-----------------+---------+----------+----------+-------+---------+--------+----------+---------+---------------------+---------------------+---------------------+----------+------+-------------+---------------+---------------------+------------+---------------------+--------+---------------------+-------------------------------+
    | 4203036  | REPAIR | REPLICA_MISSING | PENDING | HIGH     | LOW      | -1    | -1      | -1     | -1       | 0       | 2019-02-21 15:00:20 | 2019-02-24 11:18:41 | 2019-02-24 11:18:41 | N/A      | N/A  | 2           | 0             | 2019-02-21 15:00:43 | 1          | 0                   | 2      | 0                   | unable to find source replica |
    +----------+--------+-----------------+---------+----------+----------+-------+---------+--------+----------+---------+---------------------+---------------------+---------------------+----------+------+-------------+---------------+---------------------+------------+---------------------+--------+---------------------+-------------------------------+
    ```

    各列的具体含义如下：
    
    * TabletId：等待调度的 Tablet 的 ID。一个调度任务只针对一个 Tablet
    * Type：任务类型，可以是 REPAIR（修复） 或 BALANCE（均衡）
    * Status：该 Tablet 当前的状态，如 REPLICA\_MISSING（副本缺失）
    * State：该调度任务的状态，可能为 PENDING/RUNNING/FINISHED/CANCELLED/TIMEOUT/UNEXPECTED
    * OrigPrio：初始的优先级
    * DynmPrio：当前动态调整后的优先级
    * SrcBe：源端 BE 节点的 ID
    * SrcPath：源端 BE 节点的路径的 hash 值
    * DestBe：目的端 BE 节点的 ID
    * DestPath：目的端 BE 节点的路径的 hash 值
    * Timeout：当任务被调度成功后，这里会显示任务的超时时间，单位秒
    * Create：任务被创建的时间
    * LstSched：上一次任务被调度的时间
    * LstVisit：上一次任务被访问的时间。这里“被访问”指包括被调度，任务执行汇报等和这个任务相关的被处理的时间点
    * Finished：任务结束时间
    * Rate：clone 任务的数据拷贝速率
    * FailedSched：任务调度失败的次数
    * FailedRunning：任务执行失败的次数
    * LstAdjPrio：上一次优先级调整的时间
    * CmtVer/CmtVerHash/VisibleVer/VisibleVerHash：用于执行 clone 任务的 version 信息
    * ErrMsg：任务被调度和运行过程中，出现的错误信息

2. 查看正在运行的任务

    `SHOW PROC '/cluster_balance/running_tablets';`
    
    其结果中各列的含义和 `pending_tablets` 相同。
    
3. 查看已结束任务

    `SHOW PROC '/cluster_balance/history_tablets';`

    我们默认只保留最近 1000 个完成的任务。其结果中各列的含义和 `pending_tablets` 相同。如果 `State` 列为 `FINISHED`，则说明任务正常完成。如果为其他，则可以根据 `ErrMsg` 列的错误信息查看具体原因。
    
## 集群负载及调度资源查看

1. 集群负载

    通过以下命令可以查看集群当前的负载情况：
    
    `SHOW PROC '/cluster_balance/cluster_load_stat/location_default';`
    
    首先看到的是对不同存储介质的划分：
    
    ```
    +---------------+
    | StorageMedium |
    +---------------+
    | HDD           |
    | SSD           |
    +---------------+
    ```
    
    点击某一种存储介质，可以看到包含该存储介质的 BE 节点的均衡状态：
    
    `SHOW PROC '/cluster_balance/cluster_load_stat/location_default/HDD';`
    
    ```
    +----------+-----------------+-----------+---------------+----------------+-------------+------------+----------+-----------+--------------------+-------+
    | BeId     | Cluster         | Available | UsedCapacity  | Capacity       | UsedPercent | ReplicaNum | CapCoeff | ReplCoeff | Score              | Class |
    +----------+-----------------+-----------+---------------+----------------+-------------+------------+----------+-----------+--------------------+-------+
    | 10003    | default_cluster | true      | 3477875259079 | 19377459077121 | 17.948      | 493477     | 0.5      | 0.5       | 0.9284678149967587 | MID   |
    | 10002    | default_cluster | true      | 3607326225443 | 19377459077121 | 18.616      | 496928     | 0.5      | 0.5       | 0.948660871419998  | MID   |
    | 10005    | default_cluster | true      | 3523518578241 | 19377459077121 | 18.184      | 545331     | 0.5      | 0.5       | 0.9843539990641831 | MID   |
    | 10001    | default_cluster | true      | 3535547090016 | 19377459077121 | 18.246      | 558067     | 0.5      | 0.5       | 0.9981869446537612 | MID   |
    | 10006    | default_cluster | true      | 3636050364835 | 19377459077121 | 18.764      | 547543     | 0.5      | 0.5       | 1.0011489897614072 | MID   |
    | 10004    | default_cluster | true      | 3506558163744 | 15501967261697 | 22.620      | 468957     | 0.5      | 0.5       | 1.0228319835582569 | MID   |
    | 10007    | default_cluster | true      | 4036460478905 | 19377459077121 | 20.831      | 551645     | 0.5      | 0.5       | 1.057279369420761  | MID   |
    | 10000    | default_cluster | true      | 4369719923760 | 19377459077121 | 22.551      | 547175     | 0.5      | 0.5       | 1.0964036415787461 | MID   |
    +----------+-----------------+-----------+---------------+----------------+-------------+------------+----------+-----------+--------------------+-------+
    ```
    
    其中一些列的含义如下：

    * Available：为 true 表示 BE 心跳正常，且没有处于下线中
    * UsedCapacity：字节，BE 上已使用的磁盘空间大小
    * Capacity：字节，BE 上总的磁盘空间大小
    * UsedPercent：百分比，BE 上的磁盘空间使用率
    * ReplicaNum：BE 上副本数量
    * CapCoeff/ReplCoeff：磁盘空间和副本数的权重系数
    * Score：负载分数。分数越高，负载越重
    * Class：根据负载情况分类，LOW/MID/HIGH。均衡调度会将高负载节点上的副本迁往低负载节点

    用户可以进一步查看某个 BE 上各个路径的使用率，比如 ID 为 10001 这个 BE：

    `SHOW PROC '/cluster_balance/cluster_load_stat/location_default/HDD/10001';`

    ```
    +------------------+------------------+---------------+---------------+---------+--------+----------------------+
    | RootPath         | DataUsedCapacity | AvailCapacity | TotalCapacity | UsedPct | State  | PathHash             |
    +------------------+------------------+---------------+---------------+---------+--------+----------------------+
    | /home/disk4/palo | 498.757 GB       | 3.033 TB      | 3.525 TB      | 13.94 % | ONLINE | 4883406271918338267  |
    | /home/disk3/palo | 704.200 GB       | 2.832 TB      | 3.525 TB      | 19.65 % | ONLINE | -5467083960906519443 |
    | /home/disk1/palo | 512.833 GB       | 3.007 TB      | 3.525 TB      | 14.69 % | ONLINE | -7733211489989964053 |
    | /home/disk2/palo | 881.955 GB       | 2.656 TB      | 3.525 TB      | 24.65 % | ONLINE | 4870995507205544622  |
    | /home/disk5/palo | 694.992 GB       | 2.842 TB      | 3.525 TB      | 19.36 % | ONLINE | 1916696897889786739  |
    +------------------+------------------+---------------+---------------+---------+--------+----------------------+
    ```

    这里显示了指定 BE 上，各个数据路径的磁盘使用率情况。
    
2. 调度资源

    用户可以通过以下命令，查看当前各个节点的 slot 使用情况：
    
    `SHOW PROC '/cluster_balance/working_slots';`

    ```
    +----------+----------------------+------------+------------+-------------+----------------------+
    | BeId     | PathHash             | AvailSlots | TotalSlots | BalanceSlot | AvgRate              |
    +----------+----------------------+------------+------------+-------------+----------------------+
    | 10000    | 8110346074333016794  | 2          | 2          | 2           | 2.459007474009069E7  |
    | 10000    | -5617618290584731137 | 2          | 2          | 2           | 2.4730105014001578E7 |
    | 10001    | 4883406271918338267  | 2          | 2          | 2           | 1.6711402709780257E7 |
    | 10001    | -5467083960906519443 | 2          | 2          | 2           | 2.7540126380326536E7 |
    | 10002    | 9137404661108133814  | 2          | 2          | 2           | 2.417217089806745E7  |
    | 10002    | 1885826196444191611  | 2          | 2          | 2           | 1.6327378456676323E7 |
    +----------+----------------------+------------+------------+-------------+----------------------+
    ```

    这里以数据路径为粒度，展示了当前 slot 的使用情况。其中 `AvgRate` 为历史统计的该路径上 clone 任务的拷贝速率，单位是字节/秒。
    
3. 优先修复查看

    以下命令，可以查看通过 `ADMIN REPAIR TABLE` 命令设置的优先修复的表或分区。
    
    `SHOW PROC '/cluster_balance/priority_repair';`
    
    其中 `RemainingTimeMs` 表示，这些优先修复的内容，将在这个时间后，被自动移出优先修复队列。以防止优先修复一直失败导致资源被占用。
    
### 调度器统计状态查看

我们收集了 TabletChecker 和 TabletScheduler 在运行过程中的一些统计信息，可以通过以下命令查看：

`SHOW PROC '/cluster_balance/sched_stat';`

```
+---------------------------------------------------+-------------+
| Item                                              | Value       |
+---------------------------------------------------+-------------+
| num of tablet check round                         | 12041       |
| cost of tablet check(ms)                          | 7162342     |
| num of tablet checked in tablet checker           | 18793506362 |
| num of unhealthy tablet checked in tablet checker | 7043900     |
| num of tablet being added to tablet scheduler     | 1153        |
| num of tablet schedule round                      | 49538       |
| cost of tablet schedule(ms)                       | 49822       |
| num of tablet being scheduled                     | 4356200     |
| num of tablet being scheduled succeeded           | 320         |
| num of tablet being scheduled failed              | 4355594     |
| num of tablet being scheduled discard             | 286         |
| num of tablet priority upgraded                   | 0           |
| num of tablet priority downgraded                 | 1096        |
| num of clone task                                 | 230         |
| num of clone task succeeded                       | 228         |
| num of clone task failed                          | 2           |
| num of clone task timeout                         | 2           |
| num of replica missing error                      | 4354857     |
| num of replica version missing error              | 967         |
| num of replica relocating                         | 0           |
| num of replica redundant error                    | 90          |
| num of replica missing in cluster error           | 0           |
| num of balance scheduled                          | 0           |
+---------------------------------------------------+-------------+
```

各行含义如下：

* num of tablet check round：Tablet Checker 检查次数
* cost of tablet check(ms)：Tablet Checker 检查总耗时
* num of tablet checked in tablet checker：Tablet Checker 检查过的 tablet 数量
* num of unhealthy tablet checked in tablet checker：Tablet Checker 检查过的不健康的 tablet 数量
* num of tablet being added to tablet scheduler：被提交到 Tablet Scheduler 中的 tablet 数量
* num of tablet schedule round：Tablet Scheduler 运行次数
* cost of tablet schedule(ms)：Tablet Scheduler 运行总耗时
* num of tablet being scheduled：被调度的 Tablet 总数量
* num of tablet being scheduled succeeded：被成功调度的 Tablet 总数量
* num of tablet being scheduled failed：调度失败的 Tablet 总数量
* num of tablet being scheduled discard：调度失败且被抛弃的 Tablet 总数量
* num of tablet priority upgraded：优先级上调次数
* num of tablet priority downgraded：优先级下调次数
* num of clone task：生成的 clone 任务数量
* num of clone task succeeded：clone 任务成功的数量
* num of clone task failed：clone 任务失败的数量
* num of clone task timeout：clone 任务超时的数量
* num of replica missing error：检查的状态为副本缺失的 tablet 的数量
* num of replica version missing error：检查的状态为版本缺失的 tablet 的数量（该统计值包括了 num of replica relocating 和 num of replica missing in cluster error）
* num of replica relocating：检查的状态为 replica relocating 的 tablet 的数量
* num of replica redundant error：检查的状态为副本冗余的 tablet 的数量
* num of replica missing in cluster error：检查的状态为不在对应 cluster 的 tablet 的数量
* num of balance scheduled：均衡调度的次数

> 注：以上状态都只是历史累加值。我们也在 FE 的日志中，定期打印了这些统计信息，其中括号内的数值表示自上次统计信息打印依赖，各个统计值的变化数量。

## 相关配置说明

### 可调整参数

以下可调整参数均为 fe.conf 中可配置参数。

* use\_new\_tablet\_scheduler

    * 说明：是否启用新的副本调度方式。新的副本调度方式即本文档介绍的副本调度方式。
    * 默认值：true
    * 重要性：高

* tablet\_repair\_delay\_factor\_second

    * 说明：对于不同的调度优先级，我们会延迟不同的时间后开始修复。以防止因为例行重启、升级等过程中，产生大量不必要的副本修复任务。此参数为一个基准系数。对于 HIGH 优先级，延迟为 基准系数 * 1；对于 NORMAL 优先级，延迟为 基准系数 * 2；对于 LOW 优先级，延迟为 基准系数 * 3。即优先级越低，延迟等待时间越长。如果用户想尽快修复副本，可以适当降低该参数。
    * 默认值：60秒
    * 重要性：高

* schedule\_slot\_num\_per\_path
  
    * 说明：默认分配给每块磁盘用于副本修复的 slot 数目。该数目表示一块磁盘能同时运行的副本修复任务数。如果想以更快的速度修复副本，可以适当调高这个参数。单数值越高，可能对 IO 影响越大。
    * 默认值：2
    * 重要性：高

* balance\_load\_score\_threshold

    * 说明：集群均衡的阈值。默认为 0.1，即 10%。当一个 BE 节点的 load score，不高于或不低于平均 load score 的 10% 时，我们认为这个节点是均衡的。如果想让集群负载更加平均，可以适当调低这个参数。
    * 默认值：0.1
    * 重要性：中

* storage\_high\_watermark\_usage\_percent 和 storage\_min\_left\_capacity\_bytes

    * 说明：这两个参数，分别表示一个磁盘的最大空间使用率上限，以及最小的空间剩余下限。当一块磁盘的空间使用率大于上限，或者剩余空间小于下限时，该磁盘将不再作为均衡调度的目的地址。
    * 默认值：0.85 和 2097152000 （2GB）
    * 重要性：中
    
* disable\_balance

    * 说明：控制是否关闭均衡功能。当副本处于均衡过程中时，有些功能，如 ALTER TABLE 等将会被禁止。而均衡可能持续很长时间。因此，如果用户希望尽快进行被禁止的操作。可以将该参数设为 true，以关闭均衡调度。
    * 默认值：false
    * 重要性：中

以下可调整参数均为 be.conf 中可配置参数。

* clone\_worker\_count

    * 说明：影响副本均衡的速度。在磁盘压力不大的情况下，可以通过调整该参数来加快副本均衡。
    * 默认值：3
    * 重要性：中

### 不可调整参数

以下参数暂不支持修改，仅作说明。

* TabletChecker 调度间隔

    TabletChecker 每20秒进行一次检查调度。
    
* TabletScheduler 调度间隔

    TabletScheduler 每5秒进行一次调度
    
* TabletScheduler 每批次调度个数

    TabletScheduler 每次调度最多 50 个 tablet。
    
* TabletScheduler 最大等待调度和运行中任务数

    最大等待调度任务数和运行中任务数为 2000。当超过 2000 后，TabletChecker 将不再产生新的调度任务给 TabletScheduler。
    
* TabletScheduler 最大均衡任务数

    最大均衡任务数为 500。当超过 500 后，将不再产生新的均衡任务。
    
* 每块磁盘用于均衡任务的 slot 数目

    每块磁盘用于均衡任务的 slot 数目为2。这个 slot 独立于用于副本修复的 slot。
    
* 集群均衡情况更新间隔

    TabletScheduler 每隔 20 秒会重新计算一次集群的 load score。
    
* Clone 任务的最小和最大超时时间

    一个 clone 任务超时时间范围是 3min ~ 2hour。具体超时时间通过 tablet 的大小计算。计算公式为 (tablet size) / (5MB/s)。当一个 clone 任务运行失败 3 次后，该任务将终止。
    
* 动态优先级调整策略

    优先级最小调整间隔为 5min。当一个 tablet 调度失败5次后，会调低优先级。当一个 tablet 30min 未被调度时，会调高优先级。

## 相关问题

* 在某些情况下，默认的副本修复和均衡策略可能会导致网络被打满（多发生在千兆网卡，且每台 BE 的磁盘数量较多的情况下）。此时需要调整一些参数来减少同时进行的均衡和修复任务数。

* 目前针对 Colocate Table 的副本的均衡策略无法保证同一个 Tablet 的副本不会分布在同一个 host 的 BE 上。但 Colocate Table 的副本的修复策略会检测到这种分布错误并校正。但可能会出现，校正后，均衡策略再次认为副本不均衡而重新均衡。从而导致在两种状态间不停交替，无法使 Colocate Group 达成稳定。针对这种情况，我们建议在使用 Colocate 属性时，尽量保证集群是同构的，以减小副本分布在同一个 host 上的概率。

## 最佳实践

### 控制并管理集群的副本修复和均衡进度

在大多数情况下，通过默认的参数配置，Doris 都可以自动的进行副本修复和集群均衡。但是某些情况下，我们需要通过人工介入调整参数，来达到一些特殊的目的。如优先修复某个表或分区、禁止集群均衡以降低集群负载、优先修复非 colocation 的表数据等等。

本小节主要介绍如何通过修改参数，来控制并管理集群的副本修复和均衡进度。

1. 删除损坏副本

    某些情况下，Doris 可能无法自动检测某些损坏的副本，从而导致查询或导入在损坏的副本上频繁报错。此时我们需要手动删除已损坏的副本。该方法可以适用于：删除版本数过高导致 -235 错误的副本、删除文件已损坏的副本等等。
    
    首先，找到副本对应的 tablet id，假设为 10001。通过 `show tablet 10001;` 并执行其中的 `show proc` 语句可以查看对应的 tablet 的各个副本详情。
    
    假设需要删除的副本的 backend id 是 20001。则执行以下语句将副本标记为 `bad`：
    
    ```
    ADMIN SET REPLICA STATUS PROPERTIES("tablet_id" = "10001", "backend_id" = "20001", "status" = "bad");
    ```
    
    此时，再次通过 `show proc` 语句可以看到对应的副本的 `IsBad` 列值为 `true`。
    
    被标记为 `bad` 的副本不会再参与导入和查询。同时副本修复逻辑会自动补充一个新的副本。
    
2. 优先修复某个表或分区

    `help admin repair table;` 查看帮助。该命令会尝试优先修复指定表或分区的tablet。
    
3. 停止均衡任务

    均衡任务会占用一定的网络带宽和IO资源。如果希望停止新的均衡任务的产生，可以通过以下命令：
    
    ```
    ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");
    ```

4. 停止所有副本调度任务

    副本调度任务包括均衡和修复任务。这些任务都会占用一定的网络带宽和IO资源。可以通过以下命令停止所有副本调度任务（不包括已经在运行的，包括 colocation 表和普通表）：
    
    ```
    ADMIN SET FRONTEND CONFIG ("disable_tablet_scheduler" = "true");
    ```

5. 停止所有 colocation 表的副本调度任务。

    colocation 表的副本调度和普通表是分开独立运行的。某些情况下，用户可能希望先停止对 colocation 表的均衡和修复工作，而将集群资源用于普通表的修复，则可以通过以下命令：
    
    ```
    ADMIN SET FRONTEND CONFIG ("disable_colocate_balance" = "true");
    ```

6. 使用更保守的策略修复副本

    Doris 在检测到副本缺失、BE宕机等情况下，会自动修复副本。但为了减少一些抖动导致的错误（如BE短暂宕机），Doris 会延迟触发这些任务。
    
    * `tablet_repair_delay_factor_second` 参数。默认 60 秒。根据修复任务优先级的不同，会推迟 60秒、120秒、180秒后开始触发修复任务。可以通过以下命令延长这个时间，这样可以容忍更长的异常时间，以避免触发不必要的修复任务：

    ```
    ADMIN SET FRONTEND CONFIG ("tablet_repair_delay_factor_second" = "120");
    ```

7. 使用更保守的策略触发 colocation group 的重分布

    colocation group 的重分布可能伴随着大量的 tablet 迁移。`colocate_group_relocate_delay_second` 用于控制重分布的触发延迟。默认 1800秒。如果某台 BE 节点可能长时间下线，可以尝试调大这个参数，以避免不必要的重分布：
    
    ```
    ADMIN SET FRONTEND CONFIG ("colocate_group_relocate_delay_second" = "3600");
    ```

8. 更快速的副本均衡

    Doris 的副本均衡逻辑会先增加一个正常副本，然后在删除老的副本，已达到副本迁移的目的。而在删除老副本时，Doris会等待这个副本上已经开始执行的导入任务完成，以避免均衡任务影响导入任务。但这样会降低均衡逻辑的执行速度。此时可以通过修改以下参数，让 Doris 忽略这个等待，直接删除老副本：
    
    ```
    ADMIN SET FRONTEND CONFIG ("enable_force_drop_redundant_replica" = "true");
    ```

    这种操作可能会导致均衡期间部分导入任务失败（需要重试），但会显著加速均衡速度。
    
总体来讲，当我们需要将集群快速恢复到正常状态时，可以考虑按照以下思路处理：

1. 找到导致高优任务报错的tablet，将有问题的副本置为 bad。
2. 通过 `admin repair` 语句高优修复某些表。
3. 停止副本均衡逻辑以避免占用集群资源，等集群恢复后，再开启即可。
4. 使用更保守的策略触发修复任务，以应对 BE 频繁宕机导致的雪崩效应。
5. 按需关闭 colocation 表的调度任务，集中集群资源修复其他高优数据。



