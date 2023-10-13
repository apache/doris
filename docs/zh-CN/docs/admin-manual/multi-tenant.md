---
{
    "title": "多租户和资源划分",
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

# 多租户和资源划分

Doris 的多租户和资源隔离方案，主要目的是为了多用户在同一 Doris 集群内进行数据操作时，减少相互之间的干扰，能够将集群资源更合理的分配给各用户。

该方案主要分为两部分，一是集群内节点级别的资源组划分，二是针对单个查询的资源限制。

## Doris 中的节点

首先先简单介绍一下 Doris 的节点组成。一个 Doris 集群中有两类节点：Frontend(FE) 和 Backend(BE)。

FE 主要负责元数据管理、集群管理、用户请求的接入和查询计划的解析等工作。

BE 主要负责数据存储、查询计划的执行等工作。

FE 不参与用户数据的处理计算等工作，因此是一个资源消耗较低的节点。而 BE 负责所有的数据计算、任务处理，属于资源消耗型的节点。因此，本文所介绍的资源划分及资源限制方案，都是针对 BE 节点的。FE 节点因为资源消耗相对较低，并且还可以横向扩展，因此通常无需做资源上的隔离和限制，FE 节点由所有用户共享即可。

## 节点资源划分

节点资源划分，是指将一个 Doris 集群内的 BE 节点设置标签（Tag），标签相同的 BE 节点组成一个资源组（Resource Group）。资源组可以看作是数据存储和计算的一个管理单元。下面我们通过一个具体示例，来介绍资源组的使用方式。

1. 为 BE 节点设置标签

   假设当前 Doris 集群有 6 个 BE 节点。分别为 host[1-6]。在初始情况下，所有节点都属于一个默认资源组（Default）。

   我们可以使用以下命令将这6个节点划分成3个资源组：group_a、group_b、group_c：

   ```sql
   alter system modify backend "host1:9050" set ("tag.location" = "group_a");
   alter system modify backend "host2:9050" set ("tag.location" = "group_a");
   alter system modify backend "host3:9050" set ("tag.location" = "group_b");
   alter system modify backend "host4:9050" set ("tag.location" = "group_b");
   alter system modify backend "host5:9050" set ("tag.location" = "group_c");
   alter system modify backend "host6:9050" set ("tag.location" = "group_c");
   ```

   这里我们将 `host[1-2]` 组成资源组 `group_a`，`host[3-4]` 组成资源组 `group_b`，`host[5-6]` 组成资源组 `group_c`。

   > 注：一个 BE 只支持设置一个 Tag。

2. 按照资源组分配数据分布

   资源组划分好后。我们可以将用户数据的不同副本分布在不同资源组内。假设一张用户表 UserTable。我们希望在3个资源组内各存放一个副本，则可以通过如下建表语句实现：

   ```sql
   create table UserTable
   (k1 int, k2 int)
   distributed by hash(k1) buckets 1
   properties(
       "replication_allocation"="tag.location.group_a:1, tag.location.group_b:1, tag.location.group_c:1"
   )
   ```

   这样一来，表 UserTable 中的数据，将会以3副本的形式，分别存储在资源组 group_a、group_b、group_c所在的节点中。

   下图展示了当前的节点划分和数据分布：

   ```text
    ┌────────────────────────────────────────────────────┐
    │                                                    │
    │         ┌──────────────────┐  ┌──────────────────┐ │
    │         │ host1            │  │ host2            │ │
    │         │  ┌─────────────┐ │  │                  │ │
    │ group_a │  │   replica1  │ │  │                  │ │
    │         │  └─────────────┘ │  │                  │ │
    │         │                  │  │                  │ │
    │         └──────────────────┘  └──────────────────┘ │
    │                                                    │
    ├────────────────────────────────────────────────────┤
    ├────────────────────────────────────────────────────┤
    │                                                    │
    │         ┌──────────────────┐  ┌──────────────────┐ │
    │         │ host3            │  │ host4            │ │
    │         │                  │  │  ┌─────────────┐ │ │
    │ group_b │                  │  │  │   replica2  │ │ │
    │         │                  │  │  └─────────────┘ │ │
    │         │                  │  │                  │ │
    │         └──────────────────┘  └──────────────────┘ │
    │                                                    │
    ├────────────────────────────────────────────────────┤
    ├────────────────────────────────────────────────────┤
    │                                                    │
    │         ┌──────────────────┐  ┌──────────────────┐ │
    │         │ host5            │  │ host6            │ │
    │         │                  │  │  ┌─────────────┐ │ │
    │ group_c │                  │  │  │   replica3  │ │ │
    │         │                  │  │  └─────────────┘ │ │
    │         │                  │  │                  │ │
    │         └──────────────────┘  └──────────────────┘ │
    │                                                    │
    └────────────────────────────────────────────────────┘
   ```
   
   为了方便设置table的数据分布策略，可以在database层面设置统一的数据分布策略，但是table设置的优先级高于database

   ```sql
   CREATE DATABASE db_name PROPERTIES (
   "replication_allocation" = "tag.location.group_a:1, tag.location.group_b:1"
   )
   ```

3. 使用不同资源组进行数据查询

   在前两步执行完成后，我们就可以通过设置用户的资源使用权限，来限制某一用户的查询，只能使用指定资源组中的节点来执行。

   比如我们可以通过以下语句，限制 user1 只能使用 `group_a` 资源组中的节点进行数据查询，user2 只能使用 `group_b` 资源组，而 user3 可以同时使用 3 个资源组：

   ```sql
   set property for 'user1' 'resource_tags.location' = 'group_a';
   set property for 'user2' 'resource_tags.location' = 'group_b';
   set property for 'user3' 'resource_tags.location' = 'group_a, group_b, group_c';
   ```

   设置完成后，user1 在发起对 UserTable 表的查询时，只会访问 `group_a` 资源组内节点上的数据副本，并且查询仅会使用 `group_a` 资源组内的节点计算资源。而 user3 的查询可以使用任意资源组内的副本和计算资源。

   > 注：默认情况下，用户的 `resource_tags.location` 属性为空，在2.0.2（含）之前的版本中，默认情况下，用户不受 tag 的限制，可以使用任意资源组。在 2.0.3 版本之后，默认情况下，用户只能使用 `default` 资源组。

   这样，我们通过对节点的划分，以及对用户的资源使用限制，实现了不同用户查询上的物理资源隔离。更进一步，我们可以给不同的业务部门创建不同的用户，并限制每个用户使用不同的资源组。以避免不同业务部分之间使用资源干扰。比如集群内有一张业务表需要共享给所有9个业务部门使用，但是希望能够尽量避免不同部门之间的资源抢占。则我们可以为这张表创建3个副本，分别存储在3个资源组中。接下来，我们为9个业务部门创建9个用户，每3个用户限制使用一个资源组。这样，资源的竞争程度就由9降低到了3。

   另一方面，针对在线和离线任务的隔离。我们可以利用资源组的方式实现。比如我们可以将节点划分为 Online 和 Offline 两个资源组。表数据依然以3副本的方式存储，其中 2 个副本存放在 Online 资源组，1 个副本存放在 Offline 资源组。Online 资源组主要用于高并发低延迟的在线数据服务，而一些大查询或离线ETL操作，则可以使用 Offline 资源组中的节点执行。从而实现在统一集群内同时提供在线和离线服务的能力。

4. 导入作业的资源组分配

   导入作业（包括insert、broker load、routine load、stream load等）的资源使用可以分为两部分：
   1. 计算资源：负责读取数据源、数据转换和分发。
   2. 写入资源：负责数据编码、压缩并写入磁盘。

   其中写入资源必须是数据副本所在的节点，而计算资源理论上可以选择任意节点完成。所以对于导入作业的资源组的分配分成两个步骤：
   1. 使用用户级别的 resource tag 来限定计算资源所能使用的资源组。
   2. 使用副本的 resource tag 来限定写入资源所能使用的资源组。

   所以如果希望导入操作所使用的全部资源都限定在数据所在的资源组的话，只需将用户级别的 resource tag 设置为和副本的 resource tag 相同即可。

## 单查询资源限制

前面提到的资源组方法是节点级别的资源隔离和限制。而在资源组内，依然可能发生资源抢占问题。比如前文提到的将3个业务部门安排在同一资源组内。虽然降低了资源竞争程度，但是这3个部门的查询依然有可能相互影响。

因此，除了资源组方案外，Doris 还提供了对单查询的资源限制功能。

目前 Doris 对单查询的资源限制主要分为 CPU 和 内存限制两方面。

1. 内存限制

   Doris 可以限制一个查询被允许使用的最大内存开销。以保证集群的内存资源不会被某一个查询全部占用。我们可以通过以下方式设置内存限制：

   ```sql
   # 设置会话变量 exec_mem_limit。则之后该会话内（连接内）的所有查询都使用这个内存限制。
   set exec_mem_limit=1G;
   # 设置全局变量 exec_mem_limit。则之后所有新会话（新连接）的所有查询都使用这个内存限制。
   set global exec_mem_limit=1G;
   # 在 SQL 中设置变量 exec_mem_limit。则该变量仅影响这个 SQL。
   select /*+ SET_VAR(exec_mem_limit=1G) */ id, name from tbl where xxx;
   ```

   因为 Doris 的查询引擎是基于全内存的 MPP 查询框架。因此当一个查询的内存使用超过限制后，查询会被终止。因此，当一个查询无法在合理的内存限制下运行时，我们就需要通过一些 SQL 优化手段，或者集群扩容的方式来解决了。

2. CPU 限制

   用户可以通过以下方式限制查询的 CPU 资源：

   ```sql
   # 设置会话变量 cpu_resource_limit。则之后该会话内（连接内）的所有查询都使用这个CPU限制。
   set cpu_resource_limit = 2
   # 设置用户的属性 cpu_resource_limit，则所有该用户的查询情况都使用这个CPU限制。该属性的优先级高于会话变量 cpu_resource_limit
   set property for 'user1' 'cpu_resource_limit' = '3';
   ```

   `cpu_resource_limit` 的取值是一个相对值，取值越大则能够使用的 CPU 资源越多。但一个查询能使用的CPU上限也取决于表的分区分桶数。原则上，一个查询的最大 CPU 使用量和查询涉及到的 tablet 数量正相关。极端情况下，假设一个查询仅涉及到一个 tablet，则即使 `cpu_resource_limit` 设置一个较大值，也仅能使用 1 个 CPU 资源。

通过内存和CPU的资源限制。我们可以在一个资源组内，将用户的查询进行更细粒度的资源划分。比如我们可以让部分时效性要求不高，但是计算量很大的离线任务使用更少的CPU资源和更多的内存资源。而部分延迟敏感的在线任务，使用更多的CPU资源以及合理的内存资源。

## 最佳实践和向前兼容

### Tag 划分和 CPU 限制是 0.15 版本中的新功能。为了保证可以从老版本平滑升级，Doris 做了如下的向前兼容：

1. 每个 BE 节点会有一个默认的 Tag：`"tag.location": "default"`。
2. 通过 `alter system add backend` 语句新增的 BE 节点也会默认设置 Tag：`"tag.location": "default"`。
3. 所有表的副本分布默认修改为：`"tag.location.default:xx`。其中 xx 为原副本数量。
4. 用户依然可以通过 `"replication_num" = "xx"` 在建表语句中指定副本数，这种属性将会自动转换成：`"tag.location.default:xx`。从而保证无需修改原建表语句。
5. 默认情况下，单查询的内存限制为单节点2GB，CPU资源无限制，和原有行为保持一致。且用户的 `resource_tags.location` 属性为空，即默认情况下，用户可以访问任意 Tag 的 BE，和原有行为保持一致。

这里我们给出一个从原集群升级到 0.15 版本后，开始使用资源划分功能的步骤示例：

1. 关闭数据修复与均衡逻辑

   因为升级后，BE的默认Tag为 `"tag.location": "default"`，而表的默认副本分布为：`"tag.location.default:xx`。所以如果直接修改 BE 的 Tag，系统会自动检测到副本分布的变化，从而开始数据重分布。这可能会占用部分系统资源。所以我们可以在修改 Tag 前，先关闭数据修复与均衡逻辑，以保证我们在规划资源时，不会有副本重分布的操作。

   ```sql
   ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");
   ADMIN SET FRONTEND CONFIG ("disable_tablet_scheduler" = "true");
   ```

2. 设置 Tag 和表副本分布

   接下来可以通过 `alter system modify backend` 语句进行 BE 的 Tag 设置。以及通过 `alter table` 语句修改表的副本分布策略。示例如下：

   ```sql
   alter system modify backend "host1:9050, 1212:9050" set ("tag.location" = "group_a");
   alter table my_table modify partition p1 set ("replication_allocation" = "tag.location.group_a:2");
   ```

3. 开启数据修复与均衡逻辑

   在 Tag 和副本分布都设置完毕后，我们可以开启数据修复与均衡逻辑来触发数据的重分布了。

   ```sql
   ADMIN SET FRONTEND CONFIG ("disable_balance" = "false");
   ADMIN SET FRONTEND CONFIG ("disable_tablet_scheduler" = "false");
   ```

   该过程根据涉及到的数据量会持续一段时间。并且会导致部分 colocation table 无法进行 colocation 规划（因为副本在迁移中）。可以通过 `show proc "/cluster_balance/"` 来查看进度。也可以通过 `show proc "/statistic"` 中 `UnhealthyTabletNum` 的数量来判断进度。当 `UnhealthyTabletNum` 降为 0 时，则代表数据重分布完毕。

4. 设置用户的资源标签权限。

   等数据重分布完毕后。我们就可以开始设置用户的资源标签权限了。因为默认情况下，用户的 `resource_tags.location` 属性为空，即可以访问任意 Tag 的 BE。所以在前面步骤中，不会影响到已有用户的正常查询。当 `resource_tags.location` 属性非空时，用户将被限制访问指定 Tag 的 BE。

通过以上4步，我们可以较为平滑的在原有集群升级后，使用资源划分功能。

### table数量很多时如何方便的设置副本分布策略

   比如有一个 db1,db1下有四个table，table1需要的副本分布策略为 `group_a:1,group_b:2`，table2，table3, table4需要的副本分布策略为 `group_c:1,group_b:2`

   那么可以使用如下语句创建db1：

  ```sql
   CREATE DATABASE db1 PROPERTIES (
   "replication_allocation" = "tag.location.group_a:1, tag.location.group_b:2"
   )
   ```
   
   使用如下语句创建table1：
   
   ```sql
   CREATE TABLE table1
   (k1 int, k2 int)
   distributed by hash(k1) buckets 1
   properties(
   "replication_allocation"="tag.location.group_c:1, tag.location.group_b:2"
   )
   ```

   table2，table3,table4的建表语句无需再指定`replication_allocation`。
   
   注意事项：更改database的副本分布策略不会对已有的table产生影响。