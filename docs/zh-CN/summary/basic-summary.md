---
{
    "title": "Doris 基本概念",
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

# Doris 基本概念

Apache Doris 是一个基于MPP的现代化、高性能、支持实时的分析型数据库，以极速易用的特性被业内所熟知。仅需亚秒级响应时间即可返回海量数据下的查询结果，不仅可以支持高并发的店查询场景，也能支持高吞吐的复杂查询场景。

Apache Doris的分布式架构非常简洁，易于运维，并且可以支持10PB以上的超大数据集。

Apache Doris可以满足多种数据分析需求，例如固定历史报表，实时数据分析，交互式数据分析和探索式数据分析等。令您的数据分析工作更加简单高效！

基于Apache Doris 用户可以灵活构建包括宽表、星型模型。雪花模型在内的各类数据模型。

Apache Doris 高度兼容 MySQL 协议，支持标准 SQL 语法，用户可以很容易的使用 Apache Doris 和其他系统进行对接，整个系统无外部依赖、提供完整高可用、并且易于运维管理。

以下是 Doris 系统的几个核心概念：

## FE（Frontend）

即 Doris 的前端节点。主要负责接收和返回客户端请求、元数据以及集群管理、查询计划生成等工作。

FE 主要有有三个角色，一个是 leader，一个是 follower，还有一个 observer。leader 跟 follower，主要是用来达到元数据的高可用，保证单节点宕机的情况下，元数据能够实时地在线恢复，而不影响整个服务。

Observer 只是用来扩展查询节点，就是说如果在发现集群压力非常大的情况下，需要去扩展整个查询的能力，那么可以加 observer 的节点。observer 不参与任何的写入，只参与读取。

**主要作用如下：**

- 管理元数据, 执行SQL DDL命令, 用Catalog记录库, 表, 分区, tablet副本等信息。
- FE高可用部署, 使用复制协议选主和主从同步元数据, 所有的元数据修改操作, 由FE leader节点完成, FE follower节点可执行读操作。 元数据的读写满足顺序一致性。 FE的节点数目采用2n+1, 可容忍n个节点故障。 当FE leader故障时, 从现有的follower节点重新选主, 完成故障切换。
- FE的SQL layer对用户提交的SQL进行解析, 分析, 改写, 语义分析和关系代数优化, 生产逻辑执行计划。
- FE的Planner负载把逻辑计划转化为可分布式执行的物理计划, 分发给一组BE。
- FE监督BE, 管理BE的上下线, 根据BE的存活和健康状态, 维持tablet副本的数量。
- FE协调数据导入, 保证数据导入的一致性。

## BE（Backend）

Doris 的后端节点。主要负责数据存储与管理、查询计划执行等工作。

Doris 数据主要都是存储在 BE 里面，BE 节点上物理数据的可靠性通过多副本来实现，默认是 3 副本，副本数可配置且可随时动态调整,满足不同可用性级别的业务需求。FE 调度 BE 上副本的分布与补齐。

主要作用如下：

- BE管理tablet副本, tablet是table经过分区分桶形成的子表, 采用列式存储。
- BE受FE指导, 创建或删除子表。
- BE接收FE分发的物理执行计划并指定BE coordinator节点, 在BE coordinator的调度下, 与其他BE worker共同协作完成执行。
- BE读本地的列存储引擎, 获取数据, 通过索引和谓词下沉快速过滤数据。
- BE后台执行compact任务, 减少查询时的读放大。
- 数据导入时, 由FE指定BE coordinator, 将数据写入到tablet多副本所在的BE上。

## Doris 元数据

Doris 采用 Paxos 协议以及 Memory + Checkpoint + Journal 的机制来确保元数据的高性能及高可靠。

- 元数据的每次更新，都首先写入到磁盘的日志文件中，然后再写到内存中，最后定期 checkpoint 到本地磁盘上。
- 相当于是一个纯内存的一个结构，也就是说所有的元数据都会缓存在内存之中，从而保证 FE 在宕机后能够快速恢复元数据，而且不丢失元数据。
- Leader、follower 和 observer 它们三个构成一个可靠的服务。
- 如果发生节点宕机的情况下我们要保持服务可用，我们一般是部署一个 leader 两个 follower，就是说三个节点去达到一个高可用服务。单机的节点故障的时候其实基本上三个就够了，因为 FE 节点毕竟它只存了一份元数据，它的压力不大，所以如果 FE 太多的时候它会去消耗机器资源，所以多数情况下三个就足够了，可以达到一个很高可用的元数据服务。

## Broker（可选组件）

Broker 为一个独立的无状态进程。封装了文件系统接口，提供 Doris 读取远端存储系统中文件的能力，包括HDFS，S3，BOS等

## Tablet

Tablet是一张表实际的物理存储单元，一张表按照分区和分桶后在BE构成分布式存储层中以Tablet为单位进行存储，每个Tablet包括元信息及若干个连续的RowSet。

## Rowset

Rowset是Tablet中一次数据变更的数据集合，数据变更包括了数据导入、删除、更新等。Rowset按版本信息进行记录。每次变更会生成一个版本。

### Version

由Start、End两个属性构成，维护数据变更的记录信息。通常用来表示Rowset的版本范围，在一次新导入后生成一个Start，End相等的Rowset，在Compaction后生成一个带范围的Rowset版本。

## Segment

表示Rowset中的数据分段。多个Segment构成一个Rowset。

### Compaction

连续版本的Rowset合并的过程成称为Compaction，合并过程中会对数据进行压缩操作。