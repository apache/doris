---
{
    "title": "Release 2.0.0",
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

亲爱的社区小伙伴们，我们很高兴地向大家宣布，Apache Doris 2.0.0 Release 版本已于 2023 年 8 月 11 日正式发布，有超过 275 位贡献者为 Apache Doris 提交了超过 4100 个优化与修复。

在 2.0.0 版本中，Apache Doris 在标准 Benchmark 数据集上盲测查询性能得到超过 10 倍的提升、在日志分析和数据湖联邦分析场景能力得到全面加强、数据更新效率和写入效率都更加高效稳定、支持了更加完善的多租户和资源隔离机制、在资源弹性与存算分离方向踏上了新的台阶、增加了一系列面向企业用户的易用性特性。在经过近半年的开发、测试与稳定性调优后，这一版本已经正式稳定可用，欢迎大家下载使用！

> 下载链接：[https://doris.apache.org/download](https://doris.apache.org/download)
> 
> GitHub 源码：[https://github.com/apache/doris/tree/2.0.0-rc04](https://github.com/apache/doris/tree/2.0.0-rc04)
  

# 盲测性能 10 倍以上提升！

在 Apache Doris 2.0.0 版本中，我们引入了全新查询优化器和自适应的并行执行模型，结合存储层、执行层以及执行算子上的一系列性能优化手段，实现了盲测性能 10 倍以上的提升。以 SSB-Flat 和 TPC-H 标准测试数据集为例，在相同的集群和机器配置下，新版本宽表场景盲测较之前版本性能提升 10 倍、多表关联场景盲测提升了 13 倍，实现了巨大的性能飞跃。

### 更智能的全新查询优化器

全新查询优化器采取了更先进的 Cascades 框架、使用了更丰富的统计信息、实现了更智能化的自适应调优，在绝大多数场景无需任何调优和 SQL 改写即可实现极致的查询性能，同时对复杂 SQL 支持得更加完备、可完整支持 TPC-DS 全部 99 个 SQL。通过全新查询优化器，我们可以胜任更多真实业务场景的挑战，减少因人工调优带来的人力消耗，真正助力业务提效。

以 TPC-H 为例，全新优化器在未进行任何手工调优和 SQL 改写的情况下，绝大多数 SQL 仍领先于旧优化器手工调优后的性能表现！而在超过百家 2.0 版本提前体验用户的真实业务场景中，绝大多数原始 SQL 执行效率得以极大提升！

参考文档：[https://doris.apache.org/zh-CN/docs/dev/query-acceleration/nereids](https://doris.apache.org/zh-CN/docs/dev/query-acceleration/nereids)

如何开启：`SET enable_nereids_planner=true` 在 Apache Doris 2.0-beta 版本中全新查询优化器已经默认开启

### 倒排索引支持

在 2.0.0 版本中我们对现有的索引结构进行了丰富，引入了倒排索引来应对多维度快速检索的需求，在关键字模糊查询、等值查询和范围查询等场景中均取得了显著的查询性能和并发能力提升。

在此以某头部手机厂商的用户行为分析场景为例，在之前的版本中，随着并发量的上升、查询耗时逐步提升，性能下降趋势比较明显。而在 2.0.0 版本开启倒排索引后，随着并发量的提升查询性能始终保持在毫秒级。在同等查询并发量的情况下，2.0.0 版本在该用户行为分析场景中并发查询性能提升了 5-90 倍！


### 点查询并发能力提升 20 倍

在银行交易流水单号查询、保险代理人保单查询、电商历史订单查询、快递运单号查询等 Data Serving 场景，会面临大量一线业务人员及 C 端用户基于主键 ID 检索整行数据的需求，同时在用户画像、实时风控等场景中还会面对机器大规模的程序化查询，在过去此类需求往往需要引入 Apache HBase 等 KV 系统来应对点查询、Redis 作为缓存层来分担高并发带来的系统压力。
对于基于列式存储引擎构建的 Apache Doris 而言，此类的点查询在数百列宽表上将会放大随机读取 IO，并且查询优化器和执行引擎对于此类简单 SQL 的解析、分发也将带来不必要的额外开销，负责 SQL 解析的 FE 模块往往会成为限制并发的瓶颈，因此需要更高效简洁的执行方式。

在 Apache Doris 2.0.0 版本，我们引入了全新的行列混合存储以及行级 Cache，使得单次读取整行数据时效率更高、大大减少磁盘访问次数，同时引入了点查询短路径优化、跳过执行引擎并直接使用快速高效的读路径来检索所需的数据，并引入了预处理语句复用执行 SQL 解析来减少 FE 开销。

通过以上一系列优化，Apache Doris 2.0.0 版本在并发能力上实现了数量级的提升，实现了单节点 30000 QPS 的并发表现，较过去版本点查询并发能力提升超 20 倍！

基于以上能力，Apache Doris 可以更好应对高并发数据服务场景的需求，替代 HBase 在此类场景中的能力，减少复杂技术栈带来的维护成本以及数据的冗余存储。

### 自适应的并行执行模型

在实现极速分析体验的同时，为了保证多个混合分析负载的执行效率以及查询的稳定性，在 2.0.0 版本中我们引入了 Pipeline 执行模型作为查询执行引擎。在 Pipeline 执行引擎中，查询的执行是由数据来驱动控制流变化的，各个查询执行过程之中的阻塞算子被拆分成不同 Pipeline，各个 Pipeline 能否获取执行线程调度执行取决于前置数据是否就绪，实现了阻塞操作的异步化、可以更加灵活地管理系统资源，同时减少了线程频繁创建和销毁带来的开销，并提升了 Apache Doris 对于 CPU 的利用效率。因此 Apache Doris 在混合负载场景中的查询性能和稳定性都得到了全面提升。

参考文档：[https://doris.apache.org/zh-CN/docs/dev/query-acceleration/pipeline-execution-engine](https://doris.apache.org/zh-CN/docs/dev/query-acceleration/pipeline-execution-engine)

如何开启：` Set enable_pipeline_engine = true  `
- 该功能在 Apache Doris 2.0 版本中将默认开启，BE 在进行查询执行时默认将 SQL 的执行模型转变 Pipeline 的执行方式。
- `parallel_pipeline_task_num`代表了 SQL 查询进行查询并发的 Pipeline Task 数目。Apache Doris 默认配置为`0`，此时 Apache Doris 会自动感知每个 BE 的 CPU 核数并把并发度设置为 CPU 核数的一半，用户也可以实际根据自己的实际情况进行调整。
- 对于从老版本升级的用户，系统自动将该参数设置成老版本中`parallel_fragment_exec_instance_num`的值。

# 更统一多样的分析场景

作为最初诞生于报表分析场景的 OLAP 系统，Apache Doris 在这一擅长领域中做到了极致，凭借自身优异的分析性能和极简的使用体验收获到了众多用户的认可，在诸如实时看板（Dashboard）、实时大屏、业务报表、管理驾驶舱等实时报表场景以及自助 BI 平台、用户行为分析等即席查询场景获得了极为广泛的运用。

而随着用户规模的极速扩张，越来越多用户开始希望通过 Apache Doris 来简化现有的繁重大数据技术栈，减少多套系统带来的使用及运维成本。因此 Apache Doris 也在不断拓展应用场景的边界，从过去的实时报表和 Ad-hoc 等典型 OLAP 场景到湖仓一体、ELT/ETL、日志检索与分析、高并发 Data Serving 等更多业务场景，而日志检索分析、湖仓一体也是我们在 Apache Doris 最新版本中的重要突破。

### 10倍以上性价比的日志检索分析平台

在 Apache Doris 2.0.0 版本中，我们提供了原生的半结构化数据支持，在已有的 JSON、Array 基础之上增加了复杂类型 Map，并基于 Light Schema Change 功能实现了 Schema Evolution。与此同时，2.0.0 版本新引入的倒排索引和高性能文本分析算法全面加强了 Apache Doris 在日志检索分析场景的能力，可以支持更高效的任意维度分析和全文检索。结合过去在大规模数据写入和低成本存储等方面的优势，相对于业内常见的日志分析解决方案，基于 Apache Doris 构建的新一代日志检索分析平台实现了 10 倍以上的性价比提升。

### 湖仓一体

在 Apache Doris 1.2 版本中，我们引入了 Multi-Catalog 功能，支持了多种异构数据源的元数据自动映射与同步，实现了便捷的元数据和数据打通。在 2.0.0 版本中，我们进一步对数据联邦分析能力进行了加强，引入了更多数据源，并针对用户的实际生产环境做了诸多性能优化，在真实工作负载情况下查询性能得到大幅提升。

在数据源方面，Apache Doris 2.0.0 版本支持了 Hudi Copy-on-Write 表的 Snapshot Query 以及 Merge-on-Read 表的 Read Optimized Query，截止目前已经支持了 Hive、Hudi、Iceberg、Paimon、MaxCompute、Elasticsearch、Trino、ClickHouse 等数十种数据源，几乎支持了所有开放湖仓格式和 Metastore。同时还支持通过 Apache Range 对 Hive Catalog 进行鉴权，可以无缝对接用户现有的权限系统。同时还支持可扩展的鉴权插件，为任意 Catalog 实现自定义的鉴权方式。

在性能方面，利用 Apache Doris 自身高效的分布式执行框架、向量化执行引擎以及查询优化器，结合 2.0 版本中对于小文件和宽表的读取优化、本地文件 Cache、ORC/Parquet 文件读取效率优化、弹性计算节点以及外表的统计信息收集，Apaceh Doris 在 TPC-H 场景下查询 Hive 外部表相较于 Presto/Trino 性能提升 3-5 倍。

通过这一系列优化，Apache Doris 湖仓一体的能力得到极大拓展，在如下场景可以更好发挥其优异的分析能力：

- 湖仓查询加速：为数据湖、Elasticsearch 以及各类关系型数据库提供优秀的查询加速能力，相比 Hive、Presto、Spark 等查询引擎实现数倍的性能提升。

- 数据导入与集成：基于可扩展的连接框架，增强 Apache Doris 在数据集成方面的能力，让数据更便捷的被消费和处理。用户可以通过 Apache Doris 对上游的多种数据源进行统一的增量、全量同步，并利用 Apache Doris 的数据处理能力对数据进行加工和展示，也可以将加工后的数据写回到数据源，或提供给下游系统进行消费。

- 统一数据分析网关：利用 Apache Doris 构建完善可扩展的数据源连接框架，便于快速接入多类数据源。提供基于各种异构数据源的快速查询和写入能力，将 Apache Doris 打造成统一的数据分析网关。

# 高效的数据更新

在实时分析场景中，数据更新是非常普遍的需求。用户不仅希望能够实时查询最新数据，也希望能够对数据进行灵活的实时更新。典型场景如电商订单分析、物流运单分析、用户画像等，需要支持数据更新类型包括整行更新、部分列更新、按条件进行批量更新或删除以及整表或者整个分区的重写（inser overwrite）。

高效的数据更新一直是大数据分析领域的痛点，离线数据仓库 hive 通常只支持分区级别的数据更新，而 Hudi 和 Iceberg 等数据湖，虽然支持 Record 级别更新，但是通常采用 Merge-on-Read 或 Copy-on-Write 的方式，仅适合低频批量更新而不适合实时高频更新。

在 Apache Doris 1.2 版本，我们在 Unique Key 主键模型实现了 Merge-on-Write 的数据更新模式，数据在写入阶段就能完成所有的数据合并工作，因此查询性能得到 5-10 倍的提升。在 Apache Doris 2.0 版本我们进一步加强了数据更新能力，主要包括：

- 对写入性能进行了大幅优化，高并发写入和混合负载写入场景的稳定性也显著提升。例如在单 Tablet 7GB 的重复导入测试中，数据导入的耗时从约 30 分钟缩短到了 90s，写入效率提升 20 倍；以某头部支付产品的场景压测为例，在 20 个并行写入任务下可以达到 30 万条每秒的写入吞吐，并且持续写入十几个小时后仍然表现非常稳定。

- 支持部分列更新功能。在 2.0.0 版本之前 Apache Doris 仅支持通过 Aggregate Key 聚合模型的 Replace_if_not_null 进行部分列更新，在 2.0.0 版本中我们增加了 Unique Key 主键模型的部分列更新，在多张上游源表同时写入一张宽表时，无需由 Flink 进行多流 Join 打宽，直接写入宽表即可，减少了计算资源的消耗并大幅降低了数据处理链路的复杂性。同时在面对画像场景的实时标签列更新、订单场景的状态更新时，直接更新指定的列即可，较过去更为便捷。

- 支持复杂条件更新和条件删除。在 2.0.0 版本之前 Unique Key 主键模型仅支持简单 Update 和 Delete 操作，在 2.0.0 版本中我们基于 Merge-on-Write 实现了复杂条件的数据更新和删除，并且执行效率更加高效。基于以上优化，Apache Doris 对于各类数据更新需求都有完备的能力支持！

# 更加高效稳定的数据写入

### 导入性能进一步提升

聚焦于实时分析，我们在过去的几个版本中在不断增强实时分析能力，其中端到端的数据实时写入能力是优化的重要方向，在 Apache Doris 2.0 版本中，我们进一步强化了这一能力。通过 Memtable 不使用 Skiplist、并行下刷、单副本导入等优化，使得导入性能有了大幅提升：

- 使用 Stream Load 对 TPC-H 144G lineitem 表原始数据进行三副本导入 48 buckets Duplicate 表，吞吐量提升 100%。
- 使用 Stream Load 对 TPC-H 144G lineitem 表原始数据进行三副本导入 48 buckets Unique Key 表，吞吐量提升 200%。
- 使用 insert into select 对 TPC-H 144G lineitem 表进行导入 48 buckets Duplicate 表，吞吐量提升 50%。
- 使用 insert into select 对 TPC-H 144G lineitem 表进行导入 48 buckets Unique Key 表，吞吐提升 150%。


### 数据高频写入更稳定

在高频数据写入过程中，小文件合并和写放大问题以及随之而来的磁盘 I/O和 CPU 资源开销是制约系统稳定性的关键，因此在 2.0 版本中我们引入了 Vertical Compaction 以及 Segment Compaction，用以彻底解决 Compaction 内存问题以及写入过程中的 Segment 文件过多问题，资源消耗降低 90%，速度提升 50%，内存占用仅为原先的 10%。


### 数据表结构自动同步

在过去版本中我们引入了毫秒级别的 Schema Change，而在最新版本 Flink-Doris-Connector 中，我们实现了从 MySQL 等关系型数据库到 Apache Doris 的一键整库同步。在实际测试中单个同步任务可以承载数千张表的实时并行写入，从此彻底告别过去繁琐复杂的同步流程，通过简单命令即可实现上游业务数据库的表结构及数据同步。同时当上游数据结构发生变更时，也可以自动捕获 Schema 变更并将 DDL 动态同步到 Doris 中，保证业务的无缝运行。

# 更加完善的多租户资源隔离

多租户与资源隔离的主要目的是为了保证高负载时避免相互发生资源抢占，Apache Doris 在过去版本中推出了资源组（Resource Group）的硬隔离方案，通过对同一个集群内部的 BE 打上标签，标签相同的 BE 会组成一个资源组。数据入库时会按照资源组配置将数据副本写入到不同的资源组中，查询时按照资源组的划分使用对应资源组上的计算资源进行计算，例如将读、写流量放在不同的副本上从而实现读写分离，或者将在线与离线业务划分在不同的资源组、避免在离线分析任务之间的资源抢占。

资源组这一硬隔离方案可以有效避免多业务间的资源抢占，但在实际业务场景中可能会存在某些资源组紧张而某些资源组空闲的情况发生，这时需要有更加灵活的方式进行空闲资源的共享，以降低资源空置率。因此在 2.0.0 版本中我们增加了 Workload Group 资源软限制的方案，通过对 Workload 进行分组管理，以保证内存和 CPU 资源的灵活调配和管控。

通过将 Query 与 Workload Group 相关联，可以限制单个 Query 在 BE 节点上的 CPU 和内存资源的百分比，并可以配置开启资源组的内存软限制。当集群资源紧张时，将自动 Kill 组内占用内存最大的若干个查询任务以减缓集群压力。当集群资源空闲时，一旦 Workload Group 使用资源超过预设值时，多个 Workload 将共享集群可用空闲资源并自动突破阈值，继续使用系统内存以保证查询任务的稳定执行。Workload Group 还支持设置优先级，通过预先设置的优先级进行资源分配管理，来确定哪些任务可正常获得资源，哪些任务只能获取少量或没有资源。

与此同时，在 Workload Group 中我们还引入了查询排队的功能，在创建 Workload Group 时可以设置最大查询数，超出最大并发的查询将会进行队列中等待执行，以此来缓解高负载下系统的压力。

# 极致弹性与存算分离

过去 Apache Doris 凭借在易用性方面的诸多设计帮助用户大幅节约了计算与存储资源成本，而面向未来的云原生架构，我们已经走出了坚实的一步。

从降本增效的趋势出发，用户对于计算和存储资源的需求可以概括为以下几方面：

- 计算资源弹性：面对业务计算高峰时可以快速进行资源扩展提升效率，在计算低谷时可以快速缩容以降低成本；

- 存储成本更低：面对海量数据可以引入更为廉价的存储介质以降低成本，同时存储与计算单独设置、相互不干预；

- 业务负载隔离：不同的业务负载可以使用独立的计算资源，避免相互资源抢占；

- 数据管控统一：统一 Catalog、统一管理数据，可以更加便捷地分析数据。

存算一体的架构在弹性需求不强的场景具有简单和易于维护的优势，但是在弹性需求较强的场景有一定的局限。而存算分离的架构本质是解决资源弹性的技术手段，在资源弹性方面有着更为明显的优势，但对于存储具有更高的稳定性要求，而存储的稳定性又会进一步影响到 OLAP 的稳定性以及业务的存续性，因此也引入了 Cache 管理、计算资源管理、垃圾数据回收等一系列机制。

而在与 Apache Doris 社区广大用户的交流中，我们发现用户对于存算分离的需求可以分为以下三类：

- 目前选择简单易用的存算一体架构，暂时没有资源弹性的需求；

- 欠缺稳定的大规模存储，要求在 Apache Doris 原有基础上提供弹性、负载隔离以及低成本；

- 有稳定的大规模存储，要求极致弹性架构、解决资源快速伸缩的问题，因此也需要更为彻底的存算分离架构；

为了满足前两类用户的需求，Apache Doris 2.0 版本中提供了可以兼容升级的存算分离方案：
第一种，计算节点。2.0 版本中我们引入了无状态的计算节点 Compute Node，专门用于数据湖分析。相对于原本存储计算一体的混合节点，Compute Node 不保存任何数据，在集群扩缩容时无需进行数据分片的负载均衡，因此在数据湖分析这种具有明显高峰的场景中可以灵活扩容、快速加入集群分摊计算压力。同时由于用户数据往往存储在 HDFS/S3 等远端存储中，执行查询时查询任务会优先调度到 Compute Node 执行，以避免内表与外表查询之间的计算资源抢占。

第二种，冷热数据分层。在存储方面，冷热数据往往面临不同频次的查询和响应速度要求，因此通常可以将冷数据存储在成本更低的存储介质中。在过去版本中 Apache Doris 支持对表分区进行生命周期管理，通过后台任务将热数据从 SSD 自动冷却到 HDD，但 HDD 上的数据是以多副本的方式存储的，并没有做到最大程度的成本节约，因此对于冷数据存储成本仍然有较大的优化空间。在 Apache Doris 2.0 版本中推出了冷热数据分层功能，冷热数据分层功能使 Apache Doris 可以将冷数据下沉到存储成本更加低廉的对象存储中，同时冷数据在对象存储上的保存方式也从多副本变为单副本，存储成本进一步降至原先的三分之一，同时也减少了因存储附加的计算资源成本和网络开销成本。通过实际测算，存储成本最高可以降低超过 70%！

面对更加彻底的存储计算分离需求，飞轮科技（SelectDB）技术团队设计并实现了全新的云原生存算分离架构（SelectDB Cloud），近一年来经历了大量企业客户的大规模使用，在性能、功能成熟度、系统稳定性等方面经受了真实生产环境的考验。在 Apache Doris 2.0.0 版本发布之际，飞轮科技宣布将这一经过大规模打磨后的成熟架构贡献至 Apache Doris 社区。这一工作预计将于 2023 年 10 月前后完成，届时全部存算分离的代码都将会提交到 Apache Doris 社区主干分支中，预计在 9 月广大社区用户就可以提前体验到基于存算分离架构的预览版本。

# 易用性进一步提升

除了以上功能需求外，在 Apache Doris 还增加了许多面向企业级特性的体验改进：

### 支持 Kubernetes 容器化部署

在过去 Apache Doris 是基于 IP 通信的，在 K8s 环境部署时由于宿主机故障发生 Pod IP 漂移将导致集群不可用，在 2.0 版本中我们支持了 FQDN，使得 Apache Doris 可以在无需人工干预的情况下实现节点自愈，因此可以更好应对 K8s 环境部署以及灵活扩缩容。

### 跨集群数据复制

在 Apache Doris 2.0.0 版本中，我们可以通过 CCR 的功能在库/表级别将源集群的数据变更同步到目标集群，可根据场景精细控制同步范围；用户也可以根据需求灵活选择全量或者增量同步，有效提升了数据同步的灵活性和效率；此外 Dors CCR 还支持 DDL 同步，源集群执行的 DDL 语句可以自动同步到目标集群，从而保证了数据的一致性。Doris CCR 配置和使用也非常简单，简单操作即可快速完成跨集群数据复制。基于 Doris CCR 优异的能力，可以更好实现读写负载分离以及多机房备份，并可以更好支持不同场景的跨集群复制需求。

# 其他升级注意事项

- 1.2-lts 需要停机升级到 2.0.0，2.0-alpha 需要停机升级到 2.0.0
- 查询优化器开关默认开启 `enable_nereids_planner=true`；
- 系统中移除了非向量化代码，所以 `enable_vectorized_engine` 参数将不再生效；
- 新增参数 `enable_single_replica_compaction`；
- 默认使用 datev2, datetimev2, decimalv3 来创建表，不支持 datev1，datetimev1， decimalv2 创建表；
- 在 JDBC 和 Iceberg Catalog 中默认使用decimalv3；
- date type 新增 AGG_STATE；
- backend 表去掉 cluster 列；
- 为了与 BI 工具更好兼容，在 show create table 时，将 datev2 和 datetimev2 显示为 date 和 datetime。
- 在 BE 启动脚本中增加了 max_openfiles 和 swap 的检查，所以如果系统配置不合理，be 有可能会启动失败；
- 禁止在 localhost 访问 FE 时无密码登录；
- 当系统中存在 Multi-Catalog 时，查询 information schema 的数据默认只显示 internal catalog 的数据；
- 限制了表达式树的深度，默认为 200；
- array string 返回值 单引号变双引号；
- 对 Doris的进程名重命名为 DorisFE 和 DorisBE；

# 正式踏上 2.0 之旅

在 Apache Doris 2.0.0 版本发布过程中，我们邀请了数百家企业参与新版本的打磨，力求为所有用户提供性能更佳、稳定性更高、易用性更好的数据分析体验。后续我们将会持续敏捷发版来响应所有用户对功能和稳定性的更高追求，预计 2.0 系列的第一个迭代版本 2.0.1 将于 8 月下旬发布，9 月会进一步发布 2.0.2 版本。在快速 Bugfix 的同时，也会不断将一些最新特性加入到新版本中。9 月份我们还将发布 2.1 版本的尝鲜版本，会增加一系列呼声已久的新能力，包括 Variant 可变数据类型以更好满足半结构化数据 Schema Free 的分析需求，多表物化视图，在导入性能方面持续优化、增加新的更加简洁的数据导入方式，通过自动攒批实现更加实时的数据写入，复合数据类型的嵌套能力等。 

期待 Apache Doris 2.0 版本的正式发布为更多社区用户提供实时统一的分析体验，我们也相信 Apache Doris 2.0 版本会成为您在实时分析场景中的最理想选择。

# 致谢

再次向所有参与 Apache Doris 2.0.0 版本开发和测试的贡献者们表示最衷心的感谢，他们分别是：

0xflotus、1330571、15767714253、924060929、ArmandoZ、AshinGau、BBB-source、BePPPower、Bears0haunt、BiteTheDDDDt、ByteYue、Cai-Yao、CalvinKirs、Centurybbx、ChaseHuangxu、CodeCooker17、DarvenDua、Dazhuwei、DongLiang-0、EvanTheBoy、FreeOnePlus、Gabriel39、GoGoWen、HHoflittlefish777、HackToday、HappenLee、Henry2SS、HonestManXin、JNSimba、JackDrogon、Jake-00、Jenson97Jibing-Li、Johnnyssc、JoverZhang、KassieZ、Kikyou1997、Larborator、Lchangliang、LemonLiTree、LiBinfeng-01、MRYOG、Mellorsssss、Moonm3n、Mryange、Myasuka、NetShrimp06、Reminiscent、SWJTU-ZhangLei、SaintBacchus、ShaoStaticTiger、Shoothzj、SilasKenneth、TangSiyang2001、Tanya-W、TeslaCN、TsukiokaKogane、UnicornLee、WinkerDu、WuWQ98、Xiaoccer、XieJiann、Yanko-7、Yukang-Lian、Yulei-Yang、ZI-MA、ZashJie、ZhangYu0123、Zhiyu-h、adonis0147、airborne12、alissa-tung、amorynan、beijita、bigben0204、bin41215、bingquanzhao、bobhan1、bowenliang123、brody715、caiconghui、cambyzju、caoliang-web、catpineapple、chenlinzhong、cjq9458、cnissnzg、colagy、csun5285、czzmmc、dataroaring、davidshtian、deadlinefen、deardeng、didiaode18、dong-shuai、dujl、dutyu、echo-hhj、eldenmoon、englefly、figurant、fornaix、fracly、freemandealer、fsilent、fuchanghai、gavinchou、git-hulk、gitccl、gnehil、guoxiaolongzte、gwxog、hailin0、hanyisong、haochengxia、haohuaijin、hechao-ustc、hello-stephen、herry2038、hey-hoho、hf200012、hqx871、httpshirley、htyoung、hubgeter、hufengkai、hust-hhb、isHuangXin、ixzc、jacktengg、jackwener、jeffreys-cat、jiugem、jixxiong、kaijchen、kaka11chen、levy5307、lexluo09、liangjiawei1110、liaoxin01、liugddx、liujinhui1994、liujiwen-up、liutang123、liuxinzero07、liwei9902、lljqy、lsy3993、luozenglin、luwei16、luzhijing、lvshaokang、maochongxin、meredith620、mklzl、mongo360、morningman、morrySnow、mrhhsg、myfjdthink、mymeiyi、nanfeng1999、neuyilan、nextdreamblue、niebayes、nikam14、pengxiangyu、pingchunzhang、platoneko、q763562998、qidaye、qzsee、reswqa、sepastian、shenxingwuying、shuke987、shysnow、siriume、sjyago、skyhitnow、smallhibiscus、sohardforaname、spaces-X、stalary、starocean999、superspeedone、taomengen、tarepanda1024、timyuer、ucasfl、vinlee19、wangbo、wanghuan2054、wangshuo128、wangtianyi2004、wangyf0555、wangyujia2023、web-flow、weizhengte、weizuo93、whutpencil、wsjz、wuwenchi、wzymumon、xiaojunjie、xiaokang、xiedeyantu、xinyiZzz、xuqinghuang、xutaoustc、xy720、xzj7019、ya-dao、yagagagaga、yangzhg、yiguolei、yimeng、yinzhijian、yixiutt、yongjinhou、youtNa、yuanyuan8983、yujian225、yujun777、yuxuan-luo、yz-jayhua、zbtzbtzbt、zclllyybb、zddr、zenoyang、zgxme、zhangguoqiang666、zhangstar333、zhangy5、zhannngchen、zhbinbin、zhengshengjun、zhengshiJ、zwuis、zxealous、zy-kkk、zzzxl1993、zzzzzzzs