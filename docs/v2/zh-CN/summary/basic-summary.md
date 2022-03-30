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

- FE：Frontend，即 Doris 的前端节点。主要负责接收和返回客户端请求、元数据以及集群管理、查询计划生成等工作。
- BE：Backend，即 Doris 的后端节点。主要负责数据存储与管理、查询计划执行等工作。
- Broker：Broker 是一个无状态的进程。其主要是帮助Doris以类Unix文件系统接口的方式，访问外部数据源例如：HDFS上的数据。比如应用于数据导入或者数据导出操作中
- Tablet：Tablet是一张表实际的物理存储单元，一张表按照分区和分桶后在BE构成分布式存储层中以Tablet为单位进行存储，每个Tablet包括元信息及若干个连续的RowSet。
- Rowset：Rowset是Tablet中一次数据变更的数据集合，数据变更包括了数据导入、删除、更新等。Rowset按版本信息进行记录。每次变更会生成一个版本。
- Version：由Start、End两个属性构成，维护数据变更的记录信息。通常用来表示Rowset的版本范围，在一次新导入后生成一个Start，End相等的Rowset，在Compaction后生成一个带范围的Rowset版本。
- Segment：表示Rowset中的数据分段。多个Segment构成一个Rowset。
- Compaction：连续版本的Rowset合并的过程成称为Compaction，合并过程中会对数据进行压缩操作。