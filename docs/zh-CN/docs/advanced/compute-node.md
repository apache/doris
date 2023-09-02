---
{
    "title": "计算节点",
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

# 计算节点

<version since="1.2.1">
</version>

## 需求场景
目前Doris是一个典型Share-Nothing的架构, 通过绑定数据和计算资源在同一个节点获得非常好的性能表现.
但随着Doris计算引擎性能持续提高, 越来越多的用户也开始选择使用Doris直接查询数据湖数据.
这类场景是一种Share-Disk场景, 数据往往存储在远端的HDFS/S3上, 计算在Doris中, Doris通过网络获取数据, 然后在内存完成计算.
而如果这两个负载都混合在同一个集群时, 对于目前Doris的架构就会出现以下不足:
1. 资源隔离差, 两个负载对集群的响应要求不一, 混合部署会有相互的影响.
2. 集群扩容时, 数据湖查询只需要扩容计算资源, 而目前只能存储计算一起扩容, 导致磁盘使用率变低.
3. 扩容效率差, 扩容后会启动Tablet数据的迁移, 整体过程比较漫长. 而数据湖查询有着明显的高峰低谷, 需要小时级弹性能力.

## 解决方案
实现一种专门用于联邦计算的BE节点角色: `计算节点`, 计算节点专门处理数据湖这类远程的联邦查询.
原来的BE节点类型称为`混合节点`, 这类节点既能做SQL查询, 又有Tablet数据存储管理.
而`计算节点`只能做SQL查询, 它不会保存任何数据.

有了计算节点后, 集群部署拓扑也会发生变化: 混合节点用于OLAP类型表的数据计算, 这个节点根据存储的需求而扩容, 而计算节点用于联邦查询, 该节点类型随着计算负载而扩容.

此外, 计算节点由于没有存储, 因此在部署时, 计算节点可以混部在HDD磁盘机器或者部署在容器之中.

## Compute Node的使用

### 配置
在BE的配置文件be.conf中添加配置项:
```
be_node_role=computation
```

该配置项默认为`mix`, 即原来的BE节点类型, 设置为`computation`后, 该节点为计算节点.

可以通过`show backends\G`命令看到其中`NodeRole`字段的值, 如果是`mix`, 则为混合节点, 如果是`computation`, 则为计算节点

```sql
*************************** 1. row ***************************
              BackendId: 10010
                Cluster: default_cluster
                     IP: 10.248.181.219
          HeartbeatPort: 9050
                 BePort: 9060
               HttpPort: 8040
               BrpcPort: 8060
          LastStartTime: 2022-11-30 23:01:40
          LastHeartbeat: 2022-12-05 15:01:18
                  Alive: true
   SystemDecommissioned: false
  ClusterDecommissioned: false
              TabletNum: 753
       DataUsedCapacity: 1.955 GB
          AvailCapacity: 202.987 GB
          TotalCapacity: 491.153 GB
                UsedPct: 58.67 %
         MaxDiskUsedPct: 58.67 %
     RemoteUsedCapacity: 0.000
                    Tag: {"location" : "default"}
                 ErrMsg:
                Version: doris-0.0.0-trunk-80baca264
                 Status: {"lastSuccessReportTabletsTime":"2022-12-05 15:00:38","lastStreamLoadTime":-1,"isQueryDisabled":false,"isLoadDisabled":false}
HeartbeatFailureCounter: 0
               NodeRole: computation
```

### 使用

在 fe.conf 中添加配置项

```
prefer_compute_node_for_external_table=true
min_backend_num_for_external_table=3
```

> 参数说明请参阅：[FE 配置项](../admin-manual/config/fe-config.md)

当查询时使用[MultiCatalog](../lakehouse/multi-catalog/multi-catalog.md)功能时, 查询会优先调度到计算节点。

### 一些限制

- 计算节点由配置项控制, 但不要将混合类型节点, 修改配置为计算节点.

## 未尽事项

- 计算外溢: Doris内表查询, 当集群负载高的时候, 上层(TableScan之外)算子调度到计算节点中.
- 优雅下线: 当节点下线的时候, 任务新任务自动调度到其他节点; 等待老任务后全部完成后节点再下线; 老任务无法按时结束时, 能够让任务能够自己结束.
