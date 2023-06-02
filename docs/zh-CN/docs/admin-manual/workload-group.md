---
{
    "title": "资源组",
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

# 资源组

<version since="dev"></version>

资源组可限制组内任务在单个be节点上的计算资源和内存资源的使用，从而达到资源隔离的目的。

## 资源组属性

* cpu_share：必选，用于设置资源组获取cpu时间的多少，可以实现cpu资源软隔离。cpu_share 是相对值，表示正在运行的资源组可获取cpu资源的权重。例如，用户创建了3个资源组 rg-a、rg-b和rg-c，cpu_share 分别为 10、30、40，某一时刻rg-a和rg-b正在跑任务，而rg-c没有任务，此时rg-a可获得 25% (10 / (10 + 30))的cpu资源，而资源组rg-b可获得75%的cpu资源。如果系统只有一个资源组正在运行，则不管其cpu_share的值为多少，它都可以获取全部的cpu资源。

* memory_limit: 必选，用于设置资源组可以使用be内存的百分比。资源组内存限制的绝对值为： 物理内存 * mem_limit * memory_limit，其中 mem_limit 为be配置项。系统所有资源组的 memory_limit总合不可超过100%。资源组在绝大多数情况下保证组内任务可使用memory_limit的内存，当资源组内存使用超出该限制后，组内内存占用较大的任务可能会被cancel以释放超出的内存，参考 enable_memory_overcommit。

* enable_memory_overcommit: 可选，用于开启资源组内存软隔离，默认为false。如果设置为false，则该资源组为内存硬隔离，系统检测到资源组内存使用超出限制后将立即cancel组内内存占用最大的若干个任务，以释放超出的内存；如果设置为true，则该资源组为内存软隔离，如果系统有空闲内存资源则该资源组在超出memory_limit的限制后可继续使用系统内存，在系统总内存紧张时会cancel组内内存占用最大的若干个任务，释放部分超出的内存以缓解系统内存压力。建议在有资源组开启该配置时，所有资源组的 memory_limit 总合低于100%，剩余部分用于资源组内存超发。

## 资源组使用

1. 开启 experimental_enable_workload_group 配置项，在fe.conf中设置：
```
experimental_enable_workload_group=true
```
在开启该配置后系统会自动创建名为`normal`的默认资源组。

2. 创建资源组：
```
create workload group if not exists g1
properties (
    "cpu_share"="10",
    "memory_limit"="30%",
    "enable_memory_overcommit"="true"
);
```
创建资源组详细可参考：[CREATE-WORKLOAD-GROUP](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-WORKLOAD-GROUP.md)，另删除资源组可参考[DROP-WORKLOAD-GROUP](../sql-manual/sql-reference/Data-Definition-Statements/Drop/DROP-WORKLOAD-GROUP.md)；修改资源组可参考：[ALTER-WORKLOAD-GROUP](../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-WORKLOAD-GROUP.md)；查看资源组可参考：[WORKLOAD_GROUPS()](../sql-manual/sql-functions/table-functions/workload-group.md)和[SHOW-WORKLOAD-GROUPS](../sql-manual/sql-reference/Show-Statements/SHOW-WORKLOAD-GROUPS.md)。

3. 开启pipeline执行引擎，资源组cpu隔离基于pipeline执行引擎实现，因此需开启session变量：
```
set experimental_enable_pipeline_engine = true;
```

4. 查询绑定资源组。目前主要通过指定session变量的方式绑定查询到资源组。如果用户不指定资源组，那么查询默认会提交到`normal`资源组:
```
set workload_group = g1;
```

5. 执行查询，查询将关联到 g1 资源组。

### 查询排队功能
```
create workload group if not exists test_group
properties (
    "cpu_share"="10",
    "memory_limit"="30%",
    "max_concurrency" = "10",
    "max_queue_size" = "20",
    "queue_timeout" = "3000"
);
```
目前的workload group支持查询排队的功能，可以在新建group时进行指定，需要以下三个参数:
* max_concurrency，当前group允许的最大查询数;超过最大并发的查询到来时会进入排队逻辑
* max_queue_size，查询排队的长度;当队列满了之后，新来的查询会被拒绝
* queue_timeout，查询在队列中等待的时间，如果查询等待时间超过这个值，那么查询会被拒绝，时间单位为毫秒

需要注意的是，目前的排队设计是不感知FE的个数的，排队的参数只在单FE粒度生效，例如：

一个Doris集群配置了一个work load group，设置max_concurrency = 1
如果集群中有1FE，那么这个workload group在Doris集群视角看同时只会运行一个SQL
如果有3台FE，那么在Doris集群视角看最大可运行的SQL个数为3