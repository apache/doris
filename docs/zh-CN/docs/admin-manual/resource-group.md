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

1. 开启 experimental_enable_resource_group 配置项，在fe.conf中设置：
```
experimental_enable_resource_group=true
```
在开启该配置后系统会自动创建名为`normal`的默认资源组。

2. 创建资源组：
```
create resource group if not exists g1
properties (
    "cpu_share"="10",
    "memory_limit"="30%",
    "enable_memory_overcommit"="true"
);
```
创建资源组详细可参考：[CREATE-RESOURCE-GROUP](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-RESOURCE-GROUP.md)，另删除资源组可参考[DROP-RESOURCE-GROUP](../sql-manual/sql-reference/Data-Definition-Statements/Drop/DROP-RESOURCE-GROUP.md)；修改资源组可参考：[ALTER-RESOURCE-GROUP](../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-RESOURCE-GROUP.md)；查看资源组可参考：[RESOURCE_GROUPS()](../sql-manual/sql-functions/table-functions/resource-group.md)和[SHOW-RESOURCE-GROUPS](../sql-manual/sql-reference/Show-Statements/SHOW-RESOURCE-GROUPS.md)。

3. 开启pipeline执行引擎，资源组cpu隔离基于pipeline执行引擎实现，因此需开启session变量：
```
set experimental_enable_pipeline_engine = true;
```

4. 设置session变量 resource_group 指定使用的资源组，默认为默认资源组`normal`:
```
resource_group = g1;
```

5. 执行查询，查询将关联到 g1 资源组。
