---
{
    "title": "RESOURCE GROUP",
    "language": "en"
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

# RESOURCE GROUP

<version since="dev"></version>

Resource groups can limit the use of compute and memory resources on a single be node for tasks within the group, thus achieving resource isolation.

## Compute Resource Isolation

Resource groups implement computing resource isolation on the basis of [pipeline execution engine](../query-acceleration/pipeline-execution-engine.md). A vruntime is evaluated in each resource group, and the pipeline execution engine prioritizes resource groups with smaller vruntimes.

### Computing resource isolation related properties

* cpu_share: Required, used to set the amount of cpu time obtained by the resource group, which can realize soft isolation of cpu resources. cpu_share is a relative value, which means that the running resource group can obtain the weight of cpu resources. For example, the user creates three resource groups rg-a, rg-b, and rg-c, and the cpu_share is 10, 30, and 40 respectively. At a certain moment, rg-a and rg-b are running tasks, but rg-c has no tasks , at this time rg-a can get 25% (10 / (10 + 30)) of cpu resources, and resource group rg-b can get 75% of cpu resources. If the system has only one resource group running, it can get all the cpu resources regardless of the value of its cpu_share.

## Memory resource isolation

Resource groups implement memory resource isolation on the basis of [memory tracker](./maint-monitor/memory-management/memory-tracker.md). It supports configuring resource groups as memory hard isolation or memory soft isolation. Under hard memory isolation, after the memory GC thread detects that the memory usage of the resource group exceeds the memory_limit limit, it will immediately cancel several tasks with the largest memory usage in the group to release the excess memory; under soft memory isolation, if memory resources are free, After the memory used by the resource group exceeds the limit of the resource group memory_limit, the resource group can borrow memory from other resource groups or the system until the total system memory is tight, it will cancel several tasks with the largest memory usage in the group, and release part of the excess memory to alleviate the problem. System memory pressure. It is recommended that when a resource group has a memory soft limit enabled, properly lower resource_group_mem_limit or make the sum of memory_limit of all resource groups lower than 100%, so as to reserve appropriate memory for resource group memory oversupply.

The total memory managed by the resource group can be specified through the resource_group_mem_limit configuration item in be.conf, and the memory managed by the resource group in each be node is
`resource_group_memory = mem_limit * resource_group_mem_limit`,
The mem_limit here is a configuration item in be.conf, indicating that the be process can use memory, and the default resource_group_mem_limit = 90%. Since be has other tasks, if the resource_group_mem_limit is set higher, it may reach the be process memory GC threshold before reaching the resource group mem_limit limit, causing the tasks in the resource group to be GC by the be process.

### Memory resource isolation related properties

* memory_limit: Required, specify the resource group memory limit. The absolute value of the resource group memory limit is: `resource_group_memory * memory_limit`. The total memory_limit of all resource groups in the system cannot exceed 100%. In most cases, the resource group guarantees that the tasks in the group can use the memory of memory_limit. When the memory usage of the resource group exceeds this limit, the tasks in the group that occupy a large amount of memory may be canceled to release the excess memory.

* enable_memory_overcommit: Optional, used to enable resource group memory soft isolation, the default is false. If set to false, the resource group is memory hard isolation; if set to true, the resource group is memory soft isolation.

## Resource group usage

1. Enable the experimental_enable_resource_group configuration, set in fe.conf to
```
experimental_enable_resource_group=true
```
The system will automatically create a default resource group named ``normal`` after this configuration is enabled. 2.

2. To create a resource group:
```
create resource group if not exists g1
properties (
    "cpu_share"="10".
    "memory_limit"="30%".
    "enable_memory_overcommit"="true"
).
```
For details on creating a resource group, see [CREATE-RESOURCE-GROUP](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-RESOURCE-GROUP.md), and to delete a resource group, refer to [DROP-RESOURCE-GROUP](../sql-manual/sql-reference/Data-Definition-Statements/Drop/DROP-RESOURCE-GROUP.md); to modify a resource group, refer to [ALTER-RESOURCE-GROUP](../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-RESOURCE-GROUP.md); to view the resource group, refer to: [RESOURCE_GROUPS()](../sql-manual/sql-functions/table-functions/resource-group.md) and [SHOW-RESOURCE-GROUPS](../sql-manual/sql-reference/Show-Statements/SHOW-RESOURCE-GROUPS.md).


3. turn on the pipeline execution engine, the resource group cpu isolation is based on the implementation of the pipeline execution engine, so you need to turn on the session variable:
```
set experimental_enable_pipeline_engine = true.
```

4. Queries bind to resource groups. Currently, queries are mainly bound to resource groups by specifying session variables. If the user does not specify a resource group, the query will be submitted to the `normal` resource group by default.
```
set resource_group = g1.
```

5. Execute the query, which will be associated with the g1 resource group.