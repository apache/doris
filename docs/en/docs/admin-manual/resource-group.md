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

## Resource group properties

* cpu_share: Required, used to set how much cpu time the resource group can acquire, which can achieve soft isolation of cpu resources. cpu_share is a relative value indicating the weight of cpu resources available to the running resource group. For example, if a user creates 3 resource groups rg-a, rg-b and rg-c with cpu_share of 10, 30 and 40 respectively, and at a certain moment rg-a and rg-b are running tasks while rg-c has no tasks, then rg-a can get 25% (10 / (10 + 30)) of the cpu resources while resource group rg-b can get 75% of the cpu resources. If the system has only one resource group running, it gets all the cpu resources regardless of the value of its cpu_share.

* memory_limit: Required, set the percentage of be memory that can be used by the resource group. The absolute value of the resource group memory limit is: physical_memory * mem_limit * memory_limit, where mem_limit is a be configuration item. The total memory_limit of all resource groups in the system must not exceed 100%. Resource groups are guaranteed to use the memory_limit for the tasks in the group in most cases. When the resource group memory usage exceeds this limit, tasks in the group with larger memory usage may be canceled to release the excess memory, refer to enable_memory_overcommit.

* enable_memory_overcommit: Optional, enable soft memory isolation for the resource group, default is false. if set to false, the resource group is hard memory isolated and the tasks with the largest memory usage will be canceled immediately after the resource group memory usage exceeds the limit to release the excess memory. if set to true, the resource group is hard memory isolated and the tasks with the largest memory usage will be canceled immediately after the resource group memory usage exceeds the limit to release the excess memory. if set to true, the resource group is softly isolated, if the system has free memory resources, the resource group can continue to use system memory after exceeding the memory_limit limit, and when the total system memory is tight, it will cancel several tasks in the group with the largest memory occupation, releasing part of the excess memory to relieve the system memory pressure. It is recommended that when this configuration is enabled for a resource group, the total memory_limit of all resource groups should be less than 100%, and the remaining portion should be used for resource group memory overcommit.

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

4. set session variable resource_group Specify the resource group to use, default is the default resource group ``normal``.
```
resource_group = g1.
```

5. Execute the query, which will be associated with the g1 resource group.