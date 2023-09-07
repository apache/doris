---
{
    "title": "WORKLOAD GROUP",
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

# WORKLOAD GROUP

<version since="dev"></version>

The workload group can limit the use of compute and memory resources on a single be node for tasks within the group. Currently, query binding to workload groups is supported.

## Workload group properties

* cpu_share: Required, used to set how much cpu time the workload group can acquire, which can achieve soft isolation of cpu resources. cpu_share is a relative value indicating the weight of cpu resources available to the running workload group. For example, if a user creates 3 workload groups rg-a, rg-b and rg-c with cpu_share of 10, 30 and 40 respectively, and at a certain moment rg-a and rg-b are running tasks while rg-c has no tasks, then rg-a can get 25% (10 / (10 + 30)) of the cpu resources while workload group rg-b can get 75% of the cpu resources. If the system has only one workload group running, it gets all the cpu resources regardless of the value of its cpu_share.

* memory_limit: Required, set the percentage of be memory that can be used by the workload group. The absolute value of the workload group memory limit is: `physical_memory * mem_limit * memory_limit`, where mem_limit is a be configuration item. The total memory_limit of all workload groups in the system must not exceed 100%. Workload groups are guaranteed to use the memory_limit for the tasks in the group in most cases. When the workload group memory usage exceeds this limit, tasks in the group with larger memory usage may be canceled to release the excess memory, refer to enable_memory_overcommit.

* enable_memory_overcommit: Optional, enable soft memory isolation for the workload group, default is false. if set to false, the workload group is hard memory isolated and the tasks with the largest memory usage will be canceled immediately after the workload group memory usage exceeds the limit to release the excess memory. if set to true, the workload group is hard memory isolated and the tasks with the largest memory usage will be canceled immediately after the workload group memory usage exceeds the limit to release the excess memory. if set to true, the workload group is softly isolated, if the system has free memory resources, the workload group can continue to use system memory after exceeding the memory_limit limit, and when the total system memory is tight, it will cancel several tasks in the group with the largest memory occupation, releasing part of the excess memory to relieve the system memory pressure. It is recommended that when this configuration is enabled for a workload group, the total memory_limit of all workload groups should be less than 100%, and the remaining portion should be used for workload group memory overcommit.

## Workload group usage

1. Enable the experimental_enable_workload_group configuration, set in fe.conf to
```
experimental_enable_workload_group=true
```
The system will automatically create a default workload group named ``normal`` after this configuration is enabled. 

2. To create a workload group:
```
create workload group if not exists g1
properties (
    "cpu_share"="10".
    "memory_limit"="30%".
    "enable_memory_overcommit"="true"
).
```
For details on creating a workload group, see [CREATE-WORKLOAD-GROUP](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-WORKLOAD-GROUP.md), and to delete a workload group, refer to [DROP-WORKLOAD-GROUP](../sql-manual/sql-reference/Data-Definition-Statements/Drop/DROP-WORKLOAD-GROUP.md); to modify a workload group, refer to [ALTER-WORKLOAD-GROUP](../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-WORKLOAD-GROUP.md); to view the workload group, refer to: [WORKLOAD_GROUPS()](../sql-manual/sql-functions/table-functions/workload-group.md) and [SHOW-WORKLOAD-GROUPS](../sql-manual/sql-reference/Show-Statements/SHOW-WORKLOAD-GROUPS.md).


3. turn on the pipeline execution engine, the workload group cpu isolation is based on the implementation of the pipeline execution engine, so you need to turn on the session variable:
```
set experimental_enable_pipeline_engine = true.
```

4. Bind the workload group.
* Bind the user to the workload group by default by setting the user property to ``normal``.
```
set property 'default_workload_group' = 'g1'.
```
The current user's query will use 'g1' by default.
* Specify the workload group via the session variable, which defaults to null.
```
set workload_group = 'g2'.
```
session variable `workload_group` takes precedence over user property `default_workload_group`, in case `workload_group` is empty, the query will be bound to `default_workload_group`, in case session variable ` workload_group` is not empty, the query will be bound to `workload_group`.

If you are a non-admin user, you need to execute [SHOW-WORKLOAD-GROUPS](../sql-manual/sql-reference/Show-Statements/SHOW-WORKLOAD-GROUPS.md) to check if the current user can see the workload group, if not, the workload group may not exist or the current user does not have permission to execute the query. If you cannot see the workload group, the workload group may not exist or the current user does not have privileges. To authorize the workload group, refer to: [grant statement](../sql-manual/sql-reference/Account-Management-Statements/GRANT.md).

5. Execute the query, which will be associated with the g1 workload group.

### Query queue Function
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
The current workload group supports the function of querying queues, which can be specified when creating a new group. The following three parameters are required:
* max_concurrency，the maximum number of queries allowed by the current group; Queries exceeding the maximum concurrency will enter the queue logic
* max_queue_size，the length of the query waiting queue; When the queue is full, new queries will be rejected
* queue_timeout，the time the query waits in the queue. If the query wait time exceeds this value, the query will be rejected, and the time unit is milliseconds

It should be noted that the current queuing design is not aware of the number of FEs, and the queuing parameters only works in a single FE, for example:

A Doris cluster is configured with a work load group and set max_concurrency=1,
If there is only 1 FE in the cluster, then this workload group will only run one SQL at the same time from the Doris cluster perspective,
If there are 3 FEs, the maximum number of query that can be run in Doris cluster is 3.