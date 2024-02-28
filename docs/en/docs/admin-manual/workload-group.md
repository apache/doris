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

## Version Description
Workload Group is a feature that has been supported since version 2.0. The main difference between version 2.0 and 2.1 is that the 2.0 version of Workload Group does not rely on CGroup, while the 2.1 version of Workload Group depends on CGroup. Therefore, when using the 2.1 version of Workload Group, the environment of CGroup needs to be configured.

#### Upgrade to version 2.0
If upgrading from version 1.2 to version 2.0, it is recommended to enable the WorkloadGroup after the overall upgrade of the Doris cluster is completed. Because if you only upgrade a single Follower and enable this feature, as the FE code of the Master has not been updated yet, there is no metadata information for Workload Group in the Doris cluster, which may cause queries for the upgraded Follower nodes to fail. The recommended upgrade process is as follows:
* First, upgrade the overall code of the Doris cluster to version 2.0.
* Start using this feature according to the section ***Workload group usage*** in the following text.

#### Upgrade to version 2.1
If the code version is upgraded from 2.0 to 2.1, there are two situations:

Scenario 1: In version 2.1, if the Workload Group has already been used, you only need to refer to the process of configuring cgroup v1 in the following text to use the new version of the Workload Group.

Scenario 2: If the Workload Group is not used in version 2.0, it is also necessary to upgrade the Doris cluster as a whole to version 2.1, and then start using this feature according to the section ***Workload group usage*** in the following text.

## Workload group properties

* cpu_share: Optional, The default value is 1024, with a range of positive integers. used to set how much cpu time the workload group can acquire, which can achieve soft isolation of cpu resources. cpu_share is a relative value indicating the weight of cpu resources available to the running workload group. For example, if a user creates 3 workload groups rg-a, rg-b and rg-c with cpu_share of 10, 30 and 40 respectively, and at a certain moment rg-a and rg-b are running tasks while rg-c has no tasks, then rg-a can get 25% (10 / (10 + 30)) of the cpu resources while workload group rg-b can get 75% of the cpu resources. If the system has only one workload group running, it gets all the cpu resources regardless of the value of its cpu_share.

* memory_limit: Optional, default value is 0% which means unlimited, range of values from 1% to 100%. set the percentage of be memory that can be used by the workload group. The absolute value of the workload group memory limit is: `physical_memory * mem_limit * memory_limit`, where mem_limit is a be configuration item. The total memory_limit of all workload groups in the system must not exceed 100%. Workload groups are guaranteed to use the memory_limit for the tasks in the group in most cases. When the workload group memory usage exceeds this limit, tasks in the group with larger memory usage may be canceled to release the excess memory, refer to enable_memory_overcommit.

* enable_memory_overcommit: Optional, enable soft memory isolation for the workload group, default is true. if set to false, the workload group is hard memory isolated and the tasks with the largest memory usage will be canceled immediately after the workload group memory usage exceeds the limit to release the excess memory. if set to true, the workload group is hard memory isolated and the tasks with the largest memory usage will be canceled immediately after the workload group memory usage exceeds the limit to release the excess memory. if set to true, the workload group is softly isolated, if the system has free memory resources, the workload group can continue to use system memory after exceeding the memory_limit limit, and when the total system memory is tight, it will cancel several tasks in the group with the largest memory occupation, releasing part of the excess memory to relieve the system memory pressure. It is recommended that when this configuration is enabled for a workload group, the total memory_limit of all workload groups should be less than 100%, and the remaining portion should be used for workload group memory overcommit.

* cpu_hard_limit: Optional, default value -1%, no limit. The range of values is from 1% to 100%. In CPU hard limit mode, the maximum available CPU percentage of Workload Group cannot exceed cpu_hard_limit value, regardless of whether the current machine's CPU resources are fully utilized.
  Sum of all Workload Groups's cpu_hard_limit cannot exceed 100%. This is a new property added since version 2.1.
* max_concurrency: Optional, maximum query concurrency, default value is the maximum integer value, which means there is no concurrency limit. When the number of running queries reaches this value, new queries will being queued.
* max_queue_size: Optional, length of the query queue. When the queue is full, new queries will be rejected. The default value is 0, which means no queuing.
* queue_timeout: Optional, query the timeout time in the queue, measured in milliseconds. If the query exceeds this value, an exception will be thrown directly to the client. The default value is 0, which means no queuing.
* scan_thread_num: Optional, the number of threads used for scanning in the current workload group. The default value is -1, which means it does not take effect, the number of scan threads in the be configuration shall prevail. The value is an integer greater than 0.

Notes:

1 At present, the simultaneous use of CPU's soft and hard limits is not supported. A cluster can only have soft or hard limits at a certain time. The switching method will be described in the following text.

2 All properties are optional, but at least one propety needs to be specified when creating a Workload Group.


## Configure cgroup v1

Doris 2.0 version uses Doris scheduling to limit CPU resources, but since version 2.1, Doris defaults to using CGgroup v1 to limit CPU resources (CGgroup v2 is currently not supported). Therefore, if CPU resources are expected to be limited in version 2.1, it is necessary to have CGgroup v1 installed on the node where BE is located.

If users use the Workload Group software limit in version 2.0 and upgrade to version 2.1, they also need to configure CGroup, Otherwise, cpu soft limit may not work.

Without configuring cgroup, users can use all functions of the workload group except for CPU limitations.

1 Firstly, confirm that the CGgroup v1 version has been installed on the node where BE is located, and the path ```/sys/fs/cgroup/cpu/``` exists.

2 Create a new directory named doris in the CPU path of cgroup, user can specify their own directory name.

```mkdir /sys/fs/cgroup/cpu/doris```

3 It is necessary to ensure that Doris's BE process has read/write/execute permissions for this directory
```
// Modify the permissions of this directory to read, write, and execute
chmod 770 /sys/fs/cgroup/cpu/doris

// Assign the ownership of this directory to Doris's account
chonw -R doris:doris /sys/fs/cgroup/cpu/doris
```

4 Modify the configuration of BE and specify the path to cgroup
```
doris_cgroup_cpu_path = /sys/fs/cgroup/cpu/doris
```

5 restart BE, in the log (be. INFO), you can see the words "add thread xxx to group" indicating successful configuration.

It should be noted that the current workload group does not support the deployment of multiple BE on same machine.

## Workload group usage

1. Manually create a workload group named normal, which is the default workload group in the system and cannot be deleted.
```
create workload group if not exists normal 
properties (
	'cpu_share'='1024',
	'memory_limit'='30%',
	'enable_memory_overcommit'='true'
);
```
The function of a normal group is that when you do not specify a Workload Group for a query, the query will use normal Group, thus avoiding query failures.

2. Enable the experimental_enable_workload_group configuration, set in fe.conf to
```
experimental_enable_workload_group=true
```
The system will automatically create a default workload group named ``normal`` after this configuration is enabled. 

3. If you expect to use other groups for testing, you can create a custom workload group,
```
create workload group if not exists g1
properties (
    "cpu_share"="1024".
    "memory_limit"="30%".
    "enable_memory_overcommit"="true"
).
```
This configured CPU limit to the soft limit.

For details on creating a workload group, see [CREATE-WORKLOAD-GROUP](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-WORKLOAD-GROUP.md), and to delete a workload group, refer to [DROP-WORKLOAD-GROUP](../sql-manual/sql-reference/Data-Definition-Statements/Drop/DROP-WORKLOAD-GROUP.md); to modify a workload group, refer to [ALTER-WORKLOAD-GROUP](../sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-WORKLOAD-GROUP.md); to view the workload group, refer to: [WORKLOAD_GROUPS()](../sql-manual/sql-functions/table-functions/workload-group.md) and [SHOW-WORKLOAD-GROUPS](../sql-manual/sql-reference/Show-Statements/SHOW-WORKLOAD-GROUPS.md).


4. turn on the pipeline execution engine, the workload group cpu isolation is based on the implementation of the pipeline execution engine, so you need to turn on the session variable:
```
set experimental_enable_pipeline_engine = true.
```

5. Bind the workload group.
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

6. Execute the query, which will be associated with the g1 workload group.

### Query Queue
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
It should be noted that the current queuing design is not aware of the number of FEs, and the queuing parameters only works in a single FE, for example:

A Doris cluster is configured with a work load group and set max_concurrency=1,
If there is only 1 FE in the cluster, then this workload group will only run one SQL at the same time from the Doris cluster perspective,
If there are 3 FEs, the maximum number of query that can be run in Doris cluster is 3.

### Configure CPU hard limits
At present, Doris defaults to running the CPU's soft limit. If you want to use Workload Group's hard limit, you can do as follows.

1 Enable the cpu hard limit in FE. If there are multiple FE, the same operation needs to be performed on each FE.
```
1 modify fe.conf in disk
experimental_enable_cpu_hard_limit = true

2 modify conf in memory
ADMIN SET FRONTEND CONFIG ("enable_cpu_hard_limit" = "true");
```

2 modify cpu_hard_limit
```
alter workload group group1 properties ( 'cpu_hard_limit'='20%' );
```

3 Viewing the current configuration of the Workload Group, it can be seen that although the cpu_share may not be 0, but due to the hard limit mode being enabled, the query will also follow the CPU's hard limit during execution. That is to say, the switch of CPU software and hardware limits does not affect workload group modification.
```
mysql [(none)]>select name, cpu_share,memory_limit,enable_memory_overcommit,cpu_hard_limit from workload_groups() where name='group1';
+--------+-----------+--------------+--------------------------+----------------+
| Name   | cpu_share | memory_limit | enable_memory_overcommit | cpu_hard_limit |
+--------+-----------+--------------+--------------------------+----------------+
| group1 |        10 | 45%          | true                     | 20%            |
+--------+-----------+--------------+--------------------------+----------------+
1 row in set (0.03 sec)
```

### How to switch CPU limit node between soft limit and hard limit
At present, Doris does not support running both the soft and hard limits of the CPU simultaneously. A Doris cluster can only have either the CPU soft limit or the CPU hard limit at any time.

Users can switch between two modes, and the main switching methods are as follows:

1 If the current cluster configuration is set to the default CPU soft limit and it is expected to be changed to the CPU hard limit, then cpu_hard_limit should be set to a valid value first.
```
alter workload group group1 properties ( 'cpu_hard_limit'='20%' );
```
It is necessary to modify cpu_hard_limit of all Workload Groups in the current cluster, sum of all Workload Group's cpu_hard_limit cannot exceed 100%.
Due to the CPU's hard limit can not being able to provide a valid default value, if only the switch is turned on without modifying cpu_hard_limit, the CPU's hard limit will not work.

2 Turn on the CPU hard limit switch in all FEs.
```
1 modify fe.conf
experimental_enable_cpu_hard_limit = true

2 modify conf in memory
ADMIN SET FRONTEND CONFIG ("enable_cpu_hard_limit" = "true");
```

If user expects to switch back from cpu hard limit to cpu soft limit, then they only need to set ```enable_cpu_hard_limit=false```.
CPU Soft Limit property ```cpu_shared``` will be filled with a valid value of 1024 by default(If the user has never set the cpu_share before), and users can adjust cpu_share based on the priority of Workload Group.
