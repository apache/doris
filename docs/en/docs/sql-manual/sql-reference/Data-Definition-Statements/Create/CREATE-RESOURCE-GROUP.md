---
{
    "title": "CREATE-RESOURCE-GORUP",
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

## CREATE-RESOURCE-GROUP

### Name

CREATE RESOURCE GROUP

<version since="dev"></version>

### Description

This statement is used to create a resource group. Resource groups enable the isolation of cpu resources and memory resources on a single be.

grammar:

```sql
CREATE RESOURCE GROUP [IF NOT EXISTS] "rg_name"
PROPERTIES (
    property_list
);
```

illustrate:

Properties supported by property_list:

* cpu_share: Required, used to set how much cpu time the resource group can acquire, which can achieve soft isolation of cpu resources. cpu_share is a relative value indicating the weight of cpu resources available to the running resource group. For example, if a user creates 3 resource groups rg-a, rg-b and rg-c with cpu_share of 10, 30 and 40 respectively, and at a certain moment rg-a and rg-b are running tasks while rg-c has no tasks, then rg-a can get 25% (10 / (10 + 30)) of the cpu resources while resource group rg-b can get 75% of the cpu resources. If the system has only one resource group running, it gets all the cpu resources regardless of the value of its cpu_share.

* memory_limit: Required, set the percentage of be memory that can be used by the resource group. The absolute value of the resource group memory limit is: physical_memory * mem_limit * memory_limit, where mem_limit is a be configuration item. The total memory_limit of all resource groups in the system must not exceed 100%. Resource groups are guaranteed to use the memory_limit for the tasks in the group in most cases. When the resource group memory usage exceeds this limit, tasks in the group with larger memory usage may be canceled to release the excess memory, refer to enable_memory_overcommit.

* enable_memory_overcommit: Optional, enable soft memory isolation for the resource group, default is false. if set to false, the resource group is hard memory isolated and the tasks with the largest memory usage will be canceled immediately after the resource group memory usage exceeds the limit to release the excess memory. if set to true, the resource group is hard memory isolated and the tasks with the largest memory usage will be canceled immediately after the resource group memory usage exceeds the limit to release the excess memory. if set to true, the resource group is softly isolated, if the system has free memory resources, the resource group can continue to use system memory after exceeding the memory_limit limit, and when the total system memory is tight, it will cancel several tasks in the group with the largest memory occupation, releasing part of the excess memory to relieve the system memory pressure. It is recommended that when this configuration is enabled for a resource group, the total memory_limit of all resource groups should be less than 100%, and the remaining portion should be used for resource group memory overcommit.

### Example

1. Create a resource group named g1:

   ```sql
    create resource group if not exists g1
    properties (
        "cpu_share"="10",
        "memory_limit"="30%",
        "enable_memory_overcommit"="true"
    );
   ```

### Keywords

    CREATE, RESOURCE, GROUP

### Best Practice

