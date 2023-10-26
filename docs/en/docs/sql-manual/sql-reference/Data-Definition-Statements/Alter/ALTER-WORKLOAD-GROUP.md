---
{
"title": "ALTER-WORKLOAD-GROUP",
"language": "en"
}
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

## ALTER-WORKLOAD-GROUP

### Name

ALTER WORKLOAD GROUP

<version since="2.0"></version>

### Description

This statement is used to modify the workload group.

Syntax:

```sql
ALTER WORKLOAD GROUP "rg_name"
PROPERTIES (
    property_list
);
```

NOTE:

* Modify the memory_limit property in such a way that the sum of all memory_limit values does not exceed 100%;
* Support modifying some properties, for example, if only cpu_share is modified, just fill in cpu_share in properties.

### Example

1. Modify the workload group named g1:

    ```sql
    alter workload group g1
    properties (
        "cpu_share"="30",
        "memory_limit"="30%"
    );
    ```

### Keywords

```sql
ALTER, WORKLOAD, GROUP
```

### Best Practice
