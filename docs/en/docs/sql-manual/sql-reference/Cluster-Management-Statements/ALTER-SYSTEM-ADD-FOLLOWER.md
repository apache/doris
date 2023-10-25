---
{
    "title": "ALTER-SYSTEM-ADD-FOLLOWER",
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

## ALTER-SYSTEM-ADD-FOLLOWER

### Name

ALTER SYSTEM ADD FOLLOWER

### Description

This statement is to increase the node of the FOLLOWER role of FRONTEND, (only for administrators!)

grammar:

```sql
ALTER SYSTEM ADD FOLLOWER "follower_host:edit_log_port[,"follower_host:edit_log_port"...]";
````

illustrate:

1. host can be a hostname or an ip address
2. edit_log_port : edit_log_port in its configuration file fe.conf

### Example

1. Add a FOLLOWER node

    ```sql
    ALTER SYSTEM ADD FOLLOWER "host_ip:9010"
    ````

### Keywords

    ALTER, SYSTEM, ADD, FOLLOWER, ALTER SYSTEM

### Best Practice

