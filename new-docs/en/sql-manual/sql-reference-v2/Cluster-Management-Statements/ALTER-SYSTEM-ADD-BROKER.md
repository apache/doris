---
{
    "title": "ALTER-SYSTEM-ADD-BROKER",
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

## ALTER-SYSTEM-ADD-BROKER

### Name

ALTER SYSTEM ADD BROKER

### Description

This statement is used to add a BROKER node. (Administrator only!)

grammar:

```sql
ALTER SYSTEM ADD BROKER broker_name "broker_host1:broker_ipc_port1","broker_host2:broker_ipc_port2",...;
````

### Example

1. Add two brokers

    ```sql
     ALTER SYSTEM ADD BROKER "host1:port", "host2:port";
    ````

### Keywords

    ALTER, SYSTEM, ADD, FOLLOWER

### Best Practice

