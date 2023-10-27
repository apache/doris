---
{
    "title": "ALTER-SYSTEM-DECOMMISSION-BACKEND",
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

## ALTER-SYSTEM-DECOMMISSION-BACKEND

### Name

ALTER SYSTEM DECOMMISSION BACKEND

### Description

The node offline operation is used to safely log off the node. The operation is asynchronous. If successful, the node is eventually removed from the metadata. If it fails, the logout will not be done (only for admins!)

grammar:

- Find backend through host and port

```sql
ALTER SYSTEM DECOMMISSION BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
````

- Find backend through backend_id

```sql
ALTER SYSTEM DECOMMISSION BACKEND "id1","id2"...;
````

  illustrate:

1. host can be a hostname or an ip address
2. heartbeat_port is the heartbeat port of the node
3. The node offline operation is used to safely log off the node. The operation is asynchronous. If successful, the node is eventually removed from the metadata. If it fails, the logout will not be completed.
4. You can manually cancel the node offline operation. See CANCEL DECOMMISSION

### Example

1. Offline two nodes

    ```sql
    ALTER SYSTEM DECOMMISSION BACKEND "host1:port", "host2:port";
    ````

    ```sql
    ALTER SYSTEM DECOMMISSION BACKEND "id1", "id2";
    ````

### Keywords

    ALTER, SYSTEM, DECOMMISSION, BACKEND, ALTER SYSTEM

### Best Practice

