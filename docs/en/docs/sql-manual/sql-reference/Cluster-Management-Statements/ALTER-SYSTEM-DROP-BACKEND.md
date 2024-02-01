---
{
    "title": "ALTER-SYSTEM-DROP-BACKEND",
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

## ALTER-SYSTEM-DROP-BACKEND

### Name

ALTER SYSTEM DROP BACKEND

### Description

This statement is used to delete the BACKEND node (administrator only!)

grammar:

- Find backend through host and port

```sql
ALTER SYSTEM DROP BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...]
````
- Find backend through backend_id

```sql
ALTER SYSTEM DROP BACKEND "id1","id2"...;
````

illustrate:

1. host can be a hostname or an ip address
2. heartbeat_port is the heartbeat port of the node
3. Adding and deleting nodes is a synchronous operation. These two operations do not consider the existing data on the node, and the node is directly deleted from the metadata, please use it with caution.

### Example

1. Delete two nodes

    ```sql
    ALTER SYSTEM DROP BACKEND "host1:port", "host2:port";
    ````

    ```sql
    ALTER SYSTEM DROP BACKEND "ids1", "ids2";
    ````

### Keywords

    ALTER, SYSTEM, DROP, BACKEND, ALTER SYSTEM

### Best Practice

