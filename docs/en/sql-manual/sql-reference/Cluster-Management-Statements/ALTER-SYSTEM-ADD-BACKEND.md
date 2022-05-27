---
{
    "title": "ALTER-SYSTEM-ADD-BACKEND",
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

## ALTER-SYSTEM-ADD-BACKEND

### Name

ALTER SYSTEM ADD BACKEND

### Description

This statement is used to manipulate nodes within a system. (Administrator only!)

grammar:

```sql
-- Add nodes (add this method if you do not use the multi-tenancy function)
   ALTER SYSTEM ADD BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
-- Add idle nodes (that is, add BACKEND that does not belong to any cluster)
   ALTER SYSTEM ADD FREE BACKEND "host:heartbeat_port"[,"host:heartbeat_port"...];
-- Add nodes to a cluster
   ALTER SYSTEM ADD BACKEND TO cluster_name "host:heartbeat_port"[,"host:heartbeat_port"...];
````

 illustrate:

1. host can be a hostname or an ip address
2. heartbeat_port is the heartbeat port of the node
3. Adding and deleting nodes is a synchronous operation. These two operations do not consider the existing data on the node, and the node is directly deleted from the metadata, please use it with caution.

### Example

 1. Add a node

    ```sql
    ALTER SYSTEM ADD BACKEND "host:port";
    ````

 1. Add an idle node

    ```sql
    ALTER SYSTEM ADD FREE BACKEND "host:port";
    ````

### Keywords

    ALTER, SYSTEM, ADD, BACKEND

### Best Practice

