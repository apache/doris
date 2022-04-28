---
{
    "title": "SHOW-TRASH",
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

## SHOW-TRASH

### Name

SHOW TRASH

### Description

This statement is used to view the garbage data footprint within the backend.

grammar:

```sql
SHOW TRASH [ON BackendHost:BackendHeartBeatPort];
````

illustrate:

1. The Backend format is the node's BackendHost:BackendHeartBeatPort
2. TrashUsedCapacity indicates the space occupied by the garbage data of the node.

### Example

1. View the space occupied by garbage data of all be nodes.

   ```sql
    SHOW TRASH;
   ````

2. View the space occupied by garbage data of '192.168.0.1:9050' (specific disk information will be displayed).

   ```sql
   SHOW TRASH ON "192.168.0.1:9050";
   ````

### Keywords

    SHOW, TRASH

### Best Practice