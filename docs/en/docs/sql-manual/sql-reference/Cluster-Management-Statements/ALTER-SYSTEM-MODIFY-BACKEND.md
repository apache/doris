---
{
    "title": "ALTER-SYSTEM-MODIFY-BACKEND",
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

## ALTER-SYSTEM-MODIFY-BACKEND

### Name

ALTER SYSTEM MKDIFY BACKEND

### Description

Modify BE node properties (administrator only!)

grammar:

- Find backend through host and port

```sql
ALTER SYSTEM MODIFY BACKEND "host:heartbeat_port" SET ("key" = "value"[, ...]);
````

- Find backend through backend_id

```sql
ALTER SYSTEM MODIFY BACKEND "id1" SET ("key" = "value"[, ...]);
````

  illustrate:

1. host can be a hostname or an ip address
2. heartbeat_port is the heartbeat port of the node
3. Modify BE node properties The following properties are currently supported:

- tag.xxxx: resource tag
- disable_query: query disable attribute
- disable_load: import disable attribute

Note:
1. A backend can be set multi resource tags. But must contain "tag.location" type.

### Example

1. Modify the resource tag of BE

    ```sql
    ALTER SYSTEM MODIFY BACKEND "host1:heartbeat_port" SET ("tag.location" = "group_a");
    ALTER SYSTEM MODIFY BACKEND "host1:heartbeat_port" SET ("tag.location" = "group_a", "tag.compute" = "c1");
    ````
   
    ```sql
    ALTER SYSTEM MODIFY BACKEND "id1" SET ("tag.location" = "group_a");
    ALTER SYSTEM MODIFY BACKEND "id1" SET ("tag.location" = "group_a", "tag.compute" = "c1");
    ````

2. Modify the query disable property of BE

    ```sql
    ALTER SYSTEM MODIFY BACKEND "host1:heartbeat_port" SET ("disable_query" = "true");
    ````
   
    ```sql
    ALTER SYSTEM MODIFY BACKEND "id1" SET ("disable_query" = "true");
    ````

3. Modify the import disable property of BE

    ```sql
    ALTER SYSTEM MODIFY BACKEND "host1:heartbeat_port" SET ("disable_load" = "true");
    ````
   
    ```sql
    ALTER SYSTEM MODIFY BACKEND "id1" SET ("disable_load" = "true");
    ````

### Keywords

    ALTER, SYSTEM, ADD, BACKEND, ALTER SYSTEM

### Best Practice

