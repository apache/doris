---
{
    "title": "SHOW-PARTITION-ID",
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

## SHOW-PARTITION-ID

### Name

SHOW PARTITION ID

### Description

This statement is used to find the corresponding database name, table name, partition name according to the partition id (only for administrators)

  grammar:

```sql
SHOW PARTITION [partition_id]
```
### Example

1. Find the corresponding database name, table name, partition name according to the partition id

    ```sql
    SHOW PARTITION 10002;
    ````

### Keywords

    SHOW, PARTITION, ID

### Best Practice

