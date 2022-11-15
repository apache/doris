---
{
    "title": "ADMIN-CHECK-TABLET",
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

## ADMIN-CHECK-TABLET

### Name

ADMIN CHECK TABLET

### Description

This statement is used to perform the specified check operation on a set of tablets.

grammar:

```sql
ADMIN CHECK TABLE (tablet_id1, tablet_id2, ...) 
PROPERTIES("type" = "...");
```

illustrate:

1. A list of tablet ids must be specified along with the type property in PROPERTIES.
2. Type only supports:

    * consistency: Check the consistency of the replica of the tablet. This command is an asynchronous command. After sending, Doris will start to execute the consistency check job of the corresponding tablet. The final result will be reflected in the InconsistentTabletNum column in the result of `SHOW PROC "/cluster_health/tablet_health";`.


### Example

1. Perform a replica data consistency check on a specified set of tablets.

    ```
    ADMIN CHECK TABLET (10000, 10001) 
   PROPERTIES("type" = "consistency");
   ```

### Keywords

    ADMIN, CHECK, TABLET

### Best Practice


