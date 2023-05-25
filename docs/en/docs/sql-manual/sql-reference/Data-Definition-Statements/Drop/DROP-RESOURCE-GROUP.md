---
{
    "title": "DROP-RESOURCE-GROUP",
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

## DROP-RESOURCE-GROUP

### Name

DROP RESOURCE GROUP

<version since="dev"></version>

### Description

This statement is used to delete a resource group.

```sql
DROP RESOURCE GROUP [IF EXISTS] 'rg_name'
```

### Example

1. Delete the resource group named g1:
    
    ```sql
    drop resource group if exists g1;
    ```

### Keywords

    DROP, RESOURCE, GROUP

### Best Practice

