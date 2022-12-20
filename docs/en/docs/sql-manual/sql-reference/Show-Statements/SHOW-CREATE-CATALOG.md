---
{
    "title": "SHOW-CREATE-CATALOG",
    "language": "zh-CN"
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

## SHOW-CREATE-CATALOG

### Name

<version since="1.2">

SHOW CREATE CATALOG

</version>

### Description

This statement shows the creating statement of a doris catalog.

grammar:

```sql
SHOW CREATE CATALOG catalog_name;
```

illustrate:
- `catalog_name`: The name of the catalog which exist in doris.

### Example

1. View the creating statement of the hive catalog in doris

   ```sql
   SHOW CREATE CATALOG hive;
   ```

### Keywords

    SHOW, CREATE, CATALOG

### Best Practice

