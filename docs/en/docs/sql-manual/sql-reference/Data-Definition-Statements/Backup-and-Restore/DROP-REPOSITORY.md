---
{
    "title": "DROP-REPOSITORY",
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

## DROP-REPOSITORY

### Name

DROP REPOSITORY

### Description

This statement is used to delete a created repository. Only root or superuser users can delete repositories.

grammar:

```sql
DROP REPOSITORY `repo_name`;
```

illustrate:

- Deleting a warehouse just deletes the warehouse's mapping in Palo, not the actual warehouse data. Once deleted, it can be mapped to the repository again by specifying the same broker and LOCATION.

### Example

1. Delete the repository named bos_repo:

```sql
DROP REPOSITORY `bos_repo`;
```

### Keywords

     DROP, REPOSITORY

### Best Practice
