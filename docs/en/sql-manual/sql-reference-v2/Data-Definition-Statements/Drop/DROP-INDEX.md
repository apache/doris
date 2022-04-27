---
{
    "title": "DROP-INDEX",
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

## DROP-INDEX

### Name

DROP INDEX

### Description

This statement is used to delete the index of the specified name from a table. Currently, only bitmap indexes are supported.
grammar:

```sql
DROP INDEX [IF EXISTS] index_name ON [db_name.]table_name;
````

### Example

1. Delete the index

    ```sql
    CREATE INDEX [IF NOT EXISTS] index_name ON table1 ;
    ````

### Keywords

     DROP, INDEX

### Best Practice
