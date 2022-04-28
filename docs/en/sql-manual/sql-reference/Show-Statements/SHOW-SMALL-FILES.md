---
{
    "title": "SHOW-SMALL-FILES",
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

## SHOW-SMALL-FILES

### Name

SHOW FILE

### Description

This statement is used to display files created by the CREATE FILE command within a database.

```sql
SHOW FILE [FROM database];
````

Return result description:

- FileId: file ID, globally unique
- DbName: the name of the database to which it belongs
- Catalog: Custom Category
- FileName: file name
- FileSize: file size in bytes
- MD5: MD5 of the file

### Example

1. View the uploaded files in the database my_database

    ```sql
    SHOW FILE FROM my_database;
    ````

### Keywords

    SHOW, SMALL, FILES

### Best Practice

