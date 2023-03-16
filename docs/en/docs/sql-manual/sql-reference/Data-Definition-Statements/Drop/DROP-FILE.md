---
{
    "title": "DROP-FILE",
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

## DROP-FILE

### Name

DROP FILE

### Description

This statement is used to delete an uploaded file.

grammar:

```sql
DROP FILE "file_name" [FROM database]
[properties]
````

illustrate:

- file_name: file name.
- database: a db to which the file belongs, if not specified, the db of the current session is used.
- properties supports the following parameters:
   - `catalog`: Required. The category the file belongs to.

### Example

1. Delete the file ca.pem

     ```sql
     DROP FILE "ca.pem" properties("catalog" = "kafka");
     ````

### Keywords

     DROP, FILE

### Best Practice
