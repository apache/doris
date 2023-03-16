---
{
    "title": "SHOW-CREATE-ROUTINE-LOAD",
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

## SHOW-CREATE-ROUTINE-LOAD

### Name

SHOW CREATE ROUTINE LOAD

### Description

This statement is used to demonstrate the creation statement of a routine import job.

The kafka partition and offset in the result show the currently consumed partition and the corresponding offset to be consumed.

grammar:

```sql
SHOW [ALL] CREATE ROUTINE LOAD for load_name;
````

illustrate:

1. `ALL`: optional parameter, which means to get all jobs, including historical jobs
2. `load_name`: routine import job name

### Example

1. Show the creation statement of the specified routine import job under the default db

    ```sql
    SHOW CREATE ROUTINE LOAD for test_load
    ````

### Keywords

    SHOW, CREATE, ROUTINE, LOAD

### Best Practice

