---
{
    "title": "SHOW-CONVERT-LIGHT-SCHEMA-CHANGE-PROCESS",
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

## SHOW-CONVERT-LIGHT-SCHEMA-CHANGE-PROCESS

### Name

SHOW CONVERT LIGHT SCHEMA CHANGE PROCESS

### Description

This statement is used to show the process of converting light schema change process. should enable config `enable_convert_light_weight_schema_change`.

grammar:

```sql
SHOW CONVERT_LIGHT_SCHEMA_CHANGE_PROCESS [FROM db]
```

### Example

1. View the converting process in db named test 

    ```sql
     SHOW CONVERT_LIGHT_SCHEMA_CHANGE_PROCESS FROM test;
    ````

2. View the converting process globally

    ```sql
    SHOW CONVERT_LIGHT_SCHEMA_CHANGE_PROCESS;
    ```


### Keywords

    SHOW, CONVERT_LIGHT_SCHEMA_CHANGE_PROCESS

### Best Practice