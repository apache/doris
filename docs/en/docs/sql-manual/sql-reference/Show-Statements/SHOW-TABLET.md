---
{
    "title": "SHOW-TABLET",
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

## SHOW-TABLET

### Name

SHOW TABLET

### Description

This statement is used to display the specified tablet id information (only for administrators)

grammar:

```sql
SHOW TABLET tablet_id
````

### Example

1. Display the parent-level id information of the tablet with the specified tablet id of 10000

    ```sql
    SHOW TABLET 10000;
    ````

### Keywords

    SHOW, TABLET

### Best Practice

