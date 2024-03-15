---
{
    "title": "UNSET-VARIABLE",
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

<version since="dev">

## UNSET-VARIABLE

</version>

### Name

UNSET VARIABLE

### Description

This statement is used to restore Doris system variables. These system variables can be modified at global or session level.

grammar:

```sql
UNSET [SESSION|GLOBAL] VARIABLE (variable_name | ALL)
````

illustrate:

1. (variable_name | ALL): statement must be ended with a variable name or keyword `ALL`.

> Note:
>
> 1. Only ADMIN users can unset variables to take effect globally
> 2. When restore a variable with `GLOBAL`,  it only affect your current using session and new open sessions. It does not affect other current open sessions.

### Example

1. Restore value of the time zone

   ````
   UNSET VARIABLE time_zone;
   ````

2. Restore the global execution memory size

   ````
   UNSET GLOBAL VARIABLE exec_mem_limit;
   ````
3. Restore all variables globally

   ```
   UNSET GLOBAL VARIABLE ALL;
   ```
### Keywords

    UNSET, VARIABLE

### Best Practice

