---
{
    "title": "SET-VARIABLE",
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

## SET-VARIABLE

### Name

SET VARIABLE

### Description

This statement is mainly used to modify Doris system variables. These system variables can be modified at the global and session level, and some can also be modified dynamically. You can also view these system variables with `SHOW VARIABLE`.

grammar:

```sql
SET variable_assignment [, variable_assignment] ...
````

illustrate:

1. variable_assignment:
         user_var_name = expr
       | [GLOBAL | SESSION] system_var_name = expr

> Note:
>
> 1. Only ADMIN users can set variables to take effect globally
> 2. The globally effective variable affects the current session and new sessions thereafter, but does not affect other sessions that currently exist.

### Example

1. Set the time zone to Dongba District

   ````
   SET time_zone = "Asia/Shanghai";
   ````

2. Set the global execution memory size

   ````
   SET GLOBAL exec_mem_limit = 137438953472
   ````

### Keywords

    SET, VARIABLE

