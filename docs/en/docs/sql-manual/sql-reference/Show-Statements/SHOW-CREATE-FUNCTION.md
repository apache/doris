---
{
    "title": "SHOW-CREATE-FUNCTION",
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

## SHOW-CREATE-FUNCTION

### Name

SHOW CREATE FUNCTION

### Description

This statement is used to display the creation statement of the user-defined function

grammar:

```sql
SHOW CREATE [GLOBAL] FUNCTION function_name(arg_type [, ...]) [FROM db_name]];
````

illustrate:
1. `global`: The show function is global 
2. `function_name`: The name of the function to display
3. `arg_type`: The parameter list of the function to display
4. If db_name is not specified, the current default db is used

**Note: the "global" keyword is only available after v2.0**

### Example

1. Show the creation statement of the specified function under the default db

    ```sql
    SHOW CREATE FUNCTION my_add(INT, INT)
    ````
2. Show the creation statement of the specified global function

    ```sql
    SHOW CREATE GLOBAL FUNCTION my_add(INT, INT)
    ````

### Keywords

    SHOW, CREATE, FUNCTION

### Best Practice

