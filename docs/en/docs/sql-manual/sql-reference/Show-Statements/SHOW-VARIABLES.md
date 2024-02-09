---
{
    "title": "SHOW-VARIABLES",
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

## SHOW-VARIABLES

### Name

SHOW VARIABLES

### Description

This statement is used to display Doris system variables, which can be queried by conditions

grammar:

````sql
SHOW [GLOBAL | SESSION] VARIABLES
     [LIKE 'pattern' | WHERE expr]
````

illustrate:

- show variables is mainly used to view the values of system variables.
- Executing the SHOW VARIABLES command does not require any privileges, it only requires being able to connect to the server.
- Use the like statement to match with variable_name.
- The % percent wildcard can be used anywhere in the matching pattern

### Example

1. The default here is to match the Variable_name, here is the exact match

    ```sql
    show variables like 'max_connections';
    ````

2. Matching through the percent sign (%) wildcard can match multiple items

    ```sql
    show variables like '%connec%';
    ````

3. Use the Where clause for matching queries

    ```sql
    show variables where variable_name = 'version';
    ````

4. Use where to list variables whose value changed

    ```sql
    show variables where changed = 1;
    ```

In addition, all variables seen through "show variables" can be queried through table "information_schema.session_variables".

### Keywords

    SHOW, VARIABLES

### Best Practice

