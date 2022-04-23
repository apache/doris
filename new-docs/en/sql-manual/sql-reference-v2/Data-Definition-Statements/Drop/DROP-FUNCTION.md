---
{
    "title": "DROP-FUNCTION",
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

## DROP-FUNCTION

### Name

DROP FUNCTION

### Description

Delete a custom function. Function names and parameter types are exactly the same to be deleted.

grammar:

```sql
DROP FUNCTION function_name
     (arg_type [, ...])
````

Parameter Description:

- `function_name`: the name of the function to delete
- `arg_type`: the argument list of the function to delete

### Example

1. Delete a function

    ```sql
    DROP FUNCTION my_add(INT, INT)
    ````

### Keywords

     DROP, FUNCTION

### Best Practice
