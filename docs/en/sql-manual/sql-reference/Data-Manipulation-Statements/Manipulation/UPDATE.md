---
{
    "title": "UPDATE",
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

## UPDATE

### Name

UPDATE

### Description

This statement is used to update the data (the update statement currently only supports the Unique Key model).

```sql
UPDATE table_name
    SET assignment_list
    WHERE expression

value:
    {expr | DEFAULT}

assignment:
    col_name = value

assignment_list:
    assignment [, assignment] ...
````

 Parameters

+ table_name: The target table of the data to be updated. Can be of the form 'db_name.table_name'
+ assignment_list: The target column to be updated, in the format 'col_name = value, col_name = value'
+ where expression: the condition that is expected to be updated, an expression that returns true or false can be

 Note

The current UPDATE statement only supports row updates on the Unique model, and there may be data conflicts caused by concurrent updates.
At present, Doris does not deal with such problems, and users need to avoid such problems from the business side.

### Example

The `test` table is a unique model table, which contains four columns: k1, k2, v1, v2. Where k1, k2 are keys, v1, v2 are values, and the aggregation method is Replace.

1. Update the v1 column in the 'test' table that satisfies the conditions k1 =1 , k2 =2 to 1

```sql
UPDATE test SET v1 = 1 WHERE k1=1 and k2=2;
````

2. Increment the v1 column of the k1=1 column in the 'test' table by 1

```sql
UPDATE test SET v1 = v1+1 WHERE k1=1;
````

### Keywords

    UPDATE

### Best Practice

