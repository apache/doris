---
{
    "title": "SHOW-FUNCTIONS",
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

## SHOW-FUNCTIONS

### Name

SHOW FUNCTIONS

### Description

View all custom (system-provided) functions under the database. If the user specifies a database, then view the corresponding database, otherwise directly query the database where the current session is located

Requires `SHOW` permission on this database

grammar

```sql
SHOW [FULL] [BUILTIN] FUNCTIONS [IN|FROM db] [LIKE 'function_pattern']
````

 Parameters

>`full`: Indicates the detailed information of the display function
>`builtin`: Indicates the functions provided by the display system
>`db`: database name to query
>`function_pattern`: parameter used to filter function names

### Example

````sql
mysql> show full functions in testDb\G
**************************** 1. row ******************** ******
        Signature: my_add(INT,INT)
      Return Type: INT
    Function Type: Scalar
Intermediate Type: NULL
       Properties: {"symbol":"_ZN9doris_udf6AddUdfEPNS_15FunctionContextERKNS_6IntValES4_","object_file":"http://host:port/libudfsample.so","md5":"cfe7a362d10f3aaf6c49974ee0f1f878"}
**************************** 2. row ******************** ******
        Signature: my_count(BIGINT)
      Return Type: BIGINT
    Function Type: Aggregate
Intermediate Type: NULL
       Properties: { "object_file": "http: // host: port / libudasample.so", "finalize_fn": "_ZN9doris_udf13CountFinalizeEPNS_15FunctionContextERKNS_9BigIntValE", "init_fn": "_ZN9doris_udf9CountInitEPNS_15FunctionContextEPNS_9BigIntValE", "merge_fn": "_ZN9doris_udf10CountMergeEPNS_15FunctionContextERKNS_9BigIntValEPS2_", "md5": " 37d185f80f95569e2676da3d5b5b9d2f","update_fn":"_ZN9doris_udf11CountUpdateEPNS_15FunctionContextERKNS_6IntValEPNS_9BigIntValE"}
**************************** 3. row ******************** ******
        Signature: id_masking(BIGINT)
      Return Type: VARCHAR
    Function Type: Alias
Intermediate Type: NULL
       Properties: {"parameter":"id","origin_function":"concat(left(`id`, 3), `****`, right(`id`, 4))"}

3 rows in set (0.00 sec)
mysql> show builtin functions in testDb like 'year%';
+---------------+
| Function Name |
+---------------+
| year          |
| years_add     |
| years_diff    |
| years_sub     |
+---------------+
2 rows in set (0.00 sec)
````

### Keywords

    SHOW, FUNCTIONS

### Best Practice

