---
{
    "title": "CREATE-FUNCTION",
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

## CREATE-FUNCTION

### Name

CREATE FUNCTION

### Description

This statement creates a custom function. Executing this command requires the user to have `ADMIN` privileges.

If `function_name` contains the database name, then the custom function will be created in the corresponding database, otherwise the function will be created in the database where the current session is located. The name and parameters of the new function cannot be the same as the existing functions in the current namespace, otherwise the creation will fail. But only with the same name and different parameters can be created successfully.

grammar:

```sql
CREATE [AGGREGATE] [ALIAS] FUNCTION function_name
    (arg_type [, ...])
    [RETURNS ret_type]
    [INTERMEDIATE inter_type]
    [WITH PARAMETER(param [,...]) AS origin_function]
    [PROPERTIES ("key" = "value" [, ...]) ]
````

Parameter Description:

- `AGGREGATE`: If there is this item, it means that the created function is an aggregate function.


- `ALIAS`: If there is this item, it means that the created function is an alias function.


 If the above two items are absent, it means that the created function is a scalar function

- `function_name`: The name of the function to be created, which can include the name of the database. For example: `db1.my_func`.


- `arg_type`: The parameter type of the function, which is the same as the type defined when creating the table. Variable-length parameters can be represented by `, ...`. If it is a variable-length type, the type of the variable-length parameter is the same as that of the last non-variable-length parameter.

   **NOTE**: `ALIAS FUNCTION` does not support variable-length arguments and must have at least one argument.

- `ret_type`: Required for creating new functions. If you are aliasing an existing function, you do not need to fill in this parameter.


- `inter_type`: The data type used to represent the intermediate stage of the aggregation function.


- `param`: used to represent the parameter of the alias function, including at least one.


- `origin_function`: used to represent the original function corresponding to the alias function.

- `properties`: Used to set properties related to aggregate functions and scalar functions. The properties that can be set include:

  - `object_file`: The URL path of the custom function dynamic library. Currently, only HTTP/HTTPS protocol is supported. This path needs to remain valid for the entire life cycle of the function. This option is required

  - `symbol`: The function signature of the scalar function, which is used to find the function entry from the dynamic library. This option is required for scalar functions

  - `init_fn`: The initialization function signature of the aggregate function. Required for aggregate functions

  - `update_fn`: update function signature of aggregate function. Required for aggregate functions

  - `merge_fn`: Merge function signature of aggregate function. Required for aggregate functions

  - `serialize_fn`: Serialize function signature of aggregate function. Optional for aggregate functions, if not specified, the default serialization function will be used

  - `finalize_fn`: The function signature of the aggregate function to get the final result. Optional for aggregate functions, if not specified, the default get-result function will be used

  - `md5`: The MD5 value of the function dynamic link library, which is used to verify whether the downloaded content is correct. This option is optional

  - `prepare_fn`: The function signature of the prepare function of the custom function, which is used to find the prepare function entry from the dynamic library. This option is optional for custom functions

  - `close_fn`: The function signature of the close function of the custom function, which is used to find the close function entry from the dynamic library. This option is optional for custom functions

### Example

1. Create a custom scalar function

   ```sql
   CREATE FUNCTION my_add(INT, INT) RETURNS INT PROPERTIES (
   "symbol" = "_ZN9doris_udf6AddUdfEPNS_15FunctionContextERKNS_6IntValES4_",
   "object_file" = "http://host:port/libmyadd.so"
   );
   ````

2. Create a custom scalar function with prepare/close functions

   ```sql
   CREATE FUNCTION my_add(INT, INT) RETURNS INT PROPERTIES (
   "symbol" = "_ZN9doris_udf6AddUdfEPNS_15FunctionContextERKNS_6IntValES4_",
   "prepare_fn" = "_ZN9doris_udf14AddUdf_prepareEPNS_15FunctionContextENS0_18FunctionStateScopeE",
   "close_fn" = "_ZN9doris_udf12AddUdf_closeEPNS_15FunctionContextENS0_18FunctionStateScopeE",
   "object_file" = "http://host:port/libmyadd.so"
   );
   ````

3. Create a custom aggregate function

    ```sql
   CREATE AGGREGATE FUNCTION my_count (BIGINT) RETURNS BIGINT PROPERTIES (
            "init_fn"="_ZN9doris_udf9CountInitEPNS_15FunctionContextEPNS_9BigIntValE",
            "update_fn"="_ZN9doris_udf11CountUpdateEPNS_15FunctionContextERKNS_6IntValEPNS_9BigIntValE",
            "merge_fn"="_ZN9doris_udf10CountMergeEPNS_15FunctionContextERKNS_9BigIntValEPS2_",
            "finalize_fn"="_ZN9doris_udf13CountFinalizeEPNS_15FunctionContextERKNS_9BigIntValE",
            "object_file"="http://host:port/libudasample.so"
   );
   ````


4. Create a scalar function with variable length arguments

   ```sql
   CREATE FUNCTION strconcat(varchar, ...) RETURNS varchar properties (
   "symbol" = "_ZN9doris_udf6StrConcatUdfEPNS_15FunctionContextERKNS_6IntValES4_",
   "object_file" = "http://host:port/libmyStrConcat.so"
   );
   ````

5. Create a custom alias function

   ```sql
   CREATE ALIAS FUNCTION id_masking(INT) WITH PARAMETER(id) AS CONCAT(LEFT(id, 3), '****', RIGHT(id, 4));
   ````

### Keywords

    CREATE, FUNCTION

### Best Practice
