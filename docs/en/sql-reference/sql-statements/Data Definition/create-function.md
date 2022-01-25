---
{
    "title": "CREATE FUNCTION",
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

# CREATE FUNCTION
## Description
### Syntax

```
CREATE [AGGREGATE] [ALIAS] FUNCTION function_name
    (arg_type [, ...])
    [RETURNS ret_type]
    [INTERMEDIATE inter_type]
    [WITH PARAMETER(param [,...]) AS origin_function]
    [PROPERTIES ("key" = "value" [, ...]) ]
```

### Parameters

> `AGGREGATE`: If this is the case, it means that the created function is an aggregate function.
>
> `ALIAS`: If this is the case, it means that the created function is an alias function.
> 
> If the above two items are not present, it means that the created function is a scalar function.
> 
> `Function_name`: To create the name of the function, you can include the name of the database. For example: `db1.my_func'.
>
> `arg_type`: The parameter type of the function is the same as the type defined at the time of table building. Variable-length parameters can be represented by `,...`. If it is a variable-length type, the type of the variable-length part of the parameters is the same as the last non-variable-length parameter type.  
> **NOTICE**: `ALIAS FUNCTION` variable-length parameters are not supported, and there is at least one parameter. In particular, the type `ALL` refers to any data type and can only be used for `ALIAS FUNCTION`.
> 
> `ret_type`: Required for creating a new function. This parameter is not required if you are aliasing an existing function.
>
> `inter_type`: A data type used to represent the intermediate stage of an aggregate function.
> 
> `param`: The parameter used to represent the alias function, containing at least one.
> 
> `origin_function`: Used to represent the original function corresponding to the alias function.
>
> `properties`: Used to set properties related to aggregate function and scalar function. Properties that can be set include
>
>           "Object_file": Custom function dynamic library URL path, currently only supports HTTP/HTTPS protocol, this path needs to remain valid throughout the life cycle of the function. This option is mandatory
>
>           "symbol": Function signature of scalar functions for finding function entries from dynamic libraries. This option is mandatory for scalar functions
>
>           "init_fn": Initialization function signature of aggregate function. Necessary for aggregation functions
>
>           "update_fn": Update function signature of aggregate function. Necessary for aggregation functions
>
>           "merge_fn": Merge function signature of aggregate function. Necessary for aggregation functions
>
>           "serialize_fn": Serialized function signature of aggregate function. For aggregation functions, it is optional, and if not specified, the default serialization function will be used
>
>           "finalize_fn": A function signature that aggregates functions to obtain the final result. For aggregation functions, it is optional. If not specified, the default fetch result function will be used.
>
>           "md5": The MD5 value of the function dynamic link library, which is used to verify that the downloaded content is correct. This option is optional
>
>           "prepare_fn": Function signature of the prepare function for finding the entry from the dynamic library. This option is optional for custom functions
> 
>           "close_fn": Function signature of the close function for finding the entry from the dynamic library. This option is optional for custom functions


This statement creates a custom function. Executing this command requires that the user have `ADMIN` privileges.

If the `function_name` contains the database name, the custom function will be created in the corresponding database, otherwise the function will be created in the database where the current session is located. The name and parameters of the new function cannot be the same as functions already existing in the current namespace, otherwise the creation will fail. But only with the same name and different parameters can the creation be successful.

## example

1. Create a custom scalar function

	```
	CREATE FUNCTION my_add(INT, INT) RETURNS INT PROPERTIES (
		"symbol" = "_ZN9doris_udf6AddUdfEPNS_15FunctionContextERKNS_6IntValES4_",
		"object_file" ="http://host:port/libmyadd.so"
	);
	```
2. Create a custom scalar function with prepare/close functions

	```
	CREATE FUNCTION my_add(INT, INT) RETURNS INT PROPERTIES (
   		"symbol" = 	"_ZN9doris_udf6AddUdfEPNS_15FunctionContextERKNS_6IntValES4_",
   		"prepare_fn" = "_ZN9doris_udf14AddUdf_prepareEPNS_15FunctionContextENS0_18FunctionStateScopeE",
   		"close_fn" = "_ZN9doris_udf12AddUdf_closeEPNS_15FunctionContextENS0_18FunctionStateScopeE",
    	"object_file" = "http://host:port/libmyadd.so"
	);
	```

3. Create a custom aggregation function
	
	```
	CREATE AGGREGATE FUNCTION my_count (BIGINT) RETURNS BIGINT PROPERTIES (
	    "init_fn"="_ZN9doris_udf9CountInitEPNS_15FunctionContextEPNS_9BigIntValE",
	    "update_fn"="_ZN9doris_udf11CountUpdateEPNS_15FunctionContextERKNS_6IntValEPNS_9BigIntValE",
	    "merge_fn"="_ZN9doris_udf10CountMergeEPNS_15FunctionContextERKNS_9BigIntValEPS2_",
	    "finalize_fn"="_ZN9doris_udf13CountFinalizeEPNS_15FunctionContextERKNS_9BigIntValE",
	    "object_file"="http://host:port/libudasample.so"
	);
	```

4.  Create a scalar function with variable length parameters

    ```
    CREATE FUNCTION strconcat(varchar, ...) RETURNS varchar properties (
        "symbol" = "_ZN9doris_udf6StrConcatUdfEPNS_15FunctionContextERKNS_6IntValES4_",
        "object_file" = "http://host:port/libmyStrConcat.so"
    );
    ```

5. Create a custom alias function

    ```
    -- create a custom functional alias function
    CREATE ALIAS FUNCTION id_masking(BIGINT) WITH PARAMETER(id) 
        AS CONCAT(LEFT(id, 3), '****', RIGHT(id, 4));

    -- create a custom cast alias function
    CREATE ALIAS FUNCTION string(ALL, INT) WITH PARAMETER(col, length) 
        AS CAST(col AS varchar(length));
    ```

## keyword
CREATE,FUNCTION
