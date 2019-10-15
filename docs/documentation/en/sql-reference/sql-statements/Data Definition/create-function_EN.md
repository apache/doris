# CREATE FUNCTION
##Description
### Syntax

```
CREATE [AGGREGATE] FUNCTION function_name
	(angry type [...])
	RETURNS ret_type
	[INTERMEDIATE inter_type]
	[PROPERTIES ("key" = "value" [, ...]) ]
```

### Parameters

>`AGGREGATE`: If this is the case, it means that the created function is an aggregate function, otherwise it is a scalar function.
>
>`Function_name`: To create the name of the function, you can include the name of the database. For example: `db1.my_func'.
>
>` arg_type': The parameter type of the function is the same as the type defined at the time of table building. Variable-length parameters can be represented by `,...`. If it is a variable-length type, the type of the variable-length part of the parameters is the same as the last non-variable-length parameter type.
>
>`ret_type`: Function return type.
>
>`Inter_type`: A data type used to represent the intermediate stage of an aggregate function.
>
>`properties`: Used to set properties related to this function. Properties that can be set include
>
> "Object_file": Custom function dynamic library URL path, currently only supports HTTP/HTTPS protocol, this path needs to remain valid throughout the life cycle of the function. This option is mandatory
>
> "symbol": Function signature of scalar functions for finding function entries from dynamic libraries. This option is mandatory for scalar functions
>
> "init_fn": Initialization function signature of aggregate function. Necessary for aggregation functions
>
> "update_fn": Update function signature of aggregate function. Necessary for aggregation functions
>
> "merge_fn": Merge function signature of aggregate function. Necessary for aggregation functions
>
> "serialize_fn": Serialized function signature of aggregate function. For aggregation functions, it is optional, and if not specified, the default serialization function will be used
>
> "finalize_fn": A function signature that aggregates functions to obtain the final result. For aggregation functions, it is optional. If not specified, the default fetch result function will be used.
>
> "md5": The MD5 value of the function dynamic link library, which is used to verify that the downloaded content is correct. This option is optional
>
> "prepare_fn": Function signature of the prepare function for finding the entry from the dynamic library. This option is optional for custom functions
> 
> "close_fn": Function signature of the close function for finding the entry from the dynamic library. This option is optional for custom functions


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
		"init_fn"= "_ZN9doris_udf6AddUdfEPNS_15FunctionContextERKNS_6IntValES4_",
		"update_fn" = "zn9dorisudf11CountupdateepnsFunctionContexterknsIntvalepnsbigintvale",
		"merge_fn" = "zn9dorisudf10CountMergeepnsFunctionContexterknsBigintvaleps2
		"finalize_fn" = "zn9dorisudf13CountFinalizepnsFunctionContexterknsBigintvale",
		"object_file" = "http://host:port/libudasample.so"
	);
	```
##keyword
CREATE,FUNCTION
