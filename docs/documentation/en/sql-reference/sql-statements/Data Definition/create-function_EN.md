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

>` AGGREGATE `: If this is the case, it means that the created function is an aggregate function, otherwise it is a scalar function.
>
>` Function_name': To create the name of the function, you can include the name of the database. For example: `db1.my_func'.
>
>` arg_type': The parameter type of the function is the same as the type defined at the time of table building. Variable-length parameters can be represented by `,...'. If it is a variable-length type, the type of the variable-length part of the parameters is the same as the last non-variable-length parameter type.
>
>` ret_type': Function return type.
>
>` Inter_type': A data type used to represent the intermediate stage of an aggregate function.
>
>` properties `: Used to set properties related to this function. Properties that can be set include
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


This statement creates a custom function. Executing this command requires that the user have `ADMIN'privileges.

If the `function_name'contains the database name, the custom function will be created in the corresponding database, otherwise the function will be created in the database where the current session is located. The name and parameters of the new function cannot be the same as functions already existing in the current namespace, otherwise the creation will fail. But only with the same name and different parameters can the creation be successful.

## example

1. Create a custom scalar function

```
CREATE FUNCTION my_add(INT, INT) RETURNS INT PROPERTIES (
"Symbol"=""\\\\\\\\ zn9doris\\\ udf6addudfepns\\ FunctionContexterkns\\ INTVales 4\,
"object file" ="http://host:port /libmyadd.so"
);
```

2. Create a custom aggregation function

```
CREATE AGGREGATE FUNCTION my_count (BIGINT) RETURNS BIGINT PROPERTIES (
"init u fn"= "ZN9doris, udf9CountInitEPNS -u 15FunctionContextEPNS, u 9BigIntValE",
"Update  fn" = " zn9doris \ udf11Countupdateepns \ \ FunctionContexterkns \ Intvalepns  bigintvale",
"Merge fn"="\ zn9doris\\ udf10CountMergeepns\ \ FunctionContexterkns\ Bigintvaleps2\\\\\\\\\\\\\
"Finalize \ fn" = "\ zn9doris \ udf13Count Finalizepns \\ FunctionContexterkns \ Bigintvale",
"object" file ="http://host:port /libudasample.so"
);
```
##keyword
CREATE,FUNCTION
