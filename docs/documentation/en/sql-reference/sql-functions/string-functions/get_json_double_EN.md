# get_json_double
## description
### Syntax

`DOUBLE get_json_double(VARCHAR json_str, VARCHAR json_path)


Parse and get the floating-point content of the specified path in the JSON string.
Where json_path must start with the $symbol and use. as the path splitter. If the path contains..., double quotation marks can be used to surround it.
Use [] to denote array subscripts, starting at 0.
The content of path cannot contain ",[and].
If the json_string format is incorrect, or the json_path format is incorrect, or matches cannot be found, NULL is returned.

## example

1. Get the value of key as "k1"

```
mysql > SELECT get'u json 'double ('{"k1":1.3, "k2":"2"}, "$.k1");
+-------------------------------------------------+
Get double ('{"k1":1.3, "k2":"2"},'$.k1')'124get;
+-------------------------------------------------+
|                                             1.3 |
+-------------------------------------------------+
```

2. Get the second element of the array whose key is "my. key"

```
mysql > SELECT get'u json 'double ('{"k1":"v1", "my.key":[1.1, 2.2, 3.3]}','$"my.key"[1]);
+---------------------------------------------------------------------------+
Get me a double ('{"k1":"v1", "my.key":[1.1, 2.2, 3.3]}','$"my.key"[1]])'124;
+---------------------------------------------------------------------------+
|                                                                       2.2 |
+---------------------------------------------------------------------------+
```

3. Get the first element in an array whose secondary path is k1. key - > K2
```
mysql > SELECT get'u json 'double ('{"k1.key":{"k2":[1.1, 2.2]}}','$."k1.key".k2 [0]);
+---------------------------------------------------------------------+
Get double ('{"k1.key":{"k2":[1.1, 2.2]}}','$"k1.key.k2 [0]])'124;
+---------------------------------------------------------------------+
|                                                                 1.1 |
+---------------------------------------------------------------------+
```
##keyword
GET_JSON_DOUBLE,GET,JSON,DOUBLE
