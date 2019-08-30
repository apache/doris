# get_json_int
## Description
### Syntax

'I don't get you int (VARCHAR is on, VARCHAR is on the path)


Parse and retrieve the integer content of the specified path in the JSON string.
Where json_path must start with the $symbol and use. as the path splitter. If the path contains..., double quotation marks can be used to surround it.
Use [] to denote array subscripts, starting at 0.
The content of path cannot contain ",[and].
If the json_string format is incorrect, or the json_path format is incorrect, or matches cannot be found, NULL is returned.

## example

1. Get the value of key as "k1"

```
mysql > SELECT get json u int ('{"k1":1, "k2":"2"}, "$.k1");
+--------------------------------------------+
Get it on int ('{"k1":1, "k2":"2"},'$.k1') 124s;
+--------------------------------------------+
|                                          1 |
+--------------------------------------------+
```

2. Get the second element of the array whose key is "my. key"

```
mysql > SELECT get u json u int ('{"k1":"v1", "my.key":[1, 2, 3]}','$"my.key"[1]);
+------------------------------------------------------------------+
Get me on int ('{"k1":"v1", "my.key":[1, 2, 3]}','$"my.key"[1]])'124;
+------------------------------------------------------------------+
|                                                                2 |
+------------------------------------------------------------------+
```

3. Get the first element in an array whose secondary path is k1. key - > K2
```
mysql > SELECT get u json u int ('{"k1.key":{"k2":[1, 2]}','$"k1.key".k2 [0]');
+--------------------------------------------------------------+
Get me on int ('{"k1.key":{"k2":[1, 2]}','$"k1.key".k2 [0]) 1244;
+--------------------------------------------------------------+
|                                                            1 |
+--------------------------------------------------------------+
```
##keyword
GET_JSON_INT,GET,JSON,INT
