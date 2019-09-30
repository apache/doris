# get_json_string
## description
### Syntax

'VARCHAR get'u string (VARCHAR json str, VARCHAR json path)


Parse and retrieve the string content of the specified path in the JSON string.
Where json_path must start with the $symbol and use. as the path splitter. If the path contains..., double quotation marks can be used to surround it.
Use [] to denote array subscripts, starting at 0.
The content of path cannot contain ",[and].
If the json_string format is incorrect, or the json_path format is incorrect, or matches cannot be found, NULL is returned.

## example

1. Get the value of key as "k1"

```
mysql > SELECT get a json string ('{"k1":"v1", "k2":"v2"}, "$.k1");
+---------------------------------------------------+
Get json string ('{"k1":"v1", "k2":"v2"}','$.k1')'124get;
+---------------------------------------------------+
1.2.2.1.;
+---------------------------------------------------+
```

2. Get the second element of the array whose key is "my. key"

```
mysql > SELECT get u json string ('{"k1":"v1", "my.key":["e1", "e2", "e3"]}','$."my.key"[1]);
+------------------------------------------------------------------------------+
Get json string ('{"k1":"v1", "my.key":["e1", "e2", "e3"]}','$"my.key"[1]])'1244;
+------------------------------------------------------------------------------+
1.2.2.;2.;
+------------------------------------------------------------------------------+
```

3. Get the first element in an array whose secondary path is k1. key - > K2
```
mysql > SELECT get u json string ('{"k1.key":{"k2":["v1", "v2"]}}','$."k1.key".k2 [0]);
+-----------------------------------------------------------------------+
Get json string ('{"k1.key":{"k2":["v1", "v2"]}','$"k1.key.k2 [0]])'124s;
+-----------------------------------------------------------------------+
1.2.2.1.;
+-----------------------------------------------------------------------+
```

4. Get all the values in the array where the key is "k1"
```
mysql > SELECT get u json string ('["k1":"v1"}, {"k2":"v2"}, {"k1":"v3"}, {"k1":"v4"}],'$.k1');
+---------------------------------------------------------------------------------+
Get your string ('[{"k1":"v1"}, {"k2":"v2"}, {"k1":"v3"}, {"k1":"v3"}, {"k1":"v4"}]],'$.k1') 1244;
+---------------------------------------------------------------------------------+
124; ["v1","v3","v4"].124
+---------------------------------------------------------------------------------+
```
##keyword
GET_JSON_STRING,GET,JSON,STRING
