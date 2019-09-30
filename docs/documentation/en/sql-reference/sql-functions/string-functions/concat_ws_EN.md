# Concat_ws
## Description
### Syntax

'VARCHAR concat ws (VARCHAR sep., VARCHAR str,...)'


Using the first parameter SEP as a connector, the second parameter and all subsequent parameters are spliced into a string.
If the separator is NULL, return NULL.
` The concat_ws` function does not skip empty strings, but NULL values.

## example

```
mysql> select concat_ws("or", "d", "is");
+----------------------------+
124concat ws (or','d','is') 124s;
+----------------------------+
1.2.2.2.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.
+----------------------------+

mysql> select concat_ws(NULL, "d", "is");
+----------------------------+
(NULL,'d','is') 1244;
+----------------------------+
No. No. No.
+----------------------------+

mysql > select concat ws ("or", "d", NULL,"is");
+---------------------------------+
Concat ws ("or", "d", NULL,"is").
+---------------------------------+
1.2.2.2.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.
+---------------------------------+
```
##keyword
CONCAT_WS,CONCAT,WS
