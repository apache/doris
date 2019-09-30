# group_concat
## description
### Syntax

`VARCHAR group_concat(VARCHAR str[, VARCHAR sep])`


This function is an aggregation function similar to sum (), and group_concat links multiple rows of results in the result set to a string. The second parameter, sep, is a connection symbol between strings, which can be omitted. This function usually needs to be used with group by statements.

## example

```
mysql> select value from test;
+-------+
| value |
+-------+
(a)'124;
(b)'1244;
(c)'1244;
+-------+

mysql> select group_concat(value) from test;
+-----------------------+
| group_concat(`value`) |
+-----------------------+
124a, b, c, 124a;
+-----------------------+

mysql> select group_concat(value, " ") from test;
+----------------------------+
| group_concat(`value`, ' ') |
+----------------------------+
| a b c                      |
+----------------------------+
```
##keyword
GROUP_CONCAT,GROUP,CONCAT
