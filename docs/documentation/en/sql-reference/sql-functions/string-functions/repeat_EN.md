# repeat
Description
'35;'35;' 35; Syntax

'VARCHAR repeat (VARCHAR str, INT count)


Repeat the str of the string count times, return empty string when count is less than 1, return NULL when str, count is any NULL

'35;'35; example

```
mysql> SELECT repeat("a", 3);
+----------------+
repeat ('a', 3)'1244;
+----------------+
| aaa            |
+----------------+

mysql> SELECT repeat("a", -1);
+-----------------+
repeat ('a', -1) 1244;
+-----------------+
|                 |
+-----------------+
```
##keyword
REPEAT,
