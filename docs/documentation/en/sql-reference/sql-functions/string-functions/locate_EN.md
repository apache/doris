# locate
## Description
### Syntax

'INT LOCATION (WARCHAR substrate, WARCHAR str [, INT pos]]'


Returns where substr appears in str (counting from 1). If the third parameter POS is specified, the position where substr appears is found from the string where STR starts with POS subscript. If not found, return 0

## example

```
mysql> SELECT LOCATE('bar', 'foobarbar');
+----------------------------+
| locate('bar', 'foobarbar') |
+----------------------------+
|                          4 |
+----------------------------+

mysql> SELECT LOCATE('xbar', 'foobar');
+--------------------------+
| locate('xbar', 'foobar') |
+--------------------------+
|                        0 |
+--------------------------+

mysql > SELECT LOCATE ('bar','foobarbar', 5);
+-------------------------------+
Location ('bar','foobarbar', 5)'124s;
+-------------------------------+
|                             7 |
+-------------------------------+
```
##keyword
LOCATE
