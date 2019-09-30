# length
## Description
### Syntax

'INT length (VARCHAR str)'


Returns the length of the string and the number of characters returned for multi-byte characters. For example, five two-byte width words return a length of 10.

## example

```
mysql> select length("abc");
+---------------+
length ('abc') 1244;
+---------------+
|             3 |
+---------------+

mysql> select length("中国");
+------------------+
| length('中国')   |
+------------------+
|                6 |
+------------------+
```
##keyword
LENGTH
