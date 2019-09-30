# split_part
## Description
### Syntax

'VARCHAR split party (VARCHAR content, VARCHAR delimiter, INT field)'


Returns the specified partition (counting from the beginning) by splitting the string according to the partitioner.

## example

```
mysql> select split_part("hello world", " ", 1);
+----------------------------------+
(hello world','1)'124split part';
+----------------------------------+
| hello                            |
+----------------------------------+


mysql> select split_part("hello world", " ", 2);
+----------------------------------+
(hello world','2)'124u;
+----------------------------------+
| world                             |
+----------------------------------+

mysql> select split_part("2019年7月8号", "月", 1);
+-----------------------------------------+
(2019726376;821495;','263761,') 1244;
+-----------------------------------------+
| July 2019|
+-----------------------------------------+

mysql> select split_part("abca", "a", 1);
+----------------------------+
split part ('abca','a', 1)
+----------------------------+
|                            |
+----------------------------+
```
##keyword
SPLIT_PART,SPLIT,PART
