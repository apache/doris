# find_in_set
## description
### Syntax

`INT find_in_set(VARCHAR str, VARCHAR strlist)`


返回 strlist 中第一次出现 str 的位置（从1开始计数）。strlist 是用逗号分隔的字符串。如果没有找到，返回0。任意参数为 NULL ，返回 NULL。

## example

```
mysql> select find_in_set("b", "a,b,c");
+---------------------------+
| find_in_set('b', 'a,b,c') |
+---------------------------+
|                         2 |
+---------------------------+
```
##keyword
FIND_IN_SET,FIND,IN,SET
