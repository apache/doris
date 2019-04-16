# group_concat

## Syntax

`VARCHAR group_concat(VARCHAR str[, VARCHAR sep])`

## Description

该函数是类似于 sum() 的聚合函数，group_concat 将结果集中的多行结果连接成一个字符串。第二个参数 sep 为字符串之间的连接符号，该参数可以省略。该函数通常需要和 group by 语句一起使用。

## Examples

```
mysql> select value from test;
+-------+
| value |
+-------+
| a     |
| b     |
| c     |
+-------+

mysql> select group_concat(value) from test;
+-----------------------+
| group_concat(`value`) |
+-----------------------+
| a, b, c               |
+-----------------------+

mysql> select group_concat(value, " ") from test;
+----------------------------+
| group_concat(`value`, ' ') |
+----------------------------+
| a b c                      |
+----------------------------+
```
