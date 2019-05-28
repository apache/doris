# locate

## Syntax

`INT locate(VARCHAR substr, VARCHAR str[, INT pos])`

## Description

返回 substr 在 str 中出现的位置（从1开始计数）。如果指定第3个参数 pos，则从 str 以 pos 下标开始的字符串处开始查找 substr 出现的位置。如果没有找到，返回0

## Examples

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

mysql> SELECT LOCATE('bar', 'foobarbar', 5);
+-------------------------------+
| locate('bar', 'foobarbar', 5) |
+-------------------------------+
|                             7 |
+-------------------------------+
```
