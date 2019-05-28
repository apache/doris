# ascii

## Syntax

`INT ascii(VARCHAR str)`

## Description

返回字符串第一个字符对应的 ascii 码

## Examples

```
mysql> select ascii('1');
+------------+
| ascii('1') |
+------------+
|         49 |
+------------+

mysql> select ascii('234');
+--------------+
| ascii('234') |
+--------------+
|           50 |
+--------------+
```
