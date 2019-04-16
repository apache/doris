# length

## Syntax

`INT length(VARCHAR str)`

## Description

返回字符串的长度，对于多字节字符，返回的字符数。比如5个两字节宽度字，返回的长度是10。

## Examples

```
mysql> select length("abc");
+---------------+
| length('abc') |
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
