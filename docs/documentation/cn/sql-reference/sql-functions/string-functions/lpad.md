# lpad

## Syntax

`VARCHAR lpad(VARCHAR str, INT len, VARCHAR pad)`

## Description

返回 str 中长度为 len（从首字母开始算起）的字符串。如果 len 大于 str 的长度，则在 str 的前面不断补充 pad 字符，直到该字符串的长度达到 len 为止。如果 len 小于 str 的长度，该函数相当于截断 str 字符串，只返回长度为 len 的字符串。

## Examples

```
mysql> SELECT lpad("hi", 5, "xy");
+---------------------+
| lpad('hi', 5, 'xy') |
+---------------------+
| xyxhi               |
+---------------------+

mysql> SELECT lpad("hi", 1, "xy");
+---------------------+
| lpad('hi', 1, 'xy') |
+---------------------+
| h                   |
+---------------------+
```
