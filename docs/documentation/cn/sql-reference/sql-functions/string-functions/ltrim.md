# ltrim

## Syntax

`VARCHAR ltrim(VARCHAR str)`

## Description

将参数 str 中从开始部分连续出现的空格去掉

## Examples

```
mysql> SELECT ltrim('   ab d');
+------------------+
| ltrim('   ab d') |
+------------------+
| ab d             |
+------------------+
```
