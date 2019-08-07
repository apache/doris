# ltrim
## description
### Syntax

`VARCHAR ltrim(VARCHAR str)`


将参数 str 中从开始部分连续出现的空格去掉

## example

```
mysql> SELECT ltrim('   ab d');
+------------------+
| ltrim('   ab d') |
+------------------+
| ab d             |
+------------------+
```
##keyword
LTRIM
