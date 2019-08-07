# instr
## description
### Syntax

`INT instr(VARCHAR str, VARCHAR substr)`


返回 substr 在 str 中第一次出现的位置（从1开始计数）。如果 substr 不在 str 中出现，则返回0。

## example

```
mysql> select instr("abc", "b");
+-------------------+
| instr('abc', 'b') |
+-------------------+
|                 2 |
+-------------------+

mysql> select instr("abc", "d");
+-------------------+
| instr('abc', 'd') |
+-------------------+
|                 0 |
+-------------------+
```
##keyword
INSTR
