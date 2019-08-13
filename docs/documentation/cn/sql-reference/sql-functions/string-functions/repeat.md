# repeat
## description
### Syntax

`VARCHAR repeat(VARCHAR str, INT count)


将字符串 str 重复 count 次输出，count 小于1时返回空串，str，count 任一为NULL时，返回 NULL

## example

```
mysql> SELECT repeat("a", 3);
+----------------+
| repeat('a', 3) |
+----------------+
| aaa            |
+----------------+

mysql> SELECT repeat("a", -1);
+-----------------+
| repeat('a', -1) |
+-----------------+
|                 |
+-----------------+
```
##keyword
REPEAT,
