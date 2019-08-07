# weekofyear

## Syntax

`INT WEEKOFYEAR(DATETIME date)`

## Description


获得一年中的第几周

参数为Date或者Datetime类型

## Examples

```
mysql> select weekofyear('2008-02-20 00:00:00');
+-----------------------------------+
| weekofyear('2008-02-20 00:00:00') |
+-----------------------------------+
|                                 8 |
+-----------------------------------+
```