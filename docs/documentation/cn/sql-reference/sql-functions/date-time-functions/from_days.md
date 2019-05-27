# from_days

## Syntax

`DATE FROM_DAYS(INT N)`

## Description

通过距离0000-01-01日的天数计算出哪一天

## Examples

```
mysql> select from_days(730669);
+-------------------+
| from_days(730669) |
+-------------------+
| 2000-07-03        |
+-------------------+
```