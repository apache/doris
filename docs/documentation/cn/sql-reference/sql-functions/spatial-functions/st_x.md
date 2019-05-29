# `ST_X`

## Syntax

`DOUBLE ST_X(POINT point)`

## Description

当point是一个合法的POINT类型时，返回对应的X坐标值

## Examples

```
mysql> SELECT ST_X(ST_Point(24.7, 56.7));
+----------------------------+
| st_x(st_point(24.7, 56.7)) |
+----------------------------+
|                       24.7 |
+----------------------------+
```
