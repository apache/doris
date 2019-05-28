# `ST_Point`

## Syntax

`POINT ST_Point(DOUBLE x, DOUBLE y)`

## Description

通过给定的X坐标值，Y坐标值返回对应的Point。
当前这个值只是在球面集合上有意义，X/Y对应的是经度/纬度(longitude/latitude)

## Examples

```
mysql> SELECT ST_AsText(ST_Point(24.7, 56.7));
+---------------------------------+
| st_astext(st_point(24.7, 56.7)) |
+---------------------------------+
| POINT (24.7 56.7)               |
+---------------------------------+
```
