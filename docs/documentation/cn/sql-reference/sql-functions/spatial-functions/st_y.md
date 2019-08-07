# `ST_Y`
## description
### Syntax

`DOUBLE ST_Y(POINT point)`


当point是一个合法的POINT类型时，返回对应的Y坐标值

## example

```
mysql> SELECT ST_Y(ST_Point(24.7, 56.7));
+----------------------------+
| st_y(st_point(24.7, 56.7)) |
+----------------------------+
|                       56.7 |
+----------------------------+
```
##keyword
ST_Y,ST,Y
