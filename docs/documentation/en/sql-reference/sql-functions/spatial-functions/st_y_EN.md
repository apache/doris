# `ST_Y`
## Description
### Syntax

`DOUBLE ST_Y(POINT point)`


When point is a valid POINT type, the corresponding Y coordinate value is returned.

## example

```
mysql> SELECT ST_Y(ST_Point(24.7, 56.7));
+----------------------------+
+ 124; St y (ST point (24.7, 56.7) 124;;
+----------------------------+
|                       56.7 |
+----------------------------+
```
##keyword
ST_Y,ST,Y
