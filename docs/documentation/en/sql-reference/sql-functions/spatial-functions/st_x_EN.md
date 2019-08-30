# `ST_X`
## Description
### Syntax

`DOUBLE ST_X(POINT point)`


When point is a valid POINT type, the corresponding X coordinate value is returned.

## example

```
mysql> SELECT ST_X(ST_Point(24.7, 56.7));
+----------------------------+
+ 124; St  x (ST  point (24.7, 56.7) 124;
+----------------------------+
|                       24.7 |
+----------------------------+
```
##keyword
ST_X,ST,X
