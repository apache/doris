35; 'ST -u Y'
Description
'35;'35;' 35; Syntax

`DOUBLE ST_Y(POINT point)`


When point is a valid POINT type, the corresponding Y coordinate value is returned.

'35;'35; example

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
