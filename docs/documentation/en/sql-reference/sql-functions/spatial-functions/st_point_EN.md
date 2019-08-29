\ 35; St upoint'
Description
'35;'35;' 35; Syntax

`POINT ST_Point(DOUBLE x, DOUBLE y)`


Given the X coordinate value, the Y coordinate value returns the corresponding Point.
The current value is meaningful only for spherical sets, and X/Y corresponds to longitude/latitude.

'35;'35; example

```
mysql> SELECT ST_AsText(ST_Point(24.7, 56.7));
+---------------------------------+
124st text (st point (24.7, 56.7))124st text;
+---------------------------------+
| POINT (24.7 56.7)               |
+---------------------------------+
```
##keyword
ST -u POINT,ST,POINT
