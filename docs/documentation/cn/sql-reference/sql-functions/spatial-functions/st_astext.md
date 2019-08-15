# `ST_AsText`,`ST_AsWKT`
## description
### Syntax

`VARCHAR ST_AsText(GEOMETRY geo)`


将一个几何图形转化为WKT（Well Known Text）的表示形式

## example

```
mysql> SELECT ST_AsText(ST_Point(24.7, 56.7));
+---------------------------------+
| st_astext(st_point(24.7, 56.7)) |
+---------------------------------+
| POINT (24.7 56.7)               |
+---------------------------------+
```
##keyword
ST_ASTEXT,ST_ASWKT,ST,ASTEXT,ASWKT
