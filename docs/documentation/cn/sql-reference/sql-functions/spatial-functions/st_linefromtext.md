# `ST_LineFromText`,`ST_LineStringFromText`
## description
### Syntax

`GEOMETRY ST_LineFromText(VARCHAR wkt)`


将一个WKT（Well Known Text）转化为一个Line形式的内存表现形式

## example

```
mysql> SELECT ST_AsText(ST_LineFromText("LINESTRING (1 1, 2 2)"));
+---------------------------------------------------------+
| st_astext(st_geometryfromtext('LINESTRING (1 1, 2 2)')) |
+---------------------------------------------------------+
| LINESTRING (1 1, 2 2)                                   |
+---------------------------------------------------------+
```
##keyword
ST_LINEFROMTEXT,ST_LINESTRINGFROMTEXT,ST,LINEFROMTEXT,LINESTRINGFROMTEXT
