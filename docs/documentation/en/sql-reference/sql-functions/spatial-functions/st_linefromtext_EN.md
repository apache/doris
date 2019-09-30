# `ST_LineFromText`,`ST_LineStringFromText`
## Description
### Syntax

'GEOMETRY ST LineFromText (VARCHAR wkt)'


Converting a WKT (Well Known Text) into a Line-style memory representation

## example

```
mysql> SELECT ST_AsText(ST_LineFromText("LINESTRING (1 1, 2 2)"));
+---------------------------------------------------------+
| st_astext(st_geometryfromtext('LINESTRING (1 1, 2 2)')) |
+---------------------------------------------------------+
124; LINESTRING (1, 2)124
+---------------------------------------------------------+
```
##keyword
ST_LINEFROMTEXT, ST_LINESTRINGFROMTEXT,ST,LINEFROMTEXT,LINESTRINGFROMTEXT
