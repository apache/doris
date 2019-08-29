'35; ` ST LineFromText','ST LineStringFromText '
Description
'35;'35;' 35; Syntax

'GEOMETRY ST LineFromText (VARCHAR wkt)'


Converting a WKT (Well Known Text) into a Line-style memory representation

'35;'35; example

```
mysql> SELECT ST_AsText(ST_LineFromText("LINESTRING (1 1, 2 2)"));
+---------------------------------------------------------+
| st_astext(st_geometryfromtext('LINESTRING (1 1, 2 2)')) |
+---------------------------------------------------------+
124; LINESTRING (1, 2)124
+---------------------------------------------------------+
```
##keyword
ST. LINEFROMTEXT, ST. LINESTRINGFROMTEXT,ST,LINEFROMTEXT,LINESTRINGFROMTEXT
