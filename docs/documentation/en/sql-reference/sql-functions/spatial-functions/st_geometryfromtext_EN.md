'35; ` ST GeometryFromText','ST GeomFromText '
Description
'35;'35;' 35; Syntax

'GEOMETRY ST'u GeometryFromText (VARCHAR wkt)'


Converting a WKT (Well Known Text) into a corresponding memory geometry

'35;'35; example

```
mysql> SELECT ST_AsText(ST_GeometryFromText("LINESTRING (1 1, 2 2)"));
+---------------------------------------------------------+
| st_astext(st_geometryfromtext('LINESTRING (1 1, 2 2)')) |
+---------------------------------------------------------+
124; LINESTRING (1, 2)124
+---------------------------------------------------------+
```
##keyword
ST. GEOMETRYFROMTEXT,ST. GEOMFROMTEXT,ST,GEOMETRYFROMTEXT,GEOMFROMTEXT
