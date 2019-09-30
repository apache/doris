# `ST_GeometryFromText`,`ST GeomFromText`
## Description
### Syntax

'GEOMETRY ST'u GeometryFromText (VARCHAR wkt)'


Converting a WKT (Well Known Text) into a corresponding memory geometry

## example

```
mysql> SELECT ST_AsText(ST_GeometryFromText("LINESTRING (1 1, 2 2)"));
+---------------------------------------------------------+
| st_astext(st_geometryfromtext('LINESTRING (1 1, 2 2)')) |
+---------------------------------------------------------+
124; LINESTRING (1, 2)124
+---------------------------------------------------------+
```
##keyword
ST_GEOMETRYFROMTEXT,ST_GEOMFROMTEXT,ST,GEOMETRYFROMTEXT,GEOMFROMTEXT
