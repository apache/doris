# `ST_Polygon`,`ST_PolyFromText`,`ST_PolygonFromText`
## Description
### Syntax

'GEOMETRY ST'u Polygon (VARCHAR wkt)'


Converting a WKT (Well Known Text) into a corresponding polygon memory form


### example

```
mysql> SELECT ST_AsText(ST_Polygon("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"));
+------------------------------------------------------------------+
| st_astext(st_polygon('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))')) |
+------------------------------------------------------------------+

+------------------------------------------------------------------+
```
##keyword
ST_POLYGON,ST_POLYFROMTEXT,ST_POLYGONFROMTEXT,ST,POLYGON,POLYFROMTEXT,POLYGONFROMTEXT
