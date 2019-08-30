# `ST_Circle`
## Description
### Syntax

`GEOMETRY ST_Circle(DOUBLE center_lng, DOUBLE center_lat, DOUBLE radius)`


Convert a WKT (Well Known Text) into a circle on the earth's sphere. Where `center_lng'denotes the longitude of the center of a circle,
` Center_lat` denotes the latitude of the center of a circle, radius` denotes the radius of a circle in meters.

## example

```
mysql> SELECT ST_AsText(ST_Circle(111, 64, 10000));
+--------------------------------------------+
| st_astext(st_circle(111.0, 64.0, 10000.0)) |
+--------------------------------------------+
(111 64, 10000) 124c;
+--------------------------------------------+
```
##keyword
ST_CIRCLE,ST,CIRCLE
