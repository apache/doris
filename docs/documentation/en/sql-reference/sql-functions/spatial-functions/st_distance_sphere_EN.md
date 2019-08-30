# `ST_Distance_Sphere`
## description
### Syntax

`DOUBLE ST_Distance_Sphere(DOUBLE x_lng, DOUBLE x_lat, DOUBLE y_lng, DOUBLE x_lat)`


Calculate the spherical distance between two points of the earth in meters. The incoming parameters are the longitude of point X, the latitude of point X, the longitude of point Y and the latitude of point Y.

## example

```
mysql> select st_distance_sphere(116.35620117, 39.939093, 116.4274406433, 39.9020987219);
+----------------------------------------------------------------------------+
| st_distance_sphere(116.35620117, 39.939093, 116.4274406433, 39.9020987219) |
+----------------------------------------------------------------------------+
|                                                         7336.9135549995917 |
+----------------------------------------------------------------------------+
```
##keyword
ST_DISTANCE_SPHERE,ST,DISTANCE,SPHERE
