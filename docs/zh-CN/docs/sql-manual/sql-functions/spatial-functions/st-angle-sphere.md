---
{
    "title": "ST_ANGLE_SPHERE",
    "language": "zh-CN"
}
---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

## ST_Angle_Sphere

### Syntax

`DOUBLE ST_Angle_Sphere(DOUBLE x_lng, DOUBLE x_lat, DOUBLE y_lng, DOUBLE y_lat)`

### description

计算地球表面两点之间的圆心角，单位为 度。传入的参数分别为X点的经度，X点的纬度，Y点的经度，Y点的纬度。

x_lng 和 y_lng 都是经度数据，合理的取值范围是 [-180, 180]。

x_lat 和 y_lat 都是纬度数据，合理的取值范围是 [-90, 90]。

### example

```
mysql> select ST_Angle_Sphere(116.35620117, 39.939093, 116.4274406433, 39.9020987219);
+---------------------------------------------------------------------------+
| st_angle_sphere(116.35620117, 39.939093, 116.4274406433, 39.9020987219) |
+---------------------------------------------------------------------------+
|                                                        0.0659823452409903 |
+---------------------------------------------------------------------------+
1 row in set (0.06 sec)

mysql> select ST_Angle_Sphere(0, 0, 45, 0);
+----------------------------------------+
| st_angle_sphere(0.0, 0.0, 45.0, 0.0) |
+----------------------------------------+
|                                     45 |
+----------------------------------------+
1 row in set (0.06 sec)
```
### keywords
ST_ANGLE_SPHERE,ST,ANGLE,SPHERE
