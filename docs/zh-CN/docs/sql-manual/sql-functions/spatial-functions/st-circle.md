---
{
    "title": "ST_CIRCLE",
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

## ST_Circle
### description
#### Syntax

`GEOMETRY ST_Circle(DOUBLE center_lng, DOUBLE center_lat, DOUBLE radius)`


将一个WKT（Well Known Text）转化为地球球面上的一个圆。其中`center_lng`表示的圆心的经度，
`center_lat`表示的是圆心的纬度，`radius`表示的是圆的半径，单位是米,最大支持9999999

### example

```
mysql> SELECT ST_AsText(ST_Circle(111, 64, 10000));
+--------------------------------------------+
| st_astext(st_circle(111.0, 64.0, 10000.0)) |
+--------------------------------------------+
| CIRCLE ((111 64), 10000)                   |
+--------------------------------------------+
```
### keywords
ST_CIRCLE,ST,CIRCLE
