---
{
    "title": "ST_AsBinary",
    "language": "en"
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

## ST_AsBinary

### Syntax

`VARCHAR ST_AsBinary(GEOMETRY geo)`

### Description

Converting a geometric figure into a standard WKB (Well-known binary) representation

Currently supported geometric figures are: Point, LineString, Polygon.

### example

```
mysql> select ST_AsBinary(st_point(24.7, 56.7));
+-----------------------------------+
| st_asbinary(st_point(24.7, 56.7)) |
+-----------------------------------+
|    33333�8@�����YL@                   |
+-----------------------------------+
1 row in set (0.04 sec)

mysql> select ST_AsBinary(ST_GeometryFromText("LINESTRING (1 1, 2 2)"));
+-----------------------------------------------------------+
| st_asbinary(st_geometryfromtext('LINESTRING (1 1, 2 2)')) |
+-----------------------------------------------------------+
|       �������?      �?       @�������?                                |
+-----------------------------------------------------------+
1 row in set (0.02 sec)

mysql> select ST_AsBinary(ST_Polygon("POLYGON ((114.104486 22.547119,114.093758 22.547753,114.096504 22.532057,114.104229 22.539826,114.106203 22.542680,114.104486 22.547119))"));
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| st_asbinary(st_polygon('POLYGON ((114.104486 22.547119,114.093758 22.547753,114.096504 22.532057,114.104229 22.539826,114.106203 22.542680,114.104486 22.547119))')) |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|          �8
             毆\@.���6@B�! �\@0Ie�9�6@��-�\@��6�4�6@ޒ���\@·g    2�6@,�̆\@{1��6@�8
                                                                                毆\@.���6@                                                                                          |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)

```
### keywords
ST_ASBINARY,ST,ASBINARY
