// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
suite("test_gis_function") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set batch_size = 4096;"

    qt_sql "SELECT ST_AsText(ST_Point(24.7, 56.7));"
    qt_sql "SELECT ST_AsWKT(ST_Point(24.7, 56.7));"

    qt_sql "SELECT ST_AsText(ST_Circle(111, 64, 10000));"

    qt_sql "SELECT ST_Contains(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(5, 5));"
    qt_sql "SELECT ST_Contains(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(50, 50));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('POINT(2 10)'));"

    qt_sql "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('LINESTRING(2 0, 8 0)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('LINESTRING(2 5, 8 5)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('LINESTRING(0 0, 10 0)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('LINESTRING(10 0, 0 0)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('LINESTRING(5 0, 5 5)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('LINESTRING(0 0, 10 10)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('LINESTRING(-0.000001 0, 10 10)'));"

    qt_sql "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('POLYGON((10 10, 20 10, 20 20, 10 20, 10 10))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('POLYGON((0 0, 0 10.000001, 10.000001 10.000001, 10.000001 0, 0 0))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), ST_GeomFromText('POLYGON((3 3, 3 7, 7 7, 7 3, 3 3))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3))'), ST_GeomFromText('POLYGON((4 4, 7 4, 7 7, 4 7, 4 4))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3))'), ST_GeomFromText('POLYGON((3 3, 8 3, 8 8, 3 8, 3 3))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3))'), ST_GeomFromText('POLYGON((2.999999 2.999999, 8.000001 2.999999, 8.000001 8.000001, 2.999999 8.000001, 2.999999 2.999999), (3 3, 8 3, 8 8, 3 8, 3 3))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3))'), ST_GeomFromText('POLYGON((1 1, 9 1, 9 9, 1 9, 1 1), (3.000001 3.000001, 7.999999 3.000001, 7.999999 7.999999, 3.000001 7.999999, 3.000001 3.000001))'));"
    
    qt_sql "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),ST_GeomFromText('MULTIPOLYGON(((2 2, 4 2, 4 4, 2 4, 2 2)), ((6 6, 8 6, 8 8, 6 8, 6 6)))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),ST_GeomFromText('MULTIPOLYGON(((2 2, 2 8, 8 8, 8 2, 2 2)), ((10 10, 10 15, 15 15, 15 10, 10 10)))'));"

    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POINT(5 5)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POINT(17 7)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POINT(12 7)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POINT(0 5)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POINT(10 10)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)))'), ST_GeomFromText('POINT(5 5)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)))'), ST_GeomFromText('POINT(2 5)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POINT(0.000001 0.000001)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POINT(-0.000001 0)'));"

    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('LINESTRING(2 2, 8 8)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('LINESTRING(16 6, 19 9)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('LINESTRING(5 5, 16 6)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('LINESTRING(0 0, 10 10)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('LINESTRING(5 0, 5 10)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)))'), ST_GeomFromText('LINESTRING(3 3, 7 7)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)))'), ST_GeomFromText('LINESTRING(1 1, 9 9)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('LINESTRING(0.000001 0.000001, 9.999999 9.999999)'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('LINESTRING(0.000001 0.000001, 10.000001 10.000001)'));"

    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POLYGON((1 1, 1 9, 9 9, 9 1, 1 1))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POLYGON((16 6, 16 9, 19 9, 19 6, 16 6))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POLYGON((5 5, 5 15, 15 15, 15 5, 5 5))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POLYGON((-5 -5, -5 15, 15 15, 15 -5, -5 -5))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)))'), ST_GeomFromText('POLYGON((3 3, 3 7, 7 7, 7 3, 3 3))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)))'), ST_GeomFromText('POLYGON((1 1, 1 9, 9 9, 9 1, 1 1))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POLYGON((0.000001 0.000001, 0.000001 9.999999, 9.999999 9.999999, 9.999999 0.000001, 0.000001 0.000001))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('POLYGON((-0.000001 -0.000001, -0.000001 10.000001, 10.000001 10.000001, 10.000001 -0.000001, -0.000001 -0.000001))'));"

    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('MULTIPOLYGON(((1 1, 1 5, 5 5, 5 1, 1 1)), ((16 6, 16 9, 19 9, 19 6, 16 6)))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('MULTIPOLYGON(((1 1, 1 5, 5 5, 5 1, 1 1)), ((12 6, 12 9, 14 9, 14 6, 12 6)))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('MULTIPOLYGON(((1 1, 1 9, 9 9, 9 1, 1 1)), ((16 6, 16 9, 19 9, 19 6, 16 6)))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (2 2, 2 8, 8 8, 8 2, 2 2)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('MULTIPOLYGON(((3 3, 3 7, 7 7, 7 3, 3 3)), ((16 6, 16 9, 19 9, 19 6, 16 6)))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((25 5, 25 10, 30 10, 30 5, 25 5)))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('MULTIPOLYGON(((0.000001 0.000001, 0.000001 9.999999, 9.999999 9.999999, 9.999999 0.000001, 0.000001 0.000001)), ((15.000001 5.000001, 15.000001 9.999999, 19.999999 9.999999, 19.999999 5.000001, 15.000001 5.000001)))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('MULTIPOLYGON(((-0.000001 -0.000001, -0.000001 10.000001, 10.000001 10.000001, 10.000001 -0.000001, -0.000001 -0.000001)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'));"
    qt_sql "SELECT ST_Contains(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (3 3, 3 7, 7 7, 7 3, 3 3)), ((15 5, 15 10, 20 10, 20 5, 15 5)))'), ST_GeomFromText('MULTIPOLYGON(((1 1, 1 9, 9 9, 9 1, 1 1), (4 4, 4 6, 6 6, 6 4, 4 4)), ((16 6, 16 9, 19 9, 19 6, 16 6)))'));"

    qt_sql "SELECT ST_Intersects(ST_Point(0, 0), ST_Point(0, 0));"
    qt_sql "SELECT ST_Intersects(ST_Point(0, 0), ST_Point(5, 5));"

    qt_sql "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(0, 0));"
    qt_sql "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(1.99, 0));"
    qt_sql "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(2.01, 0));"
    qt_sql "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(2, 0));"
    qt_sql "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(0, 1));"

    qt_sql "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (2 0, 3 0)\"));"
    qt_sql "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (2.1 0, 3 0)\"));"
    qt_sql "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (1.99 0, 3 0)\"));"
    qt_sql "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (-2 0.01, 3 0.01)\"));"
    qt_sql "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (3 0, 4 0)\"));"
    qt_sql "SELECT ST_Intersects(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (1 0, 4 0)\"));"

    qt_sql "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(0, 0));"
    qt_sql "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(5, 0));"
    qt_sql "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(5, 5));"

    qt_sql "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (20 0, 0 20)\"));"
    qt_sql "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (-20 0, 20 0)\"));"
    qt_sql "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (3 5, 8 5)\"));"
    qt_sql "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (0 0.01, 10 0.01)\"));"
    qt_sql "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (0 -0.01, 10 -0.01)\"));"

    qt_sql "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((5 0, 15 0, 15 10, 5 10, 5 0))\"));"
    qt_sql "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((10 0, 10 10, 20 10, 20 0, 10 0))\"));"
    qt_sql "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((11 0, 11 10, 21 10, 21 0, 11 0))\"));"
    qt_sql "SELECT ST_Intersects(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))\"));"

    qt_sql "SELECT ST_Intersects(ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"), ST_Point(0, 0));"
    qt_sql "SELECT ST_Intersects(ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"), ST_Point(8, 8));"
    qt_sql "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_Point(5.000001, 0));"
    qt_sql "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POINT(5 3)'));"
    qt_sql "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POINT(5 5)'));"

    qt_sql "SELECT ST_Intersects(ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"), ST_LineFromText(\"LINESTRING (4 4, 7 7)\"));"
    qt_sql "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_LineFromText('LINESTRING (5.000001 0, 5.999999 10)'));"
    qt_sql "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('LINESTRING(3 3, 8 8)'));"
    qt_sql "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('LINESTRING(4 3, 4 7)'));"
    qt_sql "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('LINESTRING(3.000001 3.000001, 7.999999 7.999999)'));"

    qt_sql "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((12 12, 12 15, 15 15, 15 12, 12 12))'));"
    qt_sql "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((3 3, 3 8, 8 8, 8 3, 3 3))'));"
    qt_sql "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((5.000001 0, 5.999999 0, 5.999999 9.999999, 5.000001 9.999999, 5.000001 0))'));"
    qt_sql "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POLYGON((3.00001 3.00001, 7.99999 3.00001, 7.99999 7.99999, 3.00001 7.99999, 3.00001 3.00001))'));"
    qt_sql "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POLYGON((4 4, 6 4, 6 6, 4 6, 4 4))'));"
    qt_sql "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POLYGON((4 3, 7 3, 7 8, 4 8, 4 3))'));"

    qt_sql "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('MULTIPOLYGON(((5 5, 5 8, 8 8, 8 5, 5 5)), ((8 8, 8 12, 12 12, 12 8, 8 8)))'));"
    qt_sql "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('MULTIPOLYGON(((2 2, 3 2, 3 3, 2 3, 2 2)), ((11 11, 11 12, 12 12, 12 11, 11 11)))'));"
    qt_sql "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('MULTIPOLYGON(((-2 -2, -3 -2, -3 -3, -2 -3, -2 -2)), ((11 11, 11 12, 12 12, 12 11, 11 11)))'));"
    qt_sql "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('MULTIPOLYGON(((5.000001 0, 6 0, 6 5.999999, 5.000001 5.999999, 5.000001 0)), ((0 5.000001, 5 5.000001, 5 6, 5.999999 6, 5.999999 10, 0 10, 0 5.000001)))'));"

    qt_sql "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_CIRCLE(2, 6, 1));"
    qt_sql "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_CIRCLE(2, 6, 0.999999));"
    qt_sql "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_CIRCLE(10, 10, 1));"
    qt_sql "SELECT ST_Intersects(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_CIRCLE(5, 6, 0.999999));"

    qt_sql "SELECT ST_Intersects(ST_Circle(1, 1, 1), ST_Point(2, 1));" 
    qt_sql "SELECT ST_Intersects(ST_Circle(1, 1, 1), ST_Point(1, 1));"
    qt_sql "SELECT ST_Intersects(ST_Circle(1, 1, 1), ST_Point(3, 1));"  

    qt_sql "SELECT ST_Intersects(ST_Circle(1, 1, 1), ST_LineFromText(\"LINESTRING (2 0, 2 2)\"));"
    qt_sql "SELECT ST_Intersects(ST_Circle(1, 1, 1), ST_LineFromText(\"LINESTRING (1.7 0, 1.7 2)\"));"
    qt_sql "SELECT ST_Intersects(ST_Circle(1, 1, 1), ST_LineFromText(\"LINESTRING (1 0.5, 1 1.5)\"));"

    qt_sql "SELECT ST_Intersects(ST_Circle(1, 1, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"
    qt_sql "SELECT ST_Intersects(ST_Circle(5, 5, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"
    qt_sql "SELECT ST_Intersects(ST_Circle(2, 1, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"
    qt_sql "SELECT ST_Intersects(ST_Circle(0, 1, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"

    qt_sql "SELECT ST_Intersects(ST_Circle(1, 1, 1), ST_Circle(3, 1, 1));" 
    qt_sql "SELECT ST_Intersects(ST_Circle(1, 1, 1), ST_Circle(2, 1, 1));"
    qt_sql "SELECT ST_Intersects(ST_Circle(1, 1, 1), ST_Circle(4, 1, 1));"

    qt_sql "SELECT ST_Touches(ST_Point(0, 0), ST_Point(0, 0));"
    qt_sql "SELECT ST_Touches(ST_Point(0, 0), ST_Point(5, 5));"

    qt_sql "SELECT ST_Touches(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(0, 0));" 
    qt_sql "SELECT ST_Touches(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(2, 0));"
    qt_sql "SELECT ST_Touches(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(0, 1));"

    qt_sql "SELECT ST_Touches(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (2 0, 3 0)\"));"
    qt_sql "SELECT ST_Touches(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (3 0, 4 0)\"));"
    qt_sql "SELECT ST_Touches(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (1 0, 4 0)\"));"

    qt_sql "SELECT ST_Touches(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(0, 0));"
    qt_sql "SELECT ST_Touches(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(5, 0));"
    qt_sql "SELECT ST_Touches(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(5, 5));"

    qt_sql "SELECT ST_Touches(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (20 0, 0 20)\"));"
    qt_sql "SELECT ST_Touches(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (-20 0, 20 0)\"));"
    qt_sql "SELECT ST_Touches(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (3 5, 8 5)\"));"
    qt_sql "SELECT ST_Touches(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (-3 5, 8 5)\"));"
    qt_sql "SELECT ST_Touches(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (-3 5, 15 5)\"));"

    qt_sql "SELECT ST_Touches(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((5 0, 15 0, 15 10, 5 10, 5 0))\"));"
    qt_sql "SELECT ST_Touches(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((10 0, 10 10, 20 10, 20 0, 10 0))\"));"
    qt_sql "SELECT ST_Touches(ST_Polygon('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'), ST_Polygon('POLYGON ((11 0, 11 10, 21 10, 21 0, 11 0))'));"
    qt_sql "SELECT ST_Touches(ST_Polygon('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'), ST_Polygon('POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))'));"
    qt_sql "SELECT ST_Touches(ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'),ST_GeomFromText('POLYGON((2 10, 5 15, 8 10, 10 15, 5 20, 0 15, 2 10))'));"

    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_Point(0, 3));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_Point(5, 5));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_Point(5, 6));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_Point(3, 3));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_Point(5.00001, 0));"

    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_LineFromText('LINESTRING (5 5, 7 7)'));"                           
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('LINESTRING (5 5, 5 10)'));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('LINESTRING (0 3, 5 3)'));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('LINESTRING (5.5 0, 5.5 10)'));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('LINESTRING (5.000001 0, 5.999999 10)'));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('LINESTRING(3 3, 8 8)'));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('LINESTRING(4 4, 4 7)'));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('LINESTRING(3.000001 3.000001, 7.999999 7.999999)'));"

    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((5 5, 5 8, 8 8, 8 5, 5 5))'));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)))'), ST_GeometryFromText('POLYGON((5 5, 5 8, 8 8, 8 5, 5 5))'));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((12 12, 12 15, 15 15, 15 12, 12 12))'));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((0 0, 0 4, 4 4, 4 0, 0 0))'));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((0 0, 0 -4, -4 -4, -4 0, 0 0))'));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((5.000001 0, 6 0, 6 5.999999, 5.000001 5.999999, 5.000001 0))'));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POLYGON((3.00001 3.00001, 7.99999 3.00001, 7.99999 7.99999, 3.00001 7.99999, 3.00001 3.00001))'));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POLYGON((4 4, 6 4, 6 6, 4 6, 4 4))'));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POLYGON((3 4, 8 4, 8 7, 3 7, 3 4))'));"
    
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)), ((15 0, 25 0, 25 10, 15 10, 15 0)), ((30 30, 35 35, 40 30, 30 30)))'),ST_GeometryFromText('MULTIPOLYGON (((10 0, 20 0, 20 10, 10 10, 10 0)))'));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('MULTIPOLYGON(((3 4, 8 4, 8 7, 3 7, 3 4)), ((-8 -8, -8 -12, -12 -12, -12 -8, -8 -8)))'));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('MULTIPOLYGON(((10.00001 0, 14.99999 0, 14.99999 10, 10.00001 10, 10.00001 0)), ((-8 -8, -8 -12, -12 -12, -12 -8, -8 -8)))'));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('MULTIPOLYGON(((3 3, 8 3, 8 8, 3 8, 3 3)), ((8 8, 8 12, 12 12, 12 8, 8 8)))'));"

    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_Circle(10, 15, 5));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_Circle(10, 15, 5.00001));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_Circle(3, 5, 2));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_Circle(4.5, 5, 1.5));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_Circle(20, 20, 1));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_Circle(5.5, 5.5, 0.5));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_Circle(1, 1, 0.5));"
    qt_sql "SELECT ST_Touches(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_Circle(8, 8, 2));"

    qt_sql "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_Point(2, 1));"
    qt_sql "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_Point(1, 2));"
    qt_sql "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_Point(1, 1));"
    qt_sql "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_Point(3, 1));"

    qt_sql "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_LineFromText(\"LINESTRING (2 0, 2 2)\"));"
    qt_sql "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_LineFromText(\"LINESTRING (1.7 0, 1.7 2)\"));"
    qt_sql "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_LineFromText(\"LINESTRING (1 0.5, 1 1.5)\"));"

    qt_sql "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));" 
    qt_sql "SELECT ST_Touches(ST_Circle(5, 5, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"
    qt_sql "SELECT ST_Touches(ST_Circle(2, 1, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"
    qt_sql "SELECT ST_Touches(ST_Circle(0, 1, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"

    qt_sql "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_Circle(3, 1, 1));"
    qt_sql "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_Circle(2, 1, 1));"
    qt_sql "SELECT ST_Touches(ST_Circle(1, 1, 1), ST_Circle(4, 1, 1));"

    qt_sql "SELECT ST_Disjoint(ST_Point(0, 0), ST_Point(0, 0));"
    qt_sql "SELECT ST_Disjoint(ST_Point(0, 0), ST_Point(5, 5));"

    qt_sql "SELECT ST_Disjoint(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(0, 0));"
    qt_sql "SELECT ST_Disjoint(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(2, 0));"
    qt_sql "SELECT ST_Disjoint(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_Point(0, 1));"

    qt_sql "SELECT ST_Disjoint(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (2 0, 3 0)\"));"
    qt_sql "SELECT ST_Disjoint(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (3 0, 4 0)\"));"
    qt_sql "SELECT ST_Disjoint(ST_LineFromText(\"LINESTRING (-2 0, 2 0)\"), ST_LineFromText(\"LINESTRING (1 0, 4 0)\"));"

    qt_sql "SELECT ST_Disjoint(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(0, 0));"
    qt_sql "SELECT ST_Disjoint(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(5, 0));"
    qt_sql "SELECT ST_Disjoint(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(5, 5));"

    qt_sql "SELECT ST_Disjoint(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (20 0, 0 20)\"));"
    qt_sql "SELECT ST_Disjoint(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (-20 0, 20 0)\"));"
    qt_sql "SELECT ST_Disjoint(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_LineFromText(\"LINESTRING (3 5, 8 5)\"));"

    qt_sql "SELECT ST_Disjoint(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((5 0, 15 0, 15 10, 5 10, 5 0))\"));"
    qt_sql "SELECT ST_Disjoint(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((10 0, 10 10, 20 10, 20 0, 10 0))\"));"
    qt_sql "SELECT ST_Disjoint(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((11 0, 11 10, 21 10, 21 0, 11 0))\"));"
    qt_sql "SELECT ST_Disjoint(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))\"));"

    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"), ST_Point(0, 0));"
    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"), ST_Point(8, 8));"
    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_Point(5.000001, 0));"
    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POINT(5 3)'));"
    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POINT(5 5)'));"

    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText(\"MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))\"), ST_LineFromText(\"LINESTRING (4 4, 7 7)\"));"
    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_LineFromText('LINESTRING (5.000001 0, 5.999999 10)'));"
    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('LINESTRING(3 3, 8 8)'));"
    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('LINESTRING(4 3, 4 7)'));"
    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('LINESTRING(3.000001 3.000001, 7.999999 7.999999)'));"

    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((12 12, 12 15, 15 15, 15 12, 12 12))'));"
    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((3 3, 3 8, 8 8, 8 3, 3 3))'));"
    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('POLYGON((5.000001 0, 5.999999 0, 5.999999 9.999999, 5.000001 9.999999, 5.000001 0))'));"
    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POLYGON((3.00001 3.00001, 7.99999 3.00001, 7.99999 7.99999, 3.00001 7.99999, 3.00001 3.00001))'));"
    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POLYGON((4 4, 6 4, 6 6, 4 6, 4 4))'));"
    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),((15 0, 25 0, 25 10, 15 10, 15 0)),((30 30, 40 30, 35 35, 30 30)))'), ST_GeometryFromText('POLYGON((4 3, 7 3, 7 8, 4 8, 4 3))'));"

    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('MULTIPOLYGON(((5 5, 5 8, 8 8, 8 5, 5 5)), ((8 8, 8 12, 12 12, 12 8, 8 8)))'));"
    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('MULTIPOLYGON(((2 2, 3 2, 3 3, 2 3, 2 2)), ((11 11, 11 12, 12 12, 12 11, 11 11)))'));"
    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('MULTIPOLYGON(((-2 -2, -3 -2, -3 -3, -2 -3, -2 -2)), ((11 11, 11 12, 12 12, 12 11, 11 11)))'));"
    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_GeometryFromText('MULTIPOLYGON(((5.000001 0, 6 0, 6 5.999999, 5.000001 5.999999, 5.000001 0)), ((0 5.000001, 5 5.000001, 5 6, 5.999999 6, 5.999999 10, 0 10, 0 5.000001)))'));"

    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_CIRCLE(2, 6, 1));"
    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_CIRCLE(2, 6, 0.999999));"
    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_CIRCLE(10, 10, 1));"
    qt_sql "SELECT ST_Disjoint(ST_GeometryFromText('MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0)), ((6 6, 6 10, 10 10, 10 6, 6 6)))'), ST_CIRCLE(5, 6, 0.999999));"

    qt_sql "SELECT ST_Disjoint(ST_Circle(1, 1, 1), ST_Point(2, 1));" 
    qt_sql "SELECT ST_Disjoint(ST_Circle(1, 1, 1), ST_Point(1, 1));"
    qt_sql "SELECT ST_Disjoint(ST_Circle(1, 1, 1), ST_Point(3, 1));"  

    qt_sql "SELECT ST_Disjoint(ST_Circle(1, 1, 1), ST_LineFromText(\"LINESTRING (2 0, 2 2)\"));"
    qt_sql "SELECT ST_Disjoint(ST_Circle(1, 1, 1), ST_LineFromText(\"LINESTRING (1.7 0, 1.7 2)\"));"
    qt_sql "SELECT ST_Disjoint(ST_Circle(1, 1, 1), ST_LineFromText(\"LINESTRING (1 0.5, 1 1.5)\"));"

    qt_sql "SELECT ST_Disjoint(ST_Circle(1, 1, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"
    qt_sql "SELECT ST_Disjoint(ST_Circle(5, 5, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"
    qt_sql "SELECT ST_Disjoint(ST_Circle(2, 1, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"
    qt_sql "SELECT ST_Disjoint(ST_Circle(0, 1, 1), ST_Polygon(\"POLYGON ((2 0, 12 0, 12 10, 2 10, 2 0))\"));"

    qt_sql "SELECT ST_Disjoint(ST_Circle(1, 1, 1), ST_Circle(3, 1, 1));" 
    qt_sql "SELECT ST_Disjoint(ST_Circle(1, 1, 1), ST_Circle(2, 1, 1));"
    qt_sql "SELECT ST_Disjoint(ST_Circle(1, 1, 1), ST_Circle(4, 1, 1));"

    qt_sql "SELECT ST_DISTANCE_SPHERE(116.35620117, 39.939093, 116.4274406433, 39.9020987219);"
    qt_sql "SELECT ST_ANGLE_SPHERE(116.35620117, 39.939093, 116.4274406433, 39.9020987219);"
    qt_sql "SELECT ST_ANGLE_SPHERE(0, 0, 45, 0);"

    qt_sql "SELECT ST_AsText(ST_GeometryFromText(\"LINESTRING (1 1, 2 2)\"));"
    qt_sql "SELECT ST_AsText(ST_GeomFromText(\"LINESTRING (1 1, 2 2)\"));"

    qt_sql "SELECT ST_AsText(ST_LineFromText(\"LINESTRING (1 1, 2 2)\"));"
    qt_sql "SELECT ST_AsText(ST_LineStringFromText(\"LINESTRING (1 1, 2 2)\"));"

    qt_sql "SELECT ST_AsText(ST_Point(24.7, 56.7));"

    qt_sql "SELECT ST_AsText(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"
    qt_sql "SELECT ST_AsText(ST_PolyFromText(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"
    qt_sql "SELECT ST_AsText(ST_PolygonFromText(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"

    qt_sql "SELECT ST_AsText(ST_GeometryFromText(\"MULTIPOLYGON (((0.0 0.0, 0.0 4.9, 5.3 5.3, 5.2 0.0, 0.0 0.0)), ((20.1 20.3, 20.4 30.6, 30.6 30.4, 30.0 20.2, 20.1 20.3)), ((50.2 50.3, 50.1 60.7, 60.7 60.9, 60.5 50.5, 50.2 50.3)), ((70.3 70.1, 70.5 80.5, 80.3 80.2, 80.0 70.0, 70.3 70.1)))\"));"
    qt_sql "SELECT ST_AsText(ST_GeometryFromText('MULTIPOLYGON(((5 5, 5 8, 8 8, 8 5, 5 5)), ((8 8, 8 12, 12 12, 12 8, 8 8)))'));"
    qt_sql "SELECT ST_AsText(ST_GeometryFromText('MULTIPOLYGON(((5 5, 5 8, 8 8, 8 5, 5 5)), ((8 6, 10 6, 10 10, 8 10, 8 6)))'));"
    qt_sql "SELECT ST_AsText(ST_GeometryFromText(\"MULTIPOLYGON (((0 0, 0 10, 10 10, 10 0, 0 0)), ((5 5, 5 15, 15 15, 15 5, 5 5)))\"));"
    qt_sql "SELECT ST_AsText(ST_GeomFromText('MULTIPOLYGON (((-10 -10, -10 10, 10 10, 10 -10, -10 -10)), ((20 20, 20 30, 30 30, 30 20, 20 20), (25 25, 25 27, 27 27, 27 25, 25 25)))'));"
    qt_sql "SELECT ST_AsText(ST_GeometryFromText('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((2 10, 5 15, 8 10, 10 15, 5 20, 0 15, 2 10)))'));"
    qt_sql "SELECT ST_AsText(ST_GeomFromText('MULTIPOLYGON (((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 15 5, 15 15, 5 15, 5 5)), ((5 10, 10 5, 15 10, 10 15, 5 10)))'));"
    qt_sql "SELECT ST_AsText(ST_GeomFromText('MULTIPOLYGON(((0 0, 0 10, 10 10, 10 0, 0 0), (3 3, 7 3, 7 7, 3 7, 3 3)), ((3 3, 3 7, 7 7, 7 3, 3 3)))'));"
    qt_sql "SELECT ST_AsText(ST_GeomFromText('MULTIPOLYGON(((0 0, 4 1, 2 4, 0 0)), ((2 4, 5 6, 3 8, 2 4)))'));"
    qt_sql "SELECT ST_AsText(ST_GeomFromText('MULTIPOLYGON(((0 0, 1e-8 0, 1e-8 1e-8, 0 1e-8, 0 0)))'));"
    qt_sql "SELECT ST_AsText(ST_GeomFromText('MULTIPOLYGON(((0 0, 1e-6 0, 1e-6 1e-6, 0 1e-6, 0 0)), ((1e-5 0, 1 0, 1 1e-5, 1e-5 1e-5, 1e-5 0)))'));"
    qt_sql "SELECT ST_AsText(ST_GeomFromText('MULTIPOLYGON(((0 0, 1e-6 0, 1e-6 1e-6, 0 1e-6, 0 0), (1e-8 1e-8, 1e-7 1e-8, 1e-7 1e-7, 1e-8 1e-7, 1e-8 1e-8)))'));"

    qt_sql "SELECT ST_X(ST_Point(1, 2));"
    qt_sql "SELECT ST_X(ST_Point(24.7, 56.7));"
    qt_sql "SELECT ST_Y(ST_Point(2, 1));"
    qt_sql "SELECT ST_Y(ST_Point(24.7, 56.7));"

    qt_sql "SELECT ST_Area_Square_Meters(ST_Circle(0, 0, 1));"
    qt_sql "SELECT ST_Area_Square_Km(ST_Circle(0, 0, 1));"
    qt_sql "SELECT ST_Area_Square_Meters(ST_Polygon(\"POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))\"));"
    qt_sql "SELECT ST_Area_Square_Km(ST_Polygon(\"POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))\"));"

    qt_sql "SELECT ST_Angle(ST_Point(1, 0),ST_Point(0, 0),ST_Point(0, 1));"
    qt_sql "SELECT ST_Angle(ST_Point(0, 0),ST_Point(1, 0),ST_Point(0, 1));"
    qt_sql "SELECT ST_Angle(ST_Point(1, 0),ST_Point(0, 0),ST_Point(1, 0));"
    qt_sql "SELECT ST_Angle(ST_Point(1, 0),ST_Point(-30, 0),ST_Point(150, 0));"

    qt_sql "SELECT St_Azimuth(ST_Point(1, 0),ST_Point(0, 0));"
    qt_sql "SELECT St_Azimuth(ST_Point(0, 0),ST_Point(1, 0));"
    qt_sql "SELECT St_Azimuth(ST_Point(0, 0),ST_Point(0, 1));"
    qt_sql "SELECT St_Azimuth(ST_Point(-30, 0),ST_Point(150, 0));"

    qt_sql "SELECT ST_AsText(ST_GeometryFromWKB(ST_AsBinary(ST_Point(24.7, 56.7))));"
    qt_sql "SELECT ST_AsText(ST_GeometryFromWKB(ST_AsBinary(ST_GeometryFromText(\"LINESTRING (1 1, 2 2)\"))));"
    qt_sql "SELECT ST_AsText(ST_GeometryFromWKB(ST_AsBinary(ST_Polygon(\"POLYGON ((114.104486 22.547119,114.093758 22.547753,114.096504 22.532057,114.104229 22.539826,114.106203 22.542680,114.104486 22.547119))\"))));"

    qt_sql "SELECT ST_AsText(ST_GeomFromWKB(ST_AsBinary(ST_Point(24.7, 56.7))));"
    qt_sql "SELECT ST_AsText(ST_GeomFromWKB(ST_AsBinary(ST_GeometryFromText(\"LINESTRING (1 1, 2 2)\"))));"
    qt_sql "SELECT ST_AsText(ST_GeomFromWKB(ST_AsBinary(ST_Polygon(\"POLYGON ((114.104486 22.547119,114.093758 22.547753,114.096504 22.532057,114.104229 22.539826,114.106203 22.542680,114.104486 22.547119))\"))));"

    // test const
    sql "drop table if exists test_gis_const"
    sql """
    CREATE TABLE test_gis_const (
        `userid` varchar(32) NOT NULL COMMENT '个人账号id',
        `c_1` double NOT NULL COMMENT '发送时间',
        `c_2` double NOT NULL COMMENT '信源类型'
    ) ENGINE=OLAP
    UNIQUE KEY(`userid`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`userid`) BUCKETS 6
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql "insert into test_gis_const values ('xxxx',78.73,31.5);"
    qt_sql_part_const_dis_sph "select st_distance_sphere(78.73,31.53,c_1,c_2) from test_gis_const; "
    qt_sql_part_const_ang_sph "select st_angle_sphere(78.73,31.53,c_1,c_2) from test_gis_const; "
}
