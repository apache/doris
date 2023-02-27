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
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    sql "set batch_size = 4096;"

    qt_sql "SELECT ST_AsText(ST_Point(24.7, 56.7));"
    qt_sql "SELECT ST_AsWKT(ST_Point(24.7, 56.7));"

    qt_sql "SELECT ST_AsText(ST_Circle(111, 64, 10000));"

    qt_sql "SELECT ST_Contains(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(5, 5));"
    qt_sql "SELECT ST_Contains(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Point(50, 50));"

    qt_sql "SELECT ST_DISTANCE_SPHERE(116.35620117, 39.939093, 116.4274406433, 39.9020987219);"

    qt_sql "SELECT ST_AsText(ST_GeometryFromText(\"LINESTRING (1 1, 2 2)\"));"
    qt_sql "SELECT ST_AsText(ST_GeomFromText(\"LINESTRING (1 1, 2 2)\"));"

    qt_sql "SELECT ST_AsText(ST_LineFromText(\"LINESTRING (1 1, 2 2)\"));"
    qt_sql "SELECT ST_AsText(ST_LineStringFromText(\"LINESTRING (1 1, 2 2)\"));"

    qt_sql "SELECT ST_AsText(ST_Point(24.7, 56.7));"

    qt_sql "SELECT ST_AsText(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"
    qt_sql "SELECT ST_AsText(ST_PolyFromText(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"
    qt_sql "SELECT ST_AsText(ST_PolygonFromText(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"

    qt_sql "SELECT ST_X(ST_Point(24.7, 56.7));"
    qt_sql "SELECT ST_Y(ST_Point(24.7, 56.7));"
}
