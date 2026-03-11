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
suite("test_geos_functions", "arrow_flight_sql") {
    sql "set batch_size = 4096;"

    // ==================== ST_IsValid ====================
    qt_st_isvalid_01 "SELECT ST_IsValid(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"
    qt_st_isvalid_02 "SELECT ST_IsValid(ST_Point(1, 2));"
    qt_st_isvalid_03 "SELECT ST_IsValid(ST_LineFromText(\"LINESTRING (0 0, 10 10, 20 0)\"));"
    qt_st_isvalid_04 "SELECT ST_IsValid(NULL);"

    // ==================== ST_MakeValid ====================
    qt_st_makevalid_01 "SELECT ST_AsText(ST_MakeValid(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\")));"
    qt_st_makevalid_02 "SELECT ST_MakeValid(NULL);"
    qt_st_makevalid_03 "SELECT ST_AsText(ST_MakeValid(ST_Point(1, 2)));"

    // ==================== ST_Buffer ====================
    qt_st_buffer_01 "SELECT ST_AsText(ST_Buffer(ST_Point(0, 0), 1)) IS NOT NULL;"
    qt_st_buffer_02 "SELECT ST_AsText(ST_Buffer(ST_Point(0, 0), 0)) IS NOT NULL;"
    qt_st_buffer_03 "SELECT ST_AsText(ST_Buffer(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), 1)) IS NOT NULL;"
    qt_st_buffer_04 "SELECT ST_Buffer(NULL, 1);"

    // ==================== ST_Centroid ====================
    qt_st_centroid_01 "SELECT ST_AsText(ST_Centroid(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\")));"
    qt_st_centroid_02 "SELECT ST_AsText(ST_Centroid(ST_Point(5, 5)));"
    qt_st_centroid_03 "SELECT ST_AsText(ST_Centroid(ST_LineFromText(\"LINESTRING (0 0, 10 0)\")));"
    qt_st_centroid_04 "SELECT ST_Centroid(NULL);"

    // ==================== ST_ConvexHull ====================
    qt_st_convexhull_01 "SELECT ST_AsText(ST_ConvexHull(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\")));"
    qt_st_convexhull_02 "SELECT ST_AsText(ST_ConvexHull(ST_LineFromText(\"LINESTRING (0 0, 10 0, 5 5)\")));"
    qt_st_convexhull_03 "SELECT ST_ConvexHull(NULL);"

    // ==================== ST_Simplify ====================
    qt_st_simplify_01 "SELECT ST_AsText(ST_Simplify(ST_LineFromText(\"LINESTRING (0 0, 5 1, 10 0)\"), 0.1));"
    qt_st_simplify_02 "SELECT ST_AsText(ST_Simplify(ST_LineFromText(\"LINESTRING (0 0, 5 1, 10 0)\"), 5));"
    qt_st_simplify_03 "SELECT ST_AsText(ST_Simplify(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), 0.1));"
    qt_st_simplify_04 "SELECT ST_Simplify(NULL, 1);"

    // ==================== ST_Union ====================
    qt_st_union_01 "SELECT ST_AsText(ST_Union(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((5 0, 15 0, 15 10, 5 10, 5 0))\"))) IS NOT NULL;"
    qt_st_union_02 "SELECT ST_AsText(ST_Union(ST_Polygon(\"POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))\"), ST_Polygon(\"POLYGON ((10 10, 15 10, 15 15, 10 15, 10 10))\"))) IS NOT NULL;"
    qt_st_union_03 "SELECT ST_AsText(ST_Union(ST_Point(5, 5), ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"))) IS NOT NULL;"
    qt_st_union_04 "SELECT ST_Union(NULL, ST_Point(1, 1));"
    qt_st_union_05 "SELECT ST_Union(ST_Point(1, 1), NULL);"

    // ==================== ST_Intersection ====================
    qt_st_intersection_01 "SELECT ST_AsText(ST_Intersection(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((5 0, 15 0, 15 10, 5 10, 5 0))\")));"
    qt_st_intersection_02 "SELECT ST_AsText(ST_Intersection(ST_Polygon(\"POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))\"), ST_Polygon(\"POLYGON ((10 10, 15 10, 15 15, 10 15, 10 10))\")));"
    qt_st_intersection_03 "SELECT ST_AsText(ST_Intersection(ST_Point(5, 5), ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\")));"
    qt_st_intersection_04 "SELECT ST_Intersection(NULL, ST_Point(1, 1));"

    // ==================== ST_Overlaps ====================
    qt_st_overlaps_01 "SELECT ST_Overlaps(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((5 0, 15 0, 15 10, 5 10, 5 0))\"));"
    qt_st_overlaps_02 "SELECT ST_Overlaps(ST_Polygon(\"POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))\"), ST_Polygon(\"POLYGON ((10 10, 15 10, 15 15, 10 15, 10 10))\"));"
    qt_st_overlaps_03 "SELECT ST_Overlaps(ST_Polygon(\"POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0))\"), ST_Polygon(\"POLYGON ((5 5, 10 5, 10 10, 5 10, 5 5))\"));"
    qt_st_overlaps_04 "SELECT ST_Overlaps(NULL, ST_Point(1, 1));"

    // ==================== ST_Crosses ====================
    qt_st_crosses_01 "SELECT ST_Crosses(ST_LineFromText(\"LINESTRING (-5 5, 15 5)\"), ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"
    qt_st_crosses_02 "SELECT ST_Crosses(ST_LineFromText(\"LINESTRING (2 2, 8 8)\"), ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"
    qt_st_crosses_03 "SELECT ST_Crosses(ST_LineFromText(\"LINESTRING (0 0, 10 10)\"), ST_LineFromText(\"LINESTRING (0 10, 10 0)\"));"
    qt_st_crosses_04 "SELECT ST_Crosses(ST_LineFromText(\"LINESTRING (0 0, 10 0)\"), ST_LineFromText(\"LINESTRING (0 5, 10 5)\"));"
    qt_st_crosses_05 "SELECT ST_Crosses(NULL, ST_Point(1, 1));"
}
