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
suite("test_geos_functions") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set batch_size = 4096;"

    // ==================== ST_IsValid ====================
    // valid polygon
    qt_st_isvalid_01 "SELECT ST_IsValid(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"
    // valid point
    qt_st_isvalid_02 "SELECT ST_IsValid(ST_Point(1, 2));"
    // valid linestring
    qt_st_isvalid_03 "SELECT ST_IsValid(ST_LineFromText(\"LINESTRING (0 0, 10 10, 20 0)\"));"
    // NULL input
    qt_st_isvalid_04 "SELECT ST_IsValid(NULL);"

    // ==================== ST_MakeValid ====================
    // already valid polygon should return as-is
    qt_st_makevalid_01 "SELECT ST_AsText(ST_MakeValid(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\")));"
    // NULL input
    qt_st_makevalid_02 "SELECT ST_MakeValid(NULL);"
    // valid point should return as-is
    qt_st_makevalid_03 "SELECT ST_AsText(ST_MakeValid(ST_Point(1, 2)));"

    // ==================== ST_Buffer ====================
    // buffer around a point
    qt_st_buffer_01 "SELECT ST_AsText(ST_Buffer(ST_Point(0, 0), 1)) IS NOT NULL;"
    // buffer with zero distance
    qt_st_buffer_02 "SELECT ST_AsText(ST_Buffer(ST_Point(0, 0), 0)) IS NOT NULL;"
    // buffer around a polygon
    qt_st_buffer_03 "SELECT ST_AsText(ST_Buffer(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), 1)) IS NOT NULL;"
    // NULL input
    qt_st_buffer_04 "SELECT ST_Buffer(NULL, 1);"

    // ==================== ST_Centroid ====================
    // centroid of a square polygon (should be center)
    qt_st_centroid_01 "SELECT ST_AsText(ST_Centroid(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\")));"
    // centroid of a point (should be the point itself)
    qt_st_centroid_02 "SELECT ST_AsText(ST_Centroid(ST_Point(5, 5)));"
    // centroid of a linestring
    qt_st_centroid_03 "SELECT ST_AsText(ST_Centroid(ST_LineFromText(\"LINESTRING (0 0, 10 0)\")));"
    // NULL input
    qt_st_centroid_04 "SELECT ST_Centroid(NULL);"

    // ==================== ST_ConvexHull ====================
    // convex hull of a polygon (convex polygon returns itself)
    qt_st_convexhull_01 "SELECT ST_AsText(ST_ConvexHull(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\")));"
    // convex hull of a linestring
    qt_st_convexhull_02 "SELECT ST_AsText(ST_ConvexHull(ST_LineFromText(\"LINESTRING (0 0, 10 0, 5 5)\")));"
    // NULL input
    qt_st_convexhull_03 "SELECT ST_ConvexHull(NULL);"

    // ==================== ST_Simplify ====================
    // simplify a linestring with small tolerance (should keep all points)
    qt_st_simplify_01 "SELECT ST_AsText(ST_Simplify(ST_LineFromText(\"LINESTRING (0 0, 5 1, 10 0)\"), 0.1));"
    // simplify a linestring with large tolerance (should reduce points)
    qt_st_simplify_02 "SELECT ST_AsText(ST_Simplify(ST_LineFromText(\"LINESTRING (0 0, 5 1, 10 0)\"), 5));"
    // simplify a polygon
    qt_st_simplify_03 "SELECT ST_AsText(ST_Simplify(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), 0.1));"
    // NULL input
    qt_st_simplify_04 "SELECT ST_Simplify(NULL, 1);"

    // ==================== ST_Union ====================
    // union of two overlapping polygons
    qt_st_union_01 "SELECT ST_AsText(ST_Union(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((5 0, 15 0, 15 10, 5 10, 5 0))\"))) IS NOT NULL;"
    // union of two non-overlapping polygons
    qt_st_union_02 "SELECT ST_AsText(ST_Union(ST_Polygon(\"POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))\"), ST_Polygon(\"POLYGON ((10 10, 15 10, 15 15, 10 15, 10 10))\"))) IS NOT NULL;"
    // union of a point and a polygon
    qt_st_union_03 "SELECT ST_AsText(ST_Union(ST_Point(5, 5), ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"))) IS NOT NULL;"
    // NULL input
    qt_st_union_04 "SELECT ST_Union(NULL, ST_Point(1, 1));"
    qt_st_union_05 "SELECT ST_Union(ST_Point(1, 1), NULL);"

    // ==================== ST_Intersection ====================
    // intersection of two overlapping polygons
    qt_st_intersection_01 "SELECT ST_AsText(ST_Intersection(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((5 0, 15 0, 15 10, 5 10, 5 0))\")));"
    // intersection of two non-overlapping polygons (should be empty)
    qt_st_intersection_02 "SELECT ST_AsText(ST_Intersection(ST_Polygon(\"POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))\"), ST_Polygon(\"POLYGON ((10 10, 15 10, 15 15, 10 15, 10 10))\")));"
    // intersection of a point inside a polygon (should be the point)
    qt_st_intersection_03 "SELECT ST_AsText(ST_Intersection(ST_Point(5, 5), ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\")));"
    // NULL input
    qt_st_intersection_04 "SELECT ST_Intersection(NULL, ST_Point(1, 1));"

    // ==================== ST_Overlaps ====================
    // two overlapping polygons
    qt_st_overlaps_01 "SELECT ST_Overlaps(ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"), ST_Polygon(\"POLYGON ((5 0, 15 0, 15 10, 5 10, 5 0))\"));"
    // two non-overlapping polygons
    qt_st_overlaps_02 "SELECT ST_Overlaps(ST_Polygon(\"POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))\"), ST_Polygon(\"POLYGON ((10 10, 15 10, 15 15, 10 15, 10 10))\"));"
    // one polygon contains the other (overlaps is false for containment)
    qt_st_overlaps_03 "SELECT ST_Overlaps(ST_Polygon(\"POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0))\"), ST_Polygon(\"POLYGON ((5 5, 10 5, 10 10, 5 10, 5 5))\"));"
    // NULL input
    qt_st_overlaps_04 "SELECT ST_Overlaps(NULL, ST_Point(1, 1));"

    // ==================== ST_Crosses ====================
    // a line crosses a polygon
    qt_st_crosses_01 "SELECT ST_Crosses(ST_LineFromText(\"LINESTRING (-5 5, 15 5)\"), ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"
    // a line inside a polygon (does not cross)
    qt_st_crosses_02 "SELECT ST_Crosses(ST_LineFromText(\"LINESTRING (2 2, 8 8)\"), ST_Polygon(\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"));"
    // two crossing lines
    qt_st_crosses_03 "SELECT ST_Crosses(ST_LineFromText(\"LINESTRING (0 0, 10 10)\"), ST_LineFromText(\"LINESTRING (0 10, 10 0)\"));"
    // two parallel lines (do not cross)
    qt_st_crosses_04 "SELECT ST_Crosses(ST_LineFromText(\"LINESTRING (0 0, 10 0)\"), ST_LineFromText(\"LINESTRING (0 5, 10 5)\"));"
    // NULL input
    qt_st_crosses_05 "SELECT ST_Crosses(NULL, ST_Point(1, 1));"
}
