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

// Bounding box accessor functions st_xmax/st_xmin/st_ymax/st_ymin
// (Trino compatible), implemented on top of GeoShape::bounding_box.
suite("nereids_scalar_fn_st_bounding_box") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'
	qt_sql_st_xmax_Varchar "select st_xmax(st_point(x_lng, x_lat)) from fn_test order by 1"
	qt_sql_st_xmax_Varchar_notnull "select st_xmax(st_point(x_lng, x_lat)) from fn_test_not_nullable order by 1"
	qt_sql_st_xmin_Varchar "select st_xmin(st_point(x_lng, x_lat)) from fn_test order by 1"
	qt_sql_st_xmin_Varchar_notnull "select st_xmin(st_point(x_lng, x_lat)) from fn_test_not_nullable order by 1"
	qt_sql_st_ymax_Varchar "select st_ymax(st_point(x_lng, x_lat)) from fn_test order by 1"
	qt_sql_st_ymax_Varchar_notnull "select st_ymax(st_point(x_lng, x_lat)) from fn_test_not_nullable order by 1"
	qt_sql_st_ymin_Varchar "select st_ymin(st_point(x_lng, x_lat)) from fn_test order by 1"
	qt_sql_st_ymin_Varchar_notnull "select st_ymin(st_point(x_lng, x_lat)) from fn_test_not_nullable order by 1"
	qt_sql_st_xmax_polygon "select st_xmax(st_polygon(polygon_wkt)) from fn_test order by 1"
	qt_sql_st_ymin_polygon "select st_ymin(st_polygon(polygon_wkt)) from fn_test order by 1"
	qt_sql_st_xmax_linestring "select st_xmax(st_linestringfromtext('LINESTRING (1 1, 3 2, 2 4)'))"
	qt_sql_st_ymin_linestring "select st_ymin(st_linestringfromtext('LINESTRING (1 1, 3 2, 2 4)'))"
	qt_sql_st_xmax_invalid "select st_xmax('not a geometry')"
	qt_sql_st_xmax_null "select st_xmax(NULL)"
}
