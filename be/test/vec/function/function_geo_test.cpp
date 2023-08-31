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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stdint.h>

#include <iomanip>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "function_test_util.h"
#include "geo/geo_common.h"
#include "geo/util/GeoCircle.h"
#include "geo/util/GeoPoint.h"
#include "geo/util/GeoShape.h"
#include "gtest/gtest_pred_impl.h"
#include "testutil/any_type.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_geometry.h"

namespace doris::vectorized {
using namespace ut_type;

TEST(VGeoFunctionsTest, function_geo_st_point_test) {
    std::string func_name = "st_point";
    {
        InputTypeSet input_types = {TypeIndex::Float64, TypeIndex::Float64};

        DataSet data_set = {{{(double)24.7, (double)56.7}, std::string("POINT (24.7 56.7)")}};

        static_cast<void>(check_function<DataTypeGeometry, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_as_text) {
    std::string func_name = "st_astext";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoPoint point;
        auto cur_res = point.from_coord(24.7, 56.7);
        EXPECT_TRUE(cur_res == GEO_PARSE_OK);
        std::string buf;
        point.encode_to(&buf);

        DataSet data_set = {{{buf}, std::string("POINT (24.7 56.7)")}, {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_as_wkt) {
    std::string func_name = "st_aswkt";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoPoint point;
        auto cur_res = point.from_coord(24.7, 56.7);
        EXPECT_TRUE(cur_res == GEO_PARSE_OK);
        std::string buf;
        point.encode_to(&buf);

        DataSet data_set = {{{buf}, std::string("POINT (24.7 56.7)")}, {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_x) {
    std::string func_name = "st_x";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoPoint point;
        auto cur_res = point.from_coord(24.7, 56.7);
        EXPECT_TRUE(cur_res == GEO_PARSE_OK);
        std::string buf;
        point.encode_to(&buf);

        DataSet data_set = {{{buf}, (double)24.7}, {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_y) {
    std::string func_name = "st_y";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoPoint point;
        auto cur_res = point.from_coord(24.7, 56.7);
        EXPECT_TRUE(cur_res == GEO_PARSE_OK);
        std::string buf;
        point.encode_to(&buf);

        DataSet data_set = {{{buf}, (double)56.7}, {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_distance_sphere) {
    std::string func_name = "st_distance_sphere";
    {
        InputTypeSet input_types = {TypeIndex::Float64, TypeIndex::Float64, TypeIndex::Float64,
                                    TypeIndex::Float64};

        DataSet data_set = {
                {{(double)116.35620117, (double)39.939093, (double)116.4274406433,
                  (double)39.9020987219},
                 (double)7336.9135549995917},
                {{(double)116.35620117, (double)39.939093, (double)116.4274406433, Null()}, Null()},
                {{(double)116.35620117, (double)39.939093, Null(), (double)39.9020987219}, Null()},
                {{(double)116.35620117, Null(), (double)116.4274406433, (double)39.9020987219},
                 Null()},
                {{Null(), (double)39.939093, (double)116.4274406433, (double)39.9020987219},
                 Null()}};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_angle_sphere) {
    std::string func_name = "st_angle_sphere";
    {
        InputTypeSet input_types = {TypeIndex::Float64, TypeIndex::Float64, TypeIndex::Float64,
                                    TypeIndex::Float64};

        DataSet data_set = {
                {{(double)116.35620117, (double)39.939093, (double)116.4274406433,
                  (double)39.9020987219},
                 (double)0.0659823452409903},
                {{(double)116.35620117, (double)39.939093, (double)116.4274406433, Null()}, Null()},
                {{(double)116.35620117, (double)39.939093, Null(), (double)39.9020987219}, Null()},
                {{(double)116.35620117, Null(), (double)116.4274406433, (double)39.9020987219},
                 Null()},
                {{Null(), (double)39.939093, (double)116.4274406433, (double)39.9020987219},
                 Null()}};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_angle) {
    std::string func_name = "st_angle";
    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};

        GeoPoint point1;
        auto cur_res1 = point1.from_coord(1, 0);
        EXPECT_TRUE(cur_res1 == GEO_PARSE_OK);
        GeoPoint point2;
        auto cur_res2 = point2.from_coord(0, 0);
        EXPECT_TRUE(cur_res2 == GEO_PARSE_OK);
        GeoPoint point3;
        auto cur_res3 = point3.from_coord(0, 1);
        EXPECT_TRUE(cur_res3 == GEO_PARSE_OK);
        std::string buf1;
        point1.encode_to(&buf1);
        std::string buf2;
        point2.encode_to(&buf2);
        std::string buf3;
        point3.encode_to(&buf3);

        DataSet data_set = {{{buf1, buf2, buf3}, (double)4.71238898038469},
                            {{buf1, buf2, Null()}, Null()},
                            {{buf1, Null(), buf3}, Null()},
                            {{Null(), buf2, buf3}, Null()}};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_azimuth) {
    std::string func_name = "st_azimuth";
    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

        GeoPoint point1;
        auto cur_res1 = point1.from_coord(0, 0);
        EXPECT_TRUE(cur_res1 == GEO_PARSE_OK);
        GeoPoint point2;
        auto cur_res2 = point2.from_coord(1, 0);
        EXPECT_TRUE(cur_res2 == GEO_PARSE_OK);

        std::string buf1;
        point1.encode_to(&buf1);
        std::string buf2;
        point2.encode_to(&buf2);

        DataSet data_set = {{{buf1, buf2}, (double)1.5707963267948966},
                            {{buf1, Null()}, Null()},
                            {{Null(), buf2}, Null()}};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_contains) {
    std::string func_name = "st_contains";
    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

        std::string buf1;
        std::string buf2;
        std::string buf3;
        GeoParseStatus status;

        std::string shape1 = std::string("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(shape1.data(), shape1.size(), &status));
        EXPECT_TRUE(status == GEO_PARSE_OK);
        EXPECT_TRUE(shape != nullptr);
        shape->encode_to(&buf1);

        GeoPoint point1;
        status = point1.from_coord(5, 5);
        EXPECT_TRUE(status == GEO_PARSE_OK);
        point1.encode_to(&buf2);

        GeoPoint point2;
        status = point2.from_coord(50, 50);
        EXPECT_TRUE(status == GEO_PARSE_OK);
        point2.encode_to(&buf3);

        DataSet data_set = {{{buf1, buf2}, (uint8_t)1},
                            {{buf1, buf3}, (uint8_t)0},
                            {{buf1, Null()}, Null()},
                            {{Null(), buf3}, Null()}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_circle) {
    std::string func_name = "st_circle";
    {
        InputTypeSet input_types = {TypeIndex::Float64, TypeIndex::Float64, TypeIndex::Float64};

        DataSet data_set = {{{(double)111, (double)64, (double)10000}, std::string("CIRCLE ((111 64), 10000)")},
                            {{Null(), (double)64, (double)10000}, Null()},
                            {{(double)111, Null(), (double)10000}, Null()},
                            {{(double)111, (double)64, Null()}, Null()}};

        static_cast<void>(check_function<DataTypeGeometry, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_geometryfromtext) {
    std::string func_name = "st_geometryfromtext";
    {
        InputTypeSet input_types = {TypeIndex::String};

        DataSet data_set = {{{std::string("LINESTRING (1 1, 2 2)")}, std::string("LINESTRING (1 1, 2 2)")}};

        static_cast<void>(check_function<DataTypeGeometry, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_geomfromtext) {
    std::string func_name = "st_geomfromtext";
    {
        InputTypeSet input_types = {TypeIndex::String};

        DataSet data_set = {{{std::string("LINESTRING (1 1, 2 2)")}, std::string("LINESTRING (1 1, 2 2)")}};

        static_cast<void>(check_function<DataTypeGeometry, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_linefromtext) {
    std::string func_name = "st_linefromtext";
    {
        InputTypeSet input_types = {TypeIndex::String};

        DataSet data_set = {{{std::string("LINESTRING (1 1, 2 2)")}, std::string("LINESTRING (1 1, 2 2)")}};

        static_cast<void>(check_function<DataTypeGeometry, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_polygon) {
    std::string func_name = "st_polygon";
    {
        InputTypeSet input_types = {TypeIndex::String};

        DataSet data_set = {{{std::string("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")}, std::string("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")}};

        static_cast<void>(check_function<DataTypeGeometry, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_polygonfromtext) {
    std::string func_name = "st_polygonfromtext";
    {
        InputTypeSet input_types = {TypeIndex::String};

        DataSet data_set = {{{std::string("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")}, std::string("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")}};

        static_cast<void>(check_function<DataTypeGeometry, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_area_square_meters) {
    std::string func_name = "st_area_square_meters";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoCircle circle;
        auto cur_res = circle.to_s2cap(0, 0, 1);
        EXPECT_TRUE(cur_res == GEO_PARSE_OK);
        std::string buf;
        circle.encode_to(&buf);
        DataSet data_set = {{{buf}, (double)3.1415926535897869}, {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_area_square_km) {
    std::string func_name = "st_area_square_km";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoParseStatus status;
        std::string buf;
        std::string input = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), &status));
        EXPECT_TRUE(shape != nullptr);
        EXPECT_TRUE(status == GEO_PARSE_OK);
        shape->encode_to(&buf);
        DataSet data_set = {{{buf}, (double)12364.036567076409}, {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }
}


TEST(VGeoFunctionsTest, function_geo_st_within) {
    std::string func_name = "st_within";
    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

        std::string buf1;
        std::string buf2;
        std::string buf3;
        GeoParseStatus status;

        std::string shape1 = std::string("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(shape1.data(), shape1.size(), &status));
        EXPECT_TRUE(status == GEO_PARSE_OK);
        EXPECT_TRUE(shape != nullptr);
        shape->encode_to(&buf1);

        GeoPoint point1;
        status = point1.from_coord(5, 5);
        EXPECT_TRUE(status == GEO_PARSE_OK);
        point1.encode_to(&buf2);

        GeoPoint point2;
        status = point2.from_coord(50, 50);
        EXPECT_TRUE(status == GEO_PARSE_OK);
        point2.encode_to(&buf3);

        DataSet data_set = {{{buf2, buf1}, (uint8_t)1},
                            {{buf3, buf1}, (uint8_t)0},
                            {{buf1, Null()}, Null()},
                            {{Null(), buf3}, Null()}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_geometryfromwkb) {
    std::string func_name = "st_geometryfromwkb";
    {
        InputTypeSet input_types = {TypeIndex::String};

        DataSet data_set = {{{std::string("010200000002000000000000000000f03f000000000000f03f00000000000000400000000000000040")}, std::string("LINESTRING (1 1, 2 2)")}};

        static_cast<void>(check_function<DataTypeGeometry, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_geomfromwkb) {
    std::string func_name = "st_geomfromwkb";
    {
        InputTypeSet input_types = {TypeIndex::String};

        DataSet data_set = {{{std::string("010200000002000000000000000000f03f000000000000f03f00000000000000400000000000000040")}, std::string("LINESTRING (1 1, 2 2)")}};

        static_cast<void>(check_function<DataTypeGeometry, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_asbinary) {
    std::string func_name = "st_asbinary";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoPoint point;
        auto cur_res = point.from_coord(24.7, 56.7);
        EXPECT_TRUE(cur_res == GEO_PARSE_OK);
        std::string buf;
        point.encode_to(&buf);

        DataSet data_set = {{{buf}, std::string("01010000003333333333b338409a99999999594c40")}, {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_asgeojson) {
    std::string func_name = "st_asgeojson";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoPoint point;
        auto cur_res = point.from_coord(24.7, 56.7);
        EXPECT_TRUE(cur_res == GEO_PARSE_OK);
        std::string buf;
        point.encode_to(&buf);

        DataSet data_set = {{{buf}, std::string("{\"type\":\"Point\",\"coordinates\":[24.7,56.7]}")}, {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_geometryfromgeojson) {
    std::string func_name = "st_geometryfromgeojson";
    {
        InputTypeSet input_types = {TypeIndex::String};

        DataSet data_set = {{{std::string("{\"type\":\"Point\",\"coordinates\":[24.7,56.7]}")}, std::string("POINT (24.7 56.7)")}};

        static_cast<void>(check_function<DataTypeGeometry, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_geomfromgeojson) {
    std::string func_name = "st_geomfromgeojson";
    {
        InputTypeSet input_types = {TypeIndex::String};

        DataSet data_set = {{{std::string("{\"type\":\"Point\",\"coordinates\":[24.7,56.7]}")}, std::string("POINT (24.7 56.7)")}};

        static_cast<void>(check_function<DataTypeGeometry, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_pointn) {
    std::string func_name = "st_pointn";
    {
        InputTypeSet input_types = {TypeIndex::String,TypeIndex::Int32};

        GeoParseStatus status;
        std::string buf;
        std::string input = "LINESTRING (1 1, 2 2)";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), &status));
        EXPECT_TRUE(shape != nullptr);
        EXPECT_TRUE(status == GEO_PARSE_OK);
        shape->encode_to(&buf);

        DataSet data_set = {{{buf,1}, std::string("POINT (1 1)")}};

        static_cast<void>(check_function<DataTypeGeometry, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_startpoint) {
    std::string func_name = "st_startpoint";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoParseStatus status;
        std::string buf;
        std::string input = "LINESTRING (1 1, 2 2)";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), &status));
        EXPECT_TRUE(shape != nullptr);
        EXPECT_TRUE(status == GEO_PARSE_OK);
        shape->encode_to(&buf);

        DataSet data_set = {{{buf}, std::string("POINT (1 1)")}};

        static_cast<void>(check_function<DataTypeGeometry, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_endpoint) {
    std::string func_name = "st_endpoint";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoParseStatus status;
        std::string buf;
        std::string input = "LINESTRING (1 1, 2 2)";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), &status));
        EXPECT_TRUE(shape != nullptr);
        EXPECT_TRUE(status == GEO_PARSE_OK);
        shape->encode_to(&buf);

        DataSet data_set = {{{buf}, std::string("POINT (2 2)")}};

        static_cast<void>(check_function<DataTypeGeometry, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_dimension) {
    std::string func_name = "st_dimension";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoParseStatus status;
        std::string buf;
        std::string input = "LINESTRING (1 1, 2 2)";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), &status));
        EXPECT_TRUE(shape != nullptr);
        EXPECT_TRUE(status == GEO_PARSE_OK);
        shape->encode_to(&buf);

        DataSet data_set = {{{buf}, (int32_t)1}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_length) {
    std::string func_name = "st_length";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoParseStatus status;
        std::string buf;
        std::string input = "LINESTRING (1 1, 2 2)";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), &status));
        EXPECT_TRUE(shape != nullptr);
        EXPECT_TRUE(status == GEO_PARSE_OK);
        shape->encode_to(&buf);

        DataSet data_set = {{{buf}, (double)157225.67882104582}};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_isclosed) {
    std::string func_name = "st_isclosed";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoParseStatus status;
        std::string buf;
        std::string input = "LINESTRING (1 1, 2 2)";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), &status));
        EXPECT_TRUE(shape != nullptr);
        EXPECT_TRUE(status == GEO_PARSE_OK);
        shape->encode_to(&buf);

        DataSet data_set = {{{buf}, (uint8_t)0}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_iscollection) {
    std::string func_name = "st_iscollection";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoParseStatus status;
        std::string buf;
        std::string input = "LINESTRING (1 1, 2 2)";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), &status));
        EXPECT_TRUE(shape != nullptr);
        EXPECT_TRUE(status == GEO_PARSE_OK);
        shape->encode_to(&buf);

        DataSet data_set = {{{buf}, (uint8_t)0}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_isring) {
    std::string func_name = "st_isring";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoParseStatus status;
        std::string buf;
        std::string input = "LINESTRING (1 1, 2 2)";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), &status));
        EXPECT_TRUE(shape != nullptr);
        EXPECT_TRUE(status == GEO_PARSE_OK);
        shape->encode_to(&buf);

        DataSet data_set = {{{buf}, (uint8_t)0}};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_numgeometries) {
    std::string func_name = "st_numgeometries";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoParseStatus status;
        std::string buf;
        std::string input = "LINESTRING (1 1, 2 2)";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), &status));
        EXPECT_TRUE(shape != nullptr);
        EXPECT_TRUE(status == GEO_PARSE_OK);
        shape->encode_to(&buf);

        DataSet data_set = {{{buf}, (int64_t)1}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_numpoints) {
    std::string func_name = "st_numpoints";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoParseStatus status;
        std::string buf;
        std::string input = "LINESTRING (1 1, 2 2)";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), &status));
        EXPECT_TRUE(shape != nullptr);
        EXPECT_TRUE(status == GEO_PARSE_OK);
        shape->encode_to(&buf);

        DataSet data_set = {{{buf}, (int64_t)2}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_geometrytype) {
    std::string func_name = "st_geometrytype";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoParseStatus status;
        std::string buf;
        std::string input = "LINESTRING (1 1, 2 2)";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), &status));
        EXPECT_TRUE(shape != nullptr);
        EXPECT_TRUE(status == GEO_PARSE_OK);
        shape->encode_to(&buf);

        DataSet data_set = {{{buf}, std::string("ST_LineString")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_centroid) {
    std::string func_name = "st_centroid";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoParseStatus status;
        std::string buf;
        std::string input = "LINESTRING (1 1, 2 2)";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), &status));
        EXPECT_TRUE(shape != nullptr);
        EXPECT_TRUE(status == GEO_PARSE_OK);
        shape->encode_to(&buf);

        DataSet data_set = {{{buf}, std::string("POINT (1.49988573656 1.50005709148)")}};

        static_cast<void>(check_function<DataTypeGeometry, true>(func_name, input_types, data_set));
    }
}

} // namespace doris::vectorized
