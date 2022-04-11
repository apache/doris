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

#include <gtest/gtest.h>
#include <time.h>

#include <string>

#include "function_test_util.h"
#include "geo/geo_types.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {
using namespace ut_type;

TEST(function_geo_test, function_geo_st_point_test) {
    std::string func_name = "st_point";
    {
        InputTypeSet input_types = {TypeIndex::Float64, TypeIndex::Float64};

        GeoPoint point;
        auto cur_res = point.from_coord(24.7, 56.7);
        ASSERT_TRUE(cur_res == GEO_PARSE_OK);
        std::string buf;
        point.encode_to(&buf);

        DataSet data_set = {
                {{(double) 24.7, (double) 56.7}, buf},
                {{Null(), (double) 5}, Null()},
                {{(double) 5, Null()}, Null()}};

        check_function<DataTypeString, true>(func_name, input_types, data_set);
    }
}

TEST(function_geo_test, function_geo_st_as_text) {
    std::string func_name = "st_astext";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoPoint point;
        auto cur_res = point.from_coord(24.7, 56.7);
        ASSERT_TRUE(cur_res == GEO_PARSE_OK);
        std::string buf;
        point.encode_to(&buf);

        DataSet data_set = {
                {{buf}, std::string("POINT (24.7 56.7)")},
                {{Null()}, Null()}};

        check_function<DataTypeString, true>(func_name, input_types, data_set);
    }
}

TEST(function_geo_test, function_geo_st_as_wkt) {
    std::string func_name = "st_aswkt";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoPoint point;
        auto cur_res = point.from_coord(24.7, 56.7);
        ASSERT_TRUE(cur_res == GEO_PARSE_OK);
        std::string buf;
        point.encode_to(&buf);

        DataSet data_set = {
                {{buf}, std::string("POINT (24.7 56.7)")},
                {{Null()}, Null()}};

        check_function<DataTypeString, true>(func_name, input_types, data_set);
    }
}

TEST(function_geo_test, function_geo_st_x) {
    std::string func_name = "st_x";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoPoint point;
        auto cur_res = point.from_coord(24.7, 56.7);
        ASSERT_TRUE(cur_res == GEO_PARSE_OK);
        std::string buf;
        point.encode_to(&buf);

        DataSet data_set = {
                {{buf}, (double) 24.7},
                {{Null()}, Null()}};

        check_function<DataTypeFloat64, true>(func_name, input_types, data_set);
    }
}

TEST(function_geo_test, function_geo_st_y) {
    std::string func_name = "st_y";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoPoint point;
        auto cur_res = point.from_coord(24.7, 56.7);
        ASSERT_TRUE(cur_res == GEO_PARSE_OK);
        std::string buf;
        point.encode_to(&buf);

        DataSet data_set = {
                {{buf}, (double) 56.7},
                {{Null()}, Null()}};

        check_function<DataTypeFloat64, true>(func_name, input_types, data_set);
    }
}

TEST(function_geo_test, function_geo_st_distance_sphere) {
    std::string func_name = "st_distance_sphere";
    {
        InputTypeSet input_types = {TypeIndex::Float64, TypeIndex::Float64, TypeIndex::Float64, TypeIndex::Float64};

        DataSet data_set = {
                {{(double) 116.35620117, (double) 39.939093, (double) 116.4274406433, (double) 39.9020987219}, (double) 7336.9135549995917},
                {{(double) 116.35620117, (double) 39.939093, (double) 116.4274406433, Null()}, Null()},
                {{(double) 116.35620117, (double) 39.939093, Null(), (double) 39.9020987219}, Null()},
                {{(double) 116.35620117, Null(), (double) 116.4274406433, (double) 39.9020987219}, Null()},
                {{Null(), (double) 39.939093, (double) 116.4274406433, (double) 39.9020987219}, Null()}};

        check_function<DataTypeFloat64, true>(func_name, input_types, data_set);
    }
}

TEST(function_geo_test, function_geo_st_contains) {
    std::string func_name = "st_contains";
    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

        std::string buf1;
        std::string buf2;
        std::string buf3;
        GeoParseStatus status;

        std::string shape1 = std::string("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
        std::unique_ptr<GeoShape> shape(
                GeoShape::from_wkt(shape1.data(), shape1.size(), &status));
        ASSERT_TRUE(status == GEO_PARSE_OK);
        ASSERT_TRUE(shape != nullptr);
        shape->encode_to(&buf1);

        GeoPoint point1;
        status = point1.from_coord(5, 5);
        ASSERT_TRUE(status == GEO_PARSE_OK);
        point1.encode_to(&buf2);

        GeoPoint point2;
        status = point2.from_coord(50, 50);
        ASSERT_TRUE(status == GEO_PARSE_OK);
        point2.encode_to(&buf3);

        DataSet data_set = {
                {{buf1, buf2}, (uint8_t) 1},
                {{buf1, buf3}, (uint8_t) 0},
                {{buf1, Null()}, Null()},
                {{Null(), buf3}, Null()}};

        check_function<DataTypeUInt8 , true>(func_name, input_types, data_set);
    }
}

TEST(function_geo_test, function_geo_st_circle) {
    std::string func_name = "st_circle";
    {
        InputTypeSet input_types = {TypeIndex::Float64, TypeIndex::Float64, TypeIndex::Float64};

        GeoCircle circle;
        std::string buf;
        auto value = circle.init(111, 64, 10000);
        ASSERT_TRUE(value == GEO_PARSE_OK);
        circle.encode_to(&buf);
        DataSet data_set = {
                {{(double) 111, (double) 64, (double) 10000}, buf},
                {{Null(), (double) 64, (double) 10000}, Null()},
                {{(double) 111, Null(), (double) 10000}, Null()},
                {{(double) 111, (double) 64, Null()}, Null()}};

        check_function<DataTypeString , true>(func_name, input_types, data_set);
    }
}

TEST(function_geo_test, function_geo_st_geometryfromtext) {
    std::string func_name = "st_geometryfromtext";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoParseStatus status;
        std::string buf;
        std::string input = "LINESTRING (1 1, 2 2)";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), &status));
        ASSERT_TRUE(shape != nullptr);
        ASSERT_TRUE(status == GEO_PARSE_OK);
        shape->encode_to(&buf);
        DataSet data_set = {
                {{std::string("LINESTRING (1 1, 2 2)")}, buf},
                {{Null()}, Null()}};

        check_function<DataTypeString , true>(func_name, input_types, data_set);
    }
}

TEST(function_geo_test, function_geo_st_geomfromtext) {
    std::string func_name = "st_geomfromtext";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoParseStatus status;
        std::string buf;
        std::string input = "LINESTRING (1 1, 2 2)";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), &status));
        ASSERT_TRUE(shape != nullptr);
        ASSERT_TRUE(status == GEO_PARSE_OK);
        shape->encode_to(&buf);
        DataSet data_set = {
            {{std::string("LINESTRING (1 1, 2 2)")}, buf},
            {{Null()}, Null()}};

        check_function<DataTypeString , true>(func_name, input_types, data_set);
    }
}

TEST(function_geo_test, function_geo_st_linefromtext) {
    std::string func_name = "st_linefromtext";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoParseStatus status;
        std::string buf;
        std::string input = "LINESTRING (1 1, 2 2)";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), &status));
        ASSERT_TRUE(shape != nullptr);
        ASSERT_TRUE(status == GEO_PARSE_OK);
        shape->encode_to(&buf);
        DataSet data_set = {
            {{std::string("LINESTRING (1 1, 2 2)")}, buf},
            {{Null()}, Null()}};

        check_function<DataTypeString , true>(func_name, input_types, data_set);
    }
}

TEST(function_geo_test, function_geo_st_linestringfromtext) {
    std::string func_name = "st_linestringfromtext";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoParseStatus status;
        std::string buf;
        std::string input = "LINESTRING (1 1, 2 2)";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), &status));
        ASSERT_TRUE(shape != nullptr);
        ASSERT_TRUE(status == GEO_PARSE_OK);
        shape->encode_to(&buf);
        DataSet data_set = {
                {{std::string("LINESTRING (1 1, 2 2)")}, buf},
                {{Null()}, Null()}};

        check_function<DataTypeString , true>(func_name, input_types, data_set);
    }
}

TEST(function_geo_test, function_geo_st_polygon) {
    std::string func_name = "st_polygon";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoParseStatus status;
        std::string buf;
        std::string input = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), &status));
        ASSERT_TRUE(shape != nullptr);
        ASSERT_TRUE(status == GEO_PARSE_OK);
        shape->encode_to(&buf);
        DataSet data_set = {
                {{std::string("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")}, buf},
                {{Null()}, Null()}};

        check_function<DataTypeString , true>(func_name, input_types, data_set);
    }
}

TEST(function_geo_test, function_geo_st_polygonfromtext) {
    std::string func_name = "st_polygonfromtext";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoParseStatus status;
        std::string buf;
        std::string input = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), &status));
        ASSERT_TRUE(shape != nullptr);
        ASSERT_TRUE(status == GEO_PARSE_OK);
        shape->encode_to(&buf);
        DataSet data_set = {
                {{std::string("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")}, buf},
                {{Null()}, Null()}};

        check_function<DataTypeString , true>(func_name, input_types, data_set);
    }
}

TEST(function_geo_test, function_geo_st_polyfromtext) {
    std::string func_name = "st_polyfromtext";
    {
        InputTypeSet input_types = {TypeIndex::String};

        GeoParseStatus status;
        std::string buf;
        std::string input = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), &status));
        ASSERT_TRUE(shape != nullptr);
        ASSERT_TRUE(status == GEO_PARSE_OK);
        shape->encode_to(&buf);
        DataSet data_set = {
                {{std::string("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")}, buf},
                {{Null()}, Null()}};

        check_function<DataTypeString , true>(func_name, input_types, data_set);
    }
}

} // namespace doris::vectorized

int main(int argc, char** argv) {
    doris::CpuInfo::init();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
