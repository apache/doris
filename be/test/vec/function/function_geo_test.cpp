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

} // namespace doris::vectorized

int main(int argc, char** argv) {
    doris::CpuInfo::init();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
