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
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/types.h"
#include "exprs/function/function_test_util.h"
#include "exprs/function/geo/geo_common.h"
#include "exprs/function/geo/geo_types.h"
#include "exprs/function/simple_function_factory.h"
#include "gtest/gtest_pred_impl.h"
#include "testutil/any_type.h"

namespace doris {
using namespace ut_type;

TEST(VGeoFunctionsTest, function_geo_st_point_test) {
    std::string func_name = "st_point";

    GeoPoint point;
    auto cur_res = point.from_coord(24.7, 56.7);
    EXPECT_TRUE(cur_res == GeoParseStatus::GEO_PARSE_OK);
    std::string buf;
    point.encode_to(&buf);

    DataSet data_set = {{{(double)24.7, (double)56.7}, buf},
                        {{Null(), (double)5}, Null()},
                        {{(double)5, Null()}, Null()}};
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {Consted {PrimitiveType::TYPE_DOUBLE},
                                    PrimitiveType::TYPE_DOUBLE};

        for (const auto& line : data_set) {
            DataSet const_dataset = {line};
            static_cast<void>(
                    check_function<DataTypeString, true>(func_name, input_types, const_dataset));
        }
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE,
                                    Consted {PrimitiveType::TYPE_DOUBLE}};

        for (const auto& line : data_set) {
            DataSet const_dataset = {line};
            static_cast<void>(
                    check_function<DataTypeString, true>(func_name, input_types, const_dataset));
        }
    }
}

TEST(VGeoFunctionsTest, function_geo_st_as_text) {
    std::string func_name = "st_astext";
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        GeoPoint point;
        auto cur_res = point.from_coord(24.7, 56.7);
        EXPECT_TRUE(cur_res == GeoParseStatus::GEO_PARSE_OK);
        std::string buf;
        point.encode_to(&buf);

        DataSet data_set = {{{buf}, std::string("POINT (24.7 56.7)")}, {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_as_wkt) {
    std::string func_name = "st_aswkt";
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        GeoPoint point;
        auto cur_res = point.from_coord(24.7, 56.7);
        EXPECT_TRUE(cur_res == GeoParseStatus::GEO_PARSE_OK);
        std::string buf;
        point.encode_to(&buf);

        DataSet data_set = {{{buf}, std::string("POINT (24.7 56.7)")}, {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_x) {
    std::string func_name = "st_x";
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        GeoPoint point;
        auto cur_res = point.from_coord(24.7, 56.7);
        EXPECT_TRUE(cur_res == GeoParseStatus::GEO_PARSE_OK);
        std::string buf;
        point.encode_to(&buf);

        DataSet data_set = {{{buf}, (double)24.7}, {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_y) {
    std::string func_name = "st_y";
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        GeoPoint point;
        auto cur_res = point.from_coord(24.7, 56.7);
        EXPECT_TRUE(cur_res == GeoParseStatus::GEO_PARSE_OK);
        std::string buf;
        point.encode_to(&buf);

        DataSet data_set = {{{buf}, (double)56.7}, {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_distance_sphere) {
    std::string func_name = "st_distance_sphere";
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE,
                                    PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE};

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
        InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE,
                                    PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE};

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
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR,
                                    PrimitiveType::TYPE_VARCHAR};

        GeoPoint point1;
        auto cur_res1 = point1.from_coord(1, 0);
        EXPECT_TRUE(cur_res1 == GeoParseStatus::GEO_PARSE_OK);
        GeoPoint point2;
        auto cur_res2 = point2.from_coord(0, 0);
        EXPECT_TRUE(cur_res2 == GeoParseStatus::GEO_PARSE_OK);
        GeoPoint point3;
        auto cur_res3 = point3.from_coord(0, 1);
        EXPECT_TRUE(cur_res3 == GeoParseStatus::GEO_PARSE_OK);
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
    GeoPoint point1;
    auto cur_res1 = point1.from_coord(0, 0);
    EXPECT_TRUE(cur_res1 == GeoParseStatus::GEO_PARSE_OK);
    GeoPoint point2;
    auto cur_res2 = point2.from_coord(1, 0);
    EXPECT_TRUE(cur_res2 == GeoParseStatus::GEO_PARSE_OK);

    std::string buf1;
    point1.encode_to(&buf1);
    std::string buf2;
    point2.encode_to(&buf2);

    DataSet data_set = {{{buf1, buf2}, (double)1.5707963267948966},
                        {{buf1, Null()}, Null()},
                        {{Null(), buf2}, Null()}};
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};

        for (const auto& line : data_set) {
            DataSet const_dataset = {line};
            static_cast<void>(
                    check_function<DataTypeFloat64, true>(func_name, input_types, const_dataset));
        }
    }
    {
        InputTypeSet input_types = {Consted {PrimitiveType::TYPE_VARCHAR},
                                    PrimitiveType::TYPE_VARCHAR};

        for (const auto& line : data_set) {
            DataSet const_dataset = {line};
            static_cast<void>(
                    check_function<DataTypeFloat64, true>(func_name, input_types, const_dataset));
        }
    }
}

TEST(VGeoFunctionsTest, function_geo_st_contains) {
    std::string func_name = "st_contains";

    std::string buf1;
    std::string buf2;
    std::string buf3;
    GeoParseStatus status;

    std::string shape1 = std::string("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(shape1.data(), shape1.size(), status));
    EXPECT_TRUE(status == GeoParseStatus::GEO_PARSE_OK);
    EXPECT_TRUE(shape != nullptr);
    shape->encode_to(&buf1);

    GeoPoint point1;
    status = point1.from_coord(5, 5);
    EXPECT_TRUE(status == GeoParseStatus::GEO_PARSE_OK);
    point1.encode_to(&buf2);

    GeoPoint point2;
    status = point2.from_coord(50, 50);
    EXPECT_TRUE(status == GeoParseStatus::GEO_PARSE_OK);
    point2.encode_to(&buf3);

    DataSet data_set = {{{buf1, buf2}, (uint8_t)1},
                        {{buf1, buf3}, (uint8_t)0},
                        {{buf1, Null()}, Null()},
                        {{Null(), buf3}, Null()}};
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};

        static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {Consted {PrimitiveType::TYPE_VARCHAR},
                                    PrimitiveType::TYPE_VARCHAR};

        for (const auto& line : data_set) {
            DataSet const_dataset = {line};
            static_cast<void>(
                    check_function<DataTypeUInt8, true>(func_name, input_types, const_dataset));
        }
    }
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};

        for (const auto& line : data_set) {
            DataSet const_dataset = {line};
            static_cast<void>(
                    check_function<DataTypeUInt8, true>(func_name, input_types, const_dataset));
        }
    }
}

namespace {
std::string encode_wkt(const std::string& wkt) {
    GeoParseStatus status;
    std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(wkt.data(), wkt.size(), status));
    EXPECT_TRUE(status == GeoParseStatus::GEO_PARSE_OK);
    std::string buf;
    shape->encode_to(&buf);
    return buf;
}

std::string encode_point(double x, double y) {
    GeoPoint point;
    EXPECT_TRUE(point.from_coord(x, y) == GeoParseStatus::GEO_PARSE_OK);
    std::string buf;
    point.encode_to(&buf);
    return buf;
}
} // namespace

// A constant polygon is decoded once and then checked against every row of the block.
TEST(VGeoFunctionsTest, function_geo_st_contains_const_polygon) {
    std::string polygon = encode_wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    DataSet data_set = {{{polygon, encode_point(5, 5)}, (uint8_t)1},
                        {{polygon, encode_point(50, 50)}, (uint8_t)0},
                        {{polygon, encode_point(10, 5)}, (uint8_t)0},
                        {{polygon, encode_point(10 - 5e-7, 5)}, (uint8_t)0},
                        {{polygon, encode_point(10 - 2e-6, 5)}, (uint8_t)1},
                        {{polygon, Null()}, Null()},
                        {{polygon, encode_point(1, 9)}, (uint8_t)1}};
    InputTypeSet input_types = {Consted {PrimitiveType::TYPE_VARCHAR}, PrimitiveType::TYPE_VARCHAR};
    static_cast<void>(check_function<DataTypeUInt8, true>("st_contains", input_types, data_set));

    // The polygon is the second argument of st_within.
    DataSet within_data_set = {{{encode_point(5, 5), polygon}, (uint8_t)1},
                               {{encode_point(50, 50), polygon}, (uint8_t)0},
                               {{encode_point(1, 9), polygon}, (uint8_t)1}};
    InputTypeSet within_input_types = {PrimitiveType::TYPE_VARCHAR,
                                       Consted {PrimitiveType::TYPE_VARCHAR}};
    static_cast<void>(
            check_function<DataTypeUInt8, true>("st_within", within_input_types, within_data_set));

    // An invalid constant makes every row NULL.
    DataSet invalid_data_set = {{{std::string("invalid"), encode_point(5, 5)}, Null()},
                                {{std::string("invalid"), encode_point(50, 50)}, Null()}};
    static_cast<void>(
            check_function<DataTypeUInt8, true>("st_contains", input_types, invalid_data_set));
}

// The decoded constant is kept in the thread-local state across blocks. A block whose constant
// differs from the previous one must not reuse the shape decoded for it.
TEST(VGeoFunctionsTest, function_geo_st_contains_const_polygon_across_blocks) {
    std::string small_polygon = encode_wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    std::string large_polygon = encode_wkt("POLYGON ((0 0, 30 0, 30 30, 0 30, 0 0))");
    std::string inside_both = encode_point(5, 5);
    std::string inside_large = encode_point(20, 20);

    DataTypePtr string_type = std::make_shared<DataTypeString>();
    DataTypePtr return_type = make_nullable(std::make_shared<DataTypeUInt8>());
    auto make_block = [&](const std::string& polygon) {
        auto polygon_column = ColumnString::create();
        polygon_column->insert_data(polygon.data(), polygon.size());
        auto point_column = ColumnString::create();
        point_column->insert_data(inside_both.data(), inside_both.size());
        point_column->insert_data(inside_large.data(), inside_large.size());
        Block block;
        block.insert({ColumnConst::create(std::move(polygon_column), 2), string_type, "polygon"});
        block.insert({std::move(point_column), string_type, "point"});
        return block;
    };

    Block sample_block = make_block(small_polygon);
    auto func = SimpleFunctionFactory::instance().get_function(
            "st_contains", sample_block.get_columns_with_type_and_name(), return_type);
    ASSERT_NE(nullptr, func);
    FunctionUtils fn_utils(return_type, {string_type, string_type}, false);
    auto* fn_ctx = fn_utils.get_fn_ctx();
    ASSERT_TRUE(func->open(fn_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(func->open(fn_ctx, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto contains = [&](const std::string& polygon) {
        Block block = make_block(polygon);
        block.insert({nullptr, return_type, "result"});
        EXPECT_TRUE(func->execute(fn_ctx, block, {0, 1}, 2, 2).ok());
        const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        EXPECT_FALSE(result.is_null_at(0));
        EXPECT_FALSE(result.is_null_at(1));
        const auto& data = assert_cast<const ColumnUInt8&>(result.get_nested_column()).get_data();
        return std::vector<uint8_t> {data[0], data[1]};
    };
    EXPECT_EQ((std::vector<uint8_t> {1, 0}), contains(small_polygon));
    EXPECT_EQ((std::vector<uint8_t> {1, 0}), contains(small_polygon));
    EXPECT_EQ((std::vector<uint8_t> {1, 1}), contains(large_polygon));
    EXPECT_EQ((std::vector<uint8_t> {1, 0}), contains(small_polygon));

    static_cast<void>(func->close(fn_ctx, FunctionContext::FunctionStateScope::THREAD_LOCAL));
    static_cast<void>(func->close(fn_ctx, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL));
}

TEST(VGeoFunctionsTest, function_geo_st_circle) {
    std::string func_name = "st_circle";
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE,
                                    PrimitiveType::TYPE_DOUBLE};

        GeoCircle circle;
        std::string buf;
        auto value = circle.init(111, 64, 10000);
        EXPECT_TRUE(value == GeoParseStatus::GEO_PARSE_OK);
        circle.encode_to(&buf);
        DataSet data_set = {{{(double)111, (double)64, (double)10000}, buf},
                            {{Null(), (double)64, (double)10000}, Null()},
                            {{(double)111, Null(), (double)10000}, Null()},
                            {{(double)111, (double)64, Null()}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_geometryfromtext) {
    std::string func_name = "st_geometryfromtext";
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        GeoParseStatus status;
        std::string buf;
        std::string input = "LINESTRING (1 1, 2 2)";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), status));
        EXPECT_TRUE(shape != nullptr);
        EXPECT_TRUE(status == GeoParseStatus::GEO_PARSE_OK);
        shape->encode_to(&buf);
        DataSet data_set = {{{std::string("LINESTRING (1 1, 2 2)")}, buf}, {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_geomfromtext) {
    std::string func_name = "st_geomfromtext";
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        GeoParseStatus status;
        std::string buf;
        std::string input = "LINESTRING (1 1, 2 2)";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), status));
        EXPECT_TRUE(shape != nullptr);
        EXPECT_TRUE(status == GeoParseStatus::GEO_PARSE_OK);
        shape->encode_to(&buf);
        DataSet data_set = {{{std::string("LINESTRING (1 1, 2 2)")}, buf}, {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_linefromtext) {
    std::string func_name = "st_linefromtext";
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        GeoParseStatus status;
        std::string buf;
        std::string input = "LINESTRING (1 1, 2 2)";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), status));
        EXPECT_TRUE(shape != nullptr);
        EXPECT_TRUE(status == GeoParseStatus::GEO_PARSE_OK);
        shape->encode_to(&buf);
        DataSet data_set = {{{std::string("LINESTRING (1 1, 2 2)")}, buf}, {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_polygon) {
    std::string func_name = "st_polygon";
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        GeoParseStatus status;
        std::string buf;
        std::string input = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), status));
        EXPECT_TRUE(shape != nullptr);
        EXPECT_TRUE(status == GeoParseStatus::GEO_PARSE_OK);
        shape->encode_to(&buf);
        DataSet data_set = {{{std::string("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")}, buf},
                            {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_polygonfromtext) {
    std::string func_name = "st_polygonfromtext";
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        GeoParseStatus status;
        std::string buf;
        std::string input = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), status));
        EXPECT_TRUE(shape != nullptr);
        EXPECT_TRUE(status == GeoParseStatus::GEO_PARSE_OK);
        shape->encode_to(&buf);
        DataSet data_set = {{{std::string("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")}, buf},
                            {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_area_square_meters) {
    std::string func_name = "st_area_square_meters";
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        GeoCircle circle;
        auto cur_res = circle.init(0, 0, 1);
        EXPECT_TRUE(cur_res == GeoParseStatus::GEO_PARSE_OK);
        std::string buf;
        circle.encode_to(&buf);
        DataSet data_set = {{{buf}, (double)3.1415926535897869}, {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }
}

TEST(VGeoFunctionsTest, function_geo_st_area_square_km) {
    std::string func_name = "st_area_square_km";
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        GeoParseStatus status;
        std::string buf;
        std::string input = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), status));
        EXPECT_TRUE(shape != nullptr);
        EXPECT_TRUE(status == GeoParseStatus::GEO_PARSE_OK);
        shape->encode_to(&buf);
        DataSet data_set = {{{buf}, (double)12364.036567076409}, {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }
}

} // namespace doris
