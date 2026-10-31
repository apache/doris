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

#include <arrow/api.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <s2/s2earth.h>
#include <stdint.h>

#include <array>
#include <bit>
#include <cmath>
#include <iomanip>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/column/column_spatial.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_spatial.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/types.h"
#include "exprs/function/function_test_util.h"
#include "exprs/function/geo/functions_geo.h"
#include "exprs/function/geo/geo_common.h"
#include "exprs/function/geo/geo_types.h"
#include "exprs/function/geo/geography_calculation.h"
#include "exprs/function/geo/wkb_parse.h"
#include "exprs/function/string_hex_util.h"
#include "format/arrow/arrow_block_convertor.h"
#include "format/arrow/arrow_row_batch.h"
#include "gtest/gtest_pred_impl.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "testutil/any_type.h"

namespace doris {
using namespace ut_type;

static void expect_typed_collection_constructors(PrimitiveType primitive) {
    const std::string point = "0101000000000000000000F03F0000000000000040";
    const std::vector<std::pair<std::string, std::string>> cases = {
            {"010400000001000000" + point, "MULTIPOINT ((1 2))"},
            {"010500000001000000010200000000000000", "MULTILINESTRING (EMPTY)"},
            {"010600000001000000010300000000000000", "MULTIPOLYGON (EMPTY)"},
            {"010700000001000000" + point, "GEOMETRYCOLLECTION (POINT (1 2))"},
            {"0101000000000000000000F87F000000000000F87F", "POINT EMPTY"},
            {"010200000000000000", "LINESTRING EMPTY"},
            {"010300000000000000", "POLYGON EMPTY"},
            {"0101000000000000000000F03F000000000000F87F", "POINT (1 NaN)"}};
    auto input = ColumnString::create();
    for (const auto& value : cases) {
        input->insert_data(value.first.data(), value.first.size());
    }
    const auto input_type = std::make_shared<DataTypeString>();
    ColumnsWithTypeAndName arguments {{std::move(input), input_type, "wkb"}};
    const auto result_type = make_nullable(std::make_shared<DataTypeSpatial>(primitive));
    auto function = SimpleFunctionFactory::instance().get_function(
            primitive == TYPE_GEOMETRY ? "st_geometryfromwkbtyped" : "st_geogfromwkb", arguments,
            result_type);
    ASSERT_NE(function, nullptr);
    Block block;
    block.insert(arguments.front());
    block.insert({nullptr, result_type, "result"});
    const auto status = function->execute(nullptr, block, {0}, 1, cases.size());
    ASSERT_TRUE(status.ok()) << status;
    const auto& result = block.get_by_position(1).column;
    ASSERT_EQ(result->size(), cases.size());
    for (size_t row = 0; row < cases.size(); ++row) {
        ASSERT_FALSE(result->is_null_at(row));
        const auto value = result->get_data_at(row);
        std::string text;
        ASSERT_EQ(WkbParse::wkb_to_wkt(value.data, value.size, &text),
                  GeoParseStatus::GEO_PARSE_OK);
        EXPECT_EQ(text, cases[row].second);
    }
}

TEST(VGeoFunctionsTest, GeometryConstructorPreservesCollectionsAndEmptyValues) {
    expect_typed_collection_constructors(TYPE_GEOMETRY);
}

TEST(VGeoFunctionsTest, GeographyConstructorPreservesCollectionsAndEmptyValues) {
    expect_typed_collection_constructors(TYPE_GEOGRAPHY);
}

TEST(VGeoFunctionsTest, GeographyCollectionCalculationsFailExplicitly) {
    const std::string point(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@", 21);
    const auto multi = std::string("\x01\x04\x00\x00\x00\x01\x00\x00\x00", 9) + point;
    const auto type = std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY);
    auto column = type->create_column();
    column->insert_data(multi.data(), multi.size());
    ColumnsWithTypeAndName arguments {{std::move(column), type, "geography"}};
    const auto result_type = make_nullable(std::make_shared<DataTypeFloat64>());
    auto function =
            SimpleFunctionFactory::instance().get_function("st_length", arguments, result_type);
    ASSERT_NE(function, nullptr);
    Block block;
    block.insert(arguments.front());
    block.insert({nullptr, result_type, "result"});
    const auto status = function->execute(nullptr, block, {0}, 1, 1);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("does not support this Geography WKB shape"),
              std::string::npos);
}

TEST(VGeoFunctionsTest, GeographyCalculationNullsSkipUnsupportedShapeEvaluation) {
    const std::string point(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@", 21);
    const auto multi = std::string("\x01\x04\x00\x00\x00\x01\x00\x00\x00", 9) + point;
    const auto type = std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY);
    auto left = type->create_column();
    left->insert_data(multi.data(), multi.size());
    left->insert_data(point.data(), point.size());
    auto right = type->create_column();
    right->insert_default();
    right->insert_data(point.data(), point.size());
    auto nulls = ColumnUInt8::create();
    nulls->get_data().assign({1, 0});
    ColumnsWithTypeAndName arguments {{std::move(left), type, "left"},
                                      {ColumnNullable::create(std::move(right), std::move(nulls)),
                                       make_nullable(type), "right"}};
    const auto result_type = make_nullable(std::make_shared<DataTypeFloat64>());
    auto function =
            SimpleFunctionFactory::instance().get_function("st_distance", arguments, result_type);
    ASSERT_NE(function, nullptr);
    Block block;
    block.insert(arguments[0]);
    block.insert(arguments[1]);
    block.insert({nullptr, result_type, "result"});
    const auto status = function->execute(nullptr, block, {0, 1}, 2, 2);
    ASSERT_TRUE(status.ok()) << status;
    const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
    EXPECT_TRUE(result.is_null_at(0));
    ASSERT_FALSE(result.is_null_at(1));
    EXPECT_DOUBLE_EQ(assert_cast<const ColumnFloat64&>(result.get_nested_column()).get_data()[1],
                     0);
}

TEST(VGeoFunctionsTest, KnownGeographicGeometryUsesCoordinateBounds) {
    const std::string wkb(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\xa0\x66\x40"
            "\x00\x00\x00\x00\x00\x00\xf0\x3f",
            21);
    for (const auto& crs : {"OGC:CRS84", "EPSG:4326", "EPSG:3857"}) {
        const auto type = std::make_shared<DataTypeSpatial>(TYPE_GEOMETRY, crs);
        auto column = type->create_column();
        column->insert_data(wkb.data(), wkb.size());
        ColumnsWithTypeAndName arguments {{std::move(column), type, "geometry"}};
        const auto result_type = make_nullable(std::make_shared<DataTypeString>());
        auto function =
                SimpleFunctionFactory::instance().get_function("st_astext", arguments, result_type);
        ASSERT_NE(function, nullptr);
        Block block;
        block.insert(arguments.front());
        block.insert({nullptr, result_type, "result"});
        const auto status = function->execute(nullptr, block, {0}, 1, 1);
        EXPECT_EQ(status.ok(), std::string(crs) == "EPSG:3857");
    }
}

static void expect_nan_spatial_accessors(PrimitiveType primitive) {
    const std::string wkb(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f"
            "\x00\x00\x00\x00\x00\x00\xf8\x7f",
            21);
    const auto type = std::make_shared<DataTypeSpatial>(primitive);
    auto column = type->create_column();
    column->insert_data(wkb.data(), wkb.size());
    ColumnsWithTypeAndName arguments {{std::move(column), type, "spatial"}};
    const auto result_type = make_nullable(std::make_shared<DataTypeVarbinary>());
    auto function =
            SimpleFunctionFactory::instance().get_function("st_asbinary", arguments, result_type);
    ASSERT_NE(function, nullptr);
    Block block;
    block.insert(arguments.front());
    block.insert({nullptr, result_type, "result"});
    ASSERT_TRUE(function->execute(nullptr, block, {0}, 1, 1).ok());
    ASSERT_FALSE(block.get_by_position(1).column->is_null_at(0));
    EXPECT_EQ(block.get_by_position(1).column->get_data_at(0).to_string(), wkb);
}

TEST(VGeoFunctionsTest, MixedNanGeometryAndGeographyPreserveRawBinary) {
    expect_nan_spatial_accessors(TYPE_GEOMETRY);
    expect_nan_spatial_accessors(TYPE_GEOGRAPHY);
}

static void expect_constant_geography_distance(bool constant_on_left, bool unsupported_shape) {
    const std::string point(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@", 21);
    const auto wkb = unsupported_shape
                             ? std::string("\x01\x04\x00\x00\x00\x01\x00\x00\x00", 9) + point
                             : point;
    const auto type = std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY);
    auto constant_value = type->create_column();
    constant_value->insert_data(wkb.data(), wkb.size());
    auto vector_value = type->create_column();
    vector_value->insert_default();
    vector_value->insert_data(point.data(), point.size());
    auto nulls = ColumnUInt8::create();
    nulls->get_data().assign({1, static_cast<uint8_t>(unsupported_shape)});
    ColumnWithTypeAndName constant {ColumnConst::create(std::move(constant_value), 2), type,
                                    "constant"};
    ColumnWithTypeAndName vector {ColumnNullable::create(std::move(vector_value), std::move(nulls)),
                                  make_nullable(type), "vector"};
    ColumnsWithTypeAndName arguments = constant_on_left ? ColumnsWithTypeAndName {constant, vector}
                                                        : ColumnsWithTypeAndName {vector, constant};
    const auto result_type = make_nullable(std::make_shared<DataTypeFloat64>());
    auto function =
            SimpleFunctionFactory::instance().get_function("st_distance", arguments, result_type);
    ASSERT_NE(function, nullptr);
    Block block;
    block.insert(arguments[0]);
    block.insert(arguments[1]);
    block.insert({nullptr, result_type, "result"});
    const auto status = function->execute(nullptr, block, {0, 1}, 2, 2);
    ASSERT_TRUE(status.ok()) << status;
    const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
    ASSERT_EQ(result.size(), 2);
    EXPECT_TRUE(result.is_null_at(0));
    EXPECT_EQ(result.is_null_at(1), unsupported_shape);
    if (!unsupported_shape) {
        EXPECT_DOUBLE_EQ(
                assert_cast<const ColumnFloat64&>(result.get_nested_column()).get_data()[1], 0);
    }
}

TEST(VGeoFunctionsTest, ConstantGeographyDistancePreservesNullableEvaluation) {
    expect_constant_geography_distance(true, false);
    expect_constant_geography_distance(false, false);
    expect_constant_geography_distance(true, true);
    expect_constant_geography_distance(false, true);
}

TEST(VGeoFunctionsTest, GeographyAngleRejectsUnsupportedArgumentsRegardlessOfOrder) {
    const std::string point(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@", 21);
    const auto line = std::string("\x01\x02\x00\x00\x00\x02\x00\x00\x00", 9) + point.substr(5) +
                      std::string(
                              "\x00\x00\x00\x00\x00\x00\x08\x40"
                              "\x00\x00\x00\x00\x00\x00\x10\x40",
                              16);
    const auto multi = std::string("\x01\x04\x00\x00\x00\x01\x00\x00\x00", 9) + point;
    const auto type = std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY);
    ColumnsWithTypeAndName arguments;
    for (const auto& wkb : {line, multi, point}) {
        auto column = type->create_column();
        column->insert_data(wkb.data(), wkb.size());
        arguments.emplace_back(std::move(column), type, "geography");
    }
    const auto result_type = make_nullable(std::make_shared<DataTypeFloat64>());
    auto function =
            SimpleFunctionFactory::instance().get_function("st_angle", arguments, result_type);
    ASSERT_NE(function, nullptr);
    Block block;
    for (const auto& argument : arguments) {
        block.insert(argument);
    }
    block.insert({nullptr, result_type, "result"});
    const auto status = function->execute(nullptr, block, {0, 1, 2}, 3, 1);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("does not support this Geography WKB shape"),
              std::string::npos);
}

static std::string spatial_test_wkb(std::string_view hex) {
    std::string wkb(hex.size() / 2, '\0');
    EXPECT_EQ(string_hex::hex_decode(hex.data(), hex.size(), wkb.data()), wkb.size());
    return wkb;
}

static Status execute_geography_test(const std::string& name, const Strings& values,
                                     const DataTypePtr& result_type, Block* block) {
    const auto type = std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY);
    ColumnNumbers numbers;
    for (const auto& hex : values) {
        auto column = type->create_column();
        const auto wkb = spatial_test_wkb(hex);
        column->insert_data(wkb.data(), wkb.size());
        numbers.push_back(block->columns());
        block->insert({std::move(column), type, "geography"});
    }
    auto function = SimpleFunctionFactory::instance().get_function(
            name, block->get_columns_with_type_and_name(), result_type);
    EXPECT_NE(function, nullptr);
    const auto result = block->columns();
    block->insert({nullptr, result_type, "result"});
    return function->execute(nullptr, *block, numbers, result, 1);
}

TEST(VGeoFunctionsTest, GeographyAreaPreservesNullableEvaluation) {
    const auto type = std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY);
    const auto point = spatial_test_wkb("0101000000000000000000F03F0000000000000040");
    const auto result_type = make_nullable(std::make_shared<DataTypeFloat64>());
    for (const auto& name : {"st_area_square_meters", "st_area_square_km"}) {
        auto column = type->create_column();
        column->insert_default();
        column->insert_data(point.data(), point.size());
        auto nulls = ColumnUInt8::create();
        nulls->get_data().assign({1, 0});
        ColumnsWithTypeAndName arguments {
                {ColumnNullable::create(std::move(column), std::move(nulls)), make_nullable(type),
                 "geography"}};
        auto function =
                SimpleFunctionFactory::instance().get_function(name, arguments, result_type);
        ASSERT_NE(function, nullptr);
        Block block;
        block.insert(arguments.front());
        block.insert({nullptr, result_type, "result"});
        const auto status = function->execute(nullptr, block, {0}, 1, 2);
        ASSERT_TRUE(status.ok()) << status;
        const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        EXPECT_TRUE(result.is_null_at(0));
        ASSERT_FALSE(result.is_null_at(1));
        EXPECT_DOUBLE_EQ(
                assert_cast<const ColumnFloat64&>(result.get_nested_column()).get_data()[1], 0);
    }
}

TEST(VGeoFunctionsTest, IsoDimensionsAreStoredButNotCalculated) {
    const Strings points {
            "01E9030000000000000000F03F00000000000000400000000000000840",
            "01D1070000000000000000F03F00000000000000400000000000000840",
            "01B90B0000000000000000F03F000000000000004000000000000008400000000000001040"};
    for (const auto& point : points) {
        for (const auto& [name, count] :
             std::vector<std::pair<std::string, size_t>> {{"st_length", 1},
                                                          {"st_area_square_meters", 1},
                                                          {"st_angle", 3},
                                                          {"st_azimuth", 2},
                                                          {"st_distance", 2},
                                                          {"st_intersects", 2}}) {
            Block block;
            Strings values(count, point);
            DataTypePtr result = name == "st_intersects"
                                         ? DataTypePtr(std::make_shared<DataTypeUInt8>())
                                         : DataTypePtr(std::make_shared<DataTypeFloat64>());
            const auto status = execute_geography_test(name, values, make_nullable(result), &block);
            EXPECT_FALSE(status.ok()) << name;
            EXPECT_NE(status.to_string().find("does not support this Geography WKB shape"),
                      std::string::npos)
                    << status.to_string();
        }
        Block block;
        ASSERT_TRUE(execute_geography_test("st_asbinary", {point},
                                           make_nullable(std::make_shared<DataTypeVarbinary>()),
                                           &block)
                            .ok());
        EXPECT_EQ(block.get_by_position(1).column->get_data_at(0).to_string(),
                  spatial_test_wkb(point));
    }
}

TEST(VGeoFunctionsTest, GeographyRelationsUseCompleteSphericalEdgesAndAliases) {
    struct Case {
        std::string left;
        std::string right;
        bool intersects;
    };
    const std::vector<Case> cases {
            {"010100000000000000008066400000000000000000",
             "010100000000000000008066c00000000000000000", true},
            {"010100000000000000000000000000000000805640",
             "010100000000000000008056400000000000805640", true},
            {"0101000000000000000000000000000000008056c0",
             "010100000000000000008056c000000000008056c0", true},
            {"0102000000020000000000000000406540000000000000000000000000004065c00000000000000000",
             "010100000000000000008066400000000000000000", true},
            {"01020000000300000000000000000000000000000000000000000000000000f03f0000000000000000000"
             "00000000000400000000000000000",
             "0101000000000000000000f83f0000000000000000", true},
            {"01020000000300000000000000000000000000000000000000000000000000f03f0000000000000000000"
             "00000000000400000000000000000",
             "010200000002000000000000000000f83f000000000000f0bf000000000000f83f000000000000f03f",
             true},
            {"01030000000100000005000000000000000040654000000000000024c000000000004065c000000000000"
             "024c000000000004065c00000000000002440000000000040654000000000000024400000000000406540"
             "00000000000024c0",
             "010100000000000000000000000000000000000000", false},
            {"01030000000100000005000000000000000040654000000000000024c000000000004065c000000000000"
             "024c000000000004065c00000000000002440000000000040654000000000000024400000000000406540"
             "00000000000024c0",
             "010100000000000000008066400000000000000000", true},
            {"01030000000100000005000000000000000000e0bf000000000000e0bf000000000000e03f00000000000"
             "0e0bf000000000000e03f000000000000e03f000000000000e0bf000000000000e03f000000000000e0bf"
             "000000000000e0bf",
             "01020000000400000000000000000000c0000000000000000000000000000000c00000000000000040000"
             "0000000000040000000000000004000000000000000400000000000000000",
             false}};
    const auto result_type = make_nullable(std::make_shared<DataTypeUInt8>());
    for (const auto& value : cases) {
        SCOPED_TRACE(value.left + " versus " + value.right);
        for (bool reversed : {false, true}) {
            const Strings arguments = reversed ? Strings {value.right, value.left}
                                               : Strings {value.left, value.right};
            for (const auto& name : {"st_intersects", "st_disjoint"}) {
                Block block;
                const auto status = execute_geography_test(name, arguments, result_type, &block);
                ASSERT_TRUE(status.ok()) << status;
                const bool expected = std::string_view(name) == "st_intersects" ? value.intersects
                                                                                : !value.intersects;
                EXPECT_EQ((*block.get_by_position(2).column)[0].get<TYPE_BOOLEAN>(), expected);
            }
        }
    }
}

TEST(VGeoFunctionsTest, GeographyNonpointRelationsAllowOnlyConversionRoundoff) {
    // Same-meridian arcs overlap despite independently rounded S2 coordinates.
    const std::string line =
            "0102000000020000000000000000002440000000000080464000000000000024400000000000004740";
    const std::string inner =
            "01020000000200000000000000000024406666666666a6464000000000000024403333333333f34640";
    const std::string reversed =
            "01020000000200000000000000000024403333333333f3464000000000000024406666666666a64640";
    const std::string near =
            "010200000002000000e7db0000000024406666666666a64640e7db0000000024403333333333f34640";
    const std::string outside =
            "0102000000020000001924ffffffff23406666666666a646401924ffffffff23403333333333f34640";
    const std::string beyond =
            "0102000000020000000000000000002440cdcccccccc0c474000000000000024409a99999999194740";
    const std::string multi =
            "0102000000030000000000000000002240000000000000464000000000000024400000000000804640"
            "00000000000024400000000000004740";
    const std::string polygon =
            "01030000000100000005000000000000000000244000000000008046400000000000002640000000000080"
            "46"
            "40000000000000264000000000000047400000000000002440000000000000474000000000000024400000"
            "00"
            "0000804640";
    const std::string neighbor =
            "0103000000010000000500000000000000000024406666666666a6464000000000000024403333333333f3"
            "46"
            "4000000000000022403333333333f3464000000000000022406666666666a6464000000000000024406666"
            "66"
            "6666a64640";
    const std::string near_polygon =
            "010300000001000000050000001924ffffffff23406666666666a646401924ffffffff23403333333333f3"
            "46"
            "4000000000000022403333333333f3464000000000000022406666666666a646401924ffffffff23406666"
            "66"
            "6666a64640";
    struct Case {
        std::string left;
        std::string right;
        bool intersects;
    };
    // The 1e-10 degree offsets are near but much larger than conversion roundoff.
    const std::vector<Case> cases {{line, inner, true},       {line, reversed, true},
                                   {line, near, false},       {line, beyond, false},
                                   {multi, inner, true},      {polygon, inner, true},
                                   {polygon, near, true},     {polygon, outside, false},
                                   {polygon, neighbor, true}, {polygon, near_polygon, false}};
    const auto result_type = make_nullable(std::make_shared<DataTypeUInt8>());
    for (const auto& value : cases) {
        SCOPED_TRACE(value.left + " versus " + value.right);
        for (bool reverse_arguments : {false, true}) {
            const Strings arguments = reverse_arguments ? Strings {value.right, value.left}
                                                        : Strings {value.left, value.right};
            for (const auto& name : {"st_intersects", "st_disjoint"}) {
                Block block;
                const auto status = execute_geography_test(name, arguments, result_type, &block);
                ASSERT_TRUE(status.ok()) << status;
                EXPECT_EQ((*block.get_by_position(2).column)[0].get<TYPE_BOOLEAN>(),
                          std::string_view(name) == "st_intersects" ? value.intersects
                                                                    : !value.intersects);
            }
        }
    }
}

static void expect_small_geography_ring(const std::string& ring, double& area) {
    const std::string inside = "010100000000000000000000000000000000000000";
    const std::string outside = "010100000000000000008066400000000000000000";
    // Reversing a small ring does not select its >hemisphere complement.
    for (const auto& point : {inside, outside}) {
        Block block;
        const auto status =
                execute_geography_test("st_contains", {ring, point},
                                       make_nullable(std::make_shared<DataTypeUInt8>()), &block);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ((*block.get_by_position(2).column)[0].get<TYPE_BOOLEAN>(), point == inside);
    }
    Block measured;
    ASSERT_TRUE(execute_geography_test("st_area_square_meters", {ring},
                                       make_nullable(std::make_shared<DataTypeFloat64>()),
                                       &measured)
                        .ok());
    area = (*measured.get_by_position(1).column)[0].get<TYPE_DOUBLE>();
    EXPECT_GT(area, 0);
    EXPECT_LT(area, 1e14);
    Block raw;
    ASSERT_TRUE(execute_geography_test("st_asbinary", {ring},
                                       make_nullable(std::make_shared<DataTypeVarbinary>()), &raw)
                        .ok());
    EXPECT_EQ(raw.get_by_position(1).column->get_data_at(0).to_string(), spatial_test_wkb(ring));
}

TEST(VGeoFunctionsTest, GeographyPolygonCalculationsUseNormalizedRings) {
    const Strings rings {
            "0103000000010000000500000000000000000024c000000000000024c000000000000024400000000000"
            "0024c00000000000002440000000000000244000000000000024c0000000000000244000000000000024"
            "c000000000000024c0",
            "0103000000010000000500000000000000000024c000000000000024c000000000000024c00000000000"
            "00244000000000000024400000000000002440000000000000244000000000000024c000000000000024"
            "c000000000000024c0"};
    std::vector<double> areas(rings.size());
    for (size_t i = 0; i < rings.size(); ++i) {
        expect_small_geography_ring(rings[i], areas[i]);
    }
    EXPECT_NEAR(areas[0], areas[1], areas[0] * 1e-12);
}

TEST(VGeoFunctionsTest, GeographyMeridianMembershipAllowsOnlyConversionRoundoff) {
    struct Case {
        std::string shape;
        std::string point;
        bool member;
        bool boundary;
    };
    const std::vector<Case> cases {
            {"0102000000020000000000000000002440000000000080464000000000000024400000000000004740",
             "010100000000000000000024400000000000c04640", true, false},
            {"0102000000020000000000000000002440000000000080464000000000000024400000000000004740",
             "0101000000e7db0000000024400000000000c04640", false, false},
            {"0102000000020000000000000000002440000000000080464000000000000024400000000000004740",
             "010100000000000000000024400000000000804640", true, true},
            {"0103000000010000000500000000000000000024400000000000804640000000000000264000000000008"
             "0464000000000000026400000000000004740000000000000244000000000000047400000000000002440"
             "0000000000804640",
             "010100000000000000000024400000000000c04640", true, true},
            {"0103000000010000000500000000000000000024400000000000804640000000000000264000000000008"
             "0464000000000000026400000000000004740000000000000244000000000000047400000000000002440"
             "0000000000804640",
             "0101000000e7db0000000024400000000000c04640", true, false},
            {"0103000000010000000500000000000000000024400000000000804640000000000000264000000000008"
             "0464000000000000026400000000000004740000000000000244000000000000047400000000000002440"
             "0000000000804640",
             "01010000001924ffffffff23400000000000c04640", false, false},
    };
    const auto result_type = make_nullable(std::make_shared<DataTypeUInt8>());
    for (const auto& value : cases) {
        SCOPED_TRACE(value.shape + " versus " + value.point);
        for (bool reversed : {false, true}) {
            const Strings arguments = reversed ? Strings {value.point, value.shape}
                                               : Strings {value.shape, value.point};
            for (const auto& name : {"st_intersects", "st_disjoint", "st_touches",
                                     reversed ? "st_within" : "st_contains"}) {
                Block block;
                ASSERT_TRUE(execute_geography_test(name, arguments, result_type, &block).ok());
                const std::string_view function_name(name);
                const bool expected =
                        (function_name == "st_intersects" && value.member) ||
                        (function_name == "st_disjoint" && !value.member) ||
                        (function_name == "st_touches" && value.boundary) ||
                        ((function_name == "st_contains" || function_name == "st_within") &&
                         value.member && !value.boundary);
                EXPECT_EQ((*block.get_by_position(2).column)[0].get<TYPE_BOOLEAN>(), expected)
                        << name << " reversed=" << reversed;
            }
        }
    }
}

// WKT makes topology cases readable; exercise the public typed WKB function, not its helper.
static std::string geography_test_hex(const std::string& wkt) {
    // Encode the fixture's decimal ordinates directly: a GeoShape round trip would first
    // round longitude through S2, defeating the meridian subdivision regression.
    std::ostringstream hex;
    hex << std::hex << std::setfill('0');
    const auto append = [&](uint64_t value, int bytes) {
        for (int i = 0; i < bytes; ++i) {
            hex << std::setw(2) << ((value >> (8 * i)) & 255);
        }
    };
    const bool point = wkt.starts_with("POINT");
    const bool polygon = wkt.starts_with("POLYGON");
    append(1, 1);
    uint32_t kind = 2;
    if (point) {
        kind = 1;
    } else if (polygon) {
        kind = 3;
    }
    append(kind, 4);
    std::vector<std::vector<double>> rings;
    size_t cursor = polygon ? wkt.find('(') + 1 : 0;
    while ((cursor = wkt.find('(', cursor)) != std::string::npos) {
        const auto close = wkt.find(')', cursor);
        auto text = wkt.substr(cursor + 1, close - cursor - 1);
        std::ranges::replace(text, ',', ' ');
        std::istringstream coordinates(text);
        auto& ring = rings.emplace_back();
        double value;
        while (coordinates >> value) {
            ring.push_back(value);
        }
        cursor = close + 1;
    }
    if (polygon) {
        append(rings.size(), 4);
    }
    for (const auto& ring : rings) {
        if (!point) {
            append(ring.size() / 2, 4);
        }
        for (double coordinate : ring) {
            append(std::bit_cast<uint64_t>(coordinate), 8);
        }
    }
    return hex.str();
}

static void expect_geography_relation(const std::string& name, const std::string& left,
                                      const std::string& right, bool expected) {
    SCOPED_TRACE(name + "(" + left + ", " + right + ")");
    Block block;
    const auto type = make_nullable(std::make_shared<DataTypeUInt8>());
    ASSERT_TRUE(execute_geography_test(name, {geography_test_hex(left), geography_test_hex(right)},
                                       type, &block)
                        .ok());
    EXPECT_EQ((*block.get_by_position(2).column)[0].get<TYPE_BOOLEAN>(), expected);
}

TEST(VGeoFunctionsTest, GeographyContainmentCoversEveryAdmittedDimension) {
    struct Case {
        std::string container;
        std::string value;
        bool expected;
    };
    const std::string polygon = "POLYGON ((0 0,4 0,4 4,0 4,0 0))";
    const std::string hole = "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,1 3,3 3,3 1,1 1))";
    const std::vector<Case> cases {
            {"POINT (1 2)", "POINT (1 2)", true},
            {"POINT (1 0)", "LINESTRING (0 0,2 0)", false},
            {"LINESTRING (0 0,2 0)", "LINESTRING (0 0,2 0)", true},
            {"LINESTRING (0 0,2 0)", "LINESTRING (2 0,1 0,0 0)", true},
            {"LINESTRING (10 45,10 46)", "LINESTRING (10 45.3,10 45.9)", true},
            {"LINESTRING (0 0,2 0)", "LINESTRING (1 0,3 0)", false},
            {polygon, "LINESTRING (1 1,3 1)", true},
            {polygon, "LINESTRING (0 0,4 0)", false},
            {polygon, "LINESTRING (1 1,5 1)", false},
            {polygon, polygon, true},
            {polygon, "POLYGON ((1 1,2 1,2 2,1 2,1 1))", true},
            {hole, "LINESTRING (0.5 2,3.5 2)", false},
            {"POLYGON ((10 45,11 45,11 46,10 46,10 45))", "LINESTRING (10 45.3,10 45.9)", false},
            {hole, "POLYGON ((1.5 1.5,2 1.5,2 2,1.5 2,1.5 1.5))", false},
            {"LINESTRING (0 0,2 0)", polygon, false},
    };
    for (const auto& value : cases) {
        expect_geography_relation("st_contains", value.container, value.value, value.expected);
        expect_geography_relation("st_within", value.value, value.container, value.expected);
    }
}

TEST(VGeoFunctionsTest, GeographyTouchesDistinguishesBoundaryFromInterior) {
    struct Case {
        std::string left;
        std::string right;
        bool expected;
    };
    const std::string polygon = "POLYGON ((0 0,2 0,2 2,0 2,0 0))";
    const std::vector<Case> cases {
            {"LINESTRING (0 0,2 0)", "LINESTRING (2 0,3 1)", true},
            {"LINESTRING (0 0,2 0)", "LINESTRING (1 -1,1 1)", false},
            {"LINESTRING (0 0,2 0)", "LINESTRING (1 0,3 0)", false},
            {"LINESTRING (0 0,1 0)", "LINESTRING (0 0.000000000000001,1 0.000000000000001)", false},
            {"LINESTRING (0 0,2 0)", polygon, true},
            {"LINESTRING (1 -1,1 1)", polygon, false},
            {polygon, "POLYGON ((2 0,4 0,4 2,2 2,2 0))", true},
            {polygon, "POLYGON ((2 2,3 2,3 3,2 3,2 2))", true},
            {polygon, "POLYGON ((1 0,3 0,3 2,1 2,1 0))", false},
            {polygon, polygon, false},
            {"POLYGON ((10 45,11 45,11 46,10 46,10 45))", "LINESTRING (10 45.3,10 45.9)", true},
            {"POINT (0 0)", "LINESTRING (0 0,1 0,1 1,0 0)", false},
    };
    for (const auto& value : cases) {
        expect_geography_relation("st_touches", value.left, value.right, value.expected);
        expect_geography_relation("st_touches", value.right, value.left, value.expected);
    }
}

TEST(VGeoFunctionsTest, GeographyTouchesKeepsRoundedZeroLengthEdgesOpen) {
    const std::string original = "LINESTRING (0 58,0 59)";
    const std::string subdivided = "LINESTRING (0 58,0 58.00000000000001,0 59)";
    const std::string other = "LINESTRING (-1 57,0 58,1 57)";
    for (const auto& line : {original, subdivided}) {
        expect_geography_relation("st_touches", line, other, true);
        expect_geography_relation("st_touches", other, line, true);
    }
}

TEST(VGeoFunctionsTest, GeographyTouchesKeepsRawInteriorCrossings) {
    const std::string crossing = "LINESTRING (0 59,-1 58.5,0 58.00000000000001,1 58.5)";
    for (const auto* line :
         {"LINESTRING (0 58,0 59)", "LINESTRING (0 58,0 58.00000000000001,0 59)"}) {
        expect_geography_relation("st_touches", line, crossing, false);
        expect_geography_relation("st_touches", crossing, line, false);
    }
}

TEST(VGeoFunctionsTest, GeographyEqualsPreservesUnequalRawLineExtents) {
    // Both first endpoints round to the same S2Point, but only the first line
    // contains POINT(0 58). Check meridian extents and distinct non-meridian arcs.
    for (const auto* end : {"0 59", "1 59"}) {
        const std::string a = std::string("LINESTRING (0 58,") + end + ")";
        const std::string b = std::string("LINESTRING (0 58.00000000000001,") + end + ")";
        SCOPED_TRACE(end);
        expect_geography_relation("st_equals", a, b, false);
        expect_geography_relation("st_equals", b, a, false);
        expect_geography_relation("st_intersects", a, "POINT (0 58)", true);
        expect_geography_relation("st_intersects", b, "POINT (0 58)", false);
    }
}

TEST(VGeoFunctionsTest, GeographyEqualsRetainsEquivalentRawLineCoverage) {
    const std::vector<std::pair<std::string, std::string>> cases {
            {"LINESTRING (0 58,0 59)", "LINESTRING (0 58,0 58.00000000000001,0 59)"},
            {"LINESTRING (0 58,0 59)", "LINESTRING (0 59,0 58.00000000000001,0 58)"},
            {"LINESTRING (0 58,0 59)", "LINESTRING (0 58,0 59,0 58.00000000000001,0 59)"},
            {"LINESTRING (58 0,59 0)", "LINESTRING (58 0,58.00000000000001 0,59 0)"},
            {"LINESTRING (180 58,180 59)", "LINESTRING (-180 58,-180 59)"},
            {"LINESTRING (0 90,0 89)", "LINESTRING (123 90,0 89)"},
            {"LINESTRING (0 90,90 90)", "LINESTRING (-90 90,180 90)"},
    };
    for (const auto& [a, b] : cases) {
        SCOPED_TRACE(a + " versus " + b);
        expect_geography_relation("st_equals", a, b, true);
        expect_geography_relation("st_equals", b, a, true);
    }
    // Empty raw edge sets still require canonical S2 position equality.
    expect_geography_relation("st_equals", "LINESTRING (0 90,90 90)", "LINESTRING (0 -90,90 -90)",
                              false);
}

TEST(VGeoFunctionsTest, GeographyContainmentPreservesRawLineCoverage) {
    const std::string outer = "LINESTRING (0 58,0 59)";
    const std::string inner = "LINESTRING (0 58.00000000000001,0 59)";
    expect_geography_relation("st_contains", outer, inner, true);
    expect_geography_relation("st_contains", inner, outer, false);
    expect_geography_relation("st_within", inner, outer, true);
    expect_geography_relation("st_within", outer, inner, false);
    for (const auto* equivalent : {"LINESTRING (0 58,0 58.00000000000001,0 59)",
                                   "LINESTRING (0 59,0 58.00000000000001,0 58)",
                                   "LINESTRING (0 58,0 59,0 58.00000000000001,0 59)"}) {
        expect_geography_relation("st_contains", equivalent, outer, true);
        expect_geography_relation("st_contains", outer, equivalent, true);
        expect_geography_relation("st_within", equivalent, outer, true);
    }
}

TEST(VGeoFunctionsTest, GeographyOverlapsPreservesRawLineCoverage) {
    const std::string line = "LINESTRING (0 58,0 59)";
    for (const auto* overlap :
         {"LINESTRING (0 58.00000000000001,0 60)", "LINESTRING (0 60,0 59,0 58.00000000000001)",
          "LINESTRING (0 57,0 58.00000000000001)"}) {
        expect_geography_relation("st_overlaps", line, overlap, true);
        expect_geography_relation("st_overlaps", overlap, line, true);
    }
    for (const auto* contained :
         {"LINESTRING (0 58.00000000000001,0 59)", "LINESTRING (0 59,0 58.00000000000001,0 58)"}) {
        expect_geography_relation("st_overlaps", line, contained, false);
        expect_geography_relation("st_overlaps", contained, line, false);
    }
}

static void expect_ambiguous_polygon_relations(const std::string& a, const std::string& b) {
    for (const bool reverse : {false, true}) {
        const auto& left = reverse ? b : a;
        const auto& right = reverse ? a : b;
        expect_geography_relation("st_equals", left, right, false);
        for (const auto* function : {"st_contains", "st_within", "st_overlaps"}) {
            SCOPED_TRACE(std::string(function) + " " + left + " versus " + right);
            Block block;
            const auto type = make_nullable(std::make_shared<DataTypeUInt8>());
            const auto status = execute_geography_test(
                    function, {geography_test_hex(left), geography_test_hex(right)}, type, &block);
            EXPECT_FALSE(status.ok());
            EXPECT_NE(status.to_string().find("distinct raw Polygon boundaries"), std::string::npos)
                    << status.to_string();
        }
    }
}

TEST(VGeoFunctionsTest, GeographyPolygonRelationsPreserveRawCollidingApex) {
    const std::string outer = "POLYGON ((0 58,1 59,-1 59,0 58))";
    expect_ambiguous_polygon_relations(
            outer, "POLYGON ((0 58.00000000000001,1 59,-1 59,0 58.00000000000001))");
    const std::string inner = "POLYGON ((0 58.5,0.25 58.8,-0.25 58.8,0 58.5))";
    expect_geography_relation("st_contains", outer, inner, true);
    expect_geography_relation("st_contains", inner, outer, false);
    expect_geography_relation("st_within", inner, outer, true);
    expect_geography_relation("st_within", outer, inner, false);
}

TEST(VGeoFunctionsTest, GeographyPolygonRelationsPreserveRawCollidingHole) {
    expect_ambiguous_polygon_relations(
            "POLYGON ((-2 57,2 57,2 60,-2 60,-2 57),(0 58,0.5 59,-0.5 59,0 58))",
            "POLYGON ((-2 57,2 57,2 60,-2 60,-2 57),"
            "(0 58.00000000000001,0.5 59,-0.5 59,0 58.00000000000001))");
}

TEST(VGeoFunctionsTest, GeographyPolygonRawBoundariesRetainEquivalentRegions) {
    const std::vector<std::pair<std::string, std::string>> cases {
            {"POLYGON ((0 58,0 59,1 59,0 58))",
             "POLYGON ((0 58,0 58.00000000000001,0 59,1 59,0 58))"},
            {"POLYGON ((0 58,1 59,-1 59,0 58))", "POLYGON ((1 59,0 58,-1 59,1 59))"},
            {"POLYGON ((0 0,10 0,10 10,0 10,0 0),"
             "(1 1,2 1,2 2,1 2,1 1),(6 6,7 6,7 7,6 7,6 6))",
             "POLYGON ((10 10,10 0,0 0,0 10,10 10),"
             "(6 6,7 6,7 7,6 7,6 6),(1 1,2 1,2 2,1 2,1 1))"},
            {"POLYGON ((180 0,179 0,179 1,180 1,180 0))",
             "POLYGON ((-180 0,179 0,179 1,-180 1,-180 0))"},
            {"POLYGON ((0 90,0 80,90 80,0 90))", "POLYGON ((123 90,0 80,90 80,123 90))"},
    };
    for (const auto& [a, b] : cases) {
        SCOPED_TRACE(a + " versus " + b);
        for (const bool reverse : {false, true}) {
            const auto& left = reverse ? b : a;
            const auto& right = reverse ? a : b;
            expect_geography_relation("st_equals", left, right, true);
            expect_geography_relation("st_contains", left, right, true);
            expect_geography_relation("st_within", left, right, true);
            expect_geography_relation("st_overlaps", left, right, false);
        }
    }
}

static void expect_rounded_polygon_interior_error(const std::string& function,
                                                  const std::string& left,
                                                  const std::string& right) {
    SCOPED_TRACE(function + "(" + left + ", " + right + ")");
    Block block;
    const auto type = make_nullable(std::make_shared<DataTypeUInt8>());
    const auto status = execute_geography_test(
            function, {geography_test_hex(left), geography_test_hex(right)}, type, &block);
    EXPECT_FALSE(status.ok())
            << "unexpected boolean "
            << (status.ok() ? (*block.get_by_position(2).column)[0].get<TYPE_BOOLEAN>() : false);
    EXPECT_NE(status.to_string().find("ambiguous spherical Polygon boundary"), std::string::npos)
            << function << ": " << status.to_string();
}

TEST(VGeoFunctionsTest, GeographyRejectsRoundedLinePolygonInteriors) {
    const std::string polygon = "POLYGON ((58 0,59 -1,58 1,58 0))";
    const std::string line = "LINESTRING (58.00000000000001 0,58 1)";
    expect_rounded_polygon_interior_error("st_contains", polygon, line);
    expect_rounded_polygon_interior_error("st_within", line, polygon);
    expect_rounded_polygon_interior_error("st_touches", line, polygon);
    expect_rounded_polygon_interior_error("st_touches", polygon, line);
    const std::string partial = "LINESTRING (58.00000000000001 -1,58.00000000000001 2,58 1)";
    expect_geography_relation("st_touches", partial, polygon, false);
    expect_geography_relation("st_touches", polygon, partial, false);
    const std::string other = "POLYGON ((58.00000000000001 0,58 1,57 0,58.00000000000001 0))";
    for (const auto* name : {"st_touches", "st_overlaps"}) {
        expect_rounded_polygon_interior_error(name, polygon, other);
        expect_rounded_polygon_interior_error(name, other, polygon);
    }
    const std::string boundary = "LINESTRING (58 0,58 1)";
    expect_geography_relation("st_touches", boundary, polygon, true);
    expect_geography_relation("st_touches", polygon, boundary, true);
    expect_geography_relation("st_contains", polygon, boundary, false);
    expect_geography_relation("st_within", boundary, polygon, false);
}

TEST(VGeoFunctionsTest, GeographyRejectsRoundedPointPolygonOwnership) {
    const std::string polygon = "POLYGON ((58 0,57 -1,58 -1,58 0))";
    const std::string point = "POINT (58.00000000000001 0)";
    expect_rounded_polygon_interior_error("st_contains", polygon, point);
    expect_rounded_polygon_interior_error("st_within", point, polygon);
    for (const auto* name : {"st_intersects", "st_disjoint"}) {
        expect_rounded_polygon_interior_error(name, point, polygon);
        expect_rounded_polygon_interior_error(name, polygon, point);
    }
    expect_geography_relation("st_touches", point, polygon, false);
    expect_geography_relation("st_touches", polygon, point, false);
    expect_geography_relation("st_contains", polygon, "POINT (57.9 -0.5)", true);
    expect_geography_relation("st_contains", polygon, "POINT (58.1 0)", false);
    expect_geography_relation("st_intersects", polygon, "POINT (58 0)", true);
    expect_geography_relation("st_contains", polygon, "POINT (58 0)", false);
    const std::string separated = "LINESTRING (58.00000000000001 0,58.00000000000001 -1)";
    expect_geography_relation("st_touches", polygon, separated, false);
    expect_geography_relation("st_touches", separated, polygon, false);
}

TEST(VGeoFunctionsTest, GeographyRejectsRoundedPointPolygonStrictSide) {
    // Unlike a rounded vertex collision, this exterior point becomes strictly inside S2.
    const std::string point = "POINT (6.999999999999999 10.25)";
    const std::vector<std::string> polygons {
            "POLYGON ((7 10,8 10,8 11,7 11,7 10))", "POLYGON ((7 10,7 11,8 11,8 10,7 10))",
            "POLYGON ((6 9,9 9,9 12,6 12,6 9),(7 10,8 10,8 11,7 11,7 10))"};
    for (const auto& polygon : polygons) {
        expect_rounded_polygon_interior_error("st_contains", polygon, point);
        expect_rounded_polygon_interior_error("st_within", point, polygon);
        for (const auto* name : {"st_intersects", "st_disjoint"}) {
            expect_rounded_polygon_interior_error(name, point, polygon);
            expect_rounded_polygon_interior_error(name, polygon, point);
        }
        expect_geography_relation("st_touches", point, polygon, false);
        expect_geography_relation("st_touches", polygon, point, false);
    }
    // A small raw separation alone is not an error: keep correctly represented close points.
    const std::string polygon = polygons.front();
    expect_geography_relation("st_contains", polygon, "POINT (7.000000000000001 10.25)", true);
    expect_geography_relation("st_contains", polygon, "POINT (6.99999999999999 10.25)", false);
    expect_geography_relation("st_contains", polygon, "POINT (7 10.25)", false);
    expect_geography_relation("st_intersects", polygon, "POINT (7 10.25)", true);
    expect_geography_relation("st_touches", polygon, "POINT (7 10.25)", true);
}

TEST(VGeoFunctionsTest, GeographyRejectsRoundedOnlyLineEndpointContact) {
    const std::string left = "LINESTRING (0 57,0 58)";
    const std::string right = "LINESTRING (0 58.00000000000001,0 59)";
    for (const auto* name : {"st_intersects", "st_disjoint"}) {
        expect_rounded_polygon_interior_error(name, left, right);
        expect_rounded_polygon_interior_error(name, right, left);
    }
    const std::string exact = "LINESTRING (0 58,0 59)";
    expect_geography_relation("st_intersects", left, exact, true);
    expect_geography_relation("st_intersects", exact, left, true);
    const std::string separated = "LINESTRING (0 58.0000000001,0 59)";
    expect_geography_relation("st_intersects", left, separated, false);
    expect_geography_relation("st_disjoint", left, separated, true);
}

TEST(VGeoFunctionsTest, GeographyRejectsNonpointProximityWithoutRawContact) {
    const std::string polygon = "POLYGON ((0 0,1 0,1 1,0 1,0 0))";
    const std::string hole = "POLYGON ((-2 -2,2 -2,2 2,-2 2,-2 -2),(0 0,1 0,1 1,0 1,0 0))";
    const std::vector<std::pair<std::string, std::string>> cases {
            {"LINESTRING (0 0,1 0)", "LINESTRING (0 1e-15,1 1e-15)"},
            {"LINESTRING (-1 0,-1e-15 0)", "LINESTRING (0 0,1 0)"},
            {polygon, "LINESTRING (0.25 -1e-15,0.75 -1e-15)"},
            {polygon, "POLYGON ((0.25 -1,0.75 -1,0.75 -1e-15,0.25 -1e-15,0.25 -1))"},
            {hole, "LINESTRING (1e-15 0.25,1e-15 0.75)"},
    };
    // Distinct raw and S2 endpoints remain separated below the conversion-distance bound.
    // Neither a parallel arc, endpoint gap, nor proximity to a shell/hole proves contact.
    const auto type = make_nullable(std::make_shared<DataTypeUInt8>());
    for (const auto& [a, b] : cases) {
        for (const bool reverse : {false, true}) {
            const auto& left = reverse ? b : a;
            const auto& right = reverse ? a : b;
            expect_geography_relation("st_touches", left, right, false);
            for (const auto* function : {"st_intersects", "st_disjoint"}) {
                SCOPED_TRACE(std::string(function) + " " + left + " versus " + right);
                Block block;
                const auto status = execute_geography_test(
                        function, {geography_test_hex(left), geography_test_hex(right)}, type,
                        &block);
                EXPECT_FALSE(status.ok());
                EXPECT_NE(status.to_string().find("ambiguous spherical nonpoint proximity"),
                          std::string::npos)
                        << status.to_string();
            }
        }
    }
}

TEST(VGeoFunctionsTest, GeographyNonpointContactRetainsRawNodesAndInteriors) {
    const std::string polygon = "POLYGON ((0 0,1 0,1 1,0 1,0 0))";
    const std::string hole = "POLYGON ((-2 -2,2 -2,2 2,-2 2,-2 -2),(0 0,1 0,1 1,0 1,0 0))";
    const std::vector<std::pair<std::string, std::string>> cases {
            {"LINESTRING (10 45,10 46)", "LINESTRING (10 45.3,10 45.9)"},
            {"LINESTRING (0 0,2 0)", "LINESTRING (1 0,3 0)"},
            {"LINESTRING (45 -45,135 45)", "LINESTRING (90 0,135 45)"},
            {"LINESTRING (-1e-15 0,1e-15 0)", "LINESTRING (0 -1e-15,0 1e-15)"},
            {polygon, "LINESTRING (0.25 0,0.75 0)"},
            {polygon, "LINESTRING (0.25 1e-15,0.75 1e-15)"},
            {polygon, "POLYGON ((0.25 -1,0.75 -1,0.75 0,0.25 0,0.25 -1))"},
            {polygon, "POLYGON ((0.25 0.25,0.75 0.25,0.75 0.75,0.25 0.75,0.25 0.25))"},
            {hole, "LINESTRING (0 0.25,0 0.75)"},
    };
    // Shared subarcs need not share original vertices. Reuse exact noding, and retain
    // proper crossings and polygon interiors even without any common boundary node.
    for (const auto& [a, b] : cases) {
        for (const bool reverse : {false, true}) {
            const auto& left = reverse ? b : a;
            const auto& right = reverse ? a : b;
            expect_geography_relation("st_intersects", left, right, true);
            expect_geography_relation("st_disjoint", left, right, false);
        }
    }
    expect_geography_relation("st_intersects", "LINESTRING (0 0,1 0)",
                              "LINESTRING (0 1e-10,1 1e-10)", false);
    expect_geography_relation("st_disjoint", hole, "LINESTRING (0.25 0.25,0.75 0.25)", true);
}

TEST(VGeoFunctionsTest, GeographyKeepsDateLineEndpointAliasesEquivalent) {
    const std::string left = "LINESTRING (180 0,179 0)";
    const std::string right = "LINESTRING (-180 0,-179 0)";
    expect_geography_relation("st_intersects", left, right, true);
    expect_geography_relation("st_intersects", right, left, true);
    expect_geography_relation("st_disjoint", left, right, false);
    expect_geography_relation("st_disjoint", right, left, false);
}

TEST(VGeoFunctionsTest, GeographyAdjacentPolygonSubdivisionKeepsBoundaryContact) {
    const std::string a = "POLYGON ((0 58,1 58,1 59,0 59,0 58))";
    const std::vector<std::string> neighbours {
            "POLYGON ((0 58,0 58.00000000000001,0 59,-1 59,-1 58,0 58))",
            "POLYGON ((0 59,-1 59,-1 58,0 58,0 58.00000000000001,0 59))",
            "POLYGON ((0 58,-1 58,-1 59,0 59,0 58.00000000000001,0 58))"};
    for (const auto& b : neighbours) {
        expect_geography_relation("st_overlaps", a, b, false);
        expect_geography_relation("st_overlaps", b, a, false);
        expect_geography_relation("st_touches", a, b, true);
        expect_geography_relation("st_touches", b, a, true);
    }
}

TEST(VGeoFunctionsTest, GeographyRejectsRoundedStrictPolygonCoverage) {
    const std::string a = "POLYGON ((58 0,57 -1,58 -1,58 0))";
    const std::string b =
            "POLYGON ((58.00000000000001 0,57.75 -0.5,57.9 -0.5,58.00000000000001 0))";
    for (const auto* name : {"st_contains", "st_within", "st_overlaps"}) {
        expect_rounded_polygon_interior_error(name, a, b);
        expect_rounded_polygon_interior_error(name, b, a);
    }
    const std::string exact = "POLYGON ((58 0,57.75 -0.5,57.9 -0.5,58 0))";
    expect_geography_relation("st_contains", a, exact, true);
    expect_geography_relation("st_within", exact, a, true);
    expect_geography_relation("st_overlaps", a, exact, false);
    expect_geography_relation("st_overlaps", exact, a, false);
}

TEST(VGeoFunctionsTest, GeographyRejectsRoundedPolygonLineCoverage) {
    const std::string polygon = "POLYGON ((58 0,57 -1,58 -1,58 0))";
    for (const auto& line : {"LINESTRING (58.00000000000001 0,57.75 -0.5)",
                             "LINESTRING (57.75 -0.5,58.00000000000001 0)"}) {
        expect_rounded_polygon_interior_error("st_contains", polygon, line);
        expect_rounded_polygon_interior_error("st_within", line, polygon);
        expect_geography_relation("st_contains", line, polygon, false);
        expect_geography_relation("st_within", polygon, line, false);
    }
    const std::string exact = "LINESTRING (58 0,57.75 -0.5)";
    expect_geography_relation("st_contains", polygon, exact, true);
    expect_geography_relation("st_within", exact, polygon, true);
    expect_geography_relation("st_contains", polygon, "LINESTRING (58 0,58 -1)", false);
    expect_geography_relation("st_contains", polygon, "LINESTRING (58.1 0,58.2 -0.5)", false);
}

TEST(VGeoFunctionsTest, GeographyOverlapsRejectsRoundedPolygonVertex) {
    const std::string a = "POLYGON ((58 58,59 58,58 59,58 58))";
    const std::string b =
            "POLYGON ((58.00000000000001 58.00000000000001,57 57,59 57,"
            "58.00000000000001 58.00000000000001))";
    expect_rounded_polygon_interior_error("st_overlaps", a, b);
    expect_rounded_polygon_interior_error("st_overlaps", b, a);
    const std::string boundary = "POLYGON ((58 58,57 57,59 57,58 58))";
    expect_geography_relation("st_overlaps", a, boundary, false);
    expect_geography_relation("st_overlaps", boundary, a, false);
    const std::string overlap = "POLYGON ((58.1 58.1,57 57,59 57,58.1 58.1))";
    expect_geography_relation("st_overlaps", a, overlap, true);
    expect_geography_relation("st_overlaps", overlap, a, true);

    // Shared noding retains both 58 and 58+ulp on each boundary, so these are not
    // one-sided raw vertex collisions even though the two S2 positions coincide.
    const std::string shared = "POLYGON ((0 58,1 58,1 59,0 59,0 58))";
    const std::string subdivided = "POLYGON ((0 58,1 58,1 59,0 59,0 58.00000000000001,0 58))";
    expect_geography_relation("st_overlaps", shared, subdivided, false);
    expect_geography_relation("st_overlaps", subdivided, shared, false);
    expect_geography_relation("st_touches", shared, subdivided, false);
    expect_geography_relation("st_touches", subdivided, shared, false);
}

TEST(VGeoFunctionsTest, GeographyTouchesRejectsRoundedPolygonContact) {
    const std::string polygon = "POLYGON ((0 58.00000000000001,1 59,-1 59,0 58.00000000000001))";
    const std::string line = "LINESTRING (0 57,0 58)";
    expect_geography_relation("st_touches", line, polygon, false);
    expect_geography_relation("st_touches", polygon, line, false);
    const std::string southern = "POLYGON ((0 58,1 57,-1 57,0 58))";
    expect_geography_relation("st_touches", southern, polygon, false);
    expect_geography_relation("st_touches", polygon, southern, false);
    const std::string contact = "LINESTRING (0 57,0 58.00000000000001)";
    expect_geography_relation("st_touches", contact, polygon, true);
    expect_geography_relation("st_touches", polygon, contact, true);
    const std::vector<std::pair<std::string, std::string>> touching = {
            {"LINESTRING (0 57,0 59)", "POLYGON ((0 58,1 59,1 57,0 58))"},
            {"LINESTRING (-1 59,0 59)", "POLYGON ((0 58,0 60,1 60,1 58,0 58))"},
            {"LINESTRING (0 58.5,0 59.5)", "POLYGON ((0 58,0 60,1 60,1 58,0 58))"},
            {"LINESTRING (0 58,0 58.00000000000001,0 59.5)",
             "POLYGON ((0 58,0 60,1 60,1 58,0 58))"},
            {"LINESTRING (0 58,-1 57,1 57,0 58)", "POLYGON ((0 58,1 59,-1 59,0 58))"},
            {"LINESTRING (-180 57,-180 59)", "POLYGON ((180 58,-179 59,-179 57,180 58))"},
            {"LINESTRING (0 90,90 90)", "POLYGON ((0 90,0 80,90 80,0 90))"},
            {southern, "POLYGON ((0 58,1 59,-1 59,0 58))"},
            {"LINESTRING (0.5 0,0 0)",
             "POLYGON ((-2 -2,2 -2,2 2,-2 2,-2 -2),(0 -1,0 1,1 1,1 -1,0 -1))"}};
    for (const auto& [left, right] : touching) {
        expect_geography_relation("st_touches", left, right, true);
        expect_geography_relation("st_touches", right, left, true);
    }
    const std::string crossing = "LINESTRING (-1 59,0 59,1 59)";
    const std::string square = "POLYGON ((0 58,0 60,1 60,1 58,0 58))";
    expect_geography_relation("st_touches", crossing, square, false);
    expect_geography_relation("st_touches", square, crossing, false);
}

TEST(VGeoFunctionsTest, GeographyIntersectsRejectsRoundedPolygonContact) {
    const std::string polygon = "POLYGON ((0 58.00000000000001,1 59,-1 59,0 58.00000000000001))";
    for (const auto& other : {"LINESTRING (0 57,0 58)", "POLYGON ((0 58,1 57,-1 57,0 58))"}) {
        for (const auto* name : {"st_intersects", "st_disjoint"}) {
            expect_rounded_polygon_interior_error(name, polygon, other);
            expect_rounded_polygon_interior_error(name, other, polygon);
        }
    }
    const std::string separated = "LINESTRING (0 57,0 57.9)";
    expect_geography_relation("st_intersects", separated, polygon, false);
    expect_geography_relation("st_intersects", polygon, separated, false);
    expect_geography_relation("st_disjoint", separated, polygon, true);
    expect_geography_relation("st_disjoint", polygon, separated, true);
}

TEST(VGeoFunctionsTest, GeographyIntersectsKeepsIndependentPolygonWitnesses) {
    // The first five pairs retain a rounded-only collision, but another raw contact or
    // admitted interior vertex proves intersection. The first vertex need not be that witness.
    const std::vector<std::pair<std::string, std::string>> cases {
            {"POLYGON ((0 58.00000000000001,1 59,-1 59,0 58.00000000000001))",
             "LINESTRING (0 57,0 58,1 59)"},
            {"POLYGON ((0 58.00000000000001,1 59,-1 59,0 58.00000000000001))",
             "LINESTRING (0 58,0.1 58.8)"},
            {"POLYGON ((0 58.00000000000001,1 59,-1 59,0 58.00000000000001))",
             "LINESTRING (0 57,0 58,0 58.5)"},
            {"POLYGON ((0 58.00000000000001,1 59,-1 59,0 58.00000000000001))",
             "POLYGON ((0 58,1 57,1 59,0 58))"},
            {"POLYGON ((0 58.00000000000001,1 59,-1 59,0 58.00000000000001))",
             "POLYGON ((0 58,0.1 58.8,-0.1 58.8,0 58))"},
            {"POLYGON ((0 0,2 0,2 2,0 2,0 0))", "LINESTRING (-1 1,3 1)"},
            {"POLYGON ((0 0,2 0,2 2,0 2,0 0))", "LINESTRING (0.5 0.5,1.5 0.5)"},
            {"POLYGON ((-2 -2,2 -2,2 2,-2 2,-2 -2),(0 -1,0 1,1 1,1 -1,0 -1))",
             "LINESTRING (0.5 0,0 0)"},
            {"POLYGON ((180 58,-179 59,-179 57,180 58))", "LINESTRING (-180 57,-180 59)"},
            {"POLYGON ((0 90,0 80,90 80,0 90))", "LINESTRING (0 90,90 90)"},
            {"POLYGON ((0 58,1 58,1 59,0 59,0 58))",
             "POLYGON ((0 58,0 58.00000000000001,0 59,-1 59,-1 58,0 58))"},
    };
    for (const auto& [left, right] : cases) {
        expect_geography_relation("st_intersects", left, right, true);
        expect_geography_relation("st_intersects", right, left, true);
        expect_geography_relation("st_disjoint", left, right, false);
        expect_geography_relation("st_disjoint", right, left, false);
    }
}

TEST(VGeoFunctionsTest, GeographyTouchesRejectsCollapsedLinePolygonInterior) {
    const std::vector<std::pair<std::string, std::string>> cases {
            {"LINESTRING (0 57,0 58.00000000000001)", "POLYGON ((0 58,1 59,-1 59,0 58))"},
            {"LINESTRING (0 57,0 58,0 58.00000000000001)",
             "POLYGON ((0 58.00000000000001,1 59,-1 59,0 58.00000000000001))"}};
    for (const auto& [line, polygon] : cases) {
        for (const bool reverse : {false, true}) {
            Block block;
            const auto type = make_nullable(std::make_shared<DataTypeUInt8>());
            const auto status =
                    execute_geography_test("st_touches",
                                           {geography_test_hex(reverse ? polygon : line),
                                            geography_test_hex(reverse ? line : polygon)},
                                           type, &block);
            EXPECT_FALSE(status.ok());
            EXPECT_NE(status.to_string().find(
                              "collapsed raw line edges outside the Polygon boundary"),
                      std::string::npos)
                    << status.to_string();
        }
    }
}

TEST(VGeoFunctionsTest, GeographyTouchesRejectsRoundedLineEndpointGap) {
    const std::string line = "LINESTRING (0 57,0 58)";
    const std::string gap = "LINESTRING (0 58.00000000000001,0 59)";
    expect_geography_relation("st_touches", line, gap, false);
    expect_geography_relation("st_touches", gap, line, false);
    const std::string overlap = "LINESTRING (0 57,0 58.00000000000001)";
    const std::string other = "LINESTRING (0 58,0 59)";
    expect_geography_relation("st_touches", overlap, other, false);
    expect_geography_relation("st_touches", other, overlap, false);
    expect_geography_relation("st_overlaps", overlap, other, true);
    for (const auto* touching : {"LINESTRING (0 58,0 59)", "LINESTRING (-1 58,0 58,1 58)"}) {
        expect_geography_relation("st_touches", line, touching, true);
        expect_geography_relation("st_touches", touching, line, true);
    }
    expect_geography_relation("st_touches", "LINESTRING (180 57,180 58)",
                              "LINESTRING (-180 58,-180 59)", true);
    expect_geography_relation("st_touches", "LINESTRING (0 89,0 90)", "LINESTRING (90 90,90 89)",
                              true);
}

TEST(VGeoFunctionsTest, GeographyEqualsUsesPointSetsAndOverlapsRequiresPositiveDimension) {
    struct Case {
        std::string left;
        std::string right;
        bool equal;
        bool overlap;
    };
    const std::string polygon = "POLYGON ((0 0,2 0,2 2,0 2,0 0))";
    const std::vector<Case> cases {
            {"POINT (1 2)", "POINT (1 2)", true, false},
            {"POLYGON ((89 0,-89 0,-89 -10,89 -10,89 0))",
             "POLYGON ((89 0,0.000000000000001 0,0 0,-0.000000000000001 0,-89 0,-89 -10,89 -10,89 "
             "0))",
             true, false},
            {"LINESTRING (-0.000000000000001 0,-0.000000000000001 2)",
             "LINESTRING (-0.000000000000002 1,-0.000000000000002 3)", false, false},
            {"LINESTRING (-170 0,-170 0.00000000000001)",
             "LINESTRING (-170 0.00000000000001,-170 0.000000000000005,-170 0)", true, false},
            {"POINT (1 2)", "POINT (1.00000001 2)", false, false},
            {"POINT (180 0)", "POINT (-180 0)", true, false},
            {"POINT (0 90)", "POINT (30 90)", true, false},
            {"LINESTRING (0 0,2 0)", "LINESTRING (2 0,1 0,0 0)", true, false},
            {"LINESTRING (10 45,10 46)", "LINESTRING (10 46,10 45.5,10 45)", true, false},
            {"LINESTRING (10 80,-170 80)", "LINESTRING (10 80,30 90,-170 80)", true, false},
            {"LINESTRING (-45 -45,45 45)", "LINESTRING (45 45,0 0,-45 -45)", true, false},
            {"LINESTRING (0 0,2 0)", "LINESTRING (1 0,3 0)", false, true},
            {"LINESTRING (0 0,2 0)", "LINESTRING (1 -1,1 1)", false, false},
            {"LINESTRING (0 0,2 0)", "LINESTRING (2 0,3 0)", false, false},
            {"LINESTRING (0 0,2 0)", "LINESTRING (0.5 0,1.5 0)", false, false},
            {polygon, "POLYGON ((2 2,2 0,0 0,0 2,2 2))", true, false},
            {polygon, "POLYGON ((1 0,3 0,3 2,1 2,1 0))", false, true},
            {polygon, "POLYGON ((2 0,4 0,4 2,2 2,2 0))", false, false},
            {polygon, "POLYGON ((0.5 0.5,1 0.5,1 1,0.5 1,0.5 0.5))", false, false},
            {polygon, "LINESTRING (0 0,2 0)", false, false},
            {"POLYGON ((10 45,11 45,11 46,10 46,10 45))",
             "POLYGON ((10 45,11 45,11 46,10 46,10 45.5,10 45))", true, false},
            {"LINESTRING (-0.00000000000001 0,0.00000000000001 0)",
             "LINESTRING (0 -0.00000000000001,0 0.00000000000001)", false, false},
            {"LINESTRING (0 0,1 0)", "LINESTRING (0 0.000000000001,1 0.000000000001)", false,
             false},
    };
    for (const auto& value : cases) {
        for (bool reverse : {false, true}) {
            const auto& left = reverse ? value.right : value.left;
            const auto& right = reverse ? value.left : value.right;
            expect_geography_relation("st_equals", left, right, value.equal);
            expect_geography_relation("st_overlaps", left, right, value.overlap);
        }
    }
}

TEST(VGeoFunctionsTest, GeographyRotatedSubdivisionsPreserveExactRawArcs) {
    const std::vector<std::pair<std::string, std::string>> equivalents {
            {"LINESTRING (45 -45,135 45)", "LINESTRING (45 -45,90 0,135 45)"},
            {"POLYGON ((45 -45,135 45,135 -45,45 -45))",
             "POLYGON ((45 -45,90 0,135 45,135 -45,45 -45))"},
            {"LINESTRING (-44.99999999999997 -45,134.99999999999997 45)",
             "LINESTRING (-44.99999999999997 -45,45 0,134.99999999999997 45)"},
            {"LINESTRING (25 -17,95 17)", "LINESTRING (95 17,60 0,25 -17)"},
            {"LINESTRING (170 -32,-110 32)", "LINESTRING (170 -32,-150 0,-110 32)"},
            {"LINESTRING (45.25 -17.5,135.75 17.5)",
             "LINESTRING (45.25 -17.5,90.5 0,135.75 17.5)"}};
    for (const auto& [a, b] : equivalents) {
        for (bool reverse : {false, true}) {
            const auto& left = reverse ? b : a;
            const auto& right = reverse ? a : b;
            for (const auto* function : {"st_equals", "st_contains", "st_within"}) {
                expect_geography_relation(function, left, right, true);
            }
            expect_geography_relation("st_overlaps", left, right, false);
        }
    }
    const std::string arc = "LINESTRING (45 -45,135 45)";
    for (const auto* latitude : {"0.000001", "0.000000000000001", "1e-300"}) {
        const std::string different = std::string("LINESTRING (45 -45,90 ") + latitude + ",135 45)";
        for (const auto* function : {"st_equals", "st_contains", "st_within"}) {
            expect_geography_relation(function, arc, different, false);
            expect_geography_relation(function, different, arc, false);
        }
    }
    // An exactly collinear point just outside an endpoint must not be appended to the arc.
    expect_geography_relation("st_contains", "LINESTRING (0 0,1 0)",
                              "LINESTRING (-1e-300 0,0 0,1 0)", false);
    expect_geography_relation("st_contains", "LINESTRING (-1e-300 0,0 0,1 0)",
                              "LINESTRING (0 0,1 0)", true);
}

TEST(VGeoFunctionsTest, GeographyAtomicEdgesRetainOriginalLineBoundaries) {
    const std::string open = "LINESTRING (0 0,1 0,0 0,2 0)";
    const std::string closed = "LINESTRING (0 0,1 0,0 0)";
    for (bool reverse : {false, true}) {
        const auto touches = [&](const std::string& a, const std::string& b, bool expected) {
            expect_geography_relation("st_touches", reverse ? b : a, reverse ? a : b, expected);
        };
        touches(open, "LINESTRING (1 -1,1 1)", false);
        touches(open, "LINESTRING (0 -1,0 1)", true);
        touches(closed, "LINESTRING (0 -1,0 1)", false);
        touches(closed, "LINESTRING (-1 0,0 0)", true);
    }
    expect_geography_relation("st_equals", closed, "LINESTRING (0 0,1 0)", true);
    expect_geography_relation("st_contains", closed, "LINESTRING (0 0,1 0)", true);
    expect_geography_relation("st_contains", open, "LINESTRING (0.5 0,1.5 0)", true);
}

TEST(VGeoFunctionsTest, GeographyNearAntipodalPointMembershipUsesRawArc) {
    const std::string line = "LINESTRING (-44.99999999999997 -45,134.99999999999997 45)";
    const std::string point = "POINT (45 0)";
    for (bool reverse : {false, true}) {
        expect_geography_relation("st_intersects", reverse ? point : line, reverse ? line : point,
                                  true);
        expect_geography_relation("st_disjoint", reverse ? point : line, reverse ? line : point,
                                  false);
        expect_geography_relation("st_touches", reverse ? point : line, reverse ? line : point,
                                  false);
    }
    expect_geography_relation("st_contains", line, point, true);
    expect_geography_relation("st_within", point, line, true);
    for (const auto* offset : {"0.000001", "0.000000000000001", "1e-300"}) {
        const std::string nearby = std::string("POINT (45 ") + offset + ")";
        expect_geography_relation("st_intersects", line, nearby, false);
        expect_geography_relation("st_contains", line, nearby, false);
    }
}

static void expect_geography_polygon_relation(const std::string& polygon, const std::string& value,
                                              bool interior, bool boundary) {
    expect_geography_relation("st_contains", polygon, value, interior);
    expect_geography_relation("st_within", value, polygon, interior);
    for (bool reverse : {false, true}) {
        const auto& left = reverse ? value : polygon;
        const auto& right = reverse ? polygon : value;
        expect_geography_relation("st_intersects", left, right, interior || boundary);
        expect_geography_relation("st_disjoint", left, right, !interior && !boundary);
        expect_geography_relation("st_touches", left, right, boundary);
    }
}

TEST(VGeoFunctionsTest, GeographyPolygonInteriorIsIndependentOfRawArcSubdivision) {
    const Strings rings {
            "(-44.99999999999997 -45,134.99999999999997 45,45 45,-44.99999999999997 -45)",
            "(-44.99999999999997 -45,45 0,134.99999999999997 45,45 45,-44.99999999999997 -45)",
            "(-44.99999999999997 -45,45 45,134.99999999999997 45,-44.99999999999997 -45)",
            "(-44.99999999999997 -45,45 45,134.99999999999997 45,45 0,-44.99999999999997 -45)"};
    for (const auto& ring : rings) {
        const std::string polygon = "POLYGON (" + ring + ")";
        expect_geography_polygon_relation(polygon, "POINT (45 0.000001)", true, false);
        expect_geography_polygon_relation(polygon, "POINT (45 -0.000001)", false, false);
        expect_geography_polygon_relation(polygon, "POINT (45 0)", false, true);
        expect_geography_polygon_relation(polygon, "POINT (-44.99999999999997 -45)", false, true);
        const std::string hole = "POLYGON (" + ring + ",(44 20,46 20,46 22,44 22,44 20))";
        expect_geography_polygon_relation(hole, "POINT (45 0.000001)", true, false);
        expect_geography_polygon_relation(hole, "POINT (45 21)", false, false);
        expect_geography_polygon_relation(hole, "POINT (44 21)", false, true);
    }
}

TEST(VGeoFunctionsTest, GeographyPolygonInteriorRetainsSmallerRingAndNonpointValues) {
    const Strings rings {
            "(-44.99999999999997 -45,134.99999999999997 45,45 45,-44.99999999999997 -45)",
            "(-44.99999999999997 -45,45 0,134.99999999999997 45,45 45,-44.99999999999997 -45)"};
    for (const auto& ring : rings) {
        const std::string polygon = "POLYGON (" + ring + ")";
        for (const auto* value :
             {"LINESTRING (45 0.000001,45 0.000002)",
              "POLYGON ((44.999999 0.00001,45.000001 0.00001,45 0.00002,44.999999 0.00001))"}) {
            expect_geography_polygon_relation(polygon, value, true, false);
        }
    }
    const std::string original = "POLYGON (" + rings[0] + ")";
    const std::string subdivided = "POLYGON (" + rings[1] + ")";
    expect_geography_relation("st_equals", original, subdivided, true);
    expect_geography_relation("st_equals", subdivided, original, true);
    expect_geography_relation("st_overlaps", original, subdivided, false);
    expect_geography_relation("st_overlaps", subdivided, original, false);
    // Normalization must select the smaller raw ring even when conversion puts
    // the third vertex on the opposite side of the rounded near-antipodal edge.
    for (const auto* ring :
         {"(-44.99999999999997 -45,134.99999999999997 45,45 0.000001,-44.99999999999997 -45)",
          "(-44.99999999999997 -45,45 0,134.99999999999997 45,45 0.000001,-44.99999999999997 "
          "-45)"}) {
        const std::string polygon = "POLYGON (" + std::string(ring) + ")";
        expect_geography_polygon_relation(polygon, "POINT (45 0.0000005)", true, false);
        expect_geography_polygon_relation(polygon, "POINT (45 20)", false, false);
        expect_geography_polygon_relation(polygon, "POINT (45 -20)", false, false);
    }
}

TEST(VGeoFunctionsTest, GeographyPolygonInteriorPreservesPoleAliases) {
    for (const auto* polygon :
         {"POLYGON ((0 90,90 90,90 80,0 80,0 90))", "POLYGON ((0 90,90 80,0 80,0 90))"}) {
        expect_geography_polygon_relation(polygon, "POINT (45 89)", true, false);
        expect_geography_polygon_relation(polygon, "POINT (-45 89)", false, false);
        expect_geography_polygon_relation(polygon, "POINT (45 90)", false, true);
    }
}

TEST(VGeoFunctionsTest, GeographySyntheticMidpointRemainsLineInterior) {
    for (const auto* line : {"LINESTRING (-44.99999999999997 -45,134.99999999999997 45)",
                             "LINESTRING (-44.99999999999997 -45,45 0,134.99999999999997 45)"}) {
        const std::string crossing = "LINESTRING (45 -1,45 1)";
        expect_geography_relation("st_intersects", line, crossing, true);
        expect_geography_relation("st_intersects", crossing, line, true);
        expect_geography_relation("st_touches", line, crossing, false);
        expect_geography_relation("st_touches", crossing, line, false);
    }
}

static void geography_measurement(const std::string& name, const Strings& wkts, double* result) {
    Strings arguments;
    for (const auto& wkt : wkts) {
        arguments.push_back(geography_test_hex(wkt));
    }
    Block block;
    ASSERT_TRUE(execute_geography_test(name, arguments,
                                       make_nullable(std::make_shared<DataTypeFloat64>()), &block)
                        .ok());
    ASSERT_FALSE(block.get_by_position(arguments.size()).column->is_null_at(0));
    const auto value = (*block.get_by_position(arguments.size()).column)[0];
    *result = value.get<TYPE_DOUBLE>();
}

static void expect_geography_line_pole(const std::string& line) {
    for (bool reverse : {false, true}) {
        const std::string point = "POINT (0 -90)";
        double distance = -1;
        geography_measurement("st_distance", {reverse ? point : line, reverse ? line : point},
                              &distance);
        EXPECT_EQ(distance, 0);
        expect_geography_relation("st_intersects", reverse ? point : line, reverse ? line : point,
                                  true);
        expect_geography_relation("st_touches", reverse ? point : line, reverse ? line : point,
                                  false);
    }
    expect_geography_relation("st_contains", line, "POINT (0 -90)", true);
    expect_geography_relation("st_within", "POINT (0 -90)", line, true);
    for (const auto* name : {"st_area_square_meters", "st_area_square_km"}) {
        double area = -1;
        geography_measurement(name, {line}, &area);
        EXPECT_EQ(area, 0);
    }
}

TEST(VGeoFunctionsTest, GeographyLineAdmissionUsesOriginalNearAntipodalArc) {
    const Strings lines {"LINESTRING (30 58,-150 -58.00000000000001)",
                         "LINESTRING (30 58,30 -90,-150 -58.00000000000001)",
                         "LINESTRING (-150 -58.00000000000001,30 58)"};
    double reference_length = 0;
    geography_measurement("st_length", {lines[1]}, &reference_length);
    for (const auto& line : lines) {
        double length = 0;
        geography_measurement("st_length", {line}, &length);
        EXPECT_NEAR(length, reference_length, 1e-6);
        expect_geography_line_pole(line);
        expect_geography_relation("st_equals", line, lines[1], true);
    }
}

TEST(VGeoFunctionsTest, GeographyRawEndpointsSurviveRoundedPointCollisions) {
    const std::string point = "POINT (0 58)";
    const std::string line = "LINESTRING (0 58.00000000000001,180 -58)";
    for (bool reverse : {false, true}) {
        // The raw minor arc runs from 58+ulp via the north pole; latitude 58 is outside it.
        // The old [0,1) assertion admitted a rounded zero despite the raw-disjoint predicates.
        Block distance;
        const auto status = execute_geography_test(
                "st_distance",
                {geography_test_hex(reverse ? line : point),
                 geography_test_hex(reverse ? point : line)},
                make_nullable(std::make_shared<DataTypeFloat64>()), &distance);
        EXPECT_EQ(status.code(), ErrorCode::NOT_IMPLEMENTED_ERROR);
        EXPECT_NE(status.to_string().find("ambiguous spherical Point/shape zero distance"),
                  std::string::npos)
                << status;
        expect_geography_relation("st_intersects", reverse ? line : point, reverse ? point : line,
                                  false);
        expect_geography_relation("st_touches", reverse ? line : point, reverse ? point : line,
                                  false);
    }
    expect_geography_relation("st_touches", line, "POINT (0 58.00000000000001)", true);
    expect_geography_relation("st_contains", line, point, false);
    expect_geography_relation("st_within", point, line, false);
    expect_geography_relation("st_equals", point, "POINT (0 58.00000000000001)", false);
    expect_geography_relation("st_equals", "POINT (180 58)", "POINT (-180 58)", true);
    expect_geography_relation("st_equals", "POINT (0 90)", "POINT (123 90)", true);
    double distance = -1;
    geography_measurement("st_distance",
                          {"LINESTRING (0 58,90 0,0 58.00000000000001,180 -58)", "POINT (90 45)"},
                          &distance);
    EXPECT_GE(distance, 0);
}

static void expect_geography_hole_points(const std::string& polygon) {
    for (bool reverse : {false, true}) {
        const std::string center = "POINT (45 0.000001)";
        const std::string vertex = "POINT (45 0.0000011)";
        expect_geography_relation("st_intersects", reverse ? center : polygon,
                                  reverse ? polygon : center, false);
        expect_geography_relation("st_touches", reverse ? vertex : polygon,
                                  reverse ? polygon : vertex, true);
        double distance = -1;
        geography_measurement("st_distance",
                              {reverse ? center : polygon, reverse ? polygon : center}, &distance);
        EXPECT_GT(distance, 0);
        EXPECT_LT(distance, 0.02);
    }
}

static void expect_geography_raw_binary(const std::string& polygon) {
    const auto hex = geography_test_hex(polygon);
    const auto bytes = spatial_test_wkb(hex);
    Block accessor;
    ASSERT_TRUE(execute_geography_test("st_asbinary", {hex},
                                       make_nullable(std::make_shared<DataTypeVarbinary>()),
                                       &accessor)
                        .ok());
    const auto raw = accessor.get_by_position(1).column->get_data_at(0);
    EXPECT_EQ(std::string(raw.data, raw.size), bytes);
}

TEST(VGeoFunctionsTest, GeographyLineAdmissionRetainsInvalidAndAliasBoundaries) {
    for (const auto* line : {"LINESTRING (0 0,180 0)", "LINESTRING (30 58,-150 -58)",
                             "LINESTRING (1 1,1 1)", "LINESTRING (1 1)"}) {
        const auto hex = geography_test_hex(line);
        for (const auto* name : {"st_length", "st_area_square_meters", "st_area_square_km"}) {
            Block block;
            EXPECT_FALSE(execute_geography_test(name, {hex},
                                                make_nullable(std::make_shared<DataTypeFloat64>()),
                                                &block)
                                 .ok());
        }
        Block distance;
        EXPECT_FALSE(execute_geography_test(
                             "st_distance", {hex, geography_test_hex("POINT (0 -90)")},
                             make_nullable(std::make_shared<DataTypeFloat64>()), &distance)
                             .ok());
    }
    expect_geography_raw_binary("LINESTRING (0 0,180 0)");
    double length = -1;
    geography_measurement("st_length", {"LINESTRING (0 90,90 90)"}, &length);
    EXPECT_GE(length, 0);
    const auto hex = geography_test_hex("LINESTRING (30 58,-150 -58.00000000000001)");
    GeoParseStatus status;
    EXPECT_EQ(GeoShape::from_wkb(hex.data(), hex.size(), status), nullptr);
    expect_geography_raw_binary("LINESTRING (30 58,-150 -58.00000000000001)");
}

TEST(VGeoFunctionsTest, GeographyStabilizesShellBeforeAdmittingHole) {
    const Strings rings {
            "(-44.99999999999997 -45,134.99999999999997 45,45 45,-44.99999999999997 -45)",
            "(-44.99999999999997 -45,45 0,134.99999999999997 45,45 45,-44.99999999999997 -45)",
            "(-44.99999999999997 -45,45 45,134.99999999999997 45,-44.99999999999997 -45)"};
    const std::string hole =
            "(44.9999999 0.0000009,45.0000001 0.0000009,45 0.0000011,"
            "44.9999999 0.0000009)";
    double reference = 0;
    for (const auto& ring : rings) {
        const std::string polygon = "POLYGON (" + ring + "," + hole + ")";
        double area = 0;
        double km = 0;
        geography_measurement("st_area_square_meters", {polygon}, &area);
        geography_measurement("st_area_square_km", {polygon}, &km);
        EXPECT_GT(area, 1e7);
        EXPECT_NEAR(area, km * 1e6, area * 1e-12);
        if (reference == 0) {
            reference = area;
        }
        EXPECT_NEAR(area, reference, reference * 1e-12);
        expect_geography_relation("st_contains", polygon, "POINT (45 0.0000005)", true);
        expect_geography_hole_points(polygon);
        expect_geography_raw_binary(polygon);
    }
    // Legacy STRING admission is intentionally unchanged.
    const auto hex = geography_test_hex("POLYGON (" + rings[0] + "," + hole + ")");
    GeoParseStatus status;
    EXPECT_EQ(GeoShape::from_wkb(hex.data(), hex.size(), status), nullptr);
}

TEST(VGeoFunctionsTest, GeographyStableAdmissionKeepsInvalidRingsAndNestedDepths) {
    for (const auto* invalid :
         {"POLYGON ((0 0,4 4,0 4,4 0,0 0))", "POLYGON ((0 0,4 0,4 4,0 4,0 0),(5 1,6 1,6 2,5 1))",
          "POLYGON ((0 0,4 0,4 4,0 4,0 0),(3 1,5 1,3 2,3 1))", "POLYGON ((0 0,4 0,4 4,0 4))",
          "POLYGON ((0 0,180 0,90 45,0 0))"}) {
        Block block;
        EXPECT_FALSE(execute_geography_test("st_area_square_meters", {geography_test_hex(invalid)},
                                            make_nullable(std::make_shared<DataTypeFloat64>()),
                                            &block)
                             .ok());
    }
    // InitNested retains an island at depth two even if it reorders input loops.
    const std::string nested =
            "POLYGON ((0 0,10 0,10 10,0 10,0 0),"
            "(4 4,6 4,6 6,4 6,4 4),(2 2,8 2,8 8,2 8,2 2))";
    expect_geography_relation("st_contains", nested, "POINT (5 5)", true);
    expect_geography_relation("st_intersects", nested, "POINT (3 3)", false);
}

TEST(VGeoFunctionsTest, GeographyAreaUsesStableRingsInBothUnits) {
    const Strings rings {
            "(-44.99999999999997 -45,134.99999999999997 45,45 45,-44.99999999999997 -45)",
            "(-44.99999999999997 -45,45 0,134.99999999999997 45,45 45,-44.99999999999997 -45)",
            "(-44.99999999999997 -45,45 45,134.99999999999997 45,-44.99999999999997 -45)"};
    const std::string hole = "(44 20,46 20,46 22,44 22,44 20)";
    double reference_meters = 0;
    for (const auto* name : {"st_area_square_meters", "st_area_square_km"}) {
        double reference = 0;
        double hole_area = 0;
        geography_measurement(name, {"POLYGON (" + rings[1] + ")"}, &reference);
        geography_measurement(name, {"POLYGON (" + hole + ")"}, &hole_area);
        EXPECT_GT(reference, 1e7);
        for (const auto& ring : rings) {
            double area = 0;
            geography_measurement(name, {"POLYGON (" + ring + ")"}, &area);
            EXPECT_NEAR(area, reference, reference * 1e-12);
            geography_measurement(name, {"POLYGON (" + ring + "," + hole + ")"}, &area);
            EXPECT_NEAR(area, reference - hole_area, reference * 1e-12);
        }
        if (std::string_view(name) == "st_area_square_meters") {
            reference_meters = reference;
        } else {
            EXPECT_NEAR(reference * 1e6, reference_meters, reference_meters * 1e-12);
        }
        for (const auto* zero : {"POINT (0 45)", "LINESTRING (0 0,90 0)"}) {
            double area = -1;
            geography_measurement(name, {zero}, &area);
            EXPECT_EQ(area, 0);
        }
    }
}

TEST(VGeoFunctionsTest, GeographyBearingUsesOriginalNearAntipodalDirections) {
    struct Case {
        std::string from;
        std::string to;
        std::string north;
        double bearing;
    };
    const std::vector<Case> cases {
            {"POINT (0 45)", "POINT (180 -44.99999999999999)", "POINT (0 46)", 0},
            {"POINT (0 45)", "POINT (180 -45.00000000000001)", "POINT (0 46)", M_PI},
            {"POINT (0 -45)", "POINT (180 45.00000000000001)", "POINT (0 -44)", 0},
            {"POINT (0 -45)", "POINT (180 44.99999999999999)", "POINT (0 -44)", M_PI},
            {"POINT (10 45)", "POINT (-170 -44.99999999999999)", "POINT (10 46)", 0},
            {"POINT (1e-300 0)", "POINT (-180 0)", "POINT (1e-300 1)", M_PI_2}};
    for (const auto& value : cases) {
        SCOPED_TRACE(value.to);
        double result = -1;
        geography_measurement("st_azimuth", {value.from, value.to}, &result);
        EXPECT_NEAR(result, value.bearing, 1e-14);
        geography_measurement("st_angle", {value.north, value.from, value.to}, &result);
        EXPECT_NEAR(result, value.bearing, 1e-14);
    }
}

TEST(VGeoFunctionsTest, GeographyPortableArithmeticRetainsTinyResiduals) {
    const std::array<std::array<double, 4>, 5> cases {{{0, 0, 0, 1},
                                                       {45, 0, 135, 0},
                                                       {10, 45, 10, 46},
                                                       {1e-300, 0, -180, 0},
                                                       {0, 45, 180, std::nextafter(-45.0, 0.0)}}};
    for (const auto& point : cases) {
        const auto arc = geography_portable_arc_for_test(point[0], point[1], point[2], point[3]);
        EXPECT_NEAR(arc[0] * arc[0] + arc[1] * arc[1] + arc[2] * arc[2], 1, 1e-15);
        EXPECT_NEAR(arc[3] * arc[3] + arc[4] * arc[4] + arc[5] * arc[5], 1, 1e-15);
        EXPECT_NEAR(arc[0] * arc[3] + arc[1] * arc[4] + arc[2] * arc[5], 0, 1e-15);
        const auto wkb = [&](int offset) {
            std::ostringstream text;
            text << std::setprecision(17) << "POINT (" << point[offset] << " " << point[offset + 1]
                 << ")";
            return spatial_test_wkb(geography_test_hex(text.str()));
        };
        const auto from = wkb(0);
        const auto to = wkb(2);
        double native = -1;
        double portable = -1;
        ASSERT_TRUE(geography_bearing(StringRef(from), StringRef(to), &native));
        ASSERT_TRUE(geography_portable_bearing_for_test(StringRef(from), StringRef(to), &portable));
        EXPECT_NEAR(portable, native, 1e-14);
    }
    // Squaring the 1e-300 longitude residual underflows binary64, but the unique
    // minor arc still goes east. Both normal and midpoint must retain its direction.
    const auto tiny = geography_portable_arc_for_test(1e-300, 0, -180, 0);
    EXPECT_NEAR(tiny[2], 1, 1e-15);
    EXPECT_NEAR(tiny[4], 1, 1e-15);
}

TEST(VGeoFunctionsTest, GeographyDensePolygonBoundsCandidateWork) {
    for (bool swap_axes : {false, true}) {
        for (const int count : {32, 64, 128}) {
            constexpr double radius = 1e-14;
            std::ostringstream polygon;
            polygon << std::setprecision(17) << "POLYGON ((";
            const auto vertex = [&](double x, double y) {
                polygon << (swap_axes ? y : x) << " " << (swap_axes ? x : y) << ",";
            };
            for (int i = 0; i < count; ++i) {
                vertex((i % 2 == 0 ? -radius : radius) + 0.1 * radius * i / (count - 1),
                       -radius + 2 * radius * i / (count - 1));
            }
            vertex(2 * radius, 2 * radius);
            vertex(2 * radius, -2 * radius);
            polygon << -radius << " " << -radius << "))";
            const auto a = spatial_test_wkb(geography_test_hex(polygon.str()));
            const auto b = spatial_test_wkb(geography_test_hex("POINT (0 0)"));
            size_t proofs = 0;
            size_t visits = 0;
            EXPECT_LE(geography_calculation_edges_for_test(a, b, &proofs, &visits), 4 * count);
            EXPECT_LE(proofs, 4 * count);
            EXPECT_LE(visits, 16 * count);
            const auto suffix = std::to_string(count) + (swap_axes ? "_swapped" : "");
            RecordProperty("polygon_proofs_" + suffix, std::to_string(proofs));
            RecordProperty("polygon_visits_" + suffix, std::to_string(visits));
        }
    }
}

TEST(VGeoFunctionsTest, GeographyPolygonCandidateBandsKeepCompleteFallback) {
    // More than eight genuine nodes must still split the equatorial ring edge.
    // Equal endpoint latitudes do not prove a monotone latitude interval.
    const auto a = spatial_test_wkb(geography_test_hex("POLYGON ((0 0,2e-14 0,0 1e-14,0 0))"));
    const auto b = spatial_test_wkb(
            geography_test_hex("POLYGON ((1e-15 0,2e-15 0,3e-15 0,4e-15 0,5e-15 0,6e-15 0,7e-15 0,"
                               "8e-15 0,9e-15 0,9e-15 -1e-14,1e-15 -1e-14,1e-15 0))"));
    EXPECT_EQ(geography_calculation_edges_for_test(a, b), 23);
    EXPECT_EQ(geography_calculation_edges_for_test(b, a), 23);
    // This tiny edge uses both sides of the raw longitude index at the date line.
    const auto cut = spatial_test_wkb(
            geography_test_hex("POLYGON ((179.99999999999997 0,-179.99999999999997 0,180 1e-14,"
                               "179.99999999999997 0))"));
    const auto point = spatial_test_wkb(geography_test_hex("POINT (180 0)"));
    EXPECT_EQ(geography_calculation_edges_for_test(cut, point), 5);
    EXPECT_EQ(geography_calculation_edges_for_test(point, cut), 5);
    // A turning arc must recall a point above its endpoint latitudes, even if
    // the final raw exact proof rejects that rounded midpoint as a true node.
    size_t proofs = 0;
    geography_calculation_edges_for_test(
            spatial_test_wkb(geography_test_hex("POLYGON ((0 45,90 45,45 40,0 45))")),
            spatial_test_wkb(geography_test_hex("POINT (45 54.735610317245346)")), &proofs);
    EXPECT_GT(proofs, 0);
    // Date-line and meridian routing retain their complete original intervals.
    expect_geography_relation("st_intersects", "POLYGON ((170 0,-170 0,180 10,170 0))",
                              "POINT (180 0)", true);
    expect_geography_relation("st_intersects", "POLYGON ((170 0,-170 0,180 10,170 0))",
                              "POINT (0 0)", false);
    expect_geography_relation("st_intersects", "POLYGON ((0 0,0 2,2 0,0 0))", "POINT (0 1)", true);
}

TEST(VGeoFunctionsTest, GeographyUncertainCircleDoesNotBroadenStableCircleSearches) {
    for (const int count : {32, 64, 128}) {
        std::ostringstream lines;
        // Lexicographic edge ordering puts the near-antipodal representative first.
        lines << std::setprecision(17) << "LINESTRING (-179.99999999999997 -10,0 10";
        for (int i = 0; i < count; ++i) {
            lines << "," << 45.0 + 0.1 * i << " " << 5.0 + 0.1 * std::sin(i);
        }
        lines << ")";
        const auto a = spatial_test_wkb(geography_test_hex(lines.str()));
        const auto b = spatial_test_wkb(geography_test_hex("POINT (20 20)"));
        size_t proofs = 0;
        geography_calculation_edges_for_test(a, b, &proofs);
        EXPECT_LE(proofs, 32 * count);
        RecordProperty("mixed_circle_proofs_" + std::to_string(count), std::to_string(proofs));
    }
}

TEST(VGeoFunctionsTest, GeographyDistinctNearAntipodalCirclesKeepCandidateWorkBounded) {
    for (bool padding : {false, true}) {
        for (const int count : {32, 64, 128}) {
            std::ostringstream lines;
            lines << std::setprecision(17) << "LINESTRING (0 0";
            for (int i = 1; i <= count; ++i) {
                lines << ",179.99999999999997 " << 1e-14 * i / (count + 1) << ",0 0";
            }
            // Unrelated meridian vertices must not disable skipping dense known
            // longitudes. Counting only exact proofs would miss repeated visits.
            if (padding) {
                for (int i = 1; i <= count + 2; ++i) {
                    lines << ",20 " << 30.0 + double(i) / (count + 3);
                }
            }
            lines << ")";
            const auto a = spatial_test_wkb(geography_test_hex(lines.str()));
            const auto b = spatial_test_wkb(geography_test_hex("POINT (20 20)"));
            size_t proofs = 0;
            size_t visits = 0;
            EXPECT_LE(geography_calculation_edges_for_test(a, b, &proofs, &visits), 12 * count);
            EXPECT_LE(proofs, 16 * count);
            EXPECT_LE(visits, 32 * count);
            const auto suffix = std::to_string(count) + (padding ? "_padded" : "");
            RecordProperty("uncertain_circle_proofs_" + suffix, std::to_string(proofs));
            RecordProperty("uncertain_circle_visits_" + suffix, std::to_string(visits));
        }
    }
}

static void expect_near_antipodal_proximity(const Strings& arguments) {
    const auto type = std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY);
    Block distance;
    ASSERT_TRUE(execute_geography_test("st_distance", arguments,
                                       make_nullable(std::make_shared<DataTypeFloat64>()),
                                       &distance)
                        .ok());
    const double metres = (*distance.get_by_position(2).column)[0].get<TYPE_DOUBLE>();
    EXPECT_GT(metres, 0);
    EXPECT_LT(metres, 0.112);
    // The raw great circle is tilted by 45 degrees at its equatorial midpoint.
    EXPECT_NEAR(metres, 0.0786268, 1e-6);
    Block within;
    for (const auto& hex : arguments) {
        const auto wkb = spatial_test_wkb(hex);
        auto column = type->create_column();
        column->insert_data(wkb.data(), wkb.size());
        within.insert({std::move(column), type, "shape"});
    }
    auto radius = ColumnFloat64::create();
    radius->insert_value(1);
    within.insert({std::move(radius), std::make_shared<DataTypeFloat64>(), "radius"});
    const auto result_type = make_nullable(std::make_shared<DataTypeUInt8>());
    const auto function = SimpleFunctionFactory::instance().get_function(
            "st_dwithin", within.get_columns_with_type_and_name(), result_type);
    ASSERT_NE(function, nullptr);
    within.insert({nullptr, result_type, "result"});
    ASSERT_TRUE(function->execute(nullptr, within, {0, 1, 2}, 3, 1).ok());
    EXPECT_TRUE((*within.get_by_position(3).column)[0].get<TYPE_BOOLEAN>());
}

TEST(VGeoFunctionsTest, GeographyNearAntipodalDistanceIsIndependentOfSubdivision) {
    const std::string raw = "LINESTRING (-44.99999999999997 -45,134.99999999999997 45)";
    const std::string divided = "LINESTRING (-44.99999999999997 -45,45 0,134.99999999999997 45)";
    for (const auto& line : {raw, divided}) {
        for (bool reverse : {false, true}) {
            const std::string point = "POINT (45 0.000001)";
            expect_near_antipodal_proximity({geography_test_hex(reverse ? point : line),
                                             geography_test_hex(reverse ? line : point)});
        }
    }
}

static void expect_distance_subdivision(const std::array<std::string, 3>& value, bool reverse) {
    const auto result_type = make_nullable(std::make_shared<DataTypeFloat64>());
    Block original;
    Block divided;
    ASSERT_TRUE(execute_geography_test("st_distance",
                                       {geography_test_hex(reverse ? value[2] : value[0]),
                                        geography_test_hex(reverse ? value[0] : value[2])},
                                       result_type, &original)
                        .ok());
    ASSERT_TRUE(execute_geography_test("st_distance",
                                       {geography_test_hex(reverse ? value[2] : value[1]),
                                        geography_test_hex(reverse ? value[1] : value[2])},
                                       result_type, &divided)
                        .ok());
    const auto actual = (*original.get_by_position(2).column)[0].get<TYPE_DOUBLE>();
    const auto expected = (*divided.get_by_position(2).column)[0].get<TYPE_DOUBLE>();
    EXPECT_GT(actual, 0);
    EXPECT_LT(actual, 0.112);
    EXPECT_NEAR(actual, expected, 1e-6);
}

TEST(VGeoFunctionsTest, GeographyStableArcDistancePreservesMeridiansAndDateLine) {
    const std::vector<std::array<std::string, 3>> cases {
            {"LINESTRING (10 -80,10 80)", "LINESTRING (10 -80,10 0,10 80)", "POINT (10.000001 0)"},
            {"LINESTRING (10 80,-170 -70)", "LINESTRING (10 80,0 90,-170 10,-170 -70)",
             "POINT (-169.999999 10)"},
            {"LINESTRING (90.00000000000003 -45,-90.00000000000003 45)",
             "LINESTRING (90.00000000000003 -45,180 0,-90.00000000000003 45)",
             "POINT (-180 0.000001)"},
            {"LINESTRING (-1e-300 -45,180 45)", "LINESTRING (-1e-300 -45,-90 0,180 45)",
             "POINT (-90 0.000001)"}};
    for (const auto& value : cases) {
        SCOPED_TRACE(value[0]);
        for (bool reverse : {false, true}) {
            expect_distance_subdivision(value, reverse);
        }
    }
}

TEST(VGeoFunctionsTest, GeographyCircleCoveragePreservesGapsWrapAndOpenBoundaries) {
    const std::string circle = "LINESTRING (-170 0,-60 0,60 0,170 0,-170 0)";
    const std::string rotated = "LINESTRING (0 0,120 0,-120 0,0 0)";
    expect_geography_relation("st_equals", circle, rotated, true);
    expect_geography_relation("st_equals", rotated, circle, true);
    expect_geography_relation("st_contains", circle, "LINESTRING (175 0,-175 0)", true);
    expect_geography_relation("st_touches", circle, "LINESTRING (0 -1,0 1)", false);
    const std::string gaps = "LINESTRING (-80 0,-70 0,-70 10,70 10,70 0,80 0)";
    expect_geography_relation("st_intersects", gaps, "LINESTRING (-1 0,1 0)", false);
    expect_geography_relation("st_contains", gaps, "LINESTRING (-1 0,1 0)", false);
    const std::string open = "LINESTRING (0 0,100 0,-50 0,50 0)";
    expect_geography_relation("st_touches", open, "LINESTRING (0 -1,0 1)", true);
    expect_geography_relation("st_touches", "LINESTRING (0 -1,0 1)", open, true);
    expect_geography_relation("st_touches", open, "LINESTRING (25 -1,25 1)", false);
    const std::string almost_antipodal =
            "LINESTRING (-44.99999999999997 -45,134.99999999999997 45,45 0,"
            "-44.99999999999997 -45)";
    expect_geography_relation("st_equals", almost_antipodal,
                              "LINESTRING (-44.99999999999997 -45,45 0,134.99999999999997 45)",
                              true);
    const std::string tilted = "LINESTRING (45 -45,90 0,135 45,90 0,45 -45)";
    expect_geography_relation("st_equals", tilted, "LINESTRING (45 -45,135 45)", true);
    expect_geography_relation("st_contains", tilted, "LINESTRING (45 -45,135 45)", true);
}

TEST(VGeoFunctionsTest, GeographyRawPointMembershipRetainsPolygonBoundaryAndLineEndpoints) {
    const std::string line = "LINESTRING (-44.99999999999997 -45,134.99999999999997 45)";
    const std::string polygon =
            "POLYGON ((-44.99999999999997 -45,134.99999999999997 45,0 -80,"
            "-44.99999999999997 -45))";
    const std::string midpoint = "POINT (45 0)";
    const std::string endpoint = "POINT (-44.99999999999997 -45)";
    for (bool reverse : {false, true}) {
        expect_geography_relation("st_intersects", reverse ? midpoint : polygon,
                                  reverse ? polygon : midpoint, true);
        expect_geography_relation("st_touches", reverse ? midpoint : polygon,
                                  reverse ? polygon : midpoint, true);
        expect_geography_relation("st_touches", reverse ? endpoint : line,
                                  reverse ? line : endpoint, true);
    }
    expect_geography_relation("st_contains", polygon, midpoint, false);
    expect_geography_relation("st_within", midpoint, polygon, false);
    expect_geography_relation("st_contains", line, endpoint, false);
    expect_geography_relation("st_within", endpoint, line, false);
}

TEST(VGeoFunctionsTest, GeographyDistinctOverlappingArcsHaveLinearExactProofWork) {
    for (const int count : {64, 128, 256}) {
        std::ostringstream retraced;
        std::ostringstream dense;
        retraced << std::setprecision(17) << "LINESTRING (";
        dense << std::setprecision(17) << "LINESTRING (";
        for (int i = 0; i <= count; ++i) {
            retraced << (i == 0 ? "" : ",") << (i % 2 == 0 ? -80.0 : 79.0) + double(i) / (count + 1)
                     << " 0";
            if (i > 0) {
                dense << (i == 1 ? "" : ",") << -1.0 + 2.0 * i / (count + 1) << " 0";
            }
        }
        retraced << ")";
        dense << ")";
        const auto a = spatial_test_wkb(geography_test_hex(retraced.str()));
        const auto b = spatial_test_wkb(geography_test_hex(dense.str()));
        size_t proofs = 0;
        EXPECT_LE(geography_calculation_edges_for_test(a, b, &proofs), 16 * count);
        // Bound actual expensive work, not just the final deduplicated node count.
        EXPECT_LE(proofs, 32 * count);
        RecordProperty("exact_proofs_" + std::to_string(count), std::to_string(proofs));
        expect_geography_relation("st_contains", retraced.str(), dense.str(), true);
    }
}

TEST(VGeoFunctionsTest, GeographyRetracedMeridiansHaveLinearCalculationStorage) {
    for (const int count : {128, 512, 2048}) {
        std::ostringstream retraced;
        std::ostringstream dense;
        retraced << "LINESTRING (10 0";
        dense << std::setprecision(17) << "LINESTRING (";
        for (int i = 1; i <= count; ++i) {
            retraced << ",10 " << (i % 2);
            dense << (i == 1 ? "" : ",") << "10 " << double(i) / (count + 1);
        }
        retraced << ")";
        dense << ")";
        const auto a = spatial_test_wkb(geography_test_hex(retraced.str()));
        const auto b = spatial_test_wkb(geography_test_hex(dense.str()));
        auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                        "GeographyNodingScratch");
        auto switch_tracker = SwitchThreadMemTrackerLimiter(tracker);
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        const auto baseline = tracker->consumption();
        // The storage budget grows with input size; the old N*M expansion violates it.
        EXPECT_LE(geography_calculation_edges_for_test(a, b), 8 * count);
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
        EXPECT_LT(tracker->peak_consumption() - baseline, 24 * 1024 * 1024);
        expect_geography_relation("st_intersects", retraced.str(), dense.str(), true);
        expect_geography_relation("st_contains", retraced.str(), dense.str(), true);
    }
}

static Status execute_dwithin_test(double threshold, bool null_threshold, Block* block) {
    const auto type = std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY);
    const auto wkb = spatial_test_wkb("0101000000000000000000f03f0000000000000040");
    for (int i = 0; i < 2; ++i) {
        auto column = type->create_column();
        column->insert_data(wkb.data(), wkb.size());
        block->insert({std::move(column), type, "shape"});
    }
    auto radius = ColumnFloat64::create();
    radius->insert_value(threshold);
    auto nulls = ColumnUInt8::create(1, null_threshold);
    block->insert({ColumnNullable::create(std::move(radius), std::move(nulls)),
                   make_nullable(std::make_shared<DataTypeFloat64>()), "radius"});
    const auto result = make_nullable(std::make_shared<DataTypeUInt8>());
    auto function = SimpleFunctionFactory::instance().get_function(
            "st_dwithin", block->get_columns_with_type_and_name(), result);
    EXPECT_NE(function, nullptr);
    block->insert({nullptr, result, "result"});
    return function->execute(nullptr, *block, {0, 1, 2}, 3, 1);
}

TEST(VGeoFunctionsTest, GeographyDWithinValidatesDynamicRadiusAndPropagatesNull) {
    for (double radius : {0.0, 1.0}) {
        Block block;
        ASSERT_TRUE(execute_dwithin_test(radius, false, &block).ok());
        EXPECT_EQ((*block.get_by_position(3).column)[0].get<TYPE_BOOLEAN>(), true);
    }
    for (double radius :
         {-1.0, std::numeric_limits<double>::infinity(), -std::numeric_limits<double>::infinity(),
          std::numeric_limits<double>::quiet_NaN()}) {
        Block block;
        const auto status = execute_dwithin_test(radius, false, &block);
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("finite and nonnegative"), std::string::npos);
        Block null_block;
        ASSERT_TRUE(execute_dwithin_test(radius, true, &null_block).ok());
        EXPECT_TRUE(null_block.get_by_position(3).column->is_null_at(0));
    }
}

static void expect_dwithin_constant_shapes(const std::vector<double>& radii, bool invalid,
                                           bool nullable_radius) {
    const auto type = std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY);
    Block block;
    for (const auto& wkt : {"POINT (0 0)", "POINT (1 0)"}) {
        const auto wkb = spatial_test_wkb(geography_test_hex(wkt));
        auto column = type->create_column();
        column->insert_data(wkb.data(), wkb.size());
        block.insert({ColumnConst::create(std::move(column), radii.size()), type, "shape"});
    }
    auto radius = ColumnFloat64::create();
    radius->get_data().assign(radii.begin(), radii.end());
    ColumnPtr radius_column = std::move(radius);
    DataTypePtr radius_type = std::make_shared<DataTypeFloat64>();
    if (nullable_radius) {
        auto nulls = ColumnUInt8::create(radii.size(), 0);
        nulls->get_data()[1] = 1;
        radius_column = ColumnNullable::create(radius_column, std::move(nulls));
        radius_type = make_nullable(radius_type);
    }
    block.insert({radius_column, radius_type, "radius"});
    const auto result_type = make_nullable(std::make_shared<DataTypeUInt8>());
    const auto function = SimpleFunctionFactory::instance().get_function(
            "st_dwithin", block.get_columns_with_type_and_name(), result_type);
    ASSERT_NE(function, nullptr);
    block.insert({nullptr, result_type, "result"});
    const auto status = function->execute(nullptr, block, {0, 1, 2}, 3, radii.size());
    if (invalid) {
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("finite and nonnegative"), std::string::npos);
        return;
    }
    ASSERT_TRUE(status.ok()) << status;
    const auto& result = *block.get_by_position(3).column;
    ASSERT_EQ(result.size(), radii.size());
    EXPECT_EQ(result[0].get<TYPE_BOOLEAN>(), false);
    EXPECT_EQ(result.is_null_at(1), nullable_radius);
    if (!nullable_radius) {
        EXPECT_EQ(result[1].get<TYPE_BOOLEAN>(), true);
    }
}

TEST(VGeoFunctionsTest, GeographyDWithinConstantShapesEvaluateEveryRadiusRow) {
    expect_dwithin_constant_shapes({0, 200000}, false, false);
    expect_dwithin_constant_shapes({0, -1}, true, false);
    expect_dwithin_constant_shapes({0, -1}, false, true);
}

static Status execute_dwithin_zero_test(const std::string& left, const std::string& right,
                                        Block* block) {
    const auto type = std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY);
    for (const auto& wkt : {left, right}) {
        const auto wkb = spatial_test_wkb(geography_test_hex(wkt));
        auto column = type->create_column();
        column->insert_data(wkb.data(), wkb.size());
        block->insert({std::move(column), type, "shape"});
    }
    const auto radius_type = std::make_shared<DataTypeFloat64>();
    block->insert({ColumnFloat64::create(1, 0), radius_type, "radius"});
    const auto result_type = make_nullable(std::make_shared<DataTypeUInt8>());
    const auto function = SimpleFunctionFactory::instance().get_function(
            "st_dwithin", block->get_columns_with_type_and_name(), result_type);
    EXPECT_NE(function, nullptr);
    block->insert({nullptr, result_type, "result"});
    return function->execute(nullptr, *block, {0, 1, 2}, 3, 1);
}

static void expect_dwithin_zero(const std::string& left, const std::string& right, bool expected) {
    Block block;
    ASSERT_TRUE(execute_dwithin_zero_test(left, right, &block).ok());
    EXPECT_EQ((*block.get_by_position(3).column)[0].get<TYPE_BOOLEAN>(), expected);
}

static void expect_rounded_point_shape_distance_error(const std::string& left,
                                                      const std::string& right) {
    SCOPED_TRACE(left + " versus " + right);
    Block distance;
    const auto status = execute_geography_test(
            "st_distance", {geography_test_hex(left), geography_test_hex(right)},
            make_nullable(std::make_shared<DataTypeFloat64>()), &distance);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("ambiguous spherical"), std::string::npos) << status;
    Block within;
    const auto within_status = execute_dwithin_zero_test(left, right, &within);
    EXPECT_FALSE(within_status.ok());
    EXPECT_NE(within_status.to_string().find("ambiguous spherical"), std::string::npos)
            << within_status;
}

TEST(VGeoFunctionsTest, GeographyPointShapeDistanceRejectsRoundedOnlyZero) {
    const std::vector<std::pair<std::string, std::string>> cases {
            {"POINT (58.00000000000001 0)", "LINESTRING (58 0,57 -1)"},
            {"POINT (0 58.00000000000001)", "LINESTRING (0 57,0 58)"},
            {"POINT (0 58)", "LINESTRING (0 58.00000000000001,180 -58)"},
            {"POINT (58.00000000000001 0)", "POLYGON ((58 0,57 -1,58 -1,58 0))"},
            {"POINT (6.999999999999999 10.25)", "POLYGON ((7 10,8 10,8 11,7 11,7 10))"}};
    // S2 reports zero for an endpoint gap or a raw exterior point rounded onto/inside a polygon.
    // Both distance APIs must reject that unsupported zero rather than turn it into contact.
    for (const auto& [point, shape] : cases) {
        expect_rounded_point_shape_distance_error(point, shape);
        expect_rounded_point_shape_distance_error(shape, point);
    }
}

static void expect_point_shape_zero_distance(const std::string& left, const std::string& right) {
    SCOPED_TRACE(left + " versus " + right);
    double distance = -1;
    geography_measurement("st_distance", {left, right}, &distance);
    EXPECT_EQ(distance, 0);
    expect_dwithin_zero(left, right, true);
}

TEST(VGeoFunctionsTest, GeographyPointShapeZeroDistanceKeepsRawContactAndInteriors) {
    const std::string hole = "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1))";
    const std::vector<std::pair<std::string, std::string>> cases {
            {"POINT (58 0)", "LINESTRING (58 0,57 -1)"},
            {"POINT (58.00000000000001 0)", "LINESTRING (58 0,59 0)"},
            {"POINT (0 58.00000000000001)", "LINESTRING (0 58,0 59)"},
            {"POINT (58 0)", "POLYGON ((58 0,57 -1,58 -1,58 0))"},
            {"POINT (58.00000000000001 0)", "POLYGON ((58 0,59 0,59 1,58 1,58 0))"},
            {"POINT (58.00000000000001 0)", "POLYGON ((57 -1,59 -1,59 1,57 1,57 -1))"},
            {"POINT (0.5 2)", hole},
            {"POINT (1 2)", hole},
            {"POINT (-180 0)", "LINESTRING (180 0,179 0)"},
            {"POINT (123 90)", "LINESTRING (0 89,0 90)"}};
    for (const auto& [point, shape] : cases) {
        expect_point_shape_zero_distance(point, shape);
        expect_point_shape_zero_distance(shape, point);
    }
}

static void expect_point_shape_positive_distance(const std::string& left,
                                                 const std::string& right) {
    SCOPED_TRACE(left + " versus " + right);
    double distance = -1;
    geography_measurement("st_distance", {left, right}, &distance);
    EXPECT_GT(distance, 0);
    expect_dwithin_zero(left, right, false);
}

TEST(VGeoFunctionsTest, GeographyPointShapePositiveDistanceKeepsSeparatedPointsAndHoles) {
    const std::vector<std::pair<std::string, std::string>> cases {
            {"POINT (58.1 0)", "LINESTRING (58 0,57 -1)"},
            {"POINT (58.1 0)", "POLYGON ((58 0,57 -1,58 -1,58 0))"},
            {"POINT (2 2)", "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1))"}};
    for (const auto& [point, shape] : cases) {
        expect_point_shape_positive_distance(point, shape);
        expect_point_shape_positive_distance(shape, point);
    }
}

TEST(VGeoFunctionsTest, GeographyDWithinHasNoLegacyMetreClamp) {
    const Strings shapes {"POINT (1 0)", "LINESTRING (0 0,2 0)", "POLYGON ((0 0,2 0,2 2,0 2,0 0))"};
    for (const auto& left : shapes) {
        for (const auto& right : shapes) {
            expect_dwithin_zero(left, right, true);
        }
    }
    expect_dwithin_zero("POINT (180 0)", "POINT (-180 0)", true);
    expect_dwithin_zero("POINT (0 90)", "POINT (20 90)", true);
    expect_dwithin_zero("LINESTRING (10 45,10 46)", "LINESTRING (10 45.3,10 45.9)", true);
    expect_dwithin_zero("LINESTRING (0 0,1 0)", "LINESTRING (0 0.000000000001,1 0.000000000001)",
                        false);
}

TEST(VGeoFunctionsTest, GeographyPointDistancePreservesOriginalOrdinates) {
    struct Case {
        std::string left;
        std::string right;
        double degrees;
    };
    const double ulp = std::nextafter(58.0, 59.0) - 58.0;
    const std::vector<Case> cases {
            {"POINT (58 0)", "POINT (58.00000000000001 0)", ulp},
            {"POINT (0 58)", "POINT (0 58.00000000000001)", ulp},
            {"POINT (179.99999999999997 0)", "POINT (-180 0)", 180.0 - std::nextafter(180.0, 0.0)},
            {"POINT (0 0)", "POINT (1e-300 0)", 1e-300},
            {"POINT (10 45)", "POINT (10 46)", 1},
            {"POINT (0 0)", "POINT (180 0)", 180},
            {"POINT (180 0)", "POINT (-180 0)", 0},
            {"POINT (0 90)", "POINT (123 90)", 0}};
    for (const auto& value : cases) {
        SCOPED_TRACE(value.left + ", " + value.right);
        // Equator and meridian arcs have the independent closed form radius * angle.
        const double expected = value.degrees * (M_PI / 180) * S2Earth::RadiusMeters();
        for (bool reverse : {false, true}) {
            const auto& left = reverse ? value.right : value.left;
            const auto& right = reverse ? value.left : value.right;
            double distance = -1;
            geography_measurement("st_distance", {left, right}, &distance);
            EXPECT_NEAR(distance, expected, expected * 1e-12);
            expect_dwithin_zero(left, right, value.degrees == 0);
        }
    }
}

TEST(VGeoFunctionsTest, GeographyPointRelationsDistinguishInteriorAndBoundary) {
    struct Case {
        std::string name;
        std::string left;
        std::string right;
        bool expected;
    };
    const std::vector<Case> cases {
            {"st_contains",
             "01020000000300000000000000000000000000000000000000000000000000f03f0000000000000000000"
             "00000000000400000000000000000",
             "0101000000000000000000f83f0000000000000000", true},
            {"st_within", "0101000000000000000000f83f0000000000000000",
             "01020000000300000000000000000000000000000000000000000000000000f03f0000000000000000000"
             "00000000000400000000000000000",
             true},
            {"st_touches",
             "01020000000300000000000000000000000000000000000000000000000000f03f0000000000000000000"
             "00000000000400000000000000000",
             "0101000000000000000000f83f0000000000000000", false},
            {"st_contains",
             "01020000000300000000000000000000000000000000000000000000000000f03f0000000000000000000"
             "00000000000400000000000000000",
             "010100000000000000000000000000000000000000", false},
            {"st_within", "010100000000000000000000000000000000000000",
             "01020000000300000000000000000000000000000000000000000000000000f03f0000000000000000000"
             "00000000000400000000000000000",
             false},
            {"st_touches", "010100000000000000000000000000000000000000",
             "01020000000300000000000000000000000000000000000000000000000000f03f0000000000000000000"
             "00000000000400000000000000000",
             true},
            {"st_within", "010100000000000000000000000000000000000000",
             "01020000000400000000000000000000000000000000000000000000000000f03f0000000000000000000"
             "000000000f03f000000000000f03f00000000000000000000000000000000",
             true},
            {"st_touches",
             "01020000000400000000000000000000000000000000000000000000000000f03f0000000000000000000"
             "000000000f03f000000000000f03f00000000000000000000000000000000",
             "010100000000000000000000000000000000000000", false},
            {"st_contains",
             "0103000000010000000500000000000000000000c00000000000000000000000000000004000000000000"
             "000000000000000000040000000000000004000000000000000c0000000000000004000000000000000c0"
             "0000000000000000",
             "010100000000000000000000000000000000000000", false},
            {"st_touches",
             "0103000000010000000500000000000000000000c00000000000000000000000000000004000000000000"
             "000000000000000000040000000000000004000000000000000c0000000000000004000000000000000c0"
             "0000000000000000",
             "010100000000000000000000000000000000000000", true},
            {"st_contains",
             "0103000000010000000500000000000000000000c00000000000000000000000000000004000000000000"
             "000000000000000000040000000000000004000000000000000c0000000000000004000000000000000c0"
             "0000000000000000",
             "01010000000000000000000000000000000000f03f", true},
            {"st_touches", "01010000000000000000000000000000000000f03f",
             "0103000000010000000500000000000000000000c00000000000000000000000000000004000000000000"
             "000000000000000000040000000000000004000000000000000c0000000000000004000000000000000c0"
             "0000000000000000",
             false},
            {"st_contains", "0101000000000000000080664080da314a7200eabf",
             "010100000000000000008066c080da314a7200eabf", true},
            {"st_within", "010100000000000000008066c080da314a7200eabf",
             "0101000000000000000080664080da314a7200eabf", true},
            {"st_intersects", "0101000000000000000080664080da314a7200eabf",
             "010100000000000000008066c080da314a7200eabf", true}};
    const auto result_type = make_nullable(std::make_shared<DataTypeUInt8>());
    for (const auto& value : cases) {
        SCOPED_TRACE(value.name + " " + value.left + " versus " + value.right);
        Block block;
        ASSERT_TRUE(
                execute_geography_test(value.name, {value.left, value.right}, result_type, &block)
                        .ok());
        EXPECT_EQ((*block.get_by_position(2).column)[0].get<TYPE_BOOLEAN>(), value.expected);
    }
}

TEST(VGeoFunctionsTest, GeographyEmptyPolygonsFailDistanceCalculations) {
    const std::string empty = "010300000000000000";
    const std::string point = "010100000000000000000000000000000000000000";
    const auto result_type = make_nullable(std::make_shared<DataTypeFloat64>());
    for (const auto& pair : std::vector<std::pair<std::string, std::string>> {
                 {empty, point}, {point, empty}, {empty, empty}}) {
        Block block;
        const auto status = execute_geography_test("st_distance", {pair.first, pair.second},
                                                   result_type, &block);
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("does not support this Geography WKB shape"),
                  std::string::npos);
    }
}

TEST(VGeoFunctionsTest, GeographyBearingRejectsExactAntipodes) {
    const auto result_type = make_nullable(std::make_shared<DataTypeFloat64>());
    const std::vector<std::pair<std::string, std::string>> antipodes {
            {"010100000000000000000000000000000000000000",
             "010100000000000000008066400000000000000000"},
            {"010100000000000000000000000000000000002440",
             "0101000000000000000080664000000000000024c0"},
            {"01010000000000000000003e400000000000804640",
             "01010000000000000000c062c000000000008046c0"},
            {"010100000000000000004065400000000000003ec0",
             "010100000000000000000024c00000000000003e40"},
            {"010100000000000000008046400000000000805640",
             "0101000000000000000080564000000000008056c0"},
    };
    for (const auto& pair : antipodes) {
        for (const auto& inputs : std::vector<std::vector<std::string>> {
                     {pair.first, pair.second},
                     {pair.second, pair.first},
                     {"010100000000000000000024400000000000003440", pair.first, pair.second},
                     {pair.second, pair.first, "010100000000000000000024400000000000003440"}}) {
            Block block;
            ASSERT_TRUE(execute_geography_test(inputs.size() == 2 ? "st_azimuth" : "st_angle",
                                               inputs, result_type, &block)
                                .ok());
            EXPECT_TRUE(block.get_by_position(inputs.size()).column->is_null_at(0));
        }
    }
}

TEST(VGeoFunctionsTest, GeographyBearingPreservesNearAntipodes) {
    const auto result_type = make_nullable(std::make_shared<DataTypeFloat64>());
    const std::vector<std::pair<std::string, std::string>> near_antipodes {
            {"010100000000000000000000000000000000000000",
             "01010000000c21e7fdff7f66400000000000000000"},
            {"010100000000000000000000000000000000002440",
             "01010000000c21e7fdff7f664000000000000024c0"},
    };
    for (const auto& pair : near_antipodes) {
        Block azimuth;
        ASSERT_TRUE(execute_geography_test("st_azimuth", {pair.first, pair.second}, result_type,
                                           &azimuth)
                            .ok());
        ASSERT_FALSE(azimuth.get_by_position(2).column->is_null_at(0));
        EXPECT_NEAR((*azimuth.get_by_position(2).column)[0].get<TYPE_DOUBLE>(), M_PI_2, 1e-7);
        Block angle;
        ASSERT_TRUE(execute_geography_test("st_angle", {pair.second, pair.first, pair.second},
                                           result_type, &angle)
                            .ok());
        ASSERT_FALSE(angle.get_by_position(3).column->is_null_at(0));
        EXPECT_NEAR((*angle.get_by_position(3).column)[0].get<TYPE_DOUBLE>(), 0, 1e-12);
    }
}

TEST(VGeoFunctionsTest, GeographyBearingUsesLatitudeLongitudeAndClockwiseConvention) {
    struct Case {
        std::string start;
        std::string end;
        double bearing;
    };
    const std::vector<Case> cases {
            {"010100000000000000000024400000000000804640",
             "010100000000000000000024400000000000004740", 0},
            {"010100000000000000000024400000000000804640",
             "010100000000000000000024400000000000004640", M_PI},
            {"010100000000000000000000000000000000000000",
             "0101000000000000000000f03f0000000000000000", 1.5707963267948966},
            {"010100000000000000000000000000000000000000",
             "0101000000000000000000f0bf0000000000000000", 4.71238898038469}};
    const auto result_type = make_nullable(std::make_shared<DataTypeFloat64>());
    for (const auto& value : cases) {
        Block block;
        ASSERT_TRUE(
                execute_geography_test("st_azimuth", {value.start, value.end}, result_type, &block)
                        .ok());
        EXPECT_NEAR((*block.get_by_position(2).column)[0].get<TYPE_DOUBLE>(), value.bearing, 1e-12);
    }
    Block angle;
    ASSERT_TRUE(execute_geography_test("st_angle",
                                       {"010100000000000000000024400000000000004740",
                                        "010100000000000000000024400000000000804640",
                                        "010100000000000000000024400000000000004640"},
                                       result_type, &angle)
                        .ok());
    EXPECT_NEAR((*angle.get_by_position(3).column)[0].get<TYPE_DOUBLE>(), M_PI, 1e-12);
    Block clockwise;
    ASSERT_TRUE(execute_geography_test("st_angle",
                                       {"010100000000000000000024400000000000004740",
                                        "010100000000000000000024400000000000804640",
                                        "010100000000000000000026400000000000804640"},
                                       result_type, &clockwise)
                        .ok());
    EXPECT_NEAR((*clockwise.get_by_position(3).column)[0].get<TYPE_DOUBLE>(), 1.564625577726284,
                1e-12);
    for (const auto& end : {"010100000000000000000024400000000000804640",
                            "010100000000000000004065c000000000008046c0"}) {
        Block undefined;
        ASSERT_TRUE(execute_geography_test("st_azimuth",
                                           {"010100000000000000000024400000000000804640", end},
                                           result_type, &undefined)
                            .ok());
        EXPECT_TRUE(undefined.get_by_position(2).column->is_null_at(0));
    }
}

static void expect_typed_binary_arrow_result(PrimitiveType primitive) {
    const auto wkb = spatial_test_wkb("0101000000000000000000f03f0000000000000040");
    const auto input_type = make_nullable(std::make_shared<DataTypeSpatial>(primitive));
    auto input = input_type->create_column();
    input->insert_data(wkb.data(), wkb.size());
    input->insert_default();
    ColumnsWithTypeAndName arguments {{std::move(input), input_type, "shape"}};
    const auto result_type = make_nullable(std::make_shared<DataTypeVarbinary>());
    auto function =
            SimpleFunctionFactory::instance().get_function("st_asbinary", arguments, result_type);
    ASSERT_NE(function, nullptr);
    EXPECT_TRUE(function->get_return_type()->equals(*result_type));
    Block block;
    block.insert(arguments.front());
    block.insert({nullptr, result_type, "wkb"});
    ASSERT_TRUE(function->execute(nullptr, block, {0}, 1, 2).ok());
    Block output;
    output.insert(block.get_by_position(1));
    std::shared_ptr<arrow::Schema> schema;
    ASSERT_TRUE(
            ArrowFlightSchemaConvertor("UTC").get_arrow_schema_from_block(output, &schema).ok());
    ASSERT_EQ(schema->field(0)->type()->id(), arrow::Type::BINARY);
    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_TRUE(ArrowFlightArrowBlockConvertor(schema, cctz::utc_time_zone())
                        .convert_to_arrow(output, arrow::default_memory_pool(), &batch)
                        .ok());
    ASSERT_TRUE(batch->ValidateFull().ok());
    const auto& binary = static_cast<const arrow::BinaryArray&>(*batch->column(0));
    EXPECT_EQ(binary.GetView(0), wkb);
    EXPECT_TRUE(binary.IsNull(1));
}

TEST(VGeoFunctionsTest, TypedAsBinaryProducesBinaryArrowWkbAndNulls) {
    expect_typed_binary_arrow_result(TYPE_GEOMETRY);
    expect_typed_binary_arrow_result(TYPE_GEOGRAPHY);
}

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

TEST(VGeoFunctionsTest, function_geo_st_as_text_with_spatial_wkb) {
    const std::string wkb(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@", 21);
    for (const auto primitive_type : {TYPE_GEOMETRY, TYPE_GEOGRAPHY}) {
        auto spatial_type = primitive_type == TYPE_GEOMETRY
                                    ? std::make_shared<DataTypeSpatial>(TYPE_GEOMETRY)
                                    : std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY, "OGC:CRS84",
                                                                        "spherical");
        auto spatial_column = ColumnSpatial::create(primitive_type);
        spatial_column->insert_data(wkb.data(), wkb.size());

        ColumnsWithTypeAndName arguments {{std::move(spatial_column), spatial_type, "spatial"}};
        auto result_type = make_nullable(std::make_shared<DataTypeString>());
        auto function =
                SimpleFunctionFactory::instance().get_function("st_astext", arguments, result_type);
        ASSERT_NE(nullptr, function);

        Block block;
        block.insert(arguments.front());
        block.insert({nullptr, result_type, "result"});
        ASSERT_TRUE(function->execute(nullptr, block, {0}, 1, 1).ok());

        const auto value = block.get_by_position(1).column->get_data_at(0);
        EXPECT_EQ("POINT (1 2)", std::string(value.data, value.size));
    }
}

TEST(VGeoFunctionsTest, function_geo_typed_wkb_constructor_returns_raw_geometry_wkb) {
    const std::string wkb(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@", 21);
    constexpr auto function_name = "st_geometryfromwkbtyped";
    auto input_column = ColumnString::create();
    input_column->insert_data("0101000000000000000000F03F0000000000000040", 42);
    auto input_type = std::make_shared<DataTypeString>();
    ColumnsWithTypeAndName arguments {{std::move(input_column), input_type, "wkb"}};
    auto result_type = make_nullable(std::make_shared<DataTypeSpatial>(TYPE_GEOMETRY));
    auto function =
            SimpleFunctionFactory::instance().get_function(function_name, arguments, result_type);
    ASSERT_NE(nullptr, function);

    Block block;
    block.insert(arguments.front());
    block.insert({nullptr, result_type, "result"});
    ASSERT_TRUE(function->execute(nullptr, block, {0}, 1, 1).ok());

    const auto value = block.get_by_position(1).column->get_data_at(0);
    EXPECT_EQ(wkb, std::string(value.data, value.size));
}

TEST(VGeoFunctionsTest, function_geo_st_geogfromwkb_returns_raw_geography_wkb) {
    const std::string wkb(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@", 21);
    auto input_column = ColumnString::create();
    input_column->insert_data("0101000000000000000000F03F0000000000000040", 42);
    auto input_type = std::make_shared<DataTypeString>();
    ColumnsWithTypeAndName arguments {{std::move(input_column), input_type, "wkb"}};
    auto result_type = make_nullable(std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY));
    auto function = SimpleFunctionFactory::instance().get_function("st_geogfromwkb", arguments,
                                                                   result_type);
    ASSERT_NE(nullptr, function);

    Block block;
    block.insert(arguments.front());
    block.insert({nullptr, result_type, "result"});
    ASSERT_TRUE(function->execute(nullptr, block, {0}, 1, 1).ok());

    EXPECT_EQ(TYPE_GEOGRAPHY, remove_nullable(block.get_data_type(1))->get_primitive_type());
    const auto value = block.get_by_position(1).column->get_data_at(0);
    EXPECT_EQ(wkb, std::string(value.data, value.size));
}

TEST(VGeoFunctionsTest, function_geo_st_distance_rejects_geometry) {
    const std::string wkb(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@", 21);
    auto geometry_type = std::make_shared<DataTypeSpatial>(TYPE_GEOMETRY);
    auto left_column = ColumnSpatial::create(TYPE_GEOMETRY);
    auto right_column = ColumnSpatial::create(TYPE_GEOMETRY);
    left_column->insert_data(wkb.data(), wkb.size());
    right_column->insert_data(wkb.data(), wkb.size());

    ColumnsWithTypeAndName arguments {{std::move(left_column), geometry_type, "left"},
                                      {std::move(right_column), geometry_type, "right"}};
    auto result_type = make_nullable(std::make_shared<DataTypeFloat64>());
    auto function =
            SimpleFunctionFactory::instance().get_function("st_distance", arguments, result_type);
    ASSERT_NE(nullptr, function);

    Block block;
    block.insert(arguments[0]);
    block.insert(arguments[1]);
    block.insert({nullptr, result_type, "result"});
    const auto status = function->execute(nullptr, block, {0, 1}, 2, 1);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("GEOGRAPHY(OGC:CRS84, spherical)"), std::string::npos)
            << status.to_string();
}

TEST(VGeoFunctionsTest, function_geo_st_distance_accepts_supported_geography) {
    const std::string wkb(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@", 21);
    auto geography_type =
            std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY, "ogc:crs84", "spherical");
    auto left_column = ColumnSpatial::create(TYPE_GEOGRAPHY);
    auto right_column = ColumnSpatial::create(TYPE_GEOGRAPHY);
    left_column->insert_data(wkb.data(), wkb.size());
    right_column->insert_data(wkb.data(), wkb.size());

    ColumnsWithTypeAndName arguments {{std::move(left_column), geography_type, "left"},
                                      {std::move(right_column), geography_type, "right"}};
    auto result_type = make_nullable(std::make_shared<DataTypeFloat64>());
    auto function =
            SimpleFunctionFactory::instance().get_function("st_distance", arguments, result_type);
    ASSERT_NE(nullptr, function);

    Block block;
    block.insert(arguments[0]);
    block.insert(arguments[1]);
    block.insert({nullptr, result_type, "result"});
    EXPECT_TRUE(function->execute(nullptr, block, {0, 1}, 2, 1).ok());
}

TEST(VGeoFunctionsTest, function_geo_st_distance_rejects_unsupported_geography_metadata) {
    const std::string wkb(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@", 21);
    for (const auto& geography_type : std::vector<DataTypePtr> {
                 std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY, "EPSG:4326", "spherical"),
                 std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY, "OGC:CRS84", "vincenty")}) {
        auto left_column = ColumnSpatial::create(TYPE_GEOGRAPHY);
        auto right_column = ColumnSpatial::create(TYPE_GEOGRAPHY);
        left_column->insert_data(wkb.data(), wkb.size());
        right_column->insert_data(wkb.data(), wkb.size());

        ColumnsWithTypeAndName arguments {{std::move(left_column), geography_type, "left"},
                                          {std::move(right_column), geography_type, "right"}};
        auto result_type = make_nullable(std::make_shared<DataTypeFloat64>());
        auto function = SimpleFunctionFactory::instance().get_function("st_distance", arguments,
                                                                       result_type);
        ASSERT_NE(nullptr, function);

        Block block;
        block.insert(arguments[0]);
        block.insert(arguments[1]);
        block.insert({nullptr, result_type, "result"});
        const auto status = function->execute(nullptr, block, {0, 1}, 2, 1);
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("GEOGRAPHY(OGC:CRS84, spherical)"), std::string::npos)
                << status.to_string();
    }
}

TEST(VGeoFunctionsTest, function_geo_st_contains_rejects_geometry) {
    const std::string wkb(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@", 21);
    auto geometry_type = std::make_shared<DataTypeSpatial>(TYPE_GEOMETRY);
    auto left_column = ColumnSpatial::create(TYPE_GEOMETRY);
    auto right_column = ColumnSpatial::create(TYPE_GEOMETRY);
    left_column->insert_data(wkb.data(), wkb.size());
    right_column->insert_data(wkb.data(), wkb.size());

    ColumnsWithTypeAndName arguments {{std::move(left_column), geometry_type, "left"},
                                      {std::move(right_column), geometry_type, "right"}};
    auto result_type = make_nullable(std::make_shared<DataTypeUInt8>());
    auto function =
            SimpleFunctionFactory::instance().get_function("st_contains", arguments, result_type);
    ASSERT_NE(nullptr, function);

    Block block;
    block.insert(arguments[0]);
    block.insert(arguments[1]);
    block.insert({nullptr, result_type, "result"});
    const auto status = function->execute(nullptr, block, {0, 1}, 2, 1);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("GEOGRAPHY(OGC:CRS84, spherical)"), std::string::npos)
            << status.to_string();
}

TEST(VGeoFunctionsTest, function_geo_st_astext_rejects_invalid_spatial_wkb) {
    auto geometry_type = std::make_shared<DataTypeSpatial>(TYPE_GEOMETRY);
    auto geometry_column = ColumnSpatial::create(TYPE_GEOMETRY);
    geometry_column->insert_data("\x01", 1);

    ColumnsWithTypeAndName arguments {{std::move(geometry_column), geometry_type, "geometry"}};
    auto result_type = make_nullable(std::make_shared<DataTypeString>());
    auto function =
            SimpleFunctionFactory::instance().get_function("st_astext", arguments, result_type);
    ASSERT_NE(nullptr, function);

    Block block;
    block.insert(arguments.front());
    block.insert({nullptr, result_type, "result"});
    const auto status = function->execute(nullptr, block, {0}, 1, 1);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Invalid WKB in spatial input"), std::string::npos)
            << status.to_string();
    EXPECT_NE(status.to_string().find("WKB syntax error"), std::string::npos) << status.to_string();
}

TEST(VGeoFunctionsTest, function_geo_st_astext_rejects_spatial_wkb_with_z) {
    const std::string point_z_wkb(
            "\x01\x01\x00\x00\x80"
            "\x00\x00\x00\x00\x00\x00\xf0?"
            "\x00\x00\x00\x00\x00\x00\x00@"
            "\x00\x00\x00\x00\x00\x00\x08@",
            29);
    auto geometry_type = std::make_shared<DataTypeSpatial>(TYPE_GEOMETRY);
    auto geometry_column = ColumnSpatial::create(TYPE_GEOMETRY);
    geometry_column->insert_data(point_z_wkb.data(), point_z_wkb.size());

    ColumnsWithTypeAndName arguments {{std::move(geometry_column), geometry_type, "geometry"}};
    auto result_type = make_nullable(std::make_shared<DataTypeString>());
    auto function =
            SimpleFunctionFactory::instance().get_function("st_astext", arguments, result_type);
    ASSERT_NE(nullptr, function);

    Block block;
    block.insert(arguments.front());
    block.insert({nullptr, result_type, "result"});
    const auto status = function->execute(nullptr, block, {0}, 1, 1);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("WKB dimensions or embedded SRID are not supported"),
              std::string::npos)
            << status.to_string();
}

TEST(VGeoFunctionsTest, function_geo_st_astext_rejects_spatial_wkb_with_m_or_srid) {
    const std::vector<std::string> unsupported_wkb {
            std::string("\x01\x01\x00\x00\x40"
                        "\x00\x00\x00\x00\x00\x00\xf0?"
                        "\x00\x00\x00\x00\x00\x00\x00@"
                        "\x00\x00\x00\x00\x00\x00\x08@",
                        29),
            std::string("\x01\x01\x00\x00\x20\xe6\x10\x00\x00"
                        "\x00\x00\x00\x00\x00\x00\xf0?"
                        "\x00\x00\x00\x00\x00\x00\x00@",
                        25)};

    for (const auto& wkb : unsupported_wkb) {
        auto geometry_type = std::make_shared<DataTypeSpatial>(TYPE_GEOMETRY);
        auto geometry_column = ColumnSpatial::create(TYPE_GEOMETRY);
        geometry_column->insert_data(wkb.data(), wkb.size());

        ColumnsWithTypeAndName arguments {{std::move(geometry_column), geometry_type, "geometry"}};
        auto result_type = make_nullable(std::make_shared<DataTypeString>());
        auto function =
                SimpleFunctionFactory::instance().get_function("st_astext", arguments, result_type);
        ASSERT_NE(nullptr, function);

        Block block;
        block.insert(arguments.front());
        block.insert({nullptr, result_type, "result"});
        const auto status = function->execute(nullptr, block, {0}, 1, 1);
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("WKB dimensions or embedded SRID are not supported"),
                  std::string::npos)
                << status.to_string();
    }
}

TEST(VGeoFunctionsTest, function_geo_point_accessors_with_spatial_wkb) {
    const std::string wkb(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@", 21);
    for (const auto& [function_name, expected] :
         std::vector<std::pair<std::string, double>> {{"st_x", 1.0}, {"st_y", 2.0}}) {
        auto geometry_type = std::make_shared<DataTypeSpatial>(TYPE_GEOMETRY);
        auto geometry_column = ColumnSpatial::create(TYPE_GEOMETRY);
        geometry_column->insert_data(wkb.data(), wkb.size());

        ColumnsWithTypeAndName arguments {{std::move(geometry_column), geometry_type, "geometry"}};
        auto result_type = make_nullable(std::make_shared<DataTypeFloat64>());
        auto function = SimpleFunctionFactory::instance().get_function(function_name, arguments,
                                                                       result_type);
        ASSERT_NE(nullptr, function);

        Block block;
        block.insert(arguments.front());
        block.insert({nullptr, result_type, "result"});
        ASSERT_TRUE(function->execute(nullptr, block, {0}, 1, 1).ok());

        const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        EXPECT_EQ(0, result.get_null_map_data()[0]);
        const auto& values = assert_cast<const ColumnFloat64&>(result.get_nested_column());
        EXPECT_DOUBLE_EQ(expected, values.get_data()[0]);
    }
}

TEST(VGeoFunctionsTest, function_geo_projected_geometry_accessors) {
    const std::string wkb(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00@\x8f@\x00\x00\x00\x00\x00@\x9f@", 21);
    const auto geometry_type = std::make_shared<DataTypeSpatial>(TYPE_GEOMETRY, "EPSG:3857");
    for (const auto& [function_name, expected] :
         std::vector<std::pair<std::string, std::string>> {{"st_astext", "POINT (1000 2000)"},
                                                           {"st_geometrytype", "ST_POINT"},
                                                           {"st_asbinary", wkb}}) {
        auto geometry_column = ColumnSpatial::create(TYPE_GEOMETRY);
        geometry_column->insert_data(wkb.data(), wkb.size());
        ColumnsWithTypeAndName arguments {{std::move(geometry_column), geometry_type, "geometry"}};
        auto result_type = make_nullable(
                function_name == "st_asbinary" ? DataTypePtr(std::make_shared<DataTypeVarbinary>())
                                               : DataTypePtr(std::make_shared<DataTypeString>()));
        auto function = SimpleFunctionFactory::instance().get_function(function_name, arguments,
                                                                       result_type);
        ASSERT_NE(nullptr, function);

        Block block;
        block.insert(arguments.front());
        block.insert({nullptr, result_type, "result"});
        ASSERT_TRUE(function->execute(nullptr, block, {0}, 1, 1).ok());
        const auto value = block.get_by_position(1).column->get_data_at(0);
        EXPECT_EQ(expected, std::string(value.data, value.size));
    }

    for (const auto& [function_name, expected] :
         std::vector<std::pair<std::string, double>> {{"st_x", 1000.0}, {"st_y", 2000.0}}) {
        auto geometry_column = ColumnSpatial::create(TYPE_GEOMETRY);
        geometry_column->insert_data(wkb.data(), wkb.size());
        ColumnsWithTypeAndName arguments {{std::move(geometry_column), geometry_type, "geometry"}};
        auto result_type = make_nullable(std::make_shared<DataTypeFloat64>());
        auto function = SimpleFunctionFactory::instance().get_function(function_name, arguments,
                                                                       result_type);
        ASSERT_NE(nullptr, function);

        Block block;
        block.insert(arguments.front());
        block.insert({nullptr, result_type, "result"});
        ASSERT_TRUE(function->execute(nullptr, block, {0}, 1, 1).ok());
        const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        EXPECT_EQ(0, result.get_null_map_data()[0]);
        EXPECT_DOUBLE_EQ(
                expected,
                assert_cast<const ColumnFloat64&>(result.get_nested_column()).get_data()[0]);
    }
}

TEST(VGeoFunctionsTest, GeographyRawAccessorsPreserveBigEndianWkb) {
    const std::string wkb(
            "\x00\x00\x00\x00\x01\x40\x66\x80\x00\x00\x00\x00\x00"
            "\x40\x56\x80\x00\x00\x00\x00\x00",
            21);
    const auto type = std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY, "EPSG:4326", "vincenty");
    for (const auto& [name, expected] :
         std::vector<std::pair<std::string, std::string>> {{"st_asbinary", wkb},
                                                           {"st_astext", "POINT (180 90)"},
                                                           {"st_geometrytype", "ST_POINT"}}) {
        auto column = type->create_column();
        column->insert_data(wkb.data(), wkb.size());
        ColumnsWithTypeAndName arguments {{std::move(column), type, "geography"}};
        const auto result_type = make_nullable(
                name == "st_asbinary" ? DataTypePtr(std::make_shared<DataTypeVarbinary>())
                                      : DataTypePtr(std::make_shared<DataTypeString>()));
        auto function =
                SimpleFunctionFactory::instance().get_function(name, arguments, result_type);
        ASSERT_NE(function, nullptr);
        Block block;
        block.insert(arguments.front());
        block.insert({nullptr, result_type, "result"});
        const auto status = function->execute(nullptr, block, {0}, 1, 1);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_FALSE(block.get_by_position(1).column->is_null_at(0));
        EXPECT_EQ(block.get_by_position(1).column->get_data_at(0).to_string(), expected);
    }
}

TEST(VGeoFunctionsTest, GeographyRawPointCoordinatesPreservePoleLongitude) {
    const std::string wkb(
            "\x00\x00\x00\x00\x01\x40\x66\x80\x00\x00\x00\x00\x00"
            "\x40\x56\x80\x00\x00\x00\x00\x00",
            21);
    const auto type = std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY, "EPSG:4326", "vincenty");
    for (const auto& [name, expected] :
         std::vector<std::pair<std::string, double>> {{"st_x", 180}, {"st_y", 90}}) {
        auto column = type->create_column();
        column->insert_data(wkb.data(), wkb.size());
        ColumnsWithTypeAndName arguments {{std::move(column), type, "geography"}};
        const auto result_type = make_nullable(std::make_shared<DataTypeFloat64>());
        auto function =
                SimpleFunctionFactory::instance().get_function(name, arguments, result_type);
        ASSERT_NE(function, nullptr);
        Block block;
        block.insert(arguments.front());
        block.insert({nullptr, result_type, "result"});
        const auto status = function->execute(nullptr, block, {0}, 1, 1);
        ASSERT_TRUE(status.ok()) << status;
        const auto& result = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        ASSERT_FALSE(result.is_null_at(0));
        EXPECT_DOUBLE_EQ(
                assert_cast<const ColumnFloat64&>(result.get_nested_column()).get_data()[0],
                expected);
    }
}

TEST(VGeoFunctionsTest, function_geo_spatial_null_map_overrides_valid_payload) {
    const std::string wkb(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@", 21);
    auto input_type = make_nullable(std::make_shared<DataTypeSpatial>(TYPE_GEOMETRY));
    for (const auto& name : {"st_astext", "st_asbinary", "st_geometrytype", "st_x", "st_y"}) {
        auto payloads = ColumnSpatial::create(TYPE_GEOMETRY);
        payloads->insert_data(wkb.data(), wkb.size());
        payloads->insert_data(wkb.data(), wkb.size());
        auto nulls = ColumnUInt8::create();
        nulls->insert_value(0);
        nulls->insert_value(1);
        auto input = ColumnNullable::create(std::move(payloads), std::move(nulls));
        // Typed ST_AsBinary has a binary carrier; the other raw accessors retain theirs.
        DataTypePtr result_type;
        if (std::string_view(name) == "st_x" || std::string_view(name) == "st_y") {
            result_type = make_nullable(std::make_shared<DataTypeFloat64>());
        } else if (std::string_view(name) == "st_asbinary") {
            result_type = make_nullable(std::make_shared<DataTypeVarbinary>());
        } else {
            result_type = make_nullable(std::make_shared<DataTypeString>());
        }
        ColumnsWithTypeAndName arguments {{std::move(input), input_type, "geometry"}};
        auto function =
                SimpleFunctionFactory::instance().get_function(name, arguments, result_type);
        ASSERT_NE(nullptr, function);
        Block block;
        block.insert(arguments.front());
        block.insert({nullptr, result_type, "result"});
        ASSERT_TRUE(function->execute(nullptr, block, {0}, 1, 2).ok());
        const auto& result = block.get_by_position(1).column;
        ASSERT_EQ(2, result->size());
        EXPECT_FALSE(result->is_null_at(0)) << name;
        EXPECT_TRUE(result->is_null_at(1)) << name;
    }
}

TEST(VGeoFunctionsTest, function_geo_legacy_wkb_accepts_embedded_srid) {
    const std::string ewkb = "01010000208A11000068270210774C5D40B8DECA334C3B4240";
    auto input_column = ColumnString::create();
    input_column->insert_data(ewkb.data(), ewkb.size());
    auto input_type = std::make_shared<DataTypeString>();
    ColumnsWithTypeAndName arguments {{std::move(input_column), input_type, "wkb"}};
    auto legacy_result_type = make_nullable(std::make_shared<DataTypeString>());
    auto legacy = SimpleFunctionFactory::instance().get_function("st_geometryfromwkb", arguments,
                                                                 legacy_result_type);
    ASSERT_NE(nullptr, legacy);

    Block legacy_block;
    legacy_block.insert(arguments.front());
    legacy_block.insert({nullptr, legacy_result_type, "result"});
    ASSERT_TRUE(legacy->execute(nullptr, legacy_block, {0}, 1, 1).ok());
    const auto& legacy_result = legacy_block.get_by_position(1).column;
    EXPECT_FALSE(legacy_result->is_null_at(0));
    const auto encoded = legacy_result->get_data_at(0);
    const auto shape = GeoShape::from_encoded(encoded.data, encoded.size);
    ASSERT_NE(nullptr, shape);
    const auto* point = dynamic_cast<const GeoPoint*>(shape.get());
    ASSERT_NE(nullptr, point);
    EXPECT_DOUBLE_EQ(117.19476700029702, point->x());
    EXPECT_DOUBLE_EQ(36.46326301007997, point->y());

    auto typed_column = ColumnString::create();
    typed_column->insert_data(ewkb.data(), ewkb.size());
    ColumnsWithTypeAndName typed_arguments {{std::move(typed_column), input_type, "wkb"}};
    auto typed_result_type = make_nullable(std::make_shared<DataTypeSpatial>(TYPE_GEOMETRY));
    auto typed = SimpleFunctionFactory::instance().get_function("st_geometryfromwkbtyped",
                                                                typed_arguments, typed_result_type);
    ASSERT_NE(nullptr, typed);
    Block typed_block;
    typed_block.insert(typed_arguments.front());
    typed_block.insert({nullptr, typed_result_type, "result"});
    ASSERT_TRUE(typed->execute(nullptr, typed_block, {0}, 1, 1).ok());
    EXPECT_TRUE(typed_block.get_by_position(1).column->is_null_at(0));
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

        DataSet data_set = {{{buf}, 24.7},
                            {{std::string("invalid")}, Null()},
                            {{Null()}, Null()},
                            {{buf}, 24.7}};

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

        DataSet data_set = {{{buf}, 56.7},
                            {{std::string("invalid")}, Null()},
                            {{Null()}, Null()},
                            {{buf}, 56.7}};

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
                            {{Null(), buf2, buf3}, Null()},
                            {{buf1, std::string("invalid"), buf3}, Null()},
                            {{buf3, buf2, buf1}, 1.5707963267948966}};

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
                        {{Null(), buf2}, Null()},
                        {{std::string("invalid"), buf2}, Null()},
                        {{buf1, buf2}, 1.5707963267948966}};
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
