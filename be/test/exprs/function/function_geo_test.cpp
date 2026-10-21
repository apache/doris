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
#include <stdint.h>

#include <iomanip>
#include <memory>
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
#include "exprs/function/geo/geo_common.h"
#include "exprs/function/geo/geo_types.h"
#include "exprs/function/geo/wkb_parse.h"
#include "exprs/function/string_hex_util.h"
#include "format/arrow/arrow_block_convertor.h"
#include "format/arrow/arrow_row_batch.h"
#include "gtest/gtest_pred_impl.h"
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

TEST(VGeoFunctionsTest, GeographyContainmentAndTouchesRejectUnimplementedPairs) {
    const std::string point = "0101000000000000000000f03f0000000000000040";
    const std::string line =
            "0102000000020000000000000000000000000000000000000000000000000000400000000000000000";
    const auto result_type = make_nullable(std::make_shared<DataTypeUInt8>());
    for (const auto& name : {"st_contains", "st_within"}) {
        Block block;
        ASSERT_TRUE(execute_geography_test(name, {point, point}, result_type, &block).ok());
        EXPECT_EQ((*block.get_by_position(2).column)[0].get<TYPE_BOOLEAN>(), 1);
        Block unsupported;
        const auto status = execute_geography_test(name, {line, line}, result_type, &unsupported);
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("does not support this Geography shape pair"),
                  std::string::npos);
    }
    Block unsupported;
    EXPECT_FALSE(
            execute_geography_test("st_touches", {line, line}, result_type, &unsupported).ok());
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
