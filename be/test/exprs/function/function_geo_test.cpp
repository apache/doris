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
#include "core/column/column_const.h"
#include "core/column/column_spatial.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_spatial.h"
#include "core/data_type/data_type_string.h"
#include "core/types.h"
#include "exprs/function/function_test_util.h"
#include "exprs/function/geo/geo_common.h"
#include "exprs/function/geo/geo_types.h"
#include "exprs/function/geo/wkb_parse.h"
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
            primitive == TYPE_GEOMETRY ? "st_geomfromwkb" : "st_geogfromwkb", arguments,
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
        ASSERT_EQ(WkbParse::wkb_to_wkt(value.data, value.size, &text), GEO_PARSE_OK);
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
    const auto result_type = make_nullable(std::make_shared<DataTypeString>());
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

TEST(VGeoFunctionsTest, function_geo_st_point_test) {
    std::string func_name = "st_point";

    GeoPoint point;
    auto cur_res = point.from_coord(24.7, 56.7);
    EXPECT_TRUE(cur_res == GEO_PARSE_OK);
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
        EXPECT_TRUE(cur_res == GEO_PARSE_OK);
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

TEST(VGeoFunctionsTest, function_geo_st_geomfromwkb_returns_raw_geometry_wkb) {
    const std::string wkb(
            "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@", 21);
    for (const auto& function_name : {"st_geomfromwkb", "st_geometryfromwkb"}) {
        auto input_column = ColumnString::create();
        input_column->insert_data("0101000000000000000000F03F0000000000000040", 42);
        auto input_type = std::make_shared<DataTypeString>();
        ColumnsWithTypeAndName arguments {{std::move(input_column), input_type, "wkb"}};
        auto result_type = make_nullable(std::make_shared<DataTypeSpatial>(TYPE_GEOMETRY));
        auto function = SimpleFunctionFactory::instance().get_function(function_name, arguments,
                                                                       result_type);
        ASSERT_NE(nullptr, function);

        Block block;
        block.insert(arguments.front());
        block.insert({nullptr, result_type, "result"});
        ASSERT_TRUE(function->execute(nullptr, block, {0}, 1, 1).ok());

        const auto value = block.get_by_position(1).column->get_data_at(0);
        EXPECT_EQ(wkb, std::string(value.data, value.size));
    }
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
            std::make_shared<DataTypeSpatial>(TYPE_GEOGRAPHY, "OGC:CRS84", "spherical");
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
        auto result_type = make_nullable(std::make_shared<DataTypeString>());
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
        const auto result_type = make_nullable(std::make_shared<DataTypeString>());
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
        DataTypePtr result_type =
                (std::string_view(name) == "st_x" || std::string_view(name) == "st_y")
                        ? make_nullable(std::make_shared<DataTypeFloat64>())
                        : make_nullable(std::make_shared<DataTypeString>());
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
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

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
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

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
        EXPECT_TRUE(value == GEO_PARSE_OK);
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
        EXPECT_TRUE(status == GEO_PARSE_OK);
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
        EXPECT_TRUE(status == GEO_PARSE_OK);
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
        EXPECT_TRUE(status == GEO_PARSE_OK);
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
        EXPECT_TRUE(status == GEO_PARSE_OK);
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
        EXPECT_TRUE(status == GEO_PARSE_OK);
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
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        GeoParseStatus status;
        std::string buf;
        std::string input = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))";
        std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(input.data(), input.size(), status));
        EXPECT_TRUE(shape != nullptr);
        EXPECT_TRUE(status == GEO_PARSE_OK);
        shape->encode_to(&buf);
        DataSet data_set = {{{buf}, (double)12364.036567076409}, {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
    }
}

} // namespace doris
