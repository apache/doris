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

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_array.h"
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

// ==================== ST_NumGeometries Tests ====================

TEST(VGeoFunctionsTest, function_geo_st_numgeometries_point) {
    std::string func_name = "st_numgeometries";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    GeoPoint point;
    auto cur_res = point.from_coord(24.7, 56.7);
    EXPECT_TRUE(cur_res == GEO_PARSE_OK);
    std::string buf;
    point.encode_to(&buf);

    DataSet data_set = {{{buf}, (int64_t)1}, {{Null()}, Null()}};

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
}

TEST(VGeoFunctionsTest, function_geo_st_numgeometries_linestring) {
    std::string func_name = "st_numgeometries";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    GeoParseStatus status;
    std::string linestring_wkt = "LINESTRING (30 10, 10 30, 40 40)";
    auto shape = GeoShape::from_wkt(linestring_wkt.data(), linestring_wkt.size(), status);
    EXPECT_TRUE(status == GEO_PARSE_OK);
    EXPECT_TRUE(shape != nullptr);

    std::string buf;
    shape->encode_to(&buf);

    // A single linestring has 1 geometry
    DataSet data_set = {{{buf}, (int64_t)1}};

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
}

TEST(VGeoFunctionsTest, function_geo_st_numgeometries_polygon) {
    std::string func_name = "st_numgeometries";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    GeoParseStatus status;
    std::string polygon_wkt = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))";
    auto shape = GeoShape::from_wkt(polygon_wkt.data(), polygon_wkt.size(), status);
    EXPECT_TRUE(status == GEO_PARSE_OK);
    EXPECT_TRUE(shape != nullptr);

    std::string buf;
    shape->encode_to(&buf);

    // A single polygon has 1 geometry
    DataSet data_set = {{{buf}, (int64_t)1}};

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
}

TEST(VGeoFunctionsTest, function_geo_st_numgeometries_multipolygon) {
    std::string func_name = "st_numgeometries";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    GeoParseStatus status;
    std::string multi_polygon_wkt =
            "MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)), ((20 20, 30 20, 30 30, 20 30, 20 "
            "20)))";
    auto shape = GeoShape::from_wkt(multi_polygon_wkt.data(), multi_polygon_wkt.size(), status);
    EXPECT_TRUE(status == GEO_PARSE_OK);
    EXPECT_TRUE(shape != nullptr);

    std::string buf;
    shape->encode_to(&buf);

    // Multi-polygon with 2 polygons
    DataSet data_set = {{{buf}, (int64_t)2}};

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
}

TEST(VGeoFunctionsTest, function_geo_st_numgeometries_invalid) {
    std::string func_name = "st_numgeometries";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    std::string invalid_buf = "invalid_geometry_data";

    DataSet data_set = {{{invalid_buf}, Null()}};

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
}

// ==================== ST_NumPoints Tests ====================

TEST(VGeoFunctionsTest, function_geo_st_numpoints_point) {
    std::string func_name = "st_numpoints";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    GeoPoint point;
    auto cur_res = point.from_coord(24.7, 56.7);
    EXPECT_TRUE(cur_res == GEO_PARSE_OK);
    std::string buf;
    point.encode_to(&buf);

    // A single point has 1 point
    DataSet data_set = {{{buf}, (int64_t)1}, {{Null()}, Null()}};

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
}

TEST(VGeoFunctionsTest, function_geo_st_numpoints_linestring) {
    std::string func_name = "st_numpoints";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    GeoParseStatus status;
    std::string linestring_wkt = "LINESTRING (30 10, 10 30, 40 40)";
    auto shape = GeoShape::from_wkt(linestring_wkt.data(), linestring_wkt.size(), status);
    EXPECT_TRUE(status == GEO_PARSE_OK);
    EXPECT_TRUE(shape != nullptr);

    std::string buf;
    shape->encode_to(&buf);

    // Linestring with 3 points
    DataSet data_set = {{{buf}, (int64_t)3}};

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
}

TEST(VGeoFunctionsTest, function_geo_st_numpoints_polygon) {
    std::string func_name = "st_numpoints";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    GeoParseStatus status;
    std::string polygon_wkt = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))";
    auto shape = GeoShape::from_wkt(polygon_wkt.data(), polygon_wkt.size(), status);
    EXPECT_TRUE(status == GEO_PARSE_OK);
    EXPECT_TRUE(shape != nullptr);

    std::string buf;
    shape->encode_to(&buf);

    // Polygon with 5 points (closed ring)
    DataSet data_set = {{{buf}, (int64_t)5}};

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
}

TEST(VGeoFunctionsTest, function_geo_st_numpoints_multipolygon) {
    std::string func_name = "st_numpoints";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    GeoParseStatus status;
    std::string multi_polygon_wkt =
            "MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)), ((20 20, 30 20, 30 30, 20 30, 20 "
            "20)))";
    auto shape = GeoShape::from_wkt(multi_polygon_wkt.data(), multi_polygon_wkt.size(), status);
    EXPECT_TRUE(status == GEO_PARSE_OK);
    EXPECT_TRUE(shape != nullptr);

    std::string buf;
    shape->encode_to(&buf);

    // Multi-polygon with 2 polygons, each with 5 points = 10 total
    DataSet data_set = {{{buf}, (int64_t)10}};

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
}

TEST(VGeoFunctionsTest, function_geo_st_numpoints_invalid) {
    std::string func_name = "st_numpoints";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    std::string invalid_buf = "invalid_geometry_data";

    DataSet data_set = {{{invalid_buf}, Null()}};

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
}

// ==================== ST_Geometries Tests ====================

TEST(VGeoFunctionsTest, function_geo_st_geometries_point) {
    std::string func_name = "st_geometries";

    // Prepare input: a single point
    GeoPoint point;
    auto cur_res = point.from_coord(24.7, 56.7);
    EXPECT_TRUE(cur_res == GEO_PARSE_OK);
    std::string point_buf;
    point.encode_to(&point_buf);

    // Build input column
    auto input_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    auto return_type = make_nullable(
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeString>())));

    Block block;
    auto input_col = input_type->create_column();
    input_col->insert_data(point_buf.data(), point_buf.size());
    block.insert({std::move(input_col), input_type, "shape"});

    FunctionBasePtr func = SimpleFunctionFactory::instance().get_function(
            func_name, block.get_columns_with_type_and_name(), return_type);
    ASSERT_TRUE(func != nullptr);

    FunctionUtils fn_utils(return_type, {input_type}, false);
    auto* fn_ctx = fn_utils.get_fn_ctx();
    ASSERT_TRUE(func->open(fn_ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(func->open(fn_ctx, FunctionContext::THREAD_LOCAL).ok());

    block.insert({nullptr, return_type, "result"});
    auto st = func->execute(fn_ctx, block, {0}, block.columns() - 1, 1);
    EXPECT_TRUE(st.ok());

    // Verify result: should be an array with one element (the point itself)
    auto result_col = block.get_by_position(block.columns() - 1).column;
    ASSERT_TRUE(result_col);
    EXPECT_EQ(result_col->size(), 1);
    // Result should not be null
    auto* nullable_col = assert_cast<const ColumnNullable*>(result_col.get());
    EXPECT_FALSE(nullable_col->is_null_at(0));

    static_cast<void>(func->close(fn_ctx, FunctionContext::THREAD_LOCAL));
    static_cast<void>(func->close(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
}

TEST(VGeoFunctionsTest, function_geo_st_geometries_multipolygon) {
    std::string func_name = "st_geometries";

    // Prepare input: a multi-polygon with 2 polygons
    GeoParseStatus status;
    std::string multi_polygon_wkt =
            "MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)), ((20 20, 30 20, 30 30, 20 30, 20 "
            "20)))";
    auto shape = GeoShape::from_wkt(multi_polygon_wkt.data(), multi_polygon_wkt.size(), status);
    EXPECT_TRUE(status == GEO_PARSE_OK);
    EXPECT_TRUE(shape != nullptr);

    std::string multi_buf;
    shape->encode_to(&multi_buf);

    // Build input column
    auto input_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    auto return_type = make_nullable(
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeString>())));

    Block block;
    auto input_col = input_type->create_column();
    input_col->insert_data(multi_buf.data(), multi_buf.size());
    block.insert({std::move(input_col), input_type, "shape"});

    FunctionBasePtr func = SimpleFunctionFactory::instance().get_function(
            func_name, block.get_columns_with_type_and_name(), return_type);
    ASSERT_TRUE(func != nullptr);

    FunctionUtils fn_utils(return_type, {input_type}, false);
    auto* fn_ctx = fn_utils.get_fn_ctx();
    ASSERT_TRUE(func->open(fn_ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(func->open(fn_ctx, FunctionContext::THREAD_LOCAL).ok());

    block.insert({nullptr, return_type, "result"});
    auto st = func->execute(fn_ctx, block, {0}, block.columns() - 1, 1);
    EXPECT_TRUE(st.ok());

    // Verify result: should be an array with 2 elements (2 polygons)
    auto result_col = block.get_by_position(block.columns() - 1).column;
    ASSERT_TRUE(result_col);
    EXPECT_EQ(result_col->size(), 1);

    auto* nullable_col = assert_cast<const ColumnNullable*>(result_col.get());
    EXPECT_FALSE(nullable_col->is_null_at(0));

    // Get the array column and check it has 2 elements
    auto& nested_col = nullable_col->get_nested_column();
    auto* array_col = assert_cast<const ColumnArray*>(&nested_col);
    const auto offsets = array_col->get_offsets();
    EXPECT_EQ(offsets[0], 2); // 2 polygons in the array

    static_cast<void>(func->close(fn_ctx, FunctionContext::THREAD_LOCAL));
    static_cast<void>(func->close(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
}

TEST(VGeoFunctionsTest, function_geo_st_geometries_null_input) {
    std::string func_name = "st_geometries";

    // Build input column with null
    auto input_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    auto return_type = make_nullable(
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeString>())));

    Block block;
    auto input_col = input_type->create_column();
    input_col->insert_default(); // insert null
    block.insert({std::move(input_col), input_type, "shape"});

    FunctionBasePtr func = SimpleFunctionFactory::instance().get_function(
            func_name, block.get_columns_with_type_and_name(), return_type);
    ASSERT_TRUE(func != nullptr);

    FunctionUtils fn_utils(return_type, {input_type}, false);
    auto* fn_ctx = fn_utils.get_fn_ctx();
    ASSERT_TRUE(func->open(fn_ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(func->open(fn_ctx, FunctionContext::THREAD_LOCAL).ok());

    block.insert({nullptr, return_type, "result"});
    auto st = func->execute(fn_ctx, block, {0}, block.columns() - 1, 1);
    EXPECT_TRUE(st.ok());

    // Verify result: should be null
    auto result_col = block.get_by_position(block.columns() - 1).column;
    ASSERT_TRUE(result_col);
    // Result column may be ColumnConst (optimization for all-null input) or ColumnNullable
    EXPECT_TRUE(result_col->is_null_at(0));

    static_cast<void>(func->close(fn_ctx, FunctionContext::THREAD_LOCAL));
    static_cast<void>(func->close(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
}

TEST(VGeoFunctionsTest, function_geo_st_geometries_invalid) {
    std::string func_name = "st_geometries";

    // Build input column with invalid geometry data
    auto input_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    auto return_type = make_nullable(
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeString>())));

    std::string invalid_buf = "invalid_geometry_data";

    Block block;
    auto input_col = input_type->create_column();
    // Insert non-null but invalid data
    auto* nullable_input = assert_cast<ColumnNullable*>(input_col.get());
    nullable_input->get_nested_column_ptr()->insert_data(invalid_buf.data(), invalid_buf.size());
    nullable_input->get_null_map_column_ptr()->insert_value(0);
    block.insert({std::move(input_col), input_type, "shape"});

    FunctionBasePtr func = SimpleFunctionFactory::instance().get_function(
            func_name, block.get_columns_with_type_and_name(), return_type);
    ASSERT_TRUE(func != nullptr);

    FunctionUtils fn_utils(return_type, {input_type}, false);
    auto* fn_ctx = fn_utils.get_fn_ctx();
    ASSERT_TRUE(func->open(fn_ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(func->open(fn_ctx, FunctionContext::THREAD_LOCAL).ok());

    block.insert({nullptr, return_type, "result"});
    auto st = func->execute(fn_ctx, block, {0}, block.columns() - 1, 1);
    EXPECT_TRUE(st.ok());

    // Verify result: should be null for invalid geometry
    auto result_col = block.get_by_position(block.columns() - 1).column;
    ASSERT_TRUE(result_col);
    auto* nullable_col = assert_cast<const ColumnNullable*>(result_col.get());
    EXPECT_TRUE(nullable_col->is_null_at(0));

    static_cast<void>(func->close(fn_ctx, FunctionContext::THREAD_LOCAL));
    static_cast<void>(func->close(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
}

TEST(VGeoFunctionsTest, function_geo_st_geometries_single_polygon) {
    std::string func_name = "st_geometries";

    // Prepare input: a single polygon (not multi)
    GeoParseStatus status;
    std::string polygon_wkt = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))";
    auto shape = GeoShape::from_wkt(polygon_wkt.data(), polygon_wkt.size(), status);
    EXPECT_TRUE(status == GEO_PARSE_OK);
    EXPECT_TRUE(shape != nullptr);

    std::string polygon_buf;
    shape->encode_to(&polygon_buf);

    // Build input column
    auto input_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    auto return_type = make_nullable(
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeString>())));

    Block block;
    auto input_col = input_type->create_column();
    input_col->insert_data(polygon_buf.data(), polygon_buf.size());
    block.insert({std::move(input_col), input_type, "shape"});

    FunctionBasePtr func = SimpleFunctionFactory::instance().get_function(
            func_name, block.get_columns_with_type_and_name(), return_type);
    ASSERT_TRUE(func != nullptr);

    FunctionUtils fn_utils(return_type, {input_type}, false);
    auto* fn_ctx = fn_utils.get_fn_ctx();
    ASSERT_TRUE(func->open(fn_ctx, FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_TRUE(func->open(fn_ctx, FunctionContext::THREAD_LOCAL).ok());

    block.insert({nullptr, return_type, "result"});
    auto st = func->execute(fn_ctx, block, {0}, block.columns() - 1, 1);
    EXPECT_TRUE(st.ok());

    // Verify result: should be an array with 1 element (the polygon itself)
    auto result_col = block.get_by_position(block.columns() - 1).column;
    ASSERT_TRUE(result_col);
    EXPECT_EQ(result_col->size(), 1);

    auto* nullable_col = assert_cast<const ColumnNullable*>(result_col.get());
    EXPECT_FALSE(nullable_col->is_null_at(0));

    // Get the array column and check it has 1 element
    auto& nested_col = nullable_col->get_nested_column();
    auto* array_col = assert_cast<const ColumnArray*>(&nested_col);
    const auto offsets = array_col->get_offsets();
    EXPECT_EQ(offsets[0], 1); // 1 polygon in the array

    static_cast<void>(func->close(fn_ctx, FunctionContext::THREAD_LOCAL));
    static_cast<void>(func->close(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
}

} // namespace doris
