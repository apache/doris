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
#include "core/column/column_spatial.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_spatial.h"
#include "core/data_type/data_type_string.h"
#include "core/types.h"
#include "exprs/function/function_test_util.h"
#include "exprs/function/geo/geo_common.h"
#include "exprs/function/geo/geo_types.h"
#include "gtest/gtest_pred_impl.h"
#include "testutil/any_type.h"

namespace doris {
using namespace ut_type;

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
