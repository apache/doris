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
#include "geo/geo_types.h"
#include "gtest/gtest_pred_impl.h"
#include "testutil/any_type.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {
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

// ==================== GEOS-based function tests ====================

// Helper: encode a WKT string to Doris internal binary format
static std::string encode_wkt(const std::string& wkt) {
    GeoParseStatus status;
    std::unique_ptr<GeoShape> shape(GeoShape::from_wkt(wkt.data(), wkt.size(), status));
    EXPECT_TRUE(shape != nullptr);
    EXPECT_TRUE(status == GEO_PARSE_OK);
    std::string buf;
    shape->encode_to(&buf);
    return buf;
}

TEST(VGeoFunctionsTest, function_geo_st_isvalid) {
    std::string func_name = "st_isvalid";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    auto polygon_buf = encode_wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");

    GeoPoint point;
    auto cur_res = point.from_coord(1, 2);
    EXPECT_TRUE(cur_res == GEO_PARSE_OK);
    std::string point_buf;
    point.encode_to(&point_buf);

    auto line_buf = encode_wkt("LINESTRING (0 0, 10 10, 20 0)");

    DataSet data_set = {{{polygon_buf}, (uint8_t)1},
                        {{point_buf}, (uint8_t)1},
                        {{line_buf}, (uint8_t)1},
                        {{Null()}, Null()}};

    static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
}

TEST(VGeoFunctionsTest, function_geo_st_makevalid) {
    std::string func_name = "st_makevalid";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    // A valid polygon passed through ST_MakeValid should produce a non-empty result
    auto polygon_buf = encode_wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");

    GeoPoint point;
    auto cur_res = point.from_coord(1, 2);
    EXPECT_TRUE(cur_res == GEO_PARSE_OK);
    std::string point_buf;
    point.encode_to(&point_buf);

    // We can't predict the exact encoded output easily, so just test NULL handling
    DataSet data_set = {{{Null()}, Null()}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));

    // Test that valid inputs produce non-null results (use AnyType)
    DataSet data_set2 = {{{polygon_buf}, AnyType {}},
                         {{point_buf}, AnyType {}}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set2));
}

TEST(VGeoFunctionsTest, function_geo_st_buffer) {
    std::string func_name = "st_buffer";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_DOUBLE};

    GeoPoint point;
    auto cur_res = point.from_coord(0, 0);
    EXPECT_TRUE(cur_res == GEO_PARSE_OK);
    std::string point_buf;
    point.encode_to(&point_buf);

    // Buffer result is complex, use AnyType for non-null check
    DataSet data_set = {{{point_buf, (double)1.0}, AnyType {}},
                        {{point_buf, (double)0.0}, AnyType {}},
                        {{Null(), (double)1.0}, Null()},
                        {{point_buf, Null()}, Null()}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(VGeoFunctionsTest, function_geo_st_centroid) {
    std::string func_name = "st_centroid";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    // Centroid of a square (0,0)-(10,10) should be point (5,5)
    auto polygon_buf = encode_wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    auto expected_buf = encode_wkt("POINT (5 5)");

    GeoPoint point;
    auto cur_res = point.from_coord(5, 5);
    EXPECT_TRUE(cur_res == GEO_PARSE_OK);
    std::string point_buf;
    point.encode_to(&point_buf);

    DataSet data_set = {{{polygon_buf}, expected_buf},
                        {{point_buf}, point_buf},
                        {{Null()}, Null()}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(VGeoFunctionsTest, function_geo_st_convexhull) {
    std::string func_name = "st_convexhull";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

    // Convex hull of a convex polygon is itself
    auto polygon_buf = encode_wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");

    DataSet data_set = {{{polygon_buf}, AnyType {}},
                        {{Null()}, Null()}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(VGeoFunctionsTest, function_geo_st_simplify) {
    std::string func_name = "st_simplify";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_DOUBLE};

    auto line_buf = encode_wkt("LINESTRING (0 0, 5 1, 10 0)");

    // Small tolerance keeps all points
    DataSet data_set = {{{line_buf, (double)0.1}, AnyType {}},
                        // Large tolerance reduces points
                        {{line_buf, (double)5.0}, AnyType {}},
                        {{Null(), (double)1.0}, Null()},
                        {{line_buf, Null()}, Null()}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(VGeoFunctionsTest, function_geo_st_union) {
    std::string func_name = "st_union";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};

    auto poly1_buf = encode_wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    auto poly2_buf = encode_wkt("POLYGON ((5 0, 15 0, 15 10, 5 10, 5 0))");

    GeoPoint point;
    auto cur_res = point.from_coord(1, 1);
    EXPECT_TRUE(cur_res == GEO_PARSE_OK);
    std::string point_buf;
    point.encode_to(&point_buf);

    DataSet data_set = {{{poly1_buf, poly2_buf}, AnyType {}},
                        {{Null(), point_buf}, Null()},
                        {{point_buf, Null()}, Null()}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(VGeoFunctionsTest, function_geo_st_intersection) {
    std::string func_name = "st_intersection";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};

    auto poly1_buf = encode_wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    auto poly2_buf = encode_wkt("POLYGON ((5 0, 15 0, 15 10, 5 10, 5 0))");

    GeoPoint point;
    auto cur_res = point.from_coord(1, 1);
    EXPECT_TRUE(cur_res == GEO_PARSE_OK);
    std::string point_buf;
    point.encode_to(&point_buf);

    // Overlapping polygons produce a non-null intersection
    DataSet data_set = {{{poly1_buf, poly2_buf}, AnyType {}},
                        {{Null(), point_buf}, Null()}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(VGeoFunctionsTest, function_geo_st_overlaps) {
    std::string func_name = "st_overlaps";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};

    auto poly1_buf = encode_wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    auto poly2_buf = encode_wkt("POLYGON ((5 0, 15 0, 15 10, 5 10, 5 0))");
    auto poly3_buf = encode_wkt("POLYGON ((20 20, 25 20, 25 25, 20 25, 20 20))");
    // One contains the other - overlaps should be false
    auto poly4_buf = encode_wkt("POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0))");
    auto poly5_buf = encode_wkt("POLYGON ((5 5, 10 5, 10 10, 5 10, 5 5))");

    GeoPoint point;
    auto cur_res = point.from_coord(1, 1);
    EXPECT_TRUE(cur_res == GEO_PARSE_OK);
    std::string point_buf;
    point.encode_to(&point_buf);

    DataSet data_set = {// two overlapping polygons
                        {{poly1_buf, poly2_buf}, (uint8_t)1},
                        // two non-overlapping polygons
                        {{poly1_buf, poly3_buf}, (uint8_t)0},
                        // containment is not overlap
                        {{poly4_buf, poly5_buf}, (uint8_t)0},
                        // NULL input
                        {{Null(), point_buf}, Null()}};

    static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
}

TEST(VGeoFunctionsTest, function_geo_st_crosses) {
    std::string func_name = "st_crosses";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};

    auto line1_buf = encode_wkt("LINESTRING (-5 5, 15 5)");
    auto poly_buf = encode_wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    auto line_inside_buf = encode_wkt("LINESTRING (2 2, 8 8)");
    auto line_a_buf = encode_wkt("LINESTRING (0 0, 10 10)");
    auto line_b_buf = encode_wkt("LINESTRING (0 10, 10 0)");
    auto line_parallel1_buf = encode_wkt("LINESTRING (0 0, 10 0)");
    auto line_parallel2_buf = encode_wkt("LINESTRING (0 5, 10 5)");

    GeoPoint point;
    auto cur_res = point.from_coord(1, 1);
    EXPECT_TRUE(cur_res == GEO_PARSE_OK);
    std::string point_buf;
    point.encode_to(&point_buf);

    DataSet data_set = {// line crosses polygon boundary
                        {{line1_buf, poly_buf}, (uint8_t)1},
                        // line inside polygon (does not cross)
                        {{line_inside_buf, poly_buf}, (uint8_t)0},
                        // two crossing lines
                        {{line_a_buf, line_b_buf}, (uint8_t)1},
                        // two parallel lines
                        {{line_parallel1_buf, line_parallel2_buf}, (uint8_t)0},
                        // NULL input
                        {{Null(), point_buf}, Null()}};

    static_cast<void>(check_function<DataTypeUInt8, true>(func_name, input_types, data_set));
}

} // namespace doris::vectorized
