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

#include "exprs/function/geo/wkb_parse.h"

#include <gtest/gtest.h>

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "exprs/function/geo/geo_types.h"

namespace doris {

class WkbParseTest : public ::testing::Test {
public:
    WkbParseTest() = default;
    ~WkbParseTest() override = default;
};

TEST_F(WkbParseTest, parse_point_little_endian_ndr) {
    std::string hex_wkb = "0101000000000000000000F03F0000000000000040";
    std::stringstream ss(hex_wkb);

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GEO_PARSE_OK, status);
    ASSERT_NE(nullptr, shape);
    EXPECT_EQ(GEO_SHAPE_POINT, shape->type());
    EXPECT_STREQ("POINT (1 2)", shape->as_wkt().c_str());
}

TEST_F(WkbParseTest, parse_point_big_endian_xdr) {
    std::string hex_wkb = "00000000013FF00000000000004000000000000000";
    std::stringstream ss(hex_wkb);

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GEO_PARSE_OK, status);
    ASSERT_NE(nullptr, shape);
    EXPECT_EQ(GEO_SHAPE_POINT, shape->type());
    EXPECT_STREQ("POINT (1 2)", shape->as_wkt().c_str());
}

TEST_F(WkbParseTest, parse_invalid_byte_order_ff) {
    std::string hex_wkb = "FF01000000000000000000F03F0000000000000040";
    std::stringstream ss(hex_wkb);

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GEO_PARSE_WKB_SYNTAX_ERROR, status);
    EXPECT_EQ(nullptr, shape);
}

TEST_F(WkbParseTest, parse_invalid_byte_order_02) {
    std::string hex_wkb = "0201000000000000000000F03F0000000000000040";
    std::stringstream ss(hex_wkb);

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GEO_PARSE_WKB_SYNTAX_ERROR, status);
    EXPECT_EQ(nullptr, shape);
}

TEST_F(WkbParseTest, parse_empty_stream) {
    std::stringstream ss;

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GEO_PARSE_WKB_SYNTAX_ERROR, status);
    EXPECT_EQ(nullptr, shape);
}

TEST_F(WkbParseTest, parse_insufficient_data) {
    std::string hex_wkb = "01";
    std::stringstream ss(hex_wkb);

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GEO_PARSE_WKB_SYNTAX_ERROR, status);
    EXPECT_EQ(nullptr, shape);
}

TEST_F(WkbParseTest, parse_odd_length_hex) {
    std::string hex_wkb = "010100000000000000000F03F0000000000000040";
    std::stringstream ss(hex_wkb);

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GEO_PARSE_WKB_SYNTAX_ERROR, status);
    EXPECT_EQ(nullptr, shape);
}

TEST_F(WkbParseTest, parse_linestring_little_endian) {
    std::string hex_wkb =
            "010200000002000000000000000000F03F00000000000000400000000000000840000000000000F03F";
    std::stringstream ss(hex_wkb);

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GEO_PARSE_OK, status);
    ASSERT_NE(nullptr, shape);
}

TEST_F(WkbParseTest, test_byte_order_coverage_multiple_invalid) {
    std::vector<std::string> invalid_prefixes = {"FF", "02", "AA", "80", "FE"};

    for (const auto& prefix : invalid_prefixes) {
        std::string hex_wkb = prefix + "01000000000000000000F03F0000000000000040";
        std::stringstream ss(hex_wkb);

        std::unique_ptr<GeoShape> shape;
        auto status = WkbParse::parse_wkb(ss, shape);

        EXPECT_EQ(GEO_PARSE_WKB_SYNTAX_ERROR, status) << "Failed for byte order prefix: " << prefix;
        EXPECT_EQ(nullptr, shape) << "Failed for byte order prefix: " << prefix;
    }
}

TEST_F(WkbParseTest, parse_unsupported_geometry_type) {
    std::string hex_wkb = "0104000000000000000000F03F0000000000000040";
    std::stringstream ss(hex_wkb);

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GEO_PARSE_WKB_SYNTAX_ERROR, status);
    EXPECT_EQ(nullptr, shape);
}

TEST_F(WkbParseTest, parse_geometry_with_srid) {
    std::string hex_wkb = "0101000020E6100000000000000000F03F0000000000000040";
    std::stringstream ss(hex_wkb);

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GEO_PARSE_OK, status);
    ASSERT_NE(nullptr, shape);
    EXPECT_EQ(GEO_SHAPE_POINT, shape->type());
}

TEST_F(WkbParseTest, parse_point_ewkb_and_iso_dimensions) {
    struct TestCase {
        const char* hex_wkb;
        GeoCoordinateType type;
        double z;
        double m;
    };
    const TestCase test_cases[] = {
            {"0101000080000000000000F03F00000000000000400000000000000840", GeoCoordinateType::XYZ,
             3, 0},
            {"0101000040000000000000F03F00000000000000400000000000001040", GeoCoordinateType::XYM,
             0, 4},
            {"01010000E0E6100000000000000000F03F00000000000000400000000000000840"
             "0000000000001040",
             GeoCoordinateType::XYZM, 3, 4},
            {"01E9030000000000000000F03F00000000000000400000000000000840", GeoCoordinateType::XYZ,
             3, 0},
            {"01D1070000000000000000F03F00000000000000400000000000001040", GeoCoordinateType::XYM,
             0, 4},
            {"0000000BB93FF0000000000000400000000000000040080000000000004010000000000000",
             GeoCoordinateType::XYZM, 3, 4},
    };

    for (const auto& test_case : test_cases) {
        std::stringstream stream(test_case.hex_wkb);
        std::unique_ptr<GeoShape> shape;
        const auto status = WkbParse::parse_wkb(stream, shape);
        ASSERT_EQ(GEO_PARSE_OK, status) << test_case.hex_wkb;
        ASSERT_NE(nullptr, shape) << test_case.hex_wkb;
        ASSERT_EQ(GEO_SHAPE_POINT, shape->type());
        EXPECT_EQ(test_case.type, shape->coordinate_type());
        const auto coordinates = static_cast<GeoPoint*>(shape.get())->to_coords();
        ASSERT_EQ(1, coordinates.list.size());
        EXPECT_DOUBLE_EQ(1, coordinates.list[0].x);
        EXPECT_DOUBLE_EQ(2, coordinates.list[0].y);
        EXPECT_DOUBLE_EQ(test_case.z, coordinates.list[0].z);
        EXPECT_DOUBLE_EQ(test_case.m, coordinates.list[0].m);
    }
}

TEST_F(WkbParseTest, parse_ewkb_dimensional_line_and_polygon) {
    const std::string line_hex =
            "010200008002000000000000000000F03F00000000000000400000000000000840"
            "000000000000104000000000000014400000000000001840";
    std::stringstream line_stream(line_hex);
    std::unique_ptr<GeoShape> line_shape;
    ASSERT_EQ(GEO_PARSE_OK, WkbParse::parse_wkb(line_stream, line_shape));
    ASSERT_NE(nullptr, line_shape);
    ASSERT_EQ(GEO_SHAPE_LINE_STRING, line_shape->type());
    const auto line_coordinates = static_cast<GeoLine*>(line_shape.get())->to_coords();
    ASSERT_EQ(2, line_coordinates.list.size());
    EXPECT_DOUBLE_EQ(3, line_coordinates.list[0].z);
    EXPECT_DOUBLE_EQ(6, line_coordinates.list[1].z);

    const std::string polygon_hex =
            "010300008001000000050000000000000000000000000000000000000000000000"
            "0000F03F0000000000000000000000000000F03F00000000000000400000000000"
            "00F03F000000000000F03F0000000000000840000000000000F03F000000000000"
            "0000000000000000104000000000000000000000000000000000000000000000F03F";
    std::stringstream polygon_stream(polygon_hex);
    std::unique_ptr<GeoShape> polygon_shape;
    ASSERT_EQ(GEO_PARSE_OK, WkbParse::parse_wkb(polygon_stream, polygon_shape));
    ASSERT_NE(nullptr, polygon_shape);
    ASSERT_EQ(GEO_SHAPE_POLYGON, polygon_shape->type());
    const auto polygon_coordinates = static_cast<GeoPolygon*>(polygon_shape.get())->to_coords();
    ASSERT_EQ(1, polygon_coordinates->list.size());
    ASSERT_EQ(5, polygon_coordinates->list[0]->list.size());
    EXPECT_DOUBLE_EQ(1, polygon_coordinates->list[0]->list[0].z);
    EXPECT_DOUBLE_EQ(4, polygon_coordinates->list[0]->list[3].z);
}

TEST_F(WkbParseTest, parse_dimensional_multipolygon_with_nested_byte_orders) {
    const std::string hex_wkb =
            "010600008002000000"
            "0103000080010000000500000000000000000000000000000000000000000000000000f03f"
            "0000000000000000000000000000f03f0000000000000040000000000000f03f0000000000"
            "00f03f0000000000000840000000000000f03f00000000000000000000000000001040000000"
            "00000000000000000000000000000000000000f03f"
            "0080000003000000010000000540000000000000004000000000000000401400000000000040"
            "0000000000000040080000000000004018000000000000400800000000000040080000000000"
            "00401c0000000000004008000000000000400000000000000040200000000000004000000000"
            "00000040000000000000004014000000000000";
    std::stringstream stream(hex_wkb);
    std::unique_ptr<GeoShape> shape;
    ASSERT_EQ(GEO_PARSE_OK, WkbParse::parse_wkb(stream, shape));
    ASSERT_NE(nullptr, shape);
    EXPECT_EQ(GEO_SHAPE_MULTI_POLYGON, shape->type());
    EXPECT_EQ(GeoCoordinateType::XYZ, shape->coordinate_type());
    EXPECT_EQ(
            "MULTIPOLYGON Z (((0 0 1, 0 1 2, 1 1 3, 1 0 4, 0 0 1)), "
            "((2 2 5, 2 3 6, 3 3 7, 3 2 8, 2 2 5)))",
            shape->as_wkt());

    const auto ewkb = GeoShape::as_ewkb(shape.get());
    GeoParseStatus status;
    auto round_trip = GeoShape::from_wkb(ewkb.data(), ewkb.size(), status);
    ASSERT_EQ(GEO_PARSE_OK, status);
    ASSERT_NE(nullptr, round_trip);
    EXPECT_EQ(shape->as_wkt(), round_trip->as_wkt());
}

TEST_F(WkbParseTest, reject_invalid_multipolygon_children) {
    const std::vector<std::string> invalid_wkbs = {
            // A MultiPolygon child must be a Polygon.
            "0106000000010000000101000000000000000000f03f0000000000000040",
            // The child dimensionality must match the Z dimensionality declared by the parent.
            "0106000080010000000103000000010000000500000000000000000000000000000000000000"
            "0000000000000000000000000000f03f000000000000f03f000000000000f03f0000000000"
            "00f03f000000000000000000000000000000000000000000000000",
    };

    for (const auto& hex_wkb : invalid_wkbs) {
        std::stringstream stream(hex_wkb);
        std::unique_ptr<GeoShape> shape;
        EXPECT_EQ(GEO_PARSE_WKB_SYNTAX_ERROR, WkbParse::parse_wkb(stream, shape)) << hex_wkb;
        EXPECT_EQ(nullptr, shape) << hex_wkb;
    }
}

TEST_F(WkbParseTest, reject_invalid_hex_truncated_and_trailing_data) {
    const std::vector<std::string> invalid_wkbs = {
            "0101000000000000000000G03F0000000000000040",
            "0101000080000000000000F03F0000000000000040",
            "0101000000000000000000F03F000000000000004000",
            "01A10F0000000000000000F03F0000000000000040",
    };

    for (const auto& hex_wkb : invalid_wkbs) {
        std::stringstream stream(hex_wkb);
        std::unique_ptr<GeoShape> shape;
        EXPECT_EQ(GEO_PARSE_WKB_SYNTAX_ERROR, WkbParse::parse_wkb(stream, shape)) << hex_wkb;
        EXPECT_EQ(nullptr, shape) << hex_wkb;
    }
}

TEST_F(WkbParseTest, parse_polygon_insufficient_points) {
    std::string hex_wkb =
            "01030000000100000002000000000000000000000000000000000000000000000000001440000000000000"
            "0000";
    std::stringstream ss(hex_wkb);

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GEO_PARSE_WKB_SYNTAX_ERROR, status);
    EXPECT_EQ(nullptr, shape);
}

} // namespace doris