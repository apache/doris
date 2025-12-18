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

#include "geo/wkb_parse.h"

#include <gtest/gtest.h>

#include <memory>
#include <sstream>
#include <string>

#include "geo/geo_types.h"

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