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

#include <bit>
#include <cmath>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "exprs/function/geo/geo_types.h"
#include "exprs/function/string_hex_util.h"

namespace doris {

class WkbParseTest : public ::testing::Test {
public:
    WkbParseTest() = default;
    ~WkbParseTest() override = default;
};

TEST_F(WkbParseTest, ByteShapesMatchLegacyHexAndOwnCoordinates) {
    const std::vector<std::string> cases {
            "0101000000000000000000F03F0000000000000040",
            "00000000013FF00000000000004000000000000000",
            "010200000002000000000000000000F03F000000000000004000000000000008400000000000001040",
            "01030000000100000005000000000000000000f0bf000000000000f0bf000000000000f03f000000000000"
            "f0bf000000000000f03f000000000000f03f000000000000f0bf000000000000f03f000000000000f0bf00"
            "0000000000f0bf",
            "010200000002000000000000000000f03f000000000000004000000000000008400000000000001040",
            "00000000030000000100000005bff0000000000000bff00000000000003ff0000000000000bff000000000"
            "00003ff00000000000003ff0000000000000bff00000000000003ff0000000000000bff0000000000000bf"
            "f0000000000000",
            "0000000002000000023ff0000000000000400000000000000040080000000000004010000000000000",
            "010300000000000000"};
    for (const auto& hex : cases) {
        std::string bytes(hex.size() / 2, '\0');
        ASSERT_EQ(string_hex::hex_decode(hex.data(), hex.size(), bytes.data()), bytes.size());
        GeoParseStatus byte_status;
        auto byte_shape = GeoShape::from_wkb_bytes(bytes.data(), bytes.size(), byte_status);
        GeoParseStatus hex_status;
        auto hex_shape = GeoShape::from_wkb(hex.data(), hex.size(), hex_status);
        ASSERT_EQ(byte_status, GeoParseStatus::GEO_PARSE_OK);
        ASSERT_EQ(hex_status, GeoParseStatus::GEO_PARSE_OK);
        ASSERT_NE(byte_shape, nullptr);
        ASSERT_NE(hex_shape, nullptr);
        const auto expected = hex_shape->as_wkt();
        bytes.assign(bytes.size(), '\0');
        EXPECT_EQ(byte_shape->as_wkt(), expected);
    }
}

TEST_F(WkbParseTest, ByteShapeParserRejectsTruncatedAndUnsupportedShapes) {
    for (const std::string hex : {"", "FF01000000", "010200000002000000000000000000F03F",
                                  "01030000000100000005000000", "010400000000000000"}) {
        std::string bytes(hex.size() / 2, '\0');
        ASSERT_EQ(string_hex::hex_decode(hex.data(), hex.size(), bytes.data()), bytes.size());
        GeoParseStatus status;
        EXPECT_EQ(GeoShape::from_wkb_bytes(bytes.data(), bytes.size(), status), nullptr);
        EXPECT_EQ(status, GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR);
    }
}

TEST_F(WkbParseTest, ByteShapeParserHandlesLargeLine) {
    std::string bytes("\x01\x02\x00\x00\x00\x00\x10\x00\x00", 9);
    for (size_t i = 0; i < 4096; ++i) {
        for (const double coordinate : {-1.0 + static_cast<double>(i) / 4096, 0.0}) {
            const auto bits = std::bit_cast<uint64_t>(coordinate);
            for (size_t byte = 0; byte < sizeof(double); ++byte) {
                bytes.push_back(static_cast<char>((bits >> (8 * byte)) & 0xff));
            }
        }
    }
    for (size_t iteration = 0; iteration < 16; ++iteration) {
        GeoParseStatus status;
        auto shape = GeoShape::from_wkb_bytes(bytes.data(), bytes.size(), status);
        ASSERT_EQ(status, GeoParseStatus::GEO_PARSE_OK);
        ASSERT_NE(shape, nullptr);
        EXPECT_EQ(shape->num_points(), 4096);
    }
}

TEST_F(WkbParseTest, parse_point_little_endian_ndr) {
    std::string hex_wkb = "0101000000000000000000F03F0000000000000040";
    std::stringstream ss(hex_wkb);

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GeoParseStatus::GEO_PARSE_OK, status);
    ASSERT_NE(nullptr, shape);
    EXPECT_EQ(GeoShapeType::GEO_SHAPE_POINT, shape->type());
    EXPECT_STREQ("POINT (1 2)", shape->as_wkt().c_str());
}

TEST_F(WkbParseTest, parse_point_big_endian_xdr) {
    std::string hex_wkb = "00000000013FF00000000000004000000000000000";
    std::stringstream ss(hex_wkb);

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GeoParseStatus::GEO_PARSE_OK, status);
    ASSERT_NE(nullptr, shape);
    EXPECT_EQ(GeoShapeType::GEO_SHAPE_POINT, shape->type());
    EXPECT_STREQ("POINT (1 2)", shape->as_wkt().c_str());
}

TEST_F(WkbParseTest, parse_invalid_byte_order_ff) {
    std::string hex_wkb = "FF01000000000000000000F03F0000000000000040";
    std::stringstream ss(hex_wkb);

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR, status);
    EXPECT_EQ(nullptr, shape);
}

TEST_F(WkbParseTest, parse_invalid_byte_order_02) {
    std::string hex_wkb = "0201000000000000000000F03F0000000000000040";
    std::stringstream ss(hex_wkb);

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR, status);
    EXPECT_EQ(nullptr, shape);
}

TEST_F(WkbParseTest, parse_empty_stream) {
    std::stringstream ss;

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR, status);
    EXPECT_EQ(nullptr, shape);
}

TEST_F(WkbParseTest, parse_insufficient_data) {
    std::string hex_wkb = "01";
    std::stringstream ss(hex_wkb);

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR, status);
    EXPECT_EQ(nullptr, shape);
}

TEST_F(WkbParseTest, parse_odd_length_hex) {
    std::string hex_wkb = "010100000000000000000F03F0000000000000040";
    std::stringstream ss(hex_wkb);

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR, status);
    EXPECT_EQ(nullptr, shape);
}

TEST_F(WkbParseTest, parse_linestring_little_endian) {
    std::string hex_wkb =
            "010200000002000000000000000000F03F00000000000000400000000000000840000000000000F03F";
    std::stringstream ss(hex_wkb);

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GeoParseStatus::GEO_PARSE_OK, status);
    ASSERT_NE(nullptr, shape);
}

TEST_F(WkbParseTest, test_byte_order_coverage_multiple_invalid) {
    std::vector<std::string> invalid_prefixes = {"FF", "02", "AA", "80", "FE"};

    for (const auto& prefix : invalid_prefixes) {
        std::string hex_wkb = prefix + "01000000000000000000F03F0000000000000040";
        std::stringstream ss(hex_wkb);

        std::unique_ptr<GeoShape> shape;
        auto status = WkbParse::parse_wkb(ss, shape);

        EXPECT_EQ(GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR, status)
                << "Failed for byte order prefix: " << prefix;
        EXPECT_EQ(nullptr, shape) << "Failed for byte order prefix: " << prefix;
    }
}

TEST_F(WkbParseTest, parse_unsupported_geometry_type) {
    std::string hex_wkb = "0104000000000000000000F03F0000000000000040";
    std::stringstream ss(hex_wkb);

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR, status);
    EXPECT_EQ(nullptr, shape);
}

TEST_F(WkbParseTest, parse_geometry_with_srid) {
    std::string hex_wkb = "0101000020E6100000000000000000F03F0000000000000040";
    std::stringstream ss(hex_wkb);

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GeoParseStatus::GEO_PARSE_OK, status);
    ASSERT_NE(nullptr, shape);
    EXPECT_EQ(GeoShapeType::GEO_SHAPE_POINT, shape->type());
}

TEST_F(WkbParseTest, parse_polygon_insufficient_points) {
    std::string hex_wkb =
            "01030000000100000002000000000000000000000000000000000000000000000000001440000000000000"
            "0000";
    std::stringstream ss(hex_wkb);

    std::unique_ptr<GeoShape> shape;
    auto status = WkbParse::parse_wkb(ss, shape);

    EXPECT_EQ(GeoParseStatus::GEO_PARSE_WKB_SYNTAX_ERROR, status);
    EXPECT_EQ(nullptr, shape);
}

static std::string raw_wkb_from_hex(const std::string& hex) {
    std::string result(hex.size() / 2, '\0');
    for (size_t i = 0; i < result.size(); ++i) {
        result[i] = static_cast<char>(std::stoi(hex.substr(2 * i, 2), nullptr, 16));
    }
    return result;
}

static std::vector<std::pair<std::string, std::string>> raw_wkb_cases() {
    const auto point = raw_wkb_from_hex("0101000000000000000000F03F0000000000000040");
    const auto big_point = raw_wkb_from_hex("000000000140080000000000004010000000000000");
    const auto line = raw_wkb_from_hex("010200000002000000") + point.substr(5) +
                      raw_wkb_from_hex("00000000000008400000000000001040");
    const auto polygon = raw_wkb_from_hex(
            "01030000000100000004000000"
            "00000000000000000000000000000000"
            "000000000000F03F0000000000000000"
            "0000000000000000000000000000F03F"
            "00000000000000000000000000000000");
    const auto multi_point = raw_wkb_from_hex("010400000002000000") + point + big_point;
    const auto multi_line =
            raw_wkb_from_hex("010500000002000000") + line + raw_wkb_from_hex("000000000200000000");
    const auto multi_polygon = raw_wkb_from_hex("010600000001000000") + polygon;
    const auto collection = raw_wkb_from_hex("010700000003000000") + multi_point + multi_line +
                            raw_wkb_from_hex("000000000700000000");
    return {{point, "POINT (1 2)"},
            {line, "LINESTRING (1 2, 3 4)"},
            {polygon, "POLYGON ((0 0, 1 0, 0 1, 0 0))"},
            {multi_point, "MULTIPOINT ((1 2), (3 4))"},
            {multi_line, "MULTILINESTRING ((1 2, 3 4), EMPTY)"},
            {multi_polygon, "MULTIPOLYGON (((0 0, 1 0, 0 1, 0 0)))"},
            {collection,
             "GEOMETRYCOLLECTION (MULTIPOINT ((1 2), (3 4)), "
             "MULTILINESTRING ((1 2, 3 4), EMPTY), GEOMETRYCOLLECTION EMPTY)"}};
}

TEST_F(WkbParseTest, RawXyTypesShareValidationAndWktTraversal) {
    for (const auto& [wkb, expected] : raw_wkb_cases()) {
        EXPECT_EQ(WkbParse::validate_wkb_bytes(wkb.data(), wkb.size()),
                  GeoParseStatus::GEO_PARSE_OK);
        EXPECT_EQ(WkbParse::validate_wkb_bytes(wkb.data(), wkb.size(), true),
                  GeoParseStatus::GEO_PARSE_OK);
        std::string text;
        ASSERT_EQ(WkbParse::wkb_to_wkt(wkb.data(), wkb.size(), &text),
                  GeoParseStatus::GEO_PARSE_OK);
        EXPECT_EQ(text, expected);
        std::string type;
        ASSERT_EQ(WkbParse::geometry_type(wkb.data(), wkb.size(), &type),
                  GeoParseStatus::GEO_PARSE_OK);
        EXPECT_EQ(type, "ST_" + expected.substr(0, expected.find(' ')));
    }
}

TEST_F(WkbParseTest, EmptyValuesRemainGeometryValues) {
    const std::vector<std::pair<std::string, std::string>> cases = {
            {"0101000000000000000000F87F000000000000F87F", "POINT EMPTY"},
            {"010200000000000000", "LINESTRING EMPTY"},
            {"010300000000000000", "POLYGON EMPTY"},
            {"010400000000000000", "MULTIPOINT EMPTY"},
            {"010500000000000000", "MULTILINESTRING EMPTY"},
            {"010600000000000000", "MULTIPOLYGON EMPTY"},
            {"010700000000000000", "GEOMETRYCOLLECTION EMPTY"}};
    for (const auto& [hex, expected] : cases) {
        const auto wkb = raw_wkb_from_hex(hex);
        EXPECT_EQ(WkbParse::validate_wkb_bytes(wkb.data(), wkb.size(), true),
                  GeoParseStatus::GEO_PARSE_OK);
        std::string text;
        ASSERT_EQ(WkbParse::wkb_to_wkt(wkb.data(), wkb.size(), &text),
                  GeoParseStatus::GEO_PARSE_OK);
        EXPECT_EQ(text, expected);
    }
    const auto point = raw_wkb_from_hex(cases.front().first);
    double x;
    double y;
    EXPECT_NE(WkbParse::point_coordinates(point.data(), point.size(), &x, &y),
              GeoParseStatus::GEO_PARSE_OK);
}

TEST_F(WkbParseTest, NestedHeadersCountsAndTrailingBytesAreValidated) {
    const auto point = raw_wkb_cases().front().first;
    const std::vector<std::string> invalid = {
            raw_wkb_from_hex("010400000001000000010200000000000000"),
            raw_wkb_from_hex("0107000000010000000201000000") + point.substr(5),
            raw_wkb_from_hex("01070000000100000001E9030000") + point.substr(5),
            raw_wkb_from_hex("0107000000010000000101000020E6100000") + point.substr(5),
            raw_wkb_from_hex("0107000000FFFFFFFF"),
            raw_wkb_from_hex("0101000100") + point.substr(5),
            point + "x",
            raw_wkb_from_hex("010200000001000000") + point.substr(5)};
    for (const auto& wkb : invalid) {
        EXPECT_NE(WkbParse::validate_wkb_bytes(wkb.data(), wkb.size()),
                  GeoParseStatus::GEO_PARSE_OK);
        std::string text;
        EXPECT_NE(WkbParse::wkb_to_wkt(wkb.data(), wkb.size(), &text),
                  GeoParseStatus::GEO_PARSE_OK);
    }
}

TEST_F(WkbParseTest, EveryTruncatedPrefixIsRejected) {
    for (const auto& value : raw_wkb_cases()) {
        const auto& wkb = value.first;
        for (size_t size = 0; size < wkb.size(); ++size) {
            EXPECT_NE(WkbParse::validate_wkb_bytes(wkb.data(), size), GeoParseStatus::GEO_PARSE_OK)
                    << size;
        }
    }
}

TEST_F(WkbParseTest, GeographyBoundsApplyToCollectionCoordinates) {
    const auto wkb = raw_wkb_from_hex(
            "010400000001000000"
            "01010000000000000000A06640000000000000F03F");
    EXPECT_EQ(WkbParse::validate_wkb_bytes(wkb.data(), wkb.size()), GeoParseStatus::GEO_PARSE_OK);
    EXPECT_NE(WkbParse::validate_wkb_bytes(wkb.data(), wkb.size(), true),
              GeoParseStatus::GEO_PARSE_OK);
}

TEST_F(WkbParseTest, DeepCollectionsUseAnExplicitTraversalStack) {
    const auto header = raw_wkb_from_hex("010700000001000000");
    std::string wkb;
    for (size_t i = 0; i < 20000; ++i) {
        wkb += header;
    }
    wkb += raw_wkb_cases().front().first;
    EXPECT_EQ(WkbParse::validate_wkb_bytes(wkb.data(), wkb.size()), GeoParseStatus::GEO_PARSE_OK);
    wkb.pop_back();
    EXPECT_NE(WkbParse::validate_wkb_bytes(wkb.data(), wkb.size()), GeoParseStatus::GEO_PARSE_OK);
}

TEST_F(WkbParseTest, RawWktPreservesRoundTripCoordinatePrecision) {
    const auto wkb = raw_wkb_from_hex("0101000000FB598C42CAC0F33F0100000000000040");
    std::string text;
    ASSERT_EQ(WkbParse::wkb_to_wkt(wkb.data(), wkb.size(), &text), GeoParseStatus::GEO_PARSE_OK);
    EXPECT_EQ(text, "POINT (1.2345678901234567 2.0000000000000004)");
}

TEST_F(WkbParseTest, MixedNanCoordinatesArePreservedWithoutBecomingEmpty) {
    const auto wkb = raw_wkb_from_hex("0101000000000000000000F03F000000000000F87F");
    EXPECT_EQ(WkbParse::validate_wkb_bytes(wkb.data(), wkb.size()), GeoParseStatus::GEO_PARSE_OK);
    EXPECT_EQ(WkbParse::validate_wkb_bytes(wkb.data(), wkb.size(), true),
              GeoParseStatus::GEO_PARSE_OK);
    std::string text;
    ASSERT_EQ(WkbParse::wkb_to_wkt(wkb.data(), wkb.size(), &text), GeoParseStatus::GEO_PARSE_OK);
    EXPECT_EQ(text, "POINT (1 NaN)");
    double x;
    double y;
    ASSERT_EQ(WkbParse::point_coordinates(wkb.data(), wkb.size(), &x, &y),
              GeoParseStatus::GEO_PARSE_OK);
    EXPECT_DOUBLE_EQ(x, 1);
    EXPECT_TRUE(std::isnan(y));
}

TEST_F(WkbParseTest, LegacyEwkbDimensionsKeepHistoricalXYInterpretation) {
    const std::string xy = "000000000000F03F0000000000000040";
    const std::string extra = "00000000000008400000000000001040";
    for (const std::string header : {"0101000080", "0101000040", "01010000C0", "01010000A0E6100000",
                                     "0101000060E6100000", "01010000E0E6100000"}) {
        const auto hex = header + xy + extra;
        std::stringstream stream(hex);
        std::unique_ptr<GeoShape> shape;
        ASSERT_EQ(GeoParseStatus::GEO_PARSE_OK, WkbParse::parse_wkb(stream, shape)) << hex;
        ASSERT_NE(nullptr, shape);
        EXPECT_EQ("POINT (1 2)", shape->as_wkt());
        const auto raw = raw_wkb_from_hex(hex);
        EXPECT_EQ(GeoParseStatus::GEO_PARSE_WKB_UNSUPPORTED_METADATA,
                  WkbParse::validate_wkb_bytes(raw.data(), raw.size()));
        EXPECT_EQ(GeoParseStatus::GEO_PARSE_WKB_UNSUPPORTED_METADATA,
                  WkbParse::validate_wkb_bytes(raw.data(), raw.size(), true));
    }
}

TEST_F(WkbParseTest, UnsupportedMetadataHasTheSameStatusAtEveryDepth) {
    const auto coordinates = raw_wkb_cases().front().first.substr(5);
    const std::vector<std::string> headers = {"01E9030000", "0101000080", "0101000040",
                                              "0101000020E6100000"};
    for (const auto& hex : headers) {
        const auto root = raw_wkb_from_hex(hex) + coordinates;
        const auto nested = raw_wkb_from_hex("010700000001000000") + root;
        EXPECT_EQ(WkbParse::validate_wkb_bytes(root.data(), root.size()),
                  GeoParseStatus::GEO_PARSE_WKB_UNSUPPORTED_METADATA);
        EXPECT_EQ(WkbParse::validate_wkb_bytes(nested.data(), nested.size()),
                  GeoParseStatus::GEO_PARSE_WKB_UNSUPPORTED_METADATA);
    }
}

TEST_F(WkbParseTest, InfiniteCoordinatesRemainInvalid) {
    const auto wkb = raw_wkb_from_hex("0101000000000000000000F07F000000000000F03F");
    EXPECT_NE(WkbParse::validate_wkb_bytes(wkb.data(), wkb.size()), GeoParseStatus::GEO_PARSE_OK);
}

} // namespace doris
