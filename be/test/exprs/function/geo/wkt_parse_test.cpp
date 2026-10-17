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

#include "exprs/function/geo/wkt_parse.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <string.h>

#include <iterator>
#include <memory>
#include <ostream>
#include <string>

#include "common/logging.h"
#include "exprs/function/geo/geo_types.h"
#include "gtest/gtest_pred_impl.h"

namespace doris {

class WktParseTest : public testing::Test {
public:
    WktParseTest() {}
    virtual ~WktParseTest() {}
};

TEST_F(WktParseTest, normal) {
    const char* wkt = "POINT(1 2)";

    std::unique_ptr<GeoShape> shape;
    auto status = WktParse::parse_wkt(wkt, strlen(wkt), shape);
    EXPECT_EQ(GEO_PARSE_OK, status);
    EXPECT_NE(nullptr, shape);
    LOG(INFO) << "parse result: " << shape->to_string();
}

TEST_F(WktParseTest, invalid_wkt) {
    const char* wkt = "POINT(1,2)";

    std::unique_ptr<GeoShape> shape;
    auto status = WktParse::parse_wkt(wkt, strlen(wkt), shape);
    EXPECT_NE(GEO_PARSE_OK, status);
    EXPECT_EQ(nullptr, shape);
}

TEST_F(WktParseTest, parse_point_dimensions) {
    struct TestCase {
        const char* wkt;
        GeoCoordinateType type;
        double z;
        double m;
    };
    const TestCase test_cases[] = {
            {"POINT Z (1 2 3)", GeoCoordinateType::XYZ, 3, 0},
            {"POINT M (1 2 4)", GeoCoordinateType::XYM, 0, 4},
            {"POINT ZM (1 2 3 4)", GeoCoordinateType::XYZM, 3, 4},
            {"POINT (1 2 3)", GeoCoordinateType::XYZ, 3, 0},
            {"POINT (1 2 3 4)", GeoCoordinateType::XYZM, 3, 4},
    };

    for (const auto& test_case : test_cases) {
        std::unique_ptr<GeoShape> shape;
        const auto status = WktParse::parse_wkt(test_case.wkt, strlen(test_case.wkt), shape);
        ASSERT_EQ(GEO_PARSE_OK, status) << test_case.wkt;
        ASSERT_NE(nullptr, shape) << test_case.wkt;
        EXPECT_EQ(test_case.type, shape->coordinate_type()) << test_case.wkt;
        const auto coordinates = static_cast<GeoPoint*>(shape.get())->to_coords();
        ASSERT_EQ(1, coordinates.list.size());
        EXPECT_DOUBLE_EQ(test_case.z, coordinates.list[0].z);
        EXPECT_DOUBLE_EQ(test_case.m, coordinates.list[0].m);
    }
}

TEST_F(WktParseTest, parse_dimensional_complex_shapes) {
    const char* wkts[] = {
            "LINESTRING Z (0 0 1, 1 1 2)",
            "POLYGON M ((0 0 1, 0 1 2, 1 1 3, 1 0 4, 0 0 1))",
            "MULTIPOLYGON ZM (((0 0 1 2, 0 1 2 3, 1 1 3 4, 1 0 4 5, 0 0 1 2)))",
    };
    const GeoCoordinateType types[] = {
            GeoCoordinateType::XYZ,
            GeoCoordinateType::XYM,
            GeoCoordinateType::XYZM,
    };

    for (size_t i = 0; i < std::size(wkts); ++i) {
        std::unique_ptr<GeoShape> shape;
        const auto status = WktParse::parse_wkt(wkts[i], strlen(wkts[i]), shape);
        ASSERT_EQ(GEO_PARSE_OK, status) << wkts[i];
        ASSERT_NE(nullptr, shape) << wkts[i];
        EXPECT_EQ(types[i], shape->coordinate_type()) << wkts[i];
    }
}

TEST_F(WktParseTest, reject_invalid_dimensions_and_empty_shapes) {
    const char* invalid_wkts[] = {
            "POINT Z (1 2)",
            "POINT M (1 2 3 4)",
            "POINT ZM (1 2 3)",
            "LINESTRING (0 0 1, 1 1)",
            "POLYGON Z ((0 0 1, 0 1 2, 1 1, 0 0 1))",
            "POINT EMPTY",
            "LINESTRING Z EMPTY",
            "POLYGON M EMPTY",
            "MULTIPOLYGON ZM EMPTY",
            "POINT (1 2) invalid",
    };

    for (const char* wkt : invalid_wkts) {
        std::unique_ptr<GeoShape> shape;
        const auto status = WktParse::parse_wkt(wkt, strlen(wkt), shape);
        EXPECT_NE(GEO_PARSE_OK, status) << wkt;
        EXPECT_EQ(nullptr, shape) << wkt;
    }
}

} // namespace doris
