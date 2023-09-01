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
#include <string.h>

#include <ostream>

#include "common/logging.h"
#include "geo/util/GeoCircle.h"
#include "geo/util/GeoPoint.h"
#include "geo/util/GeoShape.h"
#include "gtest/gtest_pred_impl.h"

namespace doris {

class GeoTypesTest : public testing::Test {
public:
    GeoTypesTest() {}
    virtual ~GeoTypesTest() {}
};

TEST_F(GeoTypesTest, point_normal) {
    {
        GeoPoint point;
        auto status = point.from_coord(116.123, 63.546);
        EXPECT_EQ(GEO_PARSE_OK, status);
        EXPECT_STREQ("POINT (116.123 63.546)", point.as_wkt().c_str());

        std::string buf;
        point.encode_to(&buf);
        {
            size_t a = buf.size();
            std::unique_ptr<GeoShape> point2(GeoShape::from_encoded(buf.data(), a));
            EXPECT_STREQ("POINT (116.123 63.546)", point2->as_wkt().c_str());
        }
    }
    {
        GeoPoint point;
        GeoCoordinate coord;
        coord.x = 116.123;
        coord.y = 63.546;
        auto status = point.from_coord(coord);
        EXPECT_EQ(GEO_PARSE_OK, status);
        EXPECT_STREQ("POINT (116.123 63.546)", point.as_wkt().c_str());
    }
}

TEST_F(GeoTypesTest, point_invalid) {
    GeoPoint point;

    auto status = point.from_coord(200, 88);
    EXPECT_NE(GEO_PARSE_OK, status);
}

TEST_F(GeoTypesTest, linestring) {
    const char* wkt = "LINESTRING (30 10, 10 30, 40 40)";
    GeoParseStatus status;
    std::unique_ptr<GeoShape> line(GeoShape::from_wkt(wkt, strlen(wkt), &status));
    EXPECT_NE(nullptr, line.get());
    EXPECT_EQ(GEO_SHAPE_LINE_STRING, line->type());

    EXPECT_STREQ(wkt, line->as_wkt().c_str());

    std::string buf;
    line->encode_to(&buf);

    {
        size_t a = buf.size();
        std::unique_ptr<GeoShape> line2(GeoShape::from_encoded(buf.data(), a));
        EXPECT_STREQ(wkt, line2->as_wkt().c_str());
    }
}

TEST_F(GeoTypesTest, polygon_contains) {
    const char* wkt = "POLYGON ((10 10, 50 10, 50 10, 50 50, 50 50, 10 50, 10 10))";
    GeoParseStatus status;
    std::unique_ptr<GeoShape> polygon(GeoShape::from_wkt(wkt, strlen(wkt), &status));
    EXPECT_NE(nullptr, polygon.get());

    {
        GeoPoint point;
        point.from_coord(20, 20);
        auto res = polygon->contains(&point);
        EXPECT_TRUE(res);
    }
    {
        GeoPoint point;
        point.from_coord(5, 5);
        auto res = polygon->contains(&point);
        EXPECT_FALSE(res);
    }

    std::string buf;
    polygon->encode_to(&buf);

    {
        size_t a = buf.size();
        std::unique_ptr<GeoShape> shape(GeoShape::from_encoded(buf.data(), a));
        EXPECT_EQ(GEO_SHAPE_POLYGON, shape->type());
        LOG(INFO) << "polygon=" << shape->as_wkt();
    }
}

TEST_F(GeoTypesTest, polygon_parse_fail) {
    {
        const char* wkt = "POLYGON ((10 10, 50 10, 50 50, 10 50), (10 10 01))";
        GeoParseStatus status;
        std::unique_ptr<GeoShape> polygon(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_EQ(GEO_PARSE_WKT_SYNTAX_ERROR, status);
        EXPECT_EQ(nullptr, polygon.get());
    }
    {
        const char* wkt = "POLYGON ((10 10, 50 10, 50 50, 10 50))";
        GeoParseStatus status;
        std::unique_ptr<GeoShape> polygon(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_EQ(GEO_PARSE_LOOP_NOT_CLOSED, status);
        EXPECT_EQ(nullptr, polygon.get());
    }
    {
        const char* wkt = "POLYGON ((10 10, 50 10, 10 10))";
        GeoParseStatus status;
        std::unique_ptr<GeoShape> polygon(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_EQ(GEO_PARSE_LOOP_LACK_VERTICES, status);
        EXPECT_EQ(nullptr, polygon.get());
    }
}

TEST_F(GeoTypesTest, polygon_hole_contains) {
    const char* wkt =
            "POLYGON ((10 10, 50 10, 50 50, 10 50, 10 10), (20 20, 40 20, 40 40, 20 40, 20 20))";
    GeoParseStatus status;
    std::unique_ptr<GeoShape> polygon(GeoShape::from_wkt(wkt, strlen(wkt), &status));
    EXPECT_EQ(GEO_PARSE_OK, status);
    EXPECT_NE(nullptr, polygon);

    {
        GeoPoint point;
        point.from_coord(15, 15);
        auto res = polygon->contains(&point);
        EXPECT_TRUE(res);
    }
    {
        GeoPoint point;
        point.from_coord(25, 25);
        auto res = polygon->contains(&point);
        EXPECT_FALSE(res);
    }
    {
        GeoPoint point;
        point.from_coord(20, 20);
        auto res = polygon->contains(&point);
        EXPECT_TRUE(res);
    }
}

TEST_F(GeoTypesTest, circle) {
    GeoCircle circle;
    auto res = circle.to_s2cap(110.123, 64, 1000);
    EXPECT_EQ(GEO_PARSE_OK, res);

    std::string buf;
    circle.encode_to(&buf);

    {
        size_t a = buf.size();
        std::unique_ptr<GeoShape> circle2(GeoShape::from_encoded(buf.data(), a));
        EXPECT_STREQ("CIRCLE ((110.123 64), 1000)", circle2->as_wkt().c_str());
    }
}

} // namespace doris
