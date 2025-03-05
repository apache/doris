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

#include "geo/geo_types.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <string.h>

#include <ostream>

#include "common/logging.h"
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
            std::unique_ptr<GeoShape> point2(GeoShape::from_encoded(buf.data(), buf.size()));
            EXPECT_STREQ("POINT (116.123 63.546)", point2->as_wkt().c_str());
        }

        {
            buf.resize(buf.size() - 1);
            std::unique_ptr<GeoShape> point2(GeoShape::from_encoded(buf.data(), buf.size()));
            EXPECT_EQ(nullptr, point2);
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
        std::unique_ptr<GeoShape> line2(GeoShape::from_encoded(buf.data(), buf.size()));
        EXPECT_STREQ(wkt, line2->as_wkt().c_str());
    }
    {
        buf.resize(buf.size() - 1);
        std::unique_ptr<GeoShape> line2(GeoShape::from_encoded(buf.data(), buf.size()));
        EXPECT_EQ(nullptr, line2);
    }
}

TEST_F(GeoTypesTest, point_intersects) {
    GeoParseStatus status;

    const char* wkt_linestring = "LINESTRING(-20 0, 20 0)";
    const char* wkt_polygon = "POLYGON((0 0,10 0,10 10,0 10,0 0))";

    std::unique_ptr<GeoShape> line(
            GeoShape::from_wkt(wkt_linestring, strlen(wkt_linestring), &status));
    std::unique_ptr<GeoShape> polygon(
            GeoShape::from_wkt(wkt_polygon, strlen(wkt_polygon), &status));
    ASSERT_NE(nullptr, line.get());
    ASSERT_NE(nullptr, polygon.get());

    {
        // point on the line (center)
        GeoPoint point;
        point.from_coord(5, 0);
        EXPECT_TRUE(point.intersects(line.get()));
    }
    {
        // point at the end of the line
        GeoPoint point;
        point.from_coord(-20, 0);
        EXPECT_TRUE(point.intersects(line.get()));
    }
    {
        // point outside the line
        GeoPoint point;
        point.from_coord(0, 5);
        EXPECT_FALSE(point.intersects(line.get()));
    }

    {
        // point inside polygons
        GeoPoint point;
        point.from_coord(5, 5);
        EXPECT_TRUE(point.intersects(polygon.get()));
    }
    {
        // point on polygon boundary edges (not vertices)
        GeoPoint point;
        point.from_coord(5, 0);
        EXPECT_TRUE(point.intersects(polygon.get()));
    }
    {
        // point at polygon vertices
        GeoPoint point;
        point.from_coord(0, 0);
        EXPECT_TRUE(point.intersects(polygon.get()));
    }
    {
        // point outside the polygon
        GeoPoint point;
        point.from_coord(20, 20);
        EXPECT_FALSE(point.intersects(polygon.get()));
    }

    std::string buf;
    polygon->encode_to(&buf);
    {
        std::unique_ptr<GeoShape> shape(GeoShape::from_encoded(buf.data(), buf.size()));
        ASSERT_NE(nullptr, shape.get());
        EXPECT_EQ(GEO_SHAPE_POLYGON, shape->type());
    }
    {
        buf.resize(buf.size() - 1);
        std::unique_ptr<GeoShape> shape(GeoShape::from_encoded(buf.data(), buf.size()));
        EXPECT_EQ(nullptr, shape.get());
    }
}

TEST_F(GeoTypesTest, linestring_intersects) {
    GeoParseStatus status;

    const char* base_line = "LINESTRING(-10 0, 10 0)";
    const char* vertical_line = "LINESTRING(0 -10, 0 10)";
    const char* polygon = "POLYGON((-5 -5,5 -5,5 5,-5 5,-5 -5))";

    std::unique_ptr<GeoShape> base_line_shape(
            GeoShape::from_wkt(base_line, strlen(base_line), &status));
    std::unique_ptr<GeoShape> vertical_line_shape(
            GeoShape::from_wkt(vertical_line, strlen(vertical_line), &status));
    std::unique_ptr<GeoShape> polygon_shape(GeoShape::from_wkt(polygon, strlen(polygon), &status));
    ASSERT_NE(nullptr, base_line_shape.get());
    ASSERT_NE(nullptr, vertical_line_shape.get());
    ASSERT_NE(nullptr, polygon_shape.get());

    // ======================
    // LineString vs Point
    // ======================
    {
        // point in the middle of the segment
        GeoPoint point;
        point.from_coord(5, 0);
        EXPECT_TRUE(base_line_shape->intersects(&point));
    }
    {
        // point at the endpoints of the segment
        GeoPoint point;
        point.from_coord(-10, 0);
        EXPECT_TRUE(base_line_shape->intersects(&point));
    }
    {
        // the point is outside the segment
        GeoPoint point;
        point.from_coord(0, 5);
        EXPECT_FALSE(base_line_shape->intersects(&point));
    }

    // ======================
    // LineString vs LineString
    // ======================
    {
        // crosswalks
        const char* wkt_string = "LINESTRING(-5 5,5 -5)";
        std::unique_ptr<GeoShape> cross_line(
                GeoShape::from_wkt(wkt_string, strlen(wkt_string), &status));
        EXPECT_TRUE(base_line_shape->intersects(cross_line.get()));
    }
    {
        // partially overlapping lines
        const char* wkt_string = "LINESTRING(-5 0,5 0)";
        std::unique_ptr<GeoShape> overlap_line(
                GeoShape::from_wkt(wkt_string, strlen(wkt_string), &status));
        EXPECT_TRUE(base_line_shape->intersects(overlap_line.get()));
    }
    {
        // end contact line
        const char* wkt_string = "LINESTRING(10 0,10 10)";
        std::unique_ptr<GeoShape> touch_line(
                GeoShape::from_wkt(wkt_string, strlen(wkt_string), &status));
        EXPECT_TRUE(base_line_shape->intersects(touch_line.get()));
    }
    {
        // fully separated lines
        const char* wkt_string = "LINESTRING(0 5,10 5)";
        std::unique_ptr<GeoShape> separate_line(
                GeoShape::from_wkt(wkt_string, strlen(wkt_string), &status));
        EXPECT_FALSE(base_line_shape->intersects(separate_line.get()));
    }

    // ======================
    // LineString vs Polygon
    // ======================
    {
        // fully internal
        const char* wkt_string = "LINESTRING(-2 0,2 0)";
        std::unique_ptr<GeoShape> inner_line(
                GeoShape::from_wkt(wkt_string, strlen(wkt_string), &status));
        EXPECT_TRUE(polygon_shape->intersects(inner_line.get()));
    }
    {
        // crossing the border
        const char* wkt_string = "LINESTRING(-10 0,10 0)";
        std::unique_ptr<GeoShape> cross_line(
                GeoShape::from_wkt(wkt_string, strlen(wkt_string), &status));
        EXPECT_TRUE(polygon_shape->intersects(cross_line.get()));
    }
    {
        // along the borderline
        const char* wkt_string = "LINESTRING(-5 -5,5 -5)";
        std::unique_ptr<GeoShape> edge_line(
                GeoShape::from_wkt(wkt_string, strlen(wkt_string), &status));
        EXPECT_TRUE(polygon_shape->intersects(edge_line.get()));
    }
    {
        // only one point
        const char* wkt_string = "LINESTRING(-5 -5,-5 -10)";
        std::unique_ptr<GeoShape> edge_line(
                GeoShape::from_wkt(wkt_string, strlen(wkt_string), &status));
        EXPECT_TRUE(polygon_shape->intersects(edge_line.get()));
    }
    {
        // fully external
        const char* wkt_string = "LINESTRING(10 10,20 20)";
        std::unique_ptr<GeoShape> outer_line(
                GeoShape::from_wkt(wkt_string, strlen(wkt_string), &status));
        EXPECT_FALSE(polygon_shape->intersects(outer_line.get()));
    }

    std::string buf;
    base_line_shape->encode_to(&buf);
    {
        std::unique_ptr<GeoShape> decoded(GeoShape::from_encoded(buf.data(), buf.size()));
        ASSERT_NE(nullptr, decoded.get());
        EXPECT_EQ(GEO_SHAPE_LINE_STRING, decoded->type());
    }
    {
        buf.resize(buf.size() - 2);
        std::unique_ptr<GeoShape> decoded(GeoShape::from_encoded(buf.data(), buf.size()));
        EXPECT_EQ(nullptr, decoded.get());
    }
}

TEST_F(GeoTypesTest, polygon_intersects) {
    GeoParseStatus status;

    const char* base_polygon = "POLYGON((0 0,10 0,10 10,0 10,0 0))";
    const char* test_line = "LINESTRING(-5 5,15 5)";
    const char* overlap_polygon = "POLYGON((5 5,15 5,15 15,5 15,5 5))";

    std::unique_ptr<GeoShape> polygon(
            GeoShape::from_wkt(base_polygon, strlen(base_polygon), &status));
    std::unique_ptr<GeoShape> line(GeoShape::from_wkt(test_line, strlen(test_line), &status));
    std::unique_ptr<GeoShape> other_polygon(
            GeoShape::from_wkt(overlap_polygon, strlen(overlap_polygon), &status));
    ASSERT_NE(nullptr, polygon.get());
    ASSERT_NE(nullptr, line.get());
    ASSERT_NE(nullptr, other_polygon.get());

    // ======================
    // Polygon vs Point
    // ======================
    {
        GeoPoint point;
        point.from_coord(5, 5);
        EXPECT_TRUE(polygon->intersects(&point));
    }
    {
        GeoPoint point;
        point.from_coord(5, 0);
        EXPECT_TRUE(polygon->intersects(&point));
    }
    {
        GeoPoint point;
        point.from_coord(0, 0);
        EXPECT_TRUE(polygon->intersects(&point));
    }
    {
        GeoPoint point;
        point.from_coord(20, 20);
        EXPECT_FALSE(polygon->intersects(&point));
    }

    // ======================
    // Polygon vs LineString
    // ======================
    {
        const char* wkt = "LINESTRING(2 2,8 8)";
        std::unique_ptr<GeoShape> inner_line(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_TRUE(polygon->intersects(inner_line.get()));
    }
    {
        const char* wkt = "LINESTRING(-5 5,15 5)";
        std::unique_ptr<GeoShape> cross_line(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_TRUE(polygon->intersects(cross_line.get()));
    }
    {
        const char* wkt = "LINESTRING(0 0,10 0)";
        std::unique_ptr<GeoShape> edge_line(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_TRUE(polygon->intersects(edge_line.get()));
    }
    {
        const char* wkt = "LINESTRING(20 20,30 30)";
        std::unique_ptr<GeoShape> outer_line(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_FALSE(polygon->intersects(outer_line.get()));
    }

    // ======================
    // Polygon vs Polygon
    // ======================
    {
        const char* wkt = "POLYGON((2 2,8 2,8 8,2 8,2 2))";
        std::unique_ptr<GeoShape> small_polygon(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_TRUE(polygon->intersects(small_polygon.get()));
    }
    {
        const char* wkt = "POLYGON((5 5,15 5,15 15,5 15,5 5))";
        std::unique_ptr<GeoShape> overlap_polygon(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_TRUE(polygon->intersects(overlap_polygon.get()));
    }
    {
        const char* wkt = "POLYGON((10 0,20 0,20 10,10 10,10 0))";
        std::unique_ptr<GeoShape> touch_polygon(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_TRUE(polygon->intersects(touch_polygon.get()));
    }
    {
        const char* wkt = "POLYGON((20 20,30 20,30 30,20 30,20 20))";
        std::unique_ptr<GeoShape> separate_polygon(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_FALSE(polygon->intersects(separate_polygon.get()));
    }

    std::string buf;
    polygon->encode_to(&buf);
    {
        std::unique_ptr<GeoShape> decoded(GeoShape::from_encoded(buf.data(), buf.size()));
        ASSERT_NE(nullptr, decoded.get());
        EXPECT_EQ(GEO_SHAPE_POLYGON, decoded->type());
    }
    {
        buf.resize(buf.size() - 2);
        std::unique_ptr<GeoShape> decoded(GeoShape::from_encoded(buf.data(), buf.size()));
        EXPECT_EQ(nullptr, decoded.get());
    }
}

TEST_F(GeoTypesTest, circle_intersect) {
    GeoParseStatus status;

    GeoCircle circle;
    auto res = circle.init(0, 0, 10);
    EXPECT_EQ(GEO_PARSE_OK, res);

    // ======================
    // Circle vs Point
    // ======================
    {
        GeoPoint point;
        point.from_coord(0, 10);
        EXPECT_TRUE(circle.intersects(&point));
    }
    {
        GeoPoint point;
        point.from_coord(15, 15);
        EXPECT_FALSE(circle.intersects(&point));
    }

    // ======================
    // Circle vs LineString
    // ======================
    {
        const char* wkt = "LINESTRING(-20 0, 20 0)";
        std::unique_ptr<GeoShape> line(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_TRUE(circle.intersects(line.get()));
    }
    {
        const char* wkt = "LINESTRING(20 20, 30 30)";
        std::unique_ptr<GeoShape> line(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_FALSE(circle.intersects(line.get()));
    }

    // ======================
    // Circle vs Polygon
    // ======================
    {
        const char* wkt = "POLYGON((-5 -5,5 -5,5 5,-5 5,-5 -5))";
        std::unique_ptr<GeoShape> poly(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_TRUE(circle.intersects(poly.get()));
    }
    {
        const char* wkt = "POLYGON((20 20,30 20,30 30,20 30,20 20))";
        std::unique_ptr<GeoShape> poly(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_FALSE(circle.intersects(poly.get()));
    }

    // ======================
    // Circle vs Circle
    // ======================
    {
        GeoCircle other;
        other.init(7, 7, 5);
        EXPECT_TRUE(circle.intersects(&other));
    }
    {
        GeoCircle other;
        other.init(20, 20, 5);
        EXPECT_FALSE(circle.intersects(&other));
    }
}

TEST_F(GeoTypesTest, point_touches) {
    GeoParseStatus status;

    const char* wkt_linestring = "LINESTRING(-20 0, 20 0)";
    const char* wkt_polygon = "POLYGON((0 0,10 0,10 10,0 10,0 0))";

    std::unique_ptr<GeoShape> line(
            GeoShape::from_wkt(wkt_linestring, strlen(wkt_linestring), &status));
    std::unique_ptr<GeoShape> polygon(
            GeoShape::from_wkt(wkt_polygon, strlen(wkt_polygon), &status));
    ASSERT_NE(nullptr, line.get());
    ASSERT_NE(nullptr, polygon.get());

    {
        // point touches the line at the center
        GeoPoint point;
        point.from_coord(5, 0);
        EXPECT_FALSE(point.touches(line.get()));
    }
    {
        // point touches the end of the line
        GeoPoint point;
        point.from_coord(-20, 0);
        EXPECT_TRUE(point.touches(line.get()));
    }
    {
        // point does not touch the line
        GeoPoint point;
        point.from_coord(0, 5);
        EXPECT_FALSE(point.touches(line.get()));
    }

    {
        // point inside the polygon (does not touch)
        GeoPoint point;
        point.from_coord(5, 5);
        EXPECT_FALSE(point.touches(polygon.get()));
    }
    {
        // point touches the polygon boundary edge (not vertex)
        GeoPoint point;
        point.from_coord(5, 0);
        EXPECT_TRUE(point.touches(polygon.get()));
    }
    {
        // point touches the polygon vertex
        GeoPoint point;
        point.from_coord(0, 0);
        EXPECT_TRUE(point.touches(polygon.get()));
    }
    {
        // point does not touch the polygon
        GeoPoint point;
        point.from_coord(20, 20);
        EXPECT_FALSE(point.touches(polygon.get()));
    }

    std::string buf;
    polygon->encode_to(&buf);
    {
        std::unique_ptr<GeoShape> shape(GeoShape::from_encoded(buf.data(), buf.size()));
        ASSERT_NE(nullptr, shape.get());
        EXPECT_EQ(GEO_SHAPE_POLYGON, shape->type());
    }
    {
        buf.resize(buf.size() - 1);
        std::unique_ptr<GeoShape> shape(GeoShape::from_encoded(buf.data(), buf.size()));
        EXPECT_EQ(nullptr, shape.get());
    }
}

TEST_F(GeoTypesTest, linestring_touches) {
    GeoParseStatus status;

    const char* base_line = "LINESTRING(-10 0, 10 0)";
    const char* vertical_line = "LINESTRING(0 -10, 0 10)";
    const char* polygon = "POLYGON((-5 -5,5 -5,5 5,-5 5,-5 -5))";

    std::unique_ptr<GeoShape> base_line_shape(
            GeoShape::from_wkt(base_line, strlen(base_line), &status));
    std::unique_ptr<GeoShape> vertical_line_shape(
            GeoShape::from_wkt(vertical_line, strlen(vertical_line), &status));
    std::unique_ptr<GeoShape> polygon_shape(GeoShape::from_wkt(polygon, strlen(polygon), &status));
    ASSERT_NE(nullptr, base_line_shape.get());
    ASSERT_NE(nullptr, vertical_line_shape.get());
    ASSERT_NE(nullptr, polygon_shape.get());

    // ======================
    // LineString vs Point
    // ======================
    {
        // point in the middle of the segment
        GeoPoint point;
        point.from_coord(5, 0);
        EXPECT_FALSE(base_line_shape->touches(&point));
    }
    {
        // point at the endpoints of the segment
        GeoPoint point;
        point.from_coord(-10, 0);
        EXPECT_TRUE(base_line_shape->touches(&point));
    }
    {
        // the point is outside the segment
        GeoPoint point;
        point.from_coord(0, 5);
        EXPECT_FALSE(base_line_shape->touches(&point));
    }

    // ======================
    // LineString vs LineString
    // ======================
    {
        // crosswalks
        const char* wkt_string = "LINESTRING(-5 5, 5 -5)";
        std::unique_ptr<GeoShape> cross_line(
                GeoShape::from_wkt(wkt_string, strlen(wkt_string), &status));
        EXPECT_FALSE(base_line_shape->touches(cross_line.get()));
    }
    {
        // partially overlapping lines
        const char* wkt_string = "LINESTRING(-5 0, 5 0)";
        std::unique_ptr<GeoShape> overlap_line(
                GeoShape::from_wkt(wkt_string, strlen(wkt_string), &status));
        EXPECT_FALSE(base_line_shape->touches(overlap_line.get()));
    }
    {
        // end contact line
        const char* wkt_string = "LINESTRING(10 0, 10 10)";
        std::unique_ptr<GeoShape> touch_line(
                GeoShape::from_wkt(wkt_string, strlen(wkt_string), &status));
        EXPECT_TRUE(base_line_shape->touches(touch_line.get()));
    }
    {
        // end intersect line
        const char* wkt_string = "LINESTRING(9 0, 10 10)";
        std::unique_ptr<GeoShape> touch_line(
                GeoShape::from_wkt(wkt_string, strlen(wkt_string), &status));
        EXPECT_FALSE(base_line_shape->touches(touch_line.get()));
    }
    {
        // fully separated lines
        const char* wkt_string = "LINESTRING(0 5, 10 5)";
        std::unique_ptr<GeoShape> separate_line(
                GeoShape::from_wkt(wkt_string, strlen(wkt_string), &status));
        EXPECT_FALSE(base_line_shape->touches(separate_line.get()));
    }

    // ======================
    // LineString vs Polygon
    // ======================
    {
        // fully internal
        const char* wkt_string = "LINESTRING(-2 0,2 0)";
        std::unique_ptr<GeoShape> inner_line(
                GeoShape::from_wkt(wkt_string, strlen(wkt_string), &status));
        EXPECT_FALSE(polygon_shape->touches(inner_line.get()));
    }
    {
        // crossing the border
        const char* wkt_string = "LINESTRING(-10 0, 10 0)";
        std::unique_ptr<GeoShape> cross_line(
                GeoShape::from_wkt(wkt_string, strlen(wkt_string), &status));
        EXPECT_FALSE(polygon_shape->touches(cross_line.get()));
    }
    {
        // along the borderline
        const char* wkt_string = "LINESTRING(-5 -5, 5 -5)";
        std::unique_ptr<GeoShape> edge_line(
                GeoShape::from_wkt(wkt_string, strlen(wkt_string), &status));
        EXPECT_TRUE(polygon_shape->touches(edge_line.get()));
    }
    {
        // along the borderline
        const char* wkt_string = "LINESTRING(-20 -5,20 -5)";
        std::unique_ptr<GeoShape> edge_line(
                GeoShape::from_wkt(wkt_string, strlen(wkt_string), &status));
        EXPECT_TRUE(polygon_shape->touches(edge_line.get()));
    }
    {
        // fully external
        const char* wkt_string = "LINESTRING(10 10,20 20)";
        std::unique_ptr<GeoShape> outer_line(
                GeoShape::from_wkt(wkt_string, strlen(wkt_string), &status));
        EXPECT_FALSE(polygon_shape->touches(outer_line.get()));
    }

    std::string buf;
    base_line_shape->encode_to(&buf);
    {
        std::unique_ptr<GeoShape> decoded(GeoShape::from_encoded(buf.data(), buf.size()));
        ASSERT_NE(nullptr, decoded.get());
        EXPECT_EQ(GEO_SHAPE_LINE_STRING, decoded->type());
    }
    {
        buf.resize(buf.size() - 2);
        std::unique_ptr<GeoShape> decoded(GeoShape::from_encoded(buf.data(), buf.size()));
        EXPECT_EQ(nullptr, decoded.get());
    }
}

TEST_F(GeoTypesTest, polygon_touches) {
    GeoParseStatus status;

    const char* base_polygon = "POLYGON((0 0,10 0,10 10,0 10,0 0))";
    const char* test_line = "LINESTRING(-5 5,15 5)";
    const char* overlap_polygon = "POLYGON((5 5,15 5,15 15,5 15,5 5))";

    std::unique_ptr<GeoShape> polygon(
            GeoShape::from_wkt(base_polygon, strlen(base_polygon), &status));
    std::unique_ptr<GeoShape> line(GeoShape::from_wkt(test_line, strlen(test_line), &status));
    std::unique_ptr<GeoShape> other_polygon(
            GeoShape::from_wkt(overlap_polygon, strlen(overlap_polygon), &status));
    ASSERT_NE(nullptr, polygon.get());
    ASSERT_NE(nullptr, line.get());
    ASSERT_NE(nullptr, other_polygon.get());

    // ======================
    // Polygon vs Point
    // ======================
    {
        GeoPoint point;
        point.from_coord(5, 5);
        EXPECT_FALSE(polygon->touches(&point));
    }
    {
        GeoPoint point;
        point.from_coord(5, 0);
        EXPECT_TRUE(polygon->touches(&point));
    }
    {
        GeoPoint point;
        point.from_coord(0, 0);
        EXPECT_TRUE(polygon->touches(&point));
    }
    {
        GeoPoint point;
        point.from_coord(20, 20);
        EXPECT_FALSE(polygon->touches(&point));
    }

    // ======================
    // Polygon vs LineString
    // ======================
    {
        const char* wkt = "LINESTRING(2 2,8 8)";
        std::unique_ptr<GeoShape> inner_line(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_FALSE(polygon->touches(inner_line.get()));
    }
    {
        const char* wkt = "LINESTRING(-5 5,15 5)";
        std::unique_ptr<GeoShape> cross_line(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_FALSE(polygon->touches(cross_line.get()));
    }
    {
        const char* wkt = "LINESTRING(10 5, 15 5)";
        std::unique_ptr<GeoShape> cross_line(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_TRUE(polygon->touches(cross_line.get()));
    }
    {
        const char* wkt = "LINESTRING(5 5, 15 15)";
        std::unique_ptr<GeoShape> cross_line(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_FALSE(polygon->touches(cross_line.get()));
    }
    {
        const char* wkt = "LINESTRING(10 10, 15 15)";
        std::unique_ptr<GeoShape> cross_line(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_TRUE(polygon->touches(cross_line.get()));
    }
    {
        const char* wkt = "LINESTRING(0 0, 5 0)";
        std::unique_ptr<GeoShape> edge_line(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_TRUE(polygon->touches(edge_line.get()));
    }
    {
        const char* wkt = "LINESTRING(2 0, 5 0)";
        std::unique_ptr<GeoShape> edge_line(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_TRUE(polygon->touches(edge_line.get()));
    }
    {
        const char* wkt = "LINESTRING(0 0,10 0)";
        std::unique_ptr<GeoShape> edge_line(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_TRUE(polygon->touches(edge_line.get()));
    }
    {
        const char* wkt = "LINESTRING(20 20,30 30)";
        std::unique_ptr<GeoShape> outer_line(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_FALSE(polygon->touches(outer_line.get()));
    }

    // ======================
    // Polygon vs Polygon
    // ======================
    {
        const char* wkt = "POLYGON((2 2,8 2,8 8,2 8,2 2))";
        std::unique_ptr<GeoShape> small_polygon(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_FALSE(polygon->touches(small_polygon.get()));
    }
    {
        const char* wkt = "POLYGON((5 5,15 5,15 15,5 15,5 5))";
        std::unique_ptr<GeoShape> overlap_polygon(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_FALSE(polygon->touches(overlap_polygon.get()));
    }
    {
        const char* wkt = "POLYGON((10 0,20 0,20 10,10 10,10 0))";
        std::unique_ptr<GeoShape> touch_polygon(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_TRUE(polygon->touches(touch_polygon.get()));
    }
    {
        const char* wkt = "POLYGON((20 20,30 20,30 30,20 30,20 20))";
        std::unique_ptr<GeoShape> separate_polygon(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_FALSE(polygon->touches(separate_polygon.get()));
    }

    std::string buf;
    polygon->encode_to(&buf);
    {
        std::unique_ptr<GeoShape> decoded(GeoShape::from_encoded(buf.data(), buf.size()));
        ASSERT_NE(nullptr, decoded.get());
        EXPECT_EQ(GEO_SHAPE_POLYGON, decoded->type());
    }
    {
        buf.resize(buf.size() - 2);
        std::unique_ptr<GeoShape> decoded(GeoShape::from_encoded(buf.data(), buf.size()));
        EXPECT_EQ(nullptr, decoded.get());
    }
}

TEST_F(GeoTypesTest, circle_touches) {
    GeoParseStatus status;

    GeoCircle circle;
    auto res = circle.init(0, 0, 10);
    EXPECT_EQ(GEO_PARSE_OK, res);

    // ======================
    // Circle vs Point
    // ======================
    {
        GeoPoint point;
        point.from_coord(0, 10);
        EXPECT_TRUE(circle.touches(&point));
    }
    {
        GeoPoint point;
        point.from_coord(15, 15);
        EXPECT_FALSE(circle.touches(&point));
    }

    // ======================
    // Circle vs LineString
    // ======================
    {
        const char* wkt = "LINESTRING(-20 0, 20 0)";
        std::unique_ptr<GeoShape> line(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_FALSE(circle.touches(line.get()));
    }
    {
        const char* wkt = "LINESTRING(20 20, 30 30)";
        std::unique_ptr<GeoShape> line(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_FALSE(circle.touches(line.get()));
    }

    // ======================
    // Circle vs Polygon
    // ======================
    {
        const char* wkt = "POLYGON((-5 -5,5 -5,5 5,-5 5,-5 -5))";
        std::unique_ptr<GeoShape> poly(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_FALSE(circle.touches(poly.get()));
    }
    {
        const char* wkt = "POLYGON((10 0,20 0,20 10,10 10,10 0))";
        std::unique_ptr<GeoShape> poly(GeoShape::from_wkt(wkt, strlen(wkt), &status));
        EXPECT_TRUE(circle.touches(poly.get()));
    }

    // ======================
    // Circle vs Circle
    // ======================
    {
        GeoCircle other;
        other.init(7, 7, 5);
        EXPECT_FALSE(circle.touches(&other));
    }
    {
        GeoCircle other;
        other.init(20, 0, 10);
        EXPECT_TRUE(circle.touches(&other));
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
        std::unique_ptr<GeoShape> shape(GeoShape::from_encoded(buf.data(), buf.size()));
        EXPECT_EQ(GEO_SHAPE_POLYGON, shape->type());
        LOG(INFO) << "polygon=" << shape->as_wkt();
    }

    {
        buf.resize(buf.size() - 1);
        std::unique_ptr<GeoShape> shape(GeoShape::from_encoded(buf.data(), buf.size()));
        EXPECT_EQ(nullptr, shape);
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
    auto res = circle.init(110.123, 64, 1000);
    EXPECT_EQ(GEO_PARSE_OK, res);

    std::string buf;
    circle.encode_to(&buf);

    {
        std::unique_ptr<GeoShape> circle2(GeoShape::from_encoded(buf.data(), buf.size()));
        EXPECT_STREQ("CIRCLE ((110.123 64), 1000)", circle2->as_wkt().c_str());
    }

    {
        buf.resize(buf.size() - 1);
        std::unique_ptr<GeoShape> circle2(GeoShape::from_encoded(buf.data(), buf.size()));
        EXPECT_EQ(nullptr, circle2);
    }
}

} // namespace doris
