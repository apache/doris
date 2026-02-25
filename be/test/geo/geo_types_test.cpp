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
            auto point2 = GeoShape::from_encoded(buf.data(), buf.size());
            EXPECT_STREQ("POINT (116.123 63.546)", point2->as_wkt().c_str());
        }

        {
            buf.resize(buf.size() - 1);
            auto point2 = GeoShape::from_encoded(buf.data(), buf.size());
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
    auto line = GeoShape::from_wkt(wkt, strlen(wkt), status);
    EXPECT_NE(nullptr, line.get());
    EXPECT_EQ(GEO_SHAPE_LINE_STRING, line->type());

    EXPECT_STREQ(wkt, line->as_wkt().c_str());

    std::string buf;
    line->encode_to(&buf);

    {
        auto line2 = GeoShape::from_encoded(buf.data(), buf.size());
        EXPECT_STREQ(wkt, line2->as_wkt().c_str());
    }
    {
        buf.resize(buf.size() - 1);
        auto line2 = GeoShape::from_encoded(buf.data(), buf.size());
        EXPECT_EQ(nullptr, line2);
    }
}

TEST_F(GeoTypesTest, point_intersects) {
    GeoParseStatus status;

    const char* wkt_linestring = "LINESTRING(-20 0, 20 0)";
    const char* wkt_polygon = "POLYGON((0 0,10 0,10 10,0 10,0 0))";
    const char* wkt_multi_polygon = "MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)))";

    auto line = GeoShape::from_wkt(wkt_linestring, strlen(wkt_linestring), status);
    auto polygon = GeoShape::from_wkt(wkt_polygon, strlen(wkt_polygon), status);
    auto multi_polygon = GeoShape::from_wkt(wkt_multi_polygon, strlen(wkt_multi_polygon), status);
    ASSERT_NE(nullptr, line.get());
    ASSERT_NE(nullptr, polygon.get());
    ASSERT_NE(nullptr, multi_polygon.get());

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
        EXPECT_TRUE(point.intersects(multi_polygon.get()));
    }
    {
        // point on polygon boundary edges (not vertices)
        GeoPoint point;
        point.from_coord(5, 0);
        EXPECT_TRUE(point.intersects(polygon.get()));
        EXPECT_TRUE(point.intersects(multi_polygon.get()));
    }
    {
        // point at polygon vertices
        GeoPoint point;
        point.from_coord(0, 0);
        EXPECT_TRUE(point.intersects(polygon.get()));
        EXPECT_TRUE(point.intersects(multi_polygon.get()));
    }
    {
        // point outside the polygon
        GeoPoint point;
        point.from_coord(20, 20);
        EXPECT_FALSE(point.intersects(polygon.get()));
        EXPECT_FALSE(point.intersects(multi_polygon.get()));
    }

    std::string buf;
    polygon->encode_to(&buf);
    {
        auto shape = GeoShape::from_encoded(buf.data(), buf.size());
        ASSERT_NE(nullptr, shape.get());
        EXPECT_EQ(GEO_SHAPE_POLYGON, shape->type());
    }
    {
        buf.resize(buf.size() - 1);
        auto shape = GeoShape::from_encoded(buf.data(), buf.size());
        EXPECT_EQ(nullptr, shape.get());
    }
}

TEST_F(GeoTypesTest, linestring_intersects) {
    GeoParseStatus status;

    const char* base_line = "LINESTRING(-10 0, 10 0)";
    const char* vertical_line = "LINESTRING(0 -10, 0 10)";
    const char* polygon = "POLYGON((-5 -5,5 -5,5 5,-5 5,-5 -5))";
    const char* multi_polygon =
            "MULTIPOLYGON(((30 30,35 30,35 35,30 35,30 30)), ((-5 -5,5 -5,5 5,-5 5,-5 "
            "-5)))";

    auto base_line_shape = GeoShape::from_wkt(base_line, strlen(base_line), status);
    auto vertical_line_shape = GeoShape::from_wkt(vertical_line, strlen(vertical_line), status);
    auto polygon_shape = GeoShape::from_wkt(polygon, strlen(polygon), status);
    auto multi_polygon_shape = GeoShape::from_wkt(multi_polygon, strlen(multi_polygon), status);
    ASSERT_NE(nullptr, base_line_shape.get());
    ASSERT_NE(nullptr, vertical_line_shape.get());
    ASSERT_NE(nullptr, polygon_shape.get());
    ASSERT_NE(nullptr, multi_polygon_shape.get());

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
        auto cross_line = GeoShape::from_wkt(wkt_string, strlen(wkt_string), status);
        EXPECT_TRUE(base_line_shape->intersects(cross_line.get()));
    }
    {
        // partially overlapping lines
        const char* wkt_string = "LINESTRING(-5 0,5 0)";
        auto overlap_line = GeoShape::from_wkt(wkt_string, strlen(wkt_string), status);
        EXPECT_TRUE(base_line_shape->intersects(overlap_line.get()));
    }
    {
        // end contact line
        const char* wkt_string = "LINESTRING(10 0,10 10)";
        auto touch_line = GeoShape::from_wkt(wkt_string, strlen(wkt_string), status);
        EXPECT_TRUE(base_line_shape->intersects(touch_line.get()));
    }
    {
        // end contact line
        const char* wkt_string = "LINESTRING(9 0,12 0)";
        auto touch_line = GeoShape::from_wkt(wkt_string, strlen(wkt_string), status);
        EXPECT_TRUE(base_line_shape->intersects(touch_line.get()));
    }
    {
        // fully separated lines
        const char* wkt_string = "LINESTRING(0 5,10 5)";
        auto separate_line = GeoShape::from_wkt(wkt_string, strlen(wkt_string), status);
        EXPECT_FALSE(base_line_shape->intersects(separate_line.get()));
    }

    // ======================
    // LineString vs Polygon
    // ======================
    {
        // fully internal
        const char* wkt_string = "LINESTRING(-2 0,2 0)";
        auto inner_line = GeoShape::from_wkt(wkt_string, strlen(wkt_string), status);
        EXPECT_TRUE(polygon_shape->intersects(inner_line.get()));
        EXPECT_TRUE(multi_polygon_shape->intersects(inner_line.get()));
    }
    {
        // crossing the border
        const char* wkt_string = "LINESTRING(-10 0,10 0)";
        auto cross_line = GeoShape::from_wkt(wkt_string, strlen(wkt_string), status);
        EXPECT_TRUE(polygon_shape->intersects(cross_line.get()));
        EXPECT_TRUE(multi_polygon_shape->intersects(cross_line.get()));
    }
    {
        // along the borderline
        const char* wkt_string = "LINESTRING(-5 -5,5 -5)";
        auto edge_line = GeoShape::from_wkt(wkt_string, strlen(wkt_string), status);
        EXPECT_TRUE(polygon_shape->intersects(edge_line.get()));
        EXPECT_TRUE(multi_polygon_shape->intersects(edge_line.get()));
    }
    {
        // only one point
        const char* wkt_string = "LINESTRING(-5 -5,-5 -10)";
        auto edge_line = GeoShape::from_wkt(wkt_string, strlen(wkt_string), status);
        EXPECT_TRUE(polygon_shape->intersects(edge_line.get()));
        EXPECT_TRUE(multi_polygon_shape->intersects(edge_line.get()));
    }
    {
        // fully external
        const char* wkt_string = "LINESTRING(10 10,20 20)";
        auto outer_line = GeoShape::from_wkt(wkt_string, strlen(wkt_string), status);
        EXPECT_FALSE(polygon_shape->intersects(outer_line.get()));
        EXPECT_FALSE(multi_polygon_shape->intersects(outer_line.get()));
    }

    std::string buf;
    base_line_shape->encode_to(&buf);
    {
        auto decoded(GeoShape::from_encoded(buf.data(), buf.size()));
        ASSERT_NE(nullptr, decoded.get());
        EXPECT_EQ(GEO_SHAPE_LINE_STRING, decoded->type());
    }
    {
        buf.resize(buf.size() - 2);
        auto decoded(GeoShape::from_encoded(buf.data(), buf.size()));
        EXPECT_EQ(nullptr, decoded.get());
    }
}

TEST_F(GeoTypesTest, polygon_intersects) {
    GeoParseStatus status;

    const char* base_polygon = "POLYGON((0 0,10 0,10 10,0 10,0 0))";
    const char* test_line = "LINESTRING(-5 5,15 5)";
    const char* overlap_polygon = "POLYGON((5 5,15 5,15 15,5 15,5 5))";
    const char* base_polygon2 = "POLYGON((-5 -5,5 -5,5 5,-5 5,-5 -5))";
    const char* multi_polygons =
            "MULTIPOLYGON(((35 35,40 35,40 40,35 40,35 35)), ((0 0,10 0,10 10,0 10,0 0)))";

    auto polygon = GeoShape::from_wkt(base_polygon, strlen(base_polygon), status);
    auto polygon2 = GeoShape::from_wkt(base_polygon2, strlen(base_polygon2), status);
    auto line = GeoShape::from_wkt(test_line, strlen(test_line), status);
    auto other_polygon = GeoShape::from_wkt(overlap_polygon, strlen(overlap_polygon), status);
    auto multi_polygon = GeoShape::from_wkt(multi_polygons, strlen(multi_polygons), status);
    ASSERT_NE(nullptr, polygon.get());
    ASSERT_NE(nullptr, polygon2.get());
    ASSERT_NE(nullptr, line.get());
    ASSERT_NE(nullptr, other_polygon.get());
    ASSERT_NE(nullptr, multi_polygon.get());

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
        point.from_coord(10.1, 10);
        EXPECT_FALSE(polygon->intersects(&point));
    }
    {
        GeoPoint point;
        point.from_coord(9.9, 10);
        EXPECT_TRUE(polygon->intersects(&point));
    }
    {
        GeoPoint point;
        point.from_coord(0, -4.99);
        EXPECT_TRUE(polygon2->intersects(&point));
    }
    {
        GeoPoint point;
        point.from_coord(0, -5.01);
        EXPECT_FALSE(polygon2->intersects(&point));
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
        const char* wkt_line = "LINESTRING(2 2,8 8)";
        auto inner_line = GeoShape::from_wkt(wkt_line, strlen(wkt_line), status);
        EXPECT_TRUE(polygon->intersects(inner_line.get()));
    }
    {
        const char* wkt_line = "LINESTRING(-5 5,15 5)";
        auto cross_line = GeoShape::from_wkt(wkt_line, strlen(wkt_line), status);
        EXPECT_TRUE(polygon->intersects(cross_line.get()));
    }
    {
        const char* wkt_line = "LINESTRING(0 0,10 0)";
        auto edge_line = GeoShape::from_wkt(wkt_line, strlen(wkt_line), status);
        EXPECT_TRUE(polygon->intersects(edge_line.get()));
    }
    {
        const char* wkt_line = "LINESTRING(0 0.1,10 0.1)";
        auto edge_line = GeoShape::from_wkt(wkt_line, strlen(wkt_line), status);
        EXPECT_TRUE(polygon->intersects(edge_line.get()));
    }
    {
        const char* wkt_line = "LINESTRING(0 -0.1,10 -0.1)";
        auto edge_line = GeoShape::from_wkt(wkt_line, strlen(wkt_line), status);
        EXPECT_FALSE(polygon->intersects(edge_line.get()));
    }
    {
        const char* wkt_line = "LINESTRING(0.1 0,0.1 10)";
        auto edge_line = GeoShape::from_wkt(wkt_line, strlen(wkt_line), status);
        EXPECT_TRUE(polygon->intersects(edge_line.get()));
    }
    {
        const char* wkt_line = "LINESTRING(-0.1 0,-0.1 10)";
        auto edge_line = GeoShape::from_wkt(wkt_line, strlen(wkt_line), status);
        EXPECT_FALSE(polygon->intersects(edge_line.get()));
    }
    {
        const char* wkt_line = "LINESTRING(0 10.1,10 10.1)";
        auto edge_line = GeoShape::from_wkt(wkt_line, strlen(wkt_line), status);
        EXPECT_FALSE(polygon->intersects(edge_line.get()));
    }
    {
        const char* wkt_line = "LINESTRING(0 9.99,10 9.99)";
        auto edge_line = GeoShape::from_wkt(wkt_line, strlen(wkt_line), status);
        EXPECT_TRUE(polygon->intersects(edge_line.get()));
    }
    {
        const char* wkt_line = "LINESTRING(20 20,30 30)";
        auto outer_line = GeoShape::from_wkt(wkt_line, strlen(wkt_line), status);
        EXPECT_FALSE(polygon->intersects(outer_line.get()));
    }
    {
        const char* wkt_line = "LINESTRING(-20 -5.01, 20 -5.01)";
        auto edge_line = GeoShape::from_wkt(wkt_line, strlen(wkt_line), status);
        EXPECT_FALSE(polygon2->intersects(edge_line.get()));
    }
    {
        const char* wkt_line = "LINESTRING(-20 -4.9, 20 -4.9)";
        auto edge_line = GeoShape::from_wkt(wkt_line, strlen(wkt_line), status);
        EXPECT_TRUE(polygon2->intersects(edge_line.get()));
    }
    {
        const char* wkt_line = "LINESTRING(-20 -5, 20 -5)";
        auto edge_line = GeoShape::from_wkt(wkt_line, strlen(wkt_line), status);
        EXPECT_TRUE(polygon2->intersects(edge_line.get()));
    }

    // ======================
    // Polygon vs Polygon
    // ======================
    {
        const char* wkt_poly = "POLYGON((2 2,8 2,8 8,2 8,2 2))";
        auto small_polygon = GeoShape::from_wkt(wkt_poly, strlen(wkt_poly), status);
        EXPECT_TRUE(polygon->intersects(small_polygon.get()));
        EXPECT_TRUE(multi_polygon->intersects(small_polygon.get()));
    }
    {
        auto overlap_polygon_shape =
                GeoShape::from_wkt(overlap_polygon, strlen(overlap_polygon), status);
        EXPECT_TRUE(polygon->intersects(overlap_polygon_shape.get()));
        EXPECT_TRUE(multi_polygon->intersects(overlap_polygon_shape.get()));
    }
    {
        const char* wkt_poly = "POLYGON((10 0,20 0,20 10,10 10,10 0))";
        auto touch_polygon = GeoShape::from_wkt(wkt_poly, strlen(wkt_poly), status);
        EXPECT_TRUE(polygon->intersects(touch_polygon.get()));
        EXPECT_TRUE(multi_polygon->intersects(touch_polygon.get()));
    }
    {
        const char* wkt_poly = "POLYGON((20 20,30 20,30 30,20 30,20 20))";
        auto separate_polygon = GeoShape::from_wkt(wkt_poly, strlen(wkt_poly), status);
        EXPECT_FALSE(polygon->intersects(separate_polygon.get()));
        EXPECT_FALSE(multi_polygon->intersects(separate_polygon.get()));
    }

    std::string buf;
    polygon->encode_to(&buf);
    {
        auto decoded = GeoShape::from_encoded(buf.data(), buf.size());
        ASSERT_NE(nullptr, decoded.get());
        EXPECT_EQ(GEO_SHAPE_POLYGON, decoded->type());
    }
    {
        buf.resize(buf.size() - 2);
        auto decoded = GeoShape::from_encoded(buf.data(), buf.size());
        EXPECT_EQ(nullptr, decoded.get());
    }
}

TEST_F(GeoTypesTest, multipolygon_intersects) {
    GeoParseStatus status;

    const char* base_multipolygon =
            "MULTIPOLYGON ("
            "((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),"
            "((15 0, 25 0, 25 10, 15 10, 15 0)),"
            "((30 30, 40 30, 35 35, 30 30))"
            ")";

    const char* test_line = "LINESTRING(-5 5, 35 5)";
    const char* overlap_polygon = "POLYGON((8 8, 18 8, 18 18, 8 18, 8 8))";
    const char* external_polygon = "POLYGON((50 50, 60 50, 60 60, 50 60, 50 50))";

    auto multipolygon = GeoShape::from_wkt(base_multipolygon, strlen(base_multipolygon), status);
    auto line = GeoShape::from_wkt(test_line, strlen(test_line), status);
    auto poly_overlap = GeoShape::from_wkt(overlap_polygon, strlen(overlap_polygon), status);
    auto poly_external = GeoShape::from_wkt(external_polygon, strlen(external_polygon), status);

    ASSERT_NE(nullptr, multipolygon.get());
    ASSERT_NE(nullptr, line.get());
    ASSERT_NE(nullptr, poly_overlap.get());
    ASSERT_NE(nullptr, poly_external.get());

    // ======================
    // MultiPolygon vs Point
    // ======================
    {
        GeoPoint point;
        point.from_coord(5, 5);
        EXPECT_FALSE(multipolygon->intersects(&point));
    }
    {
        GeoPoint point;
        point.from_coord(1.5, 1.8);
        EXPECT_TRUE(multipolygon->intersects(&point));
    }
    {
        GeoPoint point;
        point.from_coord(20, 5);
        EXPECT_TRUE(multipolygon->intersects(&point));
    }
    {
        GeoPoint point;
        point.from_coord(12, 0);
        EXPECT_FALSE(multipolygon->intersects(&point));
    }
    {
        GeoPoint point;
        point.from_coord(40, 30);
        EXPECT_TRUE(multipolygon->intersects(&point));
    }

    // ======================
    // MultiPolygon vs LineString
    // ======================
    {
        const char* wkt = "LINESTRING(4 4, 7 7)";
        auto in_hole_line = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_FALSE(multipolygon->intersects(in_hole_line.get()));
    }
    {
        const char* wkt = "LINESTRING(-5 5, 35 5)";
        auto cross_line = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_TRUE(multipolygon->intersects(cross_line.get()));
    }
    {
        const char* wkt = "LINESTRING(3 3, 7 3)";
        auto inner_line = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_TRUE(multipolygon->intersects(inner_line.get()));
    }
    {
        const char* wkt = "LINESTRING(30 30, 35 35)";
        auto triangle_line = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_TRUE(multipolygon->intersects(triangle_line.get()));
    }
    {
        const char* wkt = "LINESTRING(50 50, 60 60)";
        auto outer_line = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_FALSE(multipolygon->intersects(outer_line.get()));
    }

    // ======================
    // MultiPolygon vs Polygon
    // ======================
    {
        const char* wkt = "POLYGON((4 4, 7 4, 7 7, 4 7, 4 4))";
        auto in_hole_polygon = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_FALSE(multipolygon->intersects(in_hole_polygon.get()));
    }
    {
        const char* wkt = "POLYGON((20 0, 30 0, 30 10, 20 10, 20 0))";
        auto overlap_polygon_shape = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_TRUE(multipolygon->intersects(overlap_polygon_shape.get()));
    }
    {
        const char* wkt = "POLYGON((50 50, 60 50, 60 60, 50 60, 50 50))";
        auto external_polygon_shape = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_FALSE(multipolygon->intersects(external_polygon_shape.get()));
    }
    {
        const char* wkt = "POLYGON((10 0, 20 0, 20 5, 10 5, 10 0))";
        auto cross_polygon = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_TRUE(multipolygon->intersects(cross_polygon.get()));
    }
    {
        const char* wkt = "POLYGON((10 0, 15 0, 15 10, 10 10, 10 0))";
        auto touch_polygon = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_TRUE(multipolygon->intersects(touch_polygon.get()));
    }

    // ======================
    // MultiPolygon vs MultiPolygon
    // ======================
    {
        const char* wkt = "MULTIPOLYGON (((4 4, 5 4, 5 5, 4 5, 4 4)), ((6 6, 7 6, 7 7, 6 7, 6 6)))";
        auto in_hole_multi = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_FALSE(multipolygon->intersects(in_hole_multi.get()));
    }
    {
        const char* wkt =
                "MULTIPOLYGON (((8 8, 18 8, 18 18, 8 18, 8 8)), ((30 30, 40 30, 35 35, 30 "
                "30)))";
        auto overlap_multi = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_TRUE(multipolygon->intersects(overlap_multi.get()));
    }
    {
        const char* wkt =
                "MULTIPOLYGON (((-10 -10, 0 -10, 0 0, -10 0, -10 -10)), ((50 50, 60 50, 60 "
                "60, "
                "50 "
                "60, 50 50)))";
        auto separate_multi = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_TRUE(multipolygon->intersects(separate_multi.get()));
    }

    std::string buf;
    multipolygon->encode_to(&buf);
    {
        auto decoded = GeoShape::from_encoded(buf.data(), buf.size());
        ASSERT_NE(nullptr, decoded.get());
        EXPECT_EQ(GEO_SHAPE_MULTI_POLYGON, decoded->type());

        GeoPoint point;
        point.from_coord(20, 5);
        EXPECT_TRUE(decoded->intersects(&point));
    }
    {
        buf.resize(buf.size() - 2);
        auto decoded = GeoShape::from_encoded(buf.data(), buf.size());
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
        point.from_coord(0, 10.1);
        EXPECT_FALSE(circle.intersects(&point));
    }
    {
        GeoPoint point;
        point.from_coord(0, 9.9);
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
        auto line(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_TRUE(circle.intersects(line.get()));
    }
    {
        const char* wkt = "LINESTRING(20 20, 30 30)";
        auto line(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(circle.intersects(line.get()));
    }

    // ======================
    // Circle vs Polygon
    // ======================
    {
        const char* wkt = "POLYGON((-5 -5,5 -5,5 5,-5 5,-5 -5))";
        auto poly(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_TRUE(circle.intersects(poly.get()));
    }
    {
        const char* wkt = "POLYGON((20 20,30 20,30 30,20 30,20 20))";
        auto poly(GeoShape::from_wkt(wkt, strlen(wkt), status));
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
    const char* wkt_multi_polygon = "MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)))";

    auto line = GeoShape::from_wkt(wkt_linestring, strlen(wkt_linestring), status);
    auto polygon = GeoShape::from_wkt(wkt_polygon, strlen(wkt_polygon), status);
    auto multi_polygon = GeoShape::from_wkt(wkt_multi_polygon, strlen(wkt_multi_polygon), status);
    ASSERT_NE(nullptr, line.get());
    ASSERT_NE(nullptr, polygon.get());
    ASSERT_NE(nullptr, multi_polygon.get());

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
        EXPECT_FALSE(point.touches(multi_polygon.get()));
    }
    {
        // point touches the polygon boundary edge (not vertex)
        GeoPoint point;
        point.from_coord(5, 0);
        EXPECT_TRUE(point.touches(polygon.get()));
        EXPECT_TRUE(point.touches(multi_polygon.get()));
    }
    {
        // point touches the polygon vertex
        GeoPoint point;
        point.from_coord(0, 0);
        EXPECT_TRUE(point.touches(polygon.get()));
        EXPECT_TRUE(point.touches(multi_polygon.get()));
    }
    {
        // point does not touch the polygon
        GeoPoint point;
        point.from_coord(20, 20);
        EXPECT_FALSE(point.touches(polygon.get()));
        EXPECT_FALSE(point.touches(multi_polygon.get()));
    }

    std::string buf;
    polygon->encode_to(&buf);
    {
        auto shape = GeoShape::from_encoded(buf.data(), buf.size());
        ASSERT_NE(nullptr, shape.get());
        EXPECT_EQ(GEO_SHAPE_POLYGON, shape->type());
    }
    {
        buf.resize(buf.size() - 1);
        auto shape = GeoShape::from_encoded(buf.data(), buf.size());
        EXPECT_EQ(nullptr, shape.get());
    }
}

TEST_F(GeoTypesTest, linestring_touches) {
    GeoParseStatus status;

    const char* base_line = "LINESTRING(-10 0, 10 0)";
    const char* vertical_line = "LINESTRING(0 -10, 0 10)";
    const char* polygon = "POLYGON((-5 -5,5 -5,5 5,-5 5,-5 -5))";
    const char* multi_polygon = "MULTIPOLYGON(((-5 -5,5 -5,5 5,-5 5,-5 -5)))";

    auto base_line_shape = GeoShape::from_wkt(base_line, strlen(base_line), status);
    auto vertical_line_shape = GeoShape::from_wkt(vertical_line, strlen(vertical_line), status);
    auto polygon_shape = GeoShape::from_wkt(polygon, strlen(polygon), status);
    auto multi_polygon_shape = GeoShape::from_wkt(multi_polygon, strlen(multi_polygon), status);
    ASSERT_NE(nullptr, base_line_shape.get());
    ASSERT_NE(nullptr, vertical_line_shape.get());
    ASSERT_NE(nullptr, polygon_shape.get());
    ASSERT_NE(nullptr, multi_polygon_shape.get());

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
        auto cross_line(GeoShape::from_wkt(wkt_string, strlen(wkt_string), status));
        EXPECT_FALSE(base_line_shape->touches(cross_line.get()));
    }
    {
        // partially overlapping lines
        const char* wkt_string = "LINESTRING(-5 0, 5 0)";
        auto overlap_line = GeoShape::from_wkt(wkt_string, strlen(wkt_string), status);
        EXPECT_FALSE(base_line_shape->touches(overlap_line.get()));
    }
    {
        // end contact line
        const char* wkt_string = "LINESTRING(10 0, 12 0)";
        auto touch_line = GeoShape::from_wkt(wkt_string, strlen(wkt_string), status);
        EXPECT_TRUE(base_line_shape->touches(touch_line.get()));
    }
    {
        // end intersect line
        const char* wkt_string = "LINESTRING(9 0, 10 0)";
        auto touch_line = GeoShape::from_wkt(wkt_string, strlen(wkt_string), status);
        EXPECT_FALSE(base_line_shape->touches(touch_line.get()));
    }
    {
        // end intersect line
        const char* wkt_string = "LINESTRING(-10 0, 10 0)";
        auto touch_line(GeoShape::from_wkt(wkt_string, strlen(wkt_string), status));
        EXPECT_FALSE(base_line_shape->touches(touch_line.get()));
    }
    {
        // fully separated lines
        const char* wkt_string = "LINESTRING(0 5, 10 5)";
        auto separate_line(GeoShape::from_wkt(wkt_string, strlen(wkt_string), status));
        EXPECT_FALSE(base_line_shape->touches(separate_line.get()));
    }

    // ======================
    // LineString vs Polygon
    // ======================
    {
        // fully internal
        const char* wkt_string = "LINESTRING(-2 0,2 0)";
        auto inner_line(GeoShape::from_wkt(wkt_string, strlen(wkt_string), status));
        EXPECT_FALSE(polygon_shape->touches(inner_line.get()));
        EXPECT_FALSE(multi_polygon_shape->touches(inner_line.get()));
    }
    {
        // crossing the border
        const char* wkt_string = "LINESTRING(-10 0, 10 0)";
        auto cross_line(GeoShape::from_wkt(wkt_string, strlen(wkt_string), status));
        EXPECT_FALSE(polygon_shape->touches(cross_line.get()));
        EXPECT_FALSE(multi_polygon_shape->touches(cross_line.get()));
    }
    {
        // along the borderline
        const char* wkt_string = "LINESTRING(-5 -5, 5 -5)";
        auto edge_line(GeoShape::from_wkt(wkt_string, strlen(wkt_string), status));
        EXPECT_TRUE(polygon_shape->touches(edge_line.get()));
        EXPECT_TRUE(multi_polygon_shape->touches(edge_line.get()));
    }
    {
        // along the borderline
        const char* wkt_string = "LINESTRING(-20 -5.01, 20 -5.01)";
        auto edge_line(GeoShape::from_wkt(wkt_string, strlen(wkt_string), status));
        EXPECT_FALSE(polygon_shape->touches(edge_line.get()));
        EXPECT_FALSE(multi_polygon_shape->touches(edge_line.get()));
    }
    {
        // along the borderline
        const char* wkt_string = "LINESTRING(-20 -4.99, 20 -4.99)";
        auto edge_line(GeoShape::from_wkt(wkt_string, strlen(wkt_string), status));
        EXPECT_FALSE(polygon_shape->touches(edge_line.get()));
        EXPECT_FALSE(multi_polygon_shape->touches(edge_line.get()));
    }
    {
        // along the borderline
        const char* wkt_string = "LINESTRING(-20 -5,20 -5)";
        auto edge_line(GeoShape::from_wkt(wkt_string, strlen(wkt_string), status));
        EXPECT_TRUE(polygon_shape->touches(edge_line.get()));
        EXPECT_TRUE(multi_polygon_shape->touches(edge_line.get()));
    }
    {
        // fully external
        const char* wkt_string = "LINESTRING(10 10,20 20)";
        auto outer_line(GeoShape::from_wkt(wkt_string, strlen(wkt_string), status));
        EXPECT_FALSE(polygon_shape->touches(outer_line.get()));
        EXPECT_FALSE(multi_polygon_shape->touches(outer_line.get()));
    }

    std::string buf;
    base_line_shape->encode_to(&buf);
    {
        auto decoded(GeoShape::from_encoded(buf.data(), buf.size()));
        ASSERT_NE(nullptr, decoded.get());
        EXPECT_EQ(GEO_SHAPE_LINE_STRING, decoded->type());
    }
    {
        buf.resize(buf.size() - 2);
        auto decoded(GeoShape::from_encoded(buf.data(), buf.size()));
        EXPECT_EQ(nullptr, decoded.get());
    }
}

TEST_F(GeoTypesTest, polygon_touches) {
    GeoParseStatus status;

    const char* base_polygon = "POLYGON((0 0,10 0,10 10,0 10,0 0))";
    const char* test_line = "LINESTRING(-5 5,15 5)";
    const char* overlap_polygon = "POLYGON((5 5,15 5,15 15,5 15,5 5))";
    const char* test_multi_polugon =
            "MULTIPOLYGON(((30 30,35 30,35 35,30 35,30 30)), ((0 0,10 0,10 10,0 10,0 0)))";

    std::unique_ptr<GeoShape> polygon(
            GeoShape::from_wkt(base_polygon, strlen(base_polygon), status));
    std::unique_ptr<GeoShape> line(GeoShape::from_wkt(test_line, strlen(test_line), status));
    std::unique_ptr<GeoShape> other_polygon(
            GeoShape::from_wkt(overlap_polygon, strlen(overlap_polygon), status));
    std::unique_ptr<GeoShape> multi_polygon(
            GeoShape::from_wkt(test_multi_polugon, strlen(test_multi_polugon), status));
    ASSERT_NE(nullptr, polygon.get());
    ASSERT_NE(nullptr, line.get());
    ASSERT_NE(nullptr, other_polygon.get());
    ASSERT_NE(nullptr, multi_polygon.get());

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
        auto inner_line(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(polygon->touches(inner_line.get()));
    }
    {
        const char* wkt = "LINESTRING(-5 5,15 5)";
        auto cross_line(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(polygon->touches(cross_line.get()));
    }
    {
        const char* wkt = "LINESTRING(10 5, 15 5)";
        auto cross_line(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_TRUE(polygon->touches(cross_line.get()));
    }
    {
        const char* wkt = "LINESTRING(5 5, 15 15)";
        auto cross_line(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(polygon->touches(cross_line.get()));
    }
    {
        const char* wkt = "LINESTRING(10 10, 15 15)";
        auto cross_line(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_TRUE(polygon->touches(cross_line.get()));
    }
    {
        const char* wkt = "LINESTRING(0 0, 5 0)";
        auto edge_line(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_TRUE(polygon->touches(edge_line.get()));
    }
    {
        const char* wkt = "LINESTRING(2 0, 5 0)";
        auto edge_line(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_TRUE(polygon->touches(edge_line.get()));
    }
    {
        const char* wkt = "LINESTRING(0 0,10 0)";
        auto edge_line(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_TRUE(polygon->touches(edge_line.get()));
    }
    {
        const char* wkt = "LINESTRING(20 20,30 30)";
        auto outer_line(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(polygon->touches(outer_line.get()));
    }

    // ======================
    // Polygon vs Polygon
    // ======================
    {
        const char* wkt = "POLYGON((2 2,8 2,8 8,2 8,2 2))";
        auto small_polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(polygon->touches(small_polygon.get()));
    }
    {
        const char* wkt = "POLYGON((5 5,15 5,15 15,5 15,5 5))";
        auto overlap_polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(polygon->touches(overlap_polygon.get()));
    }
    {
        const char* wkt = "POLYGON((10 0,20 0,20 10,10 10,10 0))";
        auto touch_polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_TRUE(polygon->touches(touch_polygon.get()));
    }
    {
        const char* wkt = "POLYGON((10.1 0,20 0,20 10,10.1 10,10.1 0))";
        auto touch_polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(polygon->touches(touch_polygon.get()));
    }
    {
        const char* wkt = "POLYGON((9.99 0,20 0,20 10,9.99 10,9.99 0))";
        auto touch_polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(polygon->touches(touch_polygon.get()));
    }
    {
        const char* wkt = "POLYGON((20 20,30 20,30 30,20 30,20 20))";
        auto separate_polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(polygon->touches(separate_polygon.get()));
    }

    // ========================
    // Polygon vs MultiPolygon
    // ========================
    {
        const char* wkt = "MULTIPOLYGON(((2 2,8 2,8 8,2 8,2 2)))";
        auto small_polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(polygon->touches(small_polygon.get()));
    }
    {
        const char* wkt = "MULTIPOLYGON(((5 5,15 5,15 15,5 15,5 5)))";
        auto overlap_polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(polygon->touches(overlap_polygon.get()));
    }
    {
        const char* wkt = "MULTIPOLYGON(((10 0,20 0,20 10,10 10,10 0)))";
        auto touch_polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_TRUE(polygon->touches(touch_polygon.get()));
    }
    {
        const char* wkt = "MULTIPOLYGON(((10.1 0,20 0,20 10,10.1 10,10.1 0)))";
        auto touch_polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(polygon->touches(touch_polygon.get()));
    }
    {
        const char* wkt = "MULTIPOLYGON(((9.99 0,20 0,20 10,9.99 10,9.99 0)))";
        auto touch_polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(polygon->touches(touch_polygon.get()));
    }
    {
        const char* wkt = "MULTIPOLYGON(((20 20,30 20,30 30,20 30,20 20)))";
        auto separate_polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(polygon->touches(separate_polygon.get()));
    }

    std::string buf;
    polygon->encode_to(&buf);
    {
        auto decoded = GeoShape::from_encoded(buf.data(), buf.size());
        ASSERT_NE(nullptr, decoded.get());
        EXPECT_EQ(GEO_SHAPE_POLYGON, decoded->type());
    }
    {
        buf.resize(buf.size() - 2);
        auto decoded = GeoShape::from_encoded(buf.data(), buf.size());
        EXPECT_EQ(nullptr, decoded.get());
    }
}

TEST_F(GeoTypesTest, multipolygon_touches) {
    GeoParseStatus status;

    const char* base_multipolygon =
            "MULTIPOLYGON ("
            "((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 8 3, 8 8, 3 8, 3 3)),"
            "((15 0, 25 0, 25 10, 15 10, 15 0)),"
            "((30 30, 40 30, 35 35, 30 30))"
            ")";
    const char* test_line = "LINESTRING(10 5, 20 5)";
    const char* overlap_polygon = "POLYGON((8 8, 18 8, 18 18, 8 18, 8 8))";
    const char* test_multi_polygon =
            "MULTIPOLYGON (((-5 -5, 0 -5, 0 0, -5 0, -5 -5)), ((40 40, 50 40, 50 50, 40 "
            "50, 40 "
            "40)))";

    auto multipolygon(GeoShape::from_wkt(base_multipolygon, strlen(base_multipolygon), status));
    auto line(GeoShape::from_wkt(test_line, strlen(test_line), status));
    auto other_polygon(GeoShape::from_wkt(overlap_polygon, strlen(overlap_polygon), status));
    auto other_multipolygon(
            GeoShape::from_wkt(test_multi_polygon, strlen(test_multi_polygon), status));
    ASSERT_NE(nullptr, multipolygon.get());
    ASSERT_NE(nullptr, line.get());
    ASSERT_NE(nullptr, other_polygon.get());
    ASSERT_NE(nullptr, other_multipolygon.get());

    // ======================
    // MultiPolygon vs Point
    // ======================
    {
        GeoPoint point;
        point.from_coord(0, 0);
        EXPECT_TRUE(multipolygon->touches(&point));
    }
    {
        GeoPoint point;
        point.from_coord(10, 5);
        EXPECT_TRUE(multipolygon->touches(&point));
    }
    {
        GeoPoint point;
        point.from_coord(30, 30);
        EXPECT_TRUE(multipolygon->touches(&point));
    }
    {
        GeoPoint point;
        point.from_coord(50, 50);
        EXPECT_FALSE(multipolygon->touches(&point));
    }

    // ===========================
    // MultiPolygon vs LineString
    // ===========================
    {
        const char* wkt = "LINESTRING(3 5, 8 5)";
        auto in_hole_line(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_TRUE(multipolygon->touches(in_hole_line.get()));
    }
    {
        const char* wkt = "LINESTRING(10 5, 20 5)";
        auto cross_line(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(multipolygon->touches(cross_line.get()));
    }
    {
        const char* wkt = "LINESTRING(30 30, 35 35)";
        auto edge_line(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_TRUE(multipolygon->touches(edge_line.get()));
    }
    {
        const char* wkt = "LINESTRING(10 10, 15 0)";
        auto cross_line(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_TRUE(multipolygon->touches(cross_line.get()));
    }

    // ========================
    // MultiPolygon vs Polygon
    // ========================
    {
        const char* wkt = "POLYGON((3 3, 8 3, 8 8, 3 8, 3 3))";
        auto in_hole_polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_TRUE(multipolygon->touches(in_hole_polygon.get()));
    }
    {
        const char* wkt = "POLYGON((3 3, 10 3, 10 8, 3 8, 3 3))";
        auto cross_hole_polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(multipolygon->touches(cross_hole_polygon.get()));
    }
    {
        const char* wkt = "POLYGON((25 0, 35 0, 35 10, 25 10, 25 0))";
        auto touch_polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_TRUE(multipolygon->touches(touch_polygon.get()));
    }
    {
        const char* wkt = "POLYGON((8 8, 18 8, 18 18, 8 18, 8 8))";
        auto overlap_poly(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(multipolygon->touches(overlap_poly.get()));
    }
    {
        const char* wkt = "POLYGON((10 0, 15 0, 15 10, 10 10, 10 0))";
        auto touch_polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_TRUE(multipolygon->touches(touch_polygon.get()));
    }
    {
        const char* wkt = "POLYGON((20 20, 30 20, 30 30, 20 30, 20 20))";
        auto separate_polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_TRUE(multipolygon->touches(separate_polygon.get()));
    }
    {
        const char* wkt = "POLYGON((10 0, 20 0, 20 5, 10 5, 10 0))";
        auto cross_polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(multipolygon->touches(cross_polygon.get()));
    }

    // =============================
    // MultiPolygon vs MultiPolygon
    // =============================
    {
        const char* wkt =
                "MULTIPOLYGON (((-5 -5, 0 -5, 0 0, -5 0, -5 -5)), ((40 30, 50 30, 50 50, "
                "40 "
                "50, 40 "
                "30)))";
        auto touch_multi(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_TRUE(multipolygon->touches(touch_multi.get()));
    }
    {
        const char* wkt =
                "MULTIPOLYGON (((8 8, 18 8, 18 18, 8 18, 8 8)), ((30 30, 40 30, 35 25, 30 "
                "30)))";
        auto overlap_multi(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(multipolygon->touches(overlap_multi.get()));
    }

    std::string buf;
    multipolygon->encode_to(&buf);
    {
        auto decoded(GeoShape::from_encoded(buf.data(), buf.size()));
        ASSERT_NE(nullptr, decoded.get());
        EXPECT_EQ(GEO_SHAPE_MULTI_POLYGON, decoded->type());

        GeoPoint point;
        point.from_coord(10, 5);
        EXPECT_TRUE(decoded->touches(&point));
    }
    {
        buf.resize(buf.size() - 2);
        auto decoded(GeoShape::from_encoded(buf.data(), buf.size()));
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
    {
        GeoPoint point;
        point.from_coord(0, 10);
        EXPECT_TRUE(circle.touches(&point));
    }
    {
        GeoCircle circle2;
        auto res = circle2.init(1, 1, 1);
        EXPECT_EQ(GEO_PARSE_OK, res);
        GeoPoint point;
        point.from_coord(2, 1);
        EXPECT_TRUE(circle2.touches(&point));
    }
    {
        GeoPoint point;
        point.from_coord(0, 10.1);
        EXPECT_FALSE(circle.touches(&point));
    }

    // ======================
    // Circle vs LineString
    // ======================
    {
        const char* wkt = "LINESTRING(-20 0, 20 0)";
        auto line(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(circle.touches(line.get()));
    }
    {
        const char* wkt = "LINESTRING(20 20, 30 30)";
        auto line(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(circle.touches(line.get()));
    }

    // ======================
    // Circle vs Polygon
    // ======================
    {
        const char* wkt = "POLYGON((-5 -5,5 -5,5 5,-5 5,-5 -5))";
        auto poly(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(circle.touches(poly.get()));
    }
    {
        const char* wkt = "POLYGON((10 0,20 0,20 10,10 10,10 0))";
        auto poly(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_TRUE(circle.touches(poly.get()));
    }
    {
        const char* wkt = "POLYGON((10.1 0,20 0,20 10,10.1 10,10.1 0))";
        auto poly(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(circle.touches(poly.get()));
    }
    {
        const char* wkt = "POLYGON((9.99 0,20 0,20 10,9.99 10,9.99 0))";
        auto poly(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_FALSE(circle.touches(poly.get()));
    }
    {
        const char* wkt = "POLYGON((-10 -10,10 -10,10 -20,-10 -20,-10 -10))";
        auto poly(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_TRUE(circle.touches(poly.get()));
    }

    // ======================
    // Circle vs Circle
    // ======================
    {
        GeoCircle circle1;
        circle1.init(1, 1, 1);
        GeoCircle circle2;
        circle2.init(3, 1, 1);
        EXPECT_TRUE(circle1.touches(&circle2));
    }
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

TEST_F(GeoTypesTest, test_geometry_type) {
    GeoParseStatus status;

    // Test GeoPoint
    {
        GeoPoint point;
        point.from_coord(116.123, 63.546);
        EXPECT_STREQ("ST_POINT", point.GeometryType().c_str());
    }

    // Test GeoLineString
    {
        const char* wkt = "LINESTRING (30 10, 10 30, 40 40)";
        auto line = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, line.get());
        EXPECT_STREQ("ST_LINESTRING", line->GeometryType().c_str());
    }

    // Test GeoPolygon
    {
        const char* wkt = "POLYGON ((10 10, 50 10, 50 50, 10 50, 10 10))";
        auto polygon = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, polygon.get());
        EXPECT_STREQ("ST_POLYGON", polygon->GeometryType().c_str());
    }

    // Test GeoMultiPolygon
    {
        const char* wkt = "MULTIPOLYGON (((0 0, 10 0, 10 10, 0 10, 0 0)))";
        auto multi_polygon = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, multi_polygon.get());
        EXPECT_STREQ("ST_MULTIPOLYGON", multi_polygon->GeometryType().c_str());
    }

    // Test GeoCircle
    {
        GeoCircle circle;
        auto res = circle.init(110.123, 64, 1000);
        EXPECT_EQ(GEO_PARSE_OK, res);
        EXPECT_STREQ("ST_CIRCLE", circle.GeometryType().c_str());
    }
}

TEST_F(GeoTypesTest, test_length) {
    GeoParseStatus status;

    // Test GeoPoint - length should be 0
    {
        GeoPoint point;
        point.from_coord(116.123, 63.546);
        EXPECT_DOUBLE_EQ(0.0, point.Length());
    }

    // Test GeoLineString - calculate length
    {
        const char* wkt = "LINESTRING (0 0, 1 0, 1 1)";
        auto line = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, line.get());
        double length = line->Length();
        EXPECT_GT(length, 100000.0);
        // Line should have two segments: (0,0)-(1,0) and (1,0)-(1,1)
        // Expected total distance is approximately 2 degrees in Earth distance
        EXPECT_LT(length, 300000.0); // Less than 300km
    }

    // Test GeoPolygon - calculate perimeter
    {
        const char* wkt = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))";
        auto polygon = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, polygon.get());
        double perimeter = polygon->Length();
        EXPECT_GT(perimeter, 400000.0);
        // Polygon is 1x1 degree square, perimeter should be roughly 4 times one degree
        EXPECT_LT(perimeter, 500000.0); // Less than 500km
    }

    // Test GeoPolygon with a hole - perimeter should include inner loop
    {
        const char* outer_wkt = "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))";
        const char* hole_wkt =
                "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0), (0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, "
                "0.5 0.5))";
        auto outer_polygon = GeoShape::from_wkt(outer_wkt, strlen(outer_wkt), status);
        auto hole_polygon = GeoShape::from_wkt(hole_wkt, strlen(hole_wkt), status);
        EXPECT_NE(nullptr, outer_polygon.get());
        EXPECT_NE(nullptr, hole_polygon.get());

        double outer_perimeter = outer_polygon->Length();
        double hole_perimeter = hole_polygon->Length();
        EXPECT_GT(hole_perimeter, outer_perimeter);
        EXPECT_LT(hole_perimeter, outer_perimeter * 2.0);
    }

    // Test GeoMultiPolygon - should have non-zero length
    {
        const char* wkt = "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)))";
        auto multi_polygon = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, multi_polygon.get());
        double length = multi_polygon->Length();
        EXPECT_GT(length, 400000.0);
        EXPECT_LT(length, 500000.0); // Less than 500km
    }

    // Test GeoCircle - length should be circumference (2 * pi * radius in meters)
    {
        GeoCircle circle;
        auto res = circle.init(0, 0, 1000); // 1000 meters radius
        EXPECT_EQ(GEO_PARSE_OK, res);
        double circumference = circle.Length();
        EXPECT_GT(circumference, 0.0);
        // Expected circumference: 2 * pi * 1000 ≈ 6283 meters
        EXPECT_GT(circumference, 6200.0);
        EXPECT_LT(circumference, 6300.0);
    }
}

TEST_F(GeoTypesTest, test_distance_point) {
    GeoParseStatus status;

    GeoPoint point1;
    point1.from_coord(0, 0);

    // ==========================
    // GeoPoint vs GeoPoint
    // ==========================
    {
        GeoPoint point2;
        point2.from_coord(0, 0);
        double dist = point1.Distance(&point2);
        EXPECT_DOUBLE_EQ(0.0, dist);
    }
    {
        GeoPoint point2;
        point2.from_coord(1, 0);
        double dist = point1.Distance(&point2);
        EXPECT_GT(dist, 0.0);
        // Distance should be roughly 111km for 1 degree latitude
        EXPECT_GT(dist, 100000.0);
        EXPECT_LT(dist, 120000.0);
    }

    // ==========================
    // GeoPoint vs GeoLineString
    // ==========================
    {
        const char* wkt = "LINESTRING (0 0, 1 0, 1 1)";
        auto line = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, line.get());
        double dist = point1.Distance(line.get());
        EXPECT_DOUBLE_EQ(0.0, dist); // Point is on the line at (0,0)
    }
    {
        const char* wkt = "LINESTRING (5 5, 6 6)";
        auto line = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, line.get());
        double dist = point1.Distance(line.get());
        EXPECT_GT(dist, 0.0);
        // Distance from (0,0) to a line at (5,5)-(6,6)
        EXPECT_GT(dist, 700000.0); // More than 700km
    }

    // ==========================
    // GeoPoint vs GeoPolygon
    // ==========================
    {
        const char* wkt = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))";
        auto polygon = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, polygon.get());
        double dist = point1.Distance(polygon.get());
        EXPECT_DOUBLE_EQ(0.0, dist); // Point is on the polygon boundary at (0,0)
    }
    {
        const char* wkt = "POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))";
        auto polygon = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, polygon.get());
        double dist = point1.Distance(polygon.get());
        EXPECT_GT(dist, 0.0);
        // Distance from (0,0) to polygon at (5,5)
        EXPECT_GT(dist, 700000.0); // More than 700km
    }

    // ==========================
    // GeoPoint vs GeoMultiPolygon
    // ==========================
    {
        const char* wkt = "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)))";
        auto multi_polygon = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, multi_polygon.get());
        double dist = point1.Distance(multi_polygon.get());
        EXPECT_DOUBLE_EQ(0.0, dist); // Point is on the multipolygon boundary
    }

    // ==========================
    // GeoPoint vs GeoCircle
    // ==========================
    {
        GeoCircle circle;
        circle.init(0, 0, 10000); // 10km radius
        double dist = point1.Distance(&circle);
        EXPECT_DOUBLE_EQ(0.0, dist); // Point is at the center of circle
    }
    {
        GeoCircle circle;
        circle.init(5, 0, 10000); // 10km radius at (5,0)
        double dist = point1.Distance(&circle);
        EXPECT_GT(dist, 0.0);
        // Point (0,0) is outside circle at (5,0)
        EXPECT_LT(dist, 600000.0); // Should be less than 600km from center minus radius
    }
}

TEST_F(GeoTypesTest, test_distance_linestring) {
    GeoParseStatus status;

    const char* base_line = "LINESTRING (0 0, 1 0, 1 1)";
    auto line1 = GeoShape::from_wkt(base_line, strlen(base_line), status);
    EXPECT_NE(nullptr, line1.get());

    // ==========================
    // GeoLineString vs GeoPoint
    // ==========================
    {
        GeoPoint point;
        point.from_coord(0, 0);
        double dist = line1->Distance(&point);
        EXPECT_DOUBLE_EQ(0.0, dist); // Point is on the line
    }

    // ==========================
    // GeoLineString vs GeoLineString
    // ==========================
    {
        const char* wkt = "LINESTRING (0 0, 1 0)";
        auto line2 = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, line2.get());
        double dist = line1->Distance(line2.get());
        EXPECT_DOUBLE_EQ(0.0, dist); // Lines intersect
    }
    {
        const char* wkt = "LINESTRING (-1 0.5, 2 0.5)";
        auto line2 = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, line2.get());
        double dist = line1->Distance(line2.get());
        EXPECT_DOUBLE_EQ(0.0, dist); // Lines cross the vertical segment at x=1
    }
    {
        const char* wkt = "LINESTRING (5 5, 6 5)";
        auto line2 = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, line2.get());
        double dist = line1->Distance(line2.get());
        EXPECT_GT(dist, 0.0);
        // Lines are separate
        EXPECT_GT(dist, 600000.0);
    }

    // ==========================
    // GeoLineString vs GeoPolygon
    // ==========================
    {
        const char* wkt = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))";
        auto polygon = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, polygon.get());
        double dist = line1->Distance(polygon.get());
        EXPECT_DOUBLE_EQ(0.0, dist); // Line is on/inside polygon
    }

    // ==========================
    // GeoLineString vs GeoMultiPolygon
    // ==========================
    {
        const char* wkt = "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)))";
        auto multi_polygon = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, multi_polygon.get());
        double dist = line1->Distance(multi_polygon.get());
        EXPECT_DOUBLE_EQ(0.0, dist); // Line is on/inside multipolygon
    }

    // ==========================
    // GeoLineString vs GeoCircle
    // ==========================
    {
        GeoCircle circle;
        circle.init(0.5, 0.5, 100000); // 100km radius
        double dist = line1->Distance(&circle);
        EXPECT_DOUBLE_EQ(0.0, dist); // Line is within/touches circle
    }
}

TEST_F(GeoTypesTest, test_distance_polygon) {
    GeoParseStatus status;

    const char* base_polygon = "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))";
    auto polygon1 = GeoShape::from_wkt(base_polygon, strlen(base_polygon), status);
    EXPECT_NE(nullptr, polygon1.get());

    // ==========================
    // GeoPolygon vs GeoPoint
    // ==========================
    {
        GeoPoint point;
        point.from_coord(0, 0);
        double dist = polygon1->Distance(&point);
        EXPECT_DOUBLE_EQ(0.0, dist); // Point is on polygon boundary
    }
    {
        GeoPoint point;
        point.from_coord(5, 5);
        double dist = polygon1->Distance(&point);
        EXPECT_GT(dist, 0.0);
        // Point is outside polygon
        EXPECT_GT(dist, 300000.0);
    }

    // ==========================
    // GeoPolygon vs GeoLineString
    // ==========================
    {
        const char* wkt = "LINESTRING (0 0, 1 1)";
        auto line = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, line.get());
        double dist = polygon1->Distance(line.get());
        EXPECT_DOUBLE_EQ(0.0, dist); // Line is within polygon
    }

    // ==========================
    // GeoPolygon vs GeoPolygon
    // ==========================
    {
        const char* wkt = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))";
        auto polygon2 = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, polygon2.get());
        double dist = polygon1->Distance(polygon2.get());
        EXPECT_DOUBLE_EQ(0.0, dist); // Polygons overlap
    }
    {
        const char* wkt = "POLYGON ((2 0, 4 0, 4 2, 2 2, 2 0))";
        auto polygon2 = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, polygon2.get());
        double dist = polygon1->Distance(polygon2.get());
        EXPECT_DOUBLE_EQ(0.0, dist); // Polygons touch at the boundary
    }
    {
        const char* wkt = "POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))";
        auto polygon2 = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, polygon2.get());
        double dist = polygon1->Distance(polygon2.get());
        EXPECT_GT(dist, 0.0);
        // Polygons are separate
        EXPECT_GT(dist, 300000.0);
    }

    // ==========================
    // GeoPolygon vs GeoMultiPolygon
    // ==========================
    {
        const char* wkt = "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)))";
        auto multi_polygon = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, multi_polygon.get());
        double dist = polygon1->Distance(multi_polygon.get());
        EXPECT_DOUBLE_EQ(0.0, dist); // MultiPolygon overlaps with Polygon
    }

    // ==========================
    // GeoPolygon vs GeoCircle
    // ==========================
    {
        GeoCircle circle;
        circle.init(1, 1, 100000); // 100km radius at (1,1)
        double dist = polygon1->Distance(&circle);
        EXPECT_DOUBLE_EQ(0.0, dist); // Circle overlaps with polygon
    }
}

TEST_F(GeoTypesTest, test_distance_multipolygon) {
    GeoParseStatus status;

    const char* base_multipolygon =
            "MULTIPOLYGON ("
            "((0 0, 2 0, 2 2, 0 2, 0 0)),"
            "((5 5, 7 5, 7 7, 5 7, 5 5))"
            ")";
    auto multipolygon1 = GeoShape::from_wkt(base_multipolygon, strlen(base_multipolygon), status);
    EXPECT_NE(nullptr, multipolygon1.get());

    // ==========================
    // GeoMultiPolygon vs GeoPoint
    // ==========================
    {
        GeoPoint point;
        point.from_coord(0, 0);
        double dist = multipolygon1->Distance(&point);
        EXPECT_DOUBLE_EQ(0.0, dist); // Point is on multipolygon boundary
    }
    {
        GeoPoint point;
        point.from_coord(10, 10);
        double dist = multipolygon1->Distance(&point);
        EXPECT_GT(dist, 0.0);
        // Point is outside all polygons, distance to nearest polygon at (5,5)-(7,7) is ~469km
        EXPECT_GT(dist, 400000.0);
        EXPECT_LT(dist, 500000.0);
    }

    // ==========================
    // GeoMultiPolygon vs GeoLineString
    // ==========================
    {
        const char* wkt = "LINESTRING (0 0, 1 1)";
        auto line = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, line.get());
        double dist = multipolygon1->Distance(line.get());
        EXPECT_DOUBLE_EQ(0.0, dist); // Line is within first polygon
    }

    // ==========================
    // GeoMultiPolygon vs GeoPolygon
    // ==========================
    {
        const char* wkt = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))";
        auto polygon = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, polygon.get());
        double dist = multipolygon1->Distance(polygon.get());
        EXPECT_DOUBLE_EQ(0.0, dist); // Polygon overlaps with first multipolygon
    }
    {
        const char* wkt = "POLYGON ((10 10, 12 10, 12 12, 10 12, 10 10))";
        auto polygon = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, polygon.get());
        double dist = multipolygon1->Distance(polygon.get());
        EXPECT_GT(dist, 0.0);
        // Polygon is separate from multipolygon, distance to nearest polygon at (5,5)-(7,7) is ~469km
        EXPECT_GT(dist, 400000.0);
        EXPECT_LT(dist, 500000.0);
    }

    // ==========================
    // GeoMultiPolygon vs GeoMultiPolygon
    // ==========================
    {
        const char* wkt = "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)))";
        auto multipolygon2 = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, multipolygon2.get());
        double dist = multipolygon1->Distance(multipolygon2.get());
        EXPECT_DOUBLE_EQ(0.0, dist); // Overlapping multipolygons
    }

    // ==========================
    // GeoMultiPolygon vs GeoCircle
    // ==========================
    {
        GeoCircle circle;
        circle.init(1, 1, 100000); // 100km radius
        double dist = multipolygon1->Distance(&circle);
        EXPECT_DOUBLE_EQ(0.0, dist); // Circle overlaps with first polygon
    }
}

TEST_F(GeoTypesTest, test_distance_circle) {
    GeoParseStatus status;

    GeoCircle circle1;
    circle1.init(0, 0, 10000); // 10km radius

    // ==========================
    // GeoCircle vs GeoPoint
    // ==========================
    {
        GeoPoint point;
        point.from_coord(0, 0);
        double dist = circle1.Distance(&point);
        EXPECT_DOUBLE_EQ(0.0, dist); // Point is at center of circle
    }
    {
        GeoPoint point;
        point.from_coord(1, 0);
        double dist = circle1.Distance(&point);
        EXPECT_GT(dist, 0.0);
        // Point is outside circle
        EXPECT_LT(dist, 200000.0); // Less than 200km
    }

    // ==========================
    // GeoCircle vs GeoLineString
    // ==========================
    {
        const char* wkt = "LINESTRING (0 0, 0 1)";
        auto line = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, line.get());
        double dist = circle1.Distance(line.get());
        EXPECT_DOUBLE_EQ(0.0, dist); // Line is within circle
    }

    // ==========================
    // GeoCircle vs GeoPolygon
    // ==========================
    {
        const char* wkt = "POLYGON ((0 0, 0.1 0, 0.1 0.1, 0 0.1, 0 0))";
        auto polygon = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, polygon.get());
        double dist = circle1.Distance(polygon.get());
        EXPECT_DOUBLE_EQ(0.0, dist); // Polygon is within circle
    }

    // ==========================
    // GeoCircle vs GeoMultiPolygon
    // ==========================
    {
        const char* wkt = "MULTIPOLYGON (((0 0, 0.1 0, 0.1 0.1, 0 0.1, 0 0)))";
        auto multi_polygon = GeoShape::from_wkt(wkt, strlen(wkt), status);
        EXPECT_NE(nullptr, multi_polygon.get());
        double dist = circle1.Distance(multi_polygon.get());
        EXPECT_DOUBLE_EQ(0.0, dist); // MultiPolygon is within circle
    }

    // ==========================
    // GeoCircle vs GeoCircle
    // ==========================
    {
        GeoCircle circle2;
        circle2.init(0, 0, 5000); // 5km radius at same center
        double dist = circle1.Distance(&circle2);
        EXPECT_DOUBLE_EQ(0.0, dist); // Circles overlap
    }
    {
        GeoCircle circle2;
        circle2.init(10, 0, 5000); // 5km radius at (10,0)
        double dist = circle1.Distance(&circle2);
        EXPECT_GT(dist, 0.0);
        // Circles are separate, distance between centers ~1110km minus radii (10+5km) = ~1095km
        EXPECT_GT(dist, 1000000.0);
        EXPECT_LT(dist, 1100000.0);
    }
}

TEST_F(GeoTypesTest, polygon_contains) {
    GeoParseStatus status;
    const char* wkt = "POLYGON ((10 10, 50 10, 50 10, 50 50, 50 50, 10 50, 10 10))";
    auto polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
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
        auto shape(GeoShape::from_encoded(buf.data(), buf.size()));
        EXPECT_EQ(GEO_SHAPE_POLYGON, shape->type());
        LOG(INFO) << "polygon=" << shape->as_wkt();
    }

    {
        buf.resize(buf.size() - 1);
        auto shape(GeoShape::from_encoded(buf.data(), buf.size()));
        EXPECT_EQ(nullptr, shape);
    }
}

TEST_F(GeoTypesTest, polygon_parse_fail) {
    {
        const char* wkt = "POLYGON ((10 10, 50 10, 50 50, 10 50), (10 10 01))";
        GeoParseStatus status;
        auto polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_EQ(GEO_PARSE_WKT_SYNTAX_ERROR, status);
        EXPECT_EQ(nullptr, polygon.get());
    }
    {
        const char* wkt = "POLYGON ((10 10, 50 10, 50 50, 10 50))";
        GeoParseStatus status;
        auto polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_EQ(GEO_PARSE_LOOP_NOT_CLOSED, status);
        EXPECT_EQ(nullptr, polygon.get());
    }
    {
        const char* wkt = "POLYGON ((10 10, 50 10, 10 10))";
        GeoParseStatus status;
        auto polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_EQ(GEO_PARSE_LOOP_LACK_VERTICES, status);
        EXPECT_EQ(nullptr, polygon.get());
    }
}

TEST_F(GeoTypesTest, polygon_hole_contains) {
    const char* wkt =
            "POLYGON ((10 10, 50 10, 50 50, 10 50, 10 10), (20 20, 40 20, 40 40, 20 40, 20 "
            "20))";
    GeoParseStatus status;
    auto polygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
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
        EXPECT_FALSE(res);
    }
}

TEST_F(GeoTypesTest, multipolygon_parse_fail) {
    {
        const char* wkt = "MULTIPOLYGON (((10 10, 50 10, 50 50, 10 50), (10 10 01)))";
        GeoParseStatus status;
        auto multipolygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_EQ(GEO_PARSE_WKT_SYNTAX_ERROR, status);
        EXPECT_EQ(nullptr, multipolygon.get());
    }
    {
        const char* wkt = "MULTIPOLYGON (((10 10, 50 10, 50 50, 10 50)))";
        GeoParseStatus status;
        auto multipolygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_EQ(GEO_PARSE_LOOP_NOT_CLOSED, status);
        EXPECT_EQ(nullptr, multipolygon.get());
    }
    {
        const char* wkt = "MULTIPOLYGON (((10 10, 50 10, 10 10)))";
        GeoParseStatus status;
        auto multipolygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_EQ(GEO_PARSE_LOOP_LACK_VERTICES, status);
        EXPECT_EQ(nullptr, multipolygon.get());
    }
    {
        const char* wkt =
                "MULTIPOLYGON (((0 0, 0 10, 10 10, 10 0, 0 0)), ((5 5, 5 15, 15 15, 15 5, "
                "5 "
                "5)))";
        GeoParseStatus status;
        auto multipolygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_EQ(GEO_PARSE_MULTIPOLYGON_OVERLAP, status);
        EXPECT_EQ(nullptr, multipolygon.get());
    }
    {
        const char* wkt =
                "MULTIPOLYGON(((5 5, 5 8, 8 8, 8 5, 5 5)), ((8 6, 10 6, 10 10, 8 10, 8 "
                "6)))";
        GeoParseStatus status;
        auto multipolygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_EQ(GEO_PARSE_MULTIPOLYGON_OVERLAP, status);
        EXPECT_EQ(nullptr, multipolygon.get());
    }
    {
        const char* wkt = "MULTIPOLYGON((()))";
        GeoParseStatus status;
        auto multipolygon(GeoShape::from_wkt(wkt, strlen(wkt), status));
        EXPECT_EQ(GEO_PARSE_WKT_SYNTAX_ERROR, status);
        EXPECT_EQ(nullptr, multipolygon.get());
    }
}

TEST_F(GeoTypesTest, circle) {
    GeoCircle circle;
    auto res = circle.init(110.123, 64, 1000);
    EXPECT_EQ(GEO_PARSE_OK, res);

    std::string buf;
    circle.encode_to(&buf);

    {
        auto circle2(GeoShape::from_encoded(buf.data(), buf.size()));
        EXPECT_STREQ("CIRCLE ((110.123 64), 1000)", circle2->as_wkt().c_str());
    }

    {
        buf.resize(buf.size() - 1);
        auto circle2(GeoShape::from_encoded(buf.data(), buf.size()));
        EXPECT_EQ(nullptr, circle2);
    }
}

} // namespace doris
