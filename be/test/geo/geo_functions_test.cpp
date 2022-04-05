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

#include "geo/geo_functions.h"

#include <gtest/gtest.h>
#include <s2/s2polygon.h>

#include <vector>

#include "common/logging.h"
#include "geo/geo_types.h"
#include "geo/wkt_parse.h"
#include "geo/wkt_parse_ctx.h"
#include "testutil/function_utils.h"
#include "udf/udf.h"
#include "udf/udf_internal.h"

namespace doris {

class GeoFunctionsTest : public testing::Test {
public:
    GeoFunctionsTest() {}
    virtual ~GeoFunctionsTest() {}
};

TEST_F(GeoFunctionsTest, st_dist_sphere) {
    FunctionUtils utils;
    FunctionContext* ctx = utils.get_fn_ctx();
    {
        DoubleVal x_lng(0.0);
        DoubleVal x_lat(0.0);
        DoubleVal y_lng(0.0);
        DoubleVal y_lat(0.0);

        auto dist = GeoFunctions::st_distance_sphere(ctx, x_lng, x_lat, y_lng, y_lat);
        ASSERT_EQ(0, dist.val);
    }
    {
        DoubleVal x_lng(0.0);
        DoubleVal x_lat(0.0);
        DoubleVal y_lng(0.0);
        DoubleVal y_lat(1.0);

        auto dist = GeoFunctions::st_distance_sphere(ctx, x_lng, x_lat, y_lng, y_lat);
        LOG(INFO) << dist.val;
    }
}

TEST_F(GeoFunctionsTest, st_point) {
    FunctionUtils utils;
    FunctionContext* ctx = utils.get_fn_ctx();
    DoubleVal lng(113);
    DoubleVal lat(64);

    auto str = GeoFunctions::st_point(ctx, lng, lat);
    ASSERT_FALSE(str.is_null);

    GeoPoint point;
    auto res = point.decode_from(str.ptr, str.len);
    ASSERT_TRUE(res);
    ASSERT_EQ(113, point.x());
    ASSERT_EQ(64, point.y());
}

TEST_F(GeoFunctionsTest, st_x_y) {
    FunctionUtils utils;
    FunctionContext* ctx = utils.get_fn_ctx();

    GeoPoint point;
    point.from_coord(134, 63);

    std::string buf;
    point.encode_to(&buf);

    auto x = GeoFunctions::st_x(ctx, StringVal((uint8_t*)buf.data(), buf.size()));
    auto y = GeoFunctions::st_y(ctx, StringVal((uint8_t*)buf.data(), buf.size()));
    ASSERT_EQ(134, x.val);
    ASSERT_EQ(63, y.val);
}

TEST_F(GeoFunctionsTest, as_wkt) {
    FunctionUtils utils;
    FunctionContext* ctx = utils.get_fn_ctx();

    GeoPoint point;
    point.from_coord(134, 63);

    std::string buf;
    point.encode_to(&buf);

    auto wkt = GeoFunctions::st_as_wkt(ctx, StringVal((uint8_t*)buf.data(), buf.size()));
    ASSERT_STREQ("POINT (134 63)", std::string((char*)wkt.ptr, wkt.len).c_str());
}

TEST_F(GeoFunctionsTest, st_from_wkt) {
    FunctionUtils utils;
    FunctionContext* ctx = utils.get_fn_ctx();

    GeoFunctions::st_from_wkt_prepare(ctx, FunctionContext::FRAGMENT_LOCAL);
    ASSERT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    std::string wkt = "POINT (10.1 20.2)";
    auto str = GeoFunctions::st_from_wkt(ctx, StringVal((uint8_t*)wkt.data(), wkt.size()));
    ASSERT_FALSE(str.is_null);
    GeoFunctions::st_from_wkt_close(ctx, FunctionContext::FRAGMENT_LOCAL);

    // second time
    {
        StringVal wkt_val((uint8_t*)wkt.data(), wkt.size());
        // push const value
        std::vector<doris_udf::AnyVal*> const_vals;
        const_vals.push_back(&wkt_val);
        ctx->impl()->set_constant_args(const_vals);

        // prepare
        GeoFunctions::st_from_wkt_prepare(ctx, FunctionContext::FRAGMENT_LOCAL);
        ASSERT_NE(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

        // convert
        auto str2 = GeoFunctions::st_from_wkt(ctx, StringVal((uint8_t*)wkt.data(), wkt.size()));
        ASSERT_FALSE(str2.is_null);

        // close
        GeoPoint point;
        auto res = point.decode_from(str2.ptr, str2.len);
        ASSERT_TRUE(res);
        ASSERT_DOUBLE_EQ(10.1, point.x());
        ASSERT_DOUBLE_EQ(20.2, point.y());
        GeoFunctions::st_from_wkt_close(ctx, FunctionContext::FRAGMENT_LOCAL);
    }
}

TEST_F(GeoFunctionsTest, st_line) {
    FunctionUtils utils;
    FunctionContext* ctx = utils.get_fn_ctx();

    GeoFunctions::st_from_wkt_prepare(ctx, FunctionContext::FRAGMENT_LOCAL);

    std::string wkt = "LINESTRING (10.1 20.2, 21.1 30.1)";
    auto str = GeoFunctions::st_from_wkt(ctx, StringVal((uint8_t*)wkt.data(), wkt.size()));
    ASSERT_FALSE(str.is_null);

    GeoLine line;
    auto res = line.decode_from(str.ptr, str.len);
    ASSERT_TRUE(res);
    GeoFunctions::st_from_wkt_close(ctx, FunctionContext::FRAGMENT_LOCAL);

    // second time
    {
        StringVal wkt_val((uint8_t*)wkt.data(), wkt.size());
        // push const value
        std::vector<doris_udf::AnyVal*> const_vals;
        const_vals.push_back(&wkt_val);
        ctx->impl()->set_constant_args(const_vals);

        // prepare
        GeoFunctions::st_line_prepare(ctx, FunctionContext::FRAGMENT_LOCAL);
        ASSERT_NE(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

        // convert
        auto str2 = GeoFunctions::st_line(ctx, StringVal((uint8_t*)wkt.data(), wkt.size()));
        ASSERT_FALSE(str2.is_null);

        // close
        GeoLine line;
        auto res = line.decode_from(str2.ptr, str2.len);
        ASSERT_TRUE(res);
        GeoFunctions::st_from_wkt_close(ctx, FunctionContext::FRAGMENT_LOCAL);
    }
}

TEST_F(GeoFunctionsTest, st_polygon) {
    FunctionUtils utils;
    FunctionContext* ctx = utils.get_fn_ctx();

    GeoFunctions::st_from_wkt_prepare(ctx, FunctionContext::FRAGMENT_LOCAL);

    std::string wkt = "POLYGON ((10 10, 50 10, 50 50, 10 50, 10 10))";
    auto str = GeoFunctions::st_from_wkt(ctx, StringVal((uint8_t*)wkt.data(), wkt.size()));
    ASSERT_FALSE(str.is_null);

    // second time
    {
        StringVal wkt_val((uint8_t*)wkt.data(), wkt.size());
        // push const value
        std::vector<doris_udf::AnyVal*> const_vals;
        const_vals.push_back(&wkt_val);
        ctx->impl()->set_constant_args(const_vals);

        // prepare
        GeoFunctions::st_polygon_prepare(ctx, FunctionContext::FRAGMENT_LOCAL);
        ASSERT_NE(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

        // convert
        auto str2 = GeoFunctions::st_polygon(ctx, StringVal((uint8_t*)wkt.data(), wkt.size()));
        ASSERT_FALSE(str2.is_null);

        // close
        GeoPolygon polygon;
        auto res = polygon.decode_from(str2.ptr, str2.len);
        ASSERT_TRUE(res);
        GeoFunctions::st_from_wkt_close(ctx, FunctionContext::FRAGMENT_LOCAL);
    }
}

TEST_F(GeoFunctionsTest, st_circle) {
    FunctionUtils utils;
    FunctionContext* ctx = utils.get_fn_ctx();

    GeoFunctions::st_from_wkt_prepare(ctx, FunctionContext::FRAGMENT_LOCAL);

    DoubleVal lng(111);
    DoubleVal lat(64);
    DoubleVal radius_meter(10 * 100);
    auto str = GeoFunctions::st_circle(ctx, lng, lat, radius_meter);
    ASSERT_FALSE(str.is_null);

    // second time
    {
        // push const value
        std::vector<doris_udf::AnyVal*> const_vals;
        const_vals.push_back(&lng);
        const_vals.push_back(&lat);
        const_vals.push_back(&radius_meter);
        ctx->impl()->set_constant_args(const_vals);

        // prepare
        GeoFunctions::st_circle_prepare(ctx, FunctionContext::FRAGMENT_LOCAL);
        ASSERT_NE(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

        // convert
        auto str2 = GeoFunctions::st_circle(ctx, lng, lat, radius_meter);
        ASSERT_FALSE(str2.is_null);

        // close
        GeoCircle circle;
        auto res = circle.decode_from(str2.ptr, str2.len);
        ASSERT_TRUE(res);
        GeoFunctions::st_from_wkt_close(ctx, FunctionContext::FRAGMENT_LOCAL);
    }
}

TEST_F(GeoFunctionsTest, st_poly_line_fail) {
    FunctionUtils utils;
    FunctionContext* ctx = utils.get_fn_ctx();

    {
        GeoFunctions::st_polygon_prepare(ctx, FunctionContext::FRAGMENT_LOCAL);

        std::string wkt = "POINT (10.1 20.2)";
        auto str = GeoFunctions::st_polygon(ctx, StringVal((uint8_t*)wkt.data(), wkt.size()));
        ASSERT_TRUE(str.is_null);
        GeoFunctions::st_from_wkt_close(ctx, FunctionContext::FRAGMENT_LOCAL);
    }
    {
        GeoFunctions::st_line_prepare(ctx, FunctionContext::FRAGMENT_LOCAL);

        std::string wkt = "POINT (10.1 20.2)";
        auto str = GeoFunctions::st_line(ctx, StringVal((uint8_t*)wkt.data(), wkt.size()));
        ASSERT_TRUE(str.is_null);
        GeoFunctions::st_from_wkt_close(ctx, FunctionContext::FRAGMENT_LOCAL);
    }
}

TEST_F(GeoFunctionsTest, st_contains) {
    FunctionUtils utils;
    FunctionContext* ctx = utils.get_fn_ctx();

    ASSERT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    std::string polygon_wkt = "POLYGON ((10 10, 50 10, 50 50, 10 50, 10 10))";
    auto polygon = GeoFunctions::st_from_wkt(
            ctx, StringVal((uint8_t*)polygon_wkt.data(), polygon_wkt.size()));
    ASSERT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    std::string point_wkt = "POINT (25 25)";
    auto point =
            GeoFunctions::st_from_wkt(ctx, StringVal((uint8_t*)point_wkt.data(), point_wkt.size()));
    ASSERT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    GeoFunctions::st_contains_prepare(ctx, FunctionContext::FRAGMENT_LOCAL);
    ASSERT_EQ(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    auto res = GeoFunctions::st_contains(ctx, polygon, point);
    ASSERT_TRUE(res.val);
    GeoFunctions::st_contains_close(ctx, FunctionContext::FRAGMENT_LOCAL);
}

TEST_F(GeoFunctionsTest, st_contains_cached) {
    FunctionUtils utils;
    FunctionContext* ctx = utils.get_fn_ctx();

    GeoFunctions::st_from_wkt_prepare(ctx, FunctionContext::FRAGMENT_LOCAL);

    std::string polygon_wkt = "POLYGON ((10 10, 50 10, 50 50, 10 50, 10 10))";
    auto polygon = GeoFunctions::st_from_wkt(
            ctx, StringVal((uint8_t*)polygon_wkt.data(), polygon_wkt.size()));
    std::string point_wkt = "POINT (25 25)";
    auto point =
            GeoFunctions::st_from_wkt(ctx, StringVal((uint8_t*)point_wkt.data(), point_wkt.size()));

    // push const value
    std::vector<doris_udf::AnyVal*> const_vals;
    const_vals.push_back(&polygon);
    const_vals.push_back(&point);
    ctx->impl()->set_constant_args(const_vals);

    // prepare
    GeoFunctions::st_contains_prepare(ctx, FunctionContext::FRAGMENT_LOCAL);
    ASSERT_NE(nullptr, ctx->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    auto res = GeoFunctions::st_contains(ctx, polygon, point);
    ASSERT_TRUE(res.val);
    GeoFunctions::st_contains_close(ctx, FunctionContext::FRAGMENT_LOCAL);
}

} // namespace doris

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    FLAGS_s2debug = false;
    return RUN_ALL_TESTS();
}
