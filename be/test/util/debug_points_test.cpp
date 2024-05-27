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

#include "util/debug_points.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <chrono>
#include <thread>

#include "gtest/gtest_pred_impl.h"
#include "testutil/http_utils.h"

namespace doris {

TEST(DebugPointsTest, BaseTest) {
    config::enable_debug_points = true;
    DebugPoints::instance()->clear();

    EXPECT_FALSE(DebugPoints::instance()->is_enable("dbug1"));
    POST_HTTP_TO_TEST_SERVER("/api/debug_point/add/dbug1");
    EXPECT_TRUE(DebugPoints::instance()->is_enable("dbug1"));
    POST_HTTP_TO_TEST_SERVER("/api/debug_point/remove/dbug1");
    EXPECT_FALSE(DebugPoints::instance()->is_enable("dbug1"));

    POST_HTTP_TO_TEST_SERVER("/api/debug_point/add/dbug2");
    EXPECT_TRUE(DebugPoints::instance()->is_enable("dbug2"));
    POST_HTTP_TO_TEST_SERVER("/api/debug_point/clear");
    EXPECT_FALSE(DebugPoints::instance()->is_enable("dbug2"));

    POST_HTTP_TO_TEST_SERVER("/api/debug_point/add/dbug3?execute=3");
    for (int i = 0; i < 3; i++) {
        EXPECT_TRUE(DebugPoints::instance()->is_enable("dbug3"));
    }
    EXPECT_FALSE(DebugPoints::instance()->is_enable("dbug3"));

    POST_HTTP_TO_TEST_SERVER("/api/debug_point/add/dbug4?timeout=1");
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_TRUE(DebugPoints::instance()->is_enable("dbug4"));
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    EXPECT_FALSE(DebugPoints::instance()->is_enable("dbug4"));

    POST_HTTP_TO_TEST_SERVER("/api/debug_point/add/dbug5?v1=1&v2=a&v3=1.2&v4=true&v5=false");
    EXPECT_TRUE(DebugPoints::instance()->is_enable("dbug5"));
    DBUG_EXECUTE_IF("dbug5", EXPECT_EQ(1, dp->param<int>("v1", 100)));
    DBUG_EXECUTE_IF("dbug5", EXPECT_EQ("a", dp->param<std::string>("v2")));
    DBUG_EXECUTE_IF("dbug5", EXPECT_EQ("a", dp->param("v2", std::string())));
    DBUG_EXECUTE_IF("dbug5", EXPECT_STREQ("a", dp->param("v2", "b")));
    DBUG_EXECUTE_IF("dbug5", EXPECT_EQ(1.2, dp->param<double>("v3")));
    DBUG_EXECUTE_IF("dbug5", EXPECT_EQ(1.2, dp->param("v3", 0.0)));
    DBUG_EXECUTE_IF("dbug5", EXPECT_TRUE(dp->param("v4", false)));
    DBUG_EXECUTE_IF("dbug5", EXPECT_FALSE(dp->param("v5", false)));
    DBUG_EXECUTE_IF("dbug5", EXPECT_EQ(0L, dp->param<int64_t>("v_not_exist")));
    DBUG_EXECUTE_IF("dbug5", EXPECT_EQ(0L, dp->param("v_not_exist", 0L)));
    DBUG_EXECUTE_IF("dbug5", EXPECT_EQ(123, dp->param("v_not_exist", 123)));
    DBUG_EXECUTE_IF("dbug5", EXPECT_STREQ("abcd", dp->param("v_not_exist", "abcd")));

    EXPECT_EQ(1.2, DebugPoints::instance()->get_debug_param_or_default("dbug5", "v3", 0.0));
    EXPECT_EQ(100,
              DebugPoints::instance()->get_debug_param_or_default("point_not_exists", "k", 100));

    POST_HTTP_TO_TEST_SERVER("/api/debug_point/add/dbug6?value=567");
    EXPECT_EQ(567, DebugPoints::instance()->get_debug_param_or_default("dbug6", 0));
}

TEST(DebugPointsTest, AddTest) {
    config::enable_debug_points = true;
    DebugPoints::instance()->clear();

    DebugPoints::instance()->add("dbug1");
    EXPECT_TRUE(DebugPoints::instance()->is_enable("dbug1"));

    DebugPoints::instance()->add_with_params("dbug2", {{"k1", "100"}});
    EXPECT_EQ(100, DebugPoints::instance()->get_debug_param_or_default("dbug2", "k1", 0));

    DebugPoints::instance()->add_with_value("dbug3", 567);
    EXPECT_EQ(567, DebugPoints::instance()->get_debug_param_or_default("dbug3", 567));

    DebugPoints::instance()->add_with_value("dbug4", "hello");
    EXPECT_EQ("hello",
              DebugPoints::instance()->get_debug_param_or_default<std::string>("dbug4", ""));
}

void demo_callback() {
    int a = 0;

    DBUG_EXECUTE_IF("set_a", DBUG_RUN_CALLBACK(&a));
    DBUG_EXECUTE_IF("get_a", DBUG_RUN_CALLBACK(a));
}

TEST(DebugPointsTest, Callback) {
    config::enable_debug_points = true;
    DebugPoints::instance()->clear();

    int got_a = 0;

    std::function<void(int*)> set_handler = [](int* a) { *a = 1000; };
    std::function<void(int)> get_handler = [&got_a](int a) { got_a = a; };
    DebugPoints::instance()->add_with_callback("set_a", set_handler);
    DebugPoints::instance()->add_with_callback("get_a", get_handler);

    demo_callback();

    EXPECT_EQ(1000, got_a);
}

} // namespace doris
