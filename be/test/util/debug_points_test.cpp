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
}

} // namespace doris
