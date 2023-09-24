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
#include "http/ev_http_server.h"
#include "http/http_client.h"
#include "runtime/exec_env.h"
#include "service/http_service.h"

namespace doris {

#define DBUG_POINT_POST(uri)                                                                  \
    {                                                                                         \
        std::string response;                                                                 \
        HttpClient client;                                                                    \
        auto status = client.init(host + uri);                                                \
        ASSERT_TRUE(status.ok()) << "uri=" << (host + uri) << ", err=" << status.to_string(); \
        status = client.execute_post_request("{}", &response);                                \
        ASSERT_TRUE(status.ok()) << status.to_string();                                       \
    }

TEST(DebugPointsTest, BaseTest) {
    auto service = std::make_unique<HttpService>(doris::ExecEnv::GetInstance(), 0, 1);
    service->start();
    int real_port = service->get_real_port();
    ASSERT_NE(0, real_port);
    std::string host = "http://127.0.0.1:" + std::to_string(real_port);

    config::enable_debug_points = true;
    DebugPoints::instance()->clear();

    EXPECT_FALSE(DebugPoints::instance()->is_enable("dbug1"));
    DBUG_POINT_POST("/api/debug_point/add/dbug1");
    EXPECT_TRUE(DebugPoints::instance()->is_enable("dbug1"));
    DBUG_POINT_POST("/api/debug_point/remove/dbug1");
    EXPECT_FALSE(DebugPoints::instance()->is_enable("dbug1"));

    DBUG_POINT_POST("/api/debug_point/add/dbug2");
    EXPECT_TRUE(DebugPoints::instance()->is_enable("dbug2"));
    DBUG_POINT_POST("/api/debug_point/clear");
    EXPECT_FALSE(DebugPoints::instance()->is_enable("dbug2"));

    DBUG_POINT_POST("/api/debug_point/add/dbug3?execute=3");
    for (int i = 0; i < 3; i++) {
        EXPECT_TRUE(DebugPoints::instance()->is_enable("dbug3"));
    }
    EXPECT_FALSE(DebugPoints::instance()->is_enable("dbug3"));

    DBUG_POINT_POST("/api/debug_point/add/dbug4?timeout=1");
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_TRUE(DebugPoints::instance()->is_enable("dbug4"));
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    EXPECT_FALSE(DebugPoints::instance()->is_enable("dbug4"));
}

#undef DBUG_POINT_POST

} // namespace doris
