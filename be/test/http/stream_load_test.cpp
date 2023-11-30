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

#include "http/action/stream_load.h"

#include <gtest/gtest.h>

#include "common/config.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_common.h"
#include "http/http_handler.h"
#include "http/http_handler_with_auth.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/utils.h"
#include "olap/wal_manager.h"

namespace doris {

ExecEnv* _env = nullptr;

class StreamLoadTest : public testing::Test {
public:
    StreamLoadTest() {}
    virtual ~StreamLoadTest() {}
    void SetUp() override {
        // 1G
        WalManager::wal_limit = 1073741824;
    }
    void TearDown() override {}
};

TEST_F(StreamLoadTest, TestHeader) {
    // 1. empty info
    {
        StreamLoadAction stream_load_action(_env);
        auto evhttp_req = evhttp_request_new(nullptr, nullptr);
        HttpRequest req(evhttp_req);
        EXPECT_EQ(load_size_smaller_than_wal_limit(&req), false);
        EXPECT_EQ(stream_load_action.on_header(&req), -1);
    }

    // 2. chunked stream load whih group commit
    {
        StreamLoadAction stream_load_action(_env);
        auto evhttp_req = evhttp_request_new(nullptr, nullptr);
        HttpRequest req(evhttp_req);
        req.add_output_header(HTTP_GROUP_COMMIT, "true");
        EXPECT_EQ(load_size_smaller_than_wal_limit(&req), false);
        EXPECT_EQ(stream_load_action.on_header(&req), -1);
    }

    // 3. small stream load whih group commit
    {
        StreamLoadAction stream_load_action(_env);
        auto evhttp_req = evhttp_request_new(nullptr, nullptr);
        HttpRequest req(evhttp_req);
        req.add_output_header(HTTP_GROUP_COMMIT, "true");
        req.add_output_header(HttpHeaders::CONTENT_LENGTH, "1000");
        EXPECT_EQ(load_size_smaller_than_wal_limit(&req), true);
        EXPECT_EQ(stream_load_action.on_header(&req), -1);
    }

    // 4. lager stream load whih group commit
    {
        StreamLoadAction stream_load_action(_env);
        auto evhttp_req = evhttp_request_new(nullptr, nullptr);
        HttpRequest req(evhttp_req);
        req.add_output_header(HTTP_GROUP_COMMIT, "true");
        req.add_output_header(HttpHeaders::CONTENT_LENGTH, "1073741824");
        EXPECT_EQ(load_size_smaller_than_wal_limit(&req), true);
        EXPECT_EQ(stream_load_action.on_header(&req), -1);
    }
}
} // namespace doris