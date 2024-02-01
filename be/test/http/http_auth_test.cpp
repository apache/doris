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

#include <gtest/gtest.h>

#include "common/config.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_handler_with_auth.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/utils.h"
#include "util/defer_op.h"

namespace doris {

class HttpAuthTestHandler : public HttpHandlerWithAuth {
public:
    HttpAuthTestHandler(ExecEnv* exec_env, TPrivilegeHier::type hier, TPrivilegeType::type type)
            : HttpHandlerWithAuth(exec_env, hier, type) {}

    ~HttpAuthTestHandler() override = default;

    void handle(HttpRequest* req) override {}

private:
    bool on_privilege(const HttpRequest& req, TCheckAuthRequest& auth_request) override {
        return !req.param("table").empty();
    };
};

static HttpAuthTestHandler s_auth_handler =
        HttpAuthTestHandler(nullptr, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN);

class HttpAuthTest : public testing::Test {};

TEST_F(HttpAuthTest, disable_auth) {
    EXPECT_FALSE(config::enable_all_http_auth);

    auto evhttp_req = evhttp_request_new(nullptr, nullptr);
    HttpRequest req(evhttp_req);
    EXPECT_EQ(s_auth_handler.on_header(&req), 0);
    evhttp_request_free(evhttp_req);
}

TEST_F(HttpAuthTest, enable_all_http_auth) {
    Defer defer {[]() { config::enable_all_http_auth = false; }};
    config::enable_all_http_auth = true;

    // 1. empty auth info
    {
        auto evhttp_req = evhttp_request_new(nullptr, nullptr);
        HttpRequest req1(evhttp_req);
        EXPECT_EQ(s_auth_handler.on_header(&req1), -1);
    }

    // 2. empty param
    {
        auto evhttp_req = evhttp_request_new(nullptr, nullptr);
        HttpRequest req2(evhttp_req);
        auto auth = encode_basic_auth("doris", "passwd");
        req2._headers.emplace(HttpHeaders::AUTHORIZATION, auth);
        EXPECT_EQ(s_auth_handler.on_header(&req2), -1);
    }

    // 3. OK
    {
        auto evhttp_req = evhttp_request_new(nullptr, nullptr);
        HttpRequest req3(evhttp_req);
        auto auth = encode_basic_auth("doris", "passwd");
        req3._headers.emplace(HttpHeaders::AUTHORIZATION, auth);
        req3._params.emplace("table", "T");
        EXPECT_EQ(s_auth_handler.on_header(&req3), 0);
        evhttp_request_free(evhttp_req);
    }
}

} // namespace doris
