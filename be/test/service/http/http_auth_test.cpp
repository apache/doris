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
#include "service/http/ev_http_server.h"
#include "service/http/http_channel.h"
#include "service/http/http_handler.h"
#include "service/http/http_handler_with_auth.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/utils.h"
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

    // 1. empty auth info - on_header returns -1 and sends UNAUTHORIZED reply
    // Note: when on_header returns -1, send_reply has been called internally,
    // so we should NOT call evhttp_request_free (would cause double-free)
    {
        auto evhttp_req = evhttp_request_new(nullptr, nullptr);
        HttpRequest req1(evhttp_req);
        EXPECT_EQ(s_auth_handler.on_header(&req1), -1);
    }

    // 2. empty param - on_header returns -1 and sends BAD_REQUEST reply
    {
        auto evhttp_req = evhttp_request_new(nullptr, nullptr);
        HttpRequest req2(evhttp_req);
        req2._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        EXPECT_EQ(s_auth_handler.on_header(&req2), -1);
    }

    // 3. OK - on_header returns 0 without calling send_reply
    {
        auto evhttp_req = evhttp_request_new(nullptr, nullptr);
        HttpRequest req3(evhttp_req);
        req3._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        req3._params.emplace("table", "T");
        EXPECT_EQ(s_auth_handler.on_header(&req3), 0);
        evhttp_request_free(evhttp_req);
    }
}

// Test NONE privilege type - when enable_all_http_auth=true, even NONE type requires auth
TEST_F(HttpAuthTest, privilege_type_none) {
    Defer defer {[]() { config::enable_all_http_auth = false; }};
    config::enable_all_http_auth = true;

    // Create handler with NONE privilege type (like HealthAction)
    HttpAuthTestHandler none_handler(nullptr, TPrivilegeHier::GLOBAL, TPrivilegeType::NONE);

    // When enable_all_http_auth=true, even NONE type requires authentication
    // (enable_all_http_auth means "enable authentication for ALL HTTP APIs")
    // Without auth header, should fail with -1 (UNAUTHORIZED reply sent)
    {
        auto evhttp_req = evhttp_request_new(nullptr, nullptr);
        HttpRequest req(evhttp_req);
        // No authorization header - should fail
        EXPECT_EQ(none_handler.on_header(&req), -1);
        // Note: when on_header returns -1, send_reply has been called internally,
        // so we should NOT call evhttp_request_free (would cause double-free)
    }

    // With valid auth header, should pass
    {
        auto evhttp_req = evhttp_request_new(nullptr, nullptr);
        HttpRequest req(evhttp_req);
        req._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        req._params.emplace("table", "T");
        EXPECT_EQ(none_handler.on_header(&req), 0);
        // Note: when on_header returns 0, the request is NOT freed by send_reply
        evhttp_request_free(evhttp_req);
    }
}

// Test LOAD privilege type
TEST_F(HttpAuthTest, privilege_type_load) {
    // Create handler with LOAD privilege type (like StreamLoadAction)
    HttpAuthTestHandler load_handler(nullptr, TPrivilegeHier::GLOBAL, TPrivilegeType::LOAD);

    // When enable_all_http_auth=false, should pass without auth
    config::enable_all_http_auth = false;
    auto evhttp_req = evhttp_request_new(nullptr, nullptr);
    HttpRequest req(evhttp_req);
    EXPECT_EQ(load_handler.on_header(&req), 0);
    evhttp_request_free(evhttp_req);
}

// Test different privilege types are distinct
TEST_F(HttpAuthTest, privilege_types_distinct) {
    Defer defer {[]() { config::enable_all_http_auth = false; }};
    config::enable_all_http_auth = true;

    // Create handlers with different privilege types
    HttpAuthTestHandler admin_handler(nullptr, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN);
    HttpAuthTestHandler load_handler(nullptr, TPrivilegeHier::GLOBAL, TPrivilegeType::LOAD);
    HttpAuthTestHandler none_handler(nullptr, TPrivilegeHier::GLOBAL, TPrivilegeType::NONE);

    // When enable_all_http_auth=true, ALL types require authentication
    // Test with valid auth - should pass for all types
    {
        auto evhttp_req = evhttp_request_new(nullptr, nullptr);
        HttpRequest req(evhttp_req);
        req._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDo=");
        req._params.emplace("table", "T");
        EXPECT_EQ(none_handler.on_header(&req), 0);
        evhttp_request_free(evhttp_req);
    }

    // Test without auth - should fail for all types
    // When on_header returns -1, send_reply has been called internally,
    // so we should NOT call evhttp_request_free (would cause double-free)
    {
        auto evhttp_req = evhttp_request_new(nullptr, nullptr);
        HttpRequest req(evhttp_req);
        // No auth header - should fail
        EXPECT_EQ(none_handler.on_header(&req), -1);
    }

    {
        auto evhttp_req = evhttp_request_new(nullptr, nullptr);
        HttpRequest req(evhttp_req);
        // No auth header - should fail
        EXPECT_EQ(admin_handler.on_header(&req), -1);
    }

    {
        auto evhttp_req = evhttp_request_new(nullptr, nullptr);
        HttpRequest req(evhttp_req);
        // No auth header - should fail
        EXPECT_EQ(load_handler.on_header(&req), -1);
    }
}

} // namespace doris
