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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>

#include "common/config.h"
#include "common/metrics/metrics.h"
#include "service/http/action/health_action.h"
#include "service/http/action/metrics_action.h"
#include "service/http/ev_http_server.h"
#include "service/http/http_channel.h"
#include "service/http/http_client.h"
#include "service/http/http_handler.h"
#include "service/http/http_handler_with_auth.h"
#include "service/http/http_headers.h"
#include "service/http/http_method.h"
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
        if (req.param("table").empty()) {
            return false;
        }
        return HttpHandlerWithAuth::on_privilege(req, auth_request);
    };
};

static HttpAuthTestHandler s_auth_handler =
        HttpAuthTestHandler(nullptr, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN);

class HttpAuthTest : public testing::Test {};

class AuthLogSink : public google::LogSink {
public:
    void send(google::LogSeverity /*severity*/, const char* /*full_filename*/,
              const char* /*base_filename*/, int /*line*/, const google::LogMessageTime& /*time*/,
              const char* message, std::size_t message_len) override {
        messages.append(message, message_len);
    }

    std::string messages;
};

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

TEST_F(HttpAuthTest, failed_auth_does_not_log_password) {
    Defer restore_auth_config {[]() { config::enable_all_http_auth = false; }};
    config::enable_all_http_auth = true;

    AuthLogSink log_sink;
    google::AddLogSink(&log_sink);
    Defer remove_log_sink {[&log_sink]() { google::RemoveLogSink(&log_sink); }};

    auto evhttp_req = evhttp_request_new(nullptr, nullptr);
    HttpRequest req(evhttp_req);
    req._headers.emplace(HttpHeaders::AUTHORIZATION, "Basic cm9vdDpwbGFpbl90ZXh0X3NlY3JldA==");
    req._params.emplace("table", "T");

    EXPECT_EQ(s_auth_handler.on_header(&req), -1);
    EXPECT_NE(log_sink.messages.find("permission verification failed, request: TCheckAuthRequest"),
              std::string::npos);
    EXPECT_NE(log_sink.messages.find("user=root"), std::string::npos);
    EXPECT_NE(log_sink.messages.find("passwd=***MASKED***"), std::string::npos);
    EXPECT_NE(log_sink.messages.find("priv_hier=GLOBAL"), std::string::npos);
    EXPECT_EQ(log_sink.messages.find("plain_text_secret"), std::string::npos);
    EXPECT_EQ(log_sink.messages.find("cm9vdDpwbGFpbl90ZXh0X3NlY3JldA=="), std::string::npos);
}

TEST_F(HttpAuthTest, invalid_token_does_not_log_token) {
    Defer restore_auth_config {[]() { config::enable_all_http_auth = false; }};
    config::enable_all_http_auth = true;

    AuthLogSink log_sink;
    google::AddLogSink(&log_sink);
    Defer remove_log_sink {[&log_sink]() { google::RemoveLogSink(&log_sink); }};

    auto evhttp_req = evhttp_request_new(nullptr, nullptr);
    HttpRequest req(evhttp_req);
    req._headers.emplace(HttpHeaders::AUTH_TOKEN, "plain_text_token");

    EXPECT_EQ(s_auth_handler.on_header(&req), -1);
    EXPECT_NE(log_sink.messages.find("invalid auth token"), std::string::npos);
    EXPECT_NE(log_sink.messages.find("value=***MASKED***"), std::string::npos);
    EXPECT_EQ(log_sink.messages.find("plain_text_token"), std::string::npos);
}

// Test NONE privilege type - when enable_all_http_auth=true, even NONE type requires auth
TEST_F(HttpAuthTest, privilege_type_none) {
    Defer defer {[]() { config::enable_all_http_auth = false; }};
    config::enable_all_http_auth = true;

    // Create handler with NONE privilege type (like MetricsAction)
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

// With enable_all_http_auth on, /api/health is the one endpoint that still answers anonymous callers
// (Kubernetes probes poll it without credentials), while /metrics, like every other handler, wants
// them. Served over a real socket so the status codes are the ones a probe or a scraper sees.
TEST_F(HttpAuthTest, health_stays_public_when_all_http_auth_is_on) {
    Defer defer {[]() { config::enable_all_http_auth = false; }};
    config::enable_all_http_auth = true;

    MetricRegistry metric_registry("http_auth_test");
    HealthAction health_action;
    MetricsAction metrics_action(&metric_registry, nullptr, TPrivilegeHier::GLOBAL,
                                 TPrivilegeType::NONE);
    EvHttpServer server(0);
    server.register_handler(GET, "/api/health", &health_action);
    server.register_handler(GET, "/metrics", &metrics_action);
    server.start();
    const std::string base_url = "http://127.0.0.1:" + std::to_string(server.get_real_port());

    auto get = [&base_url](const std::string& path, bool with_credentials, std::string* body) {
        HttpClient client;
        EXPECT_TRUE(client.init(base_url + path, /*set_fail_on_error=*/false).ok());
        client.set_method(GET);
        if (with_credentials) {
            // BE_TEST accepts root with an empty password in place of the RPC to the master FE.
            client.set_basic_auth("root", "");
        }
        auto st = client.execute(body);
        EXPECT_TRUE(st.ok()) << st.to_string();
        return client.get_http_status();
    };

    std::string body;
    EXPECT_EQ(get("/api/health", /*with_credentials=*/false, &body), 200);
    EXPECT_NE(body.find("\"status\""), std::string::npos) << body;

    body.clear();
    EXPECT_EQ(get("/metrics", /*with_credentials=*/false, &body), 401);

    body.clear();
    EXPECT_EQ(get("/metrics", /*with_credentials=*/true, &body), 200);
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
