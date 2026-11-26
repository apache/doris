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

#include <event2/http.h>
#include <gen_cpp/FrontendService_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <string>

#include "common/utils.h"
#include "gtest/gtest_pred_impl.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/utils.h"
#include "util/url_coding.h"

struct evhttp_request;

namespace doris {

class HttpUtilsTest : public testing::Test {
public:
    HttpUtilsTest() {}
    virtual ~HttpUtilsTest() {}
    void SetUp() override { _evhttp_req = evhttp_request_new(nullptr, nullptr); }
    void TearDown() override {
        if (_evhttp_req != nullptr) {
            evhttp_request_free(_evhttp_req);
        }
    }

private:
    template <typename Request>
    void check_token_request(const AuthInfo& auth) {
        Request request;
        set_request_auth(&request, auth);
        EXPECT_TRUE(request.__isset.token);
        EXPECT_EQ("internal-token", request.token);
        EXPECT_EQ("admin", request.user);
        EXPECT_FALSE(request.__isset.auth_code);
    }

    evhttp_request* _evhttp_req = nullptr;
};

TEST_F(HttpUtilsTest, parse_basic_auth) {
    {
        HttpRequest req(_evhttp_req);
        auto auth = encode_basic_auth("doris", "passwd");
        req._headers.emplace(HttpHeaders::AUTHORIZATION, auth);
        std::string user;
        std::string passwd;
        auto res = parse_basic_auth(req, &user, &passwd);
        EXPECT_TRUE(res);
        EXPECT_STREQ("doris", user.data());
        EXPECT_STREQ("passwd", passwd.data());
    }
    {
        HttpRequest req(_evhttp_req);
        std::string auth = "Basic ";
        std::string encoded_str = "doris:passwd";
        auth += encoded_str;
        req._headers.emplace(HttpHeaders::AUTHORIZATION, auth);
        std::string user;
        std::string passwd;
        auto res = parse_basic_auth(req, &user, &passwd);
        EXPECT_FALSE(res);
    }
    {
        HttpRequest req(_evhttp_req);
        std::string auth = "Basic ";
        std::string encoded_str;
        base64_encode("dorispasswd", &encoded_str);
        auth += encoded_str;
        req._headers.emplace(HttpHeaders::AUTHORIZATION, auth);
        std::string user;
        std::string passwd;
        auto res = parse_basic_auth(req, &user, &passwd);
        EXPECT_FALSE(res);
    }
    {
        HttpRequest req(_evhttp_req);
        std::string auth = "Basic";
        std::string encoded_str;
        base64_encode("doris:passwd", &encoded_str);
        auth += encoded_str;
        req._headers.emplace(HttpHeaders::AUTHORIZATION, auth);
        std::string user;
        std::string passwd;
        auto res = parse_basic_auth(req, &user, &passwd);
        EXPECT_FALSE(res);
    }
}

TEST_F(HttpUtilsTest, deprecated_auth_code_does_not_replace_credentials) {
    for (const auto* value : {"0", "1234567890123", "-1", "not_a_number"}) {
        HttpRequest req(_evhttp_req);
        req._headers.emplace("auth_code", value);
        AuthInfo missing_auth;
        EXPECT_FALSE(parse_basic_auth(req, &missing_auth));

        req._headers.emplace(HttpHeaders::AUTHORIZATION, encode_basic_auth("doris", "passwd"));
        AuthInfo auth;
        EXPECT_TRUE(parse_basic_auth(req, &auth));
        TLoadTxnBeginRequest request;
        set_request_auth(&request, auth);
        EXPECT_EQ("doris", request.user);
        EXPECT_EQ("passwd", request.passwd);
        EXPECT_FALSE(request.__isset.auth_code);
        EXPECT_FALSE(request.__isset.token);
    }
}

TEST_F(HttpUtilsTest, internal_token_is_forwarded_with_user) {
    for (const auto& header : {std::string("token"), HttpHeaders::AUTH_TOKEN}) {
        HttpRequest req(_evhttp_req);
        req._headers.emplace(header, "internal-token");
        req._headers.emplace("auth_code", "0");
        req._headers.emplace(HttpHeaders::AUTHORIZATION, encode_basic_auth("admin", "unused"));
        AuthInfo auth;
        ASSERT_TRUE(parse_basic_auth(req, &auth));
        check_token_request<TStreamLoadPutRequest>(auth);
        check_token_request<TLoadTxnCommitRequest>(auth);
        check_token_request<TLoadTxnRollbackRequest>(auth);
    }
}

} // namespace doris
