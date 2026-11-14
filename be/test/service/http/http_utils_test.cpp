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
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <string>

#include "common/utils.h"
#include "gtest/gtest_pred_impl.h"
#include "service/http/http_common.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/utils.h"
#include "util/string_util.h"
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

TEST_F(HttpUtilsTest, http_auth_code_does_not_override_basic_credentials) {
    for (const auto* code : {"1", "not_a_number", "9223372036854775808"}) {
        HttpRequest req(_evhttp_req);
        req._headers.emplace(HttpHeaders::AUTHORIZATION, encode_basic_auth("doris", "passwd"));
        req._headers.emplace(HTTP_AUTH_CODE, code);
        AuthInfo auth;
        ASSERT_TRUE(parse_basic_auth(req, &auth));
        EXPECT_EQ(-1, auth.auth_code);
        EXPECT_EQ("doris", auth.user);
        EXPECT_EQ("passwd", auth.passwd);

        // FE must receive the password, not an auth_code that skips its authentication branch.
        TLoadTxnBeginRequest request;
        set_request_auth(&request, auth);
        EXPECT_FALSE(request.__isset.auth_code);
        EXPECT_EQ("doris", request.user);
        EXPECT_EQ("passwd", request.passwd);
    }
}

TEST_F(HttpUtilsTest, http_auth_code_alone_is_not_a_credential) {
    for (const auto* code : {"1", "not_a_number"}) {
        HttpRequest req(_evhttp_req);
        req._headers.emplace(HTTP_AUTH_CODE, code);
        AuthInfo auth;
        EXPECT_FALSE(parse_basic_auth(req, &auth));
        EXPECT_EQ(-1, auth.auth_code);
    }
}

TEST_F(HttpUtilsTest, http_auth_code_does_not_override_token) {
    for (const auto& header : {std::string("token"), std::string(HttpHeaders::AUTH_TOKEN)}) {
        HttpRequest req(_evhttp_req);
        req._headers.emplace(header, "cluster-token");
        req._headers.emplace(HTTP_AUTH_CODE, "1");
        AuthInfo auth;
        ASSERT_TRUE(parse_basic_auth(req, &auth));
        EXPECT_EQ(-1, auth.auth_code);
        EXPECT_EQ("cluster-token", auth.token);

        TLoadTxnBeginRequest request;
        set_request_auth(&request, auth);
        EXPECT_FALSE(request.__isset.auth_code);
        EXPECT_TRUE(request.__isset.token);
        EXPECT_EQ("cluster-token", request.token);
    }
}

TEST_F(HttpUtilsTest, internal_auth_code_is_still_forwarded) {
    AuthInfo auth;
    auth.auth_code = 123;
    TLoadTxnBeginRequest request;
    set_request_auth(&request, auth);
    EXPECT_TRUE(request.__isset.auth_code);
    EXPECT_EQ(123, request.auth_code);
}

} // namespace doris
