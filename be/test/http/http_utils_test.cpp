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

#include "common/logging.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/utils.h"
#include "util/url_coding.h"

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

} // namespace doris
