// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "http/utils.h"

#include <gtest/gtest.h>

#include "common/logging.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "util/url_coding.h"

namespace palo {

class HttpUtilsTest : public testing::Test {
public:
    HttpUtilsTest() { }
    virtual ~HttpUtilsTest() {
    }
};

TEST_F(HttpUtilsTest, parse_basic_auth) {
    {
        HttpRequest req;
        std::string auth = "Basic ";
        std::string encoded_str;
        base64_encode("palo:passwd", &encoded_str);
        auth += encoded_str;
        req._headers.emplace(HttpHeaders::AUTHORIZATION, auth);
        std::string user;
        std::string passwd;
        auto res = parse_basic_auth(req, &user, &passwd);
        ASSERT_TRUE(res);
        ASSERT_STREQ("palo", user.data());
        ASSERT_STREQ("passwd", passwd.data());
    }
    {
        HttpRequest req;
        std::string auth = "Basic ";
        std::string encoded_str = "palo:passwd";
        auth += encoded_str;
        req._headers.emplace(HttpHeaders::AUTHORIZATION, auth);
        std::string user;
        std::string passwd;
        auto res = parse_basic_auth(req, &user, &passwd);
        ASSERT_FALSE(res);
    }
    {
        HttpRequest req;
        std::string auth = "Basic ";
        std::string encoded_str;
        base64_encode("palopasswd", &encoded_str);
        auth += encoded_str;
        req._headers.emplace(HttpHeaders::AUTHORIZATION, auth);
        std::string user;
        std::string passwd;
        auto res = parse_basic_auth(req, &user, &passwd);
        ASSERT_FALSE(res);
    }
    {
        HttpRequest req;
        std::string auth = "Basic";
        std::string encoded_str;
        base64_encode("palo:passwd", &encoded_str);
        auth += encoded_str;
        req._headers.emplace(HttpHeaders::AUTHORIZATION, auth);
        std::string user;
        std::string passwd;
        auto res = parse_basic_auth(req, &user, &passwd);
        ASSERT_FALSE(res);
    }
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
