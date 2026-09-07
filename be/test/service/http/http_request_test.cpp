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

#include "service/http/http_request.h"

#include <event2/http.h>
#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include "service/http/http_headers.h"
#include "util/url_coding.h"

namespace doris {

namespace {

constexpr char kMasked[] = "***MASKED***";

std::string basic_of(const std::string& credentials) {
    std::string encoded;
    base64_encode(credentials, &encoded);
    return "Basic " + encoded;
}

// Renders debug_string() for a request carrying a single header.
std::string debug_string_with_header(const std::string& name, const std::string& value) {
    auto* evhttp_req = evhttp_request_new(nullptr, nullptr);
    HttpRequest req(evhttp_req);
    req.set_header(name, value);
    std::string dumped = req.debug_string();
    evhttp_request_free(evhttp_req);
    return dumped;
}

} // namespace

class HttpRequestTest : public testing::Test {};

// The user name is what makes a request attributable, so it is kept while the password is not.
TEST_F(HttpRequestTest, basic_auth_keeps_user_name) {
    const std::string header = basic_of("root:Secret123");
    const std::string dumped = debug_string_with_header(HttpHeaders::AUTHORIZATION, header);

    EXPECT_NE(dumped.find("key=Authorization, value=root:***MASKED***"), std::string::npos)
            << dumped;
    EXPECT_EQ(dumped.find("Secret123"), std::string::npos) << dumped;
    // The base64 blob decodes to the password, so it must not survive either.
    EXPECT_EQ(dumped.find(header.substr(strlen("Basic "))), std::string::npos) << dumped;
}

// The password may contain colons, so only the first one separates it from the user name.
TEST_F(HttpRequestTest, basic_auth_password_containing_colons) {
    const std::string dumped =
            debug_string_with_header(HttpHeaders::AUTHORIZATION, basic_of("admin:pa:ss:word"));

    EXPECT_NE(dumped.find("key=Authorization, value=admin:***MASKED***"), std::string::npos)
            << dumped;
    EXPECT_EQ(dumped.find("pa:ss:word"), std::string::npos) << dumped;
}

// RFC 7617 makes the scheme token case insensitive.
TEST_F(HttpRequestTest, basic_auth_scheme_is_case_insensitive) {
    std::string encoded;
    base64_encode(std::string("alice:secret"), &encoded);

    for (const std::string& scheme : {"Basic", "basic", "BASIC", "BaSiC"}) {
        const std::string dumped =
                debug_string_with_header(HttpHeaders::AUTHORIZATION, scheme + " " + encoded);
        EXPECT_NE(dumped.find("value=alice:***MASKED***"), std::string::npos)
                << "scheme=" << scheme << ", dumped=" << dumped;
        EXPECT_EQ(dumped.find("secret"), std::string::npos) << "scheme=" << scheme;
    }
}

// Anything that is not parseable as Basic credentials is masked as a whole, so a malformed
// header can never leak the part that would have been the password.
TEST_F(HttpRequestTest, unparseable_credentials_are_masked_entirely) {
    std::string encoded;
    base64_encode(std::string("alice:secret"), &encoded);

    const std::vector<std::pair<std::string, std::string>> cases = {
            // more than one space between the scheme and the credentials
            {"two spaces", "Basic  " + encoded},
            // no scheme at all
            {"no scheme", encoded},
            // a scheme that does not carry user:password
            {"bearer", "Bearer eyJhbGciOiJIUzI1NiJ9.payload"},
            // not decodable
            {"bad base64", "Basic !!!not-base64!!!"},
            // decodes, but carries no colon to split on
            {"no colon", basic_of("no-colon-here")},
            // empty value
            {"empty", ""},
    };

    for (const auto& [name, value] : cases) {
        const std::string dumped = debug_string_with_header(HttpHeaders::AUTHORIZATION, value);
        EXPECT_NE(dumped.find(std::string("key=Authorization, value=") + kMasked),
                  std::string::npos)
                << "case=" << name << ", dumped=" << dumped;
        EXPECT_EQ(dumped.find("secret"), std::string::npos) << "case=" << name;
        EXPECT_EQ(dumped.find("payload"), std::string::npos) << "case=" << name;
    }
}

// Only Basic credentials carry a user name; every other sensitive header stays fully masked.
TEST_F(HttpRequestTest, other_sensitive_headers_are_masked_entirely) {
    for (const std::string& name :
         {std::string("token"), std::string("auth_code"), std::string(HttpHeaders::AUTH_TOKEN),
          std::string(HttpHeaders::PROXY_AUTHORIZATION)}) {
        const std::string dumped = debug_string_with_header(name, "SUPERSECRET123");
        EXPECT_NE(dumped.find(kMasked), std::string::npos) << "header=" << name << ", " << dumped;
        EXPECT_EQ(dumped.find("SUPERSECRET123"), std::string::npos) << "header=" << name;
    }
}

TEST_F(HttpRequestTest, non_sensitive_headers_are_untouched) {
    const std::string dumped = debug_string_with_header(HttpHeaders::USER_AGENT, "curl/7.76.1");

    EXPECT_NE(dumped.find("key=User-Agent, value=curl/7.76.1"), std::string::npos) << dumped;
    EXPECT_EQ(dumped.find(kMasked), std::string::npos) << dumped;
}

} // namespace doris
