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

#include <gtest/gtest.h>

namespace doris {

TEST(HttpRequestTest, MaskSensitiveHeadersInGetAllHeaders) {
    HttpRequest req(nullptr);
    req._headers.emplace("Authorization", "Bearer fake_secret");
    req._headers.emplace("Proxy-Authorization", "Basic fake_proxy");
    req._headers.emplace("Auth-Token", "fake_auth_token");
    req._headers.emplace("token", "fake_lower_token");
    req._headers.emplace("label", "normal_label");

    std::string headers = req.get_all_headers();

    EXPECT_NE(headers.find("Authorization:***MASKED***"), std::string::npos);
    EXPECT_NE(headers.find("Proxy-Authorization:***MASKED***"), std::string::npos);
    EXPECT_NE(headers.find("Auth-Token:***MASKED***"), std::string::npos);
    EXPECT_NE(headers.find("token:***MASKED***"), std::string::npos);
    EXPECT_NE(headers.find("label:normal_label"), std::string::npos);

    EXPECT_EQ(headers.find("fake_secret"), std::string::npos);
    EXPECT_EQ(headers.find("fake_proxy"), std::string::npos);
    EXPECT_EQ(headers.find("fake_auth_token"), std::string::npos);
    EXPECT_EQ(headers.find("fake_lower_token"), std::string::npos);
}

TEST(HttpRequestTest, MaskSensitiveHeadersInDebugString) {
    HttpRequest req(nullptr);
    req._headers.emplace("Authorization", "Bearer fake_secret");
    req._headers.emplace("token", "fake_lower_token");
    req._headers.emplace("label", "normal_label");

    std::string debug_string = req.debug_string();

    EXPECT_NE(debug_string.find("key=Authorization, value=***MASKED***"), std::string::npos);
    EXPECT_NE(debug_string.find("key=token, value=***MASKED***"), std::string::npos);
    EXPECT_NE(debug_string.find("key=label, value=normal_label"), std::string::npos);

    EXPECT_EQ(debug_string.find("fake_secret"), std::string::npos);
    EXPECT_EQ(debug_string.find("fake_lower_token"), std::string::npos);
}

} // namespace doris
