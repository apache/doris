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

#include "util/http_url_security.h"

#include <gtest/gtest.h>

namespace doris {

TEST(HttpUrlSecurityTest, RejectsUnsafeHttpTargetsByDefault) {
    EXPECT_FALSE(HttpUrlSecurity::validate_url("http://127.0.0.1/data", {}).ok());
    EXPECT_FALSE(HttpUrlSecurity::validate_url("http://[::1]/data", {}).ok());
    EXPECT_FALSE(HttpUrlSecurity::validate_url("http://169.254.169.254/latest/meta-data", {}).ok());
    EXPECT_FALSE(HttpUrlSecurity::validate_url("http://10.0.0.1/data", {}).ok());
    EXPECT_FALSE(HttpUrlSecurity::validate_url("file:///tmp/data", {}).ok());
}

TEST(HttpUrlSecurityTest, AllowsExplicitPrivateEndpointAllowlist) {
    EXPECT_TRUE(HttpUrlSecurity::validate_url("http://127.0.0.1/data", {"127.0.0.1/32"}).ok());
    EXPECT_TRUE(HttpUrlSecurity::validate_url("http://10.1.2.3/data", {"10.0.0.0/8"}).ok());
    EXPECT_TRUE(HttpUrlSecurity::validate_url("http://localhost/data", {"localhost"}).ok());
}

TEST(HttpUrlSecurityTest, AllowsPublicIpTargets) {
    EXPECT_TRUE(HttpUrlSecurity::validate_url("https://93.184.216.34/index.html", {}).ok());
    EXPECT_TRUE(HttpUrlSecurity::validate_url("https://[2606:2800:220:1:248:1893:25c8:1946]/", {})
                        .ok());
}

} // namespace doris
