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

#include "common/tls_protocol_config.h"

#include <gtest/gtest.h>

#include <string>

#include "common/config.h"

namespace doris {

class TlsProtocolConfigTest : public testing::Test {
protected:
    void SetUp() override {
        _old_enable_https = config::enable_https;
        _old_enable_tls = config::enable_tls;
        _old_tls_excluded_protocols = config::tls_excluded_protocols;
        config::enable_https = false;
        config::enable_tls = false;
        config::tls_excluded_protocols.clear();
    }

    void TearDown() override {
        config::enable_https = _old_enable_https;
        config::enable_tls = _old_enable_tls;
        config::tls_excluded_protocols = _old_tls_excluded_protocols;
    }

private:
    bool _old_enable_https = false;
    bool _old_enable_tls = false;
    std::string _old_tls_excluded_protocols;
};

TEST_F(TlsProtocolConfigTest, UsesLegacyHttpsWhenConfigured) {
    EXPECT_STREQ("http://", get_internal_http_scheme());

    config::enable_https = true;
    EXPECT_STREQ("https://", get_internal_http_scheme());
}

TEST_F(TlsProtocolConfigTest, UsesUnifiedTlsUnlessHttpIsExcluded) {
    config::enable_tls = true;
    EXPECT_STREQ("https://", get_internal_http_scheme());

    config::tls_excluded_protocols = "brpc, HTTP , thrift";
    EXPECT_STREQ("http://", get_internal_http_scheme());

    config::tls_excluded_protocols = "http2, thrift";
    EXPECT_STREQ("https://", get_internal_http_scheme());
}

} // namespace doris
