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

#include <cstdint>
#include <string>

#include "common/config.h"
#include "runtime/fragment_mgr.h"
#include "service/backend_options.h"

namespace doris {

class FragmentMgrUrlTest : public testing::Test {
protected:
    void SetUp() override {
        _old_enable_https = config::enable_https;
        _old_enable_tls = config::enable_tls;
        _old_tls_excluded_protocols = config::tls_excluded_protocols;
        _old_webserver_port = config::webserver_port;
        _old_localhost = BackendOptions::get_localhost();
        config::enable_https = false;
        config::enable_tls = false;
        config::tls_excluded_protocols.clear();
        config::webserver_port = 8040;
        BackendOptions::set_localhost("127.0.0.1");
    }

    void TearDown() override {
        config::enable_https = _old_enable_https;
        config::enable_tls = _old_enable_tls;
        config::tls_excluded_protocols = _old_tls_excluded_protocols;
        config::webserver_port = _old_webserver_port;
        BackendOptions::set_localhost(_old_localhost);
    }

private:
    bool _old_enable_https = false;
    bool _old_enable_tls = false;
    std::string _old_tls_excluded_protocols;
    int32_t _old_webserver_port = 0;
    std::string _old_localhost;
};

TEST_F(FragmentMgrUrlTest, LoadErrorUrlUsesEffectiveInternalHttpScheme) {
    EXPECT_EQ("http://127.0.0.1:8040/api/_load_error_log?file=error.log",
              to_load_error_http_path("error.log"));

    config::enable_tls = true;
    EXPECT_EQ("https://127.0.0.1:8040/api/_load_error_log?file=error.log",
              to_load_error_http_path("error.log"));

    config::tls_excluded_protocols = "brpc, HTTP ";
    EXPECT_EQ("http://127.0.0.1:8040/api/_load_error_log?file=error.log",
              to_load_error_http_path("error.log"));

    config::enable_https = true;
    EXPECT_EQ("https://127.0.0.1:8040/api/_load_error_log?file=error.log",
              to_load_error_http_path("error.log"));
}

TEST_F(FragmentMgrUrlTest, LoadErrorUrlPreservesAbsoluteUrls) {
    config::enable_tls = true;
    EXPECT_EQ("http://external/error.log", to_load_error_http_path("http://external/error.log"));
    EXPECT_EQ("https://external/error.log", to_load_error_http_path("https://external/error.log"));
}

} // namespace doris
