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

#include "client/client_connection_provider.h"

#include <brpc/channel.h>
#include <gtest/gtest.h>

#include "common/config.h"

namespace doris::cloud::client {
namespace {

class ClientConnectionProviderTest : public testing::Test {
protected:
    void SetUp() override {
        original_enable_tls = config::enable_tls;
        config::enable_tls = false;
    }

    void TearDown() override { config::enable_tls = original_enable_tls; }

private:
    bool original_enable_tls = false;
};

TEST_F(ClientConnectionProviderTest, OssProvidersKeepPlaintextDefaults) {
    int fdb_option_calls = 0;
    EXPECT_TRUE(configure_fdb_network_options([&](FDBNetworkOption, std::string_view) {
        ++fdb_option_calls;
        return true;
    }));
    EXPECT_EQ(fdb_option_calls, 0);

    brpc::ChannelOptions options;
    EXPECT_TRUE(configure_meta_service_channel_options(&options));
    EXPECT_FALSE(options.has_ssl_options());
}

TEST_F(ClientConnectionProviderTest, OssProvidersRejectTls) {
    config::enable_tls = true;

    int fdb_option_calls = 0;
    EXPECT_FALSE(configure_fdb_network_options([&](FDBNetworkOption, std::string_view) {
        ++fdb_option_calls;
        return true;
    }));
    EXPECT_EQ(fdb_option_calls, 0);

    brpc::ChannelOptions options;
    EXPECT_FALSE(configure_meta_service_channel_options(&options));
    EXPECT_FALSE(options.has_ssl_options());
}

} // namespace
} // namespace doris::cloud::client
