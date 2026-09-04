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

#include "server/cloud_server_starter_factory.h"

#include <brpc/server.h>
#include <gtest/gtest.h>

#include <memory>

#include "common/config.h"

namespace doris::cloud {

class CloudServerStarterFactoryTest : public testing::Test {
protected:
    void SetUp() override { original_enable_tls = config::enable_tls; }

    void TearDown() override { config::enable_tls = original_enable_tls; }

    bool original_enable_tls = false;
};

TEST_F(CloudServerStarterFactoryTest, PlaintextConfigIsValid) {
    brpc::Server server;
    std::unique_ptr<ICloudServerStarter> starter;
    ASSERT_TRUE(create_meta_brpc_starter(&server, 0, &starter));

    config::enable_tls = false;
    EXPECT_TRUE(starter->validate_config());
}

TEST_F(CloudServerStarterFactoryTest, TlsConfigIsRejected) {
    brpc::Server server;
    std::unique_ptr<ICloudServerStarter> starter;
    ASSERT_TRUE(create_meta_brpc_starter(&server, 0, &starter));

    config::enable_tls = true;
    EXPECT_FALSE(starter->validate_config());
}

TEST_F(CloudServerStarterFactoryTest, NullServerIsRejected) {
    std::unique_ptr<ICloudServerStarter> starter;
    ASSERT_TRUE(create_meta_brpc_starter(nullptr, 0, &starter));

    config::enable_tls = false;
    EXPECT_FALSE(starter->validate_config());
}

} // namespace doris::cloud
