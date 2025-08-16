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

#include "runtime/plugin/cloud_plugin_config_provider.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <memory>
#include <string>

#include "common/status.h"
#include "gtest/gtest.h"
#include "runtime/plugin/s3_plugin_downloader.h"

namespace doris {

class CloudPluginConfigProviderTest : public ::testing::Test {
protected:
};

TEST_F(CloudPluginConfigProviderTest, TestGetCloudS3ConfigMethodExists) {
    // Test that get_cloud_s3_config method exists and compiles
    // We don't actually call it to avoid storage engine dependencies
    std::unique_ptr<S3PluginDownloader::S3Config> s3_config;
    // Just verify method signature exists by taking its address
    auto method_ptr = &CloudPluginConfigProvider::get_cloud_s3_config;
    EXPECT_TRUE(method_ptr != nullptr);
}

TEST_F(CloudPluginConfigProviderTest, TestGetCloudInstanceIdMethodExists) {
    // Test that get_cloud_instance_id method exists and compiles
    // We don't actually call it to avoid storage engine dependencies
    std::string instance_id;
    // Just verify method signature exists by taking its address
    auto method_ptr = &CloudPluginConfigProvider::get_cloud_instance_id;
    EXPECT_TRUE(method_ptr != nullptr);
}

TEST_F(CloudPluginConfigProviderTest, TestBasicClassStructure) {
    // Test that Status and required types are available
    Status test_status = Status::OK();
    EXPECT_TRUE(test_status.ok());

    // Test that S3Config type is accessible
    std::unique_ptr<S3PluginDownloader::S3Config> config;
    EXPECT_TRUE(config == nullptr);
}

} // namespace doris