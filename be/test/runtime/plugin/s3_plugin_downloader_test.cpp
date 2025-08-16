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

#include "runtime/plugin/s3_plugin_downloader.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <filesystem>
#include <fstream>
#include <string>

#include "gtest/gtest.h"

namespace doris {

class S3PluginDownloaderTest : public ::testing::Test {
protected:
};

TEST_F(S3PluginDownloaderTest, TestS3ConfigCreation) {
    S3PluginDownloader::S3Config config("http://s3.amazonaws.com", "us-west-2", "test-bucket",
                                        "access-key", "secret-key");

    EXPECT_EQ("http://s3.amazonaws.com", config.endpoint);
    EXPECT_EQ("us-west-2", config.region);
    EXPECT_EQ("test-bucket", config.bucket);
    EXPECT_EQ("access-key", config.access_key);
    EXPECT_EQ("secret-key", config.secret_key);
}

TEST_F(S3PluginDownloaderTest, TestS3ConfigToString) {
    S3PluginDownloader::S3Config config("http://s3.amazonaws.com", "us-west-2", "test-bucket",
                                        "access-key", "secret-key");

    std::string config_str = config.to_string();

    // Should contain basic info but mask secret info
    EXPECT_TRUE(config_str.find("s3.amazonaws.com") != std::string::npos);
    EXPECT_TRUE(config_str.find("us-west-2") != std::string::npos);
    EXPECT_TRUE(config_str.find("test-bucket") != std::string::npos);
    EXPECT_TRUE(config_str.find("***") != std::string::npos ||
                config_str.find("null") !=
                        std::string::npos); // Access key should be masked or null
    EXPECT_FALSE(config_str.find("access-key") !=
                 std::string::npos); // Actual key should not appear
}
} // namespace doris