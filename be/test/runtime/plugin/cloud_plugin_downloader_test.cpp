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

#include "runtime/plugin/cloud_plugin_downloader.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <string>

#include "common/status.h"
#include "gtest/gtest.h"

namespace doris {

class CloudPluginDownloaderTest : public ::testing::Test {
protected:
};

TEST_F(CloudPluginDownloaderTest, TestPluginTypeEnumValues) {
    // Test PluginType enum values exist
    CloudPluginDownloader::PluginType jdbc_type = CloudPluginDownloader::PluginType::JDBC_DRIVERS;
    CloudPluginDownloader::PluginType udf_type = CloudPluginDownloader::PluginType::JAVA_UDF;
    CloudPluginDownloader::PluginType connector_type =
            CloudPluginDownloader::PluginType::CONNECTORS;
    CloudPluginDownloader::PluginType hadoop_type = CloudPluginDownloader::PluginType::HADOOP_CONF;

    // Basic validation that enums are distinct
    EXPECT_NE(jdbc_type, udf_type);
    EXPECT_NE(udf_type, connector_type);
    EXPECT_NE(connector_type, hadoop_type);
    EXPECT_NE(hadoop_type, jdbc_type);
}

TEST_F(CloudPluginDownloaderTest, TestDownloadFromCloudMethodExists) {
    // Test that download_from_cloud static method exists and compiles
    // We don't actually call it to avoid storage engine dependencies
    std::string local_path;

    // Just verify method signature exists by taking its address
    auto method_ptr = &CloudPluginDownloader::download_from_cloud;
    EXPECT_TRUE(method_ptr != nullptr);

    // Test that all plugin types are accessible
    CloudPluginDownloader::PluginType udf_type = CloudPluginDownloader::PluginType::JAVA_UDF;
    CloudPluginDownloader::PluginType jdbc_type = CloudPluginDownloader::PluginType::JDBC_DRIVERS;

    EXPECT_NE(udf_type, jdbc_type);
}

TEST_F(CloudPluginDownloaderTest, TestStatusTypeAvailable) {
    // Test that Status type is available and working
    // This ensures common/status.h is properly included
    Status test_status = Status::OK();
    EXPECT_TRUE(test_status.ok());

    Status error_status = Status::InvalidArgument("test error");
    EXPECT_FALSE(error_status.ok());
    EXPECT_FALSE(error_status.to_string().empty());
}

} // namespace doris