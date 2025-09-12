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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "cloud/cloud_storage_engine.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"

namespace doris {

class CloudPluginDownloaderTest : public ::testing::Test {
protected:
    void SetUp() override { downloader = std::make_unique<CloudPluginDownloader>(); }

    void TearDown() override {
        downloader.reset();
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    void SetupRegularStorageEngine() {
        ExecEnv::GetInstance()->set_storage_engine(
                std::make_unique<StorageEngine>(EngineOptions()));
    }

    void SetupCloudStorageEngine() {
        ExecEnv::GetInstance()->set_storage_engine(
                std::make_unique<CloudStorageEngine>(EngineOptions()));
    }

    std::unique_ptr<CloudPluginDownloader> downloader;
};

// ============== Core Business Logic Tests ==============

TEST_F(CloudPluginDownloaderTest, TestBuildPluginPath) {
    std::string path;
    Status status = downloader->_build_plugin_path(CloudPluginDownloader::PluginType::JDBC_DRIVERS,
                                                   "mysql-connector.jar", &path);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ("plugins/jdbc_drivers/mysql-connector.jar", path);

    status = downloader->_build_plugin_path(CloudPluginDownloader::PluginType::JAVA_UDF,
                                            "my-udf.jar", &path);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ("plugins/java_udf/my-udf.jar", path);
}

TEST_F(CloudPluginDownloaderTest, TestBuildPluginPathEdgeCases) {
    std::string path;
    Status status = downloader->_build_plugin_path(CloudPluginDownloader::PluginType::JDBC_DRIVERS,
                                                   "test-file_v1.2.jar", &path);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ("plugins/jdbc_drivers/test-file_v1.2.jar", path);

    status = downloader->_build_plugin_path(CloudPluginDownloader::PluginType::JAVA_UDF,
                                            "sub/dir/file.jar", &path);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ("plugins/java_udf/sub/dir/file.jar", path);

    std::string long_name(100, 'a');
    long_name += ".jar";
    status = downloader->_build_plugin_path(CloudPluginDownloader::PluginType::JDBC_DRIVERS,
                                            long_name, &path);
    EXPECT_TRUE(status.ok());
    std::string expected = "plugins/jdbc_drivers/" + long_name;
    EXPECT_EQ(expected, path);
}

TEST_F(CloudPluginDownloaderTest, TestBuildPluginPathUnsupportedTypes) {
    std::string path;
    Status status = downloader->_build_plugin_path(CloudPluginDownloader::PluginType::CONNECTORS,
                                                   "kafka-connector.jar", &path);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_THAT(status.msg(), testing::HasSubstr("Unsupported plugin type"));

    status = downloader->_build_plugin_path(CloudPluginDownloader::PluginType::HADOOP_CONF,
                                            "core-site.xml", &path);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_THAT(status.msg(), testing::HasSubstr("Unsupported plugin type"));
}

TEST_F(CloudPluginDownloaderTest, TestBuildPluginPathInvalidType) {
    CloudPluginDownloader::PluginType invalid_type =
            static_cast<CloudPluginDownloader::PluginType>(999);
    std::string path;
    Status status = downloader->_build_plugin_path(invalid_type, "test.jar", &path);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_THAT(status.msg(), testing::HasSubstr("Unsupported plugin type"));
}

// ============== Storage Engine Integration Tests ==============

TEST_F(CloudPluginDownloaderTest, TestDownloadFromCloudEmptyName) {
    std::string result_path;
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JDBC_DRIVERS, "", "/tmp/test.jar", &result_path);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ("Plugin name cannot be empty", status.msg());
}

TEST_F(CloudPluginDownloaderTest, TestDownloadFromCloudRegularStorageEngine) {
    SetupRegularStorageEngine();

    std::string result_path;
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JDBC_DRIVERS, "mysql.jar", "/tmp/test.jar",
            &result_path);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::NOT_FOUND);
    EXPECT_TRUE(status.to_string().find("CloudStorageEngine not found") != std::string::npos);
}

TEST_F(CloudPluginDownloaderTest, TestGetCloudFilesystemNonCloudEnvironment) {
    SetupRegularStorageEngine();

    io::RemoteFileSystemSPtr filesystem;
    Status status = downloader->_get_cloud_filesystem(&filesystem);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::NOT_FOUND);
    EXPECT_TRUE(status.to_string().find("CloudStorageEngine not found") != std::string::npos);
}

TEST_F(CloudPluginDownloaderTest, TestGetCloudFilesystemNoStorageEngine) {
    SetupRegularStorageEngine();

    io::RemoteFileSystemSPtr filesystem;
    Status status = downloader->_get_cloud_filesystem(&filesystem);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::NOT_FOUND);
}

TEST_F(CloudPluginDownloaderTest, TestGetCloudFilesystemCloudEnvironment) {
    SetupCloudStorageEngine();
    io::RemoteFileSystemSPtr filesystem;
    Status status = downloader->_get_cloud_filesystem(&filesystem);
    if (!status.ok()) {
        EXPECT_EQ(status.code(), ErrorCode::NOT_FOUND);
        EXPECT_TRUE(status.to_string().find("No latest filesystem available") != std::string::npos);
    }
}

TEST_F(CloudPluginDownloaderTest, TestDownloadFromCloudCloudStorageEngine) {
    SetupCloudStorageEngine();
    std::string result_path;
    Status status = CloudPluginDownloader::download_from_cloud(
            CloudPluginDownloader::PluginType::JDBC_DRIVERS, "mysql.jar", "/tmp/test.jar",
            &result_path);
    EXPECT_FALSE(status.ok());
}

// ============== File System Tests ==============

TEST_F(CloudPluginDownloaderTest, TestPrepareLocalPathSuccess) {
    std::string test_path = "/tmp/test_new_file.jar";
    Status status = downloader->_prepare_local_path(test_path);
    EXPECT_TRUE(status.ok());
}

TEST_F(CloudPluginDownloaderTest, TestPrepareLocalPathWithExistingFile) {
    std::string existing_file = "/tmp/existing_file.jar";
    std::ofstream file(existing_file);
    file << "existing content";
    file.close();
    EXPECT_TRUE(std::filesystem::exists(existing_file));

    Status status = downloader->_prepare_local_path(existing_file);
    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(std::filesystem::exists(existing_file));
}

TEST_F(CloudPluginDownloaderTest, TestPrepareLocalPathWithNestedDirectory) {
    std::string nested_path = "/tmp/test_dir/nested/test.jar";
    Status status = downloader->_prepare_local_path(nested_path);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(std::filesystem::exists("/tmp/test_dir/nested"));
    std::filesystem::remove_all("/tmp/test_dir");
}

TEST_F(CloudPluginDownloaderTest, TestPrepareLocalPathRootDirectory) {
    std::string root_path = "/test_root.jar";
    Status status = downloader->_prepare_local_path(root_path);
    EXPECT_TRUE(status.ok());
    std::filesystem::remove(root_path);
}

// ============== Consistency Tests ==============

TEST_F(CloudPluginDownloaderTest, TestAllTypePathCombinations) {
    struct TestCase {
        CloudPluginDownloader::PluginType type;
        std::string name;
        std::string expected;
    };

    std::vector<TestCase> test_cases = {
            {CloudPluginDownloader::PluginType::JDBC_DRIVERS, "driver.jar",
             "plugins/jdbc_drivers/driver.jar"},
            {CloudPluginDownloader::PluginType::JAVA_UDF, "udf.jar", "plugins/java_udf/udf.jar"},
    };

    for (const auto& test_case : test_cases) {
        std::string result;
        Status status = downloader->_build_plugin_path(test_case.type, test_case.name, &result);
        EXPECT_TRUE(status.ok()) << "Failed for type: " << static_cast<int>(test_case.type)
                                 << ", name: " << test_case.name << ", error: " << status.msg();
        EXPECT_EQ(test_case.expected, result)
                << "Failed for type: " << static_cast<int>(test_case.type)
                << ", name: " << test_case.name;
    }
}

} // namespace doris