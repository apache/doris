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

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

#include "common/config.h"
#include "common/status.h"
#include "util/jdbc_utils.h"

namespace doris {

class JdbcUtilsTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Save original config and environment
        original_jdbc_drivers_dir_ = config::jdbc_drivers_dir;
        original_doris_home_ = getenv("DORIS_HOME");

        // Set DORIS_HOME for testing
        setenv("DORIS_HOME", "/tmp/test_doris", 1);
    }

    void TearDown() override {
        // Restore original config and environment
        config::jdbc_drivers_dir = original_jdbc_drivers_dir_;

        if (original_doris_home_) {
            setenv("DORIS_HOME", original_doris_home_, 1);
        } else {
            unsetenv("DORIS_HOME");
        }
    }

private:
    std::string original_jdbc_drivers_dir_;
    const char* original_doris_home_ = nullptr;
};

// Test resolve_driver_url with absolute URLs
TEST_F(JdbcUtilsTest, TestResolveDriverUrlWithAbsoluteUrl) {
    std::string result_url;

    // Test with HTTP URL (contains ":/")
    Status status = JdbcUtils::resolve_driver_url("http://example.com/driver.jar", &result_url);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "http://example.com/driver.jar");

    // Test with S3 URL
    status = JdbcUtils::resolve_driver_url("s3://bucket/path/driver.jar", &result_url);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "s3://bucket/path/driver.jar");

    // Test with file URL
    status = JdbcUtils::resolve_driver_url("file:///path/to/driver.jar", &result_url);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "file:///path/to/driver.jar");
}

TEST_F(JdbcUtilsTest, TestResolveDriverUrlWithRelativeUrl) {
    std::string result_url;

    // Set config to default value to trigger the default directory logic
    config::jdbc_drivers_dir = "/tmp/test_doris/plugins/jdbc_drivers";

    // Create the target directory and file for testing
    std::string dir = "/tmp/test_doris/plugins/jdbc_drivers";
    std::string file_path = dir + "/mysql-connector.jar";

    // Create directory and file
    std::filesystem::create_directories(dir);
    std::ofstream file(file_path);
    file << "test content";
    file.close();

    // Test with relative URL (no ":/") - should resolve to file://
    Status status = JdbcUtils::resolve_driver_url("mysql-connector.jar", &result_url);
    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(result_url.empty());
    EXPECT_EQ(result_url, "file://" + file_path);

    // Cleanup
    std::filesystem::remove(file_path);
    std::filesystem::remove_all(dir);
}

// Test resolve_driver_url with default directory
TEST_F(JdbcUtilsTest, TestResolveWithDefaultConfig) {
    config::jdbc_drivers_dir = "/tmp/test_doris/plugins/jdbc_drivers";

    // Create the target directory and file for testing
    std::string dir = "/tmp/test_doris/plugins/jdbc_drivers";
    std::string file_path = dir + "/mysql-connector.jar";

    std::filesystem::create_directories(dir);
    std::ofstream file(file_path);
    file << "test content";
    file.close();

    std::string result_url;
    Status status = JdbcUtils::resolve_driver_url("mysql-connector.jar", &result_url);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "file://" + file_path);

    // Cleanup
    std::filesystem::remove(file_path);
    std::filesystem::remove_all(dir);
}

TEST_F(JdbcUtilsTest, TestResolveWithCustomConfig) {
    // Set custom JDBC drivers directory
    config::jdbc_drivers_dir = "/custom/jdbc/path";

    std::string result_url;
    Status status = JdbcUtils::resolve_driver_url("postgres-driver.jar", &result_url);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "file:///custom/jdbc/path/postgres-driver.jar");
}

TEST_F(JdbcUtilsTest, TestDefaultDirectoryFileExistsPath) {
    config::jdbc_drivers_dir = "/tmp/test_doris/plugins/jdbc_drivers";

    std::string dir = "/tmp/test_doris/plugins/jdbc_drivers";
    std::string file_path = dir + "/existing-driver.jar";

    std::filesystem::create_directories(dir);
    std::ofstream file(file_path);
    file << "test content";
    file.close();

    std::string result_url;
    Status status = JdbcUtils::resolve_driver_url("existing-driver.jar", &result_url);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "file://" + file_path);

    // Cleanup
    std::filesystem::remove(file_path);
    std::filesystem::remove_all(dir);
}

TEST_F(JdbcUtilsTest, TestFallbackToOldDirectory) {
    config::jdbc_drivers_dir = "/tmp/test_doris/plugins/jdbc_drivers";

    // Create only the old directory and file (not the new one)
    std::string old_dir = "/tmp/test_doris/jdbc_drivers";
    std::string file_path = old_dir + "/fallback-driver.jar";

    std::filesystem::create_directories(old_dir);
    std::ofstream file(file_path);
    file << "test content";
    file.close();

    std::string result_url;
    Status status = JdbcUtils::resolve_driver_url("fallback-driver.jar", &result_url);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "file://" + file_path);

    // Cleanup
    std::filesystem::remove(file_path);
    std::filesystem::remove_all(old_dir);
}

TEST_F(JdbcUtilsTest, TestPathConstruction) {
    setenv("DORIS_HOME", "/tmp/test_doris2", 1);
    config::jdbc_drivers_dir = "/tmp/test_doris2/plugins/jdbc_drivers";

    std::string old_dir = "/tmp/test_doris2/jdbc_drivers";
    std::string file_path = old_dir + "/test.jar";

    std::filesystem::create_directories(old_dir);
    std::ofstream file(file_path);
    file << "test content";
    file.close();

    std::string result_url;
    Status status = JdbcUtils::resolve_driver_url("test.jar", &result_url);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "file://" + file_path);

    // Cleanup
    std::filesystem::remove(file_path);
    std::filesystem::remove_all(old_dir);
}

TEST_F(JdbcUtilsTest, TestEdgeCases) {
    std::string result_url;

    // Test empty URL - treated as relative, should go through resolve logic
    config::jdbc_drivers_dir = "/custom/path";
    Status status = JdbcUtils::resolve_driver_url("", &result_url);
    EXPECT_TRUE(status.ok());

    // Test URL with just colon (no slash after) - treated as relative
    status = JdbcUtils::resolve_driver_url("invalid:url", &result_url);
    EXPECT_TRUE(status.ok());

    // Test URL with spaces - treated as relative
    status = JdbcUtils::resolve_driver_url("my driver.jar", &result_url);
    EXPECT_TRUE(status.ok());
}

TEST_F(JdbcUtilsTest, TestMultipleCallsConsistency) {
    config::jdbc_drivers_dir = "/tmp/test_doris/plugins/jdbc_drivers";

    std::string dir = "/tmp/test_doris/plugins/jdbc_drivers";
    std::string file_path = dir + "/same-driver.jar";

    std::filesystem::create_directories(dir);
    std::ofstream file(file_path);
    file << "test content";
    file.close();

    std::string result_url1, result_url2;
    Status status1 = JdbcUtils::resolve_driver_url("same-driver.jar", &result_url1);
    Status status2 = JdbcUtils::resolve_driver_url("same-driver.jar", &result_url2);

    EXPECT_TRUE(status1.ok());
    EXPECT_TRUE(status2.ok());
    EXPECT_EQ(result_url1, result_url2);
    EXPECT_EQ(result_url1, "file://" + file_path);

    // Cleanup
    std::filesystem::remove(file_path);
    std::filesystem::remove_all(dir);
}

TEST_F(JdbcUtilsTest, TestUrlDetectionLogic) {
    std::string result_url;

    // Test various URL patterns that should be detected as absolute
    std::vector<std::string> absolute_urls = {
            "http://example.com/driver.jar", "https://example.com/driver.jar",
            "s3://bucket/driver.jar", "file:///local/driver.jar", "ftp://server/driver.jar"};

    for (const auto& url : absolute_urls) {
        Status status = JdbcUtils::resolve_driver_url(url, &result_url);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(result_url, url);
    }

    // Test patterns that should be treated as relative
    config::jdbc_drivers_dir = "/custom/path";
    std::vector<std::string> relative_urls = {"driver.jar", "path/driver.jar", "invalid:no-slash"};

    for (const auto& url : relative_urls) {
        Status status = JdbcUtils::resolve_driver_url(url, &result_url);
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(result_url.find(url) != std::string::npos);
    }
}

} // namespace doris
