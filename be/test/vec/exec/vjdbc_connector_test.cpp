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

#include "vec/exec/vjdbc_connector.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

#include "common/config.h"
#include "common/status.h"

namespace doris::vectorized {

class JdbcConnectorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Save original config and environment
        original_jdbc_drivers_dir_ = config::jdbc_drivers_dir;
        original_doris_home_ = getenv("DORIS_HOME");

        // Set DORIS_HOME for testing
        setenv("DORIS_HOME", "/tmp/test_doris", 1);

        // Initialize test JDBC parameters
        param_.catalog_id = 1;
        param_.driver_path = "test-driver.jar";
        param_.driver_class = "com.test.Driver";
        param_.resource_name = "test_resource";
        param_.driver_checksum = "test_checksum";
        param_.jdbc_url = "jdbc:test://localhost:3306/test";
        param_.user = "test_user";
        param_.passwd = "test_passwd";
        param_.query_string = "SELECT * FROM test";
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

    JdbcConnector createConnector() { return JdbcConnector(param_); }

private:
    std::string original_jdbc_drivers_dir_;
    const char* original_doris_home_ = nullptr;
    JdbcConnectorParam param_;
};

// Test _get_real_url method
TEST_F(JdbcConnectorTest, TestGetRealUrlWithAbsoluteUrl) {
    auto connector = createConnector();
    std::string result_url;

    // Test with absolute URL (contains ":/ ")
    Status status = connector._get_real_url("http://example.com/driver.jar", &result_url);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "http://example.com/driver.jar");

    // Test with S3 URL
    status = connector._get_real_url("s3://bucket/path/driver.jar", &result_url);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "s3://bucket/path/driver.jar");

    // Test with file URL
    status = connector._get_real_url("file:///path/to/driver.jar", &result_url);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "file:///path/to/driver.jar");
}

TEST_F(JdbcConnectorTest, TestGetRealUrlWithRelativeUrl) {
    auto connector = createConnector();
    std::string result_url;

    // Test with relative URL (no ":/" found) - should call _check_and_return_default_driver_url
    Status status = connector._get_real_url("mysql-connector.jar", &result_url);

    // This should process successfully
    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(result_url.empty());
}

// Test _check_and_return_default_driver_url method with default directory
TEST_F(JdbcConnectorTest, TestCheckAndReturnDefaultDriverUrlWithDefaultConfig) {
    auto connector = createConnector();

    // Set config to default value to trigger the default directory logic
    config::jdbc_drivers_dir = "/tmp/test_doris/plugins/jdbc_drivers";

    std::string result_url;
    Status status =
            connector._check_and_return_default_driver_url("mysql-connector.jar", &result_url);

    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(result_url.find("file://") == 0);
    EXPECT_TRUE(result_url.find("mysql-connector.jar") != std::string::npos);
}

TEST_F(JdbcConnectorTest, TestCheckAndReturnDefaultDriverUrlWithCustomConfig) {
    auto connector = createConnector();

    // Set custom JDBC drivers directory
    config::jdbc_drivers_dir = "/custom/jdbc/path";

    std::string result_url;
    Status status =
            connector._check_and_return_default_driver_url("postgres-driver.jar", &result_url);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "file:///custom/jdbc/path/postgres-driver.jar");
}

TEST_F(JdbcConnectorTest, TestDefaultDirectoryFileExistsPath) {
    auto connector = createConnector();

    // Set config to default value
    config::jdbc_drivers_dir = "/tmp/test_doris/plugins/jdbc_drivers";

    // Create the target directory and file for testing
    std::string dir = "/tmp/test_doris/plugins/jdbc_drivers";
    std::string file_path = dir + "/existing-driver.jar";

    // Create directory
    std::filesystem::create_directories(dir);

    // Create test file
    std::ofstream file(file_path);
    file << "test content";
    file.close();

    std::string result_url;
    Status status =
            connector._check_and_return_default_driver_url("existing-driver.jar", &result_url);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "file://" + file_path);

    // Cleanup
    std::filesystem::remove(file_path);
    std::filesystem::remove_all(dir);
}

// Simplified test without cloud mode dependency
TEST_F(JdbcConnectorTest, TestCloudModeSimulation) {
    auto connector = createConnector();

    // Set config to default value
    config::jdbc_drivers_dir = "/tmp/test_doris/plugins/jdbc_drivers";

    std::string result_url;
    Status status = connector._check_and_return_default_driver_url("cloud-driver.jar", &result_url);

    // Should process successfully and return fallback path
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(result_url.find("file://") == 0);
    EXPECT_TRUE(result_url.find("jdbc_drivers/cloud-driver.jar") != std::string::npos);
}

TEST_F(JdbcConnectorTest, TestFallbackToOldDirectory) {
    auto connector = createConnector();

    // Set config to default value but file doesn't exist in new directory
    config::jdbc_drivers_dir = "/tmp/test_doris/plugins/jdbc_drivers";

    std::string result_url;
    Status status =
            connector._check_and_return_default_driver_url("fallback-driver.jar", &result_url);

    EXPECT_TRUE(status.ok());
    // Should fallback to old directory when file not found and not in cloud mode
    EXPECT_EQ(result_url, "file:///tmp/test_doris/jdbc_drivers/fallback-driver.jar");
}

TEST_F(JdbcConnectorTest, TestPathConstruction) {
    auto connector = createConnector();

    // Test different DORIS_HOME values
    setenv("DORIS_HOME", "/test/doris", 1);

    // Set to default config
    config::jdbc_drivers_dir = "/test/doris/plugins/jdbc_drivers";

    std::string result_url;
    Status status = connector._check_and_return_default_driver_url("test.jar", &result_url);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "file:///test/doris/jdbc_drivers/test.jar"); // Fallback path
}

TEST_F(JdbcConnectorTest, TestEdgeCases) {
    auto connector = createConnector();
    std::string result_url;

    // Test empty URL
    Status status = connector._get_real_url("", &result_url);
    EXPECT_TRUE(status.ok()); // Should be treated as relative URL

    // Test URL with just colon (no slash after)
    status = connector._get_real_url("invalid:url", &result_url);
    EXPECT_TRUE(status.ok()); // Should be treated as relative URL

    // Test URL with spaces
    status = connector._get_real_url("my driver.jar", &result_url);
    EXPECT_TRUE(status.ok()); // Should be treated as relative URL
}

TEST_F(JdbcConnectorTest, TestMultipleCallsConsistency) {
    auto connector = createConnector();

    config::jdbc_drivers_dir = "/tmp/test_doris/plugins/jdbc_drivers";

    std::string result_url1, result_url2;
    Status status1 =
            connector._check_and_return_default_driver_url("same-driver.jar", &result_url1);
    Status status2 =
            connector._check_and_return_default_driver_url("same-driver.jar", &result_url2);

    EXPECT_TRUE(status1.ok());
    EXPECT_TRUE(status2.ok());
    EXPECT_EQ(result_url1, result_url2); // Should be consistent
}

TEST_F(JdbcConnectorTest, TestUrlDetectionLogic) {
    auto connector = createConnector();
    std::string result_url;

    // Test various URL patterns that should be detected as absolute
    std::vector<std::string> absolute_urls = {
            "http://example.com/driver.jar", "https://example.com/driver.jar",
            "s3://bucket/driver.jar", "file:///local/driver.jar", "ftp://server/driver.jar"};

    for (const auto& url : absolute_urls) {
        Status status = connector._get_real_url(url, &result_url);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(result_url, url);
    }

    // Test patterns that should be treated as relative
    std::vector<std::string> relative_urls = {"driver.jar", "path/driver.jar", "invalid:no-slash"};

    for (const auto& url : relative_urls) {
        Status status = connector._get_real_url(url, &result_url);
        // Should process through _check_and_return_default_driver_url
        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(result_url.find(url) != std::string::npos);
    }
}

} // namespace doris::vectorized