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

#include "runtime/user_function_cache.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <string>
#include <vector>

#include "common/status.h"

namespace doris {

class UserFunctionCacheTest : public ::testing::Test {
protected:
    UserFunctionCache ufc;

    void SetUp() override {
        // Save original DORIS_HOME
        original_doris_home_ = getenv("DORIS_HOME");
    }

    void TearDown() override {
        // Restore original DORIS_HOME
        if (original_doris_home_) {
            setenv("DORIS_HOME", original_doris_home_, 1);
        } else {
            unsetenv("DORIS_HOME");
        }
    }

private:
    const char* original_doris_home_ = nullptr;
};

TEST_F(UserFunctionCacheTest, SplitStringByChecksumTest) {
    // Test valid string format
    std::string valid_str =
            "7119053928154065546.20c8228267b6c9ce620fddb39467d3eb.postgresql-42.5.0.jar";
    auto result = ufc._split_string_by_checksum(valid_str);
    ASSERT_EQ(result.size(), 4);
    EXPECT_EQ(result[0], "7119053928154065546");
    EXPECT_EQ(result[1], "20c8228267b6c9ce620fddb39467d3eb");
    EXPECT_EQ(result[2], "postgresql-42.5.0");
    EXPECT_EQ(result[3], "jar");
}

// Test _get_real_url method
TEST_F(UserFunctionCacheTest, TestGetRealUrlWithAbsoluteUrl) {
    std::string result_url;

    // Test with absolute URL (contains ":/")
    Status status = ufc._get_real_url("http://example.com/udf.jar", &result_url);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "http://example.com/udf.jar");

    // Test with S3 URL
    status = ufc._get_real_url("s3://bucket/path/udf.jar", &result_url);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "s3://bucket/path/udf.jar");

    // Test with file URL
    status = ufc._get_real_url("file:///path/to/udf.jar", &result_url);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "file:///path/to/udf.jar");

    // Test with https URL
    status = ufc._get_real_url("https://secure.example.com/udf.jar", &result_url);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "https://secure.example.com/udf.jar");
}

TEST_F(UserFunctionCacheTest, TestGetRealUrlWithRelativeUrl) {
    std::string result_url;

    // Set DORIS_HOME for testing
    setenv("DORIS_HOME", "/tmp/test_doris", 1);

    // Test with relative URL (no ":/" found) - should call _check_and_return_default_java_udf_url
    Status status = ufc._get_real_url("my-udf.jar", &result_url);

    // This should process successfully in non-cloud mode
    EXPECT_TRUE(status.ok() || !result_url.empty());
}

// Test _check_and_return_default_java_udf_url method
TEST_F(UserFunctionCacheTest, TestCheckAndReturnDefaultJavaUdfUrlNonCloudMode) {
    // Set DORIS_HOME environment variable for testing
    setenv("DORIS_HOME", "/tmp/doris_test", 1);

    std::string result_url;
    Status status = ufc._check_and_return_default_java_udf_url("test-udf.jar", &result_url);

    // In non-cloud mode, should return file:// URL
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(result_url.find("file://") == 0);
    EXPECT_TRUE(result_url.find("test-udf.jar") != std::string::npos);
    EXPECT_TRUE(result_url.find("/plugins/java_udf/") != std::string::npos);
}

TEST_F(UserFunctionCacheTest, TestPathConstruction) {
    // Set DORIS_HOME environment variable
    setenv("DORIS_HOME", "/test/doris", 1);

    std::string result_url;
    Status status = ufc._check_and_return_default_java_udf_url("my-udf.jar", &result_url);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "file:///test/doris/plugins/java_udf/my-udf.jar");

    // Test with nested path
    status = ufc._check_and_return_default_java_udf_url("nested/path/udf.jar", &result_url);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "file:///test/doris/plugins/java_udf/nested/path/udf.jar");
}

TEST_F(UserFunctionCacheTest, TestEdgeCases) {
    setenv("DORIS_HOME", "/tmp/test", 1);

    std::string result_url;

    // Test empty URL
    Status status = ufc._get_real_url("", &result_url);
    // Empty string should be treated as relative URL
    EXPECT_TRUE(status.ok());

    // Test URL with just colon (no slash after)
    status = ufc._get_real_url("invalid:url", &result_url);
    // Should be treated as relative URL since it doesn't contain ":/"
    EXPECT_TRUE(status.ok());

    // Test URL with spaces
    status = ufc._get_real_url("my udf.jar", &result_url);
    // Should be treated as relative URL
    EXPECT_TRUE(status.ok());
}

TEST_F(UserFunctionCacheTest, TestUrlDetectionLogic) {
    std::string result_url;

    // Test various URL patterns that should be detected as absolute
    std::vector<std::string> absolute_urls = {
            "http://example.com/file.jar", "https://example.com/file.jar", "s3://bucket/file.jar",
            "file:///local/file.jar", "ftp://server/file.jar"};

    for (const auto& url : absolute_urls) {
        Status status = ufc._get_real_url(url, &result_url);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(result_url, url);
    }

    // Test patterns that should be treated as relative
    std::vector<std::string> relative_urls = {"file.jar", "path/file.jar", "invalid:no-slash", ""};

    setenv("DORIS_HOME", "/tmp", 1);

    for (const auto& url : relative_urls) {
        Status status = ufc._get_real_url(url, &result_url);
        // Should process through _check_and_return_default_java_udf_url
        EXPECT_TRUE(status.ok());
        if (!url.empty()) {
            EXPECT_TRUE(result_url.find(url) != std::string::npos);
        }
    }
}

TEST_F(UserFunctionCacheTest, TestConsistentBehavior) {
    setenv("DORIS_HOME", "/consistent/test", 1);

    std::string result_url1, result_url2;

    // Multiple calls should return consistent results
    Status status1 = ufc._check_and_return_default_java_udf_url("same-udf.jar", &result_url1);
    Status status2 = ufc._check_and_return_default_java_udf_url("same-udf.jar", &result_url2);

    EXPECT_TRUE(status1.ok());
    EXPECT_TRUE(status2.ok());
    EXPECT_EQ(result_url1, result_url2);
}

} // namespace doris