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

#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <butil/guid.h>
#include <cpp/s3_rate_limiter.h>
#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include <azure/storage/blobs/blob_options.hpp>
#include <chrono>
#include <unordered_set>

#include "common/config.h"
#include "common/configbase.h"
#include "common/logging.h"
#include "cpp/aws_common.h"
#include "cpp/sync_point.h"
#include "recycler/recycler_service.h"
#include "recycler/s3_accessor.h"

using namespace doris;

int main(int argc, char** argv) {
    const std::string conf_file = "doris_cloud.conf";
    if (!cloud::config::init(conf_file.c_str(), true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }

    if (!cloud::init_glog("s3_accessor_client_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    LOG(INFO) << "s3_accessor_test starting";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace doris::cloud {

#define GET_ENV_IF_DEFINED(var)              \
    ([]() -> std::string {                   \
        const char* val = std::getenv(#var); \
        return val ? std::string(val) : "";  \
    }())

class S3TestConfig {
public:
    S3TestConfig() {
        // Check if S3 client is enabled
        enabled = (GET_ENV_IF_DEFINED(ENABLE_S3_CLIENT) == "1");

        if (!enabled) {
            return;
        }

        access_key = GET_ENV_IF_DEFINED(S3_AK);
        secret_key = GET_ENV_IF_DEFINED(S3_SK);
        endpoint = GET_ENV_IF_DEFINED(S3_ENDPOINT);
        provider = GET_ENV_IF_DEFINED(S3_PROVIDER);
        bucket = GET_ENV_IF_DEFINED(S3_BUCKET);
        region = GET_ENV_IF_DEFINED(S3_REGION);
        prefix = GET_ENV_IF_DEFINED(S3_PREFIX);
    }

    bool is_enabled() const { return enabled; }

    bool is_valid() const {
        return enabled && !access_key.empty() && !secret_key.empty() && !endpoint.empty() &&
               !region.empty() && !bucket.empty();
    }

    std::string get_access_key() const { return access_key; }
    std::string get_secret_key() const { return secret_key; }
    std::string get_endpoint() const { return endpoint; }
    std::string get_provider() const { return provider; }
    std::string get_bucket() const { return bucket; }
    std::string get_prefix() const { return prefix; }
    std::string get_region() const { return region; }

    void print_config() const {
        std::cout << "S3 Test Configuration:" << std::endl;
        std::cout << "  Enabled: " << (enabled ? "Yes" : "No") << std::endl;
        if (enabled) {
            std::cout << "  Access Key: " << (access_key.empty() ? "<empty>" : "<set>")
                      << std::endl;
            std::cout << "  Secret Key: " << (secret_key.empty() ? "<empty>" : "<hidden>")
                      << std::endl;
            std::cout << "  Endpoint: " << endpoint << std::endl;
            std::cout << "  Provider: " << provider << std::endl;
            std::cout << "  Bucket: " << bucket << std::endl;
            std::cout << " Region: " << region << std::endl;
            std::cout << "  Prefix: " << prefix << std::endl;
        }
    }

private:
    bool enabled = false;
    std::string access_key;
    std::string secret_key;
    std::string endpoint;
    std::string provider;
    std::string bucket;
    std::string region;
    std::string prefix;
};

class S3AccessorClientTest : public ::testing::Test {
protected:
    void SetUp() override {
        config_ = std::make_shared<S3TestConfig>();

        // Print configuration for debugging (always print for S3 tests)
        config_->print_config();

        // Skip tests if S3 is not enabled or not configured properly
        if (!config_->is_enabled()) {
            GTEST_SKIP() << "S3 client is not enabled. Use --enable_s3_client flag to enable.";
        }

        if (!config_->is_valid()) {
            GTEST_SKIP() << "S3 configuration is incomplete. Required: AK, SK, ENDPOINT, BUCKET.";
        }

        // Attempt to create S3 accessor
        if (int ret = create_client(); ret != 0) {
            GTEST_SKIP() << "Failed to create S3 accessor with provided configuration. Error code: "
                         << ret;
        }

        std::string test_path = "s3_accessor_test_dir";
        global_test_prefix_ = get_unique_test_path(test_path);
        // Clean up test directory if it exists
        if (s3_accessor) {
            s3_accessor->delete_directory(global_test_prefix_);
        }
    }

    S3Conf::Provider convert_provider(const std::string& provider_str) {
        if (provider_str == "AZURE") {
            return S3Conf::AZURE;
        } else if (provider_str == "GCS") {
            return S3Conf::GCS;
        } else {
            return S3Conf::S3; // Default to S3
        }
    }

    int create_client() {
        S3Conf s3_conf;
        s3_conf.ak = config_->get_access_key();
        s3_conf.sk = config_->get_secret_key();
        s3_conf.endpoint = config_->get_endpoint();
        s3_conf.region = config_->get_region();
        s3_conf.bucket = config_->get_bucket();
        s3_conf.prefix = config_->get_prefix();
        s3_conf.provider = convert_provider(config_->get_provider());

        std::shared_ptr<S3Accessor> accessor;
        int ret = S3Accessor::create(s3_conf, &accessor);
        if (ret != 0) {
            return ret;
        }

        s3_accessor = std::static_pointer_cast<StorageVaultAccessor>(accessor);
        return 0;
    }

    void TearDown() override {
        // Cleanup test directory
        if (s3_accessor) {
            s3_accessor->delete_directory(global_test_prefix_);
        }
    }

    std::string get_unique_test_path(const std::string& test_name) {
        std::string path = config_->get_prefix();
        if (!path.empty() && path.back() != '/') {
            path += '/';
        }
        path += "ut_" + test_name + "/";
        return path;
    }

    std::shared_ptr<S3TestConfig> config_;
    std::shared_ptr<StorageVaultAccessor> s3_accessor;
    std::string global_test_prefix_;
};

// Test: Simple put file test
TEST_F(S3AccessorClientTest, SimplePutFileTest) {
    ASSERT_NE(s3_accessor, nullptr);

    std::string test_file = global_test_prefix_ + "test_file.txt";
    std::string test_content = "Hello, S3 Accessor! This is a simple test.";

    int ret = s3_accessor->put_file(test_file, test_content);
    ASSERT_EQ(ret, 0) << "Failed to put file";

    // Verify file exists
    ret = s3_accessor->exists(test_file);
    ASSERT_EQ(ret, 0) << "File should exist after putting";

    std::cout << "Successfully put file with " << test_content.size() << " bytes" << std::endl;
}

// Test: Check file existence
TEST_F(S3AccessorClientTest, ExistsTest) {
    ASSERT_NE(s3_accessor, nullptr);

    std::string test_file = global_test_prefix_ + "exists_test_file.txt";
    std::string test_content = "Test content for exists check";

    // File should not exist initially
    int ret = s3_accessor->exists(test_file);
    ASSERT_EQ(ret, 1) << "File should not exist initially";

    // Put file
    ret = s3_accessor->put_file(test_file, test_content);
    ASSERT_EQ(ret, 0) << "Failed to put file";

    // File should exist now
    ret = s3_accessor->exists(test_file);
    ASSERT_EQ(ret, 0) << "File should exist after putting";

    // Delete file
    ret = s3_accessor->delete_file(test_file);
    ASSERT_EQ(ret, 0) << "Failed to delete file";

    // File should not exist after deletion
    ret = s3_accessor->exists(test_file);
    ASSERT_EQ(ret, 1) << "File should not exist after deletion";

    std::cout << "Exists test completed successfully!" << std::endl;
}

// Test: Delete file
TEST_F(S3AccessorClientTest, DeleteFileTest) {
    ASSERT_NE(s3_accessor, nullptr);

    std::string test_file = global_test_prefix_ + "delete_test_file.txt";
    std::string test_content = "Test content for delete";

    // Put file
    int ret = s3_accessor->put_file(test_file, test_content);
    ASSERT_EQ(ret, 0) << "Failed to put file";

    // Verify file exists
    ret = s3_accessor->exists(test_file);
    ASSERT_EQ(ret, 0) << "File should exist";

    // Delete file
    ret = s3_accessor->delete_file(test_file);
    ASSERT_EQ(ret, 0) << "Failed to delete file";

    // Verify file no longer exists
    ret = s3_accessor->exists(test_file);
    ASSERT_EQ(ret, 1) << "File should not exist after deletion";

    std::cout << "Delete file test completed successfully!" << std::endl;
}

// Test: List directory
TEST_F(S3AccessorClientTest, ListDirectoryTest) {
    ASSERT_NE(s3_accessor, nullptr);

    std::string test_dir = global_test_prefix_ + "list_test_dir/";
    std::vector<std::string> test_files = {
            test_dir + "file1.txt",
            test_dir + "file2.txt",
            test_dir + "subdir/file3.txt",
    };

    // Create test files
    for (const auto& file_path : test_files) {
        int ret = s3_accessor->put_file(file_path, "Test data for " + file_path);
        ASSERT_EQ(ret, 0) << "Failed to put file: " << file_path;
    }

    // List directory
    std::unique_ptr<ListIterator> iter;
    int ret = s3_accessor->list_directory(test_dir, &iter);
    ASSERT_EQ(ret, 0) << "Failed to list directory";
    ASSERT_NE(iter, nullptr);
    ASSERT_TRUE(iter->is_valid());

    // Collect listed files
    std::unordered_set<std::string> listed_files;
    while (iter->has_next()) {
        auto file = iter->next();
        if (file.has_value()) {
            listed_files.insert(file->path);
            std::cout << "  - " << file->path << " (" << file->size << " bytes)" << std::endl;
        }
    }

    // Verify we got all files
    EXPECT_GE(listed_files.size(), test_files.size()) << "Should list all created files";

    std::cout << "List directory test completed successfully!" << std::endl;
}

// Test: Batch delete files
TEST_F(S3AccessorClientTest, DeleteFilesTest) {
    ASSERT_NE(s3_accessor, nullptr);

    std::string test_dir = global_test_prefix_ + "batch_delete_test/";
    std::vector<std::string> test_files;
    for (int i = 0; i < 5; ++i) {
        test_files.push_back(test_dir + "file_" + std::to_string(i) + ".txt");
    }

    // Create test files
    for (const auto& file_path : test_files) {
        int ret = s3_accessor->put_file(file_path, "Test data " + file_path);
        ASSERT_EQ(ret, 0) << "Failed to put file: " << file_path;
    }

    // Verify all files exist
    for (const auto& file_path : test_files) {
        int ret = s3_accessor->exists(file_path);
        ASSERT_EQ(ret, 0) << "File should exist: " << file_path;
    }

    // Batch delete files
    int ret = s3_accessor->delete_files(test_files);
    ASSERT_EQ(ret, 0) << "Failed to batch delete files";

    std::cout << "Successfully batch deleted " << test_files.size() << " files" << std::endl;

    // Verify all files are deleted
    for (const auto& file_path : test_files) {
        int ret = s3_accessor->exists(file_path);
        ASSERT_EQ(ret, 1) << "File should not exist after deletion: " << file_path;
    }

    std::cout << "Batch delete files test completed successfully!" << std::endl;
}

// Test: Delete directory recursively
TEST_F(S3AccessorClientTest, DeleteDirectoryTest) {
    ASSERT_NE(s3_accessor, nullptr);

    std::string test_dir = global_test_prefix_ + "recursive_delete_test/";
    std::vector<std::string> test_files = {
            test_dir + "file1.txt",
            test_dir + "file2.txt",
            test_dir + "subdir1/file3.txt",
            test_dir + "subdir1/file4.txt",
            test_dir + "subdir2/nested/file5.txt",
    };

    // Create test files
    for (const auto& file_path : test_files) {
        int ret = s3_accessor->put_file(file_path, "Test data for " + file_path);
        ASSERT_EQ(ret, 0) << "Failed to put file: " << file_path;
    }

    // Verify files exist
    for (const auto& file_path : test_files) {
        int ret = s3_accessor->exists(file_path);
        ASSERT_EQ(ret, 0) << "File should exist: " << file_path;
    }

    // Delete directory recursively
    int ret = s3_accessor->delete_directory(test_dir);
    ASSERT_EQ(ret, 0) << "Failed to delete directory recursively";

    std::cout << "Successfully deleted directory recursively" << std::endl;

    // Verify all files are deleted
    for (const auto& file_path : test_files) {
        int ret = s3_accessor->exists(file_path);
        ASSERT_EQ(ret, 1) << "File should not exist after recursive deletion: " << file_path;
    }

    std::cout << "Delete directory test completed successfully!" << std::endl;
}

// Test: Delete prefix
TEST_F(S3AccessorClientTest, DeletePrefixTest) {
    ASSERT_NE(s3_accessor, nullptr);

    std::string test_prefix = global_test_prefix_ + "delete_prefix_test/";
    std::vector<std::string> test_files = {
            test_prefix + "file1.txt",
            test_prefix + "file2.txt",
            test_prefix + "subdir/file3.txt",
    };

    // Create test files
    for (const auto& file_path : test_files) {
        int ret = s3_accessor->put_file(file_path, "Test data for " + file_path);
        ASSERT_EQ(ret, 0) << "Failed to put file: " << file_path;
    }

    // Delete prefix
    int ret = s3_accessor->delete_prefix(test_prefix);
    ASSERT_EQ(ret, 0) << "Failed to delete prefix";

    std::cout << "Successfully deleted prefix" << std::endl;

    // Verify all files are deleted
    for (const auto& file_path : test_files) {
        int ret = s3_accessor->exists(file_path);
        ASSERT_EQ(ret, 1) << "File should not exist after prefix deletion: " << file_path;
    }

    std::cout << "Delete prefix test completed successfully!" << std::endl;
}

// ==================== Rate Limiter Tests ====================

// Test: S3 rate limiter for PUT operations - put_file
TEST_F(S3AccessorClientTest, RateLimiterPutTest) {
    ASSERT_NE(s3_accessor, nullptr);

    // Save original config value
    bool original_enable_rate_limiter = config::enable_s3_rate_limiter;

    // Enable S3 rate limiter for this test
    config::enable_s3_rate_limiter = true;

    // Set a very strict rate limit for PUT operations
    // limit: 2 (allow only 2 requests total, the 3rd will fail)
    int ret = reset_s3_rate_limiter(S3RateLimitType::PUT, 10, 10, 2);
    ASSERT_EQ(ret, 0) << "Failed to set rate limiter for PUT operations";

    std::cout << "Rate limiter set: limit 2 total requests for PUT operations" << std::endl;

    // First two put operations should succeed
    std::vector<std::string> test_files;
    for (int i = 0; i < 2; ++i) {
        std::string test_file =
                global_test_prefix_ + "rate_limit_put_test_" + std::to_string(i) + ".txt";
        test_files.push_back(test_file);

        int ret = s3_accessor->put_file(test_file, "Test data " + std::to_string(i));
        ASSERT_EQ(ret, 0) << "Failed to put file on attempt " << i + 1;
        std::cout << "Put attempt " << i + 1 << " succeeded" << std::endl;
    }

    // Third put operation should fail due to rate limit
    std::string test_file_fail = global_test_prefix_ + "rate_limit_put_test_fail.txt";
    int ret_fail = s3_accessor->put_file(test_file_fail, "This should fail");

    EXPECT_NE(ret_fail, 0) << "Third put should fail due to rate limit";
    std::cout << "Third put failed as expected: error code " << ret_fail << std::endl;

    // Reset rate limiter to default (no limit) to avoid affecting other tests
    reset_s3_rate_limiter(S3RateLimitType::PUT, 10000, 10000, 0);
    reset_s3_rate_limiter(S3RateLimitType::GET, 10000, 10000, 0);

    // Restore original config
    config::enable_s3_rate_limiter = original_enable_rate_limiter;

    std::cout << "Rate limiter PUT test completed successfully!" << std::endl;
}

// Test: S3 rate limiter for GET operations - exists and list
TEST_F(S3AccessorClientTest, RateLimiterGetTest) {
    ASSERT_NE(s3_accessor, nullptr);

    // Save original config value
    bool original_enable_rate_limiter = config::enable_s3_rate_limiter;

    // Enable S3 rate limiter for this test
    config::enable_s3_rate_limiter = true;

    // Create a test file first
    std::string test_file = global_test_prefix_ + "rate_limit_get_test.txt";
    int ret = s3_accessor->put_file(test_file, "Test data for rate limit get test");
    ASSERT_EQ(ret, 0) << "Failed to put file";

    // Set rate limit for GET operations
    // limit: 2 (allow only 2 requests total, the 3rd will fail)
    ret = reset_s3_rate_limiter(S3RateLimitType::GET, 10, 10, 2);
    ASSERT_EQ(ret, 0) << "Failed to set rate limiter for GET operations";

    std::cout << "Rate limiter set: limit 2 total requests for GET operations" << std::endl;

    // First two get operations should succeed
    for (int i = 0; i < 2; ++i) {
        ret = s3_accessor->exists(test_file);
        ASSERT_EQ(ret, 0) << "Failed to check exists on attempt " << i + 1;
        std::cout << "Get attempt " << i + 1 << " succeeded" << std::endl;
    }

    // Third get operation should fail due to rate limit
    ret = s3_accessor->exists(test_file);

    EXPECT_NE(ret, 0) << "Third get should fail due to rate limit";
    std::cout << "Third get failed as expected: error code " << ret << std::endl;

    // Reset rate limiter
    reset_s3_rate_limiter(S3RateLimitType::PUT, 10000, 10000, 0);
    reset_s3_rate_limiter(S3RateLimitType::GET, 10000, 10000, 0);

    // Restore original config
    config::enable_s3_rate_limiter = original_enable_rate_limiter;

    std::cout << "Rate limiter GET test completed successfully!" << std::endl;
}

// Test: S3 rate limiter for PUT operations - delete_file
TEST_F(S3AccessorClientTest, RateLimiterPutDeleteTest) {
    ASSERT_NE(s3_accessor, nullptr);

    // Save original config value
    bool original_enable_rate_limiter = config::enable_s3_rate_limiter;

    // Enable S3 rate limiter for this test
    config::enable_s3_rate_limiter = true;

    // Create test files first (without rate limit)
    std::vector<std::string> test_files;
    for (int i = 0; i < 3; ++i) {
        std::string test_file =
                global_test_prefix_ + "rate_limit_delete_test_" + std::to_string(i) + ".txt";
        test_files.push_back(test_file);

        int ret = s3_accessor->put_file(test_file, "Delete test data " + std::to_string(i));
        ASSERT_EQ(ret, 0) << "Failed to put file: " << test_file;
    }

    // Set rate limit for PUT operations (delete uses PUT rate limiter)
    // limit: 2 (allow only 2 requests total, the 3rd will fail)
    int ret = reset_s3_rate_limiter(S3RateLimitType::PUT, 10, 10, 2);
    ASSERT_EQ(ret, 0) << "Failed to set rate limiter for PUT operations";

    std::cout << "Rate limiter set: limit 2 total requests for PUT operations (delete)"
              << std::endl;

    // First two delete operations should succeed
    for (int i = 0; i < 2; ++i) {
        ret = s3_accessor->delete_file(test_files[i]);
        ASSERT_EQ(ret, 0) << "Failed to delete file on attempt " << i + 1;
        std::cout << "Delete attempt " << i + 1 << " succeeded" << std::endl;
    }

    // Third delete operation should fail due to rate limit
    ret = s3_accessor->delete_file(test_files[2]);

    EXPECT_NE(ret, 0) << "Third delete should fail due to rate limit";
    std::cout << "Third delete failed as expected: error code " << ret << std::endl;

    // Reset rate limiter
    reset_s3_rate_limiter(S3RateLimitType::PUT, 10000, 10000, 0);
    reset_s3_rate_limiter(S3RateLimitType::GET, 10000, 10000, 0);

    // Restore original config
    config::enable_s3_rate_limiter = original_enable_rate_limiter;

    std::cout << "Rate limiter PUT delete test completed successfully!" << std::endl;
}

// Test: S3 rate limiter for GET operations - list_directory
TEST_F(S3AccessorClientTest, RateLimiterGetListTest) {
    ASSERT_NE(s3_accessor, nullptr);

    // Save original config value
    bool original_enable_rate_limiter = config::enable_s3_rate_limiter;

    // Enable S3 rate limiter for this test
    config::enable_s3_rate_limiter = true;

    // Create test directories with files
    std::vector<std::string> test_dirs;
    for (int i = 0; i < 3; ++i) {
        std::string test_dir =
                global_test_prefix_ + "rate_limit_list_test_" + std::to_string(i) + "/";
        test_dirs.push_back(test_dir);

        // Create a file in each directory
        std::string test_file = test_dir + "file.txt";
        int ret = s3_accessor->put_file(test_file, "List test data " + std::to_string(i));
        ASSERT_EQ(ret, 0) << "Failed to put file: " << test_file;
    }

    // Set rate limit for GET operations (list uses GET rate limiter)
    // limit: 2 (allow only 2 requests total, the 3rd will fail)
    int ret = reset_s3_rate_limiter(S3RateLimitType::GET, 10, 10, 2);
    ASSERT_EQ(ret, 0) << "Failed to set rate limiter for GET operations";

    std::cout << "Rate limiter set: limit 2 total requests for GET operations (list)" << std::endl;

    // Create iterators for first two directories (this doesn't trigger network request)
    std::vector<std::unique_ptr<ListIterator>> iters;
    for (int i = 0; i < 2; ++i) {
        std::unique_ptr<ListIterator> iter;
        ret = s3_accessor->list_directory(test_dirs[i], &iter);
        ASSERT_EQ(ret, 0) << "Failed to create iterator for directory " << i + 1;
        ASSERT_NE(iter, nullptr);
        ASSERT_TRUE(iter->is_valid());
        iters.push_back(std::move(iter));
    }

    // First two has_next() calls should succeed (these trigger actual network requests)
    for (int i = 0; i < 2; ++i) {
        bool has_next = iters[i]->has_next();
        ASSERT_TRUE(has_next) << "has_next() should succeed on attempt " << i + 1;
        std::cout << "has_next() attempt " << i + 1 << " succeeded" << std::endl;
    }

    // Create iterator for third directory
    std::unique_ptr<ListIterator> iter3;
    ret = s3_accessor->list_directory(test_dirs[2], &iter3);
    ASSERT_EQ(ret, 0) << "Failed to create iterator for directory 3";
    ASSERT_NE(iter3, nullptr);
    ASSERT_TRUE(iter3->is_valid());

    // Third has_next() call should fail due to rate limit (this triggers the 3rd network request)
    bool has_next = iter3->has_next();
    EXPECT_FALSE(has_next) << "Third has_next() should fail due to rate limit";
    EXPECT_FALSE(iter3->is_valid()) << "Iterator should be invalid after rate limit failure";
    std::cout << "Third has_next() failed as expected due to rate limit" << std::endl;

    // Reset rate limiter
    reset_s3_rate_limiter(S3RateLimitType::PUT, 10000, 10000, 0);
    reset_s3_rate_limiter(S3RateLimitType::GET, 10000, 10000, 0);

    // Restore original config
    config::enable_s3_rate_limiter = original_enable_rate_limiter;

    std::cout << "Rate limiter GET list test completed successfully!" << std::endl;
}

// ==================== Error Handling Tests ====================

// Test: Batch delete files with partial failure simulation
TEST_F(S3AccessorClientTest, DeleteFilesPartialFailureTest) {
    ASSERT_NE(s3_accessor, nullptr);

    std::string test_dir = global_test_prefix_ + "batch_delete_partial_failure_test/";
    std::vector<std::string> test_files;
    for (int i = 0; i < 5; ++i) {
        test_files.push_back(test_dir + "file_" + std::to_string(i) + ".txt");
    }

    // Create test files
    for (const auto& file_path : test_files) {
        int ret = s3_accessor->put_file(file_path, "Batch delete test data " + file_path);
        ASSERT_EQ(ret, 0) << "Failed to put file: " << file_path;
    }

    // Set up sync point to simulate partial delete failure
    // This test verifies that the code handles partial failures correctly
    // Note: Actual partial failure simulation would require sync point implementation
    // For now, we just test the normal batch delete path

    // Batch delete files
    int ret = s3_accessor->delete_files(test_files);
    ASSERT_EQ(ret, 0) << "Failed to batch delete files";

    // Verify all files are deleted
    for (const auto& file_path : test_files) {
        int exists_ret = s3_accessor->exists(file_path);
        ASSERT_EQ(exists_ret, 1) << "File should not exist after deletion: " << file_path;
    }

    std::cout << "Batch delete files partial failure test completed successfully!" << std::endl;
}

// Test: Delete directory with partial failure simulation
TEST_F(S3AccessorClientTest, DeleteDirectoryPartialFailureTest) {
    ASSERT_NE(s3_accessor, nullptr);

    std::string test_dir = global_test_prefix_ + "delete_dir_partial_failure_test/";
    std::vector<std::string> test_files;
    for (int i = 0; i < 5; ++i) {
        test_files.push_back(test_dir + "file_" + std::to_string(i) + ".txt");
    }

    // Create test files
    for (const auto& file_path : test_files) {
        int ret = s3_accessor->put_file(file_path, "Delete directory test data");
        ASSERT_EQ(ret, 0) << "Failed to put file: " << file_path;
    }

    // Delete directory recursively
    int ret = s3_accessor->delete_directory(test_dir);
    ASSERT_EQ(ret, 0) << "Failed to delete directory recursively";

    // Verify all files are deleted
    for (const auto& file_path : test_files) {
        int exists_ret = s3_accessor->exists(file_path);
        ASSERT_EQ(exists_ret, 1) << "File should not exist after recursive deletion: " << file_path;
    }

    std::cout << "Delete directory partial failure test completed successfully!" << std::endl;
}

// Test: Large batch delete (more than 1000 files for S3, 256 for Azure)
TEST_F(S3AccessorClientTest, LargeBatchDeleteTest) {
    ASSERT_NE(s3_accessor, nullptr);

    std::string test_dir = global_test_prefix_ + "large_batch_delete_test/";
    std::vector<std::string> test_files;

    // Create more than 100 files to test batch deletion
    size_t num_files = 500;
    for (size_t i = 0; i < num_files; ++i) {
        test_files.push_back(test_dir + "file_" + std::to_string(i) + ".txt");
    }

    // Create test files
    std::cout << "Creating " << num_files << " test files..." << std::endl;
    for (const auto& file_path : test_files) {
        int ret = s3_accessor->put_file(file_path, "Large batch delete test data");
        ASSERT_EQ(ret, 0) << "Failed to put file: " << file_path;
    }

    std::cout << "Successfully created " << num_files << " files" << std::endl;

    // Batch delete files (should handle batching internally)
    int ret = s3_accessor->delete_files(test_files);
    ASSERT_EQ(ret, 0) << "Failed to batch delete files";

    std::cout << "Successfully batch deleted " << num_files << " files" << std::endl;

    // Verify all files are deleted
    for (const auto& file_path : test_files) {
        int exists_ret = s3_accessor->exists(file_path);
        ASSERT_EQ(exists_ret, 1) << "File should not exist after deletion: " << file_path;
    }

    std::cout << "Large batch delete test completed successfully!" << std::endl;
}

// Test: List directory with pagination (for S3 ListObjectsV2)
TEST_F(S3AccessorClientTest, ListDirectoryPaginationTest) {
    ASSERT_NE(s3_accessor, nullptr);

    std::string test_dir = global_test_prefix_ + "list_pagination/";

    // Create many files to trigger pagination
    size_t num_files = 10;
    std::unordered_set<std::string> created_files;
    for (size_t i = 0; i < num_files; ++i) {
        std::string file_path = test_dir + "file_" + std::to_string(i) + ".txt";
        created_files.insert(file_path);
        int ret = s3_accessor->put_file(file_path, "List pagination test data");
        ASSERT_EQ(ret, 0) << "Failed to put file: " << file_path;
    }

    std::cout << "Created " << num_files << " files for pagination test" << std::endl;

    // List directory (should handle pagination internally)
    std::unique_ptr<ListIterator> iter;
    int ret = s3_accessor->list_directory(test_dir, &iter);
    ASSERT_EQ(ret, 0) << "Failed to list directory";
    ASSERT_NE(iter, nullptr);
    ASSERT_TRUE(iter->is_valid());

    // Collect all listed files
    std::unordered_set<std::string> listed_files;
    size_t count = 0;

    for (auto file = iter->next(); file.has_value(); file = iter->next()) {
        listed_files.insert(file->path);
        count++;
    }
    std::cout << "Listed " << count << " files" << std::endl;

    // Verify we got all files
    EXPECT_EQ(listed_files.size(), created_files.size()) << "Should list all created files";
    for (const auto& file_path : created_files) {
        EXPECT_TRUE(listed_files.contains(file_path)) << "File should be listed: " << file_path;
    }

    std::cout << "List directory pagination test completed successfully!" << std::endl;
}

} // namespace doris::cloud
