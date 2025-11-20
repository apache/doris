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

#include "io/fs/s3_file_system.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteObjectsResult.h>
#include <aws/s3/model/Error.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <ios>
#include <memory>
#include <string>

#include "common/config.h"
#include "cpp/sync_point.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/obj_storage_client.h"
#include "runtime/exec_env.h"
#include "util/s3_util.h"

namespace doris {

#define GET_ENV_IF_DEFINED(var)              \
    ([]() -> std::string {                   \
        const char* val = std::getenv(#var); \
        return val ? std::string(val) : "";  \
    }())

static const char* MOCK_S3_FS_ID = "mock_s3_fs_id_6";

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

class S3FileSystemTest : public ::testing::Test {
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

        // Attempt to create S3 client
        if (auto st = create_client(); !st.ok()) {
            GTEST_SKIP() << "Failed to create S3 client with provided configuration."
                         << st.to_string();
        }

        std::unique_ptr<ThreadPool> _pool;
        std::ignore = ThreadPoolBuilder("s3_upload_file_thread_pool")
                              .set_min_threads(5)
                              .set_max_threads(10)
                              .build(&_pool);
        ExecEnv::GetInstance()->_s3_file_upload_thread_pool = std::move(_pool);

        auto test_path = config_->get_prefix() == "" ? "s3_fs_test_dir" : config_->get_prefix();
        global_test_prefix_ = get_unique_test_path(test_path);
        auto status = s3_fs_->delete_directory(global_test_prefix_);
        EXPECT_TRUE(status.ok()) << "Failed to delete test file: " << status.to_string();
    }

    io::ObjStorageType convert_provider(const std::string& provider_str) {
        if (provider_str == "AZURE") {
            return io::ObjStorageType::AZURE;
        } else {
            return io::ObjStorageType::AWS; // Default to AWS S3
        }
    }

    Status create_client() {
        S3Conf s3_conf;
        s3_conf.bucket = config_->get_bucket();
        s3_conf.prefix = config_->get_prefix();
        s3_conf.client_conf.ak = config_->get_access_key();
        s3_conf.client_conf.sk = config_->get_secret_key();
        s3_conf.client_conf.endpoint = config_->get_endpoint();
        s3_conf.client_conf.bucket = config_->get_bucket();
        s3_conf.client_conf.region = config_->get_region();
        s3_conf.client_conf.provider = convert_provider(config_->get_provider());

        s3_fs_ = DORIS_TRY(io::S3FileSystem::create(s3_conf, MOCK_S3_FS_ID));
        return Status::OK();
    }

    void TearDown() override {
        // Cleanup resources
        ExecEnv::GetInstance()->_s3_file_upload_thread_pool.reset();
        if (s3_fs_) {
            auto status = s3_fs_->delete_directory(global_test_prefix_);
            EXPECT_TRUE(status.ok()) << "Failed to delete test file: " << status.to_string();
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
    std::shared_ptr<io::S3FileSystem> s3_fs_;
    std::string global_test_prefix_;
};

// Test: Simple put object test
TEST_F(S3FileSystemTest, SimplePutObjectTest) {
    ASSERT_NE(s3_fs_, nullptr);

    // Generate a unique test file path
    std::string test_file = global_test_prefix_ + "test_file.txt";
    std::cout << "Test file path: " << test_file << std::endl;

    // Create file writer
    io::FileWriterPtr writer;
    Status status = s3_fs_->create_file(test_file, &writer);
    ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.to_string();
    ASSERT_NE(writer, nullptr);

    // Write test data
    std::string test_data = "Hello, S3! This is a simple test.";
    status = writer->append(test_data);
    ASSERT_TRUE(status.ok()) << "Failed to write data: " << status.to_string();

    // Close the writer
    status = writer->close();
    ASSERT_TRUE(status.ok()) << "Failed to close writer: " << status.to_string();

    std::cout << "Successfully wrote " << test_data.size() << " bytes to S3" << std::endl;

    // Verify file exists
    bool exists = false;
    status = s3_fs_->exists(test_file, &exists);
    ASSERT_TRUE(status.ok()) << "Failed to check file existence: " << status.to_string();
    EXPECT_TRUE(exists) << "File should exist after writing";

    // Verify file size
    int64_t file_size = 0;
    status = s3_fs_->file_size(test_file, &file_size);
    ASSERT_TRUE(status.ok()) << "Failed to get file size: " << status.to_string();
    EXPECT_EQ(file_size, test_data.size()) << "File size mismatch";

    std::cout << "File size: " << file_size << " bytes" << std::endl;
    std::cout << "Test completed successfully!" << std::endl;

    // Cleanup test file
    status = s3_fs_->delete_file(test_file);
    ASSERT_TRUE(status.ok()) << "Failed to delete test file: " << status.to_string();
}

// Test: Large file with multipart upload
TEST_F(S3FileSystemTest, MultipartUploadTest) {
    ASSERT_NE(s3_fs_, nullptr);

    std::string test_file = global_test_prefix_ + "large_file.dat";
    std::cout << "Test file path: " << test_file << std::endl;

    // Create file writer
    io::FileWriterPtr writer;
    Status status = s3_fs_->create_file(test_file, &writer);
    ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.to_string();
    ASSERT_NE(writer, nullptr);

    // Write large data to trigger multipart upload (>5MB for multipart)
    size_t chunk_size = 1024 * 1024; // 1MB
    size_t num_chunks = 6;           // 6MB total
    std::string chunk_data(chunk_size, 'A');

    for (size_t i = 0; i < num_chunks; ++i) {
        // Vary the data pattern for each chunk
        std::ranges::fill(chunk_data, 'A' + (i % 26));
        status = writer->append(chunk_data);
        ASSERT_TRUE(status.ok()) << "Failed to write chunk " << i << ": " << status.to_string();
    }

    // Close the writer to complete multipart upload
    status = writer->close();
    ASSERT_TRUE(status.ok()) << "Failed to close writer: " << status.to_string();

    std::cout << "Successfully uploaded " << (chunk_size * num_chunks)
              << " bytes using multipart upload" << std::endl;

    // Verify file exists and has correct size
    bool exists = false;
    status = s3_fs_->exists(test_file, &exists);
    ASSERT_TRUE(status.ok()) << "Failed to check file existence: " << status.to_string();
    EXPECT_TRUE(exists) << "File should exist after multipart upload";

    int64_t file_size = 0;
    status = s3_fs_->file_size(test_file, &file_size);
    ASSERT_TRUE(status.ok()) << "Failed to get file size: " << status.to_string();
    EXPECT_EQ(file_size, chunk_size * num_chunks) << "File size mismatch";

    std::cout << "Multipart upload test completed successfully!" << std::endl;

    // Cleanup
    status = s3_fs_->delete_file(test_file);
    ASSERT_TRUE(status.ok()) << "Failed to delete test file: " << status.to_string();
}

// Test: List objects in directory
TEST_F(S3FileSystemTest, ListObjectsTest) {
    ASSERT_NE(s3_fs_, nullptr);

    std::string test_dir = global_test_prefix_ + "list_test_dir/";
    std::cout << "Test directory path: " << test_dir << std::endl;

    // Create multiple test files
    std::vector<std::string> test_files = {
            test_dir + "file1.txt",
            test_dir + "file2.txt",
            test_dir + "subdir/file3.txt",
    };

    for (const auto& file_path : test_files) {
        io::FileWriterPtr writer;
        Status status = s3_fs_->create_file(file_path, &writer);
        ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.to_string();

        std::string data = "Test data for " + file_path;
        status = writer->append(data);
        ASSERT_TRUE(status.ok()) << "Failed to write data: " << status.to_string();

        status = writer->close();
        ASSERT_TRUE(status.ok()) << "Failed to close writer: " << status.to_string();
    }

    // List files in directory
    bool exists = false;
    std::vector<io::FileInfo> files;
    Status status = s3_fs_->list(test_dir, true, &files, &exists);
    ASSERT_TRUE(status.ok()) << "Failed to list files: " << status.to_string();
    EXPECT_TRUE(exists);

    std::cout << "Found " << files.size() << " files" << std::endl;
    for (const auto& file : files) {
        std::cout << "  - " << file.file_name << " (" << file.file_size << " bytes)" << std::endl;
    }

    // Verify we got all files
    EXPECT_GE(files.size(), test_files.size()) << "Should list all created files";

    std::cout << "List objects test completed successfully!" << std::endl;

    // Cleanup
    status = s3_fs_->delete_directory(test_dir);
    ASSERT_TRUE(status.ok()) << "Failed to delete test directory: " << status.to_string();
}

// Test: Batch delete files
TEST_F(S3FileSystemTest, BatchDeleteTest) {
    ASSERT_NE(s3_fs_, nullptr);

    std::string test_dir = global_test_prefix_ + "batch_delete_test/";
    std::cout << "Test directory path: " << test_dir << std::endl;

    // Create multiple test files
    std::vector<std::string> test_files;
    for (int i = 0; i < 5; ++i) {
        std::string file_path = test_dir + "file_" + std::to_string(i) + ".txt";
        test_files.push_back(file_path);

        io::FileWriterPtr writer;
        Status status = s3_fs_->create_file(file_path, &writer);
        ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.to_string();

        std::string data = "Test data " + std::to_string(i);
        status = writer->append(data);
        ASSERT_TRUE(status.ok()) << "Failed to write data: " << status.to_string();

        status = writer->close();
        ASSERT_TRUE(status.ok()) << "Failed to close writer: " << status.to_string();
    }

    // Verify all files exist
    for (const auto& file_path : test_files) {
        bool exists = false;
        Status status = s3_fs_->exists(file_path, &exists);
        ASSERT_TRUE(status.ok()) << "Failed to check file existence: " << status.to_string();
        EXPECT_TRUE(exists) << "File should exist: " << file_path;
    }

    // Batch delete files
    std::vector<io::Path> paths_to_delete;
    for (const auto& file_path : test_files) {
        paths_to_delete.emplace_back(file_path);
    }

    Status status = s3_fs_->batch_delete(paths_to_delete);
    ASSERT_TRUE(status.ok()) << "Failed to batch delete files: " << status.to_string();

    std::cout << "Successfully batch deleted " << paths_to_delete.size() << " files" << std::endl;

    // Verify all files are deleted
    for (const auto& file_path : test_files) {
        bool exists = false;
        status = s3_fs_->exists(file_path, &exists);
        ASSERT_TRUE(status.ok()) << "Failed to check file existence: " << status.to_string();
        EXPECT_FALSE(exists) << "File should not exist after deletion: " << file_path;
    }

    std::cout << "Batch delete test completed successfully!" << std::endl;
}

// Test: Delete directory recursively
TEST_F(S3FileSystemTest, DeleteDirectoryRecursivelyTest) {
    ASSERT_NE(s3_fs_, nullptr);

    std::string test_dir = global_test_prefix_ + "recursive_delete_test/";
    std::cout << "Test directory path: " << test_dir << std::endl;

    // Create nested directory structure with files
    std::vector<std::string> test_files = {
            test_dir + "file1.txt",
            test_dir + "file2.txt",
            test_dir + "subdir1/file3.txt",
            test_dir + "subdir1/file4.txt",
            test_dir + "subdir2/nested/file5.txt",
    };

    for (const auto& file_path : test_files) {
        io::FileWriterPtr writer;
        Status status = s3_fs_->create_file(file_path, &writer);
        ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.to_string();

        std::string data = "Test data for " + file_path;
        status = writer->append(data);
        ASSERT_TRUE(status.ok()) << "Failed to write data: " << status.to_string();

        status = writer->close();
        ASSERT_TRUE(status.ok()) << "Failed to close writer: " << status.to_string();
    }

    // Verify files exist
    for (const auto& file_path : test_files) {
        bool exists = false;
        Status status = s3_fs_->exists(file_path, &exists);
        ASSERT_TRUE(status.ok()) << "Failed to check file existence: " << status.to_string();
        EXPECT_TRUE(exists) << "File should exist: " << file_path;
    }

    // Delete directory recursively
    Status status = s3_fs_->delete_directory(test_dir);
    ASSERT_TRUE(status.ok()) << "Failed to delete directory recursively: " << status.to_string();

    std::cout << "Successfully deleted directory recursively" << std::endl;

    // Verify all files are deleted
    for (const auto& file_path : test_files) {
        bool exists = false;
        status = s3_fs_->exists(file_path, &exists);
        ASSERT_TRUE(status.ok()) << "Failed to check file existence: " << status.to_string();
        EXPECT_FALSE(exists) << "File should not exist after recursive deletion: " << file_path;
    }

    std::cout << "Delete directory recursively test completed successfully!" << std::endl;
}

// Test: Upload and download local file
TEST_F(S3FileSystemTest, UploadDownloadTest) {
    ASSERT_NE(s3_fs_, nullptr);

    // Create a temporary local file
    std::string local_file = "/tmp/test_upload_file.txt";
    std::string test_content = "This is test content for upload/download test.\nLine 2\nLine 3";

    std::ofstream ofs(local_file);
    ASSERT_TRUE(ofs.is_open()) << "Failed to create local file";
    ofs << test_content;
    ofs.close();

    std::string remote_file = global_test_prefix_ + "uploaded_file.txt";
    std::cout << "Remote file path: " << remote_file << std::endl;

    // Upload local file to S3
    Status status = s3_fs_->upload(local_file, remote_file);
    ASSERT_TRUE(status.ok()) << "Failed to upload file: " << status.to_string();

    std::cout << "Successfully uploaded file" << std::endl;

    // Verify file exists on S3
    bool exists = false;
    status = s3_fs_->exists(remote_file, &exists);
    ASSERT_TRUE(status.ok()) << "Failed to check file existence: " << status.to_string();
    EXPECT_TRUE(exists) << "Uploaded file should exist";

    // Download file from S3
    std::string download_file = "/tmp/test_download_file.txt";
    status = s3_fs_->download(remote_file, download_file);
    ASSERT_TRUE(status.ok()) << "Failed to download file: " << status.to_string();

    std::cout << "Successfully downloaded file" << std::endl;

    // Verify downloaded content matches original
    std::ifstream ifs(download_file);
    ASSERT_TRUE(ifs.is_open()) << "Failed to open downloaded file";
    std::string downloaded_content((std::istreambuf_iterator<char>(ifs)),
                                   std::istreambuf_iterator<char>());
    ifs.close();

    EXPECT_EQ(test_content, downloaded_content) << "Downloaded content should match original";

    std::cout << "Upload/download test completed successfully!" << std::endl;

    // Cleanup
    std::remove(local_file.c_str());
    std::remove(download_file.c_str());
    status = s3_fs_->delete_file(remote_file);
    ASSERT_TRUE(status.ok()) << "Failed to delete remote file: " << status.to_string();
}

// Test: Open file and read content
TEST_F(S3FileSystemTest, OpenFileAndReadTest) {
    ASSERT_NE(s3_fs_, nullptr);

    std::string test_file = global_test_prefix_ + "read_test_file.txt";
    std::cout << "Test file path: " << test_file << std::endl;

    // Create and write test file
    io::FileWriterPtr writer;
    Status status = s3_fs_->create_file(test_file, &writer);
    ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.to_string();

    std::string test_data =
            "Hello, S3 Read Test! This is line 1.\nThis is line 2.\nThis is line 3.";
    status = writer->append(test_data);
    ASSERT_TRUE(status.ok()) << "Failed to write data: " << status.to_string();

    status = writer->close();
    ASSERT_TRUE(status.ok()) << "Failed to close writer: " << status.to_string();

    // Open file for reading
    io::FileReaderSPtr reader;
    status = s3_fs_->open_file(test_file, &reader);
    ASSERT_TRUE(status.ok()) << "Failed to open file: " << status.to_string();
    ASSERT_NE(reader, nullptr);

    // Read entire file
    size_t file_size = reader->size();
    EXPECT_EQ(file_size, test_data.size()) << "File size should match written data";

    std::vector<char> buffer(file_size);
    size_t bytes_read = 0;
    status = reader->read_at(0, Slice(buffer.data(), file_size), &bytes_read);
    ASSERT_TRUE(status.ok()) << "Failed to read file: " << status.to_string();
    EXPECT_EQ(bytes_read, file_size) << "Should read entire file";

    std::string read_data(buffer.data(), bytes_read);
    EXPECT_EQ(read_data, test_data) << "Read data should match written data";

    std::cout << "Successfully read " << bytes_read << " bytes from S3" << std::endl;

    // Test partial read
    size_t offset = 7;
    size_t read_size = 15;
    std::vector<char> partial_buffer(read_size);
    bytes_read = 0;
    status = reader->read_at(offset, Slice(partial_buffer.data(), read_size), &bytes_read);
    ASSERT_TRUE(status.ok()) << "Failed to read file partially: " << status.to_string();
    EXPECT_EQ(bytes_read, read_size) << "Should read requested bytes";

    std::string partial_data(partial_buffer.data(), bytes_read);
    std::string expected_partial = test_data.substr(offset, read_size);
    EXPECT_EQ(partial_data, expected_partial) << "Partial read data should match";

    std::cout << "Open file and read test completed successfully!" << std::endl;

    // Cleanup
    status = s3_fs_->delete_file(test_file);
    ASSERT_TRUE(status.ok()) << "Failed to delete test file: " << status.to_string();
}

// Test: Generate presigned URL
TEST_F(S3FileSystemTest, GeneratePresignedUrlTest) {
    ASSERT_NE(s3_fs_, nullptr);

    std::string test_file = global_test_prefix_ + "presigned_url_test.txt";
    std::cout << "Test file path: " << test_file << std::endl;

    // Create test file
    io::FileWriterPtr writer;
    Status status = s3_fs_->create_file(test_file, &writer);
    ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.to_string();

    std::string test_data = "Test data for presigned URL";
    status = writer->append(test_data);
    ASSERT_TRUE(status.ok()) << "Failed to write data: " << status.to_string();

    status = writer->close();
    ASSERT_TRUE(status.ok()) << "Failed to close writer: " << status.to_string();

    // Generate presigned URL
    int64_t expiration_secs = 3600; // 1 hour
    std::string presigned_url = s3_fs_->generate_presigned_url(test_file, expiration_secs, false);

    std::cout << "Generated presigned URL: " << presigned_url << std::endl;

    // Verify URL is not empty and contains expected components
    EXPECT_FALSE(presigned_url.empty()) << "Presigned URL should not be empty";
    EXPECT_NE(presigned_url.find("http"), std::string::npos) << "URL should start with http";

    // For public endpoint test (if using OSS)
    if (config_->get_provider() == "OSS") {
        std::string presigned_url_public =
                s3_fs_->generate_presigned_url(test_file, expiration_secs, true);
        std::cout << "Generated presigned URL (public): " << presigned_url_public << std::endl;
        EXPECT_FALSE(presigned_url_public.empty()) << "Public presigned URL should not be empty";
    }

    std::cout << "Generate presigned URL test completed successfully!" << std::endl;

    // Cleanup
    status = s3_fs_->delete_file(test_file);
    ASSERT_TRUE(status.ok()) << "Failed to delete test file: " << status.to_string();
}

// Test: Create directory (should be no-op but succeed)
TEST_F(S3FileSystemTest, CreateDirectoryTest) {
    ASSERT_NE(s3_fs_, nullptr);

    std::string test_dir = global_test_prefix_ + "test_directory/";

    // Create directory (should succeed but is essentially a no-op for S3)
    Status status = s3_fs_->create_directory(test_dir);
    ASSERT_TRUE(status.ok()) << "Failed to create directory: " << status.to_string();

    std::cout << "Create directory test completed successfully!" << std::endl;
}

// Test: File size operation with head_object
TEST_F(S3FileSystemTest, FileSizeHeadObjectTest) {
    ASSERT_NE(s3_fs_, nullptr);

    std::string test_file = global_test_prefix_ + "size_test_file.txt";

    // Create file with known size
    io::FileWriterPtr writer;
    Status status = s3_fs_->create_file(test_file, &writer);
    ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.to_string();

    std::string test_data = "This is a test string with known length: 12345";
    size_t expected_size = test_data.size();

    status = writer->append(test_data);
    ASSERT_TRUE(status.ok()) << "Failed to write data: " << status.to_string();

    status = writer->close();
    ASSERT_TRUE(status.ok()) << "Failed to close writer: " << status.to_string();

    // Get file size using head_object
    int64_t file_size = 0;
    status = s3_fs_->file_size(test_file, &file_size);
    ASSERT_TRUE(status.ok()) << "Failed to get file size: " << status.to_string();
    EXPECT_EQ(file_size, expected_size) << "File size should match written data";

    std::cout << "File size (head_object) test completed successfully!" << std::endl;

    // Cleanup
    status = s3_fs_->delete_file(test_file);
    ASSERT_TRUE(status.ok()) << "Failed to delete test file: " << status.to_string();
}

// Test: Exists check for non-existent file
TEST_F(S3FileSystemTest, ExistsNonExistentFileTest) {
    ASSERT_NE(s3_fs_, nullptr);

    std::string non_existent_file = global_test_prefix_ + "non_existent_file_12345.txt";

    bool exists = true;
    Status status = s3_fs_->exists(non_existent_file, &exists);
    ASSERT_TRUE(status.ok()) << "Failed to check file existence: " << status.to_string();
    EXPECT_FALSE(exists) << "Non-existent file should not exist";

    std::cout << "Exists non-existent file test completed successfully!" << std::endl;
}

// ==================== Rate Limiter Tests ====================

// Test: S3 rate limiter for GET operations - open_file and read_at
TEST_F(S3FileSystemTest, RateLimiterGetTest) {
    ASSERT_NE(s3_fs_, nullptr);

    // Save original config value
    bool original_enable_rate_limiter = config::enable_s3_rate_limiter;

    // Enable S3 rate limiter for this test
    config::enable_s3_rate_limiter = true;

    // Create a test file first
    std::string test_file = global_test_prefix_ + "rate_limit_get_test.txt";
    std::cout << "Test file path: " << test_file << std::endl;

    io::FileWriterPtr writer;
    Status status = s3_fs_->create_file(test_file, &writer);
    ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.to_string();

    // Write some data (about 1KB)
    std::string test_data(1024, 'A');
    status = writer->append(test_data);
    ASSERT_TRUE(status.ok()) << "Failed to write data: " << status.to_string();

    status = writer->close();
    ASSERT_TRUE(status.ok()) << "Failed to close writer: " << status.to_string();

    // Set a very strict rate limit for GET operations to trigger error
    // max_speed: 10 tokens per second
    // max_burst: 10 tokens (bucket size)
    // limit: 5 (allow only 5 requests total, the 6th will fail)
    int ret = reset_s3_rate_limiter(S3RateLimitType::GET, 10, 10, 11);
    ASSERT_EQ(ret, 0) << "Failed to set rate limiter for GET operations";

    std::cout << "Rate limiter set: limit 5 total requests for GET operations" << std::endl;

    // First 5 read operations should succeed
    for (int i = 0; i < 5; ++i) {
        io::FileReaderSPtr reader;
        status = s3_fs_->open_file(test_file, &reader);
        ASSERT_TRUE(status.ok()) << "Failed to open file on attempt " << i + 1 << ": "
                                 << status.to_string();

        std::vector<char> buffer(1024);
        size_t bytes_read = 0;
        status = reader->read_at(0, Slice(buffer.data(), 1024), &bytes_read);
        ASSERT_TRUE(status.ok()) << "Failed to read file on attempt " << i + 1 << ": "
                                 << status.to_string();
        std::cout << "Read attempt " << i + 1 << " succeeded" << std::endl;
    }

    // 6th read operation should fail due to rate limit
    io::FileReaderSPtr reader;
    status = s3_fs_->open_file(test_file, &reader);
    if (status.ok()) {
        std::vector<char> buffer(1024);
        size_t bytes_read = 0;
        status = reader->read_at(0, Slice(buffer.data(), 1024), &bytes_read);
    }

    EXPECT_FALSE(status.ok()) << "6th read should fail due to rate limit";
    std::cout << "6th read failed as expected: " << status.to_string() << std::endl;

    // Reset rate limiter to default (no limit) to avoid affecting other tests
    reset_s3_rate_limiter(S3RateLimitType::PUT, 10000, 10000, 0);
    reset_s3_rate_limiter(S3RateLimitType::GET, 10000, 10000, 0);

    // Restore original config
    config::enable_s3_rate_limiter = original_enable_rate_limiter;

    std::cout << "Rate limiter GET test completed successfully!" << std::endl;

    // Cleanup
    status = s3_fs_->delete_file(test_file);
    ASSERT_TRUE(status.ok()) << "Failed to delete test file: " << status.to_string();
}

// Test: S3 rate limiter for PUT operations - trigger error with limit
TEST_F(S3FileSystemTest, RateLimiterPutTest) {
    ASSERT_NE(s3_fs_, nullptr);

    // Save original config value
    bool original_enable_rate_limiter = config::enable_s3_rate_limiter;

    // Enable S3 rate limiter for this test
    config::enable_s3_rate_limiter = true;

    // Set a very strict rate limit for PUT operations to trigger error
    // max_speed: 1 token per second (1 QPS)
    // max_burst: 1 token (bucket size)
    // limit: 2 (allow only 2 requests total, the 3rd will fail)
    int ret = reset_s3_rate_limiter(S3RateLimitType::PUT, 10, 10, 2);
    ASSERT_EQ(ret, 0) << "Failed to set rate limiter for PUT operations";

    std::cout << "Rate limiter set: limit 2 total requests for PUT operations" << std::endl;

    // First two write operations should succeed
    std::vector<std::string> test_files;
    Status status;

    for (int i = 0; i < 2; ++i) {
        std::string test_file =
                global_test_prefix_ + "rate_limit_put_test_" + std::to_string(i) + ".txt";
        test_files.push_back(test_file);

        io::FileWriterPtr writer;
        status = s3_fs_->create_file(test_file, &writer);
        ASSERT_TRUE(status.ok()) << "Failed to create file on attempt " << i + 1 << ": "
                                 << status.to_string();

        std::string test_data = "Test data " + std::to_string(i);
        status = writer->append(test_data);
        ASSERT_TRUE(status.ok()) << "Failed to write data on attempt " << i + 1 << ": "
                                 << status.to_string();

        status = writer->close();
        ASSERT_TRUE(status.ok()) << "Failed to close writer on attempt " << i + 1 << ": "
                                 << status.to_string();

        std::cout << "Write attempt " << i + 1 << " succeeded" << std::endl;
    }

    // Third write operation should fail due to rate limit
    std::string test_file_fail = global_test_prefix_ + "rate_limit_put_test_fail.txt";
    io::FileWriterPtr writer;
    status = s3_fs_->create_file(test_file_fail, &writer);
    if (status.ok()) {
        std::string test_data = "This should fail";
        status = writer->append(test_data);
        if (status.ok()) {
            status = writer->close();
        }
    }

    EXPECT_FALSE(status.ok()) << "Third write should fail due to rate limit";
    std::cout << "Third write failed as expected: " << status.to_string() << std::endl;

    // Reset rate limiter to default (no limit) to avoid affecting other tests
    reset_s3_rate_limiter(S3RateLimitType::PUT, 10000, 10000, 0);
    reset_s3_rate_limiter(S3RateLimitType::GET, 10000, 10000, 0);

    // Restore original config
    config::enable_s3_rate_limiter = original_enable_rate_limiter;

    std::cout << "Rate limiter PUT test completed successfully!" << std::endl;

    // Cleanup successfully created files
    for (const auto& file : test_files) {
        status = s3_fs_->delete_file(file);
        ASSERT_TRUE(status.ok()) << "Failed to delete test file: " << status.to_string();
    }
}

// Test: S3 rate limiter for GET operations - download
TEST_F(S3FileSystemTest, RateLimiterGetDownloadTest) {
    ASSERT_NE(s3_fs_, nullptr);

    // Save original config value
    bool original_enable_rate_limiter = config::enable_s3_rate_limiter;

    // Enable S3 rate limiter for this test
    config::enable_s3_rate_limiter = true;

    // Create test files first
    std::vector<std::string> test_files;
    for (int i = 0; i < 3; ++i) {
        std::string test_file =
                global_test_prefix_ + "rate_limit_download_test_" + std::to_string(i) + ".txt";
        test_files.push_back(test_file);

        io::FileWriterPtr writer;
        Status status = s3_fs_->create_file(test_file, &writer);
        ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.to_string();

        std::string test_data = "Download test data " + std::to_string(i);
        status = writer->append(test_data);
        ASSERT_TRUE(status.ok()) << "Failed to write data: " << status.to_string();

        status = writer->close();
        ASSERT_TRUE(status.ok()) << "Failed to close writer: " << status.to_string();
    }

    // Set rate limit for GET operations
    // limit: 2 (allow only 2 requests total, the 3rd will fail)
    int ret = reset_s3_rate_limiter(S3RateLimitType::GET, 10, 10, 4);
    ASSERT_EQ(ret, 0) << "Failed to set rate limiter for GET operations";

    std::cout << "Rate limiter set: limit 2 total requests for GET operations (download)"
              << std::endl;

    // First two download operations should succeed
    Status status;
    for (int i = 0; i < 2; ++i) {
        std::string local_file = "/tmp/rate_limit_download_" + std::to_string(i) + ".txt";
        status = s3_fs_->download(test_files[i], local_file);
        ASSERT_TRUE(status.ok()) << "Failed to download file on attempt " << i + 1 << ": "
                                 << status.to_string();
        std::cout << "Download attempt " << i + 1 << " succeeded" << std::endl;
        std::remove(local_file.c_str());
    }

    // Third download operation should fail due to rate limit
    std::string local_file_fail = "/tmp/rate_limit_download_fail.txt";
    status = s3_fs_->download(test_files[2], local_file_fail);

    EXPECT_FALSE(status.ok()) << "Third download should fail due to rate limit";
    std::cout << "Third download failed as expected: " << status.to_string() << std::endl;

    // Reset rate limiter
    reset_s3_rate_limiter(S3RateLimitType::PUT, 10000, 10000, 0);
    reset_s3_rate_limiter(S3RateLimitType::GET, 10000, 10000, 0);

    // Restore original config
    config::enable_s3_rate_limiter = original_enable_rate_limiter;

    std::cout << "Rate limiter GET download test completed successfully!" << std::endl;

    // Cleanup
    for (const auto& file : test_files) {
        status = s3_fs_->delete_file(file);
        ASSERT_TRUE(status.ok()) << "Failed to delete test file: " << status.to_string();
    }
}

// Test: S3 rate limiter for PUT operations - multipart upload
TEST_F(S3FileSystemTest, RateLimiterPutMultipartTest) {
    ASSERT_NE(s3_fs_, nullptr);

    // Save original config value
    bool original_enable_rate_limiter = config::enable_s3_rate_limiter;

    // Enable S3 rate limiter for this test
    config::enable_s3_rate_limiter = true;

    // Set rate limit for PUT operations with higher limit for multipart
    // Each chunk in multipart upload counts as one request
    // limit: 3 (allow only 3 PUT requests, multipart with 6 chunks should fail)
    int ret = reset_s3_rate_limiter(S3RateLimitType::PUT, 10, 10, 3);
    ASSERT_EQ(ret, 0) << "Failed to set rate limiter for PUT operations";

    std::cout << "Rate limiter set: limit 3 total requests for PUT operations (multipart)"
              << std::endl;

    std::string test_file = global_test_prefix_ + "rate_limit_multipart_test.dat";

    // Create file writer
    io::FileWriterPtr writer;
    Status status = s3_fs_->create_file(test_file, &writer);
    ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.to_string();

    // Write large data to trigger multipart upload (>5MB for multipart)
    // This will trigger multiple PUT requests and should fail
    size_t chunk_size = 1024 * 1024; // 1MB
    size_t num_chunks = 6;           // 6MB total, will trigger multipart
    std::string chunk_data(chunk_size, 'A');

    bool failed = false;
    for (size_t i = 0; i < num_chunks; ++i) {
        std::ranges::fill(chunk_data, 'A' + (i % 26));
        status = writer->append(chunk_data);
        if (!status.ok()) {
            failed = true;
            std::cout << "Multipart upload failed at chunk " << i
                      << " as expected: " << status.to_string() << std::endl;
            break;
        }
    }

    if (!failed) {
        status = writer->close();
        if (!status.ok()) {
            failed = true;
            std::cout << "Multipart upload failed at close as expected: " << status.to_string()
                      << std::endl;
        }
    }

    EXPECT_TRUE(failed) << "Multipart upload should fail due to rate limit";

    // Reset rate limiter
    reset_s3_rate_limiter(S3RateLimitType::PUT, 10000, 10000, 0);
    reset_s3_rate_limiter(S3RateLimitType::GET, 10000, 10000, 0);

    // Restore original config
    config::enable_s3_rate_limiter = original_enable_rate_limiter;

    std::cout << "Rate limiter PUT multipart test completed successfully!" << std::endl;

    // Try to cleanup (may fail if file was not created)
    std::ignore = s3_fs_->delete_file(test_file);
}

// Test: S3 rate limiter for PUT operations - upload
TEST_F(S3FileSystemTest, RateLimiterPutUploadTest) {
    ASSERT_NE(s3_fs_, nullptr);

    // Save original config value
    bool original_enable_rate_limiter = config::enable_s3_rate_limiter;

    // Enable S3 rate limiter for this test
    config::enable_s3_rate_limiter = true;

    // Create local test files
    std::vector<std::string> local_files;
    std::vector<std::string> remote_files;

    for (int i = 0; i < 3; ++i) {
        std::string local_file = "/tmp/rate_limit_upload_" + std::to_string(i) + ".txt";
        local_files.push_back(local_file);

        std::ofstream ofs(local_file);
        ASSERT_TRUE(ofs.is_open()) << "Failed to create local file";
        ofs << "Upload test data " << i;
        ofs.close();

        remote_files.push_back(global_test_prefix_ + "rate_limit_upload_" + std::to_string(i) +
                               ".txt");
    }

    // Set rate limit for PUT operations
    // limit: 2 (allow only 2 requests total, the 3rd will fail)
    int ret = reset_s3_rate_limiter(S3RateLimitType::PUT, 10, 10, 2);
    ASSERT_EQ(ret, 0) << "Failed to set rate limiter for PUT operations";

    std::cout << "Rate limiter set: limit 2 total requests for PUT operations (upload)"
              << std::endl;

    // First two upload operations should succeed
    Status status;
    for (int i = 0; i < 2; ++i) {
        status = s3_fs_->upload(local_files[i], remote_files[i]);
        ASSERT_TRUE(status.ok()) << "Failed to upload file on attempt " << i + 1 << ": "
                                 << status.to_string();
        std::cout << "Upload attempt " << i + 1 << " succeeded" << std::endl;
    }

    // Third upload operation should fail due to rate limit
    status = s3_fs_->upload(local_files[2], remote_files[2]);

    EXPECT_FALSE(status.ok()) << "Third upload should fail due to rate limit";
    std::cout << "Third upload failed as expected: " << status.to_string() << std::endl;

    // Reset rate limiter
    reset_s3_rate_limiter(S3RateLimitType::PUT, 10000, 10000, 0);
    reset_s3_rate_limiter(S3RateLimitType::GET, 10000, 10000, 0);

    // Restore original config
    config::enable_s3_rate_limiter = original_enable_rate_limiter;

    std::cout << "Rate limiter PUT upload test completed successfully!" << std::endl;

    // Cleanup local files
    for (const auto& file : local_files) {
        std::remove(file.c_str());
    }

    // Cleanup remote files (only first 2 should exist)
    for (int i = 0; i < 2; ++i) {
        status = s3_fs_->delete_file(remote_files[i]);
        ASSERT_TRUE(status.ok()) << "Failed to delete remote file: " << status.to_string();
    }
}

// Test: S3 rate limiter for GET operations - head_object (file_size/exists)
TEST_F(S3FileSystemTest, RateLimiterGetHeadObjectTest) {
    ASSERT_NE(s3_fs_, nullptr);

    // Save original config value
    bool original_enable_rate_limiter = config::enable_s3_rate_limiter;

    // Enable S3 rate limiter for this test
    config::enable_s3_rate_limiter = true;

    // Create test files first
    std::vector<std::string> test_files;
    for (int i = 0; i < 3; ++i) {
        std::string test_file =
                global_test_prefix_ + "rate_limit_head_test_" + std::to_string(i) + ".txt";
        test_files.push_back(test_file);

        io::FileWriterPtr writer;
        Status status = s3_fs_->create_file(test_file, &writer);
        ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.to_string();

        std::string test_data = "Head object test data " + std::to_string(i);
        status = writer->append(test_data);
        ASSERT_TRUE(status.ok()) << "Failed to write data: " << status.to_string();

        status = writer->close();
        ASSERT_TRUE(status.ok()) << "Failed to close writer: " << status.to_string();
    }

    // Set rate limit for GET operations (head_object uses GET rate limiter)
    // limit: 2 (allow only 2 requests total, the 3rd will fail)
    int ret = reset_s3_rate_limiter(S3RateLimitType::GET, 10, 10, 2);
    ASSERT_EQ(ret, 0) << "Failed to set rate limiter for GET operations";

    std::cout << "Rate limiter set: limit 2 total requests for GET operations (head_object)"
              << std::endl;

    // First two head operations should succeed
    Status status;
    for (int i = 0; i < 2; ++i) {
        int64_t file_size = 0;
        status = s3_fs_->file_size(test_files[i], &file_size);
        ASSERT_TRUE(status.ok()) << "Failed to get file size on attempt " << i + 1 << ": "
                                 << status.to_string();
        std::cout << "Head object attempt " << i + 1 << " succeeded, size: " << file_size
                  << std::endl;
    }

    // Third head operation should fail due to rate limit
    bool exists = false;
    status = s3_fs_->exists(test_files[2], &exists);

    EXPECT_FALSE(status.ok()) << "Third head object should fail due to rate limit";
    std::cout << "Third head object failed as expected: " << status.to_string() << std::endl;

    // Reset rate limiter
    reset_s3_rate_limiter(S3RateLimitType::PUT, 10000, 10000, 0);
    reset_s3_rate_limiter(S3RateLimitType::GET, 10000, 10000, 0);

    // Restore original config
    config::enable_s3_rate_limiter = original_enable_rate_limiter;

    std::cout << "Rate limiter GET head object test completed successfully!" << std::endl;

    // Cleanup
    for (const auto& file : test_files) {
        status = s3_fs_->delete_file(file);
        ASSERT_TRUE(status.ok()) << "Failed to delete test file: " << status.to_string();
    }
}

// Test: S3 rate limiter for GET operations - list objects
TEST_F(S3FileSystemTest, RateLimiterGetListTest) {
    ASSERT_NE(s3_fs_, nullptr);

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
        io::FileWriterPtr writer;
        Status status = s3_fs_->create_file(test_file, &writer);
        ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.to_string();

        std::string test_data = "List test data " + std::to_string(i);
        status = writer->append(test_data);
        ASSERT_TRUE(status.ok()) << "Failed to write data: " << status.to_string();

        status = writer->close();
        ASSERT_TRUE(status.ok()) << "Failed to close writer: " << status.to_string();
    }

    // Set rate limit for GET operations (list uses GET rate limiter)
    // limit: 2 (allow only 2 requests total, the 3rd will fail)
    int ret = reset_s3_rate_limiter(S3RateLimitType::GET, 10, 10, 2);
    ASSERT_EQ(ret, 0) << "Failed to set rate limiter for GET operations";

    std::cout << "Rate limiter set: limit 2 total requests for GET operations (list)" << std::endl;

    // First two list operations should succeed
    Status status;
    for (int i = 0; i < 2; ++i) {
        bool exists = false;
        std::vector<io::FileInfo> files;
        status = s3_fs_->list(test_dirs[i], true, &files, &exists);
        ASSERT_TRUE(status.ok()) << "Failed to list on attempt " << i + 1 << ": "
                                 << status.to_string();
        std::cout << "List attempt " << i + 1 << " succeeded, found " << files.size() << " files"
                  << std::endl;
    }

    // Third list operation should fail due to rate limit
    bool exists = false;
    std::vector<io::FileInfo> files;
    status = s3_fs_->list(test_dirs[2], true, &files, &exists);

    EXPECT_FALSE(status.ok()) << "Third list should fail due to rate limit";
    std::cout << "Third list failed as expected: " << status.to_string() << std::endl;

    // Reset rate limiter
    reset_s3_rate_limiter(S3RateLimitType::PUT, 10000, 10000, 0);
    reset_s3_rate_limiter(S3RateLimitType::GET, 10000, 10000, 0);

    // Restore original config
    config::enable_s3_rate_limiter = original_enable_rate_limiter;

    std::cout << "Rate limiter GET list test completed successfully!" << std::endl;

    // Cleanup
    for (const auto& dir : test_dirs) {
        status = s3_fs_->delete_directory(dir);
        ASSERT_TRUE(status.ok()) << "Failed to delete test directory: " << status.to_string();
    }
}

// Test: S3 rate limiter for PUT operations - delete_file (uses PUT rate limiter)
TEST_F(S3FileSystemTest, RateLimiterPutDeleteTest) {
    ASSERT_NE(s3_fs_, nullptr);

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

        io::FileWriterPtr writer;
        Status status = s3_fs_->create_file(test_file, &writer);
        ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.to_string();

        std::string test_data = "Delete test data " + std::to_string(i);
        status = writer->append(test_data);
        ASSERT_TRUE(status.ok()) << "Failed to write data: " << status.to_string();

        status = writer->close();
        ASSERT_TRUE(status.ok()) << "Failed to close writer: " << status.to_string();
    }

    // Set rate limit for PUT operations (delete uses PUT rate limiter)
    // limit: 2 (allow only 2 requests total, the 3rd will fail)
    int ret = reset_s3_rate_limiter(S3RateLimitType::PUT, 10, 10, 2);
    ASSERT_EQ(ret, 0) << "Failed to set rate limiter for PUT operations";

    std::cout << "Rate limiter set: limit 2 total requests for PUT operations (delete)"
              << std::endl;

    // First two delete operations should succeed
    Status status;
    for (int i = 0; i < 2; ++i) {
        status = s3_fs_->delete_file(test_files[i]);
        ASSERT_TRUE(status.ok()) << "Failed to delete file on attempt " << i + 1 << ": "
                                 << status.to_string();
        std::cout << "Delete attempt " << i + 1 << " succeeded" << std::endl;
    }

    // Third delete operation should fail due to rate limit
    status = s3_fs_->delete_file(test_files[2]);

    EXPECT_FALSE(status.ok()) << "Third delete should fail due to rate limit";
    std::cout << "Third delete failed as expected: " << status.to_string() << std::endl;

    // Reset rate limiter to clean up the remaining file
    reset_s3_rate_limiter(S3RateLimitType::PUT, 10000, 10000, 0);
    reset_s3_rate_limiter(S3RateLimitType::GET, 10000, 10000, 0);

    // Restore original config
    config::enable_s3_rate_limiter = original_enable_rate_limiter;

    std::cout << "Rate limiter PUT delete test completed successfully!" << std::endl;

    // Cleanup the file that failed to delete
    status = s3_fs_->delete_file(test_files[2]);
    ASSERT_TRUE(status.ok()) << "Failed to cleanup remaining file: " << status.to_string();
}

// Test: S3 rate limiter for PUT operations - batch_delete (uses PUT rate limiter)
TEST_F(S3FileSystemTest, RateLimiterPutBatchDeleteTest) {
    ASSERT_NE(s3_fs_, nullptr);

    // Save original config value
    bool original_enable_rate_limiter = config::enable_s3_rate_limiter;

    // Enable S3 rate limiter for this test
    config::enable_s3_rate_limiter = true;

    // Create multiple batches of test files
    std::vector<std::vector<std::string>> file_batches;

    for (int batch = 0; batch < 3; ++batch) {
        std::vector<std::string> batch_files;
        for (int i = 0; i < 3; ++i) {
            std::string test_file = global_test_prefix_ + "rate_limit_batch_delete_batch_" +
                                    std::to_string(batch) + "_file_" + std::to_string(i) + ".txt";
            batch_files.push_back(test_file);

            io::FileWriterPtr writer;
            Status status = s3_fs_->create_file(test_file, &writer);
            ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.to_string();

            std::string test_data = "Batch delete test data";
            status = writer->append(test_data);
            ASSERT_TRUE(status.ok()) << "Failed to write data: " << status.to_string();

            status = writer->close();
            ASSERT_TRUE(status.ok()) << "Failed to close writer: " << status.to_string();
        }
        file_batches.push_back(batch_files);
    }

    // Set rate limit for PUT operations (batch_delete uses PUT rate limiter)
    // limit: 2 (allow only 2 batch delete requests, the 3rd will fail)
    int ret = reset_s3_rate_limiter(S3RateLimitType::PUT, 10, 10, 2);
    ASSERT_EQ(ret, 0) << "Failed to set rate limiter for PUT operations";

    std::cout << "Rate limiter set: limit 2 total requests for PUT operations (batch_delete)"
              << std::endl;

    // First two batch delete operations should succeed
    Status status;
    for (int batch = 0; batch < 2; ++batch) {
        std::vector<io::Path> paths_to_delete;
        for (const auto& file : file_batches[batch]) {
            paths_to_delete.emplace_back(file);
        }

        status = s3_fs_->batch_delete(paths_to_delete);
        ASSERT_TRUE(status.ok()) << "Failed to batch delete on attempt " << batch + 1 << ": "
                                 << status.to_string();
        std::cout << "Batch delete attempt " << batch + 1 << " succeeded, deleted "
                  << paths_to_delete.size() << " files" << std::endl;
    }

    // Third batch delete operation should fail due to rate limit
    std::vector<io::Path> paths_to_delete;
    for (const auto& file : file_batches[2]) {
        paths_to_delete.emplace_back(file);
    }
    status = s3_fs_->batch_delete(paths_to_delete);

    EXPECT_FALSE(status.ok()) << "Third batch delete should fail due to rate limit";
    std::cout << "Third batch delete failed as expected: " << status.to_string() << std::endl;

    // Reset rate limiter to clean up remaining files
    reset_s3_rate_limiter(S3RateLimitType::PUT, 10000, 10000, 0);
    reset_s3_rate_limiter(S3RateLimitType::GET, 10000, 10000, 0);

    // Restore original config
    config::enable_s3_rate_limiter = original_enable_rate_limiter;

    std::cout << "Rate limiter PUT batch delete test completed successfully!" << std::endl;

    // Cleanup remaining files from failed batch
    status = s3_fs_->batch_delete(paths_to_delete);
    ASSERT_TRUE(status.ok()) << "Failed to cleanup remaining files: " << status.to_string();
}

// Test: S3 rate limiter for GET operations - delete_directory with ListObjectsV2
TEST_F(S3FileSystemTest, RateLimiterGetDeleteDirectoryListTest) {
    ASSERT_NE(s3_fs_, nullptr);

    // Save original config value
    bool original_enable_rate_limiter = config::enable_s3_rate_limiter;

    // Enable S3 rate limiter for this test
    config::enable_s3_rate_limiter = true;

    // Create multiple test directories with files
    std::vector<std::string> test_dirs;
    for (int i = 0; i < 3; ++i) {
        std::string test_dir =
                global_test_prefix_ + "rate_limit_delete_dir_list_" + std::to_string(i) + "/";
        test_dirs.push_back(test_dir);

        // Create multiple files in each directory
        for (int j = 0; j < 5; ++j) {
            std::string test_file = test_dir + "file_" + std::to_string(j) + ".txt";
            io::FileWriterPtr writer;
            Status status = s3_fs_->create_file(test_file, &writer);
            ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.to_string();

            std::string test_data = "Delete directory test data";
            status = writer->append(test_data);
            ASSERT_TRUE(status.ok()) << "Failed to write data: " << status.to_string();

            status = writer->close();
            ASSERT_TRUE(status.ok()) << "Failed to close writer: " << status.to_string();
        }
    }

    // Set rate limit for GET operations (ListObjectsV2 uses GET rate limiter)
    // limit: 2 (allow only 2 ListObjectsV2 requests, the 3rd will fail)
    int ret = reset_s3_rate_limiter(S3RateLimitType::GET, 10, 10, 2);
    ASSERT_EQ(ret, 0) << "Failed to set rate limiter for GET operations";

    std::cout << "Rate limiter set: limit 2 total requests for GET operations (ListObjectsV2 in "
                 "delete_directory)"
              << std::endl;

    // First two delete_directory operations should succeed
    // Each delete_directory calls ListObjectsV2 (GET) and DeleteObjects (PUT)
    Status status;
    for (int i = 0; i < 2; ++i) {
        status = s3_fs_->delete_directory(test_dirs[i]);
        ASSERT_TRUE(status.ok()) << "Failed to delete directory on attempt " << i + 1 << ": "
                                 << status.to_string();
        std::cout << "Delete directory attempt " << i + 1 << " succeeded" << std::endl;
    }

    // Third delete_directory operation should fail due to rate limit on ListObjectsV2
    status = s3_fs_->delete_directory(test_dirs[2]);

    EXPECT_FALSE(status.ok())
            << "Third delete_directory should fail due to rate limit on ListObjectsV2";
    std::cout << "Third delete_directory failed as expected (ListObjectsV2 rate limit): "
              << status.to_string() << std::endl;

    // Reset rate limiter to clean up remaining directory
    reset_s3_rate_limiter(S3RateLimitType::PUT, 10000, 10000, 0);
    reset_s3_rate_limiter(S3RateLimitType::GET, 10000, 10000, 0);

    // Restore original config
    config::enable_s3_rate_limiter = original_enable_rate_limiter;

    std::cout << "Rate limiter GET delete_directory (ListObjectsV2) test completed successfully!"
              << std::endl;

    // Cleanup remaining directory
    status = s3_fs_->delete_directory(test_dirs[2]);
    ASSERT_TRUE(status.ok()) << "Failed to cleanup remaining directory: " << status.to_string();
}

// Test: S3 rate limiter for PUT operations - multipart upload with UploadPart failure
TEST_F(S3FileSystemTest, RateLimiterPutMultipartUploadPartFailureTest) {
    ASSERT_NE(s3_fs_, nullptr);

    // Save original config value
    bool original_enable_rate_limiter = config::enable_s3_rate_limiter;

    // Enable S3 rate limiter for this test
    config::enable_s3_rate_limiter = true;

    // Set a very strict rate limit for PUT operations
    // limit: 1 (allow only 1 PUT request: CreateMultipartUpload will succeed, but UploadPart will fail)
    int ret = reset_s3_rate_limiter(S3RateLimitType::PUT, 10, 10, 1);
    ASSERT_EQ(ret, 0) << "Failed to set rate limiter for PUT operations";

    std::cout << "Rate limiter set: limit 1 total request for PUT operations (UploadPart will fail)"
              << std::endl;

    std::string test_file = global_test_prefix_ + "rate_limit_upload_part_test.dat";

    // Create file writer
    io::FileWriterPtr writer;
    Status status = s3_fs_->create_file(test_file, &writer);
    ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.to_string();

    // Write large data to trigger multipart upload (>5MB for multipart)
    // This will trigger CreateMultipartUpload (1st PUT, succeeds) and UploadPart (2nd PUT, should fail)
    size_t chunk_size = 1024 * 1024; // 1MB
    size_t num_chunks = 6;           // 6MB total, will trigger multipart
    std::string chunk_data(chunk_size, 'A');

    std::cout << "Writing " << (chunk_size * num_chunks) << " bytes to trigger multipart upload"
              << std::endl;

    bool write_failed = false;
    for (size_t i = 0; i < num_chunks; ++i) {
        std::ranges::fill(chunk_data, 'A' + (i % 26));
        status = writer->append(chunk_data);
        if (!status.ok()) {
            write_failed = true;
            std::cout << "Write failed at chunk " << i << " as expected: " << status.to_string()
                      << std::endl;
            break;
        }
    }

    if (!write_failed) {
        // If write succeeded, close should fail
        status = writer->close();
        if (!status.ok()) {
            write_failed = true;
            std::cout << "Close failed as expected (UploadPart rate limit): " << status.to_string()
                      << std::endl;
        }
    }

    EXPECT_TRUE(write_failed) << "Multipart upload should fail due to UploadPart rate limit";

    // Verify the error message contains information about UploadPart failure
    if (write_failed) {
        std::string error_msg = status.to_string();
        // The error should be about upload failure
        std::cout << "Verified UploadPart failure with error: " << error_msg << std::endl;
    }

    // Reset rate limiter
    reset_s3_rate_limiter(S3RateLimitType::PUT, 10000, 10000, 0);
    reset_s3_rate_limiter(S3RateLimitType::GET, 10000, 10000, 0);

    // Restore original config
    config::enable_s3_rate_limiter = original_enable_rate_limiter;

    std::cout << "Rate limiter PUT multipart UploadPart failure test completed successfully!"
              << std::endl;

    // Try to cleanup (may fail if file was not created)
    std::ignore = s3_fs_->delete_file(test_file);
}

// Test: S3 DeleteObjects with partial failure - some objects fail to delete
TEST_F(S3FileSystemTest, DeleteDirectoryPartialFailureTest) {
    ASSERT_NE(s3_fs_, nullptr);

    std::string test_dir = global_test_prefix_ + "delete_partial_failure_test/";
    std::cout << "Test directory path: " << test_dir << std::endl;

    // Create multiple test files
    std::vector<std::string> test_files;
    for (int i = 0; i < 5; ++i) {
        std::string test_file = test_dir + "file_" + std::to_string(i) + ".txt";
        test_files.push_back(test_file);

        io::FileWriterPtr writer;
        Status status = s3_fs_->create_file(test_file, &writer);
        ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.to_string();

        std::string test_data = "Test data " + std::to_string(i);
        status = writer->append(test_data);
        ASSERT_TRUE(status.ok()) << "Failed to write data: " << status.to_string();

        status = writer->close();
        ASSERT_TRUE(status.ok()) << "Failed to close writer: " << status.to_string();
    }

    // Set up sync point to simulate partial delete failure
    // When DeleteObjects is called, we'll inject errors into the result
    SyncPoint::get_instance()->set_call_back(
            "s3_obj_storage_client::delete_objects_recursively", [](std::vector<std::any>&& args) {
                // The callback receives delete_outcome as argument
                // delete_outcome is of type: Aws::Utils::Outcome<DeleteObjectsResult, S3Error>*
                using DeleteObjectsOutcome =
                        Aws::Utils::Outcome<Aws::S3::Model::DeleteObjectsResult, Aws::S3::S3Error>;
                auto* delete_outcome = std::any_cast<DeleteObjectsOutcome*>(args[0]);

                // Create a mock error for one of the objects
                Aws::S3::Model::Error error;
                error.SetKey("file_1.txt");
                error.SetCode("AccessDenied");
                error.SetMessage("Simulated partial delete failure");

                // Get the mutable result and add error
                // Note: We need to create a new result with errors
                auto& result = const_cast<Aws::S3::Model::DeleteObjectsResult&>(
                        delete_outcome->GetResult());
                Aws::Vector<Aws::S3::Model::Error> errors;
                errors.push_back(error);
                result.SetErrors(errors);
            });

    // Enable sync point processing
    SyncPoint::get_instance()->enable_processing();

    // Try to delete directory - should fail due to partial delete failure
    Status status = s3_fs_->delete_directory(test_dir);

    // The delete should fail because some objects failed to delete
    EXPECT_FALSE(status.ok()) << "Delete directory should fail due to partial delete failure";
    std::cout << "Delete directory failed as expected with partial failure: " << status.to_string()
              << std::endl;

    // Verify error message contains information about the failed object
    std::string error_msg = status.to_string();
    std::cout << "Error message: " << error_msg << std::endl;

    // Disable sync point processing and clear callbacks
    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();

    std::cout << "Delete directory partial failure test completed successfully!" << std::endl;

    // Cleanup - now without sync point, it should succeed
    status = s3_fs_->delete_directory(test_dir);
    ASSERT_TRUE(status.ok()) << "Failed to cleanup test directory: " << status.to_string();
}

// Test: S3 batch_delete (delete_objects) with partial failure - some objects fail to delete
TEST_F(S3FileSystemTest, BatchDeletePartialFailureTest) {
    ASSERT_NE(s3_fs_, nullptr);

    std::string test_dir = global_test_prefix_ + "batch_delete_partial_failure_test/";
    std::cout << "Test directory path: " << test_dir << std::endl;

    // Create multiple test files
    std::vector<std::string> test_files;
    for (int i = 0; i < 5; ++i) {
        std::string test_file = test_dir + "file_" + std::to_string(i) + ".txt";
        test_files.push_back(test_file);

        io::FileWriterPtr writer;
        Status status = s3_fs_->create_file(test_file, &writer);
        ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.to_string();

        std::string test_data = "Batch delete test data " + std::to_string(i);
        status = writer->append(test_data);
        ASSERT_TRUE(status.ok()) << "Failed to write data: " << status.to_string();

        status = writer->close();
        ASSERT_TRUE(status.ok()) << "Failed to close writer: " << status.to_string();
    }

    // Verify all files exist
    for (const auto& file_path : test_files) {
        bool exists = false;
        Status status = s3_fs_->exists(file_path, &exists);
        ASSERT_TRUE(status.ok()) << "Failed to check file existence: " << status.to_string();
        EXPECT_TRUE(exists) << "File should exist: " << file_path;
    }

    // Set up sync point to simulate partial delete failure in batch_delete
    // When DeleteObjects is called via batch_delete, we'll inject errors into the result
    SyncPoint::get_instance()->set_call_back(
            "s3_obj_storage_client::delete_objects", [](std::vector<std::any>&& args) {
                // The callback receives delete_outcome as argument
                // delete_outcome is of type: Aws::Utils::Outcome<DeleteObjectsResult, S3Error>*
                using DeleteObjectsOutcome =
                        Aws::Utils::Outcome<Aws::S3::Model::DeleteObjectsResult, Aws::S3::S3Error>;
                auto* delete_outcome = std::any_cast<DeleteObjectsOutcome*>(args[0]);

                // Create mock errors for some of the objects
                Aws::S3::Model::Error error1;
                error1.SetKey("file_2.txt");
                error1.SetCode("AccessDenied");
                error1.SetMessage("Simulated batch delete partial failure for file_2");

                Aws::S3::Model::Error error2;
                error2.SetKey("file_4.txt");
                error2.SetCode("InternalError");
                error2.SetMessage("Simulated batch delete partial failure for file_4");

                // Get the mutable result and add errors
                auto& result = const_cast<Aws::S3::Model::DeleteObjectsResult&>(
                        delete_outcome->GetResult());
                Aws::Vector<Aws::S3::Model::Error> errors;
                errors.push_back(error1);
                errors.push_back(error2);
                result.SetErrors(errors);
            });

    // Enable sync point processing
    SyncPoint::get_instance()->enable_processing();

    // Try to batch delete files - should fail due to partial delete failure
    std::vector<io::Path> paths_to_delete;
    for (const auto& file_path : test_files) {
        paths_to_delete.emplace_back(file_path);
    }

    Status status = s3_fs_->batch_delete(paths_to_delete);

    // The batch_delete should fail because some objects failed to delete
    EXPECT_FALSE(status.ok()) << "Batch delete should fail due to partial delete failure";
    std::cout << "Batch delete failed as expected with partial failure: " << status.to_string()
              << std::endl;

    // Verify error message contains information about the failed object
    std::string error_msg = status.to_string();
    std::cout << "Error message: " << error_msg << std::endl;
    // The error should mention one of the failed files
    EXPECT_TRUE(error_msg.find("file_2") != std::string::npos ||
                error_msg.find("file_4") != std::string::npos)
            << "Error message should mention failed file";

    // Disable sync point processing and clear callbacks
    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();

    std::cout << "Batch delete partial failure test completed successfully!" << std::endl;

    // Cleanup - now without sync point, it should succeed
    status = s3_fs_->batch_delete(paths_to_delete);
    ASSERT_TRUE(status.ok()) << "Failed to cleanup test files: " << status.to_string();
}

// Test: S3 get_object with incomplete read - bytes_read != size_return
TEST_F(S3FileSystemTest, GetObjectIncompleteReadTest) {
    ASSERT_NE(s3_fs_, nullptr);

    std::string test_file = global_test_prefix_ + "incomplete_read_test.txt";
    std::cout << "Test file path: " << test_file << std::endl;

    // Create a test file with known content
    io::FileWriterPtr writer;
    Status status = s3_fs_->create_file(test_file, &writer);
    ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.to_string();

    std::string test_data =
            "This is test data for incomplete read simulation. "
            "The content should be long enough to test partial reads.";
    status = writer->append(test_data);
    ASSERT_TRUE(status.ok()) << "Failed to write data: " << status.to_string();

    status = writer->close();
    ASSERT_TRUE(status.ok()) << "Failed to close writer: " << status.to_string();

    std::cout << "Created test file with " << test_data.size() << " bytes" << std::endl;

    // Open file for reading
    io::FileReaderSPtr reader;
    status = s3_fs_->open_file(test_file, &reader);
    ASSERT_TRUE(status.ok()) << "Failed to open file: " << status.to_string();
    ASSERT_NE(reader, nullptr);

    // Set up sync point to simulate incomplete read
    // When get_object is called, we'll modify size_return to be less than bytes_read
    SyncPoint::get_instance()->set_call_back(
            "s3_obj_storage_client::get_object", [](std::vector<std::any>&& args) {
                // The callback receives size_return as argument
                auto* size_return = std::any_cast<size_t*>(args[0]);

                // Simulate incomplete read by reducing the returned size
                // For example, if we requested 50 bytes but only got 30
                size_t original_size = *size_return;
                *size_return = original_size / 2; // Return only half of what was requested

                std::cout << "SyncPoint: Modified size_return from " << original_size << " to "
                          << *size_return << std::endl;
            });

    // Enable sync point processing
    SyncPoint::get_instance()->enable_processing();

    // Try to read from file - should fail due to incomplete read
    size_t read_size = 50;
    std::vector<char> buffer(read_size);
    size_t bytes_read = 0;
    status = reader->read_at(0, Slice(buffer.data(), read_size), &bytes_read);

    // The read should fail because size_return != bytes_read
    EXPECT_FALSE(status.ok()) << "Read should fail due to incomplete read";
    std::cout << "Read failed as expected with incomplete read: " << status.to_string()
              << std::endl;

    // Verify error message contains information about the mismatch
    std::string error_msg = status.to_string();
    std::cout << "Error message: " << error_msg << std::endl;
    EXPECT_TRUE(error_msg.find("bytes read") != std::string::npos ||
                error_msg.find("bytes req") != std::string::npos)
            << "Error message should mention byte count mismatch";

    // Disable sync point processing and clear callbacks
    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();

    std::cout << "Get object incomplete read test completed successfully!" << std::endl;

    // Cleanup
    status = s3_fs_->delete_file(test_file);
    ASSERT_TRUE(status.ok()) << "Failed to cleanup test file: " << status.to_string();
}

// Test: S3 rate limiter for PUT operations - delete_directory with DeleteObjects
TEST_F(S3FileSystemTest, RateLimiterPutDeleteDirectoryDeleteObjectsTest) {
    ASSERT_NE(s3_fs_, nullptr);

    // Save original config value
    bool original_enable_rate_limiter = config::enable_s3_rate_limiter;

    // Enable S3 rate limiter for this test
    config::enable_s3_rate_limiter = true;

    // Create multiple test directories with files
    std::vector<std::string> test_dirs;
    for (int i = 0; i < 3; ++i) {
        std::string test_dir =
                global_test_prefix_ + "rate_limit_delete_dir_objects_" + std::to_string(i) + "/";
        test_dirs.push_back(test_dir);

        // Create multiple files in each directory
        for (int j = 0; j < 5; ++j) {
            std::string test_file = test_dir + "file_" + std::to_string(j) + ".txt";
            io::FileWriterPtr writer;
            Status status = s3_fs_->create_file(test_file, &writer);
            ASSERT_TRUE(status.ok()) << "Failed to create file: " << status.to_string();

            std::string test_data = "Delete directory test data";
            status = writer->append(test_data);
            ASSERT_TRUE(status.ok()) << "Failed to write data: " << status.to_string();

            status = writer->close();
            ASSERT_TRUE(status.ok()) << "Failed to close writer: " << status.to_string();
        }
    }

    // Set rate limit for PUT operations (DeleteObjects uses PUT rate limiter)
    // limit: 2 (allow only 2 DeleteObjects requests, the 3rd will fail)
    // Note: We need to account for the file creation PUT requests above
    // So we set limit to allow those + 2 DeleteObjects calls
    int ret = reset_s3_rate_limiter(S3RateLimitType::PUT, 10, 10, 2);
    ASSERT_EQ(ret, 0) << "Failed to set rate limiter for PUT operations";

    std::cout << "Rate limiter set: limit 2 total requests for PUT operations (DeleteObjects in "
                 "delete_directory)"
              << std::endl;

    // First two delete_directory operations should succeed
    // Each delete_directory calls ListObjectsV2 (GET) and DeleteObjects (PUT)
    Status status;
    for (int i = 0; i < 2; ++i) {
        status = s3_fs_->delete_directory(test_dirs[i]);
        ASSERT_TRUE(status.ok()) << "Failed to delete directory on attempt " << i + 1 << ": "
                                 << status.to_string();
        std::cout << "Delete directory attempt " << i + 1 << " succeeded" << std::endl;
    }

    // Third delete_directory operation should fail due to rate limit on DeleteObjects
    status = s3_fs_->delete_directory(test_dirs[2]);

    EXPECT_FALSE(status.ok())
            << "Third delete_directory should fail due to rate limit on DeleteObjects";
    std::cout << "Third delete_directory failed as expected (DeleteObjects rate limit): "
              << status.to_string() << std::endl;

    // Reset rate limiter to clean up remaining directory
    reset_s3_rate_limiter(S3RateLimitType::PUT, 10000, 10000, 0);
    reset_s3_rate_limiter(S3RateLimitType::GET, 10000, 10000, 0);

    // Restore original config
    config::enable_s3_rate_limiter = original_enable_rate_limiter;

    std::cout << "Rate limiter PUT delete_directory (DeleteObjects) test completed successfully!"
              << std::endl;

    // Cleanup remaining directory
    status = s3_fs_->delete_directory(test_dirs[2]);
    ASSERT_TRUE(status.ok()) << "Failed to cleanup remaining directory: " << status.to_string();
}

// Test: S3 CreateMultipartUpload failure - simulates error when initiating multipart upload
TEST_F(S3FileSystemTest, CreateMultipartUploadFailureTest) {
    ASSERT_NE(s3_fs_, nullptr);

    std::string test_file = global_test_prefix_ + "create_multipart_upload_failure_test.dat";
    std::cout << "Test file path: " << test_file << std::endl;

    // Set up sync point to simulate CreateMultipartUpload failure
    SyncPoint::get_instance()->set_call_back("s3_file_writer::_open", [](std::vector<std::any>&&
                                                                                 args) {
        // The callback receives create_multipart_upload_outcome as argument
        using CreateMultipartUploadOutcome =
                Aws::Utils::Outcome<Aws::S3::Model::CreateMultipartUploadResult, Aws::S3::S3Error>;
        auto* outcome = std::any_cast<CreateMultipartUploadOutcome*>(args[0]);

        // Create a mock S3 error to simulate CreateMultipartUpload failure
        Aws::S3::S3Error error;
        error.SetResponseCode(Aws::Http::HttpResponseCode::FORBIDDEN);
        error.SetExceptionName("AccessDenied");
        error.SetMessage("Simulated CreateMultipartUpload failure - Access Denied");
        error.SetRequestId("test-request-id-12345");

        // Replace the successful outcome with a failure outcome
        *outcome = CreateMultipartUploadOutcome(error);

        std::cout << "SyncPoint: Injected CreateMultipartUpload failure" << std::endl;
    });

    // Enable sync point processing
    SyncPoint::get_instance()->enable_processing();

    // Try to create a large file that will trigger multipart upload
    io::FileWriterPtr writer;
    Status status = s3_fs_->create_file(test_file, &writer);
    ASSERT_TRUE(status.ok()) << "Failed to create file writer: " << status.to_string();

    // Write large data to trigger multipart upload (>5MB for multipart)
    size_t chunk_size = 1024 * 1024; // 1MB
    size_t num_chunks = 6;           // 6MB total, will trigger multipart
    std::string chunk_data(chunk_size, 'A');

    std::cout << "Writing " << (chunk_size * num_chunks)
              << " bytes to trigger CreateMultipartUpload" << std::endl;

    bool write_failed = false;
    for (size_t i = 0; i < num_chunks; ++i) {
        std::ranges::fill(chunk_data, 'A' + (i % 26));
        status = writer->append(chunk_data);
        if (!status.ok()) {
            write_failed = true;
            std::cout << "Write failed at chunk " << i << " as expected: " << status.to_string()
                      << std::endl;
            break;
        }
    }

    if (!write_failed) {
        // If write succeeded, close should fail
        status = writer->close();
        if (!status.ok()) {
            write_failed = true;
            std::cout << "Close failed as expected (CreateMultipartUpload failure): "
                      << status.to_string() << std::endl;
        }
    }

    EXPECT_TRUE(write_failed)
            << "Multipart upload should fail due to CreateMultipartUpload failure";

    // Verify the error message contains information about CreateMultipartUpload failure
    if (write_failed) {
        std::string error_msg = status.to_string();
        std::cout << "Verified CreateMultipartUpload failure with error: " << error_msg
                  << std::endl;

        // Check for expected error indicators
        bool has_expected_error = error_msg.find("CreateMultipartUpload") != std::string::npos ||
                                  error_msg.find("AccessDenied") != std::string::npos ||
                                  error_msg.find("Access Denied") != std::string::npos;

        EXPECT_TRUE(has_expected_error)
                << "Error message should mention CreateMultipartUpload or AccessDenied failure";
    }

    // Disable sync point processing and clear callbacks
    SyncPoint::get_instance()->disable_processing();
    SyncPoint::get_instance()->clear_all_call_backs();

    std::cout << "CreateMultipartUpload failure test completed successfully!" << std::endl;

    // Try to cleanup (file should not exist since upload failed)
    std::ignore = s3_fs_->delete_file(test_file);
}

} // namespace doris
