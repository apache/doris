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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "io/fs/file_system.h"

namespace doris::io {
class S3ObjStorageClient;
}

namespace doris {

/**
 * S3PluginDownloader is an independent, generic S3 downloader.
 * 
 * Design principles:
 * 1. Single responsibility: Only downloads files from S3, no business logic
 * 2. Complete decoupling: No dependency on cloud mode or Doris-specific configuration
 * 3. Reusable: Supports both cloud mode auto-configuration and manual S3 parameters
 * 4. Features: Single file download, batch directory download, MD5 verification, retry mechanism
 */
class S3PluginDownloader {
public:
    /**
     * S3 configuration info (completely independent, no dependency on Doris internal types)
     */
    struct S3Config {
        std::string endpoint;
        std::string region;
        std::string bucket;
        std::string access_key;
        std::string secret_key;

        S3Config(const std::string& endpoint, const std::string& region, const std::string& bucket,
                 const std::string& access_key, const std::string& secret_key)
                : endpoint(endpoint),
                  region(region),
                  bucket(bucket),
                  access_key(access_key),
                  secret_key(secret_key) {}

        std::string to_string() const;
    };

    explicit S3PluginDownloader(const S3Config& config);
    ~S3PluginDownloader();

    // ======================== Core Download Methods ========================

    /**
     * Download single file (supports MD5 verification and retry)
     * 
     * @param remote_s3_path complete S3 path like "s3://bucket/path/to/file.jar"
     * @param local_path local target file path
     * @param expected_md5 optional MD5 checksum, empty to skip verification
     * @return local file path on success, empty string on failure
     */
    std::string download_file(const std::string& remote_s3_path, const std::string& local_path,
                              const std::string& expected_md5 = "");

    /**
     * Download and extract tar.gz file directory (for connectors and other batch resources)
     * 
     * @param remote_s3_directory S3 directory path like "s3://bucket/path/to/connectors/"
     * @param local_directory local target directory path
     * @return local directory path on success, empty string on failure
     */
    std::string download_and_extract_directory(const std::string& remote_s3_directory,
                                               const std::string& local_directory);

private:
    static constexpr int MAX_RETRY_ATTEMPTS = 3;
    static constexpr int RETRY_DELAY_MS = 1000;

    S3Config config_;
    std::shared_ptr<io::S3ObjStorageClient> s3_client_;

    // ======================== Internal Implementation Methods ========================

    /**
     * Execute single file download (internal method, may throw exceptions)
     */
    std::string do_download_file(const std::string& remote_s3_path, const std::string& local_path,
                                 const std::string& expected_md5);

    /**
     * Check if local file exists and is valid
     */
    bool is_local_file_valid(const std::string& local_path, const std::string& expected_md5);

    /**
     * Create parent directory
     */
    Status create_parent_directory(const std::string& file_path);

    /**
     * Extract tar.gz file
     */
    Status extract_tar_gz(const std::string& tar_gz_path, const std::string& target_dir);

    /**
     * Calculate file MD5
     */
    std::string calculate_file_md5(const std::string& file_path);

    /**
     * Create S3 client
     */
    std::shared_ptr<io::S3ObjStorageClient> create_s3_client(const S3Config& config);

    /**
     * Parse S3 path
     */
    struct S3PathInfo {
        std::string bucket;
        std::string key;
    };
    S3PathInfo parse_s3_path(const std::string& s3_path);

    /**
     * List remote files
     */
    Status list_remote_files(const std::string& remote_prefix, std::vector<io::FileInfo>* files);
};

} // namespace doris