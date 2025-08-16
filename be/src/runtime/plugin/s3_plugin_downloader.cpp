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

#include <fmt/format.h>

#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include "common/logging.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "io/fs/s3_file_system.h"
#include "util/s3_util.h"

namespace doris {

std::mutex S3PluginDownloader::_download_mutex;
std::string S3PluginDownloader::S3Config::to_string() const {
    return fmt::format("S3Config{{endpoint='{}', region='{}', bucket='{}', access_key='{}'}}",
                       endpoint, region, bucket, access_key.empty() ? "null" : "***");
}

S3PluginDownloader::S3PluginDownloader(const S3Config& config) : _config(config) {
    _s3_fs = _create_s3_filesystem(_config);
}

S3PluginDownloader::~S3PluginDownloader() = default;

Status S3PluginDownloader::download_file(const std::string& remote_s3_path,
                                         const std::string& local_target_path,
                                         std::string* local_path) {
    // Check if S3 filesystem is initialized
    if (!_s3_fs) {
        return Status::InternalError("S3 filesystem not initialized");
    }

    // Execute download
    Status download_status = _execute_download(remote_s3_path, local_target_path);

    if (download_status.ok()) {
        *local_path = local_target_path;
        return Status::OK();
    }
    return download_status;
}

Status S3PluginDownloader::_execute_download(const std::string& remote_s3_path,
                                             const std::string& local_path) {
    std::lock_guard<std::mutex> lock(_download_mutex);
    std::filesystem::path file_path(local_path);
    std::filesystem::path parent_dir = file_path.parent_path();
    if (!parent_dir.empty()) {
        Status status = io::global_local_filesystem()->create_directory(parent_dir.string());
        // Ignore error if directory already exists
        if (!status.ok() && !status.is<ErrorCode::FILE_ALREADY_EXIST>()) {
            RETURN_IF_ERROR(status);
        }
    }

    // Delete existing file if present (to ensure clean download)
    if (std::filesystem::exists(local_path)) {
        std::error_code ec;
        if (!std::filesystem::remove(local_path, ec)) {
            return Status::InternalError("Failed to delete existing file: {} ({})", local_path,
                                         ec.message());
        }
    }

    // Use S3FileSystem's public download method
    Status download_status = _s3_fs->download(remote_s3_path, local_path);
    RETURN_IF_ERROR(download_status);

    LOG(INFO) << "Successfully downloaded " << remote_s3_path << " to " << local_path;
    return Status::OK();
}

std::shared_ptr<io::S3FileSystem> S3PluginDownloader::_create_s3_filesystem(
        const S3Config& config) {
    try {
        // Create S3 configuration for S3FileSystem
        S3Conf s3_conf;
        s3_conf.client_conf.endpoint = config.endpoint;
        s3_conf.client_conf.region = config.region;
        s3_conf.client_conf.ak = config.access_key;
        s3_conf.client_conf.sk = config.secret_key;
        s3_conf.client_conf.provider = io::ObjStorageType::AWS; // Default to AWS compatible
        s3_conf.bucket = config.bucket;
        s3_conf.prefix = ""; // No prefix for direct S3 access

        // Create S3FileSystem using static factory method
        auto result = io::S3FileSystem::create(s3_conf, "s3_plugin_downloader");
        if (!result.has_value()) {
            LOG(WARNING) << "Failed to create S3FileSystem: " << result.error().to_string();
            return nullptr;
        }

        return result.value();
    } catch (const std::exception& e) {
        LOG(WARNING) << "Exception creating S3 filesystem: " << e.what();
        return nullptr;
    }
}

} // namespace doris