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

#include "util/jdbc_utils.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <filesystem>
#include <fstream>

#include "cloud/config.h"
#include "common/config.h"
#include "runtime/plugin/cloud_plugin_downloader.h"
#include "util/defer_op.h"
#include "util/md5.h"

namespace doris {

namespace {

std::atomic_uint64_t jdbc_driver_temp_sequence {0};

bool is_md5_checksum(const std::string& checksum) {
    return checksum.size() == MD5_HEX_LENGTH &&
           std::all_of(checksum.begin(), checksum.end(),
                       [](unsigned char ch) { return std::isxdigit(ch) != 0; });
}

Status compute_file_checksum(const std::filesystem::path& path, std::string* checksum) {
    std::ifstream input(path, std::ios::binary);
    if (!input.is_open()) {
        return Status::IOError("Failed to open JDBC driver for checksum: {}", path.string());
    }

    Md5Digest digest;
    char buffer[8192];
    while (input.read(buffer, sizeof(buffer)) || input.gcount() > 0) {
        digest.update(buffer, static_cast<size_t>(input.gcount()));
    }
    if (!input.eof()) {
        return Status::IOError("Failed to read JDBC driver for checksum: {}", path.string());
    }
    digest.digest();
    *checksum = digest.hex();
    return Status::OK();
}

Status materialize_driver_version(const std::filesystem::path& source_path,
                                  const std::filesystem::path& target_path,
                                  const std::string& expected_checksum, std::string* result_url) {
    if (std::filesystem::exists(target_path)) {
        std::string target_checksum;
        RETURN_IF_ERROR(compute_file_checksum(target_path, &target_checksum));
        if (target_checksum == expected_checksum) {
            *result_url = "file://" + target_path.string();
            return Status::OK();
        }
    }

    std::string source_checksum;
    RETURN_IF_ERROR(compute_file_checksum(source_path, &source_checksum));
    if (source_checksum != expected_checksum) {
        return Status::InternalError("JDBC driver checksum mismatch for {}: expected {}, actual {}",
                                     source_path.string(), expected_checksum, source_checksum);
    }

    std::error_code error;
    std::filesystem::create_directories(target_path.parent_path(), error);
    if (error) {
        return Status::IOError("Failed to create JDBC driver cache directory {}: {}",
                               target_path.parent_path().string(), error.message());
    }

    std::filesystem::path temp_path =
            target_path.string() + ".tmp." +
            std::to_string(jdbc_driver_temp_sequence.fetch_add(1, std::memory_order_relaxed));
    Defer cleanup_temp {[&]() {
        std::error_code cleanup_error;
        std::filesystem::remove(temp_path, cleanup_error);
    }};
    std::filesystem::copy_file(source_path, temp_path,
                               std::filesystem::copy_options::overwrite_existing, error);
    if (error) {
        return Status::IOError("Failed to copy JDBC driver {} to {}: {}", source_path.string(),
                               temp_path.string(), error.message());
    }
    std::filesystem::rename(temp_path, target_path, error);
    if (error) {
        return Status::IOError("Failed to publish JDBC driver {}: {}", target_path.string(),
                               error.message());
    }
    *result_url = "file://" + target_path.string();
    return Status::OK();
}

} // namespace

Status JdbcUtils::resolve_driver_url(const std::string& url, std::string* result_url) {
    return resolve_driver_url(url, "", result_url);
}

Status JdbcUtils::resolve_driver_url(const std::string& url, const std::string& checksum,
                                     std::string* result_url) {
    // Already a full URL (e.g. "file:///path/to/driver.jar" or "hdfs://...")
    if (url.find(":/") != std::string::npos) {
        *result_url = url;
        return Status::OK();
    }

    const char* doris_home = std::getenv("DORIS_HOME");
    if (doris_home == nullptr) {
        return Status::InternalError("DORIS_HOME environment variable is not set");
    }

    std::string default_url = std::string(doris_home) + "/plugins/jdbc_drivers";
    std::string default_old_url = std::string(doris_home) + "/jdbc_drivers";

    if (config::jdbc_drivers_dir == default_url) {
        std::string target_path = default_url + "/" + url;
        std::string old_target_path = default_old_url + "/" + url;
        std::string normalized_checksum = checksum;
        std::transform(normalized_checksum.begin(), normalized_checksum.end(),
                       normalized_checksum.begin(),
                       [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
        if (!normalized_checksum.empty() && !is_md5_checksum(normalized_checksum)) {
            return Status::InvalidArgument("Invalid JDBC driver checksum: {}", checksum);
        }
        if (!normalized_checksum.empty()) {
            std::filesystem::path versioned_path = std::filesystem::path(default_url) / ".cache" /
                                                   normalized_checksum / "driver.jar";
            if (std::filesystem::exists(versioned_path)) {
                std::string versioned_checksum;
                RETURN_IF_ERROR(compute_file_checksum(versioned_path, &versioned_checksum));
                if (versioned_checksum == normalized_checksum) {
                    *result_url = "file://" + versioned_path.string();
                    return Status::OK();
                }
            }
        }

        std::string source_path;
        if (!normalized_checksum.empty() && std::filesystem::exists(target_path)) {
            std::string mutable_mirror_checksum;
            RETURN_IF_ERROR(compute_file_checksum(target_path, &mutable_mirror_checksum));
            if (mutable_mirror_checksum == normalized_checksum) {
                source_path = target_path;
            }
        }
        bool legacy_saas_mode = false;
        if (source_path.empty() && config::is_cloud_mode()) {
            RETURN_IF_ERROR(CloudPluginDownloader::get_legacy_saas_mode(&legacy_saas_mode));
        }
        if (source_path.empty()) {
            if (legacy_saas_mode) {
                // The instance object store is authoritative for the mutable bare-name mirror. The
                // checksum cache below keeps each catalog generation immutable after publication.
                std::string downloaded_path;
                RETURN_IF_ERROR(CloudPluginDownloader::download_from_cloud(
                        CloudPluginDownloader::PluginType::JDBC_DRIVERS, url, target_path,
                        &downloaded_path));
                source_path = downloaded_path;
            } else if (std::filesystem::exists(target_path)) {
                source_path = target_path;
            } else if (std::filesystem::exists(old_target_path)) {
                source_path = old_target_path;
            } else {
                return Status::InternalError("JDBC driver file does not exist: " + url);
            }
        }

        if (normalized_checksum.empty()) {
            *result_url = "file://" + source_path;
            return Status::OK();
        }
        std::filesystem::path versioned_path =
                std::filesystem::path(default_url) / ".cache" / normalized_checksum / "driver.jar";
        return materialize_driver_version(source_path, versioned_path, normalized_checksum,
                                          result_url);
    } else {
        *result_url = "file://" + config::jdbc_drivers_dir + "/" + url;
    }
    return Status::OK();
}

} // namespace doris
