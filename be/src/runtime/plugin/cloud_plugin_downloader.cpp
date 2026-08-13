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

#include <fmt/format.h>

#include <cstdlib>
#include <filesystem>
#include <mutex>
#include <unordered_map>

#include "cloud/cloud_storage_engine.h"
#include "io/fs/local_file_system.h"
#include "io/fs/remote_file_system.h"
#include "runtime/exec_env.h"
#include "util/defer_op.h"

namespace doris {

// Use 10MB buffer for all downloads - same as cloud_warm_up_manager
static constexpr size_t DOWNLOAD_BUFFER_SIZE = 10 * 1024 * 1024; // 10MB

namespace {

std::mutex download_locks_guard;
std::unordered_map<std::string, std::weak_ptr<std::mutex>> download_locks;

std::shared_ptr<std::mutex> get_download_lock(const std::string& local_path) {
    std::lock_guard<std::mutex> guard(download_locks_guard);
    for (auto it = download_locks.begin(); it != download_locks.end();) {
        if (it->second.expired()) {
            it = download_locks.erase(it);
        } else {
            ++it;
        }
    }
    std::string normalized_path =
            std::filesystem::path(local_path).lexically_normal().generic_string();
    std::shared_ptr<std::mutex> target_lock = download_locks[normalized_path].lock();
    if (!target_lock) {
        target_lock = std::make_shared<std::mutex>();
        download_locks[normalized_path] = target_lock;
    }
    return target_lock;
}

} // namespace

Status CloudPluginDownloader::download_from_cloud(PluginType type, const std::string& name,
                                                  const std::string& local_path,
                                                  std::string* result_path) {
    CloudPluginDownloader downloader;

    // 1. Build and validate the remote key before accessing cloud state.
    std::string remote_path;
    RETURN_IF_ERROR(downloader._build_plugin_path(type, name, &remote_path));
    RETURN_IF_ERROR(downloader._validate_local_path(remote_path, local_path));

    // 2. Get FileSystem
    io::RemoteFileSystemSPtr filesystem;
    RETURN_IF_ERROR(downloader._get_cloud_filesystem(&filesystem));

    std::shared_ptr<std::mutex> target_lock = get_download_lock(local_path);
    std::lock_guard<std::mutex> lock(*target_lock);
    LOG(INFO) << "Downloading plugin: " << name << " -> " << local_path;

    // 3. Prepare local environment
    RETURN_IF_ERROR(downloader._prepare_local_path(local_path));

    // 4. Download remote file to local
    RETURN_IF_ERROR(downloader._download_remote_file(filesystem, remote_path, local_path));

    *result_path = local_path;
    LOG(INFO) << "Successfully downloaded plugin: " << name << " to " << local_path;

    return Status::OK();
}

Status CloudPluginDownloader::get_legacy_saas_mode(bool* legacy_saas_mode) {
    BaseStorageEngine& base_engine = ExecEnv::GetInstance()->storage_engine();
    CloudStorageEngine* cloud_engine = dynamic_cast<CloudStorageEngine*>(&base_engine);
    if (!cloud_engine) {
        return Status::NotFound("CloudStorageEngine not found, not in cloud mode");
    }
    *legacy_saas_mode = !cloud_engine->enable_storage_vault();
    return Status::OK();
}

Status CloudPluginDownloader::_build_plugin_path(PluginType type, const std::string& name,
                                                 std::string* path) {
    std::string type_name;
    switch (type) {
    case PluginType::JDBC_DRIVERS:
        type_name = "jdbc_drivers";
        break;
    case PluginType::JAVA_UDF:
        type_name = "java_udf";
        break;
    default:
        return Status::InvalidArgument("Unsupported plugin type: {}", static_cast<int>(type));
    }
    RETURN_IF_ERROR(_validate_plugin_name(name));
    *path = fmt::format("plugins/{}/{}", type_name, name);
    return Status::OK();
}

Status CloudPluginDownloader::_validate_plugin_name(const std::string& name) {
    if (name.empty()) {
        return Status::InvalidArgument("Plugin name cannot be empty");
    }
    if (name.size() < 4 || name.compare(name.size() - 4, 4, ".jar") != 0) {
        return Status::InvalidArgument("Plugin name must be a safe relative jar path: {}", name);
    }
    for (char ch : name) {
        bool is_ascii_letter = (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z');
        bool is_digit = ch >= '0' && ch <= '9';
        if (!is_ascii_letter && !is_digit && ch != '.' && ch != '_' && ch != '@' && ch != '-' &&
            ch != '/') {
            return Status::InvalidArgument("Plugin name must be a safe relative jar path: {}",
                                           name);
        }
    }

    std::filesystem::path plugin_name(name);
    if (plugin_name.is_absolute() || plugin_name.lexically_normal().generic_string() != name) {
        return Status::InvalidArgument("Plugin name must stay inside its plugin directory: {}",
                                       name);
    }
    return Status::OK();
}

Status CloudPluginDownloader::_validate_local_path(const std::string& remote_path,
                                                   const std::string& local_path) {
    const char* doris_home = std::getenv("DORIS_HOME");
    if (doris_home == nullptr) {
        return Status::InternalError("DORIS_HOME environment variable is not set");
    }
    std::filesystem::path expected_path =
            (std::filesystem::path(doris_home) / remote_path).lexically_normal();
    std::filesystem::path actual_path = std::filesystem::path(local_path).lexically_normal();
    if (actual_path != expected_path) {
        return Status::InvalidArgument("Plugin target must be {}", expected_path.generic_string());
    }
    return Status::OK();
}

Status CloudPluginDownloader::_get_cloud_filesystem(io::RemoteFileSystemSPtr* filesystem) {
    BaseStorageEngine& base_engine = ExecEnv::GetInstance()->storage_engine();
    CloudStorageEngine* cloud_engine = dynamic_cast<CloudStorageEngine*>(&base_engine);
    if (!cloud_engine) {
        return Status::NotFound("CloudStorageEngine not found, not in cloud mode");
    }

    *filesystem = cloud_engine->latest_fs();
    if (!*filesystem) {
        return Status::NotFound("No latest filesystem available in cloud mode");
    }

    return Status::OK();
}

Status CloudPluginDownloader::_prepare_local_path(const std::string& local_path) {
    // Ensure local directory exists
    size_t separator = local_path.find_last_of('/');
    if (separator != std::string::npos && separator > 0) {
        std::string dir_path = local_path.substr(0, separator);
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(dir_path));
    }

    return Status::OK();
}

Status CloudPluginDownloader::_download_remote_file(io::RemoteFileSystemSPtr filesystem,
                                                    const std::string& remote_path,
                                                    const std::string& local_path) {
    // Open remote file for reading
    io::FileReaderSPtr remote_reader;
    io::FileReaderOptions opts;
    RETURN_IF_ERROR(filesystem->open_file(remote_path, &remote_reader, &opts));

    // Get file size
    int64_t file_size;
    RETURN_IF_ERROR(filesystem->file_size(remote_path, &file_size));

    // Download into a sibling temporary file so a failed transfer never destroys the current
    // plugin. rename() publishes the completed file atomically on the local filesystem.
    std::string temp_path = local_path + ".tmp";
    bool temp_exists = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(temp_path, &temp_exists));
    if (temp_exists) {
        RETURN_IF_ERROR(io::global_local_filesystem()->delete_file(temp_path));
    }
    Defer cleanup_temp {[&]() {
        bool exists = false;
        Status exists_status = io::global_local_filesystem()->exists(temp_path, &exists);
        if (exists_status.ok() && exists) {
            Status delete_status = io::global_local_filesystem()->delete_file(temp_path);
            if (!delete_status.ok()) {
                LOG(WARNING) << "Failed to clean temporary plugin file " << temp_path << ": "
                             << delete_status;
            }
        }
    }};

    io::FileWriterPtr local_writer;
    RETURN_IF_ERROR(io::global_local_filesystem()->create_file(temp_path, &local_writer));

    auto buffer = std::make_unique<char[]>(DOWNLOAD_BUFFER_SIZE);
    size_t total_read = 0;
    while (total_read < file_size) {
        size_t to_read =
                std::min(DOWNLOAD_BUFFER_SIZE, static_cast<size_t>(file_size - total_read));
        size_t bytes_read;
        RETURN_IF_ERROR(remote_reader->read_at(total_read, {buffer.get(), to_read}, &bytes_read));
        if (bytes_read == 0) {
            return Status::IOError("Remote plugin {} ended after {} of {} bytes", remote_path,
                                   total_read, file_size);
        }
        RETURN_IF_ERROR(local_writer->append({buffer.get(), bytes_read}));
        total_read += bytes_read;
    }

    RETURN_IF_ERROR(local_writer->close());
    return io::global_local_filesystem()->rename(temp_path, local_path);
}

} // namespace doris
