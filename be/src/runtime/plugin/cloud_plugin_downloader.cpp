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

#include "cloud/cloud_storage_engine.h"
#include "io/fs/local_file_system.h"
#include "io/fs/remote_file_system.h"
#include "runtime/exec_env.h"

namespace doris {

// Use 10MB buffer for all downloads - same as cloud_warm_up_manager
static constexpr size_t DOWNLOAD_BUFFER_SIZE = 10 * 1024 * 1024; // 10MB

// Static mutex definition for synchronizing downloads
std::mutex CloudPluginDownloader::_download_mutex;

Status CloudPluginDownloader::download_from_cloud(PluginType type, const std::string& name,
                                                  const std::string& local_path,
                                                  std::string* result_path) {
    // Use lock_guard to synchronize concurrent downloads
    std::lock_guard<std::mutex> lock(_download_mutex);

    if (name.empty()) {
        return Status::InvalidArgument("Plugin name cannot be empty");
    }

    CloudPluginDownloader downloader;

    // 1. Get FileSystem
    io::RemoteFileSystemSPtr filesystem;
    RETURN_IF_ERROR(downloader._get_cloud_filesystem(&filesystem));

    // 2. Build remote plugin path
    std::string remote_path;
    RETURN_IF_ERROR(downloader._build_plugin_path(type, name, &remote_path));
    LOG(INFO) << "Downloading plugin: " << name << " -> " << local_path;

    // 3. Prepare local environment
    RETURN_IF_ERROR(downloader._prepare_local_path(local_path));

    // 4. Download remote file to local
    RETURN_IF_ERROR(downloader._download_remote_file(filesystem, remote_path, local_path));

    *result_path = local_path;
    LOG(INFO) << "Successfully downloaded plugin: " << name << " to " << local_path;

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
    *path = fmt::format("plugins/{}/{}", type_name, name);
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
    // Remove existing file if present
    bool exists = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(local_path, &exists));
    if (exists) {
        RETURN_IF_ERROR(io::global_local_filesystem()->delete_file(local_path));
        LOG(INFO) << "Removed existing file: " << local_path;
    }

    // Ensure local directory exists
    std::string dir_path = local_path.substr(0, local_path.find_last_of('/'));
    if (!dir_path.empty()) {
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

    // Create local file writer
    io::FileWriterPtr local_writer;
    RETURN_IF_ERROR(io::global_local_filesystem()->create_file(local_path, &local_writer));

    auto buffer = std::make_unique<char[]>(DOWNLOAD_BUFFER_SIZE);
    size_t total_read = 0;
    while (total_read < file_size) {
        size_t to_read =
                std::min(DOWNLOAD_BUFFER_SIZE, static_cast<size_t>(file_size - total_read));
        size_t bytes_read;
        RETURN_IF_ERROR(remote_reader->read_at(total_read, {buffer.get(), to_read}, &bytes_read));
        RETURN_IF_ERROR(local_writer->append({buffer.get(), bytes_read}));
        total_read += bytes_read;
    }

    RETURN_IF_ERROR(local_writer->close());
    return Status::OK();
}

} // namespace doris