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
#include <mutex>
#include <string>

#include "common/status.h"

namespace doris::io {
class RemoteFileSystem;
using RemoteFileSystemSPtr = std::shared_ptr<RemoteFileSystem>;
} // namespace doris::io

namespace doris {

/**
 * Cloud plugin downloader with testable design
 * Uses friend class pattern for easy unit testing
 */
class CloudPluginDownloader {
public:
    enum class PluginType { JDBC_DRIVERS, JAVA_UDF, CONNECTORS, HADOOP_CONF };

    /**
     * Download plugin from cloud storage to local path
     */
    static Status download_from_cloud(PluginType type, const std::string& name,
                                      const std::string& local_path, std::string* result_path);

private:
    friend class CloudPluginDownloaderTest;

    // Build remote plugin path: plugins/{type}/{name}
    Status _build_plugin_path(PluginType type, const std::string& name, std::string* path);

    // Get cloud filesystem
    Status _get_cloud_filesystem(io::RemoteFileSystemSPtr* filesystem);

    // Prepare local environment (remove existing file, create directory)
    Status _prepare_local_path(const std::string& local_path);

    // High-performance file download using 10MB buffer
    Status _download_remote_file(io::RemoteFileSystemSPtr remote_fs, const std::string& remote_path,
                                 const std::string& local_path);

    // Static mutex for synchronizing concurrent downloads
    static std::mutex _download_mutex;
};

} // namespace doris