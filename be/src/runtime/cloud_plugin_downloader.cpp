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

#include "runtime/cloud_plugin_downloader.h"

#include <fmt/format.h>

#include "cloud/config.h"
#include "common/logging.h"
#include "runtime/cloud_plugin_config_provider.h"

namespace doris {

std::string CloudPluginDownloader::download_plugin_if_needed(PluginType plugin_type,
                                                             const std::string& plugin_name,
                                                             const std::string& local_target_path) {
    // Return directly in non-cloud mode
    if (!config::is_cloud_mode()) {
        return "";
    }

    try {
        return download_from_cloud(plugin_type, plugin_name, local_target_path);
    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to download plugin " << plugin_name << " from cloud: " << e.what();
        return "";
    }
}

std::string CloudPluginDownloader::download_from_cloud(PluginType plugin_type,
                                                       const std::string& plugin_name,
                                                       const std::string& local_target_path,
                                                       const std::string& expected_md5) {
    try {
        // 1. Get cloud S3 configuration
        auto s3_config = CloudPluginConfigProvider::get_cloud_s3_config();
        if (!s3_config) {
            LOG(WARNING) << "Cannot get cloud S3 configuration";
            return "";
        }

        // 2. Build S3 path
        std::string s3_path = build_s3_path(*s3_config, plugin_type, plugin_name);
        if (s3_path.empty()) {
            LOG(WARNING) << "Cannot build S3 path for plugin " << plugin_type_to_string(plugin_type)
                         << "/" << plugin_name;
            return "";
        }

        // 3. Use S3 downloader to download
        S3PluginDownloader downloader(*s3_config);
        if (plugin_type == PluginType::CONNECTORS && plugin_name.empty()) {
            // Batch download connectors directory
            return downloader.download_and_extract_directory(s3_path, local_target_path);
        } else {
            // Single file download
            return downloader.download_file(s3_path, local_target_path, expected_md5);
        }

    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to download plugin from cloud: " << e.what();
        return "";
    }
}

std::string CloudPluginDownloader::download_from_s3(const S3PluginDownloader::S3Config& s3_config,
                                                    const std::string& remote_s3_path,
                                                    const std::string& local_path,
                                                    const std::string& expected_md5) {
    try {
        S3PluginDownloader downloader(s3_config);
        return downloader.download_file(remote_s3_path, local_path, expected_md5);
    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to download from S3: " << e.what();
        return "";
    }
}

std::string CloudPluginDownloader::build_s3_path(const S3PluginDownloader::S3Config& s3_config,
                                                 PluginType plugin_type,
                                                 const std::string& plugin_name) {
    try {
        // Build relative path
        std::string relative_path = CloudPluginConfigProvider::build_plugin_path(
                plugin_type_to_string(plugin_type), plugin_name);
        if (relative_path.empty()) {
            return "";
        }

        // Build complete S3 path
        if (plugin_name.empty()) {
            // Directory path, ensure it ends with /
            return fmt::format("s3://{}/{}/", s3_config.bucket, relative_path);
        } else {
            // File path
            return fmt::format("s3://{}/{}", s3_config.bucket, relative_path);
        }

    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to build S3 path: " << e.what();
        return "";
    }
}

std::string CloudPluginDownloader::plugin_type_to_string(PluginType plugin_type) {
    switch (plugin_type) {
    case PluginType::JDBC_DRIVERS:
        return "jdbc_drivers";
    case PluginType::JAVA_UDF:
        return "java_udf";
    case PluginType::CONNECTORS:
        return "connectors";
    case PluginType::HADOOP_CONF:
        return "hadoop_conf";
    default:
        return "unknown";
    }
}

} // namespace doris