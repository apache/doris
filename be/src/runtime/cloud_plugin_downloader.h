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

#include "runtime/s3_plugin_downloader.h"

namespace doris {

/**
 * CloudPluginDownloader is the unified entry point for plugin downloads in cloud mode.
 * 
 * Architecture design (decoupled separation):
 * 1. CloudPluginConfigProvider - Get S3 authentication info and path configuration from cloud mode
 * 2. S3PluginDownloader - Execute pure S3 download operations (no dependency on cloud mode)
 * 3. CloudPluginDownloader - Compose the above two, providing a simple API
 * 
 * Advantages:
 * - Responsibility separation: Configuration retrieval vs file download
 * - Extensibility: Non-cloud mode users can manually configure S3 parameters using S3PluginDownloader
 * - Easy testing: Each component can be tested independently
 */
class CloudPluginDownloader {
public:
    /**
     * Plugin type enumeration
     */
    enum class PluginType {
        JDBC_DRIVERS, // JDBC driver jar files
        JAVA_UDF,     // Java UDF jar files
        CONNECTORS,   // Trino connector tar.gz packages
        HADOOP_CONF   // Hadoop configuration files
    };

    /**
     * Download plugin (main entry method, backward compatible)
     * 
     * @param plugin_type Plugin type
     * @param plugin_name Plugin name (can be empty for CONNECTORS batch download)
     * @param local_target_path Local target path
     * @return Returns local path on success, empty string on failure
     */
    static std::string download_plugin_if_needed(PluginType plugin_type,
                                                 const std::string& plugin_name,
                                                 const std::string& local_target_path);

    /**
     * Download plugin from cloud storage (supports MD5 verification)
     * 
     * @param plugin_type Plugin type
     * @param plugin_name Plugin name
     * @param local_target_path Local target path
     * @param expected_md5 Optional MD5 verification value
     * @return Returns local path on success, empty string on failure
     */
    static std::string download_from_cloud(PluginType plugin_type, const std::string& plugin_name,
                                           const std::string& local_target_path,
                                           const std::string& expected_md5 = "");

    /**
     * Manually configured S3 plugin download (also usable in non-cloud mode)
     * 
     * @param s3_config Manually configured S3 parameters
     * @param remote_s3_path Remote S3 path
     * @param local_path Local path  
     * @param expected_md5 Optional MD5 verification value
     * @return Returns local path on success, empty string on failure
     */
    static std::string download_from_s3(const S3PluginDownloader::S3Config& s3_config,
                                        const std::string& remote_s3_path,
                                        const std::string& local_path,
                                        const std::string& expected_md5 = "");

private:
    /**
     * Build S3 path  
     */
    static std::string build_s3_path(const S3PluginDownloader::S3Config& s3_config,
                                     PluginType plugin_type, const std::string& plugin_name);

    /**
     * Convert plugin type to directory name
     */
    static std::string plugin_type_to_string(PluginType plugin_type);
};

} // namespace doris