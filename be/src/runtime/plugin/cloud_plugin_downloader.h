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

#include <string>

#include "common/status.h"

namespace doris {

/**
 * CloudPluginDownloader is the unified entry point for plugin downloads in cloud mode.
 * 
 * Architecture:
 * 1. CloudPluginConfigProvider - Get S3 config from cloud mode
 * 2. S3PluginDownloader - Execute S3 downloads
 * 3. CloudPluginDownloader - Simple unified API
 */
class CloudPluginDownloader {
public:
    // Plugin type enumeration
    enum class PluginType {
        JDBC_DRIVERS, // JDBC driver jar files
        JAVA_UDF,     // Java UDF jar files
        CONNECTORS,   // Trino connector tar.gz packages
        HADOOP_CONF   // Hadoop configuration files
    };

    // Download plugin from cloud storage
    static Status download_from_cloud(PluginType plugin_type, const std::string& plugin_name,
                                      const std::string& local_target_path,
                                      std::string* local_path);

private:
    // Convert plugin type to directory name
    static std::string _plugin_type_to_string(PluginType plugin_type);
};

} // namespace doris