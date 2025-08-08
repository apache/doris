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

#include "common/status.h"
#include "runtime/s3_plugin_downloader.h"

namespace doris {

/**
 * CloudPluginConfigProvider retrieves S3 authentication info and plugin base paths 
 * from Doris cloud mode environment.
 * 
 * Responsibilities:
 * - Configuration retrieval only, no download logic
 * - Converts complex cloud mode configuration to simple S3 config objects
 * - Provides unified configuration interface, hiding implementation details
 * 
 * Uses CloudMetaMgr to get storage configuration, corresponding to FE's StorageVaultMgr approach.
 */
class CloudPluginConfigProvider {
public:
    /**
     * Get S3 configuration from cloud mode
     * 
     * @return S3 config object, or nullptr if failed
     */
    static std::unique_ptr<S3PluginDownloader::S3Config> get_cloud_s3_config();

    /**
     * Get plugin base path for building complete S3 paths
     * 
     * @return base path like "instanceId/plugins"
     */
    static std::string get_plugin_base_path();

    /**
     * Build complete S3 plugin path
     * 
     * @param plugin_type plugin type like "jdbc_drivers"
     * @param plugin_name plugin name like "mysql-connector.jar", empty for directory downloads
     * @return complete path like "instanceId/plugins/jdbc_drivers/mysql-connector.jar"
     */
    static std::string build_plugin_path(const std::string& plugin_type,
                                         const std::string& plugin_name = "");

private:
    /**
     * Get default storage vault info using CloudMetaMgr
     * Corresponds to FE's StorageVaultMgr approach
     */
    static Status get_default_storage_vault_info(S3PluginDownloader::S3Config* s3_config);

    /**
     * Get cloud instance ID
     */
    static std::string get_cloud_instance_id();
};

} // namespace doris