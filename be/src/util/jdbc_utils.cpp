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

#include <filesystem>

#include "cloud/config.h"
#include "common/config.h"
#include "runtime/plugin/cloud_plugin_downloader.h"

namespace doris {

Status JdbcUtils::resolve_driver_url(const std::string& url, std::string* result_url) {
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
        if (std::filesystem::exists(target_path)) {
            *result_url = "file://" + target_path;
        } else if (std::filesystem::exists(old_target_path)) {
            *result_url = "file://" + old_target_path;
        } else if (config::is_cloud_mode()) {
            // In cloud/elastic deployments, BEs are ephemeral and driver JARs
            // may not exist locally. Try downloading from cloud storage.
            std::string downloaded_path;
            RETURN_IF_ERROR(CloudPluginDownloader::download_from_cloud(
                    CloudPluginDownloader::PluginType::JDBC_DRIVERS, url, target_path,
                    &downloaded_path));
            *result_url = "file://" + downloaded_path;
        } else {
            return Status::InternalError("JDBC driver file does not exist: " + url);
        }
    } else {
        *result_url = "file://" + config::jdbc_drivers_dir + "/" + url;
    }
    return Status::OK();
}

} // namespace doris
