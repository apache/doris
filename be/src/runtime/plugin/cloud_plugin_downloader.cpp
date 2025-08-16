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

#include "common/status.h"
#include "runtime/plugin/cloud_plugin_config_provider.h"

namespace doris {

Status CloudPluginDownloader::download_from_cloud(PluginType plugin_type,
                                                  const std::string& plugin_name,
                                                  const std::string& local_target_path,
                                                  std::string* local_path) {
    // Check supported plugin types first
    if (plugin_type != PluginType::JDBC_DRIVERS && plugin_type != PluginType::JAVA_UDF) {
        return Status::InvalidArgument("Unsupported plugin type for cloud download: {}",
                                       _plugin_type_to_string(plugin_type));
    }

    if (plugin_name.empty()) {
        return Status::InvalidArgument("plugin_name cannot be empty");
    }

    // 1. Get cloud configuration and build S3 path
    std::unique_ptr<S3PluginDownloader::S3Config> s3_config;
    Status status = CloudPluginConfigProvider::get_cloud_s3_config(&s3_config);
    RETURN_IF_ERROR(status);

    std::string instance_id;
    status = CloudPluginConfigProvider::get_cloud_instance_id(&instance_id);
    RETURN_IF_ERROR(status);

    // 2. Direct path construction
    std::string s3_path = fmt::format("s3://{}/{}/plugins/{}/{}", s3_config->bucket, instance_id,
                                      _plugin_type_to_string(plugin_type), plugin_name);

    // 3. Execute download
    S3PluginDownloader downloader(*s3_config);
    return downloader.download_file(s3_path, local_target_path, local_path);
}

std::string CloudPluginDownloader::_plugin_type_to_string(PluginType plugin_type) {
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