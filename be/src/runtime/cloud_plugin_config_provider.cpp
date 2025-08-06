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

#include "runtime/cloud_plugin_config_provider.h"

#include <fmt/format.h>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/config.h"
#include "common/logging.h"
#include "runtime/exec_env.h"
#include "util/s3_util.h"

namespace doris {

std::unique_ptr<S3PluginDownloader::S3Config> CloudPluginConfigProvider::get_cloud_s3_config() {
    try {
        S3PluginDownloader::S3Config s3_config("", "", "", "", "");
        Status status = get_default_storage_vault_info(&s3_config);
        if (!status.ok()) {
            LOG(WARNING) << "Cannot get default storage vault info for plugin download: "
                         << status.to_string();
            return nullptr;
        }

        if (s3_config.bucket.empty() || s3_config.access_key.empty() ||
            s3_config.secret_key.empty()) {
            LOG(WARNING) << "Incomplete S3 configuration: bucket=" << s3_config.bucket
                         << ", access_key=" << (s3_config.access_key.empty() ? "empty" : "***")
                         << ", secret_key=" << (s3_config.secret_key.empty() ? "empty" : "***");
            return nullptr;
        }

        return std::make_unique<S3PluginDownloader::S3Config>(
                s3_config.endpoint, s3_config.region, s3_config.bucket, s3_config.access_key,
                s3_config.secret_key);

    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to get cloud S3 configuration: " << e.what();
        return nullptr;
    }
}

std::string CloudPluginConfigProvider::get_plugin_base_path() {
    try {
        std::string instance_id = get_cloud_instance_id();
        if (instance_id.empty()) {
            LOG(WARNING) << "Cannot get cloud instance ID";
            return "";
        }

        return fmt::format("{}/plugins", instance_id);

    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to get plugin base path: " << e.what();
        return "";
    }
}

std::string CloudPluginConfigProvider::build_plugin_path(const std::string& plugin_type,
                                                         const std::string& plugin_name) {
    std::string base_path = get_plugin_base_path();
    if (base_path.empty()) {
        return "";
    }

    std::string path = fmt::format("{}/{}", base_path, plugin_type);
    if (!plugin_name.empty()) {
        path += "/" + plugin_name;
    }

    return path;
}

Status CloudPluginConfigProvider::get_default_storage_vault_info(
        S3PluginDownloader::S3Config* s3_config) {
    try {
        BaseStorageEngine& base_engine = ExecEnv::GetInstance()->storage_engine();
        CloudStorageEngine* cloud_engine = dynamic_cast<CloudStorageEngine*>(&base_engine);
        if (!cloud_engine) {
            return Status::NotFound("CloudStorageEngine not found, not in cloud mode");
        }

        cloud::CloudMetaMgr& meta_mgr = cloud_engine->meta_mgr();

        cloud::StorageVaultInfos vault_infos;
        bool is_vault_mode = false;
        RETURN_IF_ERROR(meta_mgr.get_storage_vault_info(&vault_infos, &is_vault_mode));

        if (vault_infos.empty()) {
            return Status::NotFound("No storage vault info available");
        }

        const auto& [vault_name, vault_conf, path_format] = vault_infos[0];

        if (const S3Conf* s3_conf = std::get_if<S3Conf>(&vault_conf)) {
            s3_config->endpoint = s3_conf->client_conf.endpoint;
            s3_config->region = s3_conf->client_conf.region;
            s3_config->bucket = s3_conf->bucket;
            s3_config->access_key = s3_conf->client_conf.ak;
            s3_config->secret_key = s3_conf->client_conf.sk;

            LOG(INFO) << "Using storage vault for plugin download: " << vault_name;
            return Status::OK();
        }

        return Status::NotSupported("Only S3-compatible storage is supported for plugin download");

    } catch (const std::exception& e) {
        return Status::InternalError("Failed to get default storage vault info: {}", e.what());
    }
}

std::string CloudPluginConfigProvider::get_cloud_instance_id() {
    try {
        if (config::cluster_id == -1) {
            std::string cloud_unique_id = config::cloud_unique_id;
            if (cloud_unique_id.empty()) {
                LOG(WARNING) << "cloud_unique_id is empty, using default instance id";
                return "default";
            } else {
                // Parse cloud_unique_id format: "1:instanceId:randomString"
                std::vector<std::string> parts;
                size_t start = 0;
                size_t end = cloud_unique_id.find(':');

                while (end != std::string::npos) {
                    parts.push_back(cloud_unique_id.substr(start, end - start));
                    start = end + 1;
                    end = cloud_unique_id.find(':', start);
                }
                parts.push_back(cloud_unique_id.substr(start));

                if (parts.size() >= 2) {
                    std::string instance_id = parts[1];
                    LOG(INFO) << "Parsed instance_id from cloud_unique_id: " << instance_id;
                    return instance_id;
                } else {
                    LOG(WARNING) << "Failed to parse cloud_unique_id, using entire value as "
                                    "instance_id: "
                                 << cloud_unique_id;
                    return cloud_unique_id;
                }
            }
        } else {
            std::string instance_id = std::to_string(config::cluster_id);
            LOG(INFO) << "Using configured cluster_id as instance_id: " << instance_id;
            return instance_id;
        }

    } catch (const std::exception& e) {
        LOG(WARNING) << "Failed to get cloud instance ID: " << e.what();
        return "";
    }
}

} // namespace doris