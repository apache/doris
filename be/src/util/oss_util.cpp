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

#include "util/oss_util.h"

#include <alibabacloud/oss/OssClient.h>
#include <alibabacloud/oss/client/ClientConfiguration.h>
#include <bvar/reducer.h>
#include <util/string_util.h>

#include "common/config.h"
#include "common/logging.h"
#include "cpp/aws_common.h"
#include "cpp/oss_credential_provider.h"
#include "util/s3_util.h"

namespace doris {

namespace oss_bvar {
bvar::LatencyRecorder oss_get_latency("oss_get");
bvar::LatencyRecorder oss_put_latency("oss_put");
bvar::LatencyRecorder oss_delete_object_latency("oss_delete_object");
bvar::LatencyRecorder oss_delete_objects_latency("oss_delete_objects");
bvar::LatencyRecorder oss_head_latency("oss_head");
bvar::LatencyRecorder oss_list_latency("oss_list");
bvar::LatencyRecorder oss_multi_part_upload_latency("oss_multi_part_upload");
} // namespace oss_bvar

OSSConf OSSConf::get_oss_conf(const cloud::ObjectStoreInfoPB& obj_info) {
    OSSConf conf;
    conf.bucket = obj_info.bucket();
    conf.prefix = obj_info.prefix();

    conf.client_conf.endpoint = normalize_oss_endpoint(obj_info.endpoint());
    conf.client_conf.region = obj_info.region();
    conf.client_conf.bucket = obj_info.bucket();

    // Parse credential provider type
    if (obj_info.has_cred_provider_type()) {
        switch (obj_info.cred_provider_type()) {
        case cloud::CredProviderTypePB::INSTANCE_PROFILE:
            conf.client_conf.cred_provider_type = OSSCredProviderType::INSTANCE_PROFILE;
            LOG(INFO) << "Using OSS INSTANCE_PROFILE credential provider";
            break;
        case cloud::CredProviderTypePB::SIMPLE:
            conf.client_conf.cred_provider_type = OSSCredProviderType::SIMPLE;
            conf.client_conf.ak = obj_info.ak();
            conf.client_conf.sk = obj_info.sk();
            // Note: token is not read from ObjectStoreInfoPB
            // For temporary credentials, use INSTANCE_PROFILE mode with ECS metadata
            LOG(INFO) << "Using OSS SIMPLE credential provider";
            break;
        default:
            conf.client_conf.cred_provider_type = OSSCredProviderType::INSTANCE_PROFILE;
            LOG(INFO) << "Unknown credential provider type, defaulting to INSTANCE_PROFILE";
            break;
        }
    } else {
        // No credential provider type specified, check if AK/SK provided
        if (!obj_info.ak().empty() && !obj_info.sk().empty()) {
            conf.client_conf.cred_provider_type = OSSCredProviderType::SIMPLE;
            conf.client_conf.ak = obj_info.ak();
            conf.client_conf.sk = obj_info.sk();
            // Note: token is not read from ObjectStoreInfoPB
            LOG(INFO) << "Using OSS SIMPLE credential provider (from AK/SK)";
        } else {
            conf.client_conf.cred_provider_type = OSSCredProviderType::INSTANCE_PROFILE;
            LOG(INFO) << "No AK/SK provided, using OSS INSTANCE_PROFILE credential provider";
        }
    }

    return conf;
}

Status OSSClientFactory::convert_properties_to_oss_conf(
        const std::map<std::string, std::string>& prop, OSSConf* oss_conf) {
    auto get_property = [&](const std::string& key, std::string* value) -> bool {
        auto it = prop.find(key);
        if (it != prop.end()) {
            *value = it->second;
            return true;
        }
        return false;
    };

    std::string endpoint, bucket, prefix, region, ak, sk, token, provider;

    if (!get_property("oss.endpoint", &endpoint)) {
        return Status::InvalidArgument("Missing oss.endpoint");
    }
    if (!get_property("oss.bucket", &bucket)) {
        return Status::InvalidArgument("Missing oss.bucket");
    }

    get_property("oss.prefix", &prefix);
    get_property("oss.region", &region);
    get_property("oss.access_key", &ak);
    get_property("oss.secret_key", &sk);
    get_property("oss.session_token", &token);
    get_property("oss.provider", &provider);

    oss_conf->bucket = bucket;
    oss_conf->prefix = prefix;
    oss_conf->client_conf.endpoint = normalize_oss_endpoint(endpoint);
    oss_conf->client_conf.region = region;
    oss_conf->client_conf.bucket = bucket;

    // Determine credential provider type
    if (provider == "INSTANCE_PROFILE" || provider == "instance_profile") {
        oss_conf->client_conf.cred_provider_type = OSSCredProviderType::INSTANCE_PROFILE;
    } else if (provider == "SIMPLE" || provider == "simple") {
        oss_conf->client_conf.cred_provider_type = OSSCredProviderType::SIMPLE;
        oss_conf->client_conf.ak = ak;
        oss_conf->client_conf.sk = sk;
        oss_conf->client_conf.token = token;
    } else if (!ak.empty() && !sk.empty()) {
        // If AK/SK provided, use SIMPLE
        oss_conf->client_conf.cred_provider_type = OSSCredProviderType::SIMPLE;
        oss_conf->client_conf.ak = ak;
        oss_conf->client_conf.sk = sk;
        oss_conf->client_conf.token = token;
    } else {
        // Default to INSTANCE_PROFILE
        oss_conf->client_conf.cred_provider_type = OSSCredProviderType::INSTANCE_PROFILE;
    }

    return Status::OK();
}

OSSClientFactory::OSSClientFactory() {
    // Initialize OSS SDK once per process
    static std::once_flag init_flag;
    std::call_once(init_flag, []() {
        AlibabaCloud::OSS::InitializeSdk();
        LOG(INFO) << "Alibaba Cloud OSS SDK initialized by OSSClientFactory";
    });
    _ca_cert_file_path = get_valid_ca_cert_path(doris::split(config::ca_cert_file_paths, ";"));
}

OSSClientFactory::~OSSClientFactory() = default;

OSSClientFactory& OSSClientFactory::instance() {
    static OSSClientFactory instance;
    return instance;
}

std::shared_ptr<AlibabaCloud::OSS::OssClient> OSSClientFactory::create(
        const OSSClientConf& oss_conf) {
    uint64_t hash = oss_conf.get_hash();

    {
        std::lock_guard<std::mutex> lock(_lock);

        // Check cache
        auto it = _cache.find(hash);
        if (it != _cache.end()) {
            VLOG(2) << "Reusing cached OSS client for endpoint: " << oss_conf.endpoint;
            return it->second;
        }
    }

    // Create new client
    AlibabaCloud::OSS::ClientConfiguration oss_client_config;
    oss_client_config.maxConnections = oss_conf.max_connections;
    oss_client_config.requestTimeoutMs = oss_conf.request_timeout_ms;
    oss_client_config.connectTimeoutMs = oss_conf.connect_timeout_ms;

    // Set CA certificate file for HTTPS verification
    if (_ca_cert_file_path.empty()) {
        _ca_cert_file_path = get_valid_ca_cert_path(doris::split(config::ca_cert_file_paths, ";"));
    }
    if (!_ca_cert_file_path.empty()) {
        oss_client_config.caFile = _ca_cert_file_path;
    }

    std::shared_ptr<AlibabaCloud::OSS::OssClient> client;

    try {
        if (oss_conf.cred_provider_type == OSSCredProviderType::INSTANCE_PROFILE) {
            // Use ECS instance profile credentials
            std::shared_ptr<ECSMetadataCredentialsProvider> provider;

            {
                std::lock_guard<std::mutex> lock(_lock);

                // Check if we already have a credential provider for this config
                auto provider_it = _credential_providers.find(hash);
                if (provider_it != _credential_providers.end()) {
                    provider = provider_it->second;
                } else {
                    provider = std::make_shared<ECSMetadataCredentialsProvider>();
                    _credential_providers[hash] = provider;
                }
            }

            // Create client with credential provider
            // Note: We pass the provider directly to OssClient constructor
            // The OssClient will call getCredentials() when making API calls
            client = std::make_shared<AlibabaCloud::OSS::OssClient>(
                    oss_conf.endpoint, std::static_pointer_cast<
                                               AlibabaCloud::OSS::CredentialsProvider>(provider),
                    oss_client_config);

            LOG(INFO) << "Created OSS client with INSTANCE_PROFILE credentials for endpoint: "
                      << oss_conf.endpoint;
        } else {
            // Use static credentials
            AlibabaCloud::OSS::Credentials creds(oss_conf.ak, oss_conf.sk, oss_conf.token);

            client = std::make_shared<AlibabaCloud::OSS::OssClient>(oss_conf.endpoint, creds,
                                                                     oss_client_config);

            LOG(INFO) << "Created OSS client with SIMPLE credentials for endpoint: "
                      << oss_conf.endpoint;
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to create OSS client: " << e.what();
        return nullptr;
    }

    // Cache the client
    {
        std::lock_guard<std::mutex> lock(_lock);
        _cache[hash] = client;
    }

    return client;
}

} // namespace doris
