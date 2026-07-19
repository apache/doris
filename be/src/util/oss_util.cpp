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

#include "common/config.h"
#include "common/logging.h"
#include "cpp/oss_credential_provider.h"
#include "util/s3_util.h"
#include "util/string_util.h"

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
    conf.client_conf.role_arn = obj_info.role_arn();
    conf.client_conf.external_id = obj_info.external_id();

    if (obj_info.has_cred_provider_type()) {
        switch (obj_info.cred_provider_type()) {
        case cloud::CredProviderTypePB::DEFAULT:
            conf.client_conf.cred_provider_type = OSSCredProviderType::DEFAULT;
            VLOG(2) << "Using OSS DEFAULT credential provider";
            break;
        case cloud::CredProviderTypePB::INSTANCE_PROFILE:
            conf.client_conf.cred_provider_type = OSSCredProviderType::INSTANCE_PROFILE;
            VLOG(2) << "Using OSS INSTANCE_PROFILE credential provider";
            break;
        case cloud::CredProviderTypePB::SIMPLE:
            conf.client_conf.cred_provider_type = OSSCredProviderType::SIMPLE;
            conf.client_conf.ak = obj_info.ak();
            conf.client_conf.sk = obj_info.sk();
            VLOG(2) << "Using OSS SIMPLE credential provider";
            break;
        default:
            conf.client_conf.cred_provider_type = OSSCredProviderType::DEFAULT;
            VLOG(2) << "Unknown credential provider type, defaulting to DEFAULT";
            break;
        }
    } else {
        if (!obj_info.ak().empty() && !obj_info.sk().empty()) {
            conf.client_conf.cred_provider_type = OSSCredProviderType::SIMPLE;
            conf.client_conf.ak = obj_info.ak();
            conf.client_conf.sk = obj_info.sk();
            VLOG(2) << "Using OSS SIMPLE credential provider (from AK/SK)";
        } else {
            conf.client_conf.cred_provider_type = OSSCredProviderType::DEFAULT;
            VLOG(2) << "No AK/SK provided, using OSS DEFAULT credential provider";
        }
    }

    if (!conf.client_conf.role_arn.empty()) {
        VLOG(2) << "OSS AssumeRole enabled with role_arn: " << conf.client_conf.role_arn;
    }

    return conf;
}

OSSClientFactory::OSSClientFactory() {
    static std::once_flag init_flag;
    std::call_once(init_flag, []() {
        AlibabaCloud::OSS::InitializeSdk();
        LOG(INFO) << "Alibaba Cloud OSS SDK initialized";
    });
    _ca_cert_file_path = get_valid_ca_cert_path(doris::split(config::ca_cert_file_paths, ";"));
}

OSSClientFactory::~OSSClientFactory() {
    AlibabaCloud::OSS::ShutdownSdk();
    LOG(INFO) << "Alibaba Cloud OSS SDK shut down";
}

OSSClientFactory& OSSClientFactory::instance() {
    static OSSClientFactory instance;
    return instance;
}

std::shared_ptr<AlibabaCloud::OSS::OssClient> OSSClientFactory::create(
        const OSSClientConf& oss_conf) {
    if (oss_conf.endpoint.empty() || oss_conf.region.empty() || oss_conf.bucket.empty()) {
        LOG(ERROR) << "Invalid OSS conf: endpoint, region and bucket are required";
        return nullptr;
    }
    // SIMPLE + non-empty role_arn is valid: role_arn takes priority in the if-chain below
    // (STS path), using AK/SK as the base credential. Only reject SIMPLE with no AK/SK and no role_arn.
    if (oss_conf.cred_provider_type == OSSCredProviderType::SIMPLE && oss_conf.role_arn.empty() &&
        (oss_conf.ak.empty() || oss_conf.sk.empty())) {
        LOG(ERROR) << "Invalid OSS conf: ak and sk required for SIMPLE credential provider";
        return nullptr;
    }

    uint64_t hash = oss_conf.get_hash();

    {
        std::lock_guard<std::mutex> lock(_lock);
        auto it = _cache.find(hash);
        if (it != _cache.end()) {
            VLOG(2) << "Reusing cached OSS client for endpoint: " << oss_conf.endpoint;
            return it->second;
        }
    }
    // TOCTOU: concurrent calls with the same hash may both miss the cache and create duplicate
    // clients. Accepted pattern matching S3ClientFactory; last writer wins under the lock below.

    AlibabaCloud::OSS::ClientConfiguration oss_client_config;
    oss_client_config.maxConnections = oss_conf.max_connections;
    oss_client_config.requestTimeoutMs = oss_conf.request_timeout_ms;
    oss_client_config.connectTimeoutMs = oss_conf.connect_timeout_ms;

    if (_ca_cert_file_path.empty()) {
        _ca_cert_file_path = get_valid_ca_cert_path(doris::split(config::ca_cert_file_paths, ";"));
    }
    if (!_ca_cert_file_path.empty()) {
        oss_client_config.caFile = _ca_cert_file_path;
    }

    std::shared_ptr<AlibabaCloud::OSS::OssClient> client;

    try {
        if (!oss_conf.role_arn.empty()) {
            std::shared_ptr<OSSSTSCredentialProvider> sts_provider;
            {
                std::lock_guard<std::mutex> lock(_lock);
                auto it = _sts_credential_providers.find(hash);
                if (it != _sts_credential_providers.end()) {
                    sts_provider = it->second;
                } else {
                    std::string region = oss_conf.region.empty() ? "cn-hangzhou" : oss_conf.region;
                    sts_provider = std::make_shared<OSSSTSCredentialProvider>(
                            oss_conf.role_arn, region, oss_conf.external_id, _ca_cert_file_path,
                            oss_conf.sts_endpoint);
                    _sts_credential_providers[hash] = sts_provider;
                }
            }
            client = std::make_shared<AlibabaCloud::OSS::OssClient>(
                    oss_conf.endpoint,
                    std::static_pointer_cast<AlibabaCloud::OSS::CredentialsProvider>(sts_provider),
                    oss_client_config);
            LOG(INFO) << "OSS client created with AssumeRole, endpoint=" << oss_conf.endpoint
                      << ", role_arn=" << oss_conf.role_arn;
        } else if (oss_conf.cred_provider_type == OSSCredProviderType::INSTANCE_PROFILE) {
            std::shared_ptr<ECSMetadataCredentialsProvider> provider;
            {
                std::lock_guard<std::mutex> lock(_lock);
                auto it = _ecs_credential_providers.find(hash);
                if (it != _ecs_credential_providers.end()) {
                    provider = it->second;
                } else {
                    provider = std::make_shared<ECSMetadataCredentialsProvider>();
                    _ecs_credential_providers[hash] = provider;
                }
            }
            client = std::make_shared<AlibabaCloud::OSS::OssClient>(
                    oss_conf.endpoint,
                    std::static_pointer_cast<AlibabaCloud::OSS::CredentialsProvider>(provider),
                    oss_client_config);
            LOG(INFO) << "OSS client created with INSTANCE_PROFILE, endpoint=" << oss_conf.endpoint;
        } else if (oss_conf.cred_provider_type == OSSCredProviderType::DEFAULT) {
            std::shared_ptr<OSSDefaultCredentialsProvider> provider;
            {
                std::lock_guard<std::mutex> lock(_lock);
                auto it = _default_credential_providers.find(hash);
                if (it != _default_credential_providers.end()) {
                    provider = it->second;
                } else {
                    provider = std::make_shared<OSSDefaultCredentialsProvider>();
                    _default_credential_providers[hash] = provider;
                }
            }
            client = std::make_shared<AlibabaCloud::OSS::OssClient>(
                    oss_conf.endpoint,
                    std::static_pointer_cast<AlibabaCloud::OSS::CredentialsProvider>(provider),
                    oss_client_config);
            LOG(INFO) << "OSS client created with DEFAULT provider, endpoint=" << oss_conf.endpoint;
        } else if (oss_conf.cred_provider_type == OSSCredProviderType::ENV) {
            // Read OSS_ACCESS_KEY_ID and OSS_ACCESS_KEY_SECRET from environment
            const char* env_ak = std::getenv("OSS_ACCESS_KEY_ID");
            const char* env_sk = std::getenv("OSS_ACCESS_KEY_SECRET");
            if (!env_ak || !env_sk) {
                LOG(ERROR) << "ENV credential type requires OSS_ACCESS_KEY_ID and "
                              "OSS_ACCESS_KEY_SECRET environment variables";
                return nullptr;
            }
            AlibabaCloud::OSS::Credentials creds(env_ak, env_sk);
            client = std::make_shared<AlibabaCloud::OSS::OssClient>(oss_conf.endpoint, creds,
                                                                    oss_client_config);
            LOG(INFO) << "OSS client created with ENV credentials, endpoint=" << oss_conf.endpoint;
        } else if (oss_conf.cred_provider_type == OSSCredProviderType::ANONYMOUS) {
            AlibabaCloud::OSS::Credentials creds("", "");
            client = std::make_shared<AlibabaCloud::OSS::OssClient>(oss_conf.endpoint, creds,
                                                                    oss_client_config);
            LOG(INFO) << "OSS client created with ANONYMOUS access, endpoint=" << oss_conf.endpoint;
        } else {
            AlibabaCloud::OSS::Credentials creds(oss_conf.ak, oss_conf.sk, oss_conf.token);
            client = std::make_shared<AlibabaCloud::OSS::OssClient>(oss_conf.endpoint, creds,
                                                                    oss_client_config);
            LOG(INFO) << "OSS client created with SIMPLE credentials, endpoint="
                      << oss_conf.endpoint;
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to create OSS client: " << e.what();
        return nullptr;
    }

    {
        std::lock_guard<std::mutex> lock(_lock);
        _cache[hash] = client;
    }

    return client;
}

} // namespace doris
