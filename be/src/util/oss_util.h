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

#include <bvar/bvar.h>
#include <fmt/format.h>
#include <gen_cpp/cloud.pb.h>

#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "common/status.h"
#include "cpp/oss_common.h"
#include "util/s3_util.h"
#include "vec/common/string_ref.h"

// Forward declare OSS SDK types
namespace AlibabaCloud {
namespace OSS {
class OssClient;
} // namespace OSS
} // namespace AlibabaCloud

namespace doris {

// Forward declaration (defined in s3_util.cpp)
std::string hide_access_key(const std::string& ak);

class ECSMetadataCredentialsProvider;
class OSSSTSCredentialProvider;
class OSSDefaultCredentialsProvider;

namespace oss_bvar {
extern bvar::LatencyRecorder oss_get_latency;
extern bvar::LatencyRecorder oss_put_latency;
extern bvar::LatencyRecorder oss_delete_object_latency;
extern bvar::LatencyRecorder oss_delete_objects_latency;
extern bvar::LatencyRecorder oss_head_latency;
extern bvar::LatencyRecorder oss_list_latency;
extern bvar::LatencyRecorder oss_multi_part_upload_latency;
} // namespace oss_bvar

// OSS Client Configuration
struct OSSClientConf {
    std::string endpoint;
    std::string region;
    std::string ak;
    std::string sk;
    std::string token;
    std::string bucket;
    std::string role_arn;      // For STS AssumeRole
    std::string external_id;   // For STS AssumeRole (cross-account security)

    int max_connections = 100;
    int request_timeout_ms = 30000;
    int connect_timeout_ms = 10000;

    OSSCredProviderType cred_provider_type = OSSCredProviderType::DEFAULT;

    uint64_t get_hash() const {
        uint64_t hash_code = 0;
        hash_code ^= crc32_hash(ak);
        hash_code ^= crc32_hash(sk);
        hash_code ^= crc32_hash(token);
        hash_code ^= crc32_hash(endpoint);
        hash_code ^= crc32_hash(region);
        hash_code ^= crc32_hash(bucket);
        hash_code ^= crc32_hash(role_arn);
        hash_code ^= crc32_hash(external_id);
        hash_code ^= max_connections;
        hash_code ^= request_timeout_ms;
        hash_code ^= connect_timeout_ms;
        hash_code ^= static_cast<int>(cred_provider_type);
        return hash_code;
    }

    std::string to_string() const {
        return fmt::format(
                "(ak={}, token={}, endpoint={}, region={}, bucket={}, role_arn={}, external_id={}, "
                "max_connections={}, request_timeout_ms={}, connect_timeout_ms={}, cred_provider_type={})",
                hide_access_key(ak), hide_access_key(token), endpoint, region, bucket,
                role_arn, hide_access_key(external_id), max_connections, request_timeout_ms,
                connect_timeout_ms, static_cast<int>(cred_provider_type));
    }
};

// OSS Configuration
struct OSSConf {
    std::string bucket;
    std::string prefix;
    OSSClientConf client_conf;

    static OSSConf get_oss_conf(const cloud::ObjectStoreInfoPB& obj_info);

    std::string to_string() const {
        return fmt::format("(bucket={}, prefix={}, client_conf={})", bucket, prefix,
                           client_conf.to_string());
    }
};

// Factory for creating OSS clients
class OSSClientFactory {
public:
    ~OSSClientFactory();

    static OSSClientFactory& instance();

    // Create or get cached OSS client
    std::shared_ptr<AlibabaCloud::OSS::OssClient> create(const OSSClientConf& oss_conf);

    // Convert properties map to OSSConf
    static Status convert_properties_to_oss_conf(const std::map<std::string, std::string>& prop,
                                                  OSSConf* oss_conf);

private:
    OSSClientFactory();

    std::mutex _lock;
    std::unordered_map<uint64_t, std::shared_ptr<AlibabaCloud::OSS::OssClient>> _cache;
    std::unordered_map<uint64_t, std::shared_ptr<ECSMetadataCredentialsProvider>>
            _ecs_credential_providers;
    std::unordered_map<uint64_t, std::shared_ptr<OSSSTSCredentialProvider>>
            _sts_credential_providers;
    std::unordered_map<uint64_t, std::shared_ptr<OSSDefaultCredentialsProvider>>
            _default_credential_providers;
    std::string _ca_cert_file_path;
};

} // namespace doris
