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

#include <bvar/latency_recorder.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "cpp/oss_common.h"
#include "recycler/storage_vault_accessor.h"

// Forward declare OSS SDK types to avoid header dependency in .h
namespace AlibabaCloud {
namespace OSS {
class OssClient;
class Credentials;
} // namespace OSS
} // namespace AlibabaCloud

namespace doris {
// Forward declare credential providers
class ECSMetadataCredentialsProvider;
class OSSSTSCredentialProvider;
class OSSDefaultCredentialsProvider;
} // namespace doris

namespace doris::cloud {
class ObjectStoreInfoPB;

namespace oss_bvar {
extern bvar::LatencyRecorder oss_get_latency;
extern bvar::LatencyRecorder oss_put_latency;
extern bvar::LatencyRecorder oss_delete_object_latency;
extern bvar::LatencyRecorder oss_delete_objects_latency;
extern bvar::LatencyRecorder oss_head_latency;
extern bvar::LatencyRecorder oss_list_latency;
} // namespace oss_bvar

// OSS Configuration
struct OSSConf {
    std::string endpoint;
    std::string bucket;
    std::string prefix;
    std::string region;

    // Credentials (empty for instance profile)
    std::string access_key_id;
    std::string access_key_secret;
    std::string security_token;

    // AssumeRole configuration (for cross-account or elevated permissions)
    std::string role_arn;    // RAM role ARN to assume
    std::string external_id; // External ID for cross-account AssumeRole security

    OSSCredProviderType provider_type = OSSCredProviderType::INSTANCE_PROFILE;

    // Connection settings
    int max_connections = 100;
    int connect_timeout_ms = 10000;
    int request_timeout_ms = 30000;

    // Create OSSConf from ObjectStoreInfoPB
    // skip_aksk: if true, don't extract AK/SK from obj_info (used for logging)
    static std::optional<OSSConf> from_obj_store_info(const ObjectStoreInfoPB& obj_info,
                                                      bool skip_aksk = false);

    // Get hash for configuration (for caching)
    uint64_t get_hash() const;
};

class OSSAccessor : public StorageVaultAccessor {
public:
    explicit OSSAccessor(OSSConf conf);
    ~OSSAccessor() override;

    // Create OSSAccessor instance and initialize
    // returns 0 for success otherwise error
    static int create(OSSConf conf, std::shared_ptr<OSSAccessor>* accessor);

    // Initialize OSS client
    // returns 0 for success otherwise error
    int init();

    // StorageVaultAccessor interface implementations
    // All methods return 0 for success, negative for error

    // Delete objects with path prefix
    // If expiration_time > 0, only delete objects older than expiration_time
    int delete_prefix(const std::string& path_prefix, int64_t expiration_time = 0) override;

    // Delete entire directory recursively
    int delete_directory(const std::string& dir_path) override;

    // Delete all objects in the vault
    int delete_all(int64_t expiration_time = 0) override;

    // Delete multiple files in a batch
    int delete_files(const std::vector<std::string>& paths) override;

    // Delete a single file
    int delete_file(const std::string& path) override;

    // List files in directory recursively
    int list_directory(const std::string& dir_path, std::unique_ptr<ListIterator>* res) override;

    // List all files in the vault
    int list_all(std::unique_ptr<ListIterator>* res) override;

    // Upload a file (for testing)
    int put_file(const std::string& path, const std::string& content) override;

    // Check if file exists
    // returns 0 if exists, 1 if not found, negative for error
    int exists(const std::string& path) override;

    // Abort an in-progress multipart upload
    int abort_multipart_upload(const std::string& path, const std::string& upload_id) override;

protected:
    // List objects with given prefix
    int list_prefix(const std::string& path_prefix, std::unique_ptr<ListIterator>* res);

    // Get full OSS key from relative path
    std::string get_key(const std::string& relative_path) const;

    // Convert relative path to full URI
    std::string to_uri(const std::string& relative_path) const;

    // Refresh OSS client if credentials expired
    int refresh_client_if_needed();

    // Create OSS client based on credential provider type
    int create_oss_client();

    // Convert OSS error to return code
    int convert_oss_error_code(const std::string& error_code) const;

    // Get thread-safe copy of OSS client (for internal use by accessor methods)
    std::shared_ptr<AlibabaCloud::OSS::OssClient> get_client() const;

    OSSConf conf_;
    std::shared_ptr<ECSMetadataCredentialsProvider> credentials_provider_;
    std::shared_ptr<OSSSTSCredentialProvider> sts_credential_provider_;
    std::shared_ptr<doris::OSSDefaultCredentialsProvider> default_credential_provider_;
    std::shared_ptr<AlibabaCloud::OSS::OssClient> oss_client_;
    mutable std::mutex client_mutex_;
    std::string _ca_cert_file_path;
};

} // namespace doris::cloud
