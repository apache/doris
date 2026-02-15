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

#include "recycler/oss_accessor.h"

#include <alibabacloud/oss/OssClient.h>
#include <alibabacloud/oss/client/ClientConfiguration.h>
#include <bvar/reducer.h>
#include <gen_cpp/cloud.pb.h>
#include <time.h>

#include <iomanip>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "common/stopwatch.h"
#include "common/string_util.h"
#include "common/util.h"
#include "cpp/oss_credential_provider.h"
#include "recycler/util.h"

namespace doris::cloud {

namespace oss_bvar {
bvar::LatencyRecorder oss_get_latency("oss_get");
bvar::LatencyRecorder oss_put_latency("oss_put");
bvar::LatencyRecorder oss_delete_object_latency("oss_delete_object");
bvar::LatencyRecorder oss_delete_objects_latency("oss_delete_objects");
bvar::LatencyRecorder oss_head_latency("oss_head");
bvar::LatencyRecorder oss_list_latency("oss_list");
} // namespace oss_bvar

// Parse OSS LastModified (ISO 8601 UTC) to Unix timestamp
static int64_t parse_oss_last_modified(const std::string& last_modified_str) {
    std::tm tm = {};
    std::istringstream ss(last_modified_str);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");

    if (ss.fail()) {
        LOG(WARNING) << "Failed to parse OSS LastModified: " << last_modified_str;
        return 0;
    }

    return static_cast<int64_t>(timegm(&tm));  // timegm() for UTC time
}

// OSS List Iterator implementation
class OSSListIterator final : public ListIterator {
public:
    OSSListIterator(std::shared_ptr<AlibabaCloud::OSS::OssClient> client, const std::string& bucket,
                    const std::string& prefix, size_t prefix_length)
            : client_(std::move(client)),
              bucket_(bucket),
              prefix_(prefix),
              prefix_length_(prefix_length),
              is_truncated_(true),
              current_index_(0) {
        // Fetch first batch
        fetch_next_batch();
    }

    ~OSSListIterator() override = default;

    bool is_valid() override { return valid_; }

    bool has_next() override {
        if (current_index_ < objects_.size()) {
            return true;
        }

        // Need to fetch next batch
        if (is_truncated_) {
            fetch_next_batch();
            return current_index_ < objects_.size();
        }

        return false;
    }

    std::optional<FileMeta> next() override {
        if (!has_next()) {
            return std::nullopt;
        }

        const auto& obj = objects_[current_index_++];
        return FileMeta{.path = get_relative_path(obj.Key()),
                        .size = obj.Size(),
                        .mtime_s = parse_oss_last_modified(obj.LastModified())};
    }

private:
    void fetch_next_batch() {
        AlibabaCloud::OSS::ListObjectsRequest request(bucket_);
        request.setPrefix(prefix_);
        request.setMaxKeys(1000); // OSS default max keys per request

        if (!next_marker_.empty()) {
            request.setMarker(next_marker_);
        }

        auto outcome = client_->ListObjects(request);
        if (!outcome.isSuccess()) {
            LOG(WARNING) << "OSS ListObjects failed: " << outcome.error().Code() << " - "
                         << outcome.error().Message();
            valid_ = false;
            is_truncated_ = false;
            return;
        }

        const auto& result = outcome.result();
        objects_ = result.ObjectSummarys();
        is_truncated_ = result.IsTruncated();
        next_marker_ = result.NextMarker();
        current_index_ = 0;
        valid_ = true;
    }

    std::string get_relative_path(const std::string& key) const {
        if (key.length() >= prefix_length_) {
            return key.substr(prefix_length_);
        }
        return key;
    }

    std::shared_ptr<AlibabaCloud::OSS::OssClient> client_;
    std::string bucket_;
    std::string prefix_;
    size_t prefix_length_;
    bool valid_ {false};
    bool is_truncated_;
    std::string next_marker_;
    AlibabaCloud::OSS::ObjectSummaryList objects_;
    size_t current_index_;
};

// OSSConf implementation

std::optional<OSSConf> OSSConf::from_obj_store_info(const ObjectStoreInfoPB& obj_info,
                                                     bool skip_aksk) {
    // Only process OSS provider
    if (obj_info.provider() != ObjectStoreInfoPB_Provider_OSS) {
        return std::nullopt;
    }

    OSSConf conf;
    conf.endpoint = obj_info.endpoint();
    conf.bucket = obj_info.bucket();
    conf.prefix = obj_info.prefix();
    conf.region = obj_info.region();

    if (!skip_aksk) {
        // Check credential provider type
        if (obj_info.has_cred_provider_type()) {
            switch (obj_info.cred_provider_type()) {
            case CredProviderTypePB::INSTANCE_PROFILE:
                conf.provider_type = OSSConf::CredProviderType::INSTANCE_PROFILE;
                LOG(INFO) << "Using OSS INSTANCE_PROFILE credential provider";
                break;
            case CredProviderTypePB::SIMPLE:
                conf.provider_type = OSSConf::CredProviderType::SIMPLE;
                conf.access_key_id = obj_info.ak();
                conf.access_key_secret = obj_info.sk();
                // Note: security_token is not read from ObjectStoreInfoPB
                // For temporary credentials, use INSTANCE_PROFILE mode
                LOG(INFO) << "Using OSS SIMPLE credential provider";
                break;
            default:
                conf.provider_type = OSSConf::CredProviderType::INSTANCE_PROFILE;
                LOG(INFO) << "Unknown credential provider type, defaulting to INSTANCE_PROFILE";
                break;
            }
        } else {
            // No credential provider type specified, check if AK/SK provided
            if (!obj_info.ak().empty() && !obj_info.sk().empty()) {
                conf.provider_type = OSSConf::CredProviderType::SIMPLE;
                conf.access_key_id = obj_info.ak();
                conf.access_key_secret = obj_info.sk();
                // Note: security_token is not read from ObjectStoreInfoPB
                LOG(INFO) << "Using OSS SIMPLE credential provider (from AK/SK)";
            } else {
                conf.provider_type = OSSConf::CredProviderType::INSTANCE_PROFILE;
                LOG(INFO) << "No AK/SK provided, using OSS INSTANCE_PROFILE credential provider";
            }
        }
    }

    return conf;
}

uint64_t OSSConf::get_hash() const {
    // Include provider_type to ensure different credential types produce different hashes
    std::string hash_str = endpoint + bucket + prefix + region +
                           std::to_string(static_cast<int>(provider_type));
    return std::hash<std::string> {}(hash_str);
}

// OSSAccessor implementation

OSSAccessor::OSSAccessor(OSSConf conf)
        : StorageVaultAccessor(AccessorType::OSS), conf_(std::move(conf)) {
    uri_ = fmt::format("oss://{}/{}", conf_.bucket,
                       conf_.prefix.empty() ? "" : conf_.prefix + "/");
}

OSSAccessor::~OSSAccessor() = default;

int OSSAccessor::create(OSSConf conf, std::shared_ptr<OSSAccessor>* accessor) {
    *accessor = std::make_shared<OSSAccessor>(std::move(conf));
    return (*accessor)->init();
}

int OSSAccessor::init() {
    // Initialize OSS SDK if not already done
    // Note: This should be called once per process, ideally in main()
    // But calling it multiple times is safe (it's a no-op after first call)
    static std::once_flag init_flag;
    std::call_once(init_flag, []() {
        AlibabaCloud::OSS::InitializeSdk();
        LOG(INFO) << "Alibaba Cloud OSS SDK initialized";
    });

    return create_oss_client();
}

int OSSAccessor::create_oss_client() {
    std::lock_guard<std::mutex> lock(client_mutex_);

    // Create client configuration
    AlibabaCloud::OSS::ClientConfiguration oss_config;
    oss_config.maxConnections = conf_.max_connections;
    oss_config.connectTimeoutMs = conf_.connect_timeout_ms;
    oss_config.requestTimeoutMs = conf_.request_timeout_ms;

    try {
        if (conf_.provider_type == OSSConf::CredProviderType::INSTANCE_PROFILE) {
            // Use ECS instance profile credentials
            if (!credentials_provider_) {
                credentials_provider_ = std::make_shared<ECSMetadataCredentialsProvider>();
            }

            // Get credentials from provider (will fetch from ECS metadata or return cached)
            auto creds = credentials_provider_->getCredentials();

            // Create OSS client with credentials
            oss_client_ = std::make_shared<AlibabaCloud::OSS::OssClient>(conf_.endpoint, creds,
                                                                          oss_config);

            LOG(INFO) << "Created OSS client with INSTANCE_PROFILE credentials for endpoint: "
                      << conf_.endpoint;
        } else if (conf_.provider_type == OSSConf::CredProviderType::SIMPLE) {
            // Use static credentials
            AlibabaCloud::OSS::Credentials creds(conf_.access_key_id, conf_.access_key_secret,
                                                  conf_.security_token);

            oss_client_ = std::make_shared<AlibabaCloud::OSS::OssClient>(conf_.endpoint, creds,
                                                                          oss_config);

            LOG(INFO) << "Created OSS client with SIMPLE credentials for endpoint: "
                      << conf_.endpoint;
        } else {
            LOG(ERROR) << "Unsupported OSS credential provider type";
            return -1;
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to create OSS client: " << e.what();
        return -1;
    }

    return 0;
}

int OSSAccessor::refresh_client_if_needed() {
    // For instance profile, recreate client to refresh credentials (OSS SDK v1.10.1 doesn't support dynamic providers)
    if (conf_.provider_type == OSSConf::CredProviderType::INSTANCE_PROFILE) {
        return create_oss_client();
    }
    return 0;
}

std::string OSSAccessor::get_key(const std::string& relative_path) const {
    // Defensive: trim leading '/' from relative_path to prevent invalid keys like "prefix//file.txt"
    std::string normalized_path = relative_path;

    if (!normalized_path.empty() && normalized_path[0] == '/') {
        LOG(WARNING) << "OSS relative path should not start with '/': " << relative_path
                     << ". Auto-trimming leading slash.";
        normalized_path = normalized_path.substr(1);
    }

    if (conf_.prefix.empty()) {
        return normalized_path;
    }

    return conf_.prefix + "/" + normalized_path;
}

std::string OSSAccessor::to_uri(const std::string& relative_path) const {
    return uri_ + relative_path;
}

int OSSAccessor::convert_oss_error_code(const std::string& error_code) const {
    if (error_code == "NoSuchKey" || error_code == "NoSuchBucket") {
        return 1; // Not found
    } else if (error_code == "AccessDenied" || error_code == "InvalidAccessKeyId") {
        return -2; // Access denied
    } else if (error_code == "SecurityTokenExpired") {
        LOG(WARNING) << "OSS security token expired, will refresh on next operation";
        return -3; // Credentials expired
    } else if (error_code == "RequestTimeout" || error_code == "ConnectionTimeout") {
        return -4; // Timeout
    }

    LOG(WARNING) << "OSS operation failed with error code: " << error_code;
    return -1; // Generic error
}

int OSSAccessor::put_file(const std::string& path, const std::string& content) {
    SCOPED_BVAR_LATENCY(oss_bvar::oss_put_latency);

    int ret = refresh_client_if_needed();
    if (ret != 0) {
        return ret;
    }

    std::string key = get_key(path);

    // Create stream from content
    std::shared_ptr<std::stringstream> content_stream =
            std::make_shared<std::stringstream>(content);

    AlibabaCloud::OSS::PutObjectRequest request(conf_.bucket, key, content_stream);

    auto outcome = oss_client_->PutObject(request);
    if (!outcome.isSuccess()) {
        LOG(WARNING) << "OSS PutObject failed: " << outcome.error().Code() << " - "
                     << outcome.error().Message() << ", key: " << key;
        return convert_oss_error_code(outcome.error().Code());
    }

    VLOG(1) << "OSS PutObject success: " << key << ", size: " << content.size();
    return 0;
}

int OSSAccessor::delete_file(const std::string& path) {
    SCOPED_BVAR_LATENCY(oss_bvar::oss_delete_object_latency);

    int ret = refresh_client_if_needed();
    if (ret != 0) {
        return ret;
    }

    std::string key = get_key(path);

    auto outcome = oss_client_->DeleteObject(conf_.bucket, key);
    if (!outcome.isSuccess()) {
        // OSS DeleteObject returns success even if object doesn't exist
        // Only log real errors
        std::string error_code = outcome.error().Code();
        if (error_code != "NoSuchKey") {
            LOG(WARNING) << "OSS DeleteObject failed: " << error_code << " - "
                         << outcome.error().Message() << ", key: " << key;
            return convert_oss_error_code(error_code);
        }
    }

    VLOG(1) << "OSS DeleteObject success: " << key;
    return 0;
}

int OSSAccessor::delete_files(const std::vector<std::string>& paths) {
    SCOPED_BVAR_LATENCY(oss_bvar::oss_delete_objects_latency);

    if (paths.empty()) {
        return 0;
    }

    int ret = refresh_client_if_needed();
    if (ret != 0) {
        return ret;
    }

    // OSS DeleteObjects supports batch delete (max 1000 keys per request)
    const size_t batch_size = 1000;

    for (size_t i = 0; i < paths.size(); i += batch_size) {
        size_t end = std::min(i + batch_size, paths.size());

        AlibabaCloud::OSS::DeletedKeyList keys;
        for (size_t j = i; j < end; ++j) {
            keys.push_back(get_key(paths[j]));
        }

        AlibabaCloud::OSS::DeleteObjectsRequest request(conf_.bucket);
        request.setKeyList(keys);

        auto outcome = oss_client_->DeleteObjects(request);
        if (!outcome.isSuccess()) {
            LOG(WARNING) << "OSS DeleteObjects failed: " << outcome.error().Code() << " - "
                         << outcome.error().Message();
            return convert_oss_error_code(outcome.error().Code());
        }

        VLOG(1) << "OSS DeleteObjects success: deleted " << outcome.result().keyList().size()
                << " objects (" << (end - i) << " requested)";
    }

    return 0;
}

int OSSAccessor::delete_prefix(const std::string& path_prefix, int64_t expiration_time) {
    int ret = refresh_client_if_needed();
    if (ret != 0) {
        return ret;
    }

    std::string prefix = get_key(path_prefix);

    // List all objects with prefix and delete them in batches
    std::vector<std::string> keys_to_delete;
    const size_t batch_size = 1000;

    bool is_truncated = true;
    std::string marker;

    while (is_truncated) {
        AlibabaCloud::OSS::ListObjectsRequest list_request(conf_.bucket);
        list_request.setPrefix(prefix);
        list_request.setMaxKeys(1000);

        if (!marker.empty()) {
            list_request.setMarker(marker);
        }

        auto outcome = oss_client_->ListObjects(list_request);
        if (!outcome.isSuccess()) {
            LOG(WARNING) << "OSS ListObjects failed: " << outcome.error().Code() << " - "
                         << outcome.error().Message();
            return convert_oss_error_code(outcome.error().Code());
        }

        const auto& result = outcome.result();
        const auto& objects = result.ObjectSummarys();

        for (const auto& obj : objects) {
            // Check expiration time if specified
            if (expiration_time > 0) {
                int64_t obj_mtime = parse_oss_last_modified(obj.LastModified());
                if (obj_mtime >= expiration_time) {
                    continue; // Skip objects newer than expiration time
                }
            }

            keys_to_delete.push_back(obj.Key());

            // Delete in batches
            if (keys_to_delete.size() >= batch_size) {
                AlibabaCloud::OSS::DeletedKeyList batch_keys(keys_to_delete.begin(),
                                                              keys_to_delete.end());
                AlibabaCloud::OSS::DeleteObjectsRequest delete_request(conf_.bucket);
                delete_request.setKeyList(batch_keys);

                auto delete_outcome = oss_client_->DeleteObjects(delete_request);
                if (!delete_outcome.isSuccess()) {
                    LOG(WARNING) << "OSS DeleteObjects failed: "
                                 << delete_outcome.error().Code() << " - "
                                 << delete_outcome.error().Message();
                    return convert_oss_error_code(delete_outcome.error().Code());
                }

                VLOG(1) << "OSS deleted batch of " << delete_outcome.result().keyList().size()
                        << " objects";
                keys_to_delete.clear();
            }
        }

        is_truncated = result.IsTruncated();
        marker = result.NextMarker();
    }

    // Delete remaining keys
    if (!keys_to_delete.empty()) {
        AlibabaCloud::OSS::DeletedKeyList batch_keys(keys_to_delete.begin(),
                                                      keys_to_delete.end());
        AlibabaCloud::OSS::DeleteObjectsRequest delete_request(conf_.bucket);
        delete_request.setKeyList(batch_keys);

        auto delete_outcome = oss_client_->DeleteObjects(delete_request);
        if (!delete_outcome.isSuccess()) {
            LOG(WARNING) << "OSS DeleteObjects failed: " << delete_outcome.error().Code()
                         << " - " << delete_outcome.error().Message();
            return convert_oss_error_code(delete_outcome.error().Code());
        }

        VLOG(1) << "OSS deleted final batch of " << delete_outcome.result().keyList().size()
                << " objects";
    }

    return 0;
}

int OSSAccessor::delete_directory(const std::string& dir_path) {
    // For OSS, directory is just a prefix, same as delete_prefix
    return delete_prefix(dir_path, 0);
}

int OSSAccessor::delete_all(int64_t expiration_time) {
    // Delete all objects under the prefix
    return delete_prefix("", expiration_time);
}

int OSSAccessor::list_directory(const std::string& dir_path,
                                 std::unique_ptr<ListIterator>* res) {
    return list_prefix(dir_path, res);
}

int OSSAccessor::list_all(std::unique_ptr<ListIterator>* res) {
    return list_prefix("", res);
}

int OSSAccessor::list_prefix(const std::string& path_prefix, std::unique_ptr<ListIterator>* res) {
    SCOPED_BVAR_LATENCY(oss_bvar::oss_list_latency);

    int ret = refresh_client_if_needed();
    if (ret != 0) {
        return ret;
    }

    std::string prefix = get_key(path_prefix);

    *res = std::make_unique<OSSListIterator>(oss_client_, conf_.bucket, prefix,
                                              conf_.prefix.empty() ? 0
                                                                   : conf_.prefix.length() + 1);
    return 0;
}

int OSSAccessor::exists(const std::string& path) {
    SCOPED_BVAR_LATENCY(oss_bvar::oss_head_latency);

    int ret = refresh_client_if_needed();
    if (ret != 0) {
        return ret;
    }

    std::string key = get_key(path);

    // Use DoesObjectExist for efficient check
    bool exists = oss_client_->DoesObjectExist(conf_.bucket, key);

    VLOG(2) << "OSS exists check: " << key << " -> " << (exists ? "found" : "not found");

    return exists ? 0 : 1; // 0 = exists, 1 = not found
}

} // namespace doris::cloud
