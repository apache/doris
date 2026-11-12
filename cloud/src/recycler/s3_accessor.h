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

#include <array>
#include <cstdint>
#include <memory>
#include <optional>

#include "cpp/obj-storage/ca_bundle_cache.h"
#include "cpp/obj-storage/obj_storage_config.h"
#include "recycler/storage_vault_accessor.h"

namespace doris {
class TokenBucketRateLimiterHolder;
using S3RateLimiterHolder = TokenBucketRateLimiterHolder;

enum class ObjStorageRateLimitType;
namespace cloud {
class ObjectStoreInfoPB;
class SimpleThreadPool;

struct AccessorRateLimiter {
public:
    ~AccessorRateLimiter() = default;
    static AccessorRateLimiter& instance();
    S3RateLimiterHolder* rate_limiter(ObjStorageRateLimitType type);

private:
    AccessorRateLimiter();
    std::array<std::unique_ptr<S3RateLimiterHolder>, 2> _rate_limiters;
};

class S3Accessor : public StorageVaultAccessor {
public:
    explicit S3Accessor(ObjStorageConfig conf);
    ~S3Accessor() override;

    // Decode the persisted vault, including the Recycler's encrypted and stage credentials.
    static std::optional<ObjStorageConfig> parse_config(ObjectStoreInfoPB info,
                                                        bool skip_aksk = false);

    // returns 0 for success otherwise error
    static int create(ObjStorageConfig conf, std::shared_ptr<S3Accessor>* accessor);

    // returns 0 for success otherwise error
    int init();

    int delete_prefix(const std::string& path_prefix, int64_t expiration_time = 0) override;

    int delete_directory(const std::string& dir_path) override;

    int delete_all(int64_t expiration_time = 0) override;

    int delete_files(const std::vector<std::string>& paths) override;

    int delete_file(const std::string& path) override;

    int list_directory(const std::string& dir_path, std::unique_ptr<ListIterator>* res) override;

    int list_all(std::unique_ptr<ListIterator>* res) override;

    int put_file(const std::string& path, const std::string& content) override;

    int exists(const std::string& path) override;

    int abort_multipart_upload(const std::string& path, const std::string& upload_id) override;

    // Get the objects' expiration time on the conf.bucket
    // returns 0 for success otherwise error
    int get_lifecycle(int64_t* expiration_days);

    // Check if the objects' versioning is on or off
    // returns 0 when versioning is on, otherwise versioning is off or check failed
    int check_versioning();

protected:
    static ObjStorageRecursiveDeleteOptions make_recursive_delete_options(
            int64_t expiration_time, std::shared_ptr<SimpleThreadPool> pool);

    int list_prefix(const std::string& path_prefix, std::unique_ptr<ListIterator>* res);

    virtual int delete_prefix_impl(const std::string& path_prefix, int64_t expiration_time = 0);

    std::string get_key(const std::string& relative_path) const;
    std::string to_uri(const std::string& relative_path) const;

    const ObjStorageConfigBase& location() const { return storage_config_base(conf_); }

    ObjStorageConfig conf_;
    std::shared_ptr<ObjStorageClient> obj_client_;
    CaBundleCache _ca_bundle;
};

class GcsAccessor final : public S3Accessor {
public:
    explicit GcsAccessor(ObjStorageConfig conf) : S3Accessor(std::move(conf)) {}
    ~GcsAccessor() override = default;

    int delete_files(const std::vector<std::string>& paths) override;

private:
    int delete_prefix_impl(const std::string& path_prefix, int64_t expiration_time = 0) override;
};

} // namespace cloud
} // namespace doris
