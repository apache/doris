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

#include "recycler/s3_accessor.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/S3Client.h>
#include <gen_cpp/cloud.pb.h>

#include <algorithm>
#include <azure/storage/blobs/blob_container_client.hpp>
#include <azure/storage/common/storage_credential.hpp>
#include <execution>
#include <memory>
#include <utility>

#include "common/config.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "common/string_util.h"
#include "common/util.h"
#include "rate-limiter/s3_rate_limiter.h"
#include "recycler/azure_obj_client.h"
#include "recycler/obj_storage_client.h"
#include "recycler/s3_obj_client.h"
#include "recycler/storage_vault_accessor.h"

namespace doris::cloud {

struct AccessorRateLimiter {
public:
    ~AccessorRateLimiter() = default;
    static AccessorRateLimiter& instance();
    S3RateLimiterHolder* rate_limiter(S3RateLimitType type);

private:
    AccessorRateLimiter();
    std::array<std::unique_ptr<S3RateLimiterHolder>, 2> _rate_limiters;
};

template <typename Func>
auto s3_rate_limit(S3RateLimitType op, Func callback) -> decltype(callback()) {
    using T = decltype(callback());
    if (!config::enable_s3_rate_limiter) {
        return callback();
    }
    auto sleep_duration = AccessorRateLimiter::instance().rate_limiter(op)->add(1);
    if (sleep_duration < 0) {
        return T(-1);
    }
    return callback();
}

template <typename Func>
auto s3_get_rate_limit(Func callback) -> decltype(callback()) {
    return s3_rate_limit(S3RateLimitType::GET, std::move(callback));
}

template <typename Func>
auto s3_put_rate_limit(Func callback) -> decltype(callback()) {
    return s3_rate_limit(S3RateLimitType::PUT, std::move(callback));
}

AccessorRateLimiter::AccessorRateLimiter()
        : _rate_limiters {std::make_unique<S3RateLimiterHolder>(
                                  S3RateLimitType::GET, config::s3_get_token_per_second,
                                  config::s3_get_bucket_tokens, config::s3_get_token_limit),
                          std::make_unique<S3RateLimiterHolder>(
                                  S3RateLimitType::PUT, config::s3_put_token_per_second,
                                  config::s3_put_bucket_tokens, config::s3_put_token_limit)} {}

S3RateLimiterHolder* AccessorRateLimiter::rate_limiter(S3RateLimitType type) {
    CHECK(type == S3RateLimitType::GET || type == S3RateLimitType::PUT) << to_string(type);
    return _rate_limiters[static_cast<size_t>(type)].get();
}

AccessorRateLimiter& AccessorRateLimiter::instance() {
    static AccessorRateLimiter instance;
    return instance;
}

int reset_s3_rate_limiter(S3RateLimitType type, size_t max_speed, size_t max_burst, size_t limit) {
    if (type == S3RateLimitType::UNKNOWN) {
        return -1;
    }
    if (type == S3RateLimitType::GET) {
        max_speed = (max_speed == 0) ? config::s3_get_token_per_second : max_speed;
        max_burst = (max_burst == 0) ? config::s3_get_bucket_tokens : max_burst;
        limit = (limit == 0) ? config::s3_get_token_limit : limit;
    } else {
        max_speed = (max_speed == 0) ? config::s3_put_token_per_second : max_speed;
        max_burst = (max_burst == 0) ? config::s3_put_bucket_tokens : max_burst;
        limit = (limit == 0) ? config::s3_put_token_limit : limit;
    }
    return AccessorRateLimiter::instance().rate_limiter(type)->reset(max_speed, max_burst, limit);
}

class S3Environment {
public:
    S3Environment() { Aws::InitAPI(aws_options_); }

    ~S3Environment() { Aws::ShutdownAPI(aws_options_); }

private:
    Aws::SDKOptions aws_options_;
};

class S3ListIterator final : public ListIterator {
public:
    S3ListIterator(std::unique_ptr<ObjectListIterator> iter, size_t prefix_length)
            : iter_(std::move(iter)), prefix_length_(prefix_length) {}

    ~S3ListIterator() override = default;

    bool is_valid() override { return iter_->is_valid(); }

    bool has_next() override { return iter_->has_next(); }

    std::optional<FileMeta> next() override {
        std::optional<FileMeta> ret;
        if (auto obj = iter_->next(); obj.has_value()) {
            ret = FileMeta {
                    .path = get_relative_path(obj->key),
                    .size = obj->size,
                    .mtime_s = obj->mtime_s,
            };
        }
        return ret;
    }

private:
    std::string get_relative_path(const std::string& key) const {
        return key.substr(prefix_length_);
    }

    std::unique_ptr<ObjectListIterator> iter_;
    size_t prefix_length_;
};

std::optional<S3Conf> S3Conf::from_obj_store_info(const ObjectStoreInfoPB& obj_info) {
    S3Conf s3_conf;

    switch (obj_info.provider()) {
    case ObjectStoreInfoPB_Provider_OSS:
    case ObjectStoreInfoPB_Provider_S3:
    case ObjectStoreInfoPB_Provider_COS:
    case ObjectStoreInfoPB_Provider_OBS:
    case ObjectStoreInfoPB_Provider_BOS:
        s3_conf.provider = S3Conf::S3;
        break;
    case ObjectStoreInfoPB_Provider_GCP:
        s3_conf.provider = S3Conf::GCS;
        break;
    case ObjectStoreInfoPB_Provider_AZURE:
        s3_conf.provider = S3Conf::AZURE;
        break;
    default:
        LOG_WARNING("unknown provider type {}").tag("obj_info", proto_to_json(obj_info));
        return std::nullopt;
    }

    if (obj_info.has_encryption_info()) {
        AkSkPair plain_ak_sk_pair;
        int ret = decrypt_ak_sk_helper(obj_info.ak(), obj_info.sk(), obj_info.encryption_info(),
                                       &plain_ak_sk_pair);
        if (ret != 0) {
            LOG_WARNING("fail to decrypt ak sk").tag("obj_info", proto_to_json(obj_info));
            return std::nullopt;
        } else {
            s3_conf.ak = std::move(plain_ak_sk_pair.first);
            s3_conf.sk = std::move(plain_ak_sk_pair.second);
        }
    } else {
        s3_conf.ak = obj_info.ak();
        s3_conf.sk = obj_info.sk();
    }

    s3_conf.endpoint = obj_info.endpoint();
    s3_conf.region = obj_info.region();
    s3_conf.bucket = obj_info.bucket();
    s3_conf.prefix = obj_info.prefix();

    return s3_conf;
}

S3Accessor::S3Accessor(S3Conf conf)
        : StorageVaultAccessor(AccessorType::S3), conf_(std::move(conf)) {}

S3Accessor::~S3Accessor() = default;

std::string S3Accessor::get_key(const std::string& relative_path) const {
    return conf_.prefix + '/' + relative_path;
}

std::string S3Accessor::to_uri(const std::string& relative_path) const {
    return uri_ + '/' + relative_path;
}

int S3Accessor::create(S3Conf conf, std::shared_ptr<S3Accessor>* accessor) {
    switch (conf.provider) {
    case S3Conf::GCS:
        *accessor = std::make_shared<GcsAccessor>(std::move(conf));
        break;
    default:
        *accessor = std::make_shared<S3Accessor>(std::move(conf));
        break;
    }

    return (*accessor)->init();
}

int S3Accessor::init() {
    switch (conf_.provider) {
    case S3Conf::AZURE: {
        auto cred =
                std::make_shared<Azure::Storage::StorageSharedKeyCredential>(conf_.ak, conf_.sk);
        uri_ = fmt::format("https://{}.blob.core.windows.net/{}", conf_.ak, conf_.bucket);
        auto container_client =
                std::make_shared<Azure::Storage::Blobs::BlobContainerClient>(uri_, cred);
        obj_client_ = std::make_shared<AzureObjClient>(std::move(container_client));
        return 0;
    }
    default: {
        uri_ = conf_.endpoint + '/' + conf_.bucket + '/' + conf_.prefix;

        static S3Environment s3_env;

        // S3Conf::S3
        Aws::Auth::AWSCredentials aws_cred(conf_.ak, conf_.sk);
        Aws::Client::ClientConfiguration aws_config;
        aws_config.endpointOverride = conf_.endpoint;
        aws_config.region = conf_.region;
        aws_config.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(
                /*maxRetries = 10, scaleFactor = 25*/);
        auto s3_client = std::make_shared<Aws::S3::S3Client>(
                std::move(aws_cred), std::move(aws_config),
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                true /* useVirtualAddressing */);
        obj_client_ = std::make_shared<S3ObjClient>(std::move(s3_client), conf_.endpoint);
        return 0;
    }
    }
}

int S3Accessor::delete_prefix_impl(const std::string& path_prefix, int64_t expiration_time) {
    LOG_INFO("delete prefix").tag("uri", to_uri(path_prefix));
    return obj_client_
            ->delete_objects_recursively({.bucket = conf_.bucket, .key = get_key(path_prefix)},
                                         expiration_time)
            .ret;
}

int S3Accessor::delete_prefix(const std::string& path_prefix, int64_t expiration_time) {
    auto norm_path_prefix = path_prefix;
    strip_leading(norm_path_prefix, "/");
    if (norm_path_prefix.empty()) {
        LOG_WARNING("invalid path_prefix {}", path_prefix);
        return -1;
    }

    return delete_prefix_impl(norm_path_prefix, expiration_time);
}

int S3Accessor::delete_directory(const std::string& dir_path) {
    auto norm_dir_path = dir_path;
    strip_leading(norm_dir_path, "/");
    if (norm_dir_path.empty()) {
        LOG_WARNING("invalid dir_path {}", dir_path);
        return -1;
    }

    return delete_prefix_impl(!norm_dir_path.ends_with('/') ? norm_dir_path + '/' : norm_dir_path);
}

int S3Accessor::delete_all(int64_t expiration_time) {
    return delete_prefix_impl("", expiration_time);
}

int S3Accessor::delete_files(const std::vector<std::string>& paths) {
    if (paths.empty()) {
        return 0;
    }

    std::vector<std::string> keys;
    keys.reserve(paths.size());
    for (auto&& path : paths) {
        LOG_INFO("delete file").tag("uri", to_uri(path));
        keys.emplace_back(get_key(path));
    }

    return obj_client_->delete_objects(conf_.bucket, std::move(keys)).ret;
}

int S3Accessor::delete_file(const std::string& path) {
    LOG_INFO("delete file").tag("uri", to_uri(path));
    return obj_client_->delete_object({.bucket = conf_.bucket, .key = get_key(path)}).ret;
}

int S3Accessor::put_file(const std::string& path, const std::string& content) {
    return obj_client_->put_object({.bucket = conf_.bucket, .key = get_key(path)}, content).ret;
}

int S3Accessor::list_prefix(const std::string& path_prefix, std::unique_ptr<ListIterator>* res) {
    *res = std::make_unique<S3ListIterator>(
            obj_client_->list_objects({conf_.bucket, get_key(path_prefix)}),
            conf_.prefix.length() + 1 /* {prefix}/ */);
    return 0;
}

int S3Accessor::list_directory(const std::string& dir_path, std::unique_ptr<ListIterator>* res) {
    auto norm_dir_path = dir_path;
    strip_leading(norm_dir_path, "/");
    if (norm_dir_path.empty()) {
        LOG_WARNING("invalid dir_path {}", dir_path);
        return -1;
    }

    return list_prefix(!norm_dir_path.ends_with('/') ? norm_dir_path + '/' : norm_dir_path, res);
}

int S3Accessor::list_all(std::unique_ptr<ListIterator>* res) {
    return list_prefix("", res);
}

int S3Accessor::exists(const std::string& path) {
    ObjectMeta obj_meta;
    return obj_client_->head_object({.bucket = conf_.bucket, .key = get_key(path)}, &obj_meta).ret;
}

int S3Accessor::get_life_cycle(int64_t* expiration_days) {
    return obj_client_->get_life_cycle(conf_.bucket, expiration_days).ret;
}

int S3Accessor::check_versioning() {
    return obj_client_->check_versioning(conf_.bucket).ret;
}

int GcsAccessor::delete_prefix_impl(const std::string& path_prefix, int64_t expiration_time) {
    LOG_INFO("delete prefix").tag("uri", to_uri(path_prefix));

    int ret = 0;
    auto iter = obj_client_->list_objects({conf_.bucket, get_key(path_prefix)});
    for (auto obj = iter->next(); obj.has_value(); obj = iter->next()) {
        if (expiration_time > 0 && obj->mtime_s > expiration_time) {
            continue;
        }

        // FIXME(plat1ko): Delete objects by batch
        if (int del_ret = obj_client_->delete_object({conf_.bucket, obj->key}).ret; del_ret != 0) {
            ret = del_ret;
        }
    }

    if (!iter->is_valid()) {
        return -1;
    }

    return ret;
}

int GcsAccessor::delete_files(const std::vector<std::string>& paths) {
    std::vector<int> delete_rets(paths.size());
    std::transform(std::execution::par, paths.begin(), paths.end(), delete_rets.begin(),
                   [this](const std::string& path) {
                       LOG_INFO("delete file").tag("uri", to_uri(path));
                       return delete_file(path);
                   });

    int ret = 0;
    for (int delete_ret : delete_rets) {
        if (delete_ret != 0) {
            ret = delete_ret;
            break;
        }
    }
    return ret;
}

} // namespace doris::cloud
