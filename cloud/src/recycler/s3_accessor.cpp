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
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetBucketLifecycleConfigurationRequest.h>
#include <aws/s3/model/GetBucketVersioningRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/LifecycleRule.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <algorithm>
#include <azure/storage/blobs/blob_container_client.hpp>
#include <azure/storage/common/storage_credential.hpp>
#include <execution>
#include <memory>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "rate-limiter/s3_rate_limiter.h"
#include "recycler/azure_obj_client.h"
#include "recycler/obj_store_accessor.h"
#include "recycler/s3_obj_client.h"

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

S3Accessor::S3Accessor(S3Conf conf) : ObjStoreAccessor(AccessorType::S3), conf_(std::move(conf)) {
    path_ = conf_.endpoint + '/' + conf_.bucket + '/' + conf_.prefix;
}

S3Accessor::~S3Accessor() = default;

std::string S3Accessor::get_key(const std::string& relative_path) const {
    return conf_.prefix + '/' + relative_path;
}

std::string S3Accessor::get_relative_path(const std::string& key) const {
    return key.find(conf_.prefix + "/") != 0 ? "" : key.substr(conf_.prefix.length() + 1);
}

int S3Accessor::init() {
    static S3Environment s3_env;
    if (type() == AccessorType::AZURE) {
        auto cred =
                std::make_shared<Azure::Storage::StorageSharedKeyCredential>(conf_.ak, conf_.sk);
        const std::string container_name = conf_.bucket;
        const std::string uri =
                fmt::format("http://{}.blob.core.windows.net/{}", conf_.ak, container_name);
        auto container_client =
                std::make_shared<Azure::Storage::Blobs::BlobContainerClient>(uri, cred);
        obj_client_ = std::make_shared<AzureObjClient>(std::move(container_client));
        return 0;
    }
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
    obj_client_ = std::make_shared<S3ObjClient>(std::move(s3_client));
    return 0;
}

int S3Accessor::delete_objects_by_prefix(const std::string& relative_path) {
    return s3_get_rate_limit([&]() {
               return obj_client_->delete_objects_recursively(
                       {.bucket = conf_.bucket, .prefix = get_key(relative_path)});
           })
            .ret;
}

int S3Accessor::delete_objects(const std::vector<std::string>& relative_paths) {
    if (relative_paths.empty()) {
        return 0;
    }
    // `DeleteObjectsRequest` can only contain 1000 keys at most.
    constexpr size_t max_delete_batch = 1000;
    auto path_iter = relative_paths.begin();

    do {
        Aws::Vector<std::string> objects;
        auto path_begin = path_iter;
        for (; path_iter != relative_paths.end() && (path_iter - path_begin < max_delete_batch);
             ++path_iter) {
            auto key = get_key(*path_iter);
            LOG_INFO("delete object")
                    .tag("endpoint", conf_.endpoint)
                    .tag("bucket", conf_.bucket)
                    .tag("key", key)
                    .tag("size", objects.size());
            objects.emplace_back(std::move(key));
        }
        if (objects.empty()) {
            return 0;
        }
        if (auto delete_resp = s3_put_rate_limit([&]() {
                return obj_client_->delete_objects({.bucket = conf_.bucket}, std::move(objects));
            });
            delete_resp.ret != 0) {
            return delete_resp.ret;
        }
    } while (path_iter != relative_paths.end());

    return 0;
}

int S3Accessor::delete_object(const std::string& relative_path) {
    return s3_put_rate_limit([&]() {
        return obj_client_->delete_object({.bucket = conf_.bucket, .key = get_key(relative_path)})
                .ret;
    });
}

int S3Accessor::put_object(const std::string& relative_path, const std::string& content) {
    return s3_put_rate_limit([&]() {
        return obj_client_
                ->put_object({.bucket = conf_.bucket, .key = get_key(relative_path)}, content)
                .ret;
    });
}

int S3Accessor::list(const std::string& relative_path, std::vector<ObjectMeta>* files) {
    return s3_get_rate_limit([&]() {
        auto resp = obj_client_->list_objects(
                {.bucket = conf_.bucket, .prefix = get_key(relative_path)}, files);

        if (resp.ret == 0) {
            auto pos = conf_.prefix.size() + 1;
            for (auto&& file : *files) {
                file.path = file.path.substr(pos);
            }
        }

        return resp.ret;
    });
}

int S3Accessor::exist(const std::string& relative_path) {
    return s3_get_rate_limit([&]() {
        return obj_client_->head_object({.bucket = conf_.bucket, .key = get_key(relative_path)})
                .ret;
    });
}

int S3Accessor::delete_expired_objects(const std::string& relative_path, int64_t expired_time) {
    return s3_put_rate_limit([&]() {
        return obj_client_
                ->delete_expired(
                        {.path_opts = {.bucket = conf_.bucket, .prefix = get_key(relative_path)},
                         .relative_path_factory =
                                 [&](const std::string& key) { return get_relative_path(key); }},
                        expired_time)
                .ret;
    });
}

int S3Accessor::get_bucket_lifecycle(int64_t* expiration_days) {
    return s3_get_rate_limit([&]() {
        return obj_client_->get_life_cycle({.bucket = conf_.bucket}, expiration_days).ret;
    });
}

int S3Accessor::check_bucket_versioning() {
    return s3_get_rate_limit(
            [&]() { return obj_client_->check_versioning({.bucket = conf_.bucket}).ret; });
}

int GcsAccessor::delete_objects(const std::vector<std::string>& relative_paths) {
    std::vector<int> delete_rets(relative_paths.size());
    std::transform(std::execution::par, relative_paths.begin(), relative_paths.end(),
                   delete_rets.begin(),
                   [this](const std::string& path) { return delete_object(path); });
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
