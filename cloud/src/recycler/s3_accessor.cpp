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

#include <gen_cpp/cloud.pb.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <execution>
#include <limits>
#include <memory>
#include <utility>
#include <variant>

#include "common/config.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "common/simple_thread_pool.h"
#include "common/string_util.h"
#include "cpp/aws_common.h"
#include "cpp/obj-storage/client/obj_storage_client_builder.h"
#include "cpp/obj-storage/obj_storage_properties.h"
#include "cpp/obj-storage/rate_limited_obj_storage_client.h"
#include "cpp/sync_point.h"
#include "cpp/token_bucket_rate_limiter.h"
#include "cpp/util.h"
#include "recycler/storage_vault_accessor.h"
#include "recycler/sync_executor.h"

namespace doris::cloud {

AccessorRateLimiter::AccessorRateLimiter()
        : _rate_limiters({std::make_unique<S3RateLimiterHolder>(
                                  config::s3_get_token_per_second, config::s3_get_bucket_tokens,
                                  config::s3_get_token_limit,
                                  s3_rate_limiter_metric_func(ObjStorageRateLimitType::GET)),
                          std::make_unique<S3RateLimiterHolder>(
                                  config::s3_put_token_per_second, config::s3_put_bucket_tokens,
                                  config::s3_put_token_limit,
                                  s3_rate_limiter_metric_func(ObjStorageRateLimitType::PUT))}) {}

S3RateLimiterHolder* AccessorRateLimiter::rate_limiter(ObjStorageRateLimitType type) {
    CHECK(type == ObjStorageRateLimitType::GET || type == ObjStorageRateLimitType::PUT)
            << to_string(type);
    return _rate_limiters[static_cast<size_t>(type)].get();
}

AccessorRateLimiter& AccessorRateLimiter::instance() {
    static AccessorRateLimiter instance;
    return instance;
}

int reset_s3_rate_limiter(ObjStorageRateLimitType type, size_t max_speed, size_t max_burst,
                          size_t limit) {
    if (type == ObjStorageRateLimitType::UNKNOWN) {
        return -1;
    }
    if (type == ObjStorageRateLimitType::GET) {
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

namespace {

class RecyclerObjStorageRateLimitPolicy final : public ObjStorageRateLimitPolicy {
public:
    ObjStorageAdmission acquire(ObjStorageRateLimitType type, size_t) const override {
        if (config::enable_s3_rate_limit_inject && type == ObjStorageRateLimitType::PUT &&
            rand() % 100 < config::s3_rate_limit_inject_probility) {
            return ObjStorageAdmission {
                    .resp = ObjStorageResponse::rate_limit(
                            TStatusCode::LIMIT_REACH, 429,
                            "object storage PUT request rejected by recycler fault injection"),
            };
        }
        if (config::enable_s3_rate_limiter &&
            doris::apply_s3_rate_limit(type, AccessorRateLimiter::instance().rate_limiter(type),
                                       config::s3_rate_limiter_log_interval) < 0) {
            return ObjStorageAdmission {
                    .resp = ObjStorageResponse::rate_limit(
                            TStatusCode::LIMIT_REACH, 429,
                            fmt::format(
                                    "object storage {} request rejected by recycler rate limiter",
                                    to_string(type))),
            };
        }
        return ObjStorageAdmission {};
    }
};

class RecyclerObjStorageDeleteExecutor final : public ObjStorageDeleteExecutor {
public:
    explicit RecyclerObjStorageDeleteExecutor(std::shared_ptr<SimpleThreadPool> pool)
            : pool_(std::move(pool)) {
        reset();
    }

    ObjStorageResponse submit(ObjStorageDeleteTask task) override {
        executor_->add(std::move(task));
        return ObjStorageResponse::OK();
    }

    ObjStorageResponse wait() override {
        bool finished = false;
        auto responses = executor_->when_all(&finished);
        reset();
        if (!finished) {
            return {
                    .status = {TStatusCode::INTERNAL_ERROR,
                               "object storage batch deletion did not finish"},
                    .http_code = 0,
            };
        }
        for (auto& response : responses) {
            if (!response.ok()) {
                return response;
            }
        }
        return ObjStorageResponse::OK();
    }

private:
    void reset() {
        executor_ = std::make_unique<SyncExecutor<ObjStorageResponse>>(
                pool_, "delete object storage batches",
                [](const ObjStorageResponse& response) { return !response.ok(); });
    }

    std::shared_ptr<SimpleThreadPool> pool_;
    std::unique_ptr<SyncExecutor<ObjStorageResponse>> executor_;
};

} // namespace

class S3ListIterator final : public ListIterator {
public:
    S3ListIterator(std::unique_ptr<ObjStorageListIterator> iter, size_t prefix_length)
            : iter_(std::move(iter)), prefix_length_(prefix_length) {}

    ~S3ListIterator() override = default;

    bool is_valid() override { return iter_->is_valid(); }

    bool has_next() override { return iter_->has_next().ok(); }

    std::optional<FileMeta> next() override {
        auto result = iter_->next();
        if (!result.object.has_value()) {
            return std::nullopt;
        }
        auto& obj = *result.object;
        return FileMeta {
                .path = get_relative_path(obj.key),
                .size = obj.size,
                .mtime_s = obj.mtime_s,
        };
    }

private:
    std::string get_relative_path(const std::string& key) const {
        return key.substr(prefix_length_);
    }

    std::unique_ptr<ObjStorageListIterator> iter_;
    size_t prefix_length_;
};

std::optional<ObjStorageResourceConfig> parse_recycler_storage_config(
        ObjectStoreInfoPB vault, const RamUserPB* bucket_acl_user) {
    if (bucket_acl_user) {
        if (bucket_acl_user->ak().empty() || bucket_acl_user->sk().empty()) {
            LOG_WARNING("BUCKET_ACL stage requires an instance RAM key pair")
                    .tag("vault_id", vault.id());
            return std::nullopt;
        }
        vault.set_ak(bucket_acl_user->ak());
        vault.set_sk(bucket_acl_user->sk());
        vault.clear_role_arn();
        vault.clear_external_id();
        vault.clear_cred_provider_type();
        vault.clear_credential();
        vault.clear_encryption_info();
        if (bucket_acl_user->has_encryption_info()) {
            vault.mutable_encryption_info()->CopyFrom(bucket_acl_user->encryption_info());
        }
    }

    if (!bucket_acl_user && (vault.ak().empty() || vault.sk().empty())) {
        // Older vaults with an incomplete key pair fell back to their credential provider.
        vault.clear_ak();
        vault.clear_sk();
    } else if (vault.has_encryption_info()) {
        AkSkPair keys;
        if (decrypt_ak_sk_helper(vault.ak(), vault.sk(), vault.encryption_info(), &keys) != 0) {
            LOG_WARNING("failed to decrypt object storage credentials").tag("vault_id", vault.id());
            return std::nullopt;
        }
        vault.set_ak(std::move(keys.first));
        vault.set_sk(std::move(keys.second));
    }
    if (!vault.role_arn().empty() && !vault.has_cred_provider_type()) {
        vault.set_cred_provider_type(CredProviderTypePB::INSTANCE_PROFILE);
    }

    ObjStorageResourceConfig resource;
    if (auto error = parse_vault_storage_config(
                vault, aws_credential_implementation(config::aws_credentials_provider_version),
                EmptyCredentialsBehavior::DEFAULT_CHAIN, &resource)) {
        LOG_WARNING("invalid object storage vault")
                .tag("vault_id", vault.id())
                .tag("error", *error);
        return std::nullopt;
    }

    auto& connection = obj_storage_connection(resource.client);
    auto ca_file = get_valid_ca_cert_path(split(config::ca_cert_file_paths, ';'));
    if (!ca_file.empty()) {
        connection.ca_cert_file = std::move(ca_file);
    }
    const auto retries = config::max_s3_client_retry;
    if (retries < 0 || retries > std::numeric_limits<int>::max()) {
        LOG_WARNING("invalid max_s3_client_retry").tag("value", retries);
        return std::nullopt;
    }
    connection.max_retries = static_cast<int>(retries);
    normalize_obj_storage_endpoint(resource.client, config::s3_client_http_scheme);
    if (!std::holds_alternative<AzureObjStorageConfig>(resource.client)) {
        const auto workers = static_cast<int64_t>(config::recycle_pool_parallelism) +
                             config::instance_recycler_worker_pool_size;
        const auto max_connections = std::max<int64_t>(default_s3_max_connections(), workers);
        if (max_connections > std::numeric_limits<int>::max()) {
            LOG_WARNING("Recycler connection pool exceeds the supported size")
                    .tag("size", max_connections);
            return std::nullopt;
        }
        connection.max_connections = static_cast<int>(max_connections);
        connection.request_timeout = std::chrono::seconds(30);
        connection.connect_timeout = std::chrono::seconds(5);
    }
    return resource;
}

S3Accessor::S3Accessor(ObjStorageResourceConfig conf)
        : StorageVaultAccessor(AccessorType::S3), conf_(std::move(conf)) {}

S3Accessor::~S3Accessor() = default;

std::string S3Accessor::get_key(const std::string& relative_path) const {
    return conf_.prefix.empty() ? relative_path : conf_.prefix + '/' + relative_path;
}

std::string S3Accessor::to_uri(const std::string& relative_path) const {
    return uri_ + '/' + relative_path;
}

int S3Accessor::create(ObjStorageResourceConfig conf, std::shared_ptr<S3Accessor>* accessor) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("S3Accessor::init.s3_init_failed", (int)-1);
    std::shared_ptr<S3Accessor> candidate;
    if (std::holds_alternative<GcpObjStorageConfig>(conf.client) &&
        !config::enable_gcs_batch_delete_object) {
        candidate = std::make_shared<GcsAccessor>(std::move(conf));
    } else {
        candidate = std::make_shared<S3Accessor>(std::move(conf));
    }
    if (int ret = candidate->init(); ret != 0) {
        return ret;
    }
    *accessor = std::move(candidate);
    return 0;
}

static std::shared_ptr<SimpleThreadPool> worker_pool;

ObjStorageRecursiveDeleteOptions S3Accessor::make_recursive_delete_options(
        int64_t expiration_time, std::shared_ptr<SimpleThreadPool> pool) {
    const auto configured_max_tasks = config::recycler_max_tasks_per_batch;
    size_t max_tasks_per_batch = 1000;
    if (configured_max_tasks > 0) {
        max_tasks_per_batch = static_cast<size_t>(configured_max_tasks);
    } else {
        LOG(WARNING) << "recycler_max_tasks_per_batch=" << configured_max_tasks
                     << " is not positive, using default 1000";
    }
    ObjStorageRecursiveDeleteOptions options {
            .expiration_time = expiration_time,
            .max_tasks_per_batch = max_tasks_per_batch,
            .executor = std::make_shared<RecyclerObjStorageDeleteExecutor>(std::move(pool)),
    };
    return options;
}

int S3Accessor::init() {
    if (auto error = validate_obj_storage_config(conf_.client)) {
        LOG_WARNING("invalid Recycler object storage client configuration").tag("error", *error);
        return -1;
    }
    static std::once_flag log_annotated_tags_key_once;
    std::call_once(log_annotated_tags_key_once, [&]() {
        LOG_INFO("start s3 accessor parallel worker pool");
        worker_pool =
                std::make_shared<SimpleThreadPool>(config::recycle_pool_parallelism, "s3_accessor");
        worker_pool->start();
    });
    auto built = build_obj_storage_client(conf_.client);
    if (!built.client) {
        LOG_WARNING("failed to create object storage client").tag("error", built.error);
        return -1;
    }
    std::string uri = obj_storage_connection(conf_.client).endpoint + '/' + conf_.bucket;
    if (!conf_.prefix.empty() || std::holds_alternative<AzureObjStorageConfig>(conf_.client)) {
        uri += '/' + conf_.prefix;
    }
    uri_ = normalize_http_uri(uri);
    static const auto rate_limit_policy =
            std::make_shared<const RecyclerObjStorageRateLimitPolicy>();
    obj_client_ = std::make_shared<RateLimitedObjStorageClient>(std::move(built.client),
                                                                rate_limit_policy);
    return 0;
}

int S3Accessor::delete_prefix_impl(const std::string& path_prefix, int64_t expiration_time) {
    LOG_INFO("delete prefix").tag("uri", to_uri(path_prefix));
    ObjStoragePath delete_path {
            .bucket = conf_.bucket,
            .prefix = get_key(path_prefix),
    };
    return delete_objects_recursively(obj_client_, delete_path,
                                      make_recursive_delete_options(expiration_time, worker_pool))
            .status.code;
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

    return obj_client_->delete_objects({.bucket = conf_.bucket}, std::move(keys)).status.code;
}

int S3Accessor::delete_file(const std::string& path) {
    LOG_INFO("delete file").tag("uri", to_uri(path));
    int ret =
            obj_client_->delete_object({.bucket = conf_.bucket, .key = get_key(path)}).status.code;
    static_assert(ObjStorageStatus::OK == 0);
    if (ret == ObjStorageStatus::OK || ret == ObjStorageStatus::NOT_FOUND) {
        return 0;
    }
    return ret;
}

int S3Accessor::put_file(const std::string& path, const std::string& content) {
    return obj_client_
            ->put_object({.bucket = conf_.bucket,
                          .key = get_key(path),
                          .sse_enabled = conf_.sse_enabled},
                         content)
            .status.code;
}

int S3Accessor::list_prefix(const std::string& path_prefix, std::unique_ptr<ListIterator>* res) {
    *res = std::make_unique<S3ListIterator>(
            obj_client_->list_objects({.bucket = conf_.bucket, .prefix = get_key(path_prefix)}),
            conf_.prefix.empty() ? 0 : conf_.prefix.length() + 1);
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
    auto response = obj_client_->head_object({.bucket = conf_.bucket, .key = get_key(path)}).resp;
    if (response.ok()) {
        return 0;
    }
    if (response.status.code == ObjStorageStatus::NOT_FOUND) {
        return 1;
    }
    return -1;
}

int S3Accessor::abort_multipart_upload(const std::string& path, const std::string& upload_id) {
    LOG_INFO("abort multipart upload").tag("uri", to_uri(path)).tag("upload_id", upload_id);
    int ret = obj_client_
                      ->abort_multipart_upload({.bucket = conf_.bucket, .key = get_key(path)},
                                               upload_id)
                      .status.code;
    static_assert(ObjStorageStatus::OK == 0);
    if (ret == ObjStorageStatus::OK || ret == ObjStorageStatus::NOT_FOUND) {
        return 0;
    }
    LOG_WARNING("fail abort multipart upload")
            .tag("uri", to_uri(path))
            .tag("upload_id", upload_id)
            .tag("ret", ret);
    return ret;
}

int S3Accessor::get_lifecycle(int64_t* expiration_days) {
    return obj_client_->get_lifecycle(conf_.bucket, expiration_days).status.code;
}

int S3Accessor::check_versioning() {
    return obj_client_->check_versioning(conf_.bucket).status.code;
}

int GcsAccessor::delete_prefix_impl(const std::string& path_prefix, int64_t expiration_time) {
    LOG_INFO("begin delete prefix").tag("uri", to_uri(path_prefix));

    int ret = 0;
    int cnt = 0;
    int skip = 0;
    int64_t del_nonexisted = 0;
    int del = 0;
    auto iter = obj_client_->list_objects({.bucket = conf_.bucket, .prefix = get_key(path_prefix)});
    for (;;) {
        auto result = iter->next();
        if (!result.object.has_value()) {
            if (result.resp.status.code != ObjStorageStatus::NOT_FOUND &&
                result.resp.status.code != ObjStorageStatus::OK) {
                ret = result.resp.status.code;
            }
            break;
        }
        auto& obj = *result.object;
        if (!(++cnt % 100)) {
            LOG_INFO("loop delete prefix")
                    .tag("uri", to_uri(path_prefix))
                    .tag("total_obj_cnt", cnt)
                    .tag("deleted", del)
                    .tag("del_nonexisted", del_nonexisted)
                    .tag("skipped", skip);
        }
        if (expiration_time > 0 && obj.mtime_s > expiration_time) {
            skip++;
            continue;
        }
        del++;

        // FIXME(plat1ko): Delete objects by batch with genuine GCS client
        int del_ret =
                obj_client_->delete_object({.bucket = conf_.bucket, .key = obj.key}).status.code;
        del_nonexisted += (del_ret == ObjStorageStatus::NOT_FOUND);
        static_assert(ObjStorageStatus::OK == 0);
        if (del_ret != ObjStorageStatus::OK && del_ret != ObjStorageStatus::NOT_FOUND) {
            ret = del_ret;
        }
    }

    LOG_INFO("finish delete prefix")
            .tag("uri", to_uri(path_prefix))
            .tag("total_obj_cnt", cnt)
            .tag("deleted", del)
            .tag("del_nonexisted", del_nonexisted)
            .tag("skipped", skip);

    if (!iter->is_valid()) {
        return -1;
    }

    return ret;
}

int GcsAccessor::delete_files(const std::vector<std::string>& paths) {
    std::vector<int> delete_rets(paths.size());
#ifdef USE_LIBCPP
    std::transform(paths.begin(), paths.end(), delete_rets.begin(),
#else
    std::transform(std::execution::par, paths.begin(), paths.end(), delete_rets.begin(),
#endif
                   [this](const std::string& path) { return delete_file(path); });

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
