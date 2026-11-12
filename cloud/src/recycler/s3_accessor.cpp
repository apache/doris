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
#include <cstdlib>
#include <execution>
#include <memory>
#include <utility>
#include <variant>

#include "common/config.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "common/simple_thread_pool.h"
#include "common/string_util.h"
#ifdef USE_AZURE
#include "cpp/obj-storage/client/azure_obj_storage_client.h"
#endif
#include "cpp/obj-storage/auth/obj_credential.h"
#include "cpp/obj-storage/client/bos_obj_storage_client.h"
#include "cpp/obj-storage/client/cos_obj_storage_client.h"
#include "cpp/obj-storage/client/gcp_obj_storage_client.h"
#include "cpp/obj-storage/client/obj_storage_client_runtime.h"
#include "cpp/obj-storage/client/obs_obj_storage_client.h"
#include "cpp/obj-storage/client/oss_obj_storage_client.h"
#include "cpp/obj-storage/client/s3_obj_storage_client.h"
#include "cpp/obj-storage/client/tos_obj_storage_client.h"
#include "cpp/obj-storage/obj_storage_config.h"
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

std::optional<S3Conf> S3Conf::from_obj_store_info(const ObjectStoreInfoPB& obj_info,
                                                  bool skip_aksk) {
    S3Conf s3_conf;

    const auto provider = vault_storage_provider(obj_info);
    switch (provider) {
    case ObjectStoreInfoPB_Provider_OSS:
    case ObjectStoreInfoPB_Provider_S3:
    case ObjectStoreInfoPB_Provider_COS:
    case ObjectStoreInfoPB_Provider_OBS:
    case ObjectStoreInfoPB_Provider_BOS:
    case ObjectStoreInfoPB_Provider_GCP:
    case ObjectStoreInfoPB_Provider_AZURE:
    case ObjectStoreInfoPB_Provider_TOS:
        s3_conf.provider = provider;
        break;
    default:
        LOG_WARNING("unknown object storage provider {}", provider);
        return std::nullopt;
    }

    if (!skip_aksk) {
        if (!obj_info.ak().empty() && !obj_info.sk().empty()) {
            if (obj_info.has_encryption_info()) {
                AkSkPair plain_ak_sk_pair;
                int ret = decrypt_ak_sk_helper(obj_info.ak(), obj_info.sk(),
                                               obj_info.encryption_info(), &plain_ak_sk_pair);
                if (ret != 0) {
                    LOG_WARNING("fail to decrypt ak sk").tag("object_store_id", obj_info.id());
                    return std::nullopt;
                } else {
                    s3_conf.ak = std::move(plain_ak_sk_pair.first);
                    s3_conf.sk = std::move(plain_ak_sk_pair.second);
                }
            } else {
                s3_conf.ak = obj_info.ak();
                s3_conf.sk = obj_info.sk();
            }
        }
        if (obj_info.has_cred_provider_type()) {
            s3_conf.cred_provider_type = cred_provider_type_from_pb(obj_info.cred_provider_type());
        }

        if (obj_info.has_role_arn() && !obj_info.role_arn().empty()) {
            s3_conf.role_arn = obj_info.role_arn();
            s3_conf.external_id = obj_info.external_id();
            if (!obj_info.has_cred_provider_type()) {
                s3_conf.cred_provider_type = CredProviderType::InstanceProfile;
            }
        }
    }

    convert_obj_credential(obj_info, &s3_conf.credential);

    s3_conf.endpoint = obj_info.endpoint();
    s3_conf.region = obj_info.region();
    s3_conf.bucket = obj_info.bucket();
    s3_conf.prefix = obj_info.prefix();
    s3_conf.use_virtual_addressing = !obj_info.use_path_style();

    return s3_conf;
}

S3Accessor::S3Accessor(S3Conf conf)
        : StorageVaultAccessor(AccessorType::S3), conf_(std::move(conf)) {}

S3Accessor::~S3Accessor() = default;

std::string S3Accessor::get_key(const std::string& relative_path) const {
    return conf_.prefix.empty() ? relative_path : conf_.prefix + '/' + relative_path;
}

std::string S3Accessor::to_uri(const std::string& relative_path) const {
    return uri_ + '/' + relative_path;
}

int S3Accessor::create(S3Conf conf, std::shared_ptr<S3Accessor>* accessor) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("S3Accessor::init.s3_init_failed", (int)-1);
    switch (conf.provider) {
    case ObjectStoreInfoPB::GCP:
        if (!config::enable_gcs_batch_delete_object) {
            *accessor = std::make_shared<GcsAccessor>(conf);
            break;
        }
    default:
        *accessor = std::make_shared<S3Accessor>(conf);
        break;
    }

    return (*accessor)->init();
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

static ObjStorageClientRuntime recycler_client_runtime(std::string ca_cert_path) {
    return {.credential_provider_version = config::aws_credentials_provider_version,
            .ca_cert_path = std::move(ca_cert_path),
            .http_scheme = config::s3_client_http_scheme,
            .max_retries = static_cast<int>(config::max_s3_client_retry),
            .min_connections = static_cast<int64_t>(config::recycle_pool_parallelism) +
                               config::instance_recycler_worker_pool_size};
}

static ObjStorageClientBuildResult create_recycler_client(const ObjStorageConfig& config,
                                                          const ObjStorageClientRuntime& runtime) {
    if (const auto* aws = std::get_if<AwsConfig>(&config)) {
        return S3ObjStorageClient::create(*aws, runtime);
    }
    if (const auto* oss = std::get_if<OssConfig>(&config)) {
        return OssObjStorageClient::create(*oss, runtime);
    }
    if (const auto* cos = std::get_if<CosConfig>(&config)) {
        return CosObjStorageClient::create(*cos, runtime);
    }
    if (const auto* bos = std::get_if<BosConfig>(&config)) {
        return BosObjStorageClient::create(*bos, runtime);
    }
    if (const auto* obs = std::get_if<ObsConfig>(&config)) {
        return ObsObjStorageClient::create(*obs, runtime);
    }
    if (const auto* gcp = std::get_if<GcpConfig>(&config)) {
        return GcpObjStorageClient::create(*gcp, runtime);
    }
    if (const auto* tos = std::get_if<TosConfig>(&config)) {
        return TosObjStorageClient::create(*tos, runtime);
    }
    return {.error = "Unsupported object storage provider"};
}

int S3Accessor::init() {
    static std::once_flag log_annotated_tags_key_once;
    std::call_once(log_annotated_tags_key_once, [&]() {
        LOG_INFO("start s3 accessor parallel worker pool");
        worker_pool =
                std::make_shared<SimpleThreadPool>(config::recycle_pool_parallelism, "s3_accessor");
        worker_pool->start();
    });
    ensure_aws_sdk_initialized(config::aws_log_level);
    auto storage_config = make_vault_storage_config(conf_.provider);
    if (!storage_config) {
        LOG_WARNING("unknown object storage provider {}", conf_.provider);
        return -1;
    }

    auto& fields = storage_config_base(*storage_config);
    fields.bucket = conf_.bucket;
    fields.prefix = conf_.prefix;
    fields.endpoint = conf_.endpoint;
    fields.region = conf_.region;
    fields.ak = conf_.ak;
    fields.sk = conf_.sk;
    fields.connect_timeout_ms = 5000;
    fields.use_virtual_addressing = conf_.use_virtual_addressing;
    fields.cred_provider_type = conf_.cred_provider_type;
    fields.role_arn = conf_.role_arn;
    fields.external_id = conf_.external_id;
    fields.credential = conf_.credential;

    ObjStorageClientBuildResult built;
    if (const auto* azure = std::get_if<AzureConfig>(&*storage_config)) {
#ifdef USE_AZURE
        uri_ = normalize_http_uri(AzureObjStorageClient::container_url(*azure) + '/' +
                                  azure->prefix);
        built = AzureObjStorageClient::create(
                *azure, {.max_retries = static_cast<int>(config::max_s3_client_retry),
                         .azure_log_level = config::azure_log_level});
#else
        LOG_FATAL(
                "Recycler is not compiled with Azure support, export BUILD_AZURE=ON before "
                "building");
        __builtin_unreachable();
#endif
    } else {
        uri_ = normalize_http_uri(conf_.prefix.empty() ? conf_.endpoint + '/' + conf_.bucket
                                                       : conf_.endpoint + '/' + conf_.bucket + '/' +
                                                                 conf_.prefix);
        built = create_recycler_client(
                *storage_config,
                recycler_client_runtime(_ca_bundle.path(config::ca_cert_file_paths)));
    }
    if (!built.client) {
        LOG_WARNING("failed to create object storage client").tag("error", built.error);
        return -1;
    }
    obj_client_ = std::make_shared<RateLimitedObjStorageClient>(
            std::move(built.client), std::make_shared<RecyclerObjStorageRateLimitPolicy>());
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
    return obj_client_->put_object({.bucket = conf_.bucket, .key = get_key(path)}, content)
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
