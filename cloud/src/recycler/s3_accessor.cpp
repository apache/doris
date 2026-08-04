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

#include <aws/core/auth/AWSAuthSigner.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/S3Client.h>
#include <gen_cpp/cloud.pb.h>

#include <algorithm>
#include <cstdlib>

#ifdef USE_AZURE
#include <azure/core/diagnostics/logger.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>
#include <azure/storage/common/storage_credential.hpp>
#endif
#include <execution>
#include <memory>
#include <utility>

#include "common/config.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "common/simple_thread_pool.h"
#include "common/string_util.h"
#include "common/util.h"
#include "cpp/client/auth/aws_credential_factory.h"
#ifdef USE_AZURE
#include "cpp/client/auth/azure_auth_factory.h"
#include "cpp/client/azure_obj_storage_backend.h"
#endif
#include "cpp/aws_logger.h"
#include "cpp/client/s3_obj_storage_backend.h"
#include "cpp/obj_retry_strategy.h"
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
                                  s3_rate_limiter_metric_func(S3RateLimitType::GET)),
                          std::make_unique<S3RateLimiterHolder>(
                                  config::s3_put_token_per_second, config::s3_put_bucket_tokens,
                                  config::s3_put_token_limit,
                                  s3_rate_limiter_metric_func(S3RateLimitType::PUT))}) {}

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

class RecyclerObjStorageRateLimitPolicy final : public ObjStorageRateLimitPolicy {
public:
    ObjStorageRateLimitToken acquire(ObjStorageRequestType type, size_t) const override {
        const auto limiter_type =
                type == ObjStorageRequestType::GET ? S3RateLimitType::GET : S3RateLimitType::PUT;
        if (config::enable_s3_rate_limit_inject && limiter_type == S3RateLimitType::PUT &&
            rand() % 100 < config::s3_rate_limit_inject_probility) {
            return ObjStorageRateLimitToken {
                    .resp = ObjectStorageResponse::rate_limit(
                            "object storage PUT request rejected by recycler fault injection"),
            };
        }
        if (config::enable_s3_rate_limiter &&
            doris::apply_s3_rate_limit(limiter_type,
                                       AccessorRateLimiter::instance().rate_limiter(limiter_type),
                                       config::s3_rate_limiter_log_interval) < 0) {
            return ObjStorageRateLimitToken {
                    .resp = ObjectStorageResponse::rate_limit(fmt::format(
                            "object storage {} request rejected by recycler rate limiter",
                            to_string(limiter_type))),
            };
        }
        return ObjStorageRateLimitToken {};
    }
};

S3Environment::S3Environment() {
    LOG(INFO) << "Initializing S3 environment";
    aws_options_ = Aws::SDKOptions {};
    auto logLevel = static_cast<Aws::Utils::Logging::LogLevel>(config::aws_log_level);
    aws_options_.loggingOptions.logLevel = logLevel;
    aws_options_.loggingOptions.logger_create_fn = [logLevel] {
        return std::make_shared<DorisAWSLogger>(logLevel);
    };
    Aws::InitAPI(aws_options_);

#ifdef USE_AZURE
    auto azureLogLevel =
            static_cast<Azure::Core::Diagnostics::Logger::Level>(config::azure_log_level);
    Azure::Core::Diagnostics::Logger::SetLevel(azureLogLevel);
    Azure::Core::Diagnostics::Logger::SetListener(
            [&](Azure::Core::Diagnostics::Logger::Level level, const std::string& message) {
                switch (level) {
                case Azure::Core::Diagnostics::Logger::Level::Verbose:
                    LOG(INFO) << message;
                    break;
                case Azure::Core::Diagnostics::Logger::Level::Informational:
                    LOG(INFO) << message;
                    break;
                case Azure::Core::Diagnostics::Logger::Level::Warning:
                    LOG(WARNING) << message;
                    break;
                case Azure::Core::Diagnostics::Logger::Level::Error:
                    LOG(ERROR) << message;
                    break;
                default:
                    LOG(WARNING) << "Unknown level: " << static_cast<int>(level)
                                 << ", message: " << message;
                    break;
                }
            });
#endif
}

S3Environment& S3Environment::getInstance() {
    static S3Environment instance;
    return instance;
}

S3Environment::~S3Environment() {
    Aws::ShutdownAPI(aws_options_);
}

class S3ListIterator final : public ListIterator {
public:
    S3ListIterator(std::shared_ptr<ObjStorageClient> client, ObjectStoragePathOptions opts,
                   size_t prefix_length)
            : iter_(std::move(client), std::move(opts)), prefix_length_(prefix_length) {}

    ~S3ListIterator() override = default;

    bool is_valid() override { return iter_.is_valid(); }

    bool has_next() override { return iter_.has_next().ok(); }

    std::optional<FileMeta> next() override {
        auto result = iter_.next();
        if (!result.results_.has_value()) {
            return std::nullopt;
        }
        auto& obj = *result.results_;
        return FileMeta {
                .path = get_relative_path(obj.file_path),
                .size = obj.size,
                .mtime_s = obj.mtime_s,
        };
    }

private:
    std::string get_relative_path(const std::string& key) const {
        return key.substr(prefix_length_);
    }

    ObjectListIterator iter_;
    size_t prefix_length_;
};

std::optional<S3Conf> S3Conf::from_obj_store_info(const ObjectStoreInfoPB& obj_info,
                                                  bool skip_aksk) {
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

    if (!skip_aksk) {
        if (!obj_info.ak().empty() && !obj_info.sk().empty()) {
            if (obj_info.has_encryption_info()) {
                AkSkPair plain_ak_sk_pair;
                int ret = decrypt_ak_sk_helper(obj_info.ak(), obj_info.sk(),
                                               obj_info.encryption_info(), &plain_ak_sk_pair);
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
    case S3Conf::GCS:
        *accessor = std::make_shared<GcsAccessor>(conf);
        break;
    default:
        *accessor = std::make_shared<S3Accessor>(conf);
        break;
    }

    return (*accessor)->init();
}

static std::shared_ptr<SimpleThreadPool> worker_pool;

RecursiveDeleteOptions S3Accessor::make_recursive_delete_options(
        int64_t expiration_time, std::shared_ptr<SimpleThreadPool> pool) {
    RecursiveDeleteOptions options {
            .expiration_time = expiration_time,
            .max_tasks_per_batch =
                    config::recycler_max_tasks_per_batch > 0
                            ? static_cast<size_t>(config::recycler_max_tasks_per_batch)
                            : 1000,
    };
    options.executor = [pool = std::move(pool)](std::vector<ObjStorageDeleteTask> tasks) {
        SyncExecutor<ObjectStorageResponse> executor(
                pool, "delete object storage batches",
                [](const ObjectStorageResponse& response) { return !response.ok(); });
        for (auto& task : tasks) {
            executor.add(std::move(task));
        }
        bool finished = false;
        auto responses = executor.when_all(&finished);
        if (!finished) {
            return ObjectStorageResponse {
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
        return ObjectStorageResponse::OK();
    };
    return options;
}

AwsCredentialResult S3Accessor::create_aws_credentials_provider(const S3Conf& s3_conf) {
    auto sts_config = S3Environment::getClientConfiguration();
    if (!_ca_cert_file_path.empty()) {
        sts_config.caFile = _ca_cert_file_path;
    }
    return AwsCredentialFactory::create({
            .version = config::aws_credentials_provider_version == "v2"
                               ? AwsCredentialProviderVersion::V2
                               : AwsCredentialProviderVersion::V1,
            .access_key = s3_conf.ak,
            .secret_key = s3_conf.sk,
            .provider_type = s3_conf.cred_provider_type,
            .role_arn = s3_conf.role_arn,
            .external_id = s3_conf.external_id,
            .empty_credentials = EmptyCredentialsBehavior::DEFAULT_CHAIN,
            .sts_client_config = std::move(sts_config),
    });
}

int S3Accessor::init() {
    static std::once_flag log_annotated_tags_key_once;
    std::call_once(log_annotated_tags_key_once, [&]() {
        LOG_INFO("start s3 accessor parallel worker pool");
        worker_pool =
                std::make_shared<SimpleThreadPool>(config::recycle_pool_parallelism, "s3_accessor");
        worker_pool->start();
    });
    S3Environment::getInstance();
    switch (conf_.provider) {
    case S3Conf::AZURE: {
#ifdef USE_AZURE
        Azure::Storage::Blobs::BlobClientOptions options;
        options.Retry.MaxRetries = config::max_s3_client_retry;
        uri_ = fmt::format("{}/{}", conf_.endpoint, conf_.bucket);
        if (uri_.find("://") == std::string::npos) {
            uri_ = "https://" + uri_;
        }
        uri_ = normalize_http_uri(uri_);
        // In Azure's HTTP requests, all policies in the vector are called in a chained manner following the HTTP pipeline approach.
        // Within the RetryPolicy, the nextPolicy is called multiple times inside a loop.
        // All policies in the PerRetryPolicies are downstream of the RetryPolicy.
        // Therefore, the policy can record retries after the RetryPolicy has handled the response.
        options.PerRetryPolicies.emplace_back(std::make_unique<AzureRetryRecordPolicy>());
        auto built = AzureAuthFactory::create(uri_,
                                              {
                                                      .type = AzureCredentialType::SHARED_KEY,
                                                      .account_name = conf_.ak,
                                                      .account_key = conf_.sk,
                                              },
                                              std::move(options));
        if (!built) {
            LOG_WARNING("failed to create Azure client").tag("error", built.error);
            return -1;
        }
        // uri format for debug: ${scheme}://${ak}.blob.core.windows.net/${bucket}/${prefix}
        uri_ = normalize_http_uri(uri_ + '/' + conf_.prefix);
        auto backend =
                std::make_shared<AzureObjStorageBackend>(std::move(built.container_client),
                                                         ObjectClientConfig {
                                                                 .endpoint = conf_.endpoint,
                                                                 .ak = conf_.ak,
                                                                 .sk = conf_.sk,
                                                         },
                                                         std::move(built.shared_key_credential));
        obj_client_ = std::make_shared<ObjStorageClient>(
                std::move(backend), std::make_shared<RecyclerObjStorageRateLimitPolicy>());
        return 0;
#else
        LOG_FATAL("BE is not compiled with azure support, export BUILD_AZURE=ON before building");
        return 0;
#endif
    }
    default: {
        if (conf_.prefix.empty()) {
            uri_ = conf_.endpoint + '/' + conf_.bucket;
        } else {
            uri_ = conf_.endpoint + '/' + conf_.bucket + '/' + conf_.prefix;
        }
        uri_ = normalize_http_uri(uri_);

        // S3Conf::S3
        Aws::Client::ClientConfiguration aws_config = S3Environment::getClientConfiguration();
        aws_config.endpointOverride = conf_.endpoint;
        aws_config.region = conf_.region;
        // Aws::Http::CurlHandleContainer::AcquireCurlHandle() may be blocked if the connecitons are bottleneck
        aws_config.maxConnections = std::max((long)(config::recycle_pool_parallelism +
                                                    config::instance_recycler_worker_pool_size),
                                             (long)aws_config.maxConnections);

        if (config::s3_client_http_scheme == "http") {
            aws_config.scheme = Aws::Http::Scheme::HTTP;
        }
        // Recycler should fail fast on S3 SlowDown instead of retrying and blocking worker threads.
        aws_config.retryStrategy = std::make_shared<S3CustomRetryStrategy>(
                config::max_s3_client_retry, /*retry_slow_down=*/false);

        if (_ca_cert_file_path.empty()) {
            _ca_cert_file_path =
                    get_valid_ca_cert_path(doris::cloud::split(config::ca_cert_file_paths, ';'));
        }
        if (!_ca_cert_file_path.empty()) {
            aws_config.caFile = _ca_cert_file_path;
        }
        // Mirror BE PR #49315: default ClientConfiguration leaves requestTimeoutMs=3000,
        // which the vendored aws-sdk-cpp maps to CURLOPT_LOW_SPEED_TIME=3 and causes
        // curl error 28 on slow/large S3 DeleteObjects (OVH cold vault).
        aws_config.requestTimeoutMs = 30000;
        aws_config.connectTimeoutMs = 5000;
        auto credentials = create_aws_credentials_provider(conf_);
        if (!credentials) {
            LOG(WARNING) << "failed to create AWS credential provider: " << credentials.error;
            return -1;
        }
        auto s3_client = std::make_shared<Aws::S3::S3Client>(
                std::move(credentials.provider), std::move(aws_config),
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                conf_.use_virtual_addressing /* useVirtualAddressing */);
        auto backend = std::make_shared<S3ObjStorageBackend>(std::move(s3_client),
                                                             ObjectClientConfig {
                                                                     .endpoint = conf_.endpoint,
                                                                     .ak = conf_.ak,
                                                                     .sk = conf_.sk,
                                                             });
        obj_client_ = std::make_shared<ObjStorageClient>(
                std::move(backend), std::make_shared<RecyclerObjStorageRateLimitPolicy>());
        return 0;
    }
    }
}

int S3Accessor::delete_prefix_impl(const std::string& path_prefix, int64_t expiration_time) {
    LOG_INFO("delete prefix").tag("uri", to_uri(path_prefix));
    return obj_client_
            ->delete_objects_recursively(
                    {
                            .bucket = conf_.bucket,
                            .prefix = get_key(path_prefix),
                    },
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
    static_assert(ObjectStorageStatus::OK == 0);
    if (ret == ObjectStorageStatus::OK || ret == ObjectStorageStatus::NOT_FOUND) {
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
            obj_client_,
            ObjectStoragePathOptions {.bucket = conf_.bucket, .prefix = get_key(path_prefix)},
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
    if (response.status.code == ObjectStorageStatus::NOT_FOUND) {
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
    static_assert(ObjectStorageStatus::OK == 0);
    if (ret == ObjectStorageStatus::OK || ret == ObjectStorageStatus::NOT_FOUND) {
        return 0;
    }
    LOG_WARNING("fail abort multipart upload")
            .tag("uri", to_uri(path))
            .tag("upload_id", upload_id)
            .tag("ret", ret);
    return ret;
}

int S3Accessor::get_life_cycle(int64_t* expiration_days) {
    return obj_client_->get_life_cycle(conf_.bucket, expiration_days).status.code;
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
    ObjectListIterator iter(obj_client_, {.bucket = conf_.bucket, .prefix = get_key(path_prefix)});
    for (;;) {
        auto result = iter.next();
        if (!result.results_.has_value()) {
            if (result.resp.status.code != ObjectStorageStatus::NOT_FOUND &&
                result.resp.status.code != ObjectStorageStatus::OK) {
                ret = result.resp.status.code;
            }
            break;
        }
        auto& obj = *result.results_;
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
        int del_ret = obj_client_->delete_object({.bucket = conf_.bucket, .key = obj.file_path})
                              .status.code;
        del_nonexisted += (del_ret == ObjectStorageStatus::NOT_FOUND);
        static_assert(ObjectStorageStatus::OK == 0);
        if (del_ret != ObjectStorageStatus::OK && del_ret != ObjectStorageStatus::NOT_FOUND) {
            ret = del_ret;
        }
    }

    LOG_INFO("finish delete prefix")
            .tag("uri", to_uri(path_prefix))
            .tag("total_obj_cnt", cnt)
            .tag("deleted", del)
            .tag("del_nonexisted", del_nonexisted)
            .tag("skipped", skip);

    if (!iter.is_valid()) {
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
