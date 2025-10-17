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
#include <aws/identity-management/auth/STSAssumeRoleCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/sts/STSClient.h>
#include <bvar/reducer.h>
#include <gen_cpp/Status_types.h>
#include <gen_cpp/cloud.pb.h>

#include <algorithm>

#include "meta-store/txn_kv_error.h"
#include "recycler/sync_executor.h"

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
#include "cpp/aws_logger.h"
#include "cpp/custom_aws_credentials_provider_chain.h"
#include "cpp/obj_retry_strategy.h"
#include "cpp/s3_rate_limiter.h"
#include "cpp/sync_point.h"
#include "cpp/util.h"
#ifdef USE_AZURE
#include "client/azure_obj_storage_client.h"
#endif
#include "client/obj_storage_client.h"
#include "client/s3_obj_storage_client.h"
#include "recycler/storage_vault_accessor.h"

using namespace std::chrono;

namespace doris::cloud {

bvar::Adder<int64_t> get_rate_limit_ns("get_rate_limit_ns");
bvar::Adder<int64_t> get_rate_limit_exceed_req_num("get_rate_limit_exceed_req_num");
bvar::Adder<int64_t> put_rate_limit_ns("put_rate_limit_ns");
bvar::Adder<int64_t> put_rate_limit_exceed_req_num("put_rate_limit_exceed_req_num");

AccessorRateLimiter::AccessorRateLimiter()
        : _rate_limiters(
                  {std::make_unique<S3RateLimiterHolder>(
                           config::s3_get_token_per_second, config::s3_get_bucket_tokens,
                           config::s3_get_token_limit,
                           metric_func_factory(get_rate_limit_ns, get_rate_limit_exceed_req_num)),
                   std::make_unique<S3RateLimiterHolder>(
                           config::s3_put_token_per_second, config::s3_put_bucket_tokens,
                           config::s3_put_token_limit,
                           metric_func_factory(put_rate_limit_ns,
                                               put_rate_limit_exceed_req_num))}) {}

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
    S3ListIterator(std::unique_ptr<ObjectListIterator> iter, size_t prefix_length)
            : iter_(std::move(iter)), prefix_length_(prefix_length) {}

    ~S3ListIterator() override = default;

    bool is_valid() override { return iter_->is_valid(); }

    ObjectStorageResponse has_next() override { return iter_->has_next(); }

    std::optional<FileMeta> next() override {
        std::optional<FileMeta> ret;
        if (auto obj = iter_->next(); obj.resp.status.code == TStatusCode::OK) {
            ret = FileMeta {
                    .path = obj.results_->file_path,
                    .size = obj.results_->size,
                    .mtime_s = obj.results_->mtime_s,
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

        if (obj_info.has_role_arn() && !obj_info.role_arn().empty()) {
            s3_conf.role_arn = obj_info.role_arn();
            s3_conf.external_id = obj_info.external_id();
            s3_conf.cred_provider_type = CredProviderType::InstanceProfile;
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
static constexpr size_t MaxDeleteBatch = 1000;

std::shared_ptr<Aws::Auth::AWSCredentialsProvider> S3Accessor::_get_aws_credentials_provider_v1(
        const S3Conf& s3_conf) {
    if (!s3_conf.ak.empty() && !s3_conf.sk.empty()) {
        Aws::Auth::AWSCredentials aws_cred(s3_conf.ak, s3_conf.sk);
        DCHECK(!aws_cred.IsExpiredOrEmpty());
        return std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(std::move(aws_cred));
    }

    if (s3_conf.cred_provider_type == CredProviderType::InstanceProfile) {
        if (s3_conf.role_arn.empty()) {
            return std::make_shared<Aws::Auth::InstanceProfileCredentialsProvider>();
        }

        Aws::Client::ClientConfiguration clientConfiguration =
                S3Environment::getClientConfiguration();
        if (_ca_cert_file_path.empty()) {
            _ca_cert_file_path =
                    get_valid_ca_cert_path(doris::cloud::split(config::ca_cert_file_paths, ';'));
        }
        if (!_ca_cert_file_path.empty()) {
            clientConfiguration.caFile = _ca_cert_file_path;
        }

        auto stsClient = std::make_shared<Aws::STS::STSClient>(
                std::make_shared<Aws::Auth::InstanceProfileCredentialsProvider>(),
                clientConfiguration);

        return std::make_shared<Aws::Auth::STSAssumeRoleCredentialsProvider>(
                s3_conf.role_arn, Aws::String(), s3_conf.external_id,
                Aws::Auth::DEFAULT_CREDS_LOAD_FREQ_SECONDS, stsClient);
    }
    return std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
}

std::shared_ptr<Aws::Auth::AWSCredentialsProvider> S3Accessor::_get_aws_credentials_provider_v2(
        const S3Conf& s3_conf) {
    if (!s3_conf.ak.empty() && !s3_conf.sk.empty()) {
        Aws::Auth::AWSCredentials aws_cred(s3_conf.ak, s3_conf.sk);
        DCHECK(!aws_cred.IsExpiredOrEmpty());
        return std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(std::move(aws_cred));
    }

    if (s3_conf.cred_provider_type == CredProviderType::InstanceProfile) {
        if (s3_conf.role_arn.empty()) {
            return std::make_shared<CustomAwsCredentialsProviderChain>();
        }

        Aws::Client::ClientConfiguration clientConfiguration =
                S3Environment::getClientConfiguration();
        if (_ca_cert_file_path.empty()) {
            _ca_cert_file_path =
                    get_valid_ca_cert_path(doris::cloud::split(config::ca_cert_file_paths, ';'));
        }
        if (!_ca_cert_file_path.empty()) {
            clientConfiguration.caFile = _ca_cert_file_path;
        }

        auto stsClient = std::make_shared<Aws::STS::STSClient>(
                std::make_shared<CustomAwsCredentialsProviderChain>(), clientConfiguration);

        return std::make_shared<Aws::Auth::STSAssumeRoleCredentialsProvider>(
                s3_conf.role_arn, Aws::String(), s3_conf.external_id,
                Aws::Auth::DEFAULT_CREDS_LOAD_FREQ_SECONDS, stsClient);
    }
    return std::make_shared<CustomAwsCredentialsProviderChain>();
}

std::shared_ptr<Aws::Auth::AWSCredentialsProvider> S3Accessor::get_aws_credentials_provider(
        const S3Conf& s3_conf) {
    if (config::aws_credentials_provider_version == "v2") {
        return _get_aws_credentials_provider_v2(s3_conf);
    }
    return _get_aws_credentials_provider_v1(s3_conf);
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
        options.Retry.StatusCodes.insert(Azure::Core::Http::HttpStatusCode::TooManyRequests);
        options.Retry.MaxRetries = config::max_s3_client_retry;
        auto cred =
                std::make_shared<Azure::Storage::StorageSharedKeyCredential>(conf_.ak, conf_.sk);
        if (config::force_azure_blob_global_endpoint) {
            uri_ = fmt::format("https://{}.blob.core.windows.net/{}", conf_.ak, conf_.bucket);
        } else {
            uri_ = fmt::format("{}/{}", conf_.endpoint, conf_.bucket);
            if (uri_.find("://") == std::string::npos) {
                uri_ = "https://" + uri_;
            }
        }
        uri_ = normalize_http_uri(uri_);
        // In Azure's HTTP requests, all policies in the vector are called in a chained manner following the HTTP pipeline approach.
        // Within the RetryPolicy, the nextPolicy is called multiple times inside a loop.
        // All policies in the PerRetryPolicies are downstream of the RetryPolicy.
        // Therefore, you only need to add a policy to check if the response code is 429 and if the retry count meets the condition, it can record the retry count.
        options.PerRetryPolicies.emplace_back(std::make_unique<AzureRetryRecordPolicy>());
        auto container_client = std::make_shared<Azure::Storage::Blobs::BlobContainerClient>(
                uri_, cred, std::move(options));
        // uri format for debug: ${scheme}://${ak}.blob.core.windows.net/${bucket}/${prefix}
        uri_ = normalize_http_uri(uri_ + '/' + conf_.prefix);
        obj_client_ = std::make_shared<AzureObjStorageClient>(
                std::move(container_client),
                ObjectClientConfig {.endpoint = conf_.endpoint, .ak = conf_.ak, .sk = conf_.sk});
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
        aws_config.retryStrategy = std::make_shared<S3CustomRetryStrategy>(
                config::max_s3_client_retry /*scaleFactor = 25*/);

        if (_ca_cert_file_path.empty()) {
            _ca_cert_file_path =
                    get_valid_ca_cert_path(doris::cloud::split(config::ca_cert_file_paths, ';'));
        }
        if (!_ca_cert_file_path.empty()) {
            aws_config.caFile = _ca_cert_file_path;
        }
        auto s3_client = std::make_shared<Aws::S3::S3Client>(
                get_aws_credentials_provider(conf_), std::move(aws_config),
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                conf_.use_virtual_addressing /* useVirtualAddressing */);
        obj_client_ = std::make_shared<S3ObjStorageClient>(
                std::move(s3_client),
                ObjectClientConfig {.endpoint = conf_.endpoint, .ak = conf_.ak, .sk = conf_.sk});
        return 0;
    }
    }
}

int S3Accessor::delete_prefix_impl(const std::string& path_prefix, int64_t expiration_time) {
    LOG_INFO("delete prefix").tag("uri", to_uri(path_prefix));
    size_t num_deleted_objects = 0;
    auto start_time = steady_clock::now();

    auto list_iter = std::make_unique<S3ListIterator>(
            obj_client_->list_objects({.bucket = conf_.bucket, .key = get_key(path_prefix)}),
            path_prefix.length() + 1 /* {prefix}/ */);

    int ret = 0;
    std::vector<std::string> keys;
    SyncExecutor<ObjectStorageResponse> concurrent_delete_executor(
            worker_pool,
            fmt::format("delete objects under bucket {}, path {}", conf_.bucket, conf_.prefix),
            [](const ObjectStorageResponse& ret) { return ret.status.code != 0; });

    for (auto obj = list_iter->next(); obj.has_value(); obj = list_iter->next()) {
        if (expiration_time > 0 && obj->mtime_s > expiration_time) {
            continue;
        }

        num_deleted_objects++;
        keys.emplace_back(std::move(obj->path));
        if (keys.size() < MaxDeleteBatch) {
            continue;
        }
        concurrent_delete_executor.add(
                [this, k = std::move(keys), relative_path = path_prefix]() mutable {
                    return obj_client_->delete_objects(
                            {.bucket = conf_.bucket, .key = get_key(relative_path)}, std::move(k));
                });
    }

    if (!list_iter->is_valid()) {
        bool finished;
        concurrent_delete_executor.when_all(&finished);
        return -1;
    }

    if (!keys.empty()) {
        concurrent_delete_executor.add(
                [this, k = std::move(keys), relative_path = path_prefix]() mutable {
                    return obj_client_->delete_objects(
                            {.bucket = conf_.bucket, .key = get_key(relative_path)}, std::move(k));
                });
    }
    bool finished = true;
    std::vector<ObjectStorageResponse> rets = concurrent_delete_executor.when_all(&finished);
    for (const auto& r : rets) {
        if (r.status.code != 0) {
            ret = -1;
        }
    }

    auto elapsed = duration_cast<milliseconds>(steady_clock::now() - start_time).count();
    LOG(INFO) << "delete objects under " << conf_.bucket << "/" << get_key(path_prefix)
              << " finished, ret=" << ret << ", finished=" << finished
              << ", num_deleted_objects=" << num_deleted_objects << ", cost=" << elapsed << " ms";

    ret = finished ? ret : -1;

    return ret;
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

    // return obj_client_->delete_objects(conf_.bucket, std::move(keys), {.executor = worker_pool})
    //         .status.code;
    // TODO: accessor work pool batch delete
    return 0;
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
            obj_client_->list_objects({.full_path = conf_.bucket, .bucket = get_key(path_prefix)}),
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
    return obj_client_->head_object({.bucket = conf_.bucket, .key = get_key(path)}).file_size;
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
    return obj_client_->get_life_cycle(conf_.endpoint, conf_.bucket, expiration_days).status.code;
}

int S3Accessor::check_versioning() {
    return obj_client_->check_versioning(conf_.endpoint, conf_.bucket).status.code;
}

int GcsAccessor::delete_prefix_impl(const std::string& path_prefix, int64_t expiration_time) {
    LOG_INFO("begin delete prefix").tag("uri", to_uri(path_prefix));

    int ret = 0;
    // TODO: wyx chore
    int cnt = 0;
    int skip = 0;
    int64_t del_nonexisted = 0;
    int del = 0;
    auto iter =
            obj_client_->list_objects({.full_path = conf_.bucket, .bucket = get_key(path_prefix)});
    do {
        auto list_result = iter->next();
        if (list_result.resp.status.code != TStatusCode::OK) {
            ret = list_result.resp.status.code;
            break;
        }
        auto obj = list_result.results_.value();
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
    } while (iter->is_valid());

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
