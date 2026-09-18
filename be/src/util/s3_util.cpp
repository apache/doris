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

#include "util/s3_util.h"

#include <aws/core/auth/AWSAuthSigner.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/core/utils/logging/LogSystemInterface.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>

#include "util/string_util.h"

#ifdef USE_AZURE
#include <azure/core/diagnostics/logger.hpp>
#include <azure/core/http/curl_transport.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>
#endif
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <ostream>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "cpp/obj-client/auth/aws_credential_factory.h"
#ifdef USE_AZURE
#include "cpp/obj-client/auth/azure_auth_factory.h"
#include "cpp/obj-client/azure_obj_storage_client.h"
#endif
#include "cloud/config.h"
#include "cpp/aws_logger.h"
#include "cpp/obj-client/rate_limited_obj_storage_client.h"
#include "cpp/obj-client/s3_obj_storage_client.h"
#include "cpp/obj_retry_strategy.h"
#include "cpp/sync_point.h"
#include "cpp/util.h"
#include "exec/scan/scanner_scheduler.h"
#include "runtime/exec_env.h"
#include "util/s3_rate_limiter_manager.h"
#include "util/s3_uri.h"

namespace doris {
namespace {

doris::Status is_s3_conf_valid(const S3ClientConf& conf) {
    if (conf.endpoint.empty()) {
        return Status::InvalidArgument<false>("Invalid s3 conf, empty endpoint");
    }
    if (conf.region.empty()) {
        return Status::InvalidArgument<false>("Invalid s3 conf, empty region");
    }

    if (conf.role_arn.empty()) {
        // Allow anonymous access when both ak and sk are empty
        bool hasAk = !conf.ak.empty();
        bool hasSk = !conf.sk.empty();

        // Either both credentials are provided or both are empty (anonymous access)
        if (hasAk && conf.sk.empty()) {
            return Status::InvalidArgument<false>("Invalid s3 conf, empty sk");
        }
        if (hasSk && conf.ak.empty()) {
            return Status::InvalidArgument<false>("Invalid s3 conf, empty ak");
        }
    }
    return Status::OK();
}

ObjStorageResponse make_be_rate_limit_response(S3RateLimitType type,
                                               S3RateLimitRejectReason reason) {
    const auto* limit_type = reason == S3RateLimitRejectReason::QPS ? "QPS" : "bytes";
    // A local admission rejection is not an S3 HTTP 429. Keep the merged #65420 behavior so S3
    // readers do not retry it as provider throttling.
    return ObjStorageResponse::rate_limit(
            ErrorCode::EXCEEDED_LIMIT, 0,
            fmt::format("s3 {} request exceeds {} limit, rejected by BE rate limiter",
                        to_string(type), limit_type));
}

class BeObjStorageRateLimitPolicy final : public ObjStorageRateLimitPolicy {
public:
    ObjStorageAdmission acquire(S3RateLimitType type, size_t estimated_bytes) const override {
        auto guard = std::make_shared<S3RateLimitGuard>(type, estimated_bytes);
        if (!guard->ok()) {
            return ObjStorageAdmission {
                    .resp = make_be_rate_limit_response(type, guard->reject_reason()),
            };
        }
        return ObjStorageAdmission {
                .settle = [guard = std::move(guard)](
                                  size_t actual_bytes) { guard->settle(actual_bytes); },
        };
    }
};

// Return true is convert `str` to int successfully
bool to_int(std::string_view str, int& res) {
    auto [_, ec] = std::from_chars(str.data(), str.data() + str.size(), res);
    return ec == std::errc {};
}

#ifdef USE_AZURE
std::string env_or_empty(const char* env_name) {
    if (const char* value = std::getenv(env_name); value != nullptr) {
        return value;
    }
    return "";
}

std::string build_azure_tls_debug_context(const std::string& selected_ca_file) {
    bool selected_ca_exists = false;
    bool selected_ca_readable = false;
    if (!selected_ca_file.empty()) {
        std::error_code ec;
        selected_ca_exists = std::filesystem::exists(selected_ca_file, ec) && !ec;
        std::ifstream input(selected_ca_file);
        selected_ca_readable = input.good();
    }

    return fmt::format(
            "tls_debug(ca_cert_file_paths='{}', selected_ca_file='{}', selected_ca_exists={}, "
            "selected_ca_readable={}, SSL_CERT_FILE='{}', CURL_CA_BUNDLE='{}', SSL_CERT_DIR='{}')",
            config::ca_cert_file_paths, selected_ca_file, selected_ca_exists, selected_ca_readable,
            env_or_empty("SSL_CERT_FILE"), env_or_empty("CURL_CA_BUNDLE"),
            env_or_empty("SSL_CERT_DIR"));
}
#endif

constexpr char USE_PATH_STYLE[] = "use_path_style";

constexpr char AZURE_PROVIDER_STRING[] = "AZURE";
constexpr char S3_PROVIDER[] = "provider";
constexpr char S3_AK[] = "AWS_ACCESS_KEY";
constexpr char S3_SK[] = "AWS_SECRET_KEY";
constexpr char S3_ENDPOINT[] = "AWS_ENDPOINT";
constexpr char S3_REGION[] = "AWS_REGION";
constexpr char S3_TOKEN[] = "AWS_TOKEN";
constexpr char S3_MAX_CONN_SIZE[] = "AWS_MAX_CONNECTIONS";
constexpr char S3_REQUEST_TIMEOUT_MS[] = "AWS_REQUEST_TIMEOUT_MS";
constexpr char S3_CONN_TIMEOUT_MS[] = "AWS_CONNECTION_TIMEOUT_MS";
constexpr char S3_NEED_OVERRIDE_ENDPOINT[] = "AWS_NEED_OVERRIDE_ENDPOINT";

constexpr char S3_ROLE_ARN[] = "AWS_ROLE_ARN";
constexpr char S3_EXTERNAL_ID[] = "AWS_EXTERNAL_ID";
constexpr char S3_CREDENTIALS_PROVIDER_TYPE[] = "AWS_CREDENTIALS_PROVIDER_TYPE";
} // namespace

S3ClientFactory::S3ClientFactory() {
    _aws_options = Aws::SDKOptions {};
    auto logLevel = static_cast<Aws::Utils::Logging::LogLevel>(config::aws_log_level);
    _aws_options.loggingOptions.logLevel = logLevel;
    _aws_options.loggingOptions.logger_create_fn = [logLevel] {
        return std::make_shared<DorisAWSLogger>(logLevel);
    };
    Aws::InitAPI(_aws_options);
    _get_ca_cert_file_path();

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

S3ClientFactory::~S3ClientFactory() {
    Aws::ShutdownAPI(_aws_options);
}

S3ClientFactory& S3ClientFactory::instance() {
    static S3ClientFactory ret;
    return ret;
}

Result<std::shared_ptr<io::ObjStorageClient>> S3ClientFactory::create(const S3ClientConf& s3_conf) {
    RETURN_IF_ERROR_RESULT(is_s3_conf_valid(s3_conf));

#ifdef BE_TEST
    {
        std::lock_guard l(_lock);
        if (_test_client_creator) {
            return _test_client_creator(s3_conf);
        }
    }
#endif

    {
        std::lock_guard l(_lock);
        auto it = _cache.find(s3_conf);
        if (it != _cache.end()) {
            return it->second;
        }
    }

    auto client_result = (s3_conf.provider == io::ObjStorageProvider::AZURE)
                                 ? _create_azure_client(s3_conf)
                                 : _create_s3_client(s3_conf);
    if (!client_result.has_value()) {
        return ResultError(std::move(client_result).error());
    }
    auto obj_client = std::move(client_result).value();
    if (!config::is_cloud_mode() || s3_conf.is_internal_bucket) {
        obj_client = std::make_shared<io::RateLimitedObjStorageClient>(
                std::move(obj_client), std::make_shared<BeObjStorageRateLimitPolicy>());
    }

    {
        std::lock_guard l(_lock);
        auto [it, _] = _cache.emplace(s3_conf, std::move(obj_client));
        return it->second;
    }
}

#ifdef BE_TEST
void S3ClientFactory::set_client_creator_for_test(
        std::function<std::shared_ptr<io::ObjStorageClient>(const S3ClientConf&)> creator) {
    std::lock_guard l(_lock);
    _test_client_creator = std::move(creator);
}

void S3ClientFactory::clear_client_creator_for_test() {
    std::lock_guard l(_lock);
    _test_client_creator = nullptr;
}
#endif

Result<std::shared_ptr<io::ObjStorageClient>> S3ClientFactory::_create_azure_client(
        const S3ClientConf& s3_conf) {
#ifdef USE_AZURE
    const std::string container_name = s3_conf.bucket;
    std::string uri = fmt::format("{}/{}", s3_conf.endpoint, container_name);
    if (s3_conf.endpoint.find("://") == std::string::npos) {
        uri = "https://" + uri;
    }

    Azure::Storage::Blobs::BlobClientOptions options;
    options.Retry.StatusCodes.insert(Azure::Core::Http::HttpStatusCode::TooManyRequests);
    options.Retry.MaxRetries = config::max_s3_client_retry;
    options.PerRetryPolicies.emplace_back(std::make_unique<AzureRetryRecordPolicy>());
    auto ca_cert_file_path = _get_ca_cert_file_path();
    if (!ca_cert_file_path.empty()) {
        Azure::Core::Http::CurlTransportOptions curl_options;
        curl_options.CAInfo = ca_cert_file_path;
        options.Transport.Transport =
                std::make_shared<Azure::Core::Http::CurlTransport>(std::move(curl_options));
    }

    std::string normalized_uri = normalize_http_uri(uri);
    VLOG_DEBUG << "uri:" << uri << ", normalized_uri:" << normalized_uri;
    std::string tls_debug_context = build_azure_tls_debug_context(ca_cert_file_path);

    auto built = AzureAuthFactory::create(uri,
                                          {
                                                  .type = AzureCredentialType::SHARED_KEY,
                                                  .account_name = s3_conf.ak,
                                                  .account_key = s3_conf.sk,
                                          },
                                          std::move(options));
    if (!built) {
        return ResultError(
                Status::InvalidArgument("failed to create Azure client: {}", built.error));
    }
    LOG_INFO("create one azure client with {}", s3_conf.to_string());
    return std::make_shared<io::AzureObjStorageClient>(
            std::move(built.container_client),
            ObjStorageEndpointInfo {
                    .endpoint = s3_conf.endpoint,
                    .ak = s3_conf.ak,
                    .sk = s3_conf.sk,
                    .tls_debug_context = std::move(tls_debug_context),
            },
            std::move(built.shared_key_credential));
#else
    return ResultError(Status::NotSupported(
            "BE is not compiled with azure support, export BUILD_AZURE=ON before building"));
#endif
}

std::string S3ClientFactory::_get_ca_cert_file_path() {
    std::lock_guard lock(_ca_cert_lock);
    if (_ca_cert_file_path.empty()) {
        _ca_cert_file_path = get_valid_ca_cert_path(doris::split(config::ca_cert_file_paths, ";"));
    }
    return _ca_cert_file_path;
}

AwsCredentialResult S3ClientFactory::create_aws_credentials_provider(const S3ClientConf& s3_conf) {
    auto sts_config = S3ClientFactory::getClientConfiguration();
    auto ca_cert_file_path = _get_ca_cert_file_path();
    if (!ca_cert_file_path.empty()) {
        sts_config.caFile = ca_cert_file_path;
    }
    return AwsCredentialFactory::create({
            .version = config::aws_credentials_provider_version == "v2"
                               ? AwsCredentialProviderVersion::V2
                               : AwsCredentialProviderVersion::V1,
            .access_key = s3_conf.ak,
            .secret_key = s3_conf.sk,
            .session_token = s3_conf.token,
            .provider_type = s3_conf.cred_provider_type,
            .role_arn = s3_conf.role_arn,
            .external_id = s3_conf.external_id,
            .empty_credentials = EmptyCredentialsBehavior::ANONYMOUS,
            .sts_client_config = std::move(sts_config),
    });
}

Result<std::shared_ptr<io::ObjStorageClient>> S3ClientFactory::_create_s3_client(
        const S3ClientConf& s3_conf) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE(
            "s3_client_factory::create",
            std::make_shared<io::S3ObjStorageClient>(std::make_shared<Aws::S3::S3Client>(),
                                                     ObjStorageEndpointInfo {}));
    Aws::Client::ClientConfiguration aws_config = S3ClientFactory::getClientConfiguration();
    if (s3_conf.need_override_endpoint) {
        aws_config.endpointOverride = s3_conf.endpoint;
    }
    aws_config.region = s3_conf.region;

    auto ca_cert_file_path = _get_ca_cert_file_path();
    if (!ca_cert_file_path.empty()) {
        aws_config.caFile = ca_cert_file_path;
    }

    if (s3_conf.max_connections > 0) {
        aws_config.maxConnections = s3_conf.max_connections;
    } else {
        aws_config.maxConnections = 102400;
    }

    aws_config.requestTimeoutMs = 30000;
    if (s3_conf.request_timeout_ms > 0) {
        aws_config.requestTimeoutMs = s3_conf.request_timeout_ms;
    }

    if (s3_conf.connect_timeout_ms > 0) {
        aws_config.connectTimeoutMs = s3_conf.connect_timeout_ms;
    }

    set_s3_client_default_http_scheme(aws_config, config::s3_client_http_scheme);

    aws_config.retryStrategy = std::make_shared<S3CustomRetryStrategy>(
            config::max_s3_client_retry /*scaleFactor = 25*/, /*retry_slow_down=*/true);

    auto credentials = create_aws_credentials_provider(s3_conf);
    if (!credentials) {
        return ResultError(Status::InvalidArgument("failed to create AWS credential provider: {}",
                                                   credentials.error));
    }
    std::shared_ptr<Aws::S3::S3Client> new_client = std::make_shared<Aws::S3::S3Client>(
            std::move(credentials.provider), std::move(aws_config),
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            s3_conf.use_virtual_addressing);

    auto provider_client = std::make_shared<io::S3ObjStorageClient>(
            std::move(new_client), ObjStorageEndpointInfo {
                                           .endpoint = s3_conf.endpoint,
                                           .ak = s3_conf.ak,
                                           .sk = s3_conf.sk,
                                   });
    LOG_INFO("create one s3 client with {}", s3_conf.to_string());
    return provider_client;
}

Status S3ClientFactory::convert_properties_to_s3_conf(
        const std::map<std::string, std::string>& prop, const S3URI& s3_uri, S3Conf* s3_conf) {
    StringCaseMap<std::string> properties(prop.begin(), prop.end());
    if (auto it = properties.find(S3_AK); it != properties.end()) {
        s3_conf->client_conf.ak = it->second;
    }
    if (auto it = properties.find(S3_SK); it != properties.end()) {
        s3_conf->client_conf.sk = it->second;
    }
    if (auto it = properties.find(S3_TOKEN); it != properties.end()) {
        s3_conf->client_conf.token = it->second;
    }
    if (auto it = properties.find(S3_ENDPOINT); it != properties.end()) {
        s3_conf->client_conf.endpoint = it->second;
    }
    if (auto it = properties.find(S3_NEED_OVERRIDE_ENDPOINT); it != properties.end()) {
        s3_conf->client_conf.need_override_endpoint = (it->second == "true");
    }
    if (auto it = properties.find(S3_REGION); it != properties.end()) {
        s3_conf->client_conf.region = it->second;
    }
    if (auto it = properties.find(S3_MAX_CONN_SIZE); it != properties.end()) {
        if (!to_int(it->second, s3_conf->client_conf.max_connections)) {
            return Status::InvalidArgument("invalid {} value \"{}\"", S3_MAX_CONN_SIZE, it->second);
        }
    }
    if (auto it = properties.find(S3_REQUEST_TIMEOUT_MS); it != properties.end()) {
        if (!to_int(it->second, s3_conf->client_conf.request_timeout_ms)) {
            return Status::InvalidArgument("invalid {} value \"{}\"", S3_REQUEST_TIMEOUT_MS,
                                           it->second);
        }
    }
    if (auto it = properties.find(S3_CONN_TIMEOUT_MS); it != properties.end()) {
        if (!to_int(it->second, s3_conf->client_conf.connect_timeout_ms)) {
            return Status::InvalidArgument("invalid {} value \"{}\"", S3_CONN_TIMEOUT_MS,
                                           it->second);
        }
    }
    if (auto it = properties.find(S3_PROVIDER); it != properties.end()) {
        // S3 Provider properties should be case insensitive.
        if (0 == strcasecmp(it->second.c_str(), AZURE_PROVIDER_STRING)) {
            s3_conf->client_conf.provider = io::ObjStorageProvider::AZURE;
        }
    }

    if (s3_uri.get_bucket().empty()) {
        return Status::InvalidArgument("Invalid S3 URI {}, bucket is not specified",
                                       s3_uri.to_string());
    }
    s3_conf->bucket = s3_uri.get_bucket();
    // For azure's compatibility
    s3_conf->client_conf.bucket = s3_uri.get_bucket();
    s3_conf->prefix = "";

    // See https://sdk.amazonaws.com/cpp/api/LATEST/class_aws_1_1_s3_1_1_s3_client.html
    s3_conf->client_conf.use_virtual_addressing = true;
    if (auto it = properties.find(USE_PATH_STYLE); it != properties.end()) {
        s3_conf->client_conf.use_virtual_addressing = it->second != "true";
    }

    if (auto it = properties.find(S3_ROLE_ARN); it != properties.end()) {
        // Keep provider type as Default unless explicitly configured by
        // AWS_CREDENTIALS_PROVIDER_TYPE, consistent with FE behavior.
        s3_conf->client_conf.role_arn = it->second;
    }

    if (auto it = properties.find(S3_EXTERNAL_ID); it != properties.end()) {
        s3_conf->client_conf.external_id = it->second;
    }

    if (auto it = properties.find(S3_CREDENTIALS_PROVIDER_TYPE); it != properties.end()) {
        s3_conf->client_conf.cred_provider_type = cred_provider_type_from_string(it->second);
    }

    if (auto st = is_s3_conf_valid(s3_conf->client_conf); !st.ok()) {
        return st;
    }
    return Status::OK();
}

static CredProviderType cred_provider_type_from_thrift(TCredProviderType::type cred_provider_type) {
    switch (cred_provider_type) {
    case TCredProviderType::DEFAULT:
        return CredProviderType::Default;
    case TCredProviderType::SIMPLE:
        return CredProviderType::Simple;
    case TCredProviderType::INSTANCE_PROFILE:
        return CredProviderType::InstanceProfile;
    case TCredProviderType::ENV:
        return CredProviderType::Env;
    case TCredProviderType::SYSTEM_PROPERTIES:
        return CredProviderType::SystemProperties;
    case TCredProviderType::WEB_IDENTITY:
        return CredProviderType::WebIdentity;
    case TCredProviderType::CONTAINER:
        return CredProviderType::Container;
    case TCredProviderType::ANONYMOUS:
        return CredProviderType::Anonymous;
    default:
        __builtin_unreachable();
        LOG(WARNING) << "Invalid TCredProviderType value: " << cred_provider_type
                     << ", use default instead.";
        return CredProviderType::Default;
    }
}

S3Conf S3Conf::get_s3_conf(const cloud::ObjectStoreInfoPB& info) {
    S3Conf ret {
            .bucket = info.bucket(),
            .prefix = info.prefix(),
            .client_conf {
                    .endpoint = info.endpoint(),
                    .region = info.region(),
                    .ak = info.ak(),
                    .sk = info.sk(),
                    .token = {},
                    .bucket = info.bucket(),
                    .provider = io::ObjStorageProvider::AWS,
                    .use_virtual_addressing =
                            info.has_use_path_style() ? !info.use_path_style() : true,

                    .role_arn = info.role_arn(),
                    .external_id = info.external_id(),
                    // ObjectStoreInfoPB always describes a storage vault, i.e. a Doris
                    // internal bucket in cloud mode.
                    .is_internal_bucket = true,
            },
            .sse_enabled = info.sse_enabled(),
    };

    if (info.has_cred_provider_type()) {
        ret.client_conf.cred_provider_type = cred_provider_type_from_pb(info.cred_provider_type());
    }

    io::ObjStorageProvider type = io::ObjStorageProvider::AWS;
    switch (info.provider()) {
    case cloud::ObjectStoreInfoPB_Provider_OSS:
        type = io::ObjStorageProvider::OSS;
        break;
    case cloud::ObjectStoreInfoPB_Provider_S3:
        type = io::ObjStorageProvider::AWS;
        break;
    case cloud::ObjectStoreInfoPB_Provider_COS:
        type = io::ObjStorageProvider::COS;
        break;
    case cloud::ObjectStoreInfoPB_Provider_OBS:
        type = io::ObjStorageProvider::OBS;
        break;
    case cloud::ObjectStoreInfoPB_Provider_BOS:
        type = io::ObjStorageProvider::BOS;
        break;
    case cloud::ObjectStoreInfoPB_Provider_GCP:
        type = io::ObjStorageProvider::GCP;
        break;
    case cloud::ObjectStoreInfoPB_Provider_AZURE:
        type = io::ObjStorageProvider::AZURE;
        break;
    case cloud::ObjectStoreInfoPB_Provider_TOS:
        type = io::ObjStorageProvider::TOS;
        break;
    default:
        __builtin_unreachable();
        LOG_FATAL("unknown provider type {}, info {}", info.provider(), ret.to_string());
    }
    ret.client_conf.provider = type;
    return ret;
}

S3Conf S3Conf::get_s3_conf(const TS3StorageParam& param) {
    S3Conf ret {
            .bucket = param.bucket,
            .prefix = param.root_path,
            .client_conf = {
                    .endpoint = param.endpoint,
                    .region = param.region,
                    .ak = param.ak,
                    .sk = param.sk,
                    .token = param.token,
                    .bucket = param.bucket,
                    .provider = io::ObjStorageProvider::AWS,
                    .max_connections = param.max_conn,
                    .request_timeout_ms = param.request_timeout_ms,
                    .connect_timeout_ms = param.conn_timeout_ms,
                    // When using cold heat separation in minio, user might use ip address directly,
                    // which needs enable use_virtual_addressing to true
                    .use_virtual_addressing = !param.use_path_style,
                    .role_arn = param.role_arn,
                    .external_id = param.external_id,
            }};

    if (param.__isset.cred_provider_type) {
        ret.client_conf.cred_provider_type =
                cred_provider_type_from_thrift(param.cred_provider_type);
    }

    io::ObjStorageProvider type = io::ObjStorageProvider::AWS;
    switch (param.provider) {
    case TObjStorageType::UNKNOWN:
        LOG_INFO("Receive one legal storage resource, set provider type to aws, param detail {}",
                 ret.to_string());
        type = io::ObjStorageProvider::AWS;
        break;
    case TObjStorageType::AWS:
        type = io::ObjStorageProvider::AWS;
        break;
    case TObjStorageType::AZURE:
        type = io::ObjStorageProvider::AZURE;
        break;
    case TObjStorageType::BOS:
        type = io::ObjStorageProvider::BOS;
        break;
    case TObjStorageType::COS:
        type = io::ObjStorageProvider::COS;
        break;
    case TObjStorageType::OBS:
        type = io::ObjStorageProvider::OBS;
        break;
    case TObjStorageType::OSS:
        type = io::ObjStorageProvider::OSS;
        break;
    case TObjStorageType::GCP:
        type = io::ObjStorageProvider::GCP;
        break;
    case TObjStorageType::TOS:
        type = io::ObjStorageProvider::TOS;
        break;
    default:
        LOG_FATAL("unknown provider type {}, info {}", param.provider, ret.to_string());
        __builtin_unreachable();
    }
    ret.client_conf.provider = type;
    return ret;
}

std::string hide_access_key(const std::string& ak) {
    std::string key = ak;
    size_t key_len = key.length();
    size_t reserved_count;
    if (key_len > 7) {
        reserved_count = 6;
    } else if (key_len > 2) {
        reserved_count = key_len - 2;
    } else {
        reserved_count = 0;
    }

    size_t x_count = key_len - reserved_count;
    size_t left_x_count = (x_count + 1) / 2;

    if (left_x_count > 0) {
        key.replace(0, left_x_count, left_x_count, 'x');
    }

    if (x_count - left_x_count > 0) {
        key.replace(key_len - (x_count - left_x_count), x_count - left_x_count,
                    x_count - left_x_count, 'x');
    }
    return key;
}

} // end namespace doris
