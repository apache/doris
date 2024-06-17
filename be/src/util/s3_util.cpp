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
#include <bvar/reducer.h>
#include <util/string_util.h>

#include <atomic>
#include <azure/storage/blobs/blob_container_client.hpp>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <memory>
#include <ostream>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/sync_point.h"
#include "io/fs/azure_obj_storage_client.h"
#include "io/fs/obj_storage_client.h"
#include "io/fs/s3_obj_storage_client.h"
#include "runtime/exec_env.h"
#include "s3_uri.h"
#include "vec/exec/scan/scanner_scheduler.h"

namespace doris {
namespace s3_bvar {
bvar::LatencyRecorder s3_get_latency("s3_get");
bvar::LatencyRecorder s3_put_latency("s3_put");
bvar::LatencyRecorder s3_delete_latency("s3_delete");
bvar::LatencyRecorder s3_head_latency("s3_head");
bvar::LatencyRecorder s3_multi_part_upload_latency("s3_multi_part_upload");
bvar::LatencyRecorder s3_list_latency("s3_list");
bvar::LatencyRecorder s3_list_object_versions_latency("s3_list_object_versions");
bvar::LatencyRecorder s3_get_bucket_version_latency("s3_get_bucket_version");
bvar::LatencyRecorder s3_copy_object_latency("s3_copy_object");
}; // namespace s3_bvar

namespace {

bool is_s3_conf_valid(const S3ClientConf& conf) {
    return !conf.endpoint.empty() && !conf.region.empty() && !conf.ak.empty() && !conf.sk.empty();
}

// Return true is convert `str` to int successfully
bool to_int(std::string_view str, int& res) {
    auto [_, ec] = std::from_chars(str.data(), str.data() + str.size(), res);
    return ec == std::errc {};
}

const std::string USE_PATH_STYLE = "use_path_style";

} // namespace
S3RateLimiterHolder* S3ClientFactory::rate_limiter(S3RateLimitType type) {
    CHECK(type == S3RateLimitType::GET || type == S3RateLimitType::PUT) << to_string(type);
    return _rate_limiters[static_cast<size_t>(type)].get();
}

class DorisAWSLogger final : public Aws::Utils::Logging::LogSystemInterface {
public:
    DorisAWSLogger() : _log_level(Aws::Utils::Logging::LogLevel::Info) {}
    DorisAWSLogger(Aws::Utils::Logging::LogLevel log_level) : _log_level(log_level) {}
    ~DorisAWSLogger() final = default;
    Aws::Utils::Logging::LogLevel GetLogLevel() const final { return _log_level; }
    void Log(Aws::Utils::Logging::LogLevel log_level, const char* tag, const char* format_str,
             ...) final {
        _log_impl(log_level, tag, format_str);
    }
    void LogStream(Aws::Utils::Logging::LogLevel log_level, const char* tag,
                   const Aws::OStringStream& message_stream) final {
        _log_impl(log_level, tag, message_stream.str().c_str());
    }

    void Flush() final {}

private:
    void _log_impl(Aws::Utils::Logging::LogLevel log_level, const char* tag, const char* message) {
        switch (log_level) {
        case Aws::Utils::Logging::LogLevel::Off:
            break;
        case Aws::Utils::Logging::LogLevel::Fatal:
            LOG(FATAL) << "[" << tag << "] " << message;
            break;
        case Aws::Utils::Logging::LogLevel::Error:
            LOG(ERROR) << "[" << tag << "] " << message;
            break;
        case Aws::Utils::Logging::LogLevel::Warn:
            LOG(WARNING) << "[" << tag << "] " << message;
            break;
        case Aws::Utils::Logging::LogLevel::Info:
            LOG(INFO) << "[" << tag << "] " << message;
            break;
        case Aws::Utils::Logging::LogLevel::Debug:
            LOG(INFO) << "[" << tag << "] " << message;
            break;
        case Aws::Utils::Logging::LogLevel::Trace:
            LOG(INFO) << "[" << tag << "] " << message;
            break;
        default:
            break;
        }
    }

    std::atomic<Aws::Utils::Logging::LogLevel> _log_level;
};

S3ClientFactory::S3ClientFactory() {
    _aws_options = Aws::SDKOptions {};
    auto logLevel = static_cast<Aws::Utils::Logging::LogLevel>(config::aws_log_level);
    _aws_options.loggingOptions.logLevel = logLevel;
    _aws_options.loggingOptions.logger_create_fn = [logLevel] {
        return std::make_shared<DorisAWSLogger>(logLevel);
    };
    Aws::InitAPI(_aws_options);
    _ca_cert_file_path = get_valid_ca_cert_path();
}

string S3ClientFactory::get_valid_ca_cert_path() {
    vector<std::string> vec_ca_file_path = doris::split(config::ca_cert_file_paths, ";");
    auto it = vec_ca_file_path.begin();
    for (; it != vec_ca_file_path.end(); ++it) {
        if (std::filesystem::exists(*it)) {
            return *it;
        }
    }
    return "";
}

S3ClientFactory::~S3ClientFactory() {
    Aws::ShutdownAPI(_aws_options);
}

S3ClientFactory& S3ClientFactory::instance() {
    static S3ClientFactory ret;
    return ret;
}

std::shared_ptr<io::ObjStorageClient> S3ClientFactory::create(const S3ClientConf& s3_conf) {
    if (!is_s3_conf_valid(s3_conf)) {
        return nullptr;
    }

    uint64_t hash = s3_conf.get_hash();
    {
        std::lock_guard l(_lock);
        auto it = _cache.find(hash);
        if (it != _cache.end()) {
            return it->second;
        }
    }

    auto obj_client = (s3_conf.provider == io::ObjStorageType::AZURE)
                              ? _create_azure_client(s3_conf)
                              : _create_s3_client(s3_conf);

    {
        uint64_t hash = s3_conf.get_hash();
        std::lock_guard l(_lock);
        _cache[hash] = obj_client;
    }
    return obj_client;
}

std::shared_ptr<io::ObjStorageClient> S3ClientFactory::_create_azure_client(
        const S3ClientConf& s3_conf) {
    auto cred =
            std::make_shared<Azure::Storage::StorageSharedKeyCredential>(s3_conf.ak, s3_conf.sk);

    const std::string container_name = s3_conf.bucket;
    const std::string uri = fmt::format("{}://{}.blob.core.windows.net/{}",
                                        config::s3_client_http_scheme, s3_conf.ak, container_name);

    auto containerClient = std::make_shared<Azure::Storage::Blobs::BlobContainerClient>(uri, cred);
    return std::make_shared<io::AzureObjStorageClient>(std::move(containerClient));
}

std::shared_ptr<io::ObjStorageClient> S3ClientFactory::_create_s3_client(
        const S3ClientConf& s3_conf) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE(
            "s3_client_factory::create",
            std::make_shared<io::S3ObjStorageClient>(std::make_shared<Aws::S3::S3Client>()));
    Aws::Client::ClientConfiguration aws_config = S3ClientFactory::getClientConfiguration();
    aws_config.endpointOverride = s3_conf.endpoint;
    aws_config.region = s3_conf.region;
    std::string ca_cert = get_valid_ca_cert_path();
    if ("" != _ca_cert_file_path) {
        aws_config.caFile = _ca_cert_file_path;
    } else {
        // config::ca_cert_file_paths is valmutable,get newest value if file path invaild
        _ca_cert_file_path = get_valid_ca_cert_path();
        if ("" != _ca_cert_file_path) {
            aws_config.caFile = _ca_cert_file_path;
        }
    }
    if (s3_conf.max_connections > 0) {
        aws_config.maxConnections = s3_conf.max_connections;
    } else {
#ifdef BE_TEST
        // the S3Client may shared by many threads.
        // So need to set the number of connections large enough.
        aws_config.maxConnections = config::doris_scanner_thread_pool_thread_num;
#else
        aws_config.maxConnections =
                ExecEnv::GetInstance()->scanner_scheduler()->remote_thread_pool_max_size();
#endif
    }

    if (s3_conf.request_timeout_ms > 0) {
        aws_config.requestTimeoutMs = s3_conf.request_timeout_ms;
    }
    if (s3_conf.connect_timeout_ms > 0) {
        aws_config.connectTimeoutMs = s3_conf.connect_timeout_ms;
    }

    if (config::s3_client_http_scheme == "http") {
        aws_config.scheme = Aws::Http::Scheme::HTTP;
    }

    aws_config.retryStrategy =
            std::make_shared<Aws::Client::DefaultRetryStrategy>(config::max_s3_client_retry);
    std::shared_ptr<Aws::S3::S3Client> new_client;
    if (!s3_conf.ak.empty() && !s3_conf.sk.empty()) {
        Aws::Auth::AWSCredentials aws_cred(s3_conf.ak, s3_conf.sk);
        DCHECK(!aws_cred.IsExpiredOrEmpty());
        if (!s3_conf.token.empty()) {
            aws_cred.SetSessionToken(s3_conf.token);
        }
        new_client = std::make_shared<Aws::S3::S3Client>(
                std::move(aws_cred), std::move(aws_config),
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                s3_conf.use_virtual_addressing);
    } else {
        std::shared_ptr<Aws::Auth::AWSCredentialsProvider> aws_provider_chain =
                std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
        new_client = std::make_shared<Aws::S3::S3Client>(
                std::move(aws_provider_chain), std::move(aws_config),
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                s3_conf.use_virtual_addressing);
    }

    auto obj_client = std::make_shared<io::S3ObjStorageClient>(std::move(new_client));
    return obj_client;
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

    if (s3_uri.get_bucket().empty()) {
        return Status::InvalidArgument("Invalid S3 URI {}, bucket is not specified",
                                       s3_uri.to_string());
    }
    s3_conf->bucket = s3_uri.get_bucket();
    s3_conf->prefix = "";

    // See https://sdk.amazonaws.com/cpp/api/LATEST/class_aws_1_1_s3_1_1_s3_client.html
    s3_conf->client_conf.use_virtual_addressing = true;
    if (auto it = properties.find(USE_PATH_STYLE); it != properties.end()) {
        s3_conf->client_conf.use_virtual_addressing = it->second != "true";
    }

    if (!is_s3_conf_valid(s3_conf->client_conf)) {
        return Status::InvalidArgument("S3 properties are incorrect, please check properties.");
    }
    return Status::OK();
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
                    .bucket = info.bucket(),
                    .provider = io::ObjStorageType::AWS,
            },
            .sse_enabled = info.sse_enabled(),
    };

    io::ObjStorageType type = io::ObjStorageType::AWS;
    switch (info.provider()) {
    case cloud::ObjectStoreInfoPB_Provider_OSS:
        type = io::ObjStorageType::OSS;
        break;
    case cloud::ObjectStoreInfoPB_Provider_S3:
        type = io::ObjStorageType::AWS;
        break;
    case cloud::ObjectStoreInfoPB_Provider_COS:
        type = io::ObjStorageType::COS;
        break;
    case cloud::ObjectStoreInfoPB_Provider_OBS:
        type = io::ObjStorageType::OBS;
        break;
    case cloud::ObjectStoreInfoPB_Provider_BOS:
        type = io::ObjStorageType::BOS;
        break;
    case cloud::ObjectStoreInfoPB_Provider_GCP:
        type = io::ObjStorageType::GCP;
        break;
    case cloud::ObjectStoreInfoPB_Provider_AZURE:
        type = io::ObjStorageType::AZURE;
        break;
    default:
        LOG_FATAL("unknown provider type {}, info {}", info.provider(), ret.to_string());
        __builtin_unreachable();
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
                    .provider = io::ObjStorageType::AWS,
                    .max_connections = param.max_conn,
                    .request_timeout_ms = param.request_timeout_ms,
                    .connect_timeout_ms = param.conn_timeout_ms,
                    // When using cold heat separation in minio, user might use ip address directly,
                    // which needs enable use_virtual_addressing to true
                    .use_virtual_addressing = !param.use_path_style,
            }};
    io::ObjStorageType type = io::ObjStorageType::AWS;
    switch (param.provider) {
    case TObjStorageType::UNKNOWN:
        LOG_INFO("Receive one legal storage resource, set provider type to aws, param detail {}",
                 ret.to_string());
        type = io::ObjStorageType::AWS;
        break;
    case TObjStorageType::AWS:
        type = io::ObjStorageType::AWS;
        break;
    case TObjStorageType::AZURE:
        type = io::ObjStorageType::AZURE;
        break;
    case TObjStorageType::BOS:
        type = io::ObjStorageType::BOS;
        break;
    case TObjStorageType::COS:
        type = io::ObjStorageType::COS;
        break;
    case TObjStorageType::OBS:
        type = io::ObjStorageType::OBS;
        break;
    case TObjStorageType::OSS:
        type = io::ObjStorageType::OSS;
        break;
    case TObjStorageType::GCP:
        type = io::ObjStorageType::GCP;
        break;
    default:
        LOG_FATAL("unknown provider type {}, info {}", param.provider, ret.to_string());
        __builtin_unreachable();
    }
    ret.client_conf.provider = type;
    return ret;
}

} // end namespace doris
