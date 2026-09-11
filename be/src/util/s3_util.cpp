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
#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <initializer_list>
#include <memory>
#include <ostream>
#include <string_view>
#include <tuple>
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

int64_t azure_client_time_millis() {
    int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
    TEST_SYNC_POINT_CALLBACK("S3ClientFactory::azure_client_time", &now);
    return now;
}

Status validate_azure_credentials_for_access(const AzureCredentialOptions& credential,
                                             int64_t now_ms, int64_t* effective_expiry_ms) {
    *effective_expiry_ms = 0;
    // Explicit expiry has no SDK dependency. Reject it before test creators as
    // well, including builds without Azure support.
    if (credential.type == AzureCredentialType::SAS) {
        *effective_expiry_ms = credential.sas_expiration_time_ms;
        if (*effective_expiry_ms > 0 && *effective_expiry_ms <= now_ms) {
            return Status::InvalidArgument("Azure SAS credential is expired");
        }
    }
#ifdef USE_AZURE
    if (auto error = AzureAuthFactory::validate(credential, now_ms, effective_expiry_ms);
        !error.empty()) {
        return Status::InvalidArgument("{}", error);
    }
#endif
    return Status::OK();
}

doris::Status is_s3_conf_valid(const S3ClientConf& conf) {
    if (conf.endpoint.empty()) {
        return Status::InvalidArgument<false>("Invalid s3 conf, empty endpoint");
    }
    if (conf.provider == io::ObjStorageProvider::AZURE) {
        if (auto error = conf.azure_credentials.validate(); !error.empty()) {
            return Status::InvalidArgument("{}", error);
        }
        return Status::OK();
    }
    if (conf.region.empty()) {
        return Status::InvalidArgument<false>("Invalid s3 conf, empty region");
    }
    if (conf.role_arn.empty()) {
        // Preserve the existing non-Azure anonymous/static credential contract.
        if (!conf.ak.empty() && conf.sk.empty()) {
            return Status::InvalidArgument<false>("Invalid s3 conf, empty sk");
        }
        if (!conf.sk.empty() && conf.ak.empty()) {
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

bool to_int64(std::string_view str, int64_t& res) {
    auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.size(), res);
    return ec == std::errc {} && ptr == str.data() + str.size();
}

#ifdef USE_AZURE
std::string env_or_empty(const char* env_name) {
    if (const char* value = std::getenv(env_name); value != nullptr) {
        return value;
    }
    return "";
}

std::string redact_azure_log_message(std::string message) {
    // Azure SDK diagnostics may include a fully-qualified request URL.  A SAS
    // token is a bearer credential, so redact the query component before it
    // reaches the Doris logger while retaining the host/path context.
    size_t query = message.find('?');
    while (query != std::string::npos) {
        size_t end = query + 1;
        while (end < message.size() && message[end] != ' ' && message[end] != '\t' &&
               message[end] != '\r' && message[end] != '\n' && message[end] != '"' &&
               message[end] != '\'' && message[end] != ')' && message[end] != ']') {
            ++end;
        }
        message.replace(query + 1, end - query - 1, "<redacted>");
        query = message.find('?', query + std::string("?<redacted>").size());
    }
    return message;
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

// Native Azure binding keys.  The AWS_* aliases above remain accepted for
// existing object-storage callers, but Azure scans use these provider-owned
// names so their meaning does not depend on the S3 adapter.
constexpr char AZURE_AUTH_TYPE[] = "AZURE_AUTH_TYPE";
constexpr char AZURE_ENDPOINT[] = "AZURE_ENDPOINT";
constexpr char AZURE_ACCOUNT_NAME[] = "AZURE_ACCOUNT_NAME";
constexpr char AZURE_ACCOUNT_KEY[] = "AZURE_ACCOUNT_KEY";
constexpr char AZURE_CONTAINER[] = "AZURE_CONTAINER";
constexpr char AZURE_SAS_TOKEN[] = "AZURE_SAS_TOKEN";
constexpr char AZURE_SAS_EXPIRY_MS[] = "AZURE_SAS_EXPIRY_MS";
constexpr char AZURE_CLIENT_ID[] = "AZURE_CLIENT_ID";
constexpr char AZURE_CLIENT_SECRET[] = "AZURE_CLIENT_SECRET";
constexpr char AZURE_TENANT_ID[] = "AZURE_TENANT_ID";
constexpr char AZURE_OAUTH_SERVER_URI[] = "AZURE_OAUTH_SERVER_URI";

const std::string* find_property(const StringCaseMap<std::string>& properties,
                                 std::initializer_list<const char*> names) {
    for (const auto* name : names) {
        auto it = properties.find(name);
        if (it != properties.end()) {
            return &it->second;
        }
    }
    return nullptr;
}

bool has_property(const StringCaseMap<std::string>& properties,
                  std::initializer_list<const char*> names) {
    return find_property(properties, names) != nullptr;
}

std::string normalize_azure_endpoint(std::string endpoint) {
    if (endpoint.empty()) {
        return endpoint;
    }
    const bool has_scheme = endpoint.find("://") != std::string::npos;
    if (!has_scheme) {
        endpoint = "https://" + endpoint;
    }
    const auto scheme_end = endpoint.find("://");
    endpoint.replace(0, scheme_end, to_lower(endpoint.substr(0, scheme_end)));
    const auto authority_begin = scheme_end == std::string::npos ? 0 : scheme_end + 3;
    const auto authority_end = endpoint.find('/', authority_begin);
    const auto authority_length = authority_end == std::string::npos
                                          ? endpoint.size() - authority_begin
                                          : authority_end - authority_begin;
    const auto authority = endpoint.substr(authority_begin, authority_length);
    if (authority.empty()) {
        return endpoint;
    }

    auto lower_authority = to_lower(authority);
    endpoint.replace(authority_begin, authority_length, lower_authority);
    // Match the host, not host:port, so explicit transport ports do not disable
    // the official DFS-to-Blob conversion. Custom proxy hosts stay unchanged.
    const auto host = lower_authority.substr(0, lower_authority.find(':'));
    const auto dfs_pos = host.find(".dfs.");
    const bool official_dfs =
            dfs_pos != std::string::npos && (host.ends_with(".dfs.core.windows.net") ||
                                             host.ends_with(".dfs.core.chinacloudapi.cn") ||
                                             host.ends_with(".dfs.core.usgovcloudapi.net") ||
                                             host.ends_with(".dfs.core.cloudapi.de"));
    if (official_dfs) {
        endpoint.replace(authority_begin + dfs_pos, 5, ".blob.");
    } else if (!has_scheme && authority.find('.') == std::string::npos &&
               authority.find(':') == std::string::npos) {
        // Preserve the legacy SharedKey endpoint shorthand. Native FE bindings always
        // materialize custom endpoints with an explicit scheme before reaching this layer.
        endpoint.insert(authority_begin + authority.size(), ".blob.core.windows.net");
    }
    while (endpoint.ends_with('/')) {
        endpoint.pop_back();
    }
    return endpoint;
}

std::string endpoint_authority(const std::string& endpoint) {
    const auto begin = endpoint.find("://") + 3;
    auto authority = endpoint.substr(begin, endpoint.find('/', begin) - begin);
    if (endpoint.starts_with("https://") && authority.ends_with(":443")) {
        authority.resize(authority.size() - 4);
    } else if (endpoint.starts_with("http://") && authority.ends_with(":80")) {
        authority.resize(authority.size() - 3);
    }
    return authority;
}

// Only established SharedKey wire producers use AWS fields for Azure. Once
// translated here the native factory never inspects these fields again.
void import_legacy_azure_shared_key(S3ClientConf* conf) {
    // Keep the established SharedKey endpoint normalization at the legacy
    // boundary. Native endpoints and object keys retain their internal slashes.
    conf->endpoint = normalize_http_uri(conf->endpoint);
    conf->azure_credentials = {};
    conf->azure_credentials.type = AzureCredentialType::SHARED_KEY;
    conf->azure_credentials.account_name = std::move(conf->ak);
    conf->azure_credentials.account_key = std::move(conf->sk);
    conf->ak.clear();
    conf->sk.clear();
    conf->token.clear();
    conf->region.clear();
    conf->role_arn.clear();
    conf->external_id.clear();
    conf->cred_provider_type = CredProviderType::Default;
}

Status convert_legacy_azure_properties(const StringCaseMap<std::string>& properties,
                                       S3ClientConf* client_conf) {
    auto& client = *client_conf;
    // Compatibility is deliberately limited to the old SharedKey map. An
    // incomplete native map must not be mistaken for that old protocol.
    for (const auto& [key, value] : properties) {
        const auto lower = to_lower(key);
        if (lower.starts_with("azure") || lower.starts_with("fs.azure.") ||
            (lower == "aws_token" && !value.empty())) {
            return Status::InvalidArgument("Azure native credentials require AZURE_AUTH_TYPE");
        }
    }
    if (!has_property(properties, {S3_ENDPOINT}) || !has_property(properties, {S3_AK}) ||
        !has_property(properties, {S3_SK})) {
        return Status::InvalidArgument("Azure native credentials require AZURE_AUTH_TYPE");
    }
    client.endpoint = *find_property(properties, {S3_ENDPOINT});
    client.ak = *find_property(properties, {S3_AK});
    client.sk = *find_property(properties, {S3_SK});
    import_legacy_azure_shared_key(&client);
    for (const auto& [name, target] :
         {std::pair {S3_MAX_CONN_SIZE, &client.max_connections},
          std::pair {S3_REQUEST_TIMEOUT_MS, &client.request_timeout_ms},
          std::pair {S3_CONN_TIMEOUT_MS, &client.connect_timeout_ms}}) {
        if (const auto* value = find_property(properties, {name}); value != nullptr) {
            if (!to_int(*value, *target)) {
                return Status::InvalidArgument("invalid Azure connection option {}", name);
            }
        }
    }
    return Status::OK();
}

Status convert_native_azure_properties(const StringCaseMap<std::string>& properties,
                                       const S3URI& uri, const std::string& auth_type,
                                       S3ClientConf* client_conf) {
    auto& client = *client_conf;
    for (const auto& [key, value] : properties) {
        const auto lower = to_lower(key);
        if (lower.starts_with("aws_") || lower.starts_with("azure.")) {
            return Status::InvalidArgument(
                    "Azure native credentials cannot use AWS or catalog property aliases");
        }
    }
    // Older FE versions attach a Hadoop configuration view for OneLake to
    // this map. Ignore those extra keys; they must never supply or override
    // any native authentication field. FE routing separates the two views.
    auto& credential = client.azure_credentials;
    if (auth_type == "SHARED_KEY") {
        credential.type = AzureCredentialType::SHARED_KEY;
    } else if (auth_type == "SAS") {
        credential.type = AzureCredentialType::SAS;
    } else if (auth_type == "OAUTH2") {
        credential.type = AzureCredentialType::OAUTH2;
    } else {
        return Status::InvalidArgument("unsupported AZURE_AUTH_TYPE in native credentials");
    }
    auto set = [&](const char* key, std::string* target) {
        if (const auto* value = find_property(properties, {key}); value != nullptr) {
            *target = *value;
        }
    };
    set(AZURE_ENDPOINT, &client.endpoint);
    set(AZURE_ACCOUNT_NAME, &credential.account_name);
    set(AZURE_ACCOUNT_KEY, &credential.account_key);
    set(AZURE_SAS_TOKEN, &credential.sas_token);
    set(AZURE_CLIENT_ID, &credential.oauth_client_id);
    set(AZURE_CLIENT_SECRET, &credential.oauth_client_secret);
    set(AZURE_TENANT_ID, &credential.oauth_tenant_id);
    set(AZURE_OAUTH_SERVER_URI, &credential.oauth_server_uri);
    if (const auto* expiry = find_property(properties, {AZURE_SAS_EXPIRY_MS}); expiry != nullptr) {
        if (!to_int64(*expiry, credential.sas_expiration_time_ms) ||
            credential.sas_expiration_time_ms <= 0) {
            return Status::InvalidArgument("invalid Azure SAS expiry value");
        }
    }
    if (client.endpoint.empty() || credential.account_name.empty()) {
        return Status::InvalidArgument(
                "Azure native credentials require endpoint and account name");
    }
    if (credential.type == AzureCredentialType::SHARED_KEY && credential.account_key.empty()) {
        return Status::InvalidArgument("Azure native SharedKey requires an account key");
    }
    // Existing Azure SharedKey catalogs also contain S3-spelled locations.
    // This explicit compatibility case carries no account in its URI;
    // SAS/OAuth2 native data locations must carry their Azure authority.
    if (uri.get_scheme().empty() ||
        (uri.get_scheme() == "s3" && credential.type != AzureCredentialType::SHARED_KEY)) {
        return Status::InvalidArgument("Azure native credentials require an Azure data URI");
    }
    if (client.endpoint.find_first_of("?#@\r\n") != std::string::npos) {
        return Status::InvalidArgument(
                "Azure endpoint must not contain credentials, query or fragment");
    }
    client.endpoint = normalize_azure_endpoint(client.endpoint);
    if (!client.endpoint.starts_with("https://") && !client.endpoint.starts_with("http://")) {
        return Status::InvalidArgument("Azure endpoint must use HTTP or HTTPS");
    }
    // A container property may constrain legacy callers, but is never a
    // fallback location. Every native data URI identifies its container.
    if (const auto* container = find_property(properties, {AZURE_CONTAINER});
        container != nullptr && *container != uri.get_bucket()) {
        return Status::InvalidArgument("Azure URI container conflicts with the storage binding");
    }
    return Status::OK();
}

Status convert_azure_properties(const StringCaseMap<std::string>& properties, const S3URI& uri,
                                S3Conf* conf) {
    auto& client = conf->client_conf;
    client.provider = io::ObjStorageProvider::AZURE;
    const auto* auth_type = find_property(properties, {AZURE_AUTH_TYPE});
    if (auth_type == nullptr) {
        RETURN_IF_ERROR(convert_legacy_azure_properties(properties, &client));
    } else {
        RETURN_IF_ERROR(convert_native_azure_properties(properties, uri, *auth_type, &client));
    }
    if (uri.get_bucket().empty()) {
        return Status::InvalidArgument("Azure data URI requires a container");
    }
    conf->bucket = uri.get_bucket();
    client.bucket = conf->bucket;
    if (auth_type != nullptr) {
        RETURN_IF_ERROR(S3ClientFactory::validate_azure_uri(uri, client));
    }
    return is_s3_conf_valid(client);
}
} // namespace

Status S3ClientFactory::validate_azure_uri(const S3URI& uri, const S3ClientConf& conf) {
    if (uri.get_scheme().empty()) {
        return Status::OK(); // Internal file-system callers may pass a raw object key.
    }
    if (uri.get_bucket() != conf.bucket) {
        return Status::InvalidArgument("Azure URI container conflicts with the storage binding");
    }
    if (uri.get_scheme() == "s3") {
        if (conf.azure_credentials.type != AzureCredentialType::SHARED_KEY) {
            return Status::InvalidArgument("Azure SAS/OAuth2 data access requires an Azure URI");
        }
        return Status::OK(); // Old SharedKey file-system paths use the S3 wire spelling.
    }
    const auto uri_host = to_lower(uri.get_endpoint());
    if (uri_host.ends_with(".dfs.fabric.microsoft.com") ||
        uri_host.ends_with(".blob.fabric.microsoft.com")) {
        return Status::NotSupported("OneLake data access requires its Hadoop storage binding");
    }
    const auto endpoint = normalize_azure_endpoint(conf.endpoint);
    const bool http_uri = uri.get_scheme() == "http" || uri.get_scheme() == "https";
    const auto uri_endpoint = normalize_azure_endpoint(
            http_uri ? uri.get_scheme() + "://" + uri.get_endpoint() : uri.get_endpoint());
    // ABFS/WASB authorities identify the logical Azure account, while a configured custom
    // endpoint may be a proxy or emulator that intentionally has a different HTTP authority.
    // The native client uses conf.endpoint as the transport origin; retain account validation
    // below, but do not reject this valid proxy form. HTTP(S) locations carry their transport
    // origin directly and must still match exactly.
    const bool custom_transport = !S3URI::is_azure_endpoint(endpoint_authority(endpoint));
    if (endpoint_authority(endpoint) != endpoint_authority(uri_endpoint) &&
        !(custom_transport && !http_uri)) {
        return Status::InvalidArgument(
                "Azure URI account host conflicts with the storage endpoint");
    }
    if (http_uri && !endpoint.starts_with(uri.get_scheme() + "://")) {
        return Status::InvalidArgument("Azure URI scheme conflicts with the storage endpoint");
    }
    if (S3URI::is_azure_endpoint(uri.get_endpoint()) &&
        !iequal(conf.azure_credentials.account_name, uri.get_account())) {
        return Status::InvalidArgument("Azure URI account conflicts with the credential account");
    }
    return Status::OK();
}

bool S3ClientConf::operator==(const S3ClientConf& other) const {
    if (std::tie(provider, endpoint, bucket, max_connections, request_timeout_ms,
                 connect_timeout_ms, is_internal_bucket) !=
        std::tie(other.provider, other.endpoint, other.bucket, other.max_connections,
                 other.request_timeout_ms, other.connect_timeout_ms, other.is_internal_bucket)) {
        return false;
    }
    if (provider == io::ObjStorageProvider::AZURE) {
        return azure_credentials == other.azure_credentials;
    }
    return std::tie(ak, sk, token, region, use_virtual_addressing, need_override_endpoint,
                    cred_provider_type, role_arn, external_id) ==
           std::tie(other.ak, other.sk, other.token, other.region, other.use_virtual_addressing,
                    other.need_override_endpoint, other.cred_provider_type, other.role_arn,
                    other.external_id);
}

uint64_t S3ClientConf::get_hash() const {
    uint64_t hash = crc32_hash(endpoint) ^ crc32_hash(bucket) ^ max_connections ^
                    request_timeout_ms ^ connect_timeout_ms ^ static_cast<int>(provider) ^
                    is_internal_bucket;
    if (provider == io::ObjStorageProvider::AZURE) {
        const auto& credential = azure_credentials;
        hash ^= static_cast<int>(credential.type);
        hash ^= crc32_hash(credential.account_name + credential.account_key);
        hash ^= crc32_hash(credential.sas_token);
        hash ^= credential.sas_expiration_time_ms;
        hash ^= crc32_hash(credential.oauth_client_id);
        hash ^= crc32_hash(credential.oauth_client_secret);
        hash ^= crc32_hash(credential.oauth_tenant_id);
        hash ^= crc32_hash(credential.oauth_server_uri);
        return hash;
    }
    // Preserve the S3 hash and full equality contract, including collision handling.
    return hash ^ crc32_hash(ak + sk) ^ crc32_hash(token) ^ crc32_hash(region) ^
           use_virtual_addressing ^ static_cast<int>(cred_provider_type) ^ crc32_hash(role_arn) ^
           crc32_hash(external_id);
}

std::string S3ClientConf::to_string() const {
    if (provider == io::ObjStorageProvider::AZURE) {
        // Endpoints have no credential query in the native protocol. Also redact
        // legacy endpoints at this logging boundary, without rewriting requests.
        return fmt::format(
                "(provider=azure, auth_type={}, account={}, endpoint={}, bucket={}, "
                "sas_expiration_time_ms={}, is_internal_bucket={})",
                static_cast<int>(azure_credentials.type), azure_credentials.account_name,
                endpoint.substr(0, endpoint.find('?')), bucket,
                azure_credentials.sas_expiration_time_ms, is_internal_bucket);
    }
    return fmt::format(
            "(ak={}, token={}, endpoint={}, region={}, bucket={}, max_connections={}, "
            "request_timeout_ms={}, connect_timeout_ms={}, use_virtual_addressing={}, "
            "cred_provider_type={}, role_arn={}, external_id={}, is_internal_bucket={})",
            hide_access_key(ak), token.empty() ? "" : "******", endpoint, region, bucket,
            max_connections, request_timeout_ms, connect_timeout_ms, use_virtual_addressing,
            cred_provider_type, role_arn, external_id, is_internal_bucket);
}

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
                const auto safe_message = redact_azure_log_message(message);
                switch (level) {
                case Azure::Core::Diagnostics::Logger::Level::Verbose:
                    LOG(INFO) << safe_message;
                    break;
                case Azure::Core::Diagnostics::Logger::Level::Informational:
                    LOG(INFO) << safe_message;
                    break;
                case Azure::Core::Diagnostics::Logger::Level::Warning:
                    LOG(WARNING) << safe_message;
                    break;
                case Azure::Core::Diagnostics::Logger::Level::Error:
                    LOG(ERROR) << safe_message;
                    break;
                default:
                    LOG(WARNING) << "Unknown level: " << static_cast<int>(level)
                                 << ", message: " << safe_message;
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
    const bool is_azure = s3_conf.provider == io::ObjStorageProvider::AZURE;
    int64_t azure_expiry_ms = 0;
    if (is_azure) {
        const auto now_ms = azure_client_time_millis();
        {
            std::lock_guard l(_lock);
            _prune_azure_clients(now_ms);
        }
        RETURN_IF_ERROR_RESULT(validate_azure_credentials_for_access(s3_conf.azure_credentials,
                                                                     now_ms, &azure_expiry_ms));
    }

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
        if (is_azure) {
            if (azure_expiry_ms > 0 && azure_expiry_ms <= azure_client_time_millis()) {
                _prune_azure_clients(azure_expiry_ms);
                return ResultError(Status::InvalidArgument("Azure SAS credential is expired"));
            }
            auto it = _azure_cache.find(s3_conf);
            if (it != _azure_cache.end()) {
                it->second.last_access = ++_azure_cache_clock;
                return it->second.client;
            }
        } else {
            auto it = _cache.find(s3_conf);
            if (it != _cache.end()) {
                return it->second;
            }
        }
    }

    if (is_azure) {
        TEST_SYNC_POINT("S3ClientFactory::azure_cache_miss");
    }
    auto client_result = (s3_conf.provider == io::ObjStorageProvider::AZURE)
                                 ? _create_azure_client(s3_conf)
                                 : _create_s3_client(s3_conf);
    if (!client_result.has_value()) {
        return ResultError(std::move(client_result).error());
    }
    if (is_azure) {
        TEST_SYNC_POINT("S3ClientFactory::azure_client_built");
    }
    auto obj_client = std::move(client_result).value();
    if (!config::is_cloud_mode() || s3_conf.is_internal_bucket) {
        obj_client = std::make_shared<io::RateLimitedObjStorageClient>(
                std::move(obj_client), std::make_shared<BeObjStorageRateLimitPolicy>());
    }

    {
        std::lock_guard l(_lock);
        if (is_azure) {
            return _publish_azure_client(s3_conf, obj_client, azure_expiry_ms);
        }
        auto [it, _] = _cache.emplace(s3_conf, std::move(obj_client));
        return it->second;
    }
}

Result<std::shared_ptr<io::ObjStorageClient>> S3ClientFactory::_publish_azure_client(
        const S3ClientConf& s3_conf, std::shared_ptr<io::ObjStorageClient>& obj_client,
        int64_t azure_expiry_ms) {
    const auto now_ms = azure_client_time_millis();
    _prune_azure_clients(now_ms);
    if (azure_expiry_ms > 0 && azure_expiry_ms <= now_ms) {
        return ResultError(Status::InvalidArgument("Azure SAS credential is expired"));
    }
    // A concurrent creator may have published the same identity while
    // SDK construction ran outside the lock. Reuse that client.
    auto it = _azure_cache.find(s3_conf);
    if (it != _azure_cache.end()) {
        it->second.last_access = ++_azure_cache_clock;
        return it->second.client;
    }
    if (_azure_cache.size() >= _azure_cache_capacity) {
        auto oldest =
                std::ranges::min_element(_azure_cache, [](const auto& left, const auto& right) {
                    return left.second.last_access < right.second.last_access;
                });
        _azure_cache.erase(oldest);
    }
    auto [inserted, _] =
            _azure_cache.emplace(s3_conf, AzureCachedClient {.client = std::move(obj_client),
                                                             .expiry_ms = azure_expiry_ms,
                                                             .last_access = ++_azure_cache_clock});
    return inserted->second.client;
}

Status S3ClientFactory::validate_credentials_for_access(const S3ClientConf& conf) {
    if (conf.provider != io::ObjStorageProvider::AZURE) {
        return Status::OK();
    }
    int64_t effective_expiry_ms = 0;
    return validate_azure_credentials_for_access(conf.azure_credentials, azure_client_time_millis(),
                                                 &effective_expiry_ms);
}

void S3ClientFactory::_prune_azure_clients(int64_t now_ms) {
    for (auto it = _azure_cache.begin(); it != _azure_cache.end();) {
        if (it->second.expiry_ms > 0 && it->second.expiry_ms <= now_ms) {
            it = _azure_cache.erase(it);
        } else {
            ++it;
        }
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
    const std::string endpoint = normalize_azure_endpoint(s3_conf.endpoint);
    const std::string container_name = s3_conf.bucket;
    std::string uri = fmt::format("{}/{}", endpoint, container_name);

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

    VLOG_DEBUG << "azure container uri:" << uri.substr(0, uri.find('?'));
    std::string tls_debug_context = build_azure_tls_debug_context(ca_cert_file_path);

    auto built = AzureAuthFactory::create(uri, s3_conf.azure_credentials, std::move(options));
    if (!built) {
        return ResultError(
                Status::InvalidArgument("failed to create Azure client: {}", built.error));
    }
    LOG_INFO("create one azure client with {}", s3_conf.to_string());
    return std::make_shared<io::AzureObjStorageClient>(
            std::move(built.container_client),
            ObjStorageEndpointInfo {
                    .endpoint = endpoint,
                    .ak = s3_conf.azure_credentials.account_name,
                    .sk = s3_conf.azure_credentials.account_key,
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
    s3_conf->client_conf.provider = io::ObjStorageProvider::AWS;
    s3_conf->client_conf.azure_credentials = {};
    if (const auto* provider = find_property(properties, {S3_PROVIDER});
        provider != nullptr && iequal(*provider, AZURE_PROVIDER_STRING)) {
        *s3_conf = {};
        return convert_azure_properties(properties, s3_uri, s3_conf);
    }
    if (s3_uri.is_azure()) {
        return Status::InvalidArgument("Azure data URI requires provider=azure");
    }
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
    if (s3_uri.get_bucket().empty()) {
        return Status::InvalidArgument("Invalid S3 URI {}, bucket is not specified",
                                       s3_uri.to_string());
    } else {
        s3_conf->bucket = s3_uri.get_bucket();
    }
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
                    .azure_credentials = {},
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
    if (type == io::ObjStorageProvider::AZURE) {
        import_legacy_azure_shared_key(&ret.client_conf);
    }
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
                    .azure_credentials = {},
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
    if (type == io::ObjStorageProvider::AZURE) {
        import_legacy_azure_shared_key(&ret.client_conf);
    }
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
