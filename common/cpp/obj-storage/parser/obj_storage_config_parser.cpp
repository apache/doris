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

#include "cpp/obj-storage/parser/obj_storage_config_parser.h"

#include <brpc/uri.h>
#include <fmt/format.h>
#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/ObjectStorage_types.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <type_traits>
#include <utility>

#include "cpp/obj-storage/auth/gcp/gcp_auth.h"

namespace doris {
namespace {
using Vault = cloud::ObjectStoreInfoPB;

struct ProviderMapping {
    std::string_view name;
    TObjStorageType::type resource;
    cloud::ObjectStoreInfoPB::Provider vault;
};

constexpr ProviderMapping PROVIDERS[] = {
        {"AWS", TObjStorageType::AWS, cloud::ObjectStoreInfoPB::S3},
        {"S3", TObjStorageType::AWS, cloud::ObjectStoreInfoPB::S3},
        {"OSS", TObjStorageType::OSS, cloud::ObjectStoreInfoPB::OSS},
        {"COS", TObjStorageType::COS, cloud::ObjectStoreInfoPB::COS},
        {"BOS", TObjStorageType::BOS, cloud::ObjectStoreInfoPB::BOS},
        {"OBS", TObjStorageType::OBS, cloud::ObjectStoreInfoPB::OBS},
        {"GCP", TObjStorageType::GCP, cloud::ObjectStoreInfoPB::GCP},
        {"GCS", TObjStorageType::GCP, cloud::ObjectStoreInfoPB::GCP},
        {"TOS", TObjStorageType::TOS, cloud::ObjectStoreInfoPB::TOS},
        {"AZURE", TObjStorageType::AZURE, cloud::ObjectStoreInfoPB::AZURE},
};

std::optional<std::string_view> credential_provider_name(TCredProviderType::type provider) {
    switch (provider) {
    case TCredProviderType::DEFAULT:
        return "DEFAULT";
    case TCredProviderType::SIMPLE:
        return "SIMPLE";
    case TCredProviderType::INSTANCE_PROFILE:
        return "INSTANCE_PROFILE";
    case TCredProviderType::ENV:
        return "ENV";
    case TCredProviderType::SYSTEM_PROPERTIES:
        return "SYSTEM_PROPERTIES";
    case TCredProviderType::WEB_IDENTITY:
        return "WEB_IDENTITY";
    case TCredProviderType::CONTAINER:
        return "CONTAINER";
    case TCredProviderType::ANONYMOUS:
        return "ANONYMOUS";
    default:
        return std::nullopt;
    }
}

std::optional<std::string_view> gcp_credential_provider_name(
        TGcpCredentialProviderType::type provider) {
    switch (provider) {
    case TGcpCredentialProviderType::DEFAULT:
        return "DEFAULT";
    case TGcpCredentialProviderType::COMPUTE_ENGINE:
        return "COMPUTE_ENGINE";
    default:
        return std::nullopt;
    }
}

std::string to_upper(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return text;
}

std::string normalize_provider(std::string text) {
    const auto non_space = [](unsigned char c) { return !std::isspace(c); };
    text.erase(text.begin(), std::find_if(text.begin(), text.end(), non_space));
    text.erase(std::find_if(text.rbegin(), text.rend(), non_space).base(), text.end());
    return to_upper(std::move(text));
}

std::string_view pop_dns_label(std::string_view& name) {
    const auto dot = name.rfind('.');
    if (dot == std::string_view::npos) {
        return std::exchange(name, std::string_view {});
    }
    auto label = name.substr(dot + 1);
    name = name.substr(0, dot);
    return label;
}

} // namespace

cloud::ObjectStoreInfoPB::Provider vault_storage_provider(const cloud::ObjectStoreInfoPB& info) {
    if (info.has_provider() || info.endpoint().empty()) {
        return info.provider();
    }
    return storage_provider_from_endpoint(info.endpoint());
}

cloud::ObjectStoreInfoPB::Provider storage_provider_from_endpoint(std::string_view endpoint) {
    std::string url(endpoint);
    std::transform(url.begin(), url.end(), url.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    brpc::URI uri;
    if (url.find('\0') != std::string::npos || uri.SetHttpURL(url.c_str()) != 0 ||
        (!uri.scheme().empty() && uri.scheme() != "http" && uri.scheme() != "https")) {
        return Vault::S3;
    }
    std::string_view host = uri.host();
    if (host.ends_with('.')) {
        host.remove_suffix(1);
    }
    if (host.empty() || host.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789-.") !=
                                std::string_view::npos) {
        return Vault::S3;
    }

    // Match from the DNS root so virtual-hosted buckets cannot change the service or cloud.
    auto labels = host;
    auto tld = pop_dns_label(labels);
    auto domain = pop_dns_label(labels);
    auto last = pop_dns_label(labels);
    auto previous = pop_dns_label(labels);
    auto is_region = [](std::string_view name) {
        return name.size() > 3 && name[2] == '-' && name[0] >= 'a' && name[0] <= 'z' &&
               name[1] >= 'a' && name[1] <= 'z';
    };
    if (domain == "aliyuncs" && tld == "com") {
        if ((last.starts_with("oss-") && is_region(last.substr(4))) || last == "oss-accelerate" ||
            last == "oss-accelerate-overseas" || (previous == "s3" && is_region(last)) ||
            (last == "oss" && is_region(previous))) {
            return Vault::OSS;
        }
    } else if (domain == "googleapis" && tld == "com" && last == "storage") {
        return Vault::GCP;
    } else if (domain == "myqcloud" && tld == "com" && previous == "cos" && !last.empty()) {
        return Vault::COS;
    } else if (domain == "myhuaweicloud" && tld == "com" && previous == "obs" && !last.empty()) {
        return Vault::OBS;
    } else if (domain == "bcebos" && tld == "com" && !last.empty()) {
        return Vault::BOS;
    } else if ((domain == "volces" || domain == "ivolces") && tld == "com" &&
               ((last.starts_with("tos-") && is_region(last.substr(4))) ||
                (last.starts_with("tos-s3-") && is_region(last.substr(7))))) {
        return Vault::TOS;
    } else if (last == "core" && (previous == "blob" || previous == "dfs") && !labels.empty() &&
               ((domain == "windows" && tld == "net") ||
                (domain == "chinacloudapi" && tld == "cn") ||
                (domain == "usgovcloudapi" && tld == "net") ||
                (domain == "cloudapi" && tld == "de"))) {
        return Vault::AZURE;
    }
    return Vault::S3;
}

AwsCredentialImplementation aws_credential_implementation(std::string_view version) {
    return version == "v2" ? AwsCredentialImplementation::V2 : AwsCredentialImplementation::V1;
}

void normalize_obj_storage_endpoint(ObjStorageClientConfig& client, std::string_view scheme) {
    auto& endpoint = obj_storage_connection(client).endpoint;
    if (auto* aws = std::get_if<AwsObjStorageConfig>(&client); aws && aws->use_sdk_endpoint) {
        endpoint.clear();
        aws->sdk_endpoint_http = scheme == "http";
        return;
    }
    if (!endpoint.empty() && endpoint.find("://") == std::string::npos) {
        endpoint.insert(0,
                        scheme == "http" && !std::holds_alternative<AzureObjStorageConfig>(client)
                                ? "http://"
                                : "https://");
    }
}

namespace {
using Props = ObjStorageProperties;
using Error = std::optional<std::string>;
template <class T>
using Parsed = ObjStorageConfigParseResult<T>;

std::string value(const Props& properties, const char* key) {
    auto it = properties.find(key);
    return it == properties.end() ? std::string {} : it->second;
}

template <class T>
Error read_number(const Props& props, const char* key, std::optional<T>& output) {
    auto it = props.find(key);
    if (it == props.end()) {
        return {};
    }
    const auto& text = it->second;
    int number;
    auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), number);
    if (error != std::errc {} || end != text.data() + text.size() || number < -1) {
        return fmt::format("invalid {} value \"{}\"", key, text);
    }
    // Legacy resources use -1 or 0 for host defaults. Passing a zero request timeout
    // to the SDK would disable stalled-transfer detection instead of keeping the BE default.
    if (number > 0) {
        output = T(number);
    }
    return {};
}
Error read_timeouts(const Props& props, ObjStorageConnectionConfig& conn) {
    if (auto error = read_number(props, "AWS_REQUEST_TIMEOUT_MS", conn.request_timeout)) {
        return error;
    }
    return read_number(props, "AWS_CONNECTION_TIMEOUT_MS", conn.connect_timeout);
}
Error read_bool(const Props& props, const char* key, bool& output) {
    auto it = props.find(key);
    if (it == props.end()) {
        return {};
    }
    auto text = it->second;
    std::transform(text.begin(), text.end(), text.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (text == "true") {
        output = true;
    } else if (text == "false") {
        output = false;
    } else {
        return fmt::format("invalid {} value \"{}\"", key, it->second);
    }
    return {};
}
template <class C>
Error read_regional_options(const Props& props, C& client) {
    client.region = value(props, "AWS_REGION");
    client.connection.endpoint = value(props, "AWS_ENDPOINT");
    if (auto error = read_bool(props, "use_path_style", client.use_path_style)) {
        return error;
    }
    bool override_endpoint = true;
    if (auto error = read_bool(props, "AWS_NEED_OVERRIDE_ENDPOINT", override_endpoint)) {
        return error;
    }
    if (!override_endpoint) {
        if constexpr (std::is_same_v<C, AwsObjStorageConfig>) {
            client.use_sdk_endpoint = true;
        } else {
            return "AWS SDK automatic endpoint selection requires provider=AWS";
        }
    }
    if (auto error = read_number(props, "AWS_MAX_CONNECTIONS", client.connection.max_connections)) {
        return error;
    }
    return read_timeouts(props, client.connection);
}

AwsCredentialChainType chain_source(CredProviderType mode) {
    switch (mode) {
    case CredProviderType::Env:
        return AwsCredentialChainType::Environment;
    case CredProviderType::SystemProperties:
        return AwsCredentialChainType::Profile;
    case CredProviderType::WebIdentity:
        return AwsCredentialChainType::WebIdentity;
    case CredProviderType::Container:
        return AwsCredentialChainType::Container;
    case CredProviderType::InstanceProfile:
        return AwsCredentialChainType::InstanceProfile;
    default:
        return AwsCredentialChainType::Default;
    }
}

Parsed<AccessKeyOrAnonymousObjStorageAuth> parse_access_key_credentials(const Props& props) {
    AccessKeyObjStorageAuth keys {value(props, "AWS_ACCESS_KEY"), value(props, "AWS_SECRET_KEY"),
                                  value(props, "AWS_TOKEN")};
    if (!keys.access_key.empty() && !keys.secret_key.empty()) {
        // Some persisted resources also carry AWS role/provider fields. Static keys
        // have always taken precedence; they include temporary credentials issued by FE.
        return AccessKeyOrAnonymousObjStorageAuth {std::move(keys)};
    }
    if (keys.access_key.empty() != keys.secret_key.empty()) {
        return std::string("Incomplete access key credentials");
    }
    if (!keys.session_token.empty()) {
        return std::string("Session token requires access key credentials");
    }
    const auto mode = to_upper(value(props, "AWS_CREDENTIALS_PROVIDER_TYPE"));
    if ((!mode.empty() && mode != "DEFAULT" && mode != "SIMPLE" && mode != "ANONYMOUS") ||
        !value(props, "AWS_ROLE_ARN").empty() || !value(props, "AWS_EXTERNAL_ID").empty()) {
        return std::string(
                "AWS credential chains and AssumeRole are not supported for this provider");
    }
    return AccessKeyOrAnonymousObjStorageAuth {AnonymousObjStorageAuth {}};
}

template <class C>
Parsed<C> parse_access_key_client(const Props& props) {
    C client;
    if (auto error = read_regional_options(props, client)) {
        return *error;
    }
    auto credentials = parse_access_key_credentials(props);
    if (auto* error = std::get_if<std::string>(&credentials)) {
        return std::move(*error);
    }
    client.auth = std::get<AccessKeyOrAnonymousObjStorageAuth>(std::move(credentials));
    return client;
}
} // namespace

ObjStorageConfigParseResult<AwsObjStorageConfig> parse_aws_obj_storage_config(
        const ObjStorageProperties& props, AwsCredentialImplementation impl) {
    AwsObjStorageConfig client;
    if (auto error = read_regional_options(props, client)) {
        return *error;
    }
    client.auth.implementation = impl;
    AccessKeyObjStorageAuth keys {value(props, "AWS_ACCESS_KEY"), value(props, "AWS_SECRET_KEY"),
                                  value(props, "AWS_TOKEN")};
    if (!keys.access_key.empty() && !keys.secret_key.empty()) {
        client.auth.source = std::move(keys);
        return client;
    }
    auto arn = value(props, "AWS_ROLE_ARN");
    bool partial = keys.access_key.empty() != keys.secret_key.empty();
    if (partial && arn.empty()) {
        return std::string("Incomplete access key credentials");
    }
    auto mode = cred_provider_type_from_string(value(props, "AWS_CREDENTIALS_PROVIDER_TYPE"));
    if (impl == AwsCredentialImplementation::V1 && mode != CredProviderType::InstanceProfile) {
        // Legacy v1 only supports AssumeRole through the instance profile. Host
        // defaults are expressed as an explicit mode by the unified entry points.
        if (partial || mode != CredProviderType::Anonymous) {
            client.auth.source = AwsCredentialChainType::Default;
        }
    } else {
        if (mode != CredProviderType::Anonymous) {
            client.auth.source = chain_source(mode);
        }
        if (!arn.empty()) {
            AwsAssumeRole role;
            role.role_arn = std::move(arn);
            if (auto ext = value(props, "AWS_EXTERNAL_ID"); !ext.empty()) {
                role.external_id = std::move(ext);
            }
            client.auth.assume_role = std::move(role);
        }
    }
    return client;
}

ObjStorageConfigParseResult<OssObjStorageConfig> parse_oss_obj_storage_config(
        const ObjStorageProperties& props) {
    return parse_access_key_client<OssObjStorageConfig>(props);
}
ObjStorageConfigParseResult<CosObjStorageConfig> parse_cos_obj_storage_config(
        const ObjStorageProperties& props) {
    return parse_access_key_client<CosObjStorageConfig>(props);
}
ObjStorageConfigParseResult<BosObjStorageConfig> parse_bos_obj_storage_config(
        const ObjStorageProperties& props) {
    return parse_access_key_client<BosObjStorageConfig>(props);
}
ObjStorageConfigParseResult<ObsObjStorageConfig> parse_obs_obj_storage_config(
        const ObjStorageProperties& props) {
    return parse_access_key_client<ObsObjStorageConfig>(props);
}
ObjStorageConfigParseResult<TosObjStorageConfig> parse_tos_obj_storage_config(
        const ObjStorageProperties& props) {
    return parse_access_key_client<TosObjStorageConfig>(props);
}

ObjStorageConfigParseResult<GcpObjStorageConfig> parse_gcp_obj_storage_config(
        const ObjStorageProperties& props, AwsCredentialImplementation implementation) {
    GcpObjStorageConfig client;
    if (auto e = read_regional_options(props, client)) {
        return *e;
    }
    // Capture the explicit timeout before BE/Recycler fill object-I/O defaults.
    // A vault without a timeout must keep IAM signing at 10s even when BE I/O uses 30s.
    client.signing_timeout = client.connection.request_timeout.value_or(std::chrono::seconds(10));
    auto native = parse_gcp_credential_properties(props);
    if (native.error) {
        return *native.error;
    }
    if (native.credential) {
        auto mode = cred_provider_type_from_string(value(props, "AWS_CREDENTIALS_PROVIDER_TYPE"));
        if (!value(props, "AWS_ACCESS_KEY").empty() || !value(props, "AWS_SECRET_KEY").empty() ||
            !value(props, "AWS_TOKEN").empty() || !value(props, "AWS_ROLE_ARN").empty() ||
            !value(props, "AWS_EXTERNAL_ID").empty() || mode != CredProviderType::Default) {
            return std::string("GCP credentials cannot be combined with other credentials");
        }
        GcpNativeObjStorageAuth auth;
        auth.source = native.credential->provider_type == GcpCredentialProviderType::ComputeEngine
                              ? GcpCredentialSource::ComputeEngine
                              : GcpCredentialSource::Default;
        if (!native.credential->impersonation_service_account.empty()) {
            auth.impersonation_service_account =
                    std::move(native.credential->impersonation_service_account);
        }
        client.auth = std::move(auth);
    } else {
        auto credentials = parse_access_key_credentials(props);
        if (auto* error = std::get_if<std::string>(&credentials)) {
            return std::move(*error);
        }
        std::visit(
                [&]<class T>(T&& selected) {
                    if constexpr (std::is_same_v<std::decay_t<T>, AccessKeyObjStorageAuth>) {
                        client.auth = GcpHmacObjStorageAuth {std::move(selected.access_key),
                                                             std::move(selected.secret_key),
                                                             std::move(selected.session_token)};
                    } else if (to_upper(value(props, "AWS_CREDENTIALS_PROVIDER_TYPE")) !=
                               "ANONYMOUS") {
                        // DEFAULT (also omitted by legacy FE Thrift) reads ambient HMAC
                        // credentials; it must not turn private GCS requests into unsigned ones.
                        client.auth = GcpDefaultHmacObjStorageAuth {implementation};
                    } else {
                        client.auth = std::move(selected);
                    }
                },
                std::get<AccessKeyOrAnonymousObjStorageAuth>(std::move(credentials)));
    }
    return client;
}
ObjStorageConfigParseResult<AzureObjStorageConfig> parse_azure_obj_storage_config(
        const ObjStorageProperties& props) {
    AzureObjStorageConfig client;
    client.connection.endpoint = value(props, "AWS_ENDPOINT");
    client.auth = {value(props, "AWS_ACCESS_KEY"),
                   AzureSharedKeyConfig {value(props, "AWS_SECRET_KEY")}};
    if (client.auth.account_name.empty() !=
        std::get<AzureSharedKeyConfig>(client.auth.credential).account_key.empty()) {
        return std::string("Incomplete Azure shared key credentials");
    }
    if (auto it = props.find("AWS_MAX_CONNECTIONS"); it != props.end()) {
        // Legacy properties checked integer conversion (including numeric prefixes), but Azure
        // ignored the pool size. Preserve that check without adding it to the effective config.
        const auto& text = it->second;
        int ignored_pool_size;
        if (std::from_chars(text.data(), text.data() + text.size(), ignored_pool_size).ec !=
            std::errc {}) {
            return fmt::format("invalid AWS_MAX_CONNECTIONS value \"{}\"", text);
        }
    }
    if (auto error = read_timeouts(props, client.connection)) {
        return *error;
    }
    return client;
}

namespace {
template <class T>
Parsed<ObjStorageClientConfig> as_client_config(Parsed<T> parsed) {
    if (auto* error = std::get_if<std::string>(&parsed)) {
        return std::move(*error);
    }
    return ObjStorageClientConfig {std::get<T>(std::move(parsed))};
}

Parsed<ObjStorageClientConfig> parse_obj_storage_client_config(Vault::Provider provider,
                                                               const Props& props,
                                                               AwsCredentialImplementation impl) {
    if (provider != Vault::GCP &&
        (props.contains(std::string(GCP_CREDENTIAL_PROVIDER_TYPE)) ||
         props.contains(std::string(GCP_IMPERSONATION_SERVICE_ACCOUNT)))) {
        return std::string("GCP credentials require provider=GCP");
    }
    switch (provider) {
    case Vault::S3:
        return as_client_config(parse_aws_obj_storage_config(props, impl));
    case Vault::OSS:
        return as_client_config(parse_oss_obj_storage_config(props));
    case Vault::COS:
        return as_client_config(parse_cos_obj_storage_config(props));
    case Vault::BOS:
        return as_client_config(parse_bos_obj_storage_config(props));
    case Vault::OBS:
        return as_client_config(parse_obs_obj_storage_config(props));
    case Vault::TOS:
        return as_client_config(parse_tos_obj_storage_config(props));
    case Vault::GCP:
        return as_client_config(parse_gcp_obj_storage_config(props, impl));
    case Vault::AZURE:
        return as_client_config(parse_azure_obj_storage_config(props));
    default:
        return fmt::format("unknown object storage provider {}", static_cast<int>(provider));
    }
}

void apply_aws_credential_defaults(Props& props, AwsCredentialImplementation implementation,
                                   EmptyCredentialsBehavior empty_credentials) {
    if (implementation == AwsCredentialImplementation::V1 &&
        cred_provider_type_from_string(value(props, "AWS_CREDENTIALS_PROVIDER_TYPE")) !=
                CredProviderType::InstanceProfile) {
        // BE v1 historically uses anonymous access for an empty key pair, while
        // Recycler v1 uses the SDK default chain, including legacy GCS HMAC access.
        props["AWS_CREDENTIALS_PROVIDER_TYPE"] =
                empty_credentials == EmptyCredentialsBehavior::ANONYMOUS ? "ANONYMOUS" : "DEFAULT";
    }
}

Parsed<ObjStorageResourceConfig> parse_resource(ObjStorageProperties properties,
                                                Vault::Provider provider, std::string bucket,
                                                std::string prefix,
                                                AwsCredentialImplementation implementation,
                                                EmptyCredentialsBehavior aws_empty_credentials) {
    if (provider == Vault::S3 ||
        (provider == Vault::GCP &&
         !properties.contains(std::string(GCP_CREDENTIAL_PROVIDER_TYPE)) &&
         !properties.contains(std::string(GCP_IMPERSONATION_SERVICE_ACCOUNT)))) {
        apply_aws_credential_defaults(properties, implementation, aws_empty_credentials);
    }
    auto parsed = parse_obj_storage_client_config(provider, properties, implementation);
    if (auto* error = std::get_if<std::string>(&parsed)) {
        return std::move(*error);
    }
    if (bucket.empty()) {
        return std::string("object storage bucket is not specified");
    }
    return ObjStorageResourceConfig {.bucket = std::move(bucket),
                                     .prefix = std::move(prefix),
                                     .client = std::get<ObjStorageClientConfig>(std::move(parsed))};
}
} // namespace

ObjStorageConfigParseResult<ObjStorageResourceConfig>
ObjStorageConfigParser::from_azure_client_config(const TAzureClientConfig& config) {
    // A query here would mix credentials with the SAS query added by the SDK.
    // Keep malformed URLs and their possible credentials out of error messages.
    brpc::URI endpoint;
    if ((!config.endpoint.starts_with("http://") && !config.endpoint.starts_with("https://")) ||
        config.endpoint.find('\0') != std::string::npos ||
        config.endpoint.find_first_of("?#@\r\n\t ") != std::string::npos ||
        endpoint.SetHttpURL(config.endpoint.c_str()) != 0 || endpoint.host().empty() ||
        (endpoint.scheme() != "http" && endpoint.scheme() != "https")) {
        return std::string(
                "Azure endpoint must be an HTTP(S) service URL without "
                "credentials, query or fragment");
    }
    if (config.container.empty()) {
        return std::string("Azure container is not specified");
    }
    const auto& credential = config.credential;
    const int credential_count = static_cast<int>(credential.__isset.shared_key) +
                                 static_cast<int>(credential.__isset.sas) +
                                 static_cast<int>(credential.__isset.client_secret);
    if (!config.__isset.credential || credential_count != 1) {
        return std::string("Azure client config must carry exactly one credential");
    }
    AzureObjStorageConfig client;
    client.connection.endpoint = config.endpoint;
    client.auth.account_name = config.account_name;
    if (credential.__isset.shared_key) {
        client.auth.credential = AzureSharedKeyConfig {credential.shared_key.account_key};
    } else if (credential.__isset.sas) {
        std::string_view token = credential.sas.token;
        if (!token.empty() && token.front() == '?') {
            token.remove_prefix(1);
        }
        client.auth.credential = AzureSasConfig {
                .token = std::string(token),
                .expires_at_ms = credential.sas.__isset.expires_at_ms
                                         ? std::optional<int64_t>(credential.sas.expires_at_ms)
                                         : std::nullopt};
    } else {
        const auto& secret = credential.client_secret;
        client.auth.credential = AzureClientSecretConfig {.tenant_id = secret.tenant_id,
                                                          .client_id = secret.client_id,
                                                          .client_secret = secret.client_secret,
                                                          .authority_host = secret.authority_host};
    }
    if (auto error = client.validate()) {
        return std::move(*error);
    }
    return ObjStorageResourceConfig {.bucket = config.container, .client = std::move(client)};
}

ObjStorageConfigParseResult<ObjStorageResourceConfig> ObjStorageConfigParser::from_vault(
        const cloud::ObjectStoreInfoPB& info, AwsCredentialImplementation implementation,
        EmptyCredentialsBehavior aws_empty_credentials) {
    ObjStorageProperties properties;
    if (info.has_endpoint()) {
        properties["AWS_ENDPOINT"] = info.endpoint();
    }
    if (info.has_region()) {
        properties["AWS_REGION"] = info.region();
    }
    if (info.has_ak()) {
        properties["AWS_ACCESS_KEY"] = info.ak();
    }
    if (info.has_sk()) {
        properties["AWS_SECRET_KEY"] = info.sk();
    }
    if (info.has_role_arn()) {
        properties["AWS_ROLE_ARN"] = info.role_arn();
    }
    if (info.has_external_id()) {
        properties["AWS_EXTERNAL_ID"] = info.external_id();
    }

    if (info.has_use_path_style()) {
        properties["use_path_style"] = info.use_path_style() ? "true" : "false";
    }
    if (info.has_cred_provider_type()) {
        properties["AWS_CREDENTIALS_PROVIDER_TYPE"] =
                cloud::CredProviderTypePB_Name(info.cred_provider_type());
    }
    if (info.credential().has_gcp_credential()) {
        const auto& gcp = info.credential().gcp_credential();
        properties[std::string(GCP_CREDENTIAL_PROVIDER_TYPE)] =
                cloud::GcpCredentialPB::CredentialProviderType_Name(gcp.credential_provider_type());
        if (gcp.has_impersonation_service_account()) {
            properties[std::string(GCP_IMPERSONATION_SERVICE_ACCOUNT)] =
                    gcp.impersonation_service_account();
        }
    }
    if (info.bucket().empty()) {
        return "Object storage bucket is not specified";
    }
    auto parsed = parse_resource(std::move(properties), vault_storage_provider(info), info.bucket(),
                                 info.prefix(), implementation, aws_empty_credentials);
    if (auto* resource = std::get_if<ObjStorageResourceConfig>(&parsed)) {
        resource->sse_enabled = info.sse_enabled();
    }
    return parsed;
}

ObjStorageConfigParseResult<ObjStorageResourceConfig> ObjStorageConfigParser::from_properties(
        const ObjStorageProperties& input, std::string bucket,
        AwsCredentialImplementation implementation,
        EmptyCredentialsBehavior aws_empty_credentials) {
    ObjStorageProperties properties;
    for (const auto& [key, v] : input) {
        auto normalized = to_upper(key);
        if (!normalized.starts_with("AWS_")) {
            std::transform(key.begin(), key.end(), normalized.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        }
        properties.try_emplace(std::move(normalized), v);
    }
    // FE's generic S3 backend map omits provider even for explicitly selected S3
    // access to another cloud. Inferring GCP from storage.googleapis.com here
    // would reject its ENV credentials instead of preserving the S3 auth path.
    auto provider = Vault::S3;
    const auto name = normalize_provider(value(properties, "provider"));
    if (!name.empty()) {
        const auto entry = std::ranges::find(PROVIDERS, name, &ProviderMapping::name);
        if (entry != std::end(PROVIDERS)) {
            provider = entry->vault;
        }
    }
    return parse_resource(std::move(properties), provider, std::move(bucket), {}, implementation,
                          aws_empty_credentials);
}

ObjStorageConfigParseResult<ObjStorageResourceConfig> ObjStorageConfigParser::from_storage_param(
        const TS3StorageParam& param, AwsCredentialImplementation implementation,
        EmptyCredentialsBehavior aws_empty_credentials) {
    Vault::Provider provider;
    if (param.provider == TObjStorageType::UNKNOWN) {
        // Legacy resource pushes omit provider even for OSS/COS endpoints. They used
        // the AWS-compatible credential chain, which endpoint inference can discard.
        provider = Vault::S3;
    } else {
        auto entry = std::ranges::find(PROVIDERS, param.provider, &ProviderMapping::resource);
        if (entry == std::end(PROVIDERS)) {
            return fmt::format("unknown storage resource provider {}",
                               static_cast<int>(param.provider));
        }
        provider = entry->vault;
    }

    // The generated Thrift fields carry their protocol defaults even when omitted.
    ObjStorageProperties properties {
            {"AWS_REQUEST_TIMEOUT_MS", std::to_string(param.request_timeout_ms)},
            {"AWS_CONNECTION_TIMEOUT_MS", std::to_string(param.conn_timeout_ms)},
            {"use_path_style", param.use_path_style ? "true" : "false"},
    };
    // Legacy Azure resources carry this S3 field, but the Azure SDK path never used it.
    if (provider != Vault::AZURE) {
        properties["AWS_MAX_CONNECTIONS"] = std::to_string(param.max_conn);
    }
    if (param.__isset.endpoint || !param.endpoint.empty()) {
        properties["AWS_ENDPOINT"] = param.endpoint;
    }
    if (param.__isset.region || !param.region.empty()) {
        properties["AWS_REGION"] = param.region;
    }
    if (param.__isset.ak || !param.ak.empty()) {
        properties["AWS_ACCESS_KEY"] = param.ak;
    }
    if (param.__isset.sk || !param.sk.empty()) {
        properties["AWS_SECRET_KEY"] = param.sk;
    }
    if (param.__isset.role_arn || !param.role_arn.empty()) {
        properties["AWS_ROLE_ARN"] = param.role_arn;
    }
    if (param.__isset.external_id || !param.external_id.empty()) {
        properties["AWS_EXTERNAL_ID"] = param.external_id;
    }
    if (param.__isset.token || !param.token.empty()) {
        properties["AWS_TOKEN"] = param.token;
    }

    if (param.__isset.cred_provider_type) {
        auto name = credential_provider_name(param.cred_provider_type);
        if (!name) {
            return fmt::format("unknown credential provider {}",
                               static_cast<int>(param.cred_provider_type));
        }
        properties["AWS_CREDENTIALS_PROVIDER_TYPE"] = *name;
    }
    if (param.__isset.credential && param.credential.__isset.gcp_credential) {
        const auto& gcp = param.credential.gcp_credential;
        auto type = gcp.__isset.credential_provider_type ? gcp.credential_provider_type
                                                         : TGcpCredentialProviderType::DEFAULT;
        auto name = gcp_credential_provider_name(type);
        if (!name) {
            return fmt::format("unknown GCP credential provider {}", static_cast<int>(type));
        }
        properties[std::string(GCP_CREDENTIAL_PROVIDER_TYPE)] = *name;
        if (gcp.__isset.impersonation_service_account) {
            properties[std::string(GCP_IMPERSONATION_SERVICE_ACCOUNT)] =
                    gcp.impersonation_service_account;
        }
    }
    return parse_resource(std::move(properties), provider, param.bucket, param.root_path,
                          implementation, aws_empty_credentials);
}

} // namespace doris
