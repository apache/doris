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

#include "cpp/obj-storage/obj_storage_properties.h"

#include <brpc/uri.h>
#include <fmt/format.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <type_traits>
#include <utility>

#include "cpp/obj-storage/auth/gcp/gcp_auth.h"

namespace doris {
namespace {
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
    using Vault = cloud::ObjectStoreInfoPB;
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
using Parsed = std::variant<T, std::string>;

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
    // Legacy resources use -1 to request host defaults.
    if (number >= 0) {
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

Parsed<S3CompatibleObjStorageAuth> parse_s3_credentials(const Props& props,
                                                        AwsCredentialImplementation impl,
                                                        EmptyCredentialsBehavior empty) {
    AccessKeyObjStorageAuth keys {value(props, "AWS_ACCESS_KEY"), value(props, "AWS_SECRET_KEY"),
                                  value(props, "AWS_TOKEN")};
    if (!keys.access_key.empty() && !keys.secret_key.empty()) {
        return S3CompatibleObjStorageAuth {std::move(keys)};
    }

    auto arn = value(props, "AWS_ROLE_ARN");
    bool partial = keys.access_key.empty() != keys.secret_key.empty();
    if (partial && arn.empty()) {
        return std::string("Incomplete access key credentials");
    }
    auto mode = cred_provider_type_from_string(value(props, "AWS_CREDENTIALS_PROVIDER_TYPE"));
    S3CredentialChain chain;
    chain.implementation = impl;
    if (impl == AwsCredentialImplementation::V1 && mode != CredProviderType::InstanceProfile) {
        // Legacy v1 only used AssumeRole with the instance profile. An incomplete key
        // pair with a role bypassed the anonymous fallback and selected the SDK chain.
        if (!partial && empty == EmptyCredentialsBehavior::ANONYMOUS) {
            return S3CompatibleObjStorageAuth {AnonymousObjStorageAuth {}};
        }
        chain.source = AwsCredentialChainType::Default;
    } else {
        if (mode != CredProviderType::Anonymous) {
            chain.source = chain_source(mode);
        }
        if (!arn.empty()) {
            AwsAssumeRole role {.role_arn = std::move(arn)};
            if (auto ext = value(props, "AWS_EXTERNAL_ID"); !ext.empty()) {
                role.external_id = std::move(ext);
            }
            chain.assume_role = std::move(role);
        } else if (mode == CredProviderType::Anonymous) {
            return S3CompatibleObjStorageAuth {AnonymousObjStorageAuth {}};
        }
    }
    return S3CompatibleObjStorageAuth {std::move(chain)};
}

Parsed<AwsObjStorageConfig> parse_aws_obj_storage_config(const Props& props,
                                                         AwsCredentialImplementation impl,
                                                         EmptyCredentialsBehavior empty) {
    AwsObjStorageConfig client;
    if (auto e = read_regional_options(props, client)) {
        return *e;
    }
    auto credentials = parse_s3_credentials(props, impl, empty);
    if (auto* error = std::get_if<std::string>(&credentials)) {
        return std::move(*error);
    }
    client.auth.implementation = impl;
    std::visit(
            [&]<class T>(T&& selected) {
                if constexpr (std::is_same_v<std::decay_t<T>, S3CredentialChain>) {
                    std::visit([&](auto source) { client.auth.source = source; }, selected.source);
                    client.auth.assume_role = std::move(selected.assume_role);
                } else {
                    client.auth.source = std::move(selected);
                }
            },
            std::get<S3CompatibleObjStorageAuth>(std::move(credentials)));
    return client;
}

template <class C>
Parsed<C> parse_access_key_client(const Props& props, AwsCredentialImplementation impl,
                                  EmptyCredentialsBehavior empty) {
    C client;
    if (auto e = read_regional_options(props, client)) {
        return *e;
    }
    auto credentials = parse_s3_credentials(props, impl, empty);
    if (auto* error = std::get_if<std::string>(&credentials)) {
        return std::move(*error);
    }
    client.auth = std::get<S3CompatibleObjStorageAuth>(std::move(credentials));
    return client;
}
Parsed<OssObjStorageConfig> parse_oss_obj_storage_config(const Props& props,
                                                         AwsCredentialImplementation impl,
                                                         EmptyCredentialsBehavior empty) {
    return parse_access_key_client<OssObjStorageConfig>(props, impl, empty);
}
Parsed<CosObjStorageConfig> parse_cos_obj_storage_config(const Props& props,
                                                         AwsCredentialImplementation impl,
                                                         EmptyCredentialsBehavior empty) {
    return parse_access_key_client<CosObjStorageConfig>(props, impl, empty);
}
Parsed<BosObjStorageConfig> parse_bos_obj_storage_config(const Props& props,
                                                         AwsCredentialImplementation impl,
                                                         EmptyCredentialsBehavior empty) {
    return parse_access_key_client<BosObjStorageConfig>(props, impl, empty);
}
Parsed<ObsObjStorageConfig> parse_obs_obj_storage_config(const Props& props,
                                                         AwsCredentialImplementation impl,
                                                         EmptyCredentialsBehavior empty) {
    return parse_access_key_client<ObsObjStorageConfig>(props, impl, empty);
}
Parsed<TosObjStorageConfig> parse_tos_obj_storage_config(const Props& props,
                                                         AwsCredentialImplementation impl,
                                                         EmptyCredentialsBehavior empty) {
    return parse_access_key_client<TosObjStorageConfig>(props, impl, empty);
}

Parsed<GcpObjStorageConfig> parse_gcp_obj_storage_config(const Props& props,
                                                         AwsCredentialImplementation impl,
                                                         EmptyCredentialsBehavior empty) {
    GcpObjStorageConfig client;
    if (auto e = read_regional_options(props, client)) {
        return *e;
    }
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
        auto credentials = parse_s3_credentials(props, impl, empty);
        if (auto* error = std::get_if<std::string>(&credentials)) {
            return std::move(*error);
        }
        std::visit(
                [&]<class T>(T&& selected) {
                    if constexpr (std::is_same_v<std::decay_t<T>, AccessKeyObjStorageAuth>) {
                        client.auth = GcpHmacObjStorageAuth {std::move(selected.access_key),
                                                             std::move(selected.secret_key),
                                                             std::move(selected.session_token)};
                    } else {
                        client.auth = std::move(selected);
                    }
                },
                std::get<S3CompatibleObjStorageAuth>(std::move(credentials)));
    }
    return client;
}
Parsed<AzureObjStorageConfig> parse_azure_obj_storage_config(const Props& props) {
    AzureObjStorageConfig client;
    client.connection.endpoint = value(props, "AWS_ENDPOINT");
    client.auth = {value(props, "AWS_ACCESS_KEY"), value(props, "AWS_SECRET_KEY")};
    if (client.auth.account_name.empty() != client.auth.account_key.empty()) {
        return std::string("Incomplete Azure shared key credentials");
    }
    if (auto error = read_number(props, "AWS_MAX_CONNECTIONS", client.connection.max_connections)) {
        return *error;
    }
    if (client.connection.max_connections) {
        return std::string("Azure does not support AWS_MAX_CONNECTIONS");
    }
    if (auto error = read_timeouts(props, client.connection)) {
        return *error;
    }
    return client;
}

template <class T>
Error store(Parsed<T> parsed, ObjStorageClientConfig* result) {
    if (auto* error = std::get_if<std::string>(&parsed)) {
        return std::move(*error);
    }
    *result = std::get<T>(std::move(parsed));
    return {};
}
} // namespace

Error parse_obj_storage_client_config(cloud::ObjectStoreInfoPB::Provider provider,
                                      const Props& props, AwsCredentialImplementation impl,
                                      EmptyCredentialsBehavior empty,
                                      ObjStorageClientConfig* result) {
    using Vault = cloud::ObjectStoreInfoPB;
    if (provider != Vault::GCP &&
        (props.contains(std::string(GCP_CREDENTIAL_PROVIDER_TYPE)) ||
         props.contains(std::string(GCP_IMPERSONATION_SERVICE_ACCOUNT)))) {
        return "GCP credentials require provider=GCP";
    }
    switch (provider) {
    case Vault::S3:
        return store(parse_aws_obj_storage_config(props, impl, empty), result);
    case Vault::OSS:
        return store(parse_oss_obj_storage_config(props, impl, empty), result);
    case Vault::COS:
        return store(parse_cos_obj_storage_config(props, impl, empty), result);
    case Vault::BOS:
        return store(parse_bos_obj_storage_config(props, impl, empty), result);
    case Vault::OBS:
        return store(parse_obs_obj_storage_config(props, impl, empty), result);
    case Vault::TOS:
        return store(parse_tos_obj_storage_config(props, impl, empty), result);
    case Vault::GCP:
        return store(parse_gcp_obj_storage_config(props, impl, empty), result);
    case Vault::AZURE:
        return store(parse_azure_obj_storage_config(props), result);
    default:
        return fmt::format("unknown object storage provider {}", static_cast<int>(provider));
    }
}

Error parse_vault_storage_config(const cloud::ObjectStoreInfoPB& info,
                                 AwsCredentialImplementation impl, EmptyCredentialsBehavior empty,
                                 ObjStorageResourceConfig* result) {
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
    ObjStorageResourceConfig resource {
            .bucket = info.bucket(), .prefix = info.prefix(), .sse_enabled = info.sse_enabled()};
    if (auto e = parse_obj_storage_client_config(vault_storage_provider(info), properties, impl,
                                                 empty, &resource.client)) {
        return e;
    }
    *result = std::move(resource);
    return {};
}

} // namespace doris
