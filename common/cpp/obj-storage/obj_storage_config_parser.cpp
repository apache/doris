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

#include "cpp/obj-storage/obj_storage_config_parser.h"

#include <brpc/uri.h>
#include <fmt/format.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <utility>

#include "cpp/obj-storage/auth/gcp/gcp_auth.h"

namespace doris {
namespace {
using Properties = std::map<std::string, std::string>;
using Error = std::optional<std::string>;
using Vault = cloud::ObjectStoreInfoPB;
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

void normalize_obj_storage_endpoint(ObjStorageClientConfig& config, std::string_view scheme) {
    auto& endpoint = obj_storage_connection(config).endpoint;
    const bool http = scheme == "http";
    if (auto* aws = std::get_if<AwsObjStorageConfig>(&config); aws && aws->use_sdk_endpoint) {
        aws->sdk_endpoint_http = http;
        endpoint.clear();
        return;
    }
    if (!endpoint.empty() && endpoint.find("://") == std::string::npos) {
        endpoint.insert(0, http && !std::holds_alternative<AzureObjStorageConfig>(config)
                                   ? "http://"
                                   : "https://");
    }
}

namespace {
std::string value(const Properties& p, const char* key) {
    auto it = p.find(key);
    return it == p.end() ? std::string {} : it->second;
}

template <typename T>
Error read_number(const Properties& p, const char* key, std::optional<T>* result) {
    auto it = p.find(key);
    if (it == p.end()) {
        return {};
    }
    const auto& text = it->second;
    int number = 0;
    const auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), number);
    if (ec != std::errc {} || end != text.data() + text.size() || number < -1) {
        return fmt::format("invalid {} value \"{}\"", key, text);
    }
    // Old resources use -1 for an unspecified SDK option. Zero is an explicit input.
    if (number != -1) {
        *result = T(number);
    }
    return {};
}
template <typename C>
Error parse_connection(const Properties& p, C& c) {
    c.connection.endpoint = value(p, "AWS_ENDPOINT");
    c.region = value(p, "AWS_REGION");
    c.use_path_style = value(p, "use_path_style") == "true";
    if (auto e = read_number(p, "AWS_MAX_CONNECTIONS", &c.connection.max_connections)) {
        return e;
    }
    if (auto e = read_number(p, "AWS_REQUEST_TIMEOUT_MS", &c.connection.request_timeout)) {
        return e;
    }
    return read_number(p, "AWS_CONNECTION_TIMEOUT_MS", &c.connection.connect_timeout);
}

AccessKeyObjStorageAuth keys(const Properties& p) {
    return {value(p, "AWS_ACCESS_KEY"), value(p, "AWS_SECRET_KEY"), value(p, "AWS_TOKEN")};
}
AwsCredentialChainType chain_type(CredProviderType mode) {
    switch (mode) {
    case CredProviderType::Default:
    case CredProviderType::Simple:
        return AwsCredentialChainType::Default;
    case CredProviderType::Env:
        return AwsCredentialChainType::Environment;
    case CredProviderType::SystemProperties:
        return AwsCredentialChainType::Profile;
    case CredProviderType::InstanceProfile:
        return AwsCredentialChainType::InstanceProfile;
    case CredProviderType::Container:
        return AwsCredentialChainType::Container;
    case CredProviderType::WebIdentity:
        return AwsCredentialChainType::WebIdentity;
    case CredProviderType::Anonymous:
        __builtin_unreachable();
    }
    __builtin_unreachable();
}
AwsCredentialSource select_source(CredProviderType mode, AwsCredentialImplementation implementation,
                                  EmptyCredentialsBehavior empty,
                                  AccessKeyObjStorageAuth credentials) {
    if (!credentials.access_key.empty() && !credentials.secret_key.empty()) {
        return credentials;
    }
    if (implementation == AwsCredentialImplementation::V1) {
        if (mode == CredProviderType::InstanceProfile) {
            return AwsCredentialChainType::InstanceProfile;
        }
        if (credentials.access_key.empty() && credentials.secret_key.empty() &&
            empty == EmptyCredentialsBehavior::ANONYMOUS) {
            return AnonymousObjStorageAuth {};
        }
        return AwsCredentialChainType::Default;
    }
    if (mode == CredProviderType::Anonymous) {
        return AnonymousObjStorageAuth {};
    }
    return chain_type(mode);
}
bool has_partial_keys(const AccessKeyObjStorageAuth& k) {
    return k.access_key.empty() != k.secret_key.empty();
}
Error parse_aws_obj_storage_config(const Properties& p, AwsCredentialImplementation impl,
                                   EmptyCredentialsBehavior empty, AwsObjStorageConfig& c) {
    if (auto e = parse_connection(p, c)) {
        return e;
    }
    const auto override = p.find("AWS_NEED_OVERRIDE_ENDPOINT");
    c.use_sdk_endpoint = override != p.end() && override->second != "true";
    auto k = keys(p);
    auto role = value(p, "AWS_ROLE_ARN");
    if (role.empty() && has_partial_keys(k)) {
        return "Incomplete access key credentials";
    }
    const auto mode = cred_provider_type_from_string(value(p, "AWS_CREDENTIALS_PROVIDER_TYPE"));
    c.auth.implementation = impl;
    c.auth.source = select_source(mode, impl, empty, std::move(k));
    if (!role.empty() && !std::holds_alternative<AccessKeyObjStorageAuth>(c.auth.source) &&
        (impl == AwsCredentialImplementation::V2 || mode == CredProviderType::InstanceProfile)) {
        c.auth.assume_role = AwsAssumeRole {.role_arn = std::move(role)};
        if (auto external = value(p, "AWS_EXTERNAL_ID"); !external.empty()) {
            c.auth.assume_role->external_id = std::move(external);
        }
    }
    return {};
}
AccessKeyOrAnonymousObjStorageAuth compatible_auth(const Properties& p,
                                                   AwsCredentialImplementation impl,
                                                   EmptyCredentialsBehavior empty) {
    const auto mode = cred_provider_type_from_string(value(p, "AWS_CREDENTIALS_PROVIDER_TYPE"));
    auto source = select_source(mode, impl, empty, keys(p));
    if (auto* k = std::get_if<AccessKeyObjStorageAuth>(&source)) {
        return std::move(*k);
    }
    if (auto* chain = std::get_if<AwsCredentialChainType>(&source)) {
        return ObjStorageCredentialChain {*chain, impl};
    }
    return AnonymousObjStorageAuth {};
}

template <typename C>
Error parse_compatible_connection(const Properties& p, C& c) {
    if (auto it = p.find("AWS_NEED_OVERRIDE_ENDPOINT"); it != p.end() && it->second != "true") {
        return "AWS_NEED_OVERRIDE_ENDPOINT=false is supported only for AWS";
    }
    return parse_connection(p, c);
}

template <typename C>
Error parse_access_key_config(const Properties& p, AwsCredentialImplementation impl,
                              EmptyCredentialsBehavior empty, C& c) {
    if (auto e = parse_compatible_connection(p, c)) {
        return e;
    }
    if (has_partial_keys(keys(p))) {
        return "Incomplete access key credentials";
    }
    c.auth = compatible_auth(p, impl, empty);
    return {};
}
Error parse_oss_obj_storage_config(const Properties& p, AwsCredentialImplementation impl,
                                   EmptyCredentialsBehavior empty, OssObjStorageConfig& c) {
    return parse_access_key_config(p, impl, empty, c);
}
Error parse_cos_obj_storage_config(const Properties& p, AwsCredentialImplementation impl,
                                   EmptyCredentialsBehavior empty, CosObjStorageConfig& c) {
    return parse_access_key_config(p, impl, empty, c);
}
Error parse_bos_obj_storage_config(const Properties& p, AwsCredentialImplementation impl,
                                   EmptyCredentialsBehavior empty, BosObjStorageConfig& c) {
    return parse_access_key_config(p, impl, empty, c);
}
Error parse_obs_obj_storage_config(const Properties& p, AwsCredentialImplementation impl,
                                   EmptyCredentialsBehavior empty, ObsObjStorageConfig& c) {
    return parse_access_key_config(p, impl, empty, c);
}
Error parse_tos_obj_storage_config(const Properties& p, AwsCredentialImplementation impl,
                                   EmptyCredentialsBehavior empty, TosObjStorageConfig& c) {
    return parse_access_key_config(p, impl, empty, c);
}

Error parse_gcp_obj_storage_config(const Properties& p, AwsCredentialImplementation impl,
                                   EmptyCredentialsBehavior empty, GcpObjStorageConfig& c) {
    if (auto e = parse_compatible_connection(p, c)) {
        return e;
    }
    auto native = parse_gcp_credential_properties(make_property_getter(p));
    if (native.error) {
        return native.error;
    }
    auto k = keys(p);
    if (native.credential) {
        if (auto e = validate_gcp_credential(
                    *native.credential,
                    {.provider = ObjCredentialProvider::Gcp,
                     .ak = k.access_key,
                     .sk = k.secret_key,
                     .token = k.session_token,
                     .has_aws_role_arn = !value(p, "AWS_ROLE_ARN").empty(),
                     .has_aws_external_id = !value(p, "AWS_EXTERNAL_ID").empty(),
                     .has_aws_credential_provider = cred_provider_type_from_string(value(
                                                            p, "AWS_CREDENTIALS_PROVIDER_TYPE")) !=
                                                    CredProviderType::Default})) {
            return e;
        }
        GcpNativeObjStorageAuth auth;
        if (native.credential->provider_type == GcpCredentialProviderType::ComputeEngine) {
            auth.source = GcpCredentialSource::ComputeEngine;
        }
        if (!native.credential->impersonation_service_account.empty()) {
            auth.impersonation_service_account =
                    std::move(native.credential->impersonation_service_account);
        }
        c.auth = std::move(auth);
    } else {
        if (has_partial_keys(k)) {
            return "Incomplete access key credentials";
        }
        auto source = compatible_auth(p, impl, empty);
        if (auto* keys = std::get_if<AccessKeyObjStorageAuth>(&source)) {
            c.auth =
                    GcpHmacObjStorageAuth {std::move(keys->access_key), std::move(keys->secret_key),
                                           std::move(keys->session_token)};
        } else if (auto* chain = std::get_if<ObjStorageCredentialChain>(&source)) {
            c.auth = *chain;
        }
    }
    return {};
}
Error parse_azure_obj_storage_config(const Properties& p, AzureObjStorageConfig& c) {
    c.connection.endpoint = value(p, "AWS_ENDPOINT");
    // Region, path style, session token and the S3 connection pool have no Blob SDK equivalent.
    if (auto e = read_number(p, "AWS_REQUEST_TIMEOUT_MS", &c.connection.request_timeout)) {
        return e;
    }
    if (auto e = read_number(p, "AWS_CONNECTION_TIMEOUT_MS", &c.connection.connect_timeout)) {
        return e;
    }
    c.auth = {value(p, "AWS_ACCESS_KEY"), value(p, "AWS_SECRET_KEY")};
    if (c.auth.account_key.empty() != c.auth.account_name.empty()) {
        return "Incomplete Azure shared key credentials";
    }
    return {};
}
} // namespace

Error parse_obj_storage_client_config(Vault::Provider provider, const Properties& properties,
                                      AwsCredentialImplementation implementation,
                                      EmptyCredentialsBehavior empty,
                                      ObjStorageClientConfig* result) {
    if (provider != Vault::GCP &&
        (properties.contains(std::string(GCP_CREDENTIAL_PROVIDER_TYPE)) ||
         properties.contains(std::string(GCP_IMPERSONATION_SERVICE_ACCOUNT)))) {
        return "GCP credentials require provider=GCP";
    }
    ObjStorageClientConfig parsed;
    Error error;
    switch (provider) {
    case Vault::S3:
        error = parse_aws_obj_storage_config(properties, implementation, empty,
                                             parsed.emplace<AwsObjStorageConfig>());
        break;
    case Vault::OSS:
        error = parse_oss_obj_storage_config(properties, implementation, empty,
                                             parsed.emplace<OssObjStorageConfig>());
        break;
    case Vault::COS:
        error = parse_cos_obj_storage_config(properties, implementation, empty,
                                             parsed.emplace<CosObjStorageConfig>());
        break;
    case Vault::BOS:
        error = parse_bos_obj_storage_config(properties, implementation, empty,
                                             parsed.emplace<BosObjStorageConfig>());
        break;
    case Vault::OBS:
        error = parse_obs_obj_storage_config(properties, implementation, empty,
                                             parsed.emplace<ObsObjStorageConfig>());
        break;
    case Vault::TOS:
        error = parse_tos_obj_storage_config(properties, implementation, empty,
                                             parsed.emplace<TosObjStorageConfig>());
        break;
    case Vault::GCP:
        error = parse_gcp_obj_storage_config(properties, implementation, empty,
                                             parsed.emplace<GcpObjStorageConfig>());
        break;
    case Vault::AZURE:
        error = parse_azure_obj_storage_config(properties, parsed.emplace<AzureObjStorageConfig>());
        break;

    default:
        return "Unknown object storage provider " + std::to_string(provider);
    }
    if (error) {
        return error;
    }
    *result = std::move(parsed);
    return {};
}

Error parse_vault_storage_config(const Vault& info, AwsCredentialImplementation implementation,
                                 EmptyCredentialsBehavior empty, ObjStorageResourceConfig* result) {
    Properties p;
    if (info.has_endpoint()) {
        p["AWS_ENDPOINT"] = info.endpoint();
    }
    if (info.has_region()) {
        p["AWS_REGION"] = info.region();
    }
    if (info.has_ak()) {
        p["AWS_ACCESS_KEY"] = info.ak();
    }
    if (info.has_sk()) {
        p["AWS_SECRET_KEY"] = info.sk();
    }
    if (info.has_role_arn()) {
        p["AWS_ROLE_ARN"] = info.role_arn();
    }
    if (info.has_external_id()) {
        p["AWS_EXTERNAL_ID"] = info.external_id();
    }
    if (info.has_cred_provider_type()) {
        p["AWS_CREDENTIALS_PROVIDER_TYPE"] =
                cloud::CredProviderTypePB_Name(info.cred_provider_type());
    }
    if (info.has_use_path_style()) {
        p["use_path_style"] = info.use_path_style() ? "true" : "false";
    }
    if (info.credential().has_gcp_credential()) {
        const auto& g = info.credential().gcp_credential();
        p[std::string(GCP_CREDENTIAL_PROVIDER_TYPE)] =
                cloud::GcpCredentialPB::CredentialProviderType_Name(g.credential_provider_type());
        if (g.has_impersonation_service_account()) {
            p[std::string(GCP_IMPERSONATION_SERVICE_ACCOUNT)] = g.impersonation_service_account();
        }
    }
    ObjStorageResourceConfig parsed {
            .bucket = info.bucket(), .prefix = info.prefix(), .sse_enabled = info.sse_enabled()};
    if (parsed.bucket.empty()) {
        return "Object storage vault bucket is not specified";
    }
    if (auto e = parse_obj_storage_client_config(vault_storage_provider(info), p, implementation,
                                                 empty, &parsed.client)) {
        return e;
    }
    *result = std::move(parsed);
    return {};
}
} // namespace doris
