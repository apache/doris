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

#include "io/object_storage/azure_blob_location.h"

#include <absl/strings/match.h>
#include <absl/strings/str_replace.h>

#include <array>
#include <utility>

#include "util/url_coding.h"

namespace doris::io {
namespace {

std::pair<std::string_view, std::string_view> split_once(std::string_view value, char separator) {
    const auto pos = value.find(separator);
    if (pos == std::string_view::npos) {
        return {value, {}};
    }
    return {value.substr(0, pos), value.substr(pos + 1)};
}

std::string_view without_default_port(std::string_view authority, std::string_view port) {
    return authority.ends_with(port) ? authority.substr(0, authority.size() - port.size())
                                     : authority;
}

struct AzureServiceHost {
    std::string_view account;
    std::string_view cloud_and_port;
};

AzureServiceHost azure_service_host(std::string_view authority) {
    auto [account, rest] = split_once(authority, '.');
    auto [service, cloud_and_port] = split_once(rest, '.');
    if (absl::EqualsIgnoreCase(service, "blob") || absl::EqualsIgnoreCase(service, "dfs")) {
        auto cloud = cloud_and_port.substr(0, cloud_and_port.find(':'));
        static constexpr std::array<std::string_view, 5> clouds = {
                "core.windows.net", "core.chinacloudapi.cn", "core.usgovcloudapi.net",
                "core.cloudapi.de", "fabric.microsoft.com"};
        for (auto known_cloud : clouds) {
            if (absl::EqualsIgnoreCase(cloud, known_cloud)) {
                return {.account = account, .cloud_and_port = cloud_and_port};
            }
        }
    }
    return {};
}

} // namespace

Result<AzureBlobLocation> AzureBlobEndpoint::resolve(std::string_view path,
                                                     std::string_view configured_container) const {
    // The SAS travels only in the credential. Never put a signed URL in an error message.
    path = path.substr(0, path.find_first_of("?#"));
    if (path.empty()) {
        return ResultError(Status::InvalidArgument<false>("Azure file location is empty"));
    }
    std::string_view endpoint = url;
    auto separator = endpoint.find("://");
    // The SDK connects using the configured transport, including for wasb/abfs locations.
    const std::string_view default_port =
            absl::EqualsIgnoreCase(endpoint.substr(0, separator), "http") ? ":80" : ":443";
    if (separator != std::string_view::npos) {
        endpoint.remove_prefix(separator + 3);
    }
    auto [endpoint_host, endpoint_path] = split_once(endpoint, '/');
    endpoint_host = without_default_port(endpoint_host, default_port);
    while (endpoint_path.ends_with('/')) {
        endpoint_path.remove_suffix(1);
    }
    auto target = azure_service_host(endpoint_host);
    if (!account.empty() && !target.account.empty() &&
        !absl::EqualsIgnoreCase(account, target.account)) {
        return ResultError(Status::InvalidArgument<false>(
                "Azure account does not match the service endpoint"));
    }

    auto scheme_end = path.find("://");
    if (scheme_end == std::string_view::npos) {
        if (configured_container.empty()) {
            return ResultError(Status::InvalidArgument<false>(
                    "Azure container is required for a relative key"));
        }
        return AzureBlobLocation {.container = std::string(configured_container),
                                  .key = std::string(path)};
    }

    auto scheme = path.substr(0, scheme_end);
    const bool is_http =
            absl::EqualsIgnoreCase(scheme, "http") || absl::EqualsIgnoreCase(scheme, "https");
    auto [authority, key] = split_once(path.substr(scheme_end + 3), '/');
    std::string_view container;
    std::string_view host;
    if (absl::EqualsIgnoreCase(scheme, "abfs") || absl::EqualsIgnoreCase(scheme, "abfss") ||
        absl::EqualsIgnoreCase(scheme, "wasb") || absl::EqualsIgnoreCase(scheme, "wasbs")) {
        auto at = authority.find('@');
        if (at == std::string_view::npos || at + 1 == authority.size() ||
            authority.find('@', at + 1) != std::string_view::npos) {
            return ResultError(
                    Status::InvalidArgument<false>("Azure location requires container@host"));
        }
        container = authority.substr(0, at);
        host = authority.substr(at + 1);
    } else if (is_http) {
        if (authority.empty() || authority.find('@') != std::string_view::npos) {
            return ResultError(Status::InvalidArgument<false>("Invalid Azure storage host"));
        }
        host = authority;
        // A path-style Blob service (e.g. Azurite) includes the account in its base URL.
        if (!endpoint_path.empty()) {
            if (!key.starts_with(endpoint_path) || key.size() <= endpoint_path.size() ||
                key[endpoint_path.size()] != '/') {
                return ResultError(Status::InvalidArgument<false>(
                        "Azure location is outside the service endpoint"));
            }
            key.remove_prefix(endpoint_path.size() + 1);
        }
        auto parts = split_once(key, '/');
        container = parts.first;
        key = parts.second;
    } else if (absl::EqualsIgnoreCase(scheme, "s3")) {
        container = authority;
    } else {
        return ResultError(Status::InvalidArgument<false>("Unsupported Azure storage URI scheme"));
    }
    if (auto file_host = without_default_port(host, default_port);
        !host.empty() && !absl::EqualsIgnoreCase(file_host, endpoint_host)) {
        // Only a standard Blob/DFS pair for the same account, cloud and port is interchangeable.
        auto file = azure_service_host(file_host);
        if (!endpoint_path.empty() || target.account.empty() ||
            !absl::EqualsIgnoreCase(file.account, target.account) ||
            !absl::EqualsIgnoreCase(file.cloud_and_port, target.cloud_and_port)) {
            return ResultError(Status::InvalidArgument<false>(
                    "Azure file location does not match the service endpoint"));
        }
    }
    if (container.empty() || (!configured_container.empty() && container != configured_container)) {
        return ResultError(Status::InvalidArgument<false>(
                "Azure file location has a missing or mismatched container"));
    }
    AzureBlobLocation location {.container = std::string(container), .key = {}};
    if (is_http && key.find('%') != std::string_view::npos) {
        // HTTP locations carry a URL path. The Blob SDK takes the decoded blob name and
        // escapes it itself. Keep a literal '+' when using the form-style percent decoder.
        if (!url_decode(absl::StrReplaceAll(key, {{"+", "%2B"}}), &location.key)) {
            return ResultError(Status::InvalidArgument<false>("Invalid escape in Azure blob URL"));
        }
    } else {
        location.key = key;
    }
    return location;
}

} // namespace doris::io
