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

#include <array>
#include <utility>

namespace doris::io {
namespace {

std::pair<std::string_view, std::string_view> split_at_slash(std::string_view value) {
    const auto slash = value.find('/');
    if (slash == std::string_view::npos) {
        return {value, {}};
    }
    return {value.substr(0, slash), value.substr(slash + 1)};
}

std::string_view account_of(std::string_view authority) {
    static constexpr std::array<std::string_view, 8> suffixes = {
            ".blob.core.windows.net",       ".dfs.core.windows.net",
            ".blob.core.chinacloudapi.cn",  ".dfs.core.chinacloudapi.cn",
            ".blob.core.usgovcloudapi.net", ".dfs.core.usgovcloudapi.net",
            ".blob.core.cloudapi.de",       ".dfs.core.cloudapi.de"};
    auto host = authority.substr(0, authority.find(':'));
    for (auto suffix : suffixes) {
        if (host.size() > suffix.size() && absl::EndsWithIgnoreCase(host, suffix)) {
            return host.substr(0, host.size() - suffix.size());
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
    if (auto separator = endpoint.find("://"); separator != std::string_view::npos) {
        endpoint.remove_prefix(separator + 3);
    }
    auto [endpoint_host, endpoint_path] = split_at_slash(endpoint);
    auto endpoint_account = account_of(endpoint_host);
    if (!account.empty() && !endpoint_account.empty() &&
        !absl::EqualsIgnoreCase(account, endpoint_account)) {
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
    auto [authority, key] = split_at_slash(path.substr(scheme_end + 3));
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
    } else if (absl::EqualsIgnoreCase(scheme, "http") || absl::EqualsIgnoreCase(scheme, "https")) {
        if (authority.empty() || authority.find('@') != std::string_view::npos) {
            return ResultError(Status::InvalidArgument<false>("Invalid Azure storage host"));
        }
        host = authority;
        // A path-style Blob service (e.g. Azurite) includes the account in its base URL.
        while (endpoint_path.ends_with('/')) {
            endpoint_path.remove_suffix(1);
        }
        if (!endpoint_path.empty() && absl::EqualsIgnoreCase(host, endpoint_host)) {
            if (!key.starts_with(endpoint_path) || key.size() <= endpoint_path.size() ||
                key[endpoint_path.size()] != '/') {
                return ResultError(Status::InvalidArgument<false>(
                        "Azure location is outside the service endpoint"));
            }
            key.remove_prefix(endpoint_path.size() + 1);
        }
        auto parts = split_at_slash(key);
        container = parts.first;
        key = parts.second;
    } else if (absl::EqualsIgnoreCase(scheme, "s3")) {
        container = authority;
    } else {
        return ResultError(Status::InvalidArgument<false>("Unsupported Azure storage URI scheme"));
    }
    const auto file_account = account_of(host);
    const std::string_view target_account =
            account.empty() ? endpoint_account : std::string_view(account);
    if (!file_account.empty() && !target_account.empty() &&
        !absl::EqualsIgnoreCase(file_account, target_account)) {
        return ResultError(
                Status::InvalidArgument<false>("Azure file location belongs to another account"));
    }
    if (container.empty() || (!configured_container.empty() && container != configured_container)) {
        return ResultError(Status::InvalidArgument<false>(
                "Azure file location has a missing or mismatched container"));
    }
    return AzureBlobLocation {.container = std::string(container), .key = std::string(key)};
}

} // namespace doris::io
