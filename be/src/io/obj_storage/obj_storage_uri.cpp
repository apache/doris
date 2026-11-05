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

#include "io/obj_storage/obj_storage_uri.h"

#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_split.h>

#include <array>
#include <string_view>
#include <utility>
#include <vector>

namespace doris::io {
namespace {

// Host suffixes of the public Azure clouds. A host "<account>.<suffix>" names its account.
constexpr std::array<std::string_view, 8> AZURE_STORAGE_HOST_SUFFIXES = {
        ".blob.core.windows.net",     ".dfs.core.windows.net",        ".blob.core.chinacloudapi.cn",
        ".dfs.core.chinacloudapi.cn", ".blob.core.usgovcloudapi.net", ".dfs.core.usgovcloudapi.net",
        ".blob.core.cloudapi.de",     ".dfs.core.cloudapi.de",
};

std::string azure_key_of_path(std::string_view path) {
    return std::string(path.substr(0, path.find_first_of("?#")));
}

} // namespace

std::string_view ObjStorageUri::azure_account_of_endpoint(std::string_view endpoint) {
    size_t scheme = endpoint.find("://");
    if (scheme != std::string_view::npos) {
        endpoint.remove_prefix(scheme + 3);
    }
    std::string_view host = endpoint.substr(0, endpoint.find_first_of(":/"));
    for (std::string_view suffix : AZURE_STORAGE_HOST_SUFFIXES) {
        if (host.size() > suffix.size() &&
            absl::EqualsIgnoreCase(host.substr(host.size() - suffix.size()), suffix)) {
            return host.substr(0, host.size() - suffix.size());
        }
    }
    return {};
}

Result<ObjStorageUri> ObjStorageUri::parse_azure(const std::string& location) {
    if (location.empty()) {
        return ResultError(Status::InvalidArgument("Azure location is empty"));
    }
    ObjStorageUri uri {.original = location, .account {}, .bucket {}, .key {}};
    size_t scheme_end = location.find("://");
    if (scheme_end == std::string::npos) {
        uri.key = azure_key_of_path(location);
        return uri;
    }

    std::string scheme = absl::AsciiStrToLower(location.substr(0, scheme_end));
    std::string_view rest = std::string_view(location).substr(scheme_end + 3);
    size_t authority_end = rest.find('/');
    std::string_view authority = rest.substr(0, authority_end);
    std::string_view path =
            authority_end == std::string_view::npos ? "" : rest.substr(authority_end + 1);

    if (scheme == "abfs" || scheme == "abfss" || scheme == "wasb" || scheme == "wasbs") {
        // container@acct.dfs.core.windows.net
        size_t at = authority.find('@');
        if (at == std::string_view::npos) {
            return ResultError(Status::InvalidArgument(
                    "Invalid Azure location, expected {}://container@host/path: {}", scheme,
                    location));
        }
        uri.bucket = std::string(authority.substr(0, at));
        uri.account = std::string(azure_account_of_endpoint(authority.substr(at + 1)));
        uri.key = azure_key_of_path(path);
    } else if (scheme == "http" || scheme == "https") {
        // acct.blob.core.windows.net/container/path
        size_t container_end = path.find('/');
        uri.account = std::string(azure_account_of_endpoint(authority));
        uri.bucket = std::string(path.substr(0, container_end));
        uri.key = container_end == std::string_view::npos
                          ? ""
                          : azure_key_of_path(path.substr(container_end + 1));
    } else if (scheme == "s3") {
        uri.bucket = std::string(authority);
        uri.key = azure_key_of_path(path);
    } else {
        return ResultError(
                Status::InvalidArgument("Unsupported Azure location scheme: {}", location));
    }

    if (uri.bucket.empty()) {
        return ResultError(
                Status::InvalidArgument("Azure location names no container: {}", location));
    }
    return uri;
}

Result<ObjStorageUri> ObjStorageUri::parse_s3(const std::string& location) {
    if (location.empty()) {
        return ResultError(Status::InvalidArgument("location is empty"));
    }
    ObjStorageUri uri {.original = location, .account {}, .bucket {}, .key {}};
    std::vector<std::string> scheme_split = absl::StrSplit(location, "://");
    if (scheme_split.size() == 2) {
        const std::string& scheme = scheme_split[0];
        const std::string& rest = scheme_split[1];
        if (scheme == "s3") {
            // s3://bucket1/path/to/file.txt; s3://bucket1 keeps the historical key "/".
            std::vector<std::string> authority_split =
                    absl::StrSplit(rest, absl::MaxSplits('/', 1));
            uri.bucket = authority_split[0];
            uri.key = authority_split.size() == 1 ? "/" : authority_split[1];
        } else if (scheme == "http" || scheme == "https") {
            // http(s)://host/bucket1/path/to/file.txt
            std::vector<std::string> authority_split =
                    absl::StrSplit(rest, absl::MaxSplits('/', 2));
            if (authority_split.size() != 3) {
                return ResultError(Status::InvalidArgument("Invalid S3 HTTP URI: {}", location));
            }
            uri.bucket = authority_split[1];
            uri.key = authority_split[2];
        } else if (scheme == "abfs" || scheme == "abfss" || scheme == "wasb" || scheme == "wasbs") {
            // A native Azure location reaching an entry without typed Azure access, e.g.
            // abfss://c1@acct.dfs.core.windows.net/db/f. FE rewrites these to s3://c1/db/f for
            // such entries today; accepting the native form as well keeps them working if that
            // rewrite is ever dropped. Only container and key are used, exactly as for the
            // rewritten form, whose account FE has already discarded.
            auto azure = parse_azure(location);
            if (!azure.has_value()) {
                return ResultError(std::move(azure).error());
            }
            uri.bucket = std::move(azure->bucket);
            uri.key = std::move(azure->key);
        } else {
            return ResultError(Status::InvalidArgument("Invalid S3 URI: {}", location));
        }
    } else if (scheme_split.size() == 1) {
        // No scheme, e.g. path/to/file.txt: a key relative to the binding's bucket.
        uri.key = location;
    } else {
        return ResultError(Status::InvalidArgument("Invalid S3 URI: {}", location));
    }
    absl::StripAsciiWhitespace(&uri.key);
    if (uri.key.empty()) {
        return ResultError(Status::InvalidArgument("Invalid S3 key: {}", location));
    }
    uri.key = uri.key.substr(0, uri.key.find_first_of("?#"));
    return uri;
}

} // namespace doris::io
