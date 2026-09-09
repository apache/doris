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

#include "util/s3_uri.h"

#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_split.h>

#include <charconv>
#include <string_view>
#include <vector>

namespace doris {

const std::string S3URI::_SCHEME_S3 = "s3";
const std::string S3URI::_SCHEME_ABFS = "abfs";
const std::string S3URI::_SCHEME_ABFSS = "abfss";
const std::string S3URI::_SCHEME_WASB = "wasb";
const std::string S3URI::_SCHEME_WASBS = "wasbs";
const std::string S3URI::_SCHEME_HTTP = "http";
const std::string S3URI::_SCHEME_HTTPS = "https";
const std::string S3URI::_SCHEME_DELIM = "://";
const std::string S3URI::_PATH_DELIM = "/";
const std::string S3URI::_QUERY_DELIM = "?";
const std::string S3URI::_FRAGMENT_DELIM = "#";

/// eg:
///     s3://bucket1/path/to/file.txt
/// _schema: s3
/// _bucket: bucket1
/// _key:    path/to/file.txt
bool S3URI::is_azure_endpoint(std::string_view authority) {
    auto host = absl::AsciiStrToLower(authority.substr(0, authority.find(':')));
    for (const auto* suffix :
         {".blob.core.windows.net", ".dfs.core.windows.net", ".blob.core.chinacloudapi.cn",
          ".dfs.core.chinacloudapi.cn", ".blob.core.usgovcloudapi.net",
          ".dfs.core.usgovcloudapi.net", ".blob.core.cloudapi.de", ".dfs.core.cloudapi.de"}) {
        if (host.ends_with(suffix) && host.size() > std::char_traits<char>::length(suffix)) {
            return true;
        }
    }
    return false;
}

Status S3URI::parse(bool azure_provider) {
    if (_location.empty()) {
        return Status::InvalidArgument("location is empty");
    }
    // S3URI instances are occasionally reused by callers.  Do not retain a
    // previous parse's Azure authority when parsing a second location.
    _bucket.clear();
    _scheme.clear();
    _key.clear();
    _endpoint.clear();
    _account.clear();
    _is_azure = false;

    const auto parsing_error = [this, azure_provider](std::string_view message) {
        if (azure_provider || _is_azure) {
            return Status::InvalidArgument("{}", message);
        }
        // The first HTTP parse may precede provider selection. Never echo a
        // signed query, including for malformed or custom Azure endpoints.
        const auto safe_location =
                std::string_view(_location).substr(0, _location.find_first_of("?#"));
        return Status::InvalidArgument("{}: {}", message, safe_location);
    };

    std::vector<std::string> scheme_split =
            absl::StrSplit(_location, absl::MaxSplits(_SCHEME_DELIM, 1));
    std::string rest;
    if (scheme_split.size() == 2) {
        const std::string& scheme = scheme_split[0];
        _scheme = absl::AsciiStrToLower(scheme);
        if (scheme == _SCHEME_S3) {
            // has scheme, eg: s3://bucket1/path/to/file.txt
            rest = scheme_split[1];
            std::vector<std::string> authority_split =
                    absl::StrSplit(rest, absl::MaxSplits(_PATH_DELIM, 1));
            if (authority_split.empty() || authority_split[0].empty()) {
                return parsing_error("Invalid S3 URI");
            }
            _bucket = authority_split[0];
            // support s3://bucket1
            _key = authority_split.size() == 1 ? "/" : authority_split[1];
        } else if (absl::EqualsIgnoreCase(scheme, _SCHEME_ABFS) ||
                   absl::EqualsIgnoreCase(scheme, _SCHEME_ABFSS) ||
                   absl::EqualsIgnoreCase(scheme, _SCHEME_WASB) ||
                   absl::EqualsIgnoreCase(scheme, _SCHEME_WASBS)) {
            // Azure Data Lake paths use container@account-host as the
            // authority.  Keep the account host so the native Azure client
            // can derive its endpoint without consulting Hadoop settings.
            _is_azure = true;
            rest = scheme_split[1];
            std::vector<std::string> authority_split =
                    absl::StrSplit(rest, absl::MaxSplits(_PATH_DELIM, 1));
            if (authority_split.empty() || authority_split[0].empty()) {
                return parsing_error("Invalid Azure URI");
            }
            const auto at = authority_split[0].find('@');
            if (at == std::string::npos || at == 0 || at + 1 == authority_split[0].size() ||
                authority_split[0].find('@', at + 1) != std::string::npos) {
                return parsing_error("Invalid Azure URI authority");
            }
            _bucket = authority_split[0].substr(0, at);
            _endpoint = authority_split[0].substr(at + 1);
            if (_endpoint.empty()) {
                return parsing_error("Invalid Azure URI authority");
            }
            const auto dot = _endpoint.find('.');
            _account = dot == std::string::npos ? _endpoint : _endpoint.substr(0, dot);
            _key = authority_split.size() == 1 ? "/" : authority_split[1];
        } else if (absl::EqualsIgnoreCase(scheme, _SCHEME_HTTP) ||
                   absl::EqualsIgnoreCase(scheme, _SCHEME_HTTPS)) {
            // has scheme, eg: http(s)://host/bucket1/path/to/file.txt
            rest = scheme_split[1];
            std::vector<std::string> authority_split =
                    absl::StrSplit(rest, absl::MaxSplits(_PATH_DELIM, 2));
            _is_azure = is_azure_endpoint(authority_split[0]);
            if (authority_split.size() != 3 || authority_split[0].empty() ||
                authority_split[1].empty()) {
                return parsing_error("Invalid S3 HTTP URI");
            }
            // authority_split[0] is host, authority_split[1] is bucket.
            _endpoint = authority_split[0];
            const auto dot = _endpoint.find('.');
            _account = dot == std::string::npos ? _endpoint : _endpoint.substr(0, dot);
            _bucket = authority_split[1];
            _key = authority_split[2];
        } else {
            return parsing_error("Invalid S3 URI");
        }
    } else if (scheme_split.size() == 1) {
        // no scheme, eg: path/to/file.txt
        _bucket = ""; // unknown
        _key = _location;
    } else {
        return parsing_error("Invalid S3 URI");
    }
    const bool azure_uri =
            (_is_azure || azure_provider) && !_scheme.empty() && _scheme != _SCHEME_S3;
    if (!azure_uri) {
        absl::StripAsciiWhitespace(&_key);
    }
    if (_key.empty()) {
        return parsing_error("Invalid S3 key");
    }
    // Strip query and fragment if they exist
    std::vector<std::string> _query_split = absl::StrSplit(_key, _QUERY_DELIM);
    std::vector<std::string> _fragment_split = absl::StrSplit(_query_split[0], _FRAGMENT_DELIM);
    _key = _fragment_split[0];
    // ADLSFileIO's ADLSLocation passes ABFS/WASB names literally to the SDK:
    // an object called p=a%2Fb must not become p=a/b. Only HTTP(S) locations
    // represent URL-encoded names. The SDK encodes the resulting object name.
    if (azure_uri && (_scheme == _SCHEME_HTTP || _scheme == _SCHEME_HTTPS)) {
        std::string decoded;
        decoded.reserve(_key.size());
        for (size_t i = 0; i < _key.size(); ++i) {
            if (_key[i] != '%') {
                // '+' is a literal object-name character, not form encoding.
                decoded.push_back(_key[i]);
                continue;
            }
            if (i + 2 >= _key.size()) {
                return Status::InvalidArgument("Invalid percent encoding in Azure object path");
            }
            unsigned int value = 0;
            const auto* begin = _key.data() + i + 1;
            const auto [end, error] = std::from_chars(begin, begin + 2, value, 16);
            if (error != std::errc {} || end != begin + 2) {
                return Status::InvalidArgument("Invalid percent encoding in Azure object path");
            }
            decoded.push_back(static_cast<char>(value));
            i += 2;
        }
        _key = std::move(decoded);
    }
    return Status::OK();
}

std::string S3URI::to_string() const {
    return _location;
}

} // end namespace doris
