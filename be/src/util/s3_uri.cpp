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
#include <absl/strings/str_split.h>

#include <vector>

namespace doris {

const std::string S3URI::_SCHEME_S3 = "s3";
const std::string S3URI::_SCHEME_S3A = "s3a";
const std::string S3URI::_SCHEME_S3N = "s3n";
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
Status S3URI::parse() {
    if (_location.empty()) {
        return Status::InvalidArgument("location is empty");
    }
    // S3URI instances are occasionally reused by callers.  Do not retain a
    // previous parse's Azure authority when parsing a second location.
    _bucket.clear();
    _key.clear();
    _endpoint.clear();
    _account.clear();
    _is_azure = false;

    std::vector<std::string> scheme_split = absl::StrSplit(_location, _SCHEME_DELIM);
    std::string rest;
    if (scheme_split.size() == 2) {
        std::string scheme = scheme_split[0];
        absl::AsciiStrToLower(&scheme);
        if (scheme == _SCHEME_S3 || scheme == _SCHEME_S3A || scheme == _SCHEME_S3N) {
            // has scheme, eg: s3://bucket1/path/to/file.txt
            rest = scheme_split[1];
            std::vector<std::string> authority_split =
                    absl::StrSplit(rest, absl::MaxSplits(_PATH_DELIM, 1));
            if (authority_split.empty() || authority_split[0].empty()) {
                return Status::InvalidArgument("Invalid S3 URI: {}", _location);
            }
            _bucket = authority_split[0];
            // support s3://bucket1
            _key = authority_split.size() == 1 ? "/" : authority_split[1];
        } else if (scheme == _SCHEME_ABFS || scheme == _SCHEME_ABFSS || scheme == _SCHEME_WASB ||
                   scheme == _SCHEME_WASBS) {
            // Azure Data Lake paths use container@account-host as the
            // authority.  Keep the account host so the native Azure client
            // can derive its endpoint without consulting Hadoop settings.
            rest = scheme_split[1];
            std::vector<std::string> authority_split =
                    absl::StrSplit(rest, absl::MaxSplits(_PATH_DELIM, 1));
            if (authority_split.empty() || authority_split[0].empty()) {
                return Status::InvalidArgument("Invalid Azure URI: {}", _location);
            }
            const auto at = authority_split[0].find('@');
            if (at == std::string::npos || at == 0 || at + 1 == authority_split[0].size() ||
                authority_split[0].find('@', at + 1) != std::string::npos) {
                return Status::InvalidArgument("Invalid Azure URI authority: {}", _location);
            }
            _bucket = authority_split[0].substr(0, at);
            _endpoint = authority_split[0].substr(at + 1);
            const auto dot = _endpoint.find('.');
            _account = dot == std::string::npos ? _endpoint : _endpoint.substr(0, dot);
            _is_azure = true;
            _key = authority_split.size() == 1 ? "/" : authority_split[1];
        } else if (scheme == _SCHEME_HTTP || scheme == _SCHEME_HTTPS) {
            // has scheme, eg: http(s)://host/bucket1/path/to/file.txt
            rest = scheme_split[1];
            std::vector<std::string> authority_split =
                    absl::StrSplit(rest, absl::MaxSplits(_PATH_DELIM, 2));
            if (authority_split.size() != 3 || authority_split[0].empty() ||
                authority_split[1].empty()) {
                return Status::InvalidArgument("Invalid S3 HTTP URI: {}", _location);
            }
            // authority_split[0] is host, authority_split[1] is bucket.
            _endpoint = authority_split[0];
            const auto dot = _endpoint.find('.');
            _account = dot == std::string::npos ? _endpoint : _endpoint.substr(0, dot);
            const auto lower_endpoint = absl::AsciiStrToLower(_endpoint);
            _is_azure = lower_endpoint.find(".blob.") != std::string::npos ||
                        lower_endpoint.find(".dfs.") != std::string::npos;
            _bucket = authority_split[1];
            _key = authority_split[2];
        } else {
            return Status::InvalidArgument("Invalid S3 URI: {}", _location);
        }
    } else if (scheme_split.size() == 1) {
        // no scheme, eg: path/to/file.txt
        _bucket = ""; // unknown
        _key = _location;
    } else {
        return Status::InvalidArgument("Invalid S3 URI: {}", _location);
    }
    absl::StripAsciiWhitespace(&_key);
    if (_key.empty()) {
        return Status::InvalidArgument("Invalid S3 key: {}", _location);
    }
    // Strip query and fragment if they exist
    std::vector<std::string> _query_split = absl::StrSplit(_key, _QUERY_DELIM);
    std::vector<std::string> _fragment_split = absl::StrSplit(_query_split[0], _FRAGMENT_DELIM);
    _key = _fragment_split[0];
    return Status::OK();
}

std::string S3URI::to_string() const {
    return _location;
}

} // end namespace doris
