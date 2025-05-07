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
    std::vector<std::string> scheme_split = absl::StrSplit(_location, _SCHEME_DELIM);
    std::string rest;
    if (scheme_split.size() == 2) {
        if (scheme_split[0] == _SCHEME_S3) {
            // has scheme, eg: s3://bucket1/path/to/file.txt
            rest = scheme_split[1];
            std::vector<std::string> authority_split =
                    absl::StrSplit(rest, absl::MaxSplits(_PATH_DELIM, 1));
            if (authority_split.size() < 1) {
                return Status::InvalidArgument("Invalid S3 URI: {}", _location);
            }
            _bucket = authority_split[0];
            // support s3://bucket1
            _key = authority_split.size() == 1 ? "/" : authority_split[1];
        } else if (scheme_split[0] == _SCHEME_HTTP || scheme_split[0] == _SCHEME_HTTPS) {
            // has scheme, eg: http(s)://host/bucket1/path/to/file.txt
            rest = scheme_split[1];
            std::vector<std::string> authority_split =
                    absl::StrSplit(rest, absl::MaxSplits(_PATH_DELIM, 2));
            if (authority_split.size() != 3) {
                return Status::InvalidArgument("Invalid S3 HTTP URI: {}", _location);
            }
            // authority_split[1] is host
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
