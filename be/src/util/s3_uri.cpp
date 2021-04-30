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

#include <vector>

#include "gutil/strings/split.h"
#include "gutil/strings/strip.h"
#include "util/logging.h"

namespace doris {

const std::string S3URI::_SCHEME_DELIM = "://";
const std::string S3URI::_PATH_DELIM = "/";
const std::string S3URI::_QUERY_DELIM = "?";
const std::string S3URI::_FRAGMENT_DELIM = "#";
const StringCaseSet S3URI::_VALID_SCHEMES = {"http", "https", "s3", "s3a", "s3n", "bos"};
bool S3URI::parse() {
    if (_location.empty()) {
        return false;
    }
    std::vector<std::string> scheme_split = strings::Split(_location, _SCHEME_DELIM);
    if (scheme_split.size() != 2) {
        LOG(WARNING) << "Invalid S3 URI: " << _location;
        return false;
    }
    _scheme = scheme_split[0];
    if (_VALID_SCHEMES.find(_scheme) == _VALID_SCHEMES.end()) {
        LOG(WARNING) << "Invalid scheme: " << _scheme;
        return false;
    }
    std::vector<std::string> authority_split =
            strings::Split(scheme_split[1], strings::delimiter::Limit(_PATH_DELIM, 1));
    if (authority_split.size() != 2) {
        LOG(WARNING) << "Invalid S3 URI: " << _location;
        return false;
    }
    _key = authority_split[1];
    StripWhiteSpace(&_key);
    if (_key.empty()) {
        LOG(WARNING) << "Invalid S3 key: " << _location;
        return false;  
    }
    _bucket = authority_split[0];
    // Strip query and fragment if they exist
    std::vector<std::string> _query_split = strings::Split(_key, _QUERY_DELIM);
    std::vector<std::string> _fragment_split = strings::Split(_query_split[0], _FRAGMENT_DELIM);
    _key = _fragment_split[0];
    return true;
}

} // end namespace doris
