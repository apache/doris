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

#pragma once

#include <string>

#include "common/status.h"
#include "util/string_util.h"

namespace doris {

// S3URI can handle following input:
// 1. s3://bucket_name/path/to/file.txt
//      bucket: bucket_num
//      key: path/to/file.txt
// 2. path/to/file.txt
//      bucket: ""
//      key: path/to/file.txt
class S3URI {
public:
    S3URI(const std::string& location) : _location(location) {}
    Status parse();
    const std::string& get_bucket() const { return _bucket; }
    const std::string& get_key() const { return _key; }
    const std::string& get_location() const { return _location; }
    std::string to_string() const;

private:
    static const std::string _SCHEME_S3;
    static const std::string _SCHEME_HTTP;
    static const std::string _SCHEME_HTTPS;
    static const std::string _SCHEME_DELIM;
    static const std::string _PATH_DELIM;
    static const std::string _QUERY_DELIM;
    static const std::string _FRAGMENT_DELIM;
    static const StringCaseSet _VALID_SCHEMES;

    std::string _location;
    std::string _bucket;
    std::string _key;
};
} // end namespace doris
