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
#include <absl/strings/str_split.h>

#include <vector>

namespace doris::io {

Result<ObjStorageUri> ObjStorageUri::parse_s3(const std::string& location) {
    if (location.empty()) {
        return ResultError(Status::InvalidArgument("location is empty"));
    }
    ObjStorageUri uri {.original = location, .bucket {}, .key {}};
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
