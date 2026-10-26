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

namespace doris::io {

// A file location on an object store (formerly S3URI). The location itself is kept untouched
// in `original`: it is the address the table format records (an Iceberg data file path, a
// Hive partition location), so it must never be replaced by the bucket/key form that is only
// used to address the request.
struct ObjStorageUri {
    std::string original;
    // Empty for a key-only location.
    std::string bucket;
    std::string key;

    // The rules of the former S3URI:
    //   s3://bucket/path/file -> bucket "bucket", key "path/file"; s3://bucket -> key "/"
    //   http[s]://host/bucket/path/file -> bucket "bucket", key "path/file"
    //   path/file (no scheme) -> no bucket, key "path/file"
    // The key must not be empty after trimming whitespace; query and fragment are dropped.
    static Result<ObjStorageUri> parse_s3(const std::string& location);
};

} // namespace doris::io
