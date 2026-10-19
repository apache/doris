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
#include <string_view>

#include "common/status.h"

namespace doris::io {

// A file location on an object store, parsed by the rules of its storage type. The location
// itself is kept untouched in `original`: it is the address the table format records (an
// Iceberg data file path, a Hive partition location), so it must never be replaced by the
// bucket/key form that is only used to address the request.
struct ObjStorageUri {
    std::string original;
    // Azure only: the storage account named by the host, e.g. "acct" for
    // acct.blob.core.windows.net. Empty when the location does not name one (s3-style or
    // key-only locations, or a host that is not an Azure storage endpoint).
    std::string account;
    // S3 bucket or Azure container; empty for a key-only location.
    std::string bucket;
    std::string key;

    // Accepts every Azure location form FE can send:
    //   abfs[s]://container@acct.dfs.core.windows.net/path/file
    //   wasb[s]://container@acct.blob.core.windows.net/path/file
    //   http[s]://acct.blob.core.windows.net/container/path/file
    //   s3://container/path/file (the s3-style form older FEs normalize Azure locations to)
    //   path/file (key only; the container comes from the client config)
    // Query and fragment are dropped from the key, as S3URI does.
    static Result<ObjStorageUri> parse_azure(const std::string& location);

    // The storage account named by a standard Azure endpoint or host, e.g. "acct" for
    // https://acct.blob.core.windows.net or acct.dfs.core.chinacloudapi.cn:443. Empty for a
    // custom endpoint (Azurite at http://127.0.0.1:10000, a private DNS name), which names no
    // account to check against.
    static std::string_view azure_account_of_endpoint(std::string_view endpoint);
};

} // namespace doris::io
