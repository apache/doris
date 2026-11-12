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

#include <cstdint>
#include <functional>
#include <optional>
#include <string>

namespace doris {

struct ObjStoragePath;
struct GcpCredentialConfig;

enum class EmptyCredentialsBehavior {
    ANONYMOUS,
    DEFAULT_CHAIN,
};

using ObjStorageSignedUrlGenerator = std::function<std::string(const ObjStoragePath&, int64_t)>;

// Process settings used to construct clients, independent of the storage resource.
struct ObjStorageClientRuntime {
    std::string credential_provider_version {};
    EmptyCredentialsBehavior empty_credentials = EmptyCredentialsBehavior::DEFAULT_CHAIN;
    std::optional<long> sts_request_timeout_ms {};
    std::string ca_cert_path {};
    std::string http_scheme {};
    int max_retries = 0;
    bool retry_slow_down = false;
    unsigned int default_max_connections = 102400;
    // The Recycler sizes its connection pool to at least the SDK default and this worker count.
    std::optional<int64_t> min_connections {};
    ObjStorageSignedUrlGenerator (*gcs_signer)(const GcpCredentialConfig&, std::string, int,
                                               std::string) = nullptr;
    int azure_log_level = 4;
    bool retry_too_many_requests = false;
    std::optional<std::string> ca_cert_file_paths {};
};

} // namespace doris
