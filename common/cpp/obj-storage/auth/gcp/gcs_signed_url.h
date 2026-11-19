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

#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

namespace doris {

struct GcsV4SignedUrlOptions {
    std::string endpoint;
    std::string bucket;
    std::string key;
    // The transport layer resolves this identity before invoking the builder.
    std::string signer_email;
    int64_t expiration_secs = 0;
};

struct GcsSignBlobResult {
    std::string signature {};
    std::string error {};

    bool ok() const noexcept { return error.empty(); }
};

// Returns the raw signature bytes for the supplied GCS V4 string-to-sign. The
// callback intentionally contains no transport-specific type so this signing
// algorithm can be shared by BE and cloud targets.
using GcsSignBlobFunction = std::function<GcsSignBlobResult(std::string_view string_to_sign)>;

struct GcsV4SignedUrlResult {
    std::string signed_url {};
    std::string error {};

    bool ok() const noexcept { return error.empty(); }
};

// Builds a GCS V4 signed URL from a caller-supplied signing function. Resolving
// credentials and invoking IAM signBlob belong to the caller's transport layer.
GcsV4SignedUrlResult build_gcs_v4_signed_url(const GcsV4SignedUrlOptions& options,
                                             std::chrono::system_clock::time_point now,
                                             const GcsSignBlobFunction& sign_blob);

} // namespace doris
