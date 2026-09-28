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
#include <memory>
#include <string>

#include "common/status.h"
#include "cpp/obj-client/auth/gcp/gcp_auth.h"

namespace doris {
class GcpTokenProvider;
}

namespace doris::io {

struct GcsV4SignedUrlProviderOptions {
    std::string endpoint;
    std::string bucket;
    std::string key;
    int64_t expiration_secs = 0;
    int64_t request_timeout_ms = 10000;
};

// Resolves the signing service account, obtains an OAuth token, and invokes
// IAM Credentials signBlob before delegating URL construction to common auth.
Status generate_gcs_v4_signed_url(const GcsV4SignedUrlProviderOptions& options,
                                  const GcpCredentialConfig& credential,
                                  const std::shared_ptr<GcpTokenProvider>& token_provider,
                                  std::string* signed_url);

} // namespace doris::io
