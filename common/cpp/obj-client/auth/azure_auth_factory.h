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

#include <azure/storage/blobs/blob_container_client.hpp>
#include <memory>
#include <string>
#include <string_view>

#include "azure_credential_options.h"

namespace Azure::Storage {
class StorageSharedKeyCredential;
}

namespace doris {

struct AzureClientBuildResult {
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> container_client {};
    std::shared_ptr<Azure::Storage::StorageSharedKeyCredential> shared_key_credential {};
    std::string error {};

    explicit operator bool() const { return container_client != nullptr; }
};

class AzureAuthFactory {
public:
    // Called before cache lookup as well as SDK construction. Does not access
    // the network or refresh a SAS credential.
    static std::string validate(const AzureCredentialOptions& credential);

    // Returns the earliest known SAS expiry (zero when unknown) without changing
    // the token or inventing a lifetime. The caller supplies its access timestamp
    // so cache lookup and SDK construction use the same expiry rule.
    static std::string validate(const AzureCredentialOptions& credential, int64_t now_ms,
                                int64_t* effective_expiry_ms);

    static AzureClientBuildResult create(std::string_view container_url,
                                         const AzureCredentialOptions& credential,
                                         Azure::Storage::Blobs::BlobClientOptions client_options);
};

} // namespace doris
