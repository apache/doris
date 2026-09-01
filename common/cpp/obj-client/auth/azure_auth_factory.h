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

namespace Azure::Storage {
class StorageSharedKeyCredential;
}

namespace doris {

enum class AzureCredentialType {
    SHARED_KEY,
};

struct AzureCredentialOptions {
    AzureCredentialType type = AzureCredentialType::SHARED_KEY;
    std::string account_name;
    std::string account_key;
};

struct AzureClientBuildResult {
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> container_client {};
    std::shared_ptr<Azure::Storage::StorageSharedKeyCredential> shared_key_credential {};
    std::string error {};

    explicit operator bool() const { return container_client != nullptr; }
};

class AzureAuthFactory {
public:
    static AzureClientBuildResult create(std::string_view container_url,
                                         const AzureCredentialOptions& credential,
                                         Azure::Storage::Blobs::BlobClientOptions client_options);
};

} // namespace doris
