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

#include "cpp/obj-client/auth/azure/azure_auth_factory.h"

#include <azure/storage/common/storage_credential.hpp>
#include <utility>
#include <variant>

namespace doris {
namespace {

AzureClientBuildResult build(std::string_view container_url, std::string_view account,
                             const AzureSharedKeyConfig& shared_key,
                             Azure::Storage::Blobs::BlobClientOptions client_options) {
    auto credential = std::make_shared<Azure::Storage::StorageSharedKeyCredential>(
            std::string(account), shared_key.account_key);
    auto client = std::make_shared<Azure::Storage::Blobs::BlobContainerClient>(
            std::string(container_url), credential, std::move(client_options));
    return {
            .container_client = std::move(client),
            .shared_key_credential = std::move(credential),
    };
}

} // namespace

AzureClientBuildResult AzureAuthFactory::create(
        std::string_view container_url, std::string_view account, const AzureAuthConfig& auth,
        Azure::Storage::Blobs::BlobClientOptions client_options) {
    return std::visit(
            [&](const auto& method) {
                return build(container_url, account, method, std::move(client_options));
            },
            auth);
}

} // namespace doris
