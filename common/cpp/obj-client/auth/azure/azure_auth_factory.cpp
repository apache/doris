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

#include <azure/identity/client_secret_credential.hpp>
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

AzureClientBuildResult build(std::string_view container_url, std::string_view /*account*/,
                             const AzureSasConfig& sas,
                             Azure::Storage::Blobs::BlobClientOptions client_options) {
    // The SDK sends the URL query with every request, so the SAS rides on the container URL
    // itself, e.g. https://acct.blob.core.windows.net/c?sv=...&se=...&sig=...
    // container_url never carries a query of its own.
    std::string signed_url = std::string(container_url) + "?" + sas.token;
    auto client = std::make_shared<Azure::Storage::Blobs::BlobContainerClient>(
            signed_url, std::move(client_options));
    return {.container_client = std::move(client)};
}

AzureClientBuildResult build(std::string_view container_url, std::string_view /*account*/,
                             const AzureClientSecretConfig& client_secret,
                             Azure::Storage::Blobs::BlobClientOptions client_options) {
    Azure::Identity::ClientSecretCredentialOptions identity_options;
    if (!client_secret.authority_host.empty()) {
        identity_options.AuthorityHost = client_secret.authority_host;
    }
    // Token requests use the same transport (and so the same CA bundle) as blob requests, but
    // not the blob retry policies, which are tuned for storage responses.
    identity_options.Transport = client_options.Transport;
    auto identity = std::make_shared<Azure::Identity::ClientSecretCredential>(
            client_secret.tenant_id, client_secret.client_id, client_secret.client_secret,
            identity_options);
    auto client = std::make_shared<Azure::Storage::Blobs::BlobContainerClient>(
            std::string(container_url), std::move(identity), std::move(client_options));
    return {.container_client = std::move(client)};
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
