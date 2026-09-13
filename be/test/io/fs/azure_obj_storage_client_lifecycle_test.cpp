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

#include <gtest/gtest.h>

#ifdef USE_AZURE
#include <azure/core/credentials/credentials.hpp>
#include <azure/core/datetime.hpp>
#include <azure/core/http/http.hpp>
#include <azure/core/http/raw_response.hpp>
#include <azure/core/http/transport.hpp>
#include <azure/core/url.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>
#include <azure/storage/blobs/blob_options.hpp>
#include <azure/storage/blobs/blob_sas_builder.hpp>
#include <azure/storage/common/storage_credential.hpp>
#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "cpp/obj-client/azure_obj_storage_client.h"

namespace doris {
namespace {

constexpr char AZURE_ENDPOINT[] = "https://account.blob.core.windows.net";
constexpr char AZURE_CONTAINER_URL[] = "https://account.blob.core.windows.net/container";

class AzureLifecycleTransport final : public Azure::Core::Http::HttpTransport {
public:
    std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request& request,
                                                         const Azure::Core::Context&) override {
        methods.push_back(request.GetMethod().ToString());
        // The pre-fix abort issues DELETE. Answer it locally so the test records the mutation
        // without relying on a real Azure account, its permissions, or its network availability.
        return std::make_unique<Azure::Core::Http::RawResponse>(
                1, 1, Azure::Core::Http::HttpStatusCode::Accepted, "Accepted");
    }

    std::vector<std::string> methods;
};

class AzureLifecycleTokenCredential final : public Azure::Core::Credentials::TokenCredential {
public:
    AzureLifecycleTokenCredential() : TokenCredential("AzureLifecycleTest") {}

    Azure::Core::Credentials::AccessToken GetToken(
            const Azure::Core::Credentials::TokenRequestContext&,
            const Azure::Core::Context&) const override {
        ++token_requests;
        return {.Token = "test-access-token",
                .ExpiresOn =
                        Azure::DateTime(std::chrono::system_clock::now() + std::chrono::hours(1))};
    }

    mutable size_t token_requests = 0;
};

class AzureObjStorageClientLifecycleTest : public testing::Test {
protected:
    void SetUp() override {
        transport = std::make_shared<AzureLifecycleTransport>();
        options.Transport.Transport = transport;
        options.Retry.MaxRetries = 0;
    }

    std::shared_ptr<AzureLifecycleTransport> transport;
    Azure::Storage::Blobs::BlobClientOptions options;
};

TEST_F(AzureObjStorageClientLifecycleTest, SasCannotPublishTheContainerCredential) {
    auto container = std::make_shared<Azure::Storage::Blobs::BlobContainerClient>(
            std::string(AZURE_CONTAINER_URL) + "?sv=2024-01-01&sr=c&sp=rwdl&sig=private-sas",
            options);
    AzureObjStorageClient client(container, {.endpoint = AZURE_ENDPOINT});

    EXPECT_TRUE(client.generate_presigned_url({.bucket = "container", .key = "directory/file"}, 60)
                        .empty());
    EXPECT_TRUE(transport->methods.empty());
}

TEST_F(AzureObjStorageClientLifecycleTest, OAuth2CannotPublishAnUnauthenticatedUrl) {
    auto credential = std::make_shared<AzureLifecycleTokenCredential>();
    auto container = std::make_shared<Azure::Storage::Blobs::BlobContainerClient>(
            AZURE_CONTAINER_URL, credential, options);
    AzureObjStorageClient client(container, {.endpoint = AZURE_ENDPOINT});

    EXPECT_TRUE(client.generate_presigned_url({.bucket = "container", .key = "directory/file"}, 60)
                        .empty());
    EXPECT_EQ(credential->token_requests, 0);
    EXPECT_TRUE(transport->methods.empty());
}

TEST_F(AzureObjStorageClientLifecycleTest, SharedKeyStillSignsOneReadOnlyBlobWithRequestedExpiry) {
    auto credential = std::make_shared<Azure::Storage::StorageSharedKeyCredential>(
            "account", "MDEyMzQ1Njc4OWFiY2RlZg==");
    auto container = std::make_shared<Azure::Storage::Blobs::BlobContainerClient>(
            AZURE_CONTAINER_URL, credential, options);
    AzureObjStorageClient client(container, {.endpoint = AZURE_ENDPOINT}, credential);
    constexpr int64_t expiry_seconds = 600;
    const auto before = std::chrono::system_clock::now();
    const auto url = client.generate_presigned_url({.bucket = "container", .key = "directory/file"},
                                                   expiry_seconds);
    const auto after = std::chrono::system_clock::now();

    ASSERT_TRUE(url.starts_with("https://account.blob.core.windows.net/container/directory/file?"));
    const auto query = Azure::Core::Url(url).GetQueryParameters();
    EXPECT_EQ(query.at("sr"), "b");
    EXPECT_EQ(query.at("sp"), "r");
    EXPECT_EQ(query.at("spr"), "https");
    EXPECT_FALSE(query.at("sig").empty());
    const auto expiry = Azure::DateTime::Parse(Azure::Core::Url::Decode(query.at("se")),
                                               Azure::DateTime::DateFormat::Rfc3339);
    EXPECT_GE(expiry, Azure::DateTime(before + std::chrono::seconds(expiry_seconds - 1)));
    EXPECT_LE(expiry, Azure::DateTime(after + std::chrono::seconds(expiry_seconds)));
    EXPECT_TRUE(transport->methods.empty());
}

TEST_F(AzureObjStorageClientLifecycleTest, SharedKeyPresignEncodesRawObjectNamesExactlyOnce) {
    auto credential = std::make_shared<Azure::Storage::StorageSharedKeyCredential>(
            "account", "MDEyMzQ1Njc4OWFiY2RlZg==");
    auto container = std::make_shared<Azure::Storage::Blobs::BlobContainerClient>(
            AZURE_CONTAINER_URL, credential, options);
    AzureObjStorageClient client(container, {.endpoint = AZURE_ENDPOINT}, credential);
    for (const auto* key :
         {"directory/100%.parquet", "directory/audit%2Fhistory", "directory/a b+c",
          "directory/中文.parquet", "directory/a%252Fb", "directory/http://example/file"}) {
        SCOPED_TRACE(key);
        const auto signed_url =
                client.generate_presigned_url({.bucket = "container", .key = key}, 600);
        const Azure::Core::Url url(signed_url);
        EXPECT_EQ(Azure::Core::Url::Decode(url.GetPath()), std::string("container/") + key);
        const auto query = url.GetQueryParameters();
        Azure::Storage::Sas::BlobSasBuilder expected;
        expected.BlobContainerName = "container";
        expected.BlobName = key;
        expected.Resource = Azure::Storage::Sas::BlobSasResource::Blob;
        expected.Protocol = Azure::Storage::Sas::SasProtocol::HttpsOnly;
        expected.SetPermissions(Azure::Storage::Sas::BlobSasPermissions::Read);
        expected.ExpiresOn = Azure::DateTime::Parse(Azure::Core::Url::Decode(query.at("se")),
                                                    Azure::DateTime::DateFormat::Rfc3339);
        // Verify that the signature still names the raw blob, not its URL-encoded spelling.
        EXPECT_EQ(signed_url.substr(signed_url.find('?')), expected.GenerateSasToken(*credential));
    }
    EXPECT_TRUE(transport->methods.empty());
}

TEST_F(AzureObjStorageClientLifecycleTest, AbortDoesNotDeletePublishedBlobOrOtherWritersBlocks) {
    auto container = std::make_shared<Azure::Storage::Blobs::BlobContainerClient>(
            AZURE_CONTAINER_URL, options);
    AzureObjStorageClient client(container, {.endpoint = AZURE_ENDPOINT});
    const ObjStoragePath path {.bucket = "container", .key = "existing-blob"};

    EXPECT_TRUE(client.abort_multipart_upload(path, "first-writer").ok());
    EXPECT_TRUE(client.abort_multipart_upload(path, "first-writer").ok());
    EXPECT_TRUE(client.abort_multipart_upload(path, "second-writer").ok());
    EXPECT_TRUE(transport->methods.empty());
}

TEST(AzureObjStorageClientLifecycleHelperTest, AbortDoesNotRequireAnSdkClient) {
    AzureObjStorageClient client(std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> {},
                                 {});

    EXPECT_TRUE(client.abort_multipart_upload({.key = "uncommitted-blob"}, "upload-id").ok());
}

} // namespace
} // namespace doris

#endif // USE_AZURE
