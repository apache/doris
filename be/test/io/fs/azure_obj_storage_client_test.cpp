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

#ifdef USE_AZURE
#include "cpp/obj-storage/client/azure_obj_storage_client.h"

#include "cpp/obj-storage/obj_storage_config.h"
#endif

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <vector>

#include "cpp/obj-storage/client/obj_storage_client.h"
#include "io/fs/file_system.h"
#include "io/object_storage/obj_storage_client_factory.h"

#ifdef USE_AZURE
#include <azure/core/base64.hpp>
#include <azure/core/http/transport.hpp>
#include <azure/storage/blobs.hpp>
#include <azure/storage/blobs/blob_client.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>
#include <azure/storage/common/storage_credential.hpp>
#endif

namespace doris {

#ifdef USE_AZURE

TEST(AzureObjStorageClientMultipartHelperTest, full_upload_uuid_isolates_writer_blocks) {
    constexpr std::string_view first_upload = "09492e3d-e231-4ed9-bf84-b6fc772cda54";
    constexpr std::string_view second_upload = "06996d15-1c2e-4ddd-8853-43816ea84a07";
    auto first_block = io::azure_multipart_block_id(first_upload, 1);
    auto second_block = io::azure_multipart_block_id(second_upload, 1);

    EXPECT_NE(first_block, second_block);
    EXPECT_EQ(first_block.size(), io::azure_multipart_block_id(first_upload, 999).size());
    auto decoded = Azure::Core::Convert::Base64Decode(first_block);
    ASSERT_EQ(first_upload.size() + sizeof(uint32_t), decoded.size());
    EXPECT_EQ(first_upload,
              std::string_view(reinterpret_cast<const char*>(decoded.data()), first_upload.size()));
    EXPECT_EQ(1, decoded[first_upload.size()]);
    EXPECT_EQ(0, decoded[first_upload.size() + 1]);
    EXPECT_EQ(0, decoded[first_upload.size() + 2]);
    EXPECT_EQ(0, decoded[first_upload.size() + 3]);
}

TEST(AzureObjStorageClientMultipartHelperTest, create_upload_is_provider_free) {
    io::AzureObjStorageClient client(
            Azure::Storage::Blobs::BlobServiceClient("https://account.blob.core.windows.net"),
            nullptr, {});

    auto first = client.create_multipart_upload({});
    auto second = client.create_multipart_upload({});

    ASSERT_EQ(ErrorCode::OK, first.resp.status.code);
    ASSERT_EQ(ErrorCode::OK, second.resp.status.code);
    ASSERT_TRUE(first.upload_id.has_value());
    ASSERT_TRUE(second.upload_id.has_value());
    EXPECT_EQ(36, first.upload_id->size());
    EXPECT_EQ(36, second.upload_id->size());
    EXPECT_NE(first.upload_id, second.upload_id);
}

using namespace Azure::Storage::Blobs;

TEST(AzureObjStorageClientOfflineTest, SignsSameKeyForDifferentContainers) {
    AzureObjStorageConfig config;
    config.connection.endpoint = "https://account.blob.core.windows.net";
    config.auth = {.account_name = "account", .account_key = "dGVzdA=="};
    auto built = AzureObjStorageClient::create(config);
    ASSERT_TRUE(built.client);
    for (std::string container : {"first", "second"}) {
        ObjStoragePath target {.bucket = container, .key = "a b"};
        auto signed_url = built.client->generate_presigned_url(target, 60);
        EXPECT_TRUE(
                signed_url.starts_with(config.connection.endpoint + "/" + container + "/a%20b?"));
    }
}

namespace {
class MultipartRecordingTransport final : public Azure::Core::Http::HttpTransport {
public:
    std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request& request,
                                                         const Azure::Core::Context&) override {
        const auto& url = request.GetUrl();
        {
            std::lock_guard lock(_mutex);
            requests.emplace_back(url.GetPath(), url.GetQueryParameters().at("comp"));
        }
        auto response = std::make_unique<Azure::Core::Http::RawResponse>(
                1, 1, Azure::Core::Http::HttpStatusCode::Created, "Created");
        response->SetHeader("ETag", "\"etag\"");
        response->SetHeader("Last-Modified", "Thu, 12 Nov 2026 00:00:00 GMT");
        response->SetHeader("x-ms-request-server-encrypted", "true");
        return response;
    }

    std::vector<std::pair<std::string, std::string>> requests;

private:
    std::mutex _mutex;
};
} // namespace

TEST(AzureObjStorageClientOfflineTest, MultipartRequestsUseTheirOwnContainer) {
    auto transport = std::make_shared<MultipartRecordingTransport>();
    BlobClientOptions options;
    options.Transport.Transport = transport;
    AzureObjStorageClient client(
            BlobServiceClient("https://account.blob.core.windows.net", options), nullptr, {});
    auto upload = [&](const std::string& container) {
        ObjStoragePath path {.bucket = container, .key = "shared"};
        auto create = client.create_multipart_upload(path);
        ASSERT_TRUE(create.upload_id.has_value());
        auto part = client.upload_part(path, *create.upload_id, "data", 1);
        ASSERT_TRUE(part.resp.ok()) << part.resp.status.msg;
        EXPECT_TRUE(
                client.complete_multipart_upload(path, *create.upload_id, {{.part_num = 1}}).ok());
    };
    auto first = std::async(std::launch::async, upload, "first");
    auto second = std::async(std::launch::async, upload, "second");
    first.get();
    second.get();
    std::sort(transport->requests.begin(), transport->requests.end());
    const std::vector<std::pair<std::string, std::string>> expected {
            {"first/shared", "block"},
            {"first/shared", "blocklist"},
            {"second/shared", "block"},
            {"second/shared", "blocklist"}};
    EXPECT_EQ(transport->requests, expected);
}

TEST(AzureObjStorageClientTlsHelperTest, detects_tls_ca_error) {
    EXPECT_TRUE(io::is_azure_tls_ca_error_message(
            "Problem with the SSL CA cert (path? access rights?)"));
    EXPECT_TRUE(io::is_azure_tls_ca_error_message(
            "curl error: peer failed verification for cert chain"));
    EXPECT_TRUE(io::is_azure_tls_ca_error_message("unable to get local issuer certificate"));
    EXPECT_FALSE(io::is_azure_tls_ca_error_message("AuthenticationFailed"));
}

TEST(AzureObjStorageClientTlsHelperTest, appends_debug_suffix_only_for_tls_ca_error) {
    std::string_view debug_ctx = "tls_debug(selected_ca_file='/etc/ssl/certs/ca-bundle.crt')";

    EXPECT_EQ(io::build_azure_tls_debug_suffix(
                      "Problem with the SSL CA cert (path? access rights?)", debug_ctx),
              ", tls_debug(selected_ca_file='/etc/ssl/certs/ca-bundle.crt')");
    EXPECT_EQ(io::build_azure_tls_debug_suffix("AuthenticationFailed", debug_ctx), "");
    EXPECT_EQ(io::build_azure_tls_debug_suffix(
                      "Problem with the SSL CA cert (path? access rights?)", ""),
              "");
}

TEST(AzureObjStorageClientBatchDeleteTest, failure_message_preserves_object_key) {
    EXPECT_EQ(io::build_azure_batch_delete_failure_message({.bucket = "container"},
                                                           "directory/failed-blob"),
              "Azure batch delete failed, path msg bucket container, key directory/failed-blob, "
              "prefix , path ");
}

class AzureObjStorageClientTest : public testing::Test {
protected:
    static std::shared_ptr<ObjStorageClient> obj_storage_client;
    inline static std::string container;

    static void SetUpTestSuite() {
        if (!std::getenv("AZURE_ACCOUNT_NAME") || !std::getenv("AZURE_ACCOUNT_KEY") ||
            !std::getenv("AZURE_CONTAINER_NAME")) {
            return;
        }

        std::string accountName = std::getenv("AZURE_ACCOUNT_NAME");
        std::string accountKey = std::getenv("AZURE_ACCOUNT_KEY");
        container = std::getenv("AZURE_CONTAINER_NAME");
        AzureObjStorageConfig config;
        config.connection.endpoint = fmt::format("https://{}.blob.core.windows.net", accountName);
        config.auth = {std::move(accountName), std::move(accountKey)};
        auto client_result = ObjStorageClientFactory::instance().create(config);
        ASSERT_TRUE(client_result.has_value()) << client_result.error();
        AzureObjStorageClientTest::obj_storage_client = std::move(client_result).value();
    }

    void SetUp() override {
        if (AzureObjStorageClientTest::obj_storage_client == nullptr) {
            GTEST_SKIP() << "Skipping Azure test, because AZURE environment not set";
        }
    }
};

std::shared_ptr<ObjStorageClient> AzureObjStorageClientTest::obj_storage_client = nullptr;

TEST_F(AzureObjStorageClientTest, put_list_delete_object) {
    LOG(INFO) << "AzureObjStorageClientTest::put_list_delete_object";

    auto response = AzureObjStorageClientTest::obj_storage_client->put_object(
            {.bucket = container, .key = "AzureObjStorageClientTest/put_list_delete_object"},
            std::string("aaaa"));
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    std::vector<ObjectMeta> objects;
    response = AzureObjStorageClientTest::obj_storage_client->list_objects(
            {.bucket = container, .prefix = "AzureObjStorageClientTest/put_list_delete_object"},
            &objects);
    EXPECT_TRUE(response.ok());
    EXPECT_EQ(objects.size(), 1);
    objects.clear();

    response = AzureObjStorageClientTest::obj_storage_client->delete_object(
            {.bucket = container, .key = "AzureObjStorageClientTest/put_list_delete_object"});
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    response = AzureObjStorageClientTest::obj_storage_client->list_objects(
            {.bucket = container, .prefix = "AzureObjStorageClientTest/put_list_delete_object"},
            &objects);
    EXPECT_TRUE(response.ok());
    EXPECT_TRUE(objects.empty());
}

TEST_F(AzureObjStorageClientTest, delete_objects_recursively) {
    LOG(INFO) << "AzureObjStorageClientTest::delete_objects_recursively";

    for (int i = 0; i < 22; i++) {
        std::string key =
                "AzureObjStorageClientTest/delete_objects_recursively" + std::to_string(i);

        auto response = AzureObjStorageClientTest::obj_storage_client->put_object(
                {.bucket = container, .key = key}, std::string("aaaa"));
        EXPECT_EQ(response.status.code, ErrorCode::OK);
        LOG(INFO) << "put " << key << " OK";
    }

    std::vector<ObjectMeta> objects;
    auto response = AzureObjStorageClientTest::obj_storage_client->list_objects(
            {.bucket = container, .prefix = "AzureObjStorageClientTest/delete_objects_recursively"},
            &objects);
    EXPECT_TRUE(response.ok());
    EXPECT_EQ(objects.size(), 22);
    objects.clear();

    response = delete_objects_recursively(
            AzureObjStorageClientTest::obj_storage_client,
            {.bucket = container,
             .prefix = "AzureObjStorageClientTest/delete_objects_recursively"});
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    response = AzureObjStorageClientTest::obj_storage_client->list_objects(
            {.bucket = container, .prefix = "AzureObjStorageClientTest/delete_objects_recursively"},
            &objects);
    EXPECT_TRUE(response.ok());
    EXPECT_TRUE(objects.empty());
}

TEST_F(AzureObjStorageClientTest, concurrent_multipart_uploads_do_not_share_staged_blocks) {
    ObjStoragePath first {.bucket = container,
                          .key = "AzureObjStorageClientTest/concurrent_multipart"};
    ObjStoragePath second = first;
    auto first_create = obj_storage_client->create_multipart_upload(first);
    auto second_create = obj_storage_client->create_multipart_upload(second);
    ASSERT_EQ(first_create.resp.status.code, ErrorCode::OK);
    ASSERT_EQ(second_create.resp.status.code, ErrorCode::OK);
    ASSERT_TRUE(first_create.upload_id.has_value());
    ASSERT_TRUE(second_create.upload_id.has_value());
    ASSERT_NE(first_create.upload_id, second_create.upload_id);

    auto first_part = obj_storage_client->upload_part(first, *first_create.upload_id, "first", 1);
    auto second_part =
            obj_storage_client->upload_part(second, *second_create.upload_id, "second", 1);
    ASSERT_EQ(first_part.resp.status.code, ErrorCode::OK);
    ASSERT_EQ(second_part.resp.status.code, ErrorCode::OK);
    ASSERT_NE(first_part.etag, second_part.etag);
    ASSERT_EQ(obj_storage_client
                      ->complete_multipart_upload(first, *first_create.upload_id, {{.part_num = 1}})
                      .status.code,
              ErrorCode::OK);
    ASSERT_NE(
            obj_storage_client
                    ->complete_multipart_upload(second, *second_create.upload_id, {{.part_num = 1}})
                    .status.code,
            ErrorCode::OK);

    std::array<char, 5> contents {};
    size_t size_return = 0;
    ASSERT_EQ(obj_storage_client
                      ->get_object(second, contents.data(), 0, contents.size(), &size_return)
                      .status.code,
              ErrorCode::OK);
    EXPECT_EQ(std::string_view(contents.data(), size_return), "first");
    EXPECT_EQ(obj_storage_client->delete_object(second).status.code, ErrorCode::OK);
}
#else

class AzureObjStorageClientTest : public testing::Test {
protected:
    void SetUp() override { GTEST_SKIP() << "Skipping Azure test, because USE_AZURE not defined"; }
};

TEST_F(AzureObjStorageClientTest, dummy_test) {
    LOG(INFO) << "AzureObjStorageClientTest::dummy_test";
}

#endif // #ifdef USE_AZURE

} // namespace doris
