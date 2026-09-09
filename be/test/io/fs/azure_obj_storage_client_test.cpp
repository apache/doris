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
#include "cpp/obj-client/azure_obj_storage_client.h"

#include "cpp/obj-client/auth/azure_auth_factory.h"
#endif

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "cpp/obj-client/obj_storage_client.h"
#include "io/file_factory.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "util/s3_uri.h"
#include "util/s3_util.h"

#ifdef USE_AZURE
#include <aws/core/utils/HashingUtils.h>

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
    auto decoded = Aws::Utils::HashingUtils::Base64Decode(first_block);
    ASSERT_EQ(first_upload.size() + sizeof(uint32_t), decoded.GetLength());
    EXPECT_EQ(first_upload,
              std::string_view(reinterpret_cast<const char*>(decoded.GetUnderlyingData()),
                               first_upload.size()));
    EXPECT_EQ(1, decoded.GetUnderlyingData()[first_upload.size()]);
    EXPECT_EQ(0, decoded.GetUnderlyingData()[first_upload.size() + 1]);
    EXPECT_EQ(0, decoded.GetUnderlyingData()[first_upload.size() + 2]);
    EXPECT_EQ(0, decoded.GetUnderlyingData()[first_upload.size() + 3]);
}

TEST(AzureObjStorageClientMultipartHelperTest, create_upload_is_provider_free) {
    io::AzureObjStorageClient client(std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> {},
                                     {});

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

TEST(AzureAuthFactoryTest, AllowsEmptySharedKeyCredentials) {
    auto result = AzureAuthFactory::create(
            "https://account.blob.core.windows.net/container",
            {.account_name = "", .account_key = "", .sas_token = {}, .sas_expiration_time_ms = 0},
            {});

    EXPECT_TRUE(result);
}

TEST(AzureAuthFactoryTest, BuildsSasClientWithoutSharedKey) {
    const auto expiry = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count() +
                        3600000;
    auto result = AzureAuthFactory::create("https://account.blob.core.windows.net/container",
                                           {.type = AzureCredentialType::SAS,
                                            .account_name = {},
                                            .account_key = {},
                                            .sas_token = "?sv=2024-01-01&sr=c&sig=temporary",
                                            .sas_expiration_time_ms = expiry},
                                           {});

    ASSERT_TRUE(result);
    EXPECT_NE(result.container_client->GetUrl().find("sv=2024-01-01"), std::string::npos);
    EXPECT_NE(result.container_client->GetUrl().find("sig=temporary"), std::string::npos);
    EXPECT_EQ(result.shared_key_credential, nullptr);
}

TEST(AzureAuthFactoryTest, RejectsExpiredOrMalformedSas) {
    auto expired = AzureAuthFactory::create("https://account.blob.core.windows.net/container",
                                            {.type = AzureCredentialType::SAS,
                                             .account_name = {},
                                             .account_key = {},
                                             .sas_token = "sv=2024-01-01&sig=expired",
                                             .sas_expiration_time_ms = 1},
                                            {});
    EXPECT_FALSE(expired);
    EXPECT_NE(expired.error.find("expired"), std::string::npos);

    auto empty = AzureAuthFactory::create("https://account.blob.core.windows.net/container",
                                          {.type = AzureCredentialType::SAS,
                                           .account_name = {},
                                           .account_key = {},
                                           .sas_token = {},
                                           .sas_expiration_time_ms = 0},
                                          {});
    EXPECT_FALSE(empty);
    EXPECT_NE(empty.error.find("non-empty"), std::string::npos);

    auto newline = AzureAuthFactory::create("https://account.blob.core.windows.net/container",
                                            {.type = AzureCredentialType::SAS,
                                             .account_name = {},
                                             .account_key = {},
                                             .sas_token = "sv=1\nsig=bad",
                                             .sas_expiration_time_ms = 0},
                                            {});
    EXPECT_FALSE(newline);
    EXPECT_NE(newline.error.find("line break"), std::string::npos);

    auto expired_in_token = AzureAuthFactory::create(
            "https://account.blob.core.windows.net/container",
            {.type = AzureCredentialType::SAS,
             .account_name = {},
             .account_key = {},
             .sas_token = "sv=2024-01-01&se=2000-01-01T00%3A00%3A00Z&sig=expired",
             .sas_expiration_time_ms = 0},
            {});
    EXPECT_FALSE(expired_in_token);
    EXPECT_NE(expired_in_token.error.find("expired"), std::string::npos);
}

TEST(AzureAuthFactoryTest, BuildsOAuth2ClientSecretCredential) {
    auto result = AzureAuthFactory::create(
            "https://account.blob.core.windows.net/container",
            {.type = AzureCredentialType::OAUTH2,
             .oauth_client_id = "client-id",
             .oauth_client_secret = "client-secret",
             .oauth_tenant_id = "tenant-id",
             .oauth_server_uri = "https://login.microsoftonline.com/tenant/oauth2/token"},
            {});
    EXPECT_TRUE(result) << result.error;
    EXPECT_NE(result.container_client, nullptr);
}

TEST(AzureAuthFactoryTest, DerivesOAuth2TenantFromServerUri) {
    auto result = AzureAuthFactory::create(
            "https://account.blob.core.windows.net/container",
            {.type = AzureCredentialType::OAUTH2,
             .oauth_client_id = "client-id",
             .oauth_client_secret = "client-secret",
             .oauth_server_uri = "https://login.microsoftonline.com/tenant/oauth2/token"},
            {});
    EXPECT_TRUE(result) << result.error;
}

TEST(AzureAuthFactoryTest, RejectsIncompleteOAuth2Credential) {
    auto result = AzureAuthFactory::create(
            "https://account.blob.core.windows.net/container",
            {.type = AzureCredentialType::OAUTH2,
             .oauth_client_id = "client-id",
             .oauth_server_uri = "https://login.microsoftonline.com/tenant/oauth2/token"},
            {});
    EXPECT_FALSE(result);
    EXPECT_NE(result.error.find("client id"), std::string::npos);
}

TEST(AzureAuthFactoryTest, RejectsConflictingAuthenticationGroups) {
    AzureCredentialOptions shared_key {
            .account_name = "account", .account_key = "key", .sas_token = "sig=secret"};
    EXPECT_FALSE(AzureAuthFactory::create("https://account.blob.core.windows.net/container",
                                          shared_key, {}));
    AzureCredentialOptions oauth {
            .type = AzureCredentialType::OAUTH2,
            .sas_token = "sig=secret",
            .oauth_client_id = "client-id",
            .oauth_client_secret = "client-secret",
            .oauth_server_uri = "https://login.microsoftonline.com/tenant/oauth2/token"};
    auto result =
            AzureAuthFactory::create("https://account.blob.core.windows.net/container", oauth, {});
    EXPECT_FALSE(result);
    EXPECT_EQ(result.error.find("sig=secret"), std::string::npos);
    EXPECT_EQ(result.error.find("client-secret"), std::string::npos);
}

TEST(AzureAuthFactoryTest, RejectsDuplicateOrMissingSasSignatureAndExpiryFields) {
    for (const auto* token : {"sv=1", "sig=", "sig=one&sig=two",
                              "se=2100-01-01T00:00:00Z&se=2099-01-01T00:00:00Z&sig=secret"}) {
        auto result = AzureAuthFactory::create(
                "https://account.blob.core.windows.net/container",
                {.type = AzureCredentialType::SAS, .sas_token = token}, {});
        EXPECT_FALSE(result);
        EXPECT_EQ(result.error.find("sig=secret"), std::string::npos);
    }
}

namespace {

// This transport exercises the real Azure SDK request/response and Doris
// native reader stack without credentials, DNS or a live storage account.
class AzureRangeTransport final : public Azure::Core::Http::HttpTransport {
public:
    explicit AzureRangeTransport(std::string expected_key)
            : _expected_key(std::move(expected_key)) {}

    std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request& request,
                                                         const Azure::Core::Context&) override {
        using namespace Azure::Core::Http;
        const bool head = request.GetMethod() == HttpMethod::Head;
        const auto headers = request.GetHeaders();
        if (head) {
            ++heads;
        } else {
            EXPECT_EQ(request.GetMethod(), HttpMethod::Get);
            ++ranges;
            EXPECT_EQ(headers.at(headers.contains("x-ms-range") ? "x-ms-range" : "range"),
                      "bytes=7-10");
        }
        EXPECT_EQ(request.GetUrl().GetQueryParameters().at("sig"), "a%2Bb%3D");
        EXPECT_EQ(headers.find("Authorization"), headers.end());
        EXPECT_EQ(headers.find("authorization"), headers.end());
        const auto decoded = Azure::Core::Url::Decode(request.GetUrl().GetPath());
        EXPECT_TRUE(decoded.ends_with("container/" + _expected_key));
        auto response = std::make_unique<RawResponse>(
                1, 1, head ? HttpStatusCode::Ok : HttpStatusCode::PartialContent, "OK");
        response->SetHeader("Content-Length", head ? "16" : "4");
        response->SetHeader("Content-Type", "application/octet-stream");
        response->SetHeader("Accept-Ranges", "bytes");
        response->SetHeader("ETag", "\"etag\"");
        response->SetHeader("Last-Modified", "Wed, 01 Jan 2025 00:00:00 GMT");
        response->SetHeader("x-ms-creation-time", "Wed, 01 Jan 2025 00:00:00 GMT");
        response->SetHeader("x-ms-blob-type", "BlockBlob");
        response->SetHeader("x-ms-server-encrypted", "true");
        response->SetHeader("x-ms-lease-status", "unlocked");
        response->SetHeader("x-ms-lease-state", "available");
        if (!head) {
            response->SetHeader("Content-Range", "bytes 7-10/16");
            response->SetBodyStream(std::make_unique<Azure::Core::IO::MemoryBodyStream>(
                    reinterpret_cast<const uint8_t*>("data"), 4));
        }
        return response;
    }

    int heads = 0;
    int ranges = 0;

private:
    std::string _expected_key;
};

void assert_native_sas_reader_range(const std::string& location, const std::string& expected_key) {
    auto transport = std::make_shared<AzureRangeTransport>(expected_key);
    Azure::Storage::Blobs::BlobClientOptions options;
    options.Transport.Transport = transport;
    options.Retry.MaxRetries = 0;
    io::FileSystemProperties properties {
            .system_type = TFileType::FILE_S3,
            .properties = {{"provider", "azure"},
                           {"AZURE_AUTH_TYPE", "SAS"},
                           {"AZURE_ACCOUNT_NAME", "account"},
                           {"AZURE_ENDPOINT", "https://account.blob.core.windows.net"},
                           {"AZURE_SAS_TOKEN", "sv=2024-01-01&sr=c&sig=a%2Bb%3D"}}};
    S3URI uri(location);
    ASSERT_TRUE(uri.parse().ok());
    EXPECT_EQ(uri.get_key(), expected_key);
    S3Conf conf;
    ASSERT_TRUE(
            S3ClientFactory::convert_properties_to_s3_conf(properties.properties, uri, &conf).ok());
    auto built = AzureAuthFactory::create(conf.client_conf.endpoint + "/" + conf.bucket,
                                          conf.client_conf.azure_credentials, std::move(options));
    ASSERT_TRUE(built) << built.error;
    auto native_client = std::make_shared<io::AzureObjStorageClient>(built.container_client,
                                                                     ObjStorageEndpointInfo {});
    S3ClientFactory::instance().set_client_creator_for_test(
            [native_client,
             conf](const S3ClientConf& received) -> std::shared_ptr<io::ObjStorageClient> {
                EXPECT_EQ(received, conf.client_conf);
                EXPECT_EQ(received.provider, ObjStorageProvider::AZURE);
                return native_client;
            });
    auto result = FileFactory::create_file_reader(properties, {.path = location}, {}, nullptr);
    S3ClientFactory::instance().clear_client_creator_for_test();
    ASSERT_TRUE(result.has_value()) << result.error();
    auto reader = std::move(result).value();
    EXPECT_EQ(reader->size(), 16);
    std::array<char, 4> buffer {};
    size_t bytes_read = 0;
    ASSERT_TRUE(reader->read_at(7, Slice(buffer.data(), buffer.size()), &bytes_read).ok());
    EXPECT_EQ(bytes_read, 4);
    EXPECT_EQ(std::string(buffer.data(), bytes_read), "data");
    EXPECT_EQ(transport->heads, 1);
    EXPECT_EQ(transport->ranges, 1);
    EXPECT_TRUE(reader->close().ok());
}

} // namespace

TEST(AzureAuthFactoryTest, NativeAdlsSasReaderPreservesLiteralNamesInHeadAndRangeRead) {
    const std::string key = "path/p=a%2Fb/with%20space/a+%2520/http://example//100%file";
    for (const auto* prefix : {"abfs://container@account.dfs.core.windows.net/",
                               "abfss://container@account.dfs.core.windows.net/",
                               "wasb://container@account.blob.core.windows.net/",
                               "wasbs://container@account.blob.core.windows.net/"}) {
        SCOPED_TRACE(prefix);
        // ADLSLocation passes this raw name to the Java SDK. The C++ SDK must
        // address that same object, not turn the literal %2F into a directory.
        assert_native_sas_reader_range(std::string(prefix) + key, key);
    }
}

TEST(AzureAuthFactoryTest, NativeHttpSasReaderDecodesUrlNamesOnceInHeadAndRangeRead) {
    assert_native_sas_reader_range(
            "https://account.blob.core.windows.net/container/path/p=a%2Fb/with%20space/"
            "a+%2520/http://example//file",
            "path/p=a/b/with space/a+%20/http://example//file");
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

    static void SetUpTestSuite() {
        if (!std::getenv("AZURE_ACCOUNT_NAME") || !std::getenv("AZURE_ACCOUNT_KEY") ||
            !std::getenv("AZURE_CONTAINER_NAME")) {
            return;
        }

        std::string accountName = std::getenv("AZURE_ACCOUNT_NAME");
        std::string accountKey = std::getenv("AZURE_ACCOUNT_KEY");
        std::string containerName = std::getenv("AZURE_CONTAINER_NAME");

        // Initialize Azure SDK
        [[maybe_unused]] auto& s3ClientFactory = S3ClientFactory::instance();

        auto client_result = S3ClientFactory::instance().create(
                {.endpoint = fmt::format("https://{}.blob.core.windows.net", accountName),
                 .region = "dummy-region",
                 .azure_credentials = {.account_name = accountName, .account_key = accountKey},
                 .bucket = containerName,
                 .provider = ObjStorageProvider::AZURE,
                 .role_arn = "",
                 .external_id = ""});
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
            {.key = "AzureObjStorageClientTest/put_list_delete_object"}, std::string("aaaa"));
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    std::vector<ObjectMeta> objects;
    response = AzureObjStorageClientTest::obj_storage_client->list_objects(
            {.bucket = "dummy", .prefix = "AzureObjStorageClientTest/put_list_delete_object"},
            &objects);
    EXPECT_TRUE(response.ok());
    EXPECT_EQ(objects.size(), 1);
    objects.clear();

    response = AzureObjStorageClientTest::obj_storage_client->delete_object(
            {.key = "AzureObjStorageClientTest/put_list_delete_object"});
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    response = AzureObjStorageClientTest::obj_storage_client->list_objects(
            {.bucket = "dummy", .prefix = "AzureObjStorageClientTest/put_list_delete_object"},
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
                {.key = key}, std::string("aaaa"));
        EXPECT_EQ(response.status.code, ErrorCode::OK);
        LOG(INFO) << "put " << key << " OK";
    }

    std::vector<ObjectMeta> objects;
    auto response = AzureObjStorageClientTest::obj_storage_client->list_objects(
            {.bucket = "dummy", .prefix = "AzureObjStorageClientTest/delete_objects_recursively"},
            &objects);
    EXPECT_TRUE(response.ok());
    EXPECT_EQ(objects.size(), 22);
    objects.clear();

    response = delete_objects_recursively(
            AzureObjStorageClientTest::obj_storage_client,
            {.prefix = "AzureObjStorageClientTest/delete_objects_recursively"});
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    response = AzureObjStorageClientTest::obj_storage_client->list_objects(
            {.bucket = "dummy", .prefix = "AzureObjStorageClientTest/delete_objects_recursively"},
            &objects);
    EXPECT_TRUE(response.ok());
    EXPECT_TRUE(objects.empty());
}

TEST_F(AzureObjStorageClientTest, concurrent_multipart_uploads_do_not_share_staged_blocks) {
    ObjStoragePath first {.key = "AzureObjStorageClientTest/concurrent_multipart"};
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
