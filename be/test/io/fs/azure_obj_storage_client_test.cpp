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

#include "io/fs/azure_obj_storage_client.h"

#include <gtest/gtest.h>

#include <array>

#include "io/fs/file_system.h"
#include "io/fs/obj_storage_client.h"
#include "util/s3_util.h"

#ifdef USE_AZURE
#include <azure/storage/blobs.hpp>
#include <azure/storage/blobs/blob_client.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>
#include <azure/storage/common/storage_credential.hpp>
#endif

namespace doris {

TEST(AzureObjStorageClientAbortHelperTest, preserves_committed_put_blob_without_block_list) {
    EXPECT_FALSE(io::azure_block_list_has_committed_blob(0, false));
    EXPECT_TRUE(io::azure_block_list_has_committed_blob(0, true));
    EXPECT_TRUE(io::azure_block_list_has_committed_blob(1, false));
}

#ifdef USE_AZURE

using namespace Azure::Storage::Blobs;

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

class AzureObjStorageClientTest : public testing::Test {
protected:
    static std::shared_ptr<io::ObjStorageClient> obj_storage_client;

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

        AzureObjStorageClientTest::obj_storage_client = S3ClientFactory::instance().create(
                {.endpoint = fmt::format("https://{}.blob.core.windows.net", accountName),
                 .region = "dummy-region",
                 .ak = accountName,
                 .sk = accountKey,
                 .token = "",
                 .bucket = containerName,
                 .provider = io::ObjStorageType::AZURE,
                 .role_arn = "",
                 .external_id = ""});
    }

    void SetUp() override {
        if (AzureObjStorageClientTest::obj_storage_client == nullptr) {
            GTEST_SKIP() << "Skipping Azure test, because AZURE environment not set";
        }
    }
};

std::shared_ptr<io::ObjStorageClient> AzureObjStorageClientTest::obj_storage_client = nullptr;

TEST_F(AzureObjStorageClientTest, put_list_delete_object) {
    LOG(INFO) << "AzureObjStorageClientTest::put_list_delete_object";

    auto response = AzureObjStorageClientTest::obj_storage_client->put_object(
            {.key = "AzureObjStorageClientTest/put_list_delete_object"}, std::string("aaaa"));
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    std::vector<io::FileInfo> files;
    // clang-format off
    response = AzureObjStorageClientTest::obj_storage_client->list_objects({.bucket = "dummy",
            .prefix = "AzureObjStorageClientTest/put_list_delete_object",}, &files);
    // clang-format on
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(files.size(), 1);
    files.clear();

    response = AzureObjStorageClientTest::obj_storage_client->delete_object(
            {.key = "AzureObjStorageClientTest/put_list_delete_object"});
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    // clang-format off
    response = AzureObjStorageClientTest::obj_storage_client->list_objects({.bucket = "dummy",
            .prefix = "AzureObjStorageClientTest/put_list_delete_object",}, &files);
    // clang-format on
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(files.size(), 0);
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

    std::vector<io::FileInfo> files;
    // clang-format off
    auto response = AzureObjStorageClientTest::obj_storage_client->list_objects({.bucket = "dummy",
            .prefix = "AzureObjStorageClientTest/delete_objects_recursively",}, &files);
    // clang-format on
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(files.size(), 22);
    files.clear();

    response = AzureObjStorageClientTest::obj_storage_client->delete_objects_recursively(
            {.prefix = "AzureObjStorageClientTest/delete_objects_recursively"});
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    // clang-format off
    response = AzureObjStorageClientTest::obj_storage_client->list_objects({.bucket = "dummy",
            .prefix = "AzureObjStorageClientTest/delete_objects_recursively",}, &files);
    // clang-format on
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(files.size(), 0);
}

TEST_F(AzureObjStorageClientTest, abort_multipart_upload_discards_staged_blocks) {
    io::ObjectStoragePathOptions opts;
    auto create_response =
            AzureObjStorageClientTest::obj_storage_client->create_multipart_upload(opts);
    ASSERT_EQ(create_response.resp.status.code, ErrorCode::OK);
    ASSERT_TRUE(create_response.upload_id.has_value());
    opts.key = "AzureObjStorageClientTest/abort_multipart_upload_" + *create_response.upload_id;
    opts.upload_id = create_response.upload_id;

    auto upload_response =
            AzureObjStorageClientTest::obj_storage_client->upload_part(opts, "staged", 1);
    ASSERT_EQ(upload_response.resp.status.code, ErrorCode::OK);
    auto abort_response =
            AzureObjStorageClientTest::obj_storage_client->abort_multipart_upload(opts);
    ASSERT_EQ(abort_response.status.code, ErrorCode::OK);

    auto head_response = AzureObjStorageClientTest::obj_storage_client->head_object(opts);
    EXPECT_EQ(head_response.resp.status.code, ErrorCode::NOT_FOUND);
}

TEST_F(AzureObjStorageClientTest, abort_multipart_upload_preserves_existing_put_blob) {
    io::ObjectStoragePathOptions opts;
    auto create_response =
            AzureObjStorageClientTest::obj_storage_client->create_multipart_upload(opts);
    ASSERT_EQ(create_response.resp.status.code, ErrorCode::OK);
    ASSERT_TRUE(create_response.upload_id.has_value());
    opts.key = "AzureObjStorageClientTest/abort_preserves_put_blob_" + *create_response.upload_id;
    opts.upload_id = create_response.upload_id;

    auto put_response = AzureObjStorageClientTest::obj_storage_client->put_object(opts, "original");
    ASSERT_EQ(put_response.status.code, ErrorCode::OK);
    auto upload_response =
            AzureObjStorageClientTest::obj_storage_client->upload_part(opts, "replacement", 1);
    ASSERT_EQ(upload_response.resp.status.code, ErrorCode::OK);

    auto abort_response =
            AzureObjStorageClientTest::obj_storage_client->abort_multipart_upload(opts);
    ASSERT_EQ(abort_response.status.code, ErrorCode::OK);
    std::array<char, 8> contents {};
    size_t size_return = 0;
    auto get_response = AzureObjStorageClientTest::obj_storage_client->get_object(
            opts, contents.data(), 0, contents.size(), &size_return);
    ASSERT_EQ(get_response.status.code, ErrorCode::OK);
    EXPECT_EQ(size_return, contents.size());
    EXPECT_EQ(std::string_view(contents.data(), contents.size()), "original");

    EXPECT_EQ(AzureObjStorageClientTest::obj_storage_client->delete_object(opts).status.code,
              ErrorCode::OK);
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
