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

#ifdef USE_AZURE

using namespace Azure::Storage::Blobs;

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