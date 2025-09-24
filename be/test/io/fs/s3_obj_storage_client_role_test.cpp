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

#include "io/fs/obj_storage_client.h"
#include "util/s3_util.h"

namespace doris {

class S3ObjStorageClientRoleTest : public testing::Test {
protected:
    static std::shared_ptr<io::ObjStorageClient> obj_storage_client;
    static std::string bucket;
    static std::string prefix;

    static void SetUpTestSuite() {
        if (!std::getenv("AWS_ROLE_ARN") || !std::getenv("AWS_EXTERNAL_ID") ||
            !std::getenv("AWS_ENDPOINT") || !std::getenv("AWS_REGION")) {
            return;
        }

        std::string role_arn = std::getenv("AWS_ROLE_ARN");
        std::string external_id = std::getenv("AWS_EXTERNAL_ID");
        std::string endpoint = std::getenv("AWS_ENDPOINT");
        std::string region = std::getenv("AWS_REGION");

        S3ObjStorageClientRoleTest::bucket = std::getenv("AWS_BUCKET");

        S3ObjStorageClientRoleTest::bucket = std::getenv("AWS_BUCKET");
        if (!std::getenv("AWS_PREFIX")) {
            S3ObjStorageClientRoleTest::prefix = "";
        } else {
            S3ObjStorageClientRoleTest::prefix = std::getenv("AWS_PREFIX");
        }

        S3ObjStorageClientRoleTest::obj_storage_client = S3ClientFactory::instance().create(
                {.endpoint = endpoint,
                 .region = region,
                 .ak = "",
                 .sk = "",
                 .token = "",
                 .bucket = bucket,
                 .provider = io::ObjStorageType::AWS,
                 .use_virtual_addressing = false,
                 .cred_provider_type = CredProviderType::InstanceProfile,
                 .role_arn = role_arn,
                 .external_id = external_id});

        ASSERT_TRUE(S3ObjStorageClientRoleTest::obj_storage_client != nullptr);
    }

    void SetUp() override {
        if (S3ObjStorageClientRoleTest::obj_storage_client == nullptr) {
            GTEST_SKIP() << "Skipping S3 test, because AWS environment not set";
        }
    }
};

std::shared_ptr<io::ObjStorageClient> S3ObjStorageClientRoleTest::obj_storage_client = nullptr;
std::string S3ObjStorageClientRoleTest::bucket;
std::string S3ObjStorageClientRoleTest::prefix;

TEST_F(S3ObjStorageClientRoleTest, put_list_delete_object) {
    LOG(INFO) << "S3ObjStorageClientRoleTest::put_list_delete_object, prefix:" << prefix;

    auto response = S3ObjStorageClientRoleTest::obj_storage_client->put_object(
            {.bucket = bucket, .key = prefix + "S3ObjStorageClientRoleTest/put_list_delete_object"},
            Slice(std::string("aaaa")));
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    std::vector<io::FileInfo> files;
    // clang-format off
    response = S3ObjStorageClientRoleTest::obj_storage_client->list_objects({.bucket = bucket,
            .prefix = prefix + "S3ObjStorageClientRoleTest/put_list_delete_object",}, &files);
    // clang-format on
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(files.size(), 1);
    files.clear();

    response = S3ObjStorageClientRoleTest::obj_storage_client->delete_object(
            {.bucket = bucket,
             .key = prefix + "S3ObjStorageClientRoleTest/put_list_delete_object"});
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    // clang-format off
    response = S3ObjStorageClientRoleTest::obj_storage_client->list_objects({.bucket = bucket,
            .prefix = prefix + "S3ObjStorageClientRoleTest/put_list_delete_object",}, &files);
    // clang-format on
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(files.size(), 0);
}

TEST_F(S3ObjStorageClientRoleTest, delete_objects_recursively) {
    LOG(INFO) << "S3ObjStorageClientRoleTest::delete_objects_recursively";

    for (int i = 0; i < 22; i++) {
        std::string key = prefix + "S3ObjStorageClientRoleTest/delete_objects_recursively" +
                          std::to_string(i);

        auto response = S3ObjStorageClientRoleTest::obj_storage_client->put_object(
                {.bucket = bucket, .key = key}, Slice(std::string("aaaa")));
        EXPECT_EQ(response.status.code, ErrorCode::OK);
        LOG(INFO) << "put " << key << " OK";
    }

    std::vector<io::FileInfo> files;
    // clang-format off
    auto response = S3ObjStorageClientRoleTest::obj_storage_client->list_objects({.bucket = bucket,
            .prefix = prefix + "S3ObjStorageClientRoleTest/delete_objects_recursively",}, &files);
    // clang-format on
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(files.size(), 22);
    files.clear();

    response = S3ObjStorageClientRoleTest::obj_storage_client->delete_objects_recursively(
            {.bucket = bucket,
             .prefix = prefix + "S3ObjStorageClientRoleTest/delete_objects_recursively"});
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    // clang-format off
    response = S3ObjStorageClientRoleTest::obj_storage_client->list_objects({.bucket = bucket,
            .prefix = prefix + "S3ObjStorageClientRoleTest/delete_objects_recursively",}, &files);
    // clang-format on
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(files.size(), 0);
}

TEST_F(S3ObjStorageClientRoleTest, multipart_upload) {
    LOG(INFO) << "S3ObjStorageClientRoleTest::multipart_upload";

    auto response = S3ObjStorageClientRoleTest::obj_storage_client->create_multipart_upload(
            {.bucket = bucket, .key = prefix + "S3ObjStorageClientRoleTest/multipart_upload"});
    EXPECT_EQ(response.resp.status.code, ErrorCode::OK);
    auto upload_id = response.upload_id;

    std::string body = "S3ObjStorageClientRoleTest::multipart_upload";
    body.resize(5 * 1024 * 1024);

    std::vector<doris::io::ObjectCompleteMultiPart> completed_parts;

    response = S3ObjStorageClientRoleTest::obj_storage_client->upload_part(
            {.bucket = bucket,
             .key = prefix + "S3ObjStorageClientRoleTest/multipart_upload",
             .upload_id = upload_id},
            Slice(body), 1);

    EXPECT_EQ(response.resp.status.code, ErrorCode::OK);
    doris::io::ObjectCompleteMultiPart completed_part {
            1, response.etag.has_value() ? std::move(response.etag.value()) : ""};

    completed_parts.emplace_back(std::move(completed_part));

    response = S3ObjStorageClientRoleTest::obj_storage_client->upload_part(
            {.bucket = bucket,
             .key = prefix + "S3ObjStorageClientRoleTest/multipart_upload",
             .upload_id = upload_id},
            Slice(body), 2);

    EXPECT_EQ(response.resp.status.code, ErrorCode::OK);
    doris::io::ObjectCompleteMultiPart completed_part2 {
            2, response.etag.has_value() ? std::move(response.etag.value()) : ""};
    completed_parts.emplace_back(std::move(completed_part2));

    auto response2 = S3ObjStorageClientRoleTest::obj_storage_client->complete_multipart_upload(
            {.bucket = bucket,
             .key = prefix + "S3ObjStorageClientRoleTest/multipart_upload",
             .upload_id = upload_id},
            completed_parts);

    EXPECT_EQ(response2.status.code, ErrorCode::OK);
}

} // namespace doris