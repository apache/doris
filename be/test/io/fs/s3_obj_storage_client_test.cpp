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

class S3ObjStorageClientTest : public testing::Test {
protected:
    static std::shared_ptr<io::ObjStorageClient> obj_storage_client;
    static std::string bucket;

    static void SetUpTestSuite() {
        if (!std::getenv("AWS_ACCESS_KEY") || !std::getenv("AWS_SECRET_KEY") ||
            !std::getenv("AWS_ENDPOINT") || !std::getenv("AWS_BUCKET")) {
            return;
        }

        std::string access_key = std::getenv("AWS_ACCESS_KEY");
        std::string secret_key = std::getenv("AWS_SECRET_KEY");
        std::string endpoint = std::getenv("AWS_ENDPOINT");

        S3ObjStorageClientTest::bucket = std::getenv("AWS_BUCKET");

        S3ObjStorageClientTest::obj_storage_client = S3ClientFactory::instance().create({
                .endpoint = endpoint,
                .region = "dummy-region",
                .ak = access_key,
                .sk = secret_key,
                .token = "",
                .bucket = bucket,
                .provider = io::ObjStorageType::AWS,
                .use_virtual_addressing = false,
                .role_arn = "",
                .external_id = "",
        });

        ASSERT_TRUE(S3ObjStorageClientTest::obj_storage_client != nullptr);
    }

    void SetUp() override {
        if (S3ObjStorageClientTest::obj_storage_client == nullptr) {
            GTEST_SKIP() << "Skipping S3 test, because AWS environment not set";
        }
    }
};

std::shared_ptr<io::ObjStorageClient> S3ObjStorageClientTest::obj_storage_client = nullptr;
std::string S3ObjStorageClientTest::bucket;

TEST_F(S3ObjStorageClientTest, put_list_delete_object) {
    LOG(INFO) << "S3ObjStorageClientTest::put_list_delete_object";

    auto response = S3ObjStorageClientTest::obj_storage_client->put_object(
            {.bucket = bucket, .key = "S3ObjStorageClientTest/put_list_delete_object"},
            std::string("aaaa"));
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    std::vector<io::FileInfo> files;
    // clang-format off
    response = S3ObjStorageClientTest::obj_storage_client->list_objects({.bucket = bucket,
            .prefix = "S3ObjStorageClientTest/put_list_delete_object",}, &files);
    // clang-format on
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(files.size(), 1);
    files.clear();

    response = S3ObjStorageClientTest::obj_storage_client->delete_object(
            {.bucket = bucket, .key = "S3ObjStorageClientTest/put_list_delete_object"});
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    // clang-format off
    response = S3ObjStorageClientTest::obj_storage_client->list_objects({.bucket = bucket,
            .prefix = "S3ObjStorageClientTest/put_list_delete_object",}, &files);
    // clang-format on
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(files.size(), 0);
}

TEST_F(S3ObjStorageClientTest, delete_objects_recursively) {
    LOG(INFO) << "S3ObjStorageClientTest::delete_objects_recursively";

    for (int i = 0; i < 22; i++) {
        std::string key = "S3ObjStorageClientTest/delete_objects_recursively" + std::to_string(i);

        auto response = S3ObjStorageClientTest::obj_storage_client->put_object(
                {.bucket = bucket, .key = key}, std::string("aaaa"));
        EXPECT_EQ(response.status.code, ErrorCode::OK);
        LOG(INFO) << "put " << key << " OK";
    }

    std::vector<io::FileInfo> files;
    // clang-format off
    auto response = S3ObjStorageClientTest::obj_storage_client->list_objects({.bucket = bucket,
            .prefix = "S3ObjStorageClientTest/delete_objects_recursively",}, &files);
    // clang-format on
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(files.size(), 22);
    files.clear();

    response = S3ObjStorageClientTest::obj_storage_client->delete_objects_recursively(
            {.bucket = bucket, .prefix = "S3ObjStorageClientTest/delete_objects_recursively"});
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    // clang-format off
    response = S3ObjStorageClientTest::obj_storage_client->list_objects({.bucket = bucket,
            .prefix = "S3ObjStorageClientTest/delete_objects_recursively",}, &files);
    // clang-format on
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(files.size(), 0);
}

TEST_F(S3ObjStorageClientTest, multipart_upload) {
    LOG(INFO) << "S3ObjStorageClientTest::multipart_upload";

    auto response = S3ObjStorageClientTest::obj_storage_client->create_multipart_upload(
            {.bucket = bucket, .key = "S3ObjStorageClientTest/multipart_upload"});
    EXPECT_EQ(response.resp.status.code, ErrorCode::OK);
    auto upload_id = response.upload_id;

    std::string body = "S3ObjStorageClientTest::multipart_upload";
    body.resize(5 * 1024 * 1024);

    std::vector<doris::io::ObjectCompleteMultiPart> completed_parts;

    response = S3ObjStorageClientTest::obj_storage_client->upload_part(
            {.bucket = bucket,
             .key = "S3ObjStorageClientTest/multipart_upload",
             .upload_id = upload_id},
            body, 1);

    EXPECT_EQ(response.resp.status.code, ErrorCode::OK);
    doris::io::ObjectCompleteMultiPart completed_part {
            1, response.etag.has_value() ? std::move(response.etag.value()) : ""};

    completed_parts.emplace_back(std::move(completed_part));

    response = S3ObjStorageClientTest::obj_storage_client->upload_part(
            {.bucket = bucket,
             .key = "S3ObjStorageClientTest/multipart_upload",
             .upload_id = upload_id},
            body, 2);

    EXPECT_EQ(response.resp.status.code, ErrorCode::OK);
    doris::io::ObjectCompleteMultiPart completed_part2 {
            2, response.etag.has_value() ? std::move(response.etag.value()) : ""};
    completed_parts.emplace_back(std::move(completed_part2));

    auto response2 = S3ObjStorageClientTest::obj_storage_client->complete_multipart_upload(
            {.bucket = bucket,
             .key = "S3ObjStorageClientTest/multipart_upload",
             .upload_id = upload_id},
            completed_parts);

    EXPECT_EQ(response2.status.code, ErrorCode::OK);
}

} // namespace doris