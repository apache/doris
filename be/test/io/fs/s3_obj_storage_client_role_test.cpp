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

#include <cstdlib>
#include <map>

#include "cpp/obj-storage/client/obj_storage_client.h"
#include "io/fs/file_system.h"
#include "io/object_storage/obj_storage_client_factory.h"
#include "io/object_storage/obj_storage_config_parser.h"
#include "util/s3_uri.h"

namespace doris {

class S3ObjStorageClientRoleTest : public testing::Test {
protected:
    static std::shared_ptr<ObjStorageClient> obj_storage_client;
    static std::string bucket;
    static std::string prefix;

    static void SetUpTestSuite() {
        auto* role_arn = std::getenv("AWS_ROLE_ARN");
        auto* external_id = std::getenv("AWS_EXTERNAL_ID");
        auto* endpoint = std::getenv("AWS_ENDPOINT");
        auto* region = std::getenv("AWS_REGION");
        auto* bucket_name = std::getenv("AWS_BUCKET");
        if (!role_arn || !external_id || !endpoint || !region || !bucket_name || !*bucket_name) {
            return;
        }

        bucket = bucket_name;
        if (auto* key_prefix = std::getenv("AWS_PREFIX")) {
            prefix = key_prefix;
        }

        S3URI uri("s3://" + bucket);
        ASSERT_TRUE(uri.parse().ok());
        const std::map<std::string, std::string> properties {
                {"provider", "AWS"},
                {"AWS_ENDPOINT", endpoint},
                {"AWS_REGION", region},
                {"AWS_ROLE_ARN", role_arn},
                {"AWS_EXTERNAL_ID", external_id},
                {"AWS_CREDENTIALS_PROVIDER_TYPE", "INSTANCE_PROFILE"},
                {"use_path_style", "true"}};
        auto config = TEST_TRY(ObjStorageConfigParser::from_properties(properties, uri));
        auto client_result = ObjStorageClientFactory::instance().create(config.client);
        ASSERT_TRUE(client_result.has_value()) << client_result.error();
        S3ObjStorageClientRoleTest::obj_storage_client = std::move(client_result).value();
    }

    void SetUp() override {
        if (S3ObjStorageClientRoleTest::obj_storage_client == nullptr) {
            GTEST_SKIP() << "Skipping S3 test, because AWS environment not set";
        }
    }
};

std::shared_ptr<ObjStorageClient> S3ObjStorageClientRoleTest::obj_storage_client = nullptr;
std::string S3ObjStorageClientRoleTest::bucket;
std::string S3ObjStorageClientRoleTest::prefix;

TEST_F(S3ObjStorageClientRoleTest, put_list_delete_object) {
    LOG(INFO) << "S3ObjStorageClientRoleTest::put_list_delete_object, prefix:" << prefix;

    auto response = S3ObjStorageClientRoleTest::obj_storage_client->put_object(
            {.bucket = bucket, .key = prefix + "S3ObjStorageClientRoleTest/put_list_delete_object"},
            std::string("aaaa"));
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    std::vector<ObjectMeta> objects;
    response = S3ObjStorageClientRoleTest::obj_storage_client->list_objects(
            {.bucket = bucket, .key = prefix + "S3ObjStorageClientRoleTest/put_list_delete_object"},
            &objects);
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(objects.size(), 1);
    objects.clear();

    response = S3ObjStorageClientRoleTest::obj_storage_client->delete_object(
            {.bucket = bucket,
             .key = prefix + "S3ObjStorageClientRoleTest/put_list_delete_object"});
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    response = S3ObjStorageClientRoleTest::obj_storage_client->list_objects(
            {.bucket = bucket, .key = prefix + "S3ObjStorageClientRoleTest/put_list_delete_object"},
            &objects);
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_TRUE(objects.empty());
}

TEST_F(S3ObjStorageClientRoleTest, delete_objects_recursively) {
    LOG(INFO) << "S3ObjStorageClientRoleTest::delete_objects_recursively";

    for (int i = 0; i < 22; i++) {
        std::string key = prefix + "S3ObjStorageClientRoleTest/delete_objects_recursively" +
                          std::to_string(i);

        auto response = S3ObjStorageClientRoleTest::obj_storage_client->put_object(
                {.bucket = bucket, .key = key}, std::string("aaaa"));
        EXPECT_EQ(response.status.code, ErrorCode::OK);
        LOG(INFO) << "put " << key << " OK";
    }

    std::vector<ObjectMeta> objects;
    auto response = S3ObjStorageClientRoleTest::obj_storage_client->list_objects(
            {.bucket = bucket,
             .key = prefix + "S3ObjStorageClientRoleTest/delete_objects_recursively"},
            &objects);
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(objects.size(), 22);
    objects.clear();

    response = delete_objects_recursively(
            S3ObjStorageClientRoleTest::obj_storage_client,
            {.bucket = bucket,
             .prefix = prefix + "S3ObjStorageClientRoleTest/delete_objects_recursively"});
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    response = S3ObjStorageClientRoleTest::obj_storage_client->list_objects(
            {.bucket = bucket,
             .key = prefix + "S3ObjStorageClientRoleTest/delete_objects_recursively"},
            &objects);
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_TRUE(objects.empty());
}

TEST_F(S3ObjStorageClientRoleTest, multipart_upload) {
    LOG(INFO) << "S3ObjStorageClientRoleTest::multipart_upload";

    auto response = S3ObjStorageClientRoleTest::obj_storage_client->create_multipart_upload(
            {.bucket = bucket, .key = prefix + "S3ObjStorageClientRoleTest/multipart_upload"});
    EXPECT_EQ(response.resp.status.code, ErrorCode::OK);
    ASSERT_TRUE(response.upload_id.has_value());
    const auto& upload_id = *response.upload_id;

    std::string body = "S3ObjStorageClientRoleTest::multipart_upload";
    body.resize(5 * 1024 * 1024);

    std::vector<ObjStorageCompletedPart> completed_parts;

    response = S3ObjStorageClientRoleTest::obj_storage_client->upload_part(
            {.bucket = bucket, .key = prefix + "S3ObjStorageClientRoleTest/multipart_upload"},
            upload_id, body, 1);

    EXPECT_EQ(response.resp.status.code, ErrorCode::OK);
    ObjStorageCompletedPart completed_part {
            .part_num = 1,
            .etag = response.etag.has_value() ? std::move(response.etag.value()) : ""};

    completed_parts.emplace_back(std::move(completed_part));

    response = S3ObjStorageClientRoleTest::obj_storage_client->upload_part(
            {.bucket = bucket, .key = prefix + "S3ObjStorageClientRoleTest/multipart_upload"},
            upload_id, body, 2);

    EXPECT_EQ(response.resp.status.code, ErrorCode::OK);
    ObjStorageCompletedPart completed_part2 {
            .part_num = 2,
            .etag = response.etag.has_value() ? std::move(response.etag.value()) : ""};
    completed_parts.emplace_back(std::move(completed_part2));

    auto response2 = S3ObjStorageClientRoleTest::obj_storage_client->complete_multipart_upload(
            {.bucket = bucket, .key = prefix + "S3ObjStorageClientRoleTest/multipart_upload"},
            upload_id, completed_parts);

    EXPECT_EQ(response2.status.code, ErrorCode::OK);
}

} // namespace doris
