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

#include "cpp/obj-storage/client/s3_obj_storage_client.h"

#include <gtest/gtest.h>

#include "cpp/obj-storage/client/gcp_obj_storage_client.h"
#include "cpp/obj-storage/client/obj_storage_client.h"
#include "cpp/obj-storage/client/obj_storage_client_factory.h"
#include "cpp/obj-storage/client/oss_obj_storage_client.h"
#include "io/fs/file_system.h"

namespace doris {

TEST(S3ObjStorageSignedUrlTest, DelegatesProviderSpecificSigning) {
    std::shared_ptr<ObjStorageClient> client = std::make_shared<GcpObjStorageClient>(
            nullptr, ObjStorageEndpointInfo {}, [](const ObjStoragePath& path, int64_t expiration) {
                EXPECT_EQ(path.bucket, "bucket");
                EXPECT_EQ(path.key, "nested/key");
                EXPECT_EQ(expiration, 300);
                return std::string {"https://storage.googleapis.com/bucket/nested/key?signed"};
            });
    EXPECT_EQ(client->generate_presigned_url({.bucket = "bucket", .key = "nested/key"}, 300),
              "https://storage.googleapis.com/bucket/nested/key?signed");
    EXPECT_EQ(client->generate_public_presigned_url({.bucket = "bucket", .key = "nested/key"}, 300),
              "https://storage.googleapis.com/bucket/nested/key?signed");
}

TEST(S3ObjStorageSignedUrlTest, OssPublicSigningPreservesPrivateClientAndSigningOptions) {
    for (bool path_style : {false, true}) {
        OssObjStorageConfig config;
        config.connection.endpoint = "https://oss-cn-beijing-internal.aliyuncs.com";
        config.region = "cn-beijing";
        config.auth = AccessKeyObjStorageAuth {"oss-test-ak", "oss-test-sk", "oss-test-token"};
        config.use_path_style = path_style;
        auto built = OssObjStorageClient::create(config);
        ASSERT_TRUE(built.client) << built.error;
        ObjStoragePath path {.bucket = "test-bucket", .key = "nested/key"};
        const std::string public_prefix =
                path_style ? "https://oss-cn-beijing.aliyuncs.com/test-bucket/nested/key?"
                           : "https://test-bucket.oss-cn-beijing.aliyuncs.com/nested/key?";

        auto private_url = built.client->generate_presigned_url(path, 300);
        EXPECT_NE(private_url.find("-internal.aliyuncs.com"), std::string::npos);
        auto public_url = built.client->generate_public_presigned_url(path, 300);
        EXPECT_TRUE(public_url.starts_with(public_prefix)) << public_url;
        EXPECT_NE(public_url.find("X-Amz-Credential=oss-test-ak%2F"), std::string::npos);
        EXPECT_NE(public_url.find("X-Amz-Security-Token=oss-test-token"), std::string::npos);
        EXPECT_NE(public_url.find("X-Amz-Expires=300"), std::string::npos);
        EXPECT_NE(built.client->generate_presigned_url(path, 300).find("-internal.aliyuncs.com"),
                  std::string::npos);

        config.connection.endpoint = "https://oss-cn-beijing.aliyuncs.com";
        auto public_client = OssObjStorageClient::create(config);
        ASSERT_TRUE(public_client.client) << public_client.error;
        EXPECT_TRUE(public_client.client->generate_public_presigned_url(path, 300).starts_with(
                public_prefix));
    }
}

class S3ObjStorageClientTest : public testing::Test {
protected:
    static std::shared_ptr<ObjStorageClient> obj_storage_client;
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

        AwsObjStorageConfig config;
        config.connection.endpoint = endpoint;
        config.region = "dummy-region";
        config.auth.source = AccessKeyObjStorageAuth {access_key, secret_key, {}};
        config.use_path_style = true;
        auto client_result = ObjStorageClientFactory::instance().create(config);
        ASSERT_NE(client_result.client, nullptr) << client_result.error;
        S3ObjStorageClientTest::obj_storage_client = std::move(client_result.client);
    }

    static void TearDownTestSuite() {
        // Do not retain an SDK client past exit-time destruction of AWS logging globals.
        obj_storage_client.reset();
    }

    void SetUp() override {
        if (S3ObjStorageClientTest::obj_storage_client == nullptr) {
            GTEST_SKIP() << "Skipping S3 test, because AWS environment not set";
        }
    }
};

std::shared_ptr<ObjStorageClient> S3ObjStorageClientTest::obj_storage_client = nullptr;
std::string S3ObjStorageClientTest::bucket;

TEST(S3ObjStorageClientErrorTest, MapsProviderResponseCodes) {
    Aws::S3::S3Error error;
    error.SetResponseCode(Aws::Http::HttpResponseCode::FORBIDDEN);

    EXPECT_EQ(s3fs_error(error, "access denied").code, ErrorCode::PERMISSION_DENIED);
    static_assert(ObjStorageStatus::PERMISSION_DENIED == ErrorCode::PERMISSION_DENIED);

    error.SetResponseCode(Aws::Http::HttpResponseCode::REQUEST_NOT_MADE);
    EXPECT_EQ(s3fs_error(error, "network failure").code, ObjStorageStatus::NETWORK_ERROR);

    error.SetResponseCode(Aws::Http::HttpResponseCode::TOO_MANY_REQUESTS);
    EXPECT_EQ(s3fs_error(error, "throttled").code, ObjStorageStatus::LIMIT_REACH);
}

TEST_F(S3ObjStorageClientTest, put_list_delete_object) {
    LOG(INFO) << "S3ObjStorageClientTest::put_list_delete_object";

    auto response = S3ObjStorageClientTest::obj_storage_client->put_object(
            {.bucket = bucket, .key = "S3ObjStorageClientTest/put_list_delete_object"},
            std::string("aaaa"));
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    std::vector<ObjectMeta> objects;
    response = S3ObjStorageClientTest::obj_storage_client->list_objects(
            {.bucket = bucket, .key = "S3ObjStorageClientTest/put_list_delete_object"}, &objects);
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(objects.size(), 1);
    objects.clear();

    response = S3ObjStorageClientTest::obj_storage_client->delete_object(
            {.bucket = bucket, .key = "S3ObjStorageClientTest/put_list_delete_object"});
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    response = S3ObjStorageClientTest::obj_storage_client->list_objects(
            {.bucket = bucket, .key = "S3ObjStorageClientTest/put_list_delete_object"}, &objects);
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_TRUE(objects.empty());
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

    std::vector<ObjectMeta> objects;
    auto response = S3ObjStorageClientTest::obj_storage_client->list_objects(
            {.bucket = bucket, .key = "S3ObjStorageClientTest/delete_objects_recursively"},
            &objects);
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(objects.size(), 22);
    objects.clear();

    response = delete_objects_recursively(
            S3ObjStorageClientTest::obj_storage_client,
            {.bucket = bucket, .prefix = "S3ObjStorageClientTest/delete_objects_recursively"});
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    response = S3ObjStorageClientTest::obj_storage_client->list_objects(
            {.bucket = bucket, .key = "S3ObjStorageClientTest/delete_objects_recursively"},
            &objects);
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_TRUE(objects.empty());
}

TEST_F(S3ObjStorageClientTest, multipart_upload) {
    LOG(INFO) << "S3ObjStorageClientTest::multipart_upload";

    auto response = S3ObjStorageClientTest::obj_storage_client->create_multipart_upload(
            {.bucket = bucket, .key = "S3ObjStorageClientTest/multipart_upload"});
    EXPECT_EQ(response.resp.status.code, ErrorCode::OK);
    ASSERT_TRUE(response.upload_id.has_value());
    const auto& upload_id = *response.upload_id;

    std::string body = "S3ObjStorageClientTest::multipart_upload";
    body.resize(5 * 1024 * 1024);

    std::vector<ObjStorageCompletedPart> completed_parts;

    response = S3ObjStorageClientTest::obj_storage_client->upload_part(
            {.bucket = bucket, .key = "S3ObjStorageClientTest/multipart_upload"}, upload_id, body,
            1);

    EXPECT_EQ(response.resp.status.code, ErrorCode::OK);
    ObjStorageCompletedPart completed_part {
            .part_num = 1,
            .etag = response.etag.has_value() ? std::move(response.etag.value()) : ""};

    completed_parts.emplace_back(std::move(completed_part));

    response = S3ObjStorageClientTest::obj_storage_client->upload_part(
            {.bucket = bucket, .key = "S3ObjStorageClientTest/multipart_upload"}, upload_id, body,
            2);

    EXPECT_EQ(response.resp.status.code, ErrorCode::OK);
    ObjStorageCompletedPart completed_part2 {
            .part_num = 2,
            .etag = response.etag.has_value() ? std::move(response.etag.value()) : ""};
    completed_parts.emplace_back(std::move(completed_part2));

    auto response2 = S3ObjStorageClientTest::obj_storage_client->complete_multipart_upload(
            {.bucket = bucket, .key = "S3ObjStorageClientTest/multipart_upload"}, upload_id,
            completed_parts);

    EXPECT_EQ(response2.status.code, ErrorCode::OK);
}

} // namespace doris
