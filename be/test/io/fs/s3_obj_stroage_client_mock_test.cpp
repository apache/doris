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

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/Object.h>

#include "cpp/obj-client/obj_storage_client.h"
#include "cpp/obj-client/rate_limited_obj_storage_client.h"
#include "cpp/obj-client/s3_express_obj_storage_client.h"
#include "cpp/obj-client/s3_obj_storage_client.h"
#include "gmock/gmock.h"
#include "io/fs/file_system.h"
#include "util/s3_util.h"
#include "util/string_util.h"

using namespace Aws::S3::Model;

namespace doris::io {
class MockS3Client : public Aws::S3::S3Client {
public:
    MockS3Client() {};

    MOCK_METHOD(Aws::S3::Model::ListObjectsV2Outcome, ListObjectsV2,
                (const Aws::S3::Model::ListObjectsV2Request& request), (const, override));
    MOCK_METHOD(Aws::S3::Model::DeleteObjectOutcome, DeleteObject,
                (const Aws::S3::Model::DeleteObjectRequest& request), (const, override));
    MOCK_METHOD(Aws::S3::Model::DeleteObjectsOutcome, DeleteObjects,
                (const Aws::S3::Model::DeleteObjectsRequest& request), (const, override));
    MOCK_METHOD(Aws::S3::Model::UploadPartOutcome, UploadPart,
                (const Aws::S3::Model::UploadPartRequest& request), (const, override));
    MOCK_METHOD(Aws::S3::Model::CompleteMultipartUploadOutcome, CompleteMultipartUpload,
                (const Aws::S3::Model::CompleteMultipartUploadRequest& request), (const, override));
};

class CountingGetRateLimitPolicy final : public ObjStorageRateLimitPolicy {
public:
    explicit CountingGetRateLimitPolicy(size_t* request_count) : request_count_(request_count) {}

    ObjStorageAdmission acquire(S3RateLimitType type, size_t) const override {
        EXPECT_EQ(type, S3RateLimitType::GET);
        ++*request_count_;
        return {};
    }

private:
    size_t* request_count_;
};

class S3ObjStorageClientMockTest : public testing::Test {
    static void SetUpTestSuite() { S3ClientFactory::instance(); };
    static void TearDownTestSuite() {};

private:
    static Aws::SDKOptions options;
};

Aws::SDKOptions S3ObjStorageClientMockTest::options {};

TEST_F(S3ObjStorageClientMockTest, list_objects_compatibility) {
    // If storage only supports ListObjectsV1, s3_obj_storage_client.list_objects
    // should return an error.
    auto mock_s3_client = std::make_shared<MockS3Client>();
    auto s3_obj_storage_client = std::make_shared<S3ObjStorageClient>(mock_s3_client);

    ListObjectsV2Result result;
    result.SetIsTruncated(true);
    EXPECT_CALL(*mock_s3_client, ListObjectsV2(testing::_))
            .WillOnce(testing::Return(ListObjectsV2Outcome(result)));

    std::vector<ObjectMeta> objects;
    auto response = s3_obj_storage_client->list_objects(
            {.bucket = "dummy-bucket", .key = "S3ObjStorageClientMockTest/list_objects_test"},
            &objects);

    EXPECT_TRUE(objects.empty());
    EXPECT_EQ(response.status.code, ErrorCode::INTERNAL_ERROR);
}

ListObjectsV2Result CreatePageResult(const std::string& nextToken,
                                     const std::vector<std::string>& keys, bool isTruncated) {
    ListObjectsV2Result result;
    result.SetIsTruncated(isTruncated);
    result.SetNextContinuationToken(nextToken);
    for (const auto& key : keys) {
        Object obj;
        obj.SetKey(key);
        result.AddContents(std::move(obj));
    }
    return result;
}

UploadPartOutcome CreateExpressUploadPartResult(const UploadPartRequest& request) {
    EXPECT_EQ(request.GetUploadId(), "upload-id");
    EXPECT_EQ(request.GetPartNumber(), 1);
    EXPECT_TRUE(request.ChecksumCRC32CHasBeenSet());
    UploadPartResult result;
    result.SetETag("etag-1");
    result.SetChecksumCRC32C(request.GetChecksumCRC32C());
    return {std::move(result)};
}

CompleteMultipartUploadOutcome VerifyExpressCompleteRequest(
        const CompleteMultipartUploadRequest& request) {
    EXPECT_EQ(request.GetUploadId(), "upload-id");
    const auto& parts = request.GetMultipartUpload().GetParts();
    EXPECT_EQ(parts.size(), 1);
    EXPECT_EQ(parts.front().GetPartNumber(), 1);
    EXPECT_EQ(parts.front().GetETag(), "etag-1");
    EXPECT_FALSE(parts.front().GetChecksumCRC32C().empty());
    return {CompleteMultipartUploadResult {}};
}

TEST_F(S3ObjStorageClientMockTest, s3_express_lists_directory_prefix_and_filters_pages) {
    auto mock_s3_client = std::make_shared<MockS3Client>();
    auto client = std::make_shared<S3ExpressObjStorageClient>(mock_s3_client, mock_s3_client);

    EXPECT_CALL(*mock_s3_client, ListObjectsV2(testing::_))
            .WillOnce([](const ListObjectsV2Request& request) {
                EXPECT_EQ(request.GetPrefix(), "dir/");
                EXPECT_FALSE(request.ContinuationTokenHasBeenSet());
                return ListObjectsV2Outcome(
                        CreatePageResult("next", {"dir/other", "dir/unrelated"}, true));
            })
            .WillOnce([](const ListObjectsV2Request& request) {
                EXPECT_EQ(request.GetPrefix(), "dir/");
                EXPECT_EQ(request.GetContinuationToken(), "next");
                return ListObjectsV2Outcome(
                        CreatePageResult("", {"dir/needle-1", "dir/other"}, false));
            });

    std::vector<ObjectMeta> objects;
    auto response = client->list_objects({.bucket = "dummy-bucket", .key = "dir/needle"}, &objects);

    ASSERT_TRUE(response.ok()) << response.status.msg;
    ASSERT_EQ(objects.size(), 1);
    EXPECT_EQ(objects.front().key, "dir/needle-1");
}

TEST_F(S3ObjStorageClientMockTest, s3_express_propagates_crc32c_to_multipart_completion) {
    auto mock_s3_client = std::make_shared<MockS3Client>();
    auto client = std::make_shared<S3ExpressObjStorageClient>(mock_s3_client, mock_s3_client);
    ObjStoragePath path {.bucket = "dummy-bucket", .key = "object"};

    EXPECT_CALL(*mock_s3_client, UploadPart(testing::_))
            .WillOnce(testing::Invoke(CreateExpressUploadPartResult));

    auto upload = client->upload_part(path, "upload-id", "part body", 1);
    ASSERT_TRUE(upload.resp.ok()) << upload.resp.status.msg;
    ASSERT_TRUE(upload.etag.has_value());
    ASSERT_TRUE(upload.checksum_crc32c.has_value());

    EXPECT_CALL(*mock_s3_client, CompleteMultipartUpload(testing::_))
            .WillOnce(testing::Invoke(VerifyExpressCompleteRequest));

    auto response = client->complete_multipart_upload(
            path, "upload-id",
            {{.part_num = 1,
              .etag = std::move(*upload.etag),
              .checksum_crc32c = std::move(upload.checksum_crc32c)}});
    EXPECT_TRUE(response.ok()) << response.status.msg;
}

TEST_F(S3ObjStorageClientMockTest, list_objects_with_pagination) {
    auto mock_s3_client = std::make_shared<MockS3Client>();
    size_t get_request_count = 0;
    auto inner_client = std::make_shared<S3ObjStorageClient>(mock_s3_client);
    auto obj_storage_client = std::make_shared<RateLimitedObjStorageClient>(
            std::move(inner_client),
            std::make_shared<CountingGetRateLimitPolicy>(&get_request_count));
    std::string prefix = "S3ObjStorageClientMockTest/list_objects_with_pagination/";

    std::vector<std::vector<std::string>> pages = {
            {"key1", "key2"}, // page1
            {"key3", "key4"}, // page2
            {"key5"}          // page3
    };

    for (auto& page : pages) {
        for (auto& key : page) {
            key = prefix + key;
        }
    }

    EXPECT_CALL(*mock_s3_client, ListObjectsV2(testing::_))
            .WillOnce([&](const ListObjectsV2Request& req) {
                // page1：no ContinuationToken
                EXPECT_FALSE(req.ContinuationTokenHasBeenSet());
                return Aws::S3::Model::ListObjectsV2Outcome(
                        CreatePageResult("token1", pages[0], true));
            })
            .WillOnce([&](const ListObjectsV2Request& req) {
                // page2: token1
                EXPECT_EQ(req.GetContinuationToken(), "token1");
                return ListObjectsV2Outcome(CreatePageResult("token2", pages[1], true));
            })
            .WillOnce([&](const ListObjectsV2Request& req) {
                // page3: token2
                EXPECT_EQ(req.GetContinuationToken(), "token2");
                return ListObjectsV2Outcome(CreatePageResult("", pages[2], false));
            });

    std::vector<ObjectMeta> objects;
    auto response = obj_storage_client->list_objects(
            {.bucket = "dummy-bucket",
             .key = "S3ObjStorageClientMockTest/list_objects_with_pagination"},
            &objects);

    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(objects.size(), 5);
    EXPECT_EQ(get_request_count, pages.size());
}

TEST_F(S3ObjStorageClientMockTest,
       delete_object_preserves_not_found_and_batch_uses_delete_objects) {
    auto mock_s3_client = std::make_shared<MockS3Client>();
    auto s3_obj_storage_client = std::make_shared<S3ObjStorageClient>(mock_s3_client);
    auto not_found = [](const DeleteObjectRequest&) {
        Aws::S3::S3Error error;
        error.SetResponseCode(Aws::Http::HttpResponseCode::NOT_FOUND);
        error.SetMessage("object not found");
        error.SetRequestId("request-id");
        return DeleteObjectOutcome(std::move(error));
    };
    EXPECT_CALL(*mock_s3_client, DeleteObject(testing::_)).WillOnce(not_found);
    EXPECT_CALL(*mock_s3_client, DeleteObjects(testing::_))
            .WillOnce([](const DeleteObjectsRequest& request) {
                const auto& objects = request.GetDelete().GetObjects();
                EXPECT_EQ(objects.size(), 1);
                EXPECT_EQ(objects.front().GetKey(), "missing-object");
                return DeleteObjectsOutcome(DeleteObjectsResult {});
            });

    auto response = s3_obj_storage_client->delete_object(
            {.bucket = "dummy-bucket", .key = "missing-object"});
    EXPECT_EQ(response.status.code, ObjStorageStatus::NOT_FOUND);
    EXPECT_EQ(response.http_code, static_cast<int>(Aws::Http::HttpResponseCode::NOT_FOUND));
    EXPECT_EQ(response.request_id, "request-id");

    response =
            s3_obj_storage_client->delete_objects({.bucket = "dummy-bucket"}, {"missing-object"});
    EXPECT_TRUE(response.ok());
}

TEST_F(S3ObjStorageClientMockTest, test_ca_cert) {
    auto path = doris::get_valid_ca_cert_path(doris::split(config::ca_cert_file_paths, ";"));
    LOG(INFO) << "config:" << config::ca_cert_file_paths << " path:" << path;
    ASSERT_FALSE(path.empty());
}
} // namespace doris::io
