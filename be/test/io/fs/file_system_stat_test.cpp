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

#include <aws/s3/S3Client.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "io/fs/s3_file_system.h"
#include "io/fs/s3_obj_storage_client.h"

namespace doris::io {

class FileStatS3Client : public Aws::S3::S3Client {
public:
    MOCK_METHOD(Aws::S3::Model::HeadObjectOutcome, HeadObject,
                (const Aws::S3::Model::HeadObjectRequest&), (const, override));
};

class FileStatStorageClient : public ObjStorageClient {
public:
    MOCK_METHOD(ObjectStorageUploadResponse, create_multipart_upload,
                (const ObjectStoragePathOptions&), (override));
    MOCK_METHOD(ObjectStorageResponse, put_object,
                (const ObjectStoragePathOptions&, std::string_view), (override));
    MOCK_METHOD(ObjectStorageUploadResponse, upload_part,
                (const ObjectStoragePathOptions&, std::string_view, int), (override));
    MOCK_METHOD(ObjectStorageResponse, complete_multipart_upload,
                (const ObjectStoragePathOptions&, const std::vector<ObjectCompleteMultiPart>&),
                (override));
    MOCK_METHOD(ObjectStorageHeadResponse, head_object, (const ObjectStoragePathOptions&),
                (override));
    MOCK_METHOD(ObjectStorageResponse, get_object,
                (const ObjectStoragePathOptions&, void*, size_t, size_t, size_t*), (override));
    MOCK_METHOD(ObjectStorageResponse, delete_objects,
                (const ObjectStoragePathOptions&, std::vector<std::string>), (override));
    MOCK_METHOD(ObjectStorageResponse, delete_object, (const ObjectStoragePathOptions&),
                (override));
    MOCK_METHOD(std::string, generate_presigned_url,
                (const ObjectStoragePathOptions&, int64_t, const S3ClientConf&), (override));
    MOCK_METHOD(ObjectStorageResponse, list_objects,
                (const ObjectStoragePathOptions&, std::vector<FileInfo>*), (override));
    MOCK_METHOD(ObjectStorageResponse, delete_objects_recursively,
                (const ObjectStoragePathOptions&), (override));
    MOCK_METHOD(ObjectStorageListPageResponse, list_objects_page,
                (const ObjectStoragePathOptions&, std::string_view), (override));
};

class FileSystemStatTest : public testing::Test {
protected:
    static void SetUpTestSuite() { S3ClientFactory::instance(); }
};

TEST_F(FileSystemStatTest, S3PreservesProviderMetadataAndUriKey) {
    const auto client = std::make_shared<FileStatS3Client>();
    S3FileSystem fs(S3Conf {.bucket = "resource-bucket", .prefix = "prefix", .client_conf = {}},
                    "stat-test");
    fs._client->_client = std::make_shared<S3ObjStorageClient>(client);
    Aws::S3::Model::HeadObjectResult result;
    result.SetContentLength(42);
    result.SetContentType("Image/PNG; custom=Value");
    result.SetETag("\"opaque-2\"");
    // Stock HEAD may return checksum headers; basic FILE stat keeps the ETag opaque.
    result.SetChecksumSHA256("not-a-file-digest");
    EXPECT_CALL(*client, HeadObject(testing::_))
            .WillOnce([&](const Aws::S3::Model::HeadObjectRequest& request) {
                EXPECT_FALSE(request.ChecksumModeHasBeenSet());
                EXPECT_EQ(request.GetBucket(), "resource-bucket");
                EXPECT_EQ(request.GetKey(), "a%2Fb/../c?versionId=AbC");
                return Aws::S3::Model::HeadObjectOutcome(result);
            });
    FileStat metadata;
    FileStatContext context;
    ASSERT_TRUE(fs.stat("s3://resource-bucket/a%2Fb/../c?versionId=AbC", &metadata, &context).ok());
    EXPECT_EQ(metadata.size, 42);
    EXPECT_EQ(metadata.content_type, "Image/PNG; custom=Value");
    EXPECT_EQ(metadata.checksum, "ETAG:opaque-2");
}

TEST_F(FileSystemStatTest, S3StatAndFileSizeUseTheSameRawKeys) {
    const auto client = std::make_shared<FileStatS3Client>();
    S3FileSystem fs(S3Conf {.bucket = "bucket", .prefix = "", .client_conf = {}}, "stat-test");
    fs._client->_client = std::make_shared<S3ObjStorageClient>(client);
    for (const std::string key : {"dir/a b.txt", "dir/a+b.txt", "dir/中文.txt", "dir/a%20b.txt",
                                  "dir/a%252Fb.txt", "dir/invalid%zz"}) {
        EXPECT_CALL(*client, HeadObject(testing::_))
                .Times(2)
                .WillRepeatedly([&](const Aws::S3::Model::HeadObjectRequest& request) {
                    EXPECT_EQ(request.GetKey(), key);
                    return Aws::S3::Model::HeadObjectOutcome(Aws::S3::Model::HeadObjectResult());
                });
        FileStat metadata;
        ASSERT_TRUE(fs.stat("s3://bucket/" + key, &metadata).ok());
        int64_t size = 0;
        ASSERT_TRUE(fs.file_size("s3://bucket/" + key, &size).ok());
    }
}

TEST_F(FileSystemStatTest, S3RelativePathAndAbsentMetadataReplacePriorResult) {
    const auto client = std::make_shared<FileStatS3Client>();
    S3FileSystem fs(S3Conf {.bucket = "bucket", .prefix = "prefix", .client_conf = {}},
                    "stat-test");
    fs._client->_client = std::make_shared<S3ObjStorageClient>(client);
    Aws::S3::Model::HeadObjectResult result;
    result.SetContentLength(0);
    EXPECT_CALL(*client, HeadObject(testing::_))
            .WillOnce([&](const Aws::S3::Model::HeadObjectRequest& request) {
                EXPECT_EQ(request.GetKey(), "prefix/empty");
                return Aws::S3::Model::HeadObjectOutcome(result);
            });
    FileStat metadata {.size = 42, .content_type = "image/png", .checksum = "ETAG:previous"};
    ASSERT_TRUE(fs.stat("empty", &metadata).ok());
    EXPECT_EQ(metadata.size, 0);
    EXPECT_FALSE(metadata.content_type.has_value());
    EXPECT_FALSE(metadata.checksum.has_value());
}

TEST_F(FileSystemStatTest, S3FailurePreservesOutputAndStatus) {
    const auto client = std::make_shared<FileStatS3Client>();
    S3FileSystem fs(S3Conf {.bucket = "bucket", .prefix = "", .client_conf = {}}, "stat-test");
    fs._client->_client = std::make_shared<S3ObjStorageClient>(client);
    Aws::S3::S3Error error(
            Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::NO_SUCH_KEY, false));
    error.SetResponseCode(Aws::Http::HttpResponseCode::NOT_FOUND);
    EXPECT_CALL(*client, HeadObject(testing::_)).WillOnce(testing::Return(error));
    FileStat metadata {.size = 99};
    const auto status = fs.stat("s3://bucket/missing", &metadata);
    EXPECT_EQ(status.code(), ErrorCode::NOT_FOUND);
    EXPECT_EQ(metadata.size, 99);
}

TEST_F(FileSystemStatTest, S3OrdinaryHeadFailureDoesNotRetry) {
    auto client = std::make_shared<FileStatStorageClient>();
    S3FileSystem fs(S3Conf {.bucket = "bucket", .prefix = "", .client_conf = {}}, "stat-test");
    fs._client->_client = client;
    ObjectStorageHeadResponse denied;
    denied.resp.status = {ErrorCode::INTERNAL_ERROR, "denied"};
    EXPECT_CALL(*client, head_object(testing::_)).Times(1).WillOnce(testing::Return(denied));
    FileStatContext context;
    FileStat metadata {.size = 99};
    EXPECT_EQ(fs.stat("key", &metadata, &context).code(), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(metadata.size, 99);
}

TEST_F(FileSystemStatTest, S3CancellationBeforeAndAfterHeadPreservesOutput) {
    auto client = std::make_shared<FileStatStorageClient>();
    S3FileSystem fs(S3Conf {.bucket = "bucket", .prefix = "", .client_conf = {}}, "stat-test");
    fs._client->_client = client;
    bool cancelled = true;
    FileStatContext context {.is_cancelled = [&] { return cancelled; }};
    FileStat metadata {.size = 99};
    EXPECT_CALL(*client, head_object(testing::_)).Times(0);
    EXPECT_EQ(fs.stat("key", &metadata, &context).code(), ErrorCode::CANCELLED);
    EXPECT_EQ(metadata.size, 99);
    ASSERT_TRUE(testing::Mock::VerifyAndClearExpectations(client.get()));

    for (const bool success : {true, false}) {
        cancelled = false;
        EXPECT_CALL(*client, head_object(testing::_))
                .WillOnce([&](const ObjectStoragePathOptions&) {
                    cancelled = true;
                    ObjectStorageHeadResponse result {.file_size = 42};
                    if (!success) {
                        result.resp.status = {ErrorCode::INTERNAL_ERROR, "transport error"};
                    }
                    return result;
                });
        EXPECT_EQ(fs.stat("key", &metadata, &context).code(), ErrorCode::CANCELLED);
        EXPECT_EQ(metadata.size, 99);
    }
}

TEST_F(FileSystemStatTest, UnsupportedBackendDoesNotPretendToHaveMetadata) {
    FileStat metadata {.size = 99};
    EXPECT_FALSE(global_local_filesystem()->stat("/unused", &metadata).ok());
    EXPECT_EQ(metadata.size, 99);
}

} // namespace doris::io
