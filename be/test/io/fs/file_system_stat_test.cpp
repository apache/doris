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

#include <cstdlib>
#include <string>
#include <string_view>

#include "cpp/obj-client/s3_obj_storage_client.h"
#include "cpp/sync_point.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/fs/file_system.h"
#include "io/fs/hdfs_file_system.h"
#include "io/fs/local_file_system.h"
#include "io/fs/s3_file_system.h"
#include "io/hdfs_util.h"
#include "testutil/mock/obj_storage_client_test_stub.h"
#include "util/defer_op.h"

namespace doris::io {

class FileStatS3Client : public Aws::S3::S3Client {
public:
    MOCK_METHOD(Aws::S3::Model::HeadObjectOutcome, HeadObject,
                (const Aws::S3::Model::HeadObjectRequest&), (const, override));
};

class FileStatStorageClient : public ObjStorageClientTestStub {
public:
    MOCK_METHOD(ObjStorageUploadResult, create_multipart_upload, (const ObjStoragePath&),
                (override));
    MOCK_METHOD(ObjStorageResponse, put_object, (const ObjStoragePath&, std::string_view),
                (override));
    MOCK_METHOD(ObjStorageUploadResult, upload_part,
                (const ObjStoragePath&, const std::string&, std::string_view, int), (override));
    MOCK_METHOD(ObjStorageResponse, complete_multipart_upload,
                (const ObjStoragePath&, const std::string&,
                 const std::vector<ObjStorageCompletedPart>&),
                (override));
    MOCK_METHOD(ObjStorageHeadResult, head_object, (const ObjStoragePath&), (override));
    MOCK_METHOD(ObjStorageResponse, get_object,
                (const ObjStoragePath&, void*, size_t, size_t, size_t*), (override));
    MOCK_METHOD(ObjStorageResponse, delete_objects,
                (const ObjStoragePath&, std::vector<std::string>), (override));
    MOCK_METHOD(ObjStorageResponse, delete_object, (const ObjStoragePath&), (override));
    MOCK_METHOD(std::string, generate_presigned_url, (const ObjStoragePath&, int64_t), (override));
    MOCK_METHOD(ObjStorageListPageResult, list_objects_page,
                (const ObjStoragePath&, std::string_view), (override));
};

class FileSystemStatTest : public testing::Test {
protected:
    static void SetUpTestSuite() { S3ClientFactory::instance(); }
};

TEST_F(FileSystemStatTest, S3PreservesProviderMetadataAndOrdinaryPathHandling) {
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
                // Same parser as file_size: it strips the query, without percent decoding
                // or resolving dot segments. A full URI does not receive the FS prefix.
                EXPECT_EQ(request.GetKey(), "a%2Fb/../c");
                return Aws::S3::Model::HeadObjectOutcome(result);
            });
    FileStat metadata;
    FileStatContext context;
    ASSERT_TRUE(fs.stat("s3://resource-bucket/a%2Fb/../c?versionId=AbC", &metadata, &context).ok());
    EXPECT_EQ(metadata.size, 42);
    EXPECT_EQ(metadata.content_type, "Image/PNG; custom=Value");
    EXPECT_EQ(metadata.checksum, "ETAG:opaque-2");
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
    ObjStorageHeadResult denied;
    denied.resp.status = {ObjStorageStatus::PERMISSION_DENIED, "denied"};
    EXPECT_CALL(*client, head_object(testing::_)).Times(1).WillOnce(testing::Return(denied));
    FileStatContext context;
    FileStat metadata {.size = 99};
    EXPECT_EQ(fs.stat("key", &metadata, &context).code(), ObjStorageStatus::PERMISSION_DENIED);
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
        EXPECT_CALL(*client, head_object(testing::_)).WillOnce([&](const ObjStoragePath&) {
            cancelled = true;
            ObjStorageHeadResult result {.file_size = 42};
            if (!success) {
                result.resp.status = {ObjStorageStatus::INTERNAL_ERROR, "transport error"};
            }
            return result;
        });
        EXPECT_EQ(fs.stat("key", &metadata, &context).code(), ErrorCode::CANCELLED);
        EXPECT_EQ(metadata.size, 99);
    }
}

TEST_F(FileSystemStatTest, HdfsReportsMetadataForFilesAndDirectories) {
    THdfsParams params;
    HdfsFileSystem fs(params, "hdfs://namenode:8020", "stat-test", "");
    fs._fs_handler = std::make_shared<HdfsHandler>(nullptr, false, "", "", "hdfs://namenode:8020");
    auto* point = SyncPoint::get_instance();
    const bool was_enabled = point->get_enable();
    point->enable_processing();
    Defer restore {[&] {
        if (!was_enabled) point->disable_processing();
    }};
    for (const auto kind : {kObjectKindFile, kObjectKindDirectory}) {
        SyncPoint::CallbackGuard guard;
        point->set_call_back(
                "HdfsFileSystem::stat::hdfsGetPathInfo",
                [&](auto&& args) {
                    EXPECT_EQ(try_any_cast<std::string>(args[0]), "/a%20b?versionId=AbC");
                    auto* info = static_cast<hdfsFileInfo*>(std::calloc(1, sizeof(hdfsFileInfo)));
                    ASSERT_NE(info, nullptr);
                    info->mKind = kind;
                    info->mSize = 123;
                    auto* ret = try_any_cast_ret<hdfsFileInfo*>(args);
                    ret->first = info;
                    ret->second = true;
                },
                &guard);
        FileStat metadata {.content_type = "previous", .checksum = "ETAG:previous"};
        ASSERT_TRUE(fs.stat("hdfs://namenode:8020/a%20b?versionId=AbC", &metadata).ok());
        EXPECT_EQ(metadata.size, 123);
        EXPECT_FALSE(metadata.content_type.has_value());
        EXPECT_FALSE(metadata.checksum.has_value());
    }
}

TEST_F(FileSystemStatTest, HdfsWithoutHandlerPreservesOutput) {
    THdfsParams params;
    HdfsFileSystem fs(params, "hdfs://namenode:8020", "stat-test", "");
    FileStat metadata {.size = 99};
    EXPECT_EQ(fs.stat("/missing", &metadata).code(), ErrorCode::IO_ERROR);
    EXPECT_EQ(metadata.size, 99);
}

TEST_F(FileSystemStatTest, HdfsCancellationBeforeAndAfterStatPreservesOutput) {
    THdfsParams params;
    HdfsFileSystem fs(params, "hdfs://namenode:8020", "stat-test", "");
    fs._fs_handler = std::make_shared<HdfsHandler>(nullptr, false, "", "", "hdfs://namenode:8020");
    auto* point = SyncPoint::get_instance();
    const bool was_enabled = point->get_enable();
    point->enable_processing();
    Defer restore {[&] {
        if (!was_enabled) point->disable_processing();
    }};
    SyncPoint::CallbackGuard guard;
    bool cancelled = true;
    bool success = true;
    size_t requests = 0;
    point->set_call_back(
            "HdfsFileSystem::stat::hdfsGetPathInfo",
            [&](auto&& args) {
                ++requests;
                cancelled = true;
                auto* ret = try_any_cast_ret<hdfsFileInfo*>(args);
                if (success) {
                    auto* info = static_cast<hdfsFileInfo*>(std::calloc(1, sizeof(hdfsFileInfo)));
                    ASSERT_NE(info, nullptr);
                    info->mKind = kObjectKindFile;
                    info->mSize = 42;
                    ret->first = info;
                } else {
                    errno = EIO;
                    ret->first = nullptr;
                }
                ret->second = true;
            },
            &guard);
    FileStatContext context {.is_cancelled = [&] { return cancelled; }};
    FileStat metadata {.size = 99};
    EXPECT_EQ(fs.stat("/file", &metadata, &context).code(), ErrorCode::CANCELLED);
    EXPECT_EQ(requests, 0);
    for (const bool call_succeeds : {true, false}) {
        success = call_succeeds;
        cancelled = false;
        EXPECT_EQ(fs.stat("/file", &metadata, &context).code(), ErrorCode::CANCELLED);
        EXPECT_EQ(metadata.size, 99);
    }
    EXPECT_EQ(requests, 2);
}

TEST_F(FileSystemStatTest, UnsupportedBackendDoesNotPretendToHaveMetadata) {
    FileStat metadata {.size = 99};
    EXPECT_FALSE(global_local_filesystem()->stat("/unused", &metadata).ok());
    EXPECT_EQ(metadata.size, 99);
}

} // namespace doris::io
