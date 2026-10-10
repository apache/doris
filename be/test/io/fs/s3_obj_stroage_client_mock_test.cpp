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

#include <aws/core/AmazonWebServiceResult.h>
#include <aws/core/Aws.h>
#include <aws/core/utils/stream/ResponseStream.h>
#include <aws/core/utils/xml/XmlSerializer.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/Object.h>

#include "cpp/obj-client/obj_storage_client.h"
#include "cpp/obj-client/rate_limited_obj_storage_client.h"
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
    MOCK_METHOD(Aws::S3::Model::HeadObjectOutcome, HeadObject,
                (const Aws::S3::Model::HeadObjectRequest& request), (const, override));
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

TEST_F(S3ObjStorageClientMockTest, HeadPreservesFileMetadata) {
    auto mock = std::make_shared<MockS3Client>();
    auto client = std::make_shared<S3ObjStorageClient>(mock);
    HeadObjectResult result;
    result.SetContentLength(42);
    result.SetContentType("image/png");
    result.SetETag("\"opaque-2\"");
    EXPECT_CALL(*mock, HeadObject(testing::_)).WillOnce(testing::Return(result));
    auto metadata = client->head_object({.bucket = "bucket", .key = "key"});
    ASSERT_TRUE(metadata.resp.ok());
    EXPECT_EQ(metadata.file_size, 42);
    EXPECT_EQ(metadata.content_type, "image/png");
    EXPECT_EQ(metadata.etag, "\"opaque-2\"");
}

TEST_F(S3ObjStorageClientMockTest, HeadMetadataUsesOrdinaryRequestThroughRateLimiter) {
    auto mock = std::make_shared<MockS3Client>();
    size_t requests = 0;
    auto client = std::make_shared<RateLimitedObjStorageClient>(
            std::make_shared<S3ObjStorageClient>(mock),
            std::make_shared<CountingGetRateLimitPolicy>(&requests));
    HeadObjectResult result;
    result.SetContentLength(42);
    result.SetETag("\"opaque-2\"");
    EXPECT_CALL(*mock, HeadObject(testing::_)).WillOnce([&](const HeadObjectRequest& request) {
        EXPECT_FALSE(request.ChecksumModeHasBeenSet());
        return HeadObjectOutcome(result);
    });
    const auto metadata = client->head_object({.bucket = "bucket", .key = "key"});
    ASSERT_TRUE(metadata.resp.ok());
    EXPECT_EQ(metadata.etag, "\"opaque-2\"");
    EXPECT_EQ(requests, 1);
}

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

TEST_F(S3ObjStorageClientMockTest, ListPagePreservesOptionalModificationTimeMilliseconds) {
    auto mock = std::make_shared<MockS3Client>();
    std::shared_ptr<ObjStorageClient> client = std::make_shared<S3ObjStorageClient>(mock);
    auto result = CreatePageResult("", {"dir/a.txt", "dir/no-time.txt"}, false);
    auto objects = result.GetContents();
    objects[0].SetLastModified(Aws::Utils::DateTime(static_cast<int64_t>(1704164645123)));
    result.SetContents(objects);
    EXPECT_CALL(*mock, ListObjectsV2(testing::_))
            .WillOnce(testing::Return(ListObjectsV2Outcome(result)));
    const auto listed = client->list_objects_page({.bucket = "bucket", .prefix = "dir/"}, "");
    ASSERT_TRUE(listed.resp.ok());
    ASSERT_EQ(listed.objects.size(), 2);
    EXPECT_EQ(listed.objects[0].mtime_s, 1704164645);
    EXPECT_EQ(listed.objects[0].modification_time_ms, 1704164645123);
    EXPECT_FALSE(listed.objects[1].modification_time_ms.has_value());
}

TEST_F(S3ObjStorageClientMockTest, ListPagePreservesModificationTimeFromResponseXml) {
    auto mock = std::make_shared<MockS3Client>();
    std::shared_ptr<ObjStorageClient> client = std::make_shared<S3ObjStorageClient>(mock);
    EXPECT_CALL(*mock, ListObjectsV2(testing::_)).WillOnce([](const ListObjectsV2Request& request) {
        Aws::Utils::Stream::ResponseStream response(request.GetResponseStreamFactory());
        response.GetUnderlyingStream() << R"(<ListBucketResult><IsTruncated>false</IsTruncated>
<Contents><Key>dir/milliseconds.txt</Key><Size>1</Size>
<LastModified>2024-01-01T00:00:00.123Z</LastModified></Contents>
<Contents><Key>dir/seconds.txt</Key><Size>2</Size>
<LastModified>2024-01-01T00:00:00Z</LastModified></Contents>
<Contents><Key>dir/missing.txt</Key><Size>0</Size></Contents>
<Contents><Key>dir/tenths.txt</Key><Size>1</Size>
<LastModified>2024-01-01T00:00:00.1Z</LastModified></Contents>
<Contents><Key>dir/hundredths.txt</Key><Size>1</Size>
<LastModified>2024-01-01T00:00:00.12Z</LastModified></Contents>
<Contents><Key>dir/microseconds.txt</Key><Size>1</Size>
<LastModified>2024-01-01T00:00:00.123456Z</LastModified></Contents></ListBucketResult>)";
        auto document =
                Aws::Utils::Xml::XmlDocument::CreateFromXmlStream(response.GetUnderlyingStream());
        EXPECT_TRUE(document.WasParseSuccessful());
        return ListObjectsV2Outcome(
                ListObjectsV2Result(Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>(
                        std::move(document), {})));
    });
    const auto listed = client->list_objects_page({.bucket = "bucket", .prefix = "dir/"}, "");
    ASSERT_TRUE(listed.resp.ok());
    ASSERT_EQ(listed.objects.size(), 6);
    EXPECT_EQ(listed.objects[0].mtime_s, 1704067200);
    EXPECT_EQ(listed.objects[0].modification_time_ms, 1704067200123);
    EXPECT_EQ(listed.objects[1].mtime_s, 1704067200);
    EXPECT_EQ(listed.objects[1].modification_time_ms, 1704067200000);
    EXPECT_FALSE(listed.objects[2].modification_time_ms.has_value());
    EXPECT_EQ(listed.objects[3].modification_time_ms, 1704067200100);
    EXPECT_EQ(listed.objects[4].modification_time_ms, 1704067200120);
    EXPECT_EQ(listed.objects[5].modification_time_ms, 1704067200123);
}

TEST_F(S3ObjStorageClientMockTest, ListPageResponseXmlKeepsOnlyFinalRetry) {
    auto mock = std::make_shared<MockS3Client>();
    std::shared_ptr<ObjStorageClient> client = std::make_shared<S3ObjStorageClient>(mock);
    EXPECT_CALL(*mock, ListObjectsV2(testing::_)).WillOnce([](const ListObjectsV2Request& request) {
        {
            Aws::Utils::Stream::ResponseStream failed_response(request.GetResponseStreamFactory());
            failed_response.GetUnderlyingStream()
                    << R"(<ListBucketResult><Contents><Key>dir/stale-retried-object.txt</Key>
<LastModified>2023-01-01T00:00:00.987Z</LastModified></Contents>
<Contents><Key>dir/another-stale-object.txt</Key></Contents></ListBucketResult>)";
        }
        Aws::Utils::Stream::ResponseStream response(request.GetResponseStreamFactory());
        response.GetUnderlyingStream()
                << R"(<ListBucketResult><Contents><Key>dir/final.txt</Key><Size>1</Size>
<LastModified>2024-01-01T00:00:00.456Z</LastModified></Contents></ListBucketResult>)";
        auto document =
                Aws::Utils::Xml::XmlDocument::CreateFromXmlStream(response.GetUnderlyingStream());
        EXPECT_TRUE(document.WasParseSuccessful());
        return ListObjectsV2Outcome(
                ListObjectsV2Result(Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>(
                        std::move(document), {})));
    });
    const auto listed = client->list_objects_page({.bucket = "bucket", .prefix = "dir/"}, "");
    ASSERT_TRUE(listed.resp.ok());
    ASSERT_EQ(listed.objects.size(), 1);
    EXPECT_EQ(listed.objects[0].key, "dir/final.txt");
    EXPECT_EQ(listed.objects[0].mtime_s, 1704067200);
    EXPECT_EQ(listed.objects[0].modification_time_ms, 1704067200456);
}

TEST_F(S3ObjStorageClientMockTest, ListPageInvalidXmlModificationTimeIsNull) {
    auto mock = std::make_shared<MockS3Client>();
    std::shared_ptr<ObjStorageClient> client = std::make_shared<S3ObjStorageClient>(mock);
    EXPECT_CALL(*mock, ListObjectsV2(testing::_)).WillOnce([](const ListObjectsV2Request& request) {
        Aws::Utils::Stream::ResponseStream response(request.GetResponseStreamFactory());
        response.GetUnderlyingStream()
                << R"(<ListBucketResult><Contents><Key>dir/invalid.txt</Key><Size>1</Size>
<LastModified>invalid-date.123Z</LastModified></Contents></ListBucketResult>)";
        auto document =
                Aws::Utils::Xml::XmlDocument::CreateFromXmlStream(response.GetUnderlyingStream());
        EXPECT_TRUE(document.WasParseSuccessful());
        return ListObjectsV2Outcome(
                ListObjectsV2Result(Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument>(
                        std::move(document), {})));
    });
    const auto listed = client->list_objects_page({.bucket = "bucket", .prefix = "dir/"}, "");
    ASSERT_TRUE(listed.resp.ok());
    ASSERT_EQ(listed.objects.size(), 1);
    EXPECT_FALSE(listed.objects[0].modification_time_ms.has_value());
}

TEST_F(S3ObjStorageClientMockTest, ListPageDelimiterIsOptIn) {
    auto mock = std::make_shared<MockS3Client>();
    std::shared_ptr<ObjStorageClient> client = std::make_shared<S3ObjStorageClient>(mock);
    EXPECT_CALL(*mock, ListObjectsV2(testing::_)).WillOnce([](const ListObjectsV2Request& request) {
        EXPECT_EQ(request.GetDelimiter(), "/");
        EXPECT_EQ(request.GetPrefix(), "dir/");
        EXPECT_EQ(request.GetContinuationToken(), "next");
        return ListObjectsV2Outcome(CreatePageResult("", {"dir/a.txt"}, false));
    });
    const auto direct = client->list_objects_page(
            {.bucket = "bucket", .prefix = "dir/", .delimiter = "/"}, "next");
    ASSERT_TRUE(direct.resp.ok());
    ASSERT_EQ(direct.objects.size(), 1);
    EXPECT_EQ(direct.objects[0].key, "dir/a.txt");
    EXPECT_CALL(*mock, ListObjectsV2(testing::_)).WillOnce([](const ListObjectsV2Request& request) {
        EXPECT_FALSE(request.DelimiterHasBeenSet());
        return ListObjectsV2Outcome(CreatePageResult("", {"dir/deep/a.txt"}, false));
    });
    const auto recursive = client->list_objects_page({.bucket = "bucket", .prefix = "dir/"}, "");
    ASSERT_TRUE(recursive.resp.ok());
    ASSERT_EQ(recursive.objects.size(), 1);
    EXPECT_EQ(recursive.objects[0].key, "dir/deep/a.txt");
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
