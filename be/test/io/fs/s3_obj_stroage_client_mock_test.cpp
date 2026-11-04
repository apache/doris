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
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectResult.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/DeleteObjectsResult.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/Object.h>

#include "gmock/gmock.h"
#include "io/fs/rate_limited_obj_storage_client.h"
#include "io/fs/s3_obj_storage_client.h"
#include "util/s3_rate_limiter_manager.h"
#include "util/s3_util.h"
#include "util/string_util.h"

using namespace Aws::S3::Model;

namespace doris::io {
namespace {

struct RateLimiterConfigGuard {
    bool enable = config::enable_s3_rate_limiter;

    ~RateLimiterConfigGuard() {
        config::enable_s3_rate_limiter = enable;
        S3RateLimiterManager::instance().refresh();
    }
};

} // namespace

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
    auto client = std::make_shared<RateLimitedObjStorageClient>(
            std::make_shared<S3ObjStorageClient>(mock));
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
}

TEST_F(S3ObjStorageClientMockTest, list_objects_compatibility) {
    // If storage only supports ListObjectsV1, s3_obj_storage_client.list_objects
    // should return an error.
    auto mock_s3_client = std::make_shared<MockS3Client>();
    S3ObjStorageClient s3_obj_storage_client(mock_s3_client);

    std::vector<io::FileInfo> files;

    ListObjectsV2Result result;
    result.SetIsTruncated(true);
    EXPECT_CALL(*mock_s3_client, ListObjectsV2(testing::_))
            .WillOnce(testing::Return(ListObjectsV2Outcome(result)));

    auto response = s3_obj_storage_client.list_objects(
            {.bucket = "dummy-bucket", .prefix = "S3ObjStorageClientMockTest/list_objects_test"},
            &files);

    EXPECT_EQ(response.status.code, ErrorCode::INTERNAL_ERROR);
    files.clear();
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

TEST_F(S3ObjStorageClientMockTest, ListPagePreservesEtagsWithoutHeadRequests) {
    auto mock = std::make_shared<testing::StrictMock<MockS3Client>>();
    S3ObjStorageClient client(mock);
    EXPECT_CALL(*mock, ListObjectsV2(testing::_)).WillOnce([](const auto&) {
        ListObjectsV2Result result;
        for (const auto& etag : {std::optional<std::string>("\"opaque-2\""),
                                 std::optional<std::string>(""), std::optional<std::string>()}) {
            Object object;
            object.SetKey("dir/" + std::to_string(result.GetContents().size()));
            object.SetSize(7);
            if (etag) {
                object.SetETag(*etag);
            }
            result.AddContents(std::move(object));
        }
        return ListObjectsV2Outcome(result);
    });
    const auto result = client.list_objects_page({.bucket = "bucket", .prefix = "dir/"}, "");
    ASSERT_TRUE(result.resp.ok());
    ASSERT_EQ(result.objects.size(), 3);
    EXPECT_EQ(result.objects[0].etag, "\"opaque-2\"");
    EXPECT_FALSE(result.objects[1].etag.has_value());
    EXPECT_FALSE(result.objects[2].etag.has_value());
}

TEST_F(S3ObjStorageClientMockTest, list_objects_pagination_charges_one_get_qps) {
    RateLimiterConfigGuard guard;
    config::enable_s3_rate_limiter = true;
    auto& manager = S3RateLimiterManager::instance();
    manager.qps_limiter(S3RateLimitType::GET)->reset(0, 0, 1);
    manager.bytes_limiter(S3RateLimitType::GET)->reset(0, 0, 0);

    auto mock_s3_client = std::make_shared<MockS3Client>();
    auto s3_obj_storage_client = std::make_shared<S3ObjStorageClient>(mock_s3_client);
    RateLimitedObjStorageClient rate_limited_client(s3_obj_storage_client);

    std::vector<std::vector<std::string>> pages = {
            {"key1", "key2"}, // page1
            {"key3", "key4"}, // page2
            {"key5"}          // page3
    };

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

    std::vector<io::FileInfo> files;
    const ObjectStoragePathOptions opts {
            .bucket = "dummy-bucket",
            .prefix = "S3ObjStorageClientMockTest/list_objects_with_pagination"};
    auto response = rate_limited_client.list_objects(opts, &files);

    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(files.size(), 5);

    // The first logical list used one GET token despite issuing three provider requests.
    // A second logical list is rejected before it reaches the provider.
    response = rate_limited_client.list_objects(opts, &files);
    EXPECT_EQ(ErrorCode::EXCEEDED_LIMIT, response.status.code);
    EXPECT_EQ(0, response.http_code);
}

TEST_F(S3ObjStorageClientMockTest, test_ca_cert) {
    auto path = doris::get_valid_ca_cert_path(doris::split(config::ca_cert_file_paths, ";"));
    LOG(INFO) << "config:" << config::ca_cert_file_paths << " path:" << path;
    ASSERT_FALSE(path.empty());
}
} // namespace doris::io
