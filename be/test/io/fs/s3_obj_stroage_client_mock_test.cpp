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

#include "cpp/client/obj_storage_client.h"
#include "cpp/client/s3_obj_storage_backend.h"
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
};

class CountingGetRateLimitPolicy final : public ObjStorageRateLimitPolicy {
public:
    explicit CountingGetRateLimitPolicy(size_t* request_count) : request_count_(request_count) {}

    ObjStorageRateLimitToken acquire(ObjStorageRequestType type, size_t) const override {
        EXPECT_EQ(type, ObjStorageRequestType::GET);
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
    // If storage only supports ListObjectsV1, s3_obj_storage_backend.list_objects
    // should return an error.
    auto mock_s3_client = std::make_shared<MockS3Client>();
    S3ObjStorageBackend s3_obj_storage_backend(mock_s3_client);

    std::vector<io::FileInfo> files;

    ListObjectsV2Result result;
    result.SetIsTruncated(true);
    EXPECT_CALL(*mock_s3_client, ListObjectsV2(testing::_))
            .WillOnce(testing::Return(ListObjectsV2Outcome(result)));

    auto page = s3_obj_storage_backend.list_objects(
            {.bucket = "dummy-bucket", .key = "S3ObjStorageClientMockTest/list_objects_test"}, {});

    EXPECT_TRUE(page.objects.empty());
    EXPECT_EQ(page.resp.status.code, ErrorCode::INTERNAL_ERROR);
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

TEST_F(S3ObjStorageClientMockTest, list_objects_with_pagination) {
    auto mock_s3_client = std::make_shared<MockS3Client>();
    size_t get_request_count = 0;
    auto backend = std::make_shared<S3ObjStorageBackend>(mock_s3_client);
    ObjStorageClient obj_storage_client(
            std::move(backend), std::make_shared<CountingGetRateLimitPolicy>(&get_request_count));
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

    std::vector<io::FileInfo> files;
    std::string continuation_token;
    bool has_more = true;
    while (has_more) {
        auto page = obj_storage_client.list_objects(
                {.bucket = "dummy-bucket",
                 .key = "S3ObjStorageClientMockTest/list_objects_with_pagination"},
                continuation_token);
        EXPECT_EQ(page.resp.status.code, ErrorCode::OK);
        for (const auto& object : page.objects) {
            files.push_back(
                    {.file_name = object.file_path, .file_size = object.size, .is_file = true});
        }
        continuation_token = std::move(page.continuation_token);
        has_more = page.has_more;
    }

    EXPECT_EQ(files.size(), 5);
    EXPECT_EQ(get_request_count, pages.size());
    files.clear();
}

TEST_F(S3ObjStorageClientMockTest, test_ca_cert) {
    auto path = doris::get_valid_ca_cert_path(doris::split(config::ca_cert_file_paths, ";"));
    LOG(INFO) << "config:" << config::ca_cert_file_paths << " path:" << path;
    ASSERT_FALSE(path.empty());
}
} // namespace doris::io
