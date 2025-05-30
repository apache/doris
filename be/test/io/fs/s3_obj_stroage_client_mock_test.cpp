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

#include "gmock/gmock.h"
#include "io/fs/s3_obj_storage_client.h"
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

TEST_F(S3ObjStorageClientMockTest, list_objects_with_pagination) {
    auto mock_s3_client = std::make_shared<MockS3Client>();
    S3ObjStorageClient s3_obj_storage_client(mock_s3_client);

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
    auto response = s3_obj_storage_client.list_objects(
            {.bucket = "dummy-bucket",
             .prefix = "S3ObjStorageClientMockTest/list_objects_with_pagination"},
            &files);

    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(files.size(), 5);
    files.clear();
}

TEST_F(S3ObjStorageClientMockTest, test_ca_cert) {
    auto path = doris::get_valid_ca_cert_path(doris::split(config::ca_cert_file_paths, ";"));
    LOG(INFO) << "config:" << config::ca_cert_file_paths << " path:" << path;
    ASSERT_FALSE(path.empty());
}
} // namespace doris::io