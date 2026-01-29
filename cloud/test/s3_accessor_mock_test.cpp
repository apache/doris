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
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/config.h"
#include "common/logging.h"
#include "cpp/sync_point.h"
#include "recycler/s3_obj_client.h"

using namespace doris;
using namespace Aws::S3::Model;

int main(int argc, char** argv) {
    const std::string conf_file = "doris_cloud.conf";
    if (!cloud::config::init(conf_file.c_str(), true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }

    if (!cloud::init_glog("s3_accessor_mock_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace doris::cloud {

class S3AccessorMockTest : public testing::Test {
    static void SetUpTestSuite() { Aws::InitAPI(S3AccessorMockTest::options); };
    static void TearDownTestSuite() { Aws::ShutdownAPI(options); };

private:
    static Aws::SDKOptions options;
};

Aws::SDKOptions S3AccessorMockTest::options {};
class MockS3Client : public Aws::S3::S3Client {
public:
    MockS3Client() {};

    MOCK_METHOD(Aws::S3::Model::ListObjectsV2Outcome, ListObjectsV2,
                (const Aws::S3::Model::ListObjectsV2Request& request), (const, override));
};

TEST_F(S3AccessorMockTest, list_objects_compatibility) {
    // If storage only supports ListObjectsV1, s3_obj_storage_client.list_objects
    // should return an error.
    auto mock_s3_client = std::make_shared<MockS3Client>();
    S3ObjClient s3_obj_client(mock_s3_client, "dummy-endpoint");

    ListObjectsV2Result result;
    result.SetIsTruncated(true);
    EXPECT_CALL(*mock_s3_client, ListObjectsV2(testing::_))
            .WillOnce(testing::Return(ListObjectsV2Outcome(result)));

    auto response = s3_obj_client.list_objects(
            {.bucket = "dummy-bucket", .key = "S3AccessorMockTest/list_objects_compatibility"});

    EXPECT_FALSE(response->has_next());
    EXPECT_FALSE(response->is_valid());
}

TEST_F(S3AccessorMockTest, list_objects_empty_page_with_more_results) {
    // Test the scenario where S3 returns IsTruncated=true but Contents is empty.
    // This can happen due to concurrent deletion or eventual consistency.
    // The iterator should continue to the next page instead of failing.
    auto mock_s3_client = std::make_shared<MockS3Client>();
    S3ObjClient s3_obj_client(mock_s3_client, "dummy-endpoint");

    // First call: returns empty contents but has more pages
    ListObjectsV2Result result1;
    result1.SetIsTruncated(true);
    result1.SetNextContinuationToken("token-page2");
    // Contents is empty (simulating concurrent deletion)

    // Second call: returns actual objects
    ListObjectsV2Result result2;
    result2.SetIsTruncated(false);
    Object obj1;
    obj1.SetKey("S3AccessorMockTest/empty_page_test/file1.txt");
    obj1.SetSize(100);
    result2.AddContents(obj1);

    EXPECT_CALL(*mock_s3_client, ListObjectsV2(testing::_))
            .WillOnce(testing::Return(ListObjectsV2Outcome(result1)))
            .WillOnce(testing::Return(ListObjectsV2Outcome(result2)));

    auto response = s3_obj_client.list_objects(
            {.bucket = "dummy-bucket", .key = "S3AccessorMockTest/empty_page_test/"});

    // Should successfully skip the empty page and get the next page
    EXPECT_TRUE(response->is_valid());
    EXPECT_TRUE(response->has_next());

    auto obj = response->next();
    EXPECT_TRUE(obj.has_value());
    EXPECT_EQ(obj->key, "S3AccessorMockTest/empty_page_test/file1.txt");
    EXPECT_EQ(obj->size, 100);

    EXPECT_FALSE(response->has_next());
}

TEST_F(S3AccessorMockTest, list_objects_multiple_empty_pages) {
    // Test multiple consecutive empty pages (extreme case of concurrent deletion)
    auto mock_s3_client = std::make_shared<MockS3Client>();
    S3ObjClient s3_obj_client(mock_s3_client, "dummy-endpoint");

    // First call: empty page with more results
    ListObjectsV2Result result1;
    result1.SetIsTruncated(true);
    result1.SetNextContinuationToken("token-page2");

    // Second call: another empty page with more results
    ListObjectsV2Result result2;
    result2.SetIsTruncated(true);
    result2.SetNextContinuationToken("token-page3");

    // Third call: finally returns objects
    ListObjectsV2Result result3;
    result3.SetIsTruncated(false);
    Object obj1;
    obj1.SetKey("S3AccessorMockTest/multi_empty_test/file1.txt");
    obj1.SetSize(200);
    result3.AddContents(obj1);

    EXPECT_CALL(*mock_s3_client, ListObjectsV2(testing::_))
            .WillOnce(testing::Return(ListObjectsV2Outcome(result1)))
            .WillOnce(testing::Return(ListObjectsV2Outcome(result2)))
            .WillOnce(testing::Return(ListObjectsV2Outcome(result3)));

    auto response = s3_obj_client.list_objects(
            {.bucket = "dummy-bucket", .key = "S3AccessorMockTest/multi_empty_test/"});

    // Should successfully skip multiple empty pages
    EXPECT_TRUE(response->is_valid());
    EXPECT_TRUE(response->has_next());

    auto obj = response->next();
    EXPECT_TRUE(obj.has_value());
    EXPECT_EQ(obj->key, "S3AccessorMockTest/multi_empty_test/file1.txt");
    EXPECT_EQ(obj->size, 200);

    EXPECT_FALSE(response->has_next());
}

TEST_F(S3AccessorMockTest, list_objects_all_pages_empty) {
    // Test the case where all pages are empty (all objects deleted concurrently)
    auto mock_s3_client = std::make_shared<MockS3Client>();
    S3ObjClient s3_obj_client(mock_s3_client, "dummy-endpoint");

    // First call: empty page with more results
    ListObjectsV2Result result1;
    result1.SetIsTruncated(true);
    result1.SetNextContinuationToken("token-page2");

    // Second call: last page, also empty
    ListObjectsV2Result result2;
    result2.SetIsTruncated(false);

    EXPECT_CALL(*mock_s3_client, ListObjectsV2(testing::_))
            .WillOnce(testing::Return(ListObjectsV2Outcome(result1)))
            .WillOnce(testing::Return(ListObjectsV2Outcome(result2)));

    auto response = s3_obj_client.list_objects(
            {.bucket = "dummy-bucket", .key = "S3AccessorMockTest/all_empty_test/"});

    // Should be valid but have no objects
    EXPECT_TRUE(response->is_valid());
    EXPECT_FALSE(response->has_next());

    auto obj = response->next();
    EXPECT_FALSE(obj.has_value());
}

} // namespace doris::cloud
