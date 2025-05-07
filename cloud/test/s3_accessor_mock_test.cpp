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

} // namespace doris::cloud
