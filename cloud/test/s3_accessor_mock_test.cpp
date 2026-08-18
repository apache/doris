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
#include <aws/core/auth/GeneralHTTPCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/Object.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdio>
#include <memory>
#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "cpp/aws_common.h"
#include "cpp/container_credentials_test_util.h"
#include "cpp/obj-client/s3_obj_storage_client.h"
#include "cpp/sync_point.h"
#include "recycler/s3_accessor.h"

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
    auto s3_obj_client = std::make_shared<S3ObjStorageClient>(
            mock_s3_client, ObjStorageEndpointInfo {.endpoint = "dummy-endpoint"});

    ListObjectsV2Result result;
    result.SetIsTruncated(true);
    EXPECT_CALL(*mock_s3_client, ListObjectsV2(testing::_))
            .WillOnce(testing::Return(ListObjectsV2Outcome(result)));

    std::vector<ObjectMeta> objects;
    auto response = s3_obj_client->list_objects(
            {.bucket = "dummy-bucket", .key = "S3AccessorMockTest/list_objects_compatibility"},
            &objects);

    EXPECT_FALSE(response.ok());
    EXPECT_TRUE(objects.empty());
}

namespace {

class ProviderProbe : public S3Accessor {
public:
    ProviderProbe() : S3Accessor(S3Conf {}) {}

    // The recycler builds its provider through create_aws_credentials_provider(), so the probe goes
    // through the same call rather than reaching past it. An S3Conf with no ak/sk and no role_arn is
    // what a vault configured for container credentials looks like, and it makes the factory return
    // the CONTAINER base provider unwrapped.
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> container_provider() {
        S3Conf conf;
        conf.cred_provider_type = CredProviderType::Container;
        return create_aws_credentials_provider(conf).provider;
    }
};

} // namespace

// The recycler reaches CredProviderType::Container through a storage vault's persisted
// cred_provider_type, and on EKS the pod's credentials live behind
// AWS_CONTAINER_CREDENTIALS_FULL_URI with the token in a kubelet-rotated file. Reading only
// AWS_CONTAINER_CREDENTIALS_RELATIVE_URI leaves this provider with no endpoint, so recycling silently
// reclaims nothing.
TEST_F(S3AccessorMockTest, container_provider_uses_pod_identity_full_uri_and_token_file) {
    ContainerCredentialsEnvGuard env;
    const std::string token_path = env.token_file_path("cloud_pod_identity");
    env.write_token_file(token_path, "token-one");
    env.set_pod_identity("http://127.0.0.1:65000/creds", token_path);

    ProviderProbe probe;
    EXPECT_NE(
            as_valid_http_provider(probe.container_provider()),
            nullptr)
            << "CONTAINER did not yield a usable container credentials provider for "
               "AWS_CONTAINER_CREDENTIALS_FULL_URI";

    ASSERT_EQ(std::remove(token_path.c_str()), 0);
    EXPECT_EQ(
            as_valid_http_provider(probe.container_provider()),
            nullptr)
            << "CONTAINER ignored AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE: the provider stayed "
               "valid "
               "with the token file removed, so the path was never forwarded";
}

// ECS is the other half of the same provider mode: forwarding the full URI must not cost the
// relative one, which the provider resolves against the ECS agent's own address.
TEST_F(S3AccessorMockTest, container_provider_still_honours_ecs_relative_uri) {
    ContainerCredentialsEnvGuard env;
    env.set_ecs_task_role("/v2/credentials/mock");

    ProviderProbe probe;
    EXPECT_NE(
            as_valid_http_provider(probe.container_provider()),
            nullptr)
            << "CONTAINER did not yield a usable container credentials provider for "
               "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";
}

// Pins the discriminator the two tests above rely on: with neither URI exported the provider really
// is unusable, so their assertions cannot be passing vacuously.
TEST_F(S3AccessorMockTest, container_provider_is_unusable_without_any_uri) {
    ContainerCredentialsEnvGuard env;

    ProviderProbe probe;
    auto provider = probe.container_provider();
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::GeneralHTTPCredentialsProvider>(provider),
              nullptr);
    EXPECT_EQ(as_valid_http_provider(provider), nullptr);
}

} // namespace doris::cloud
