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
#include <gen_cpp/cloud.pb.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdio>
#include <memory>
#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "cpp/aws_sdk.h"
#include "cpp/obj-storage/client/s3_obj_storage_client.h"
#include "cpp/sync_point.h"
#include "cpp/test/container_credentials_test_util.h"
#include "meta-service/meta_service_helper.h"
#include "recycler/s3_accessor.h"

#ifdef USE_AZURE
#include "cpp/obj-storage/client/azure_obj_storage_client.h"
#endif

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
protected:
    static void SetUpTestSuite() {
        ensure_aws_sdk_initialized(config::aws_log_level);
#ifdef USE_AZURE
        initialize_azure_sdk(config::azure_log_level);
#endif
    }
};

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

std::shared_ptr<Aws::Auth::AWSCredentialsProvider> container_provider() {
    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> provider;
    SyncPoint::CallbackGuard guard;
    sp->set_call_back(
            "S3ObjStorageClient::credentials_provider",
            [&](auto&& args) {
                provider =
                        *try_any_cast<std::shared_ptr<Aws::Auth::AWSCredentialsProvider>*>(args[0]);
            },
            &guard);
    ObjectStoreInfoPB vault;
    vault.set_endpoint("example.com");
    vault.set_region("us-east-1");
    vault.set_bucket("bucket");
    vault.set_provider(ObjectStoreInfoPB::S3);
    vault.set_cred_provider_type(CredProviderTypePB::CONTAINER);
    auto config = parse_recycler_storage_config(std::move(vault));
    EXPECT_TRUE(config.has_value());
    if (config) {
        std::shared_ptr<S3Accessor> accessor;
        EXPECT_EQ(S3Accessor::create(std::move(*config), &accessor), 0);
    }
    sp->disable_processing();
    return provider;
}

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

    EXPECT_NE(as_valid_http_provider(container_provider()), nullptr)
            << "CONTAINER did not yield a usable container credentials provider for "
               "AWS_CONTAINER_CREDENTIALS_FULL_URI";

    ASSERT_EQ(std::remove(token_path.c_str()), 0);
    EXPECT_EQ(as_valid_http_provider(container_provider()), nullptr)
            << "CONTAINER ignored AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE: the provider stayed "
               "valid "
               "with the token file removed, so the path was never forwarded";
}

// ECS is the other half of the same provider mode: forwarding the full URI must not cost the
// relative one, which the provider resolves against the ECS agent's own address.
TEST_F(S3AccessorMockTest, container_provider_still_honours_ecs_relative_uri) {
    ContainerCredentialsEnvGuard env;
    env.set_ecs_task_role("/v2/credentials/mock");

    EXPECT_NE(as_valid_http_provider(container_provider()), nullptr)
            << "CONTAINER did not yield a usable container credentials provider for "
               "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";
}

// Pins the discriminator the two tests above rely on: with neither URI exported the provider really
// is unusable, so their assertions cannot be passing vacuously.
TEST_F(S3AccessorMockTest, container_provider_is_unusable_without_any_uri) {
    ContainerCredentialsEnvGuard env;

    auto provider = container_provider();
    ASSERT_NE(std::dynamic_pointer_cast<Aws::Auth::GeneralHTTPCredentialsProvider>(provider),
              nullptr);
    EXPECT_EQ(as_valid_http_provider(provider), nullptr);
}

TEST(S3AccessorLocationTest, location_is_the_configured_location) {
    auto location = [](std::string endpoint, std::string bucket, std::string prefix) {
        AwsObjStorageConfig client;
        client.connection.endpoint = std::move(endpoint);
        return S3Accessor(ObjStorageResourceConfig {.bucket = std::move(bucket),
                                                    .prefix = std::move(prefix),
                                                    .client = std::move(client)})
                .location();
    };
    EXPECT_EQ(location("endpoint", "bucket", "a/b"), location("endpoint", "bucket", "a/b"));
    // Prefixes with repeated '/' hold different objects although their normalized uri is the same
    EXPECT_NE(location("endpoint", "bucket", "a/b"), location("endpoint", "bucket", "a//b"));
    EXPECT_NE(location("endpoint", "bucket", "a/b"), location("endpoint", "bucket2", "a/b"));
    EXPECT_NE(location("endpoint", "bucket", "a/b"), location("endpoint2", "bucket", "a/b"));
}

TEST(S3AccessorLocationTest, location_includes_the_credential_identity) {
    auto accessor = [](std::string ak, std::string sk, std::string role_arn) {
        AwsObjStorageConfig client;
        client.connection.endpoint = "endpoint";
        client.auth.source = AccessKeyObjStorageAuth {std::move(ak), std::move(sk), {}};
        if (!role_arn.empty()) {
            AwsAssumeRole role;
            role.role_arn = std::move(role_arn);
            client.auth.assume_role = std::move(role);
        }
        return std::make_unique<S3Accessor>(ObjStorageResourceConfig {
                .bucket = "bucket", .prefix = "/a/b/", .client = std::move(client)});
    };
    const std::string ak1 = "AKIAIOSFODNN7EXAMPLE";
    const std::string ak2 = "AKIAI44QH8DHBEXAMPLE";
    auto base = accessor(ak1, "sk1", "");
    // An S3-compatible gateway may map one bucket name to the buckets of different tenants by the
    // credentials, so another identity is listed again
    EXPECT_NE(base->location(), accessor(ak2, "sk1", "")->location());
    EXPECT_NE(base->location(), accessor(ak1, "sk1", "role")->location());
    // The secret key is not part of the location, it identifies no namespace of its own
    EXPECT_EQ(base->location(), accessor(ak1, "sk2", "")->location());
    EXPECT_EQ(base->location().find("sk1"), std::string::npos);
    // The ak is hidden as in logs
    EXPECT_EQ(base->location().find(ak1), std::string::npos);
    EXPECT_NE(base->location().find(hide_access_key(ak1)), std::string::npos);
    // The root ignores the credentials
    EXPECT_EQ(base->root(), accessor(ak2, "sk2", "role")->root());
    EXPECT_EQ(base->root(), (StorageRoot {.container = "bucket", .prefix = "a/b"}));
}

TEST(S3AccessorLocationTest, root_ignores_endpoint_aliases) {
    auto accessor = [](std::string endpoint, std::string prefix) {
        AwsObjStorageConfig client;
        client.connection.endpoint = std::move(endpoint);
        return std::make_unique<S3Accessor>(ObjStorageResourceConfig {
                .bucket = "bucket", .prefix = std::move(prefix), .client = std::move(client)});
    };
    // Different locations, which are listed separately, may be the same bucket: they share a root
    auto http = accessor("http://s3.example.com", "a/b");
    auto https = accessor("https://s3.example.com", "a//b/");
    EXPECT_NE(http->location(), https->location());
    EXPECT_EQ(http->root(), https->root());
    EXPECT_NE(http->root(), accessor("http://s3.example.com", "a/c")->root());
}

TEST(S3AccessorLocationTest, location_distinguishes_aws_credential_sources_and_roles) {
    auto location = [](AwsObjStorageAuthConfig auth) {
        AwsObjStorageConfig client;
        client.connection.endpoint = "endpoint";
        client.auth = std::move(auth);
        return S3Accessor(ObjStorageResourceConfig {
                                  .bucket = "bucket", .prefix = "p", .client = std::move(client)})
                .location();
    };
    AwsObjStorageAuthConfig auth;
    auto anonymous = location(auth);
    auth.source = AwsCredentialChainType::Container;
    auto container = location(auth);
    EXPECT_NE(anonymous, container);
    auth.source = AwsCredentialChainType::InstanceProfile;
    EXPECT_NE(container, location(auth));
    auto instance_profile = location(auth);
    auth.implementation = AwsCredentialImplementation::V2;
    EXPECT_NE(instance_profile, location(auth));

    AwsAssumeRole role;
    role.role_arn = "arn:aws:iam::123456789012:role/recycler";
    role.external_id = "tenant1";
    auth.assume_role = role;
    auto tenant1 = location(auth);
    auth.assume_role->external_id = "tenant2";
    EXPECT_NE(tenant1, location(auth));
}

TEST(S3AccessorLocationTest, location_distinguishes_provider_credentials) {
    auto location = [](ObjStorageClientConfig client) {
        obj_storage_connection(client).endpoint = "endpoint";
        return S3Accessor(ObjStorageResourceConfig {
                                  .bucket = "bucket", .prefix = "p", .client = std::move(client)})
                .location();
    };
    AwsObjStorageConfig aws;
    CosObjStorageConfig cos;
    EXPECT_NE(location(aws), location(cos));

    GcpObjStorageConfig gcp;
    GcpNativeObjStorageAuth native;
    gcp.auth = native;
    auto default_credentials = location(gcp);
    native.source = GcpCredentialSource::ComputeEngine;
    gcp.auth = native;
    EXPECT_NE(default_credentials, location(gcp));
    native.impersonation_service_account = "tenant1@example.iam.gserviceaccount.com";
    gcp.auth = native;
    auto tenant1 = location(gcp);
    native.impersonation_service_account = "tenant2@example.iam.gserviceaccount.com";
    gcp.auth = native;
    EXPECT_NE(tenant1, location(gcp));

    const std::string access_id = "GOOG1EXAMPLEACCESSKEY";
    gcp.auth = GcpHmacObjStorageAuth {access_id, "secret1", "session1"};
    auto hmac = location(gcp);
    EXPECT_EQ(hmac.find(access_id), std::string::npos);
    EXPECT_NE(hmac.find(hide_access_key(access_id)), std::string::npos);
    EXPECT_EQ(hmac.find("secret1"), std::string::npos);
    EXPECT_EQ(hmac.find("session1"), std::string::npos);
    gcp.auth = GcpHmacObjStorageAuth {access_id, "secret2", "session2"};
    EXPECT_EQ(hmac, location(gcp));
    gcp.auth = GcpDefaultHmacObjStorageAuth {};
    EXPECT_NE(hmac, location(gcp));

    AzureObjStorageConfig azure;
    azure.auth = {.account_name = "account1", .credential = AzureSharedKeyConfig {"secret1"}};
    auto account1 = location(azure);
    EXPECT_EQ(account1.find("secret1"), std::string::npos);
    azure.auth.credential = AzureSharedKeyConfig {"secret2"};
    EXPECT_EQ(account1, location(azure));
    azure.auth.account_name = "account2";
    EXPECT_NE(account1, location(azure));
}

} // namespace doris::cloud
