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

#include "runtime/aws_msk_iam_auth.h"

#include <aws/core/auth/AWSCredentials.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "common/status.h"
#include "testutil/container_credentials_endpoint.h"
#include "util/s3_util.h"

namespace doris {

class AwsMskIamAuthTest : public testing::Test {
protected:
    void SetUp() override {
        // Setup test configuration
        config.region = "us-east-1";
    }

    // Nothing in this binary initialises the AWS SDK on its own - neither BE-UT's main() nor this
    // file - and without Aws::InitAPI there is no HTTP client factory, so a credentials provider
    // quietly returns nothing instead of making a request. Constructing the S3 client factory is
    // the cheapest in-tree way to get InitAPI called, exactly once per process, and it is the same
    // trick the S3 client factory tests use. Only the tests that drive a real fetch need it, so it
    // stays out of SetUp() where it would change what the older tests in this file do.
    static void ensure_aws_sdk_initialized() { (void)S3ClientFactory::instance(); }

    AwsMskIamAuth::Config config;
};

TEST_F(AwsMskIamAuthTest, TestConfigCreation) {
    // Test basic configuration creation
    AwsMskIamAuth auth(config);

    // This test just ensures the object can be created without crashing
    ASSERT_TRUE(true);
}

TEST_F(AwsMskIamAuthTest, TestTokenGeneration) {
    // This test requires AWS credentials to be available
    // In a real environment, you would mock the AWS SDK or use test credentials

    AwsMskIamAuth auth(config);
    std::string token;
    int64_t token_lifetime_ms;

    std::string broker_hostname = "b-1.test-msk.us-east-1.amazonaws.com";

    // In a real test environment with credentials, this should succeed
    // For CI/CD without credentials, we expect it to fail gracefully
    Status status = auth.generate_token(broker_hostname, &token, &token_lifetime_ms);

    if (status.ok()) {
        // If we have credentials, verify token properties
        ASSERT_FALSE(token.empty());
        ASSERT_GT(token_lifetime_ms, 0);
        ASSERT_LT(token_lifetime_ms, 3600000); // Less than 1 hour

        // Token should be valid JSON
        ASSERT_NE(token.find("version"), std::string::npos);
        ASSERT_NE(token.find("host"), std::string::npos);
        ASSERT_NE(token.find(broker_hostname), std::string::npos);
    } else {
        // Without credentials, we expect an error but no crash
        ASSERT_FALSE(status.ok());
        LOG(INFO) << "Token generation failed (expected without AWS credentials): "
                  << status.to_string();
    }
}

TEST_F(AwsMskIamAuthTest, TestConfigWithRoleArn) {
    config.role_arn = "arn:aws:iam::123456789012:role/TestRole";

    AwsMskIamAuth auth(config);

    // Should create without error even if role doesn't exist
    // (actual assumption will fail later when trying to get credentials)
    ASSERT_TRUE(true);
}

TEST_F(AwsMskIamAuthTest, TestConfigWithRoleArnAndExternalId) {
    config.role_arn = "arn:aws:iam::123456789012:role/TestRole";
    config.external_id = "external-id-123";

    AwsMskIamAuth auth(config);

    // Should create without error even if role doesn't exist
    ASSERT_TRUE(true);
}

TEST_F(AwsMskIamAuthTest, TestConfigWithProfile) {
    config.profile_name = "test-profile";

    AwsMskIamAuth auth(config);

    // Should create without error even if profile doesn't exist
    ASSERT_TRUE(true);
}

TEST_F(AwsMskIamAuthTest, TestConfigWithCredentialsProvider) {
    config.credentials_provider = "INSTANCE_PROFILE";

    AwsMskIamAuth auth(config);

    // Should create without error
    ASSERT_TRUE(true);
}

TEST_F(AwsMskIamAuthTest, TestConfigWithExplicitCredentials) {
    config.access_key = "AKIAIOSFODNN7EXAMPLE";
    config.secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

    AwsMskIamAuth auth(config);

    // Should create without error
    ASSERT_TRUE(true);
}

TEST_F(AwsMskIamAuthTest, TestCrossAccountAssumeRole) {
    config.role_arn = "arn:aws:iam::123456789012:role/CrossAccountRole";
    config.access_key = "AKIAIOSFODNN7EXAMPLE";
    config.secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

    AwsMskIamAuth auth(config);

    // Should create without error (actual assume role will fail without real credentials)
    ASSERT_TRUE(true);
}

TEST_F(AwsMskIamAuthTest, TestRoleArnWithCredentialsProvider) {
    config.role_arn = "arn:aws:iam::123456789012:role/TestRole";
    config.credentials_provider = "ENV";

    AwsMskIamAuth auth(config);

    // Should create without error. ENV provider will be used as AssumeRole base credentials provider.
    ASSERT_TRUE(true);
}

TEST_F(AwsMskIamAuthTest, TestRoleArnWithProfileName) {
    config.role_arn = "arn:aws:iam::123456789012:role/TestRole";
    config.profile_name = "default";

    AwsMskIamAuth auth(config);

    // Should create without error. Profile provider will be used as AssumeRole base credentials provider.
    ASSERT_TRUE(true);
}

TEST_F(AwsMskIamAuthTest, TestMultipleRegions) {
    std::vector<std::string> regions = {"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"};

    for (const auto& region : regions) {
        config.region = region;
        AwsMskIamAuth auth(config);

        std::string broker_hostname = "b-1.test-msk." + region + ".amazonaws.com";
        std::string token;
        int64_t token_lifetime_ms;

        Status status = auth.generate_token(broker_hostname, &token, &token_lifetime_ms);

        // We don't expect this to succeed without credentials,
        // but it should fail gracefully without crashing
        if (!status.ok()) {
            LOG(INFO) << "Token generation for region " << region
                      << " failed (expected): " << status.to_string();
        }
    }
}

TEST_F(AwsMskIamAuthTest, TestOAuthCallbackCreation) {
    auto auth = std::make_shared<AwsMskIamAuth>(config);
    std::string broker_hostname = "b-1.test-msk.us-east-1.amazonaws.com";

    AwsMskIamOAuthCallback callback(auth, broker_hostname);

    // Callback should be created successfully
    ASSERT_TRUE(true);
}

TEST_F(AwsMskIamAuthTest, TestOAuthCallbackCreationWithExternalIdAndRoleArn) {
    std::unordered_map<std::string, std::string> properties {
            {"security.protocol", "SASL_SSL"},
            {"sasl.mechanism", "OAUTHBEARER"},
            {"aws.region", "us-east-1"},
            {"aws.role_arn", "arn:aws:iam::123456789012:role/TestRole"},
            {"aws.external_id", "external-id-123"},
    };

    auto callback = AwsMskIamOAuthCallback::create_from_properties(
            properties, "b-1.test-msk.us-east-1.amazonaws.com:9098");
    ASSERT_NE(callback, nullptr);
}

TEST_F(AwsMskIamAuthTest, TestOAuthCallbackCreationWithExternalIdWithoutRoleArn) {
    std::unordered_map<std::string, std::string> properties {
            {"security.protocol", "SASL_SSL"},
            {"sasl.mechanism", "OAUTHBEARER"},
            {"aws.region", "us-east-1"},
            {"aws.external_id", "external-id-123"},
    };

    auto callback = AwsMskIamOAuthCallback::create_from_properties(
            properties, "b-1.test-msk.us-east-1.amazonaws.com:9098");
    ASSERT_EQ(callback, nullptr);
}

TEST_F(AwsMskIamAuthTest, ContainerProviderReadsTokenFileForPodIdentity) {
    ensure_aws_sdk_initialized();

    ContainerCredentialsEndpoint endpoint;
    ASSERT_TRUE(endpoint.start());

    ContainerCredentialsEnvGuard env;
    const std::string token_path = env.token_file_path("msk_container");
    env.write_token_file(token_path, "token-one");
    env.set_pod_identity(endpoint.url(), token_path);

    config.credentials_provider = "CONTAINER";
    AwsMskIamAuth auth(config);

    Aws::Auth::AWSCredentials credentials;
    ASSERT_TRUE(auth.get_credentials(&credentials).ok());
    EXPECT_EQ(credentials.GetAWSAccessKeyId(), "AKIDTEST");
    EXPECT_EQ(credentials.GetSessionToken(), "SESSIONTEST");

    const auto auth_headers = endpoint.auth_headers();
    ASSERT_FALSE(auth_headers.empty());
    EXPECT_EQ(auth_headers.back(), "token-one");
}

TEST_F(AwsMskIamAuthTest, EcsProviderAliasReachesContainerCredentialsEndpoint) {
    ensure_aws_sdk_initialized();

    ContainerCredentialsEndpoint endpoint;
    ASSERT_TRUE(endpoint.start());

    ContainerCredentialsEnvGuard env;
    const std::string token_path = env.token_file_path("msk_ecs_alias");
    env.write_token_file(token_path, "token-one");
    env.set_pod_identity(endpoint.url(), token_path);

    config.credentials_provider = "ECS";
    AwsMskIamAuth auth(config);

    Aws::Auth::AWSCredentials credentials;
    ASSERT_TRUE(auth.get_credentials(&credentials).ok());
    EXPECT_EQ(credentials.GetAWSAccessKeyId(), "AKIDTEST");
}

// Integration test - only runs if AWS credentials are available
TEST_F(AwsMskIamAuthTest, DISABLED_IntegrationTestWithRealCredentials) {
    // This test is disabled by default
    // To run it, you need:
    // 1. AWS credentials configured (environment variables, ~/.aws/credentials, or IAM role)
    // 2. Run with: --gtest_also_run_disabled_tests

    config.credentials_provider = "INSTANCE_PROFILE";

    AwsMskIamAuth auth(config);
    std::string token;
    int64_t token_lifetime_ms;

    std::string broker_hostname = "b-1.real-msk-cluster.us-east-1.amazonaws.com";

    Status status = auth.generate_token(broker_hostname, &token, &token_lifetime_ms);

    ASSERT_TRUE(status.ok()) << "Token generation failed: " << status.to_string();
    ASSERT_FALSE(token.empty());
    ASSERT_GT(token_lifetime_ms, 0);

    LOG(INFO) << "Generated token (first 100 chars): "
              << token.substr(0, std::min(size_t(100), token.size()));
    LOG(INFO) << "Token lifetime: " << token_lifetime_ms << "ms";
}

} // namespace doris
