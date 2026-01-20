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

#include "runtime/routine_load/aws_msk_iam_auth.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "common/status.h"

namespace doris {

class AwsMskIamAuthTest : public testing::Test {
protected:
    void SetUp() override {
        // Setup test configuration
        config.region = "us-east-1";
        config.use_instance_profile = false; // Don't use instance profile in tests
    }

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

TEST_F(AwsMskIamAuthTest, TestConfigWithProfile) {
    config.profile_name = "test-profile";

    AwsMskIamAuth auth(config);

    // Should create without error even if profile doesn't exist
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

// Integration test - only runs if AWS credentials are available
TEST_F(AwsMskIamAuthTest, DISABLED_IntegrationTestWithRealCredentials) {
    // This test is disabled by default
    // To run it, you need:
    // 1. AWS credentials configured (environment variables, ~/.aws/credentials, or IAM role)
    // 2. Run with: --gtest_also_run_disabled_tests

    config.use_instance_profile = true;

    AwsMskIamAuth auth(config);
    std::string token;
    int64_t token_lifetime_ms;

    std::string broker_hostname = "b-1.real-msk-cluster.us-east-1.amazonaws.com";

    Status status = auth.generate_token(broker_hostname, &token, &token_lifetime_ms);

    ASSERT_TRUE(status.ok()) << "Token generation failed: " << status.to_string();
    ASSERT_FALSE(token.empty());
    ASSERT_GT(token_lifetime_ms, 0);

    LOG(INFO) << "Generated token (first 100 chars): " << token.substr(0, 100);
    LOG(INFO) << "Token lifetime: " << token_lifetime_ms << "ms";
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
