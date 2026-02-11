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

#include "util/oss_credential_provider.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "common/logging.h"

namespace doris {

class OSSCredentialProviderTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup test environment
        LOG(INFO) << "Setting up OSSCredentialProviderTest";
    }

    void TearDown() override {
        // Cleanup
        LOG(INFO) << "Tearing down OSSCredentialProviderTest";
    }
};

// Test: Basic credential fetching from ECS metadata service
// NOTE: This test only works on real ECS instances with attached RAM role
TEST_F(OSSCredentialProviderTest, TestECSMetadataProvider) {
    LOG(INFO) << "Testing ECS metadata credential provider";

    ECSMetadataCredentialsProvider provider;

    // Attempt to get credentials
    try {
        auto creds = provider.getCredentials();

        // Verify credentials are not empty
        EXPECT_FALSE(creds.AccessKeyId().empty())
                << "AccessKeyId should not be empty when running on ECS with RAM role";
        EXPECT_FALSE(creds.AccessKeySecret().empty())
                << "AccessKeySecret should not be empty when running on ECS with RAM role";
        EXPECT_FALSE(creds.SessionToken().empty())
                << "SessionToken should not be empty when running on ECS with RAM role";

        LOG(INFO) << "Successfully fetched credentials from ECS metadata service";
        LOG(INFO) << "AccessKeyId prefix: " << creds.AccessKeyId().substr(0, 8) << "...";
    } catch (const AlibabaCloud::OSS::OssException& e) {
        // Expected when not running on ECS or no RAM role attached
        LOG(WARNING) << "Not running on ECS or no RAM role attached: " << e.GetErrorCode()
                     << " - " << e.GetErrorMessage();
        LOG(WARNING) << "This test requires running on ECS instance with attached RAM role";

        // Not a test failure, just skip
        GTEST_SKIP() << "Skipping test - not running on ECS instance with RAM role";
    }
}

// Test: Credential caching mechanism
// Verifies that subsequent calls return cached credentials without hitting metadata service
TEST_F(OSSCredentialProviderTest, TestCredentialCaching) {
    LOG(INFO) << "Testing credential caching mechanism";

    ECSMetadataCredentialsProvider provider;

    try {
        // First call - should fetch from metadata service
        auto creds1 = provider.getCredentials();
        EXPECT_FALSE(creds1.AccessKeyId().empty());

        // Second call - should return cached credentials
        auto creds2 = provider.getCredentials();
        EXPECT_FALSE(creds2.AccessKeyId().empty());

        // Should return same credentials (cached)
        EXPECT_EQ(creds1.AccessKeyId(), creds2.AccessKeyId())
                << "Cached credentials should have same AccessKeyId";
        EXPECT_EQ(creds1.AccessKeySecret(), creds2.AccessKeySecret())
                << "Cached credentials should have same AccessKeySecret";
        EXPECT_EQ(creds1.SessionToken(), creds2.SessionToken())
                << "Cached credentials should have same SessionToken";

        LOG(INFO) << "Credential caching verified successfully";
    } catch (const AlibabaCloud::OSS::OssException& e) {
        GTEST_SKIP() << "Skipping test - not running on ECS instance with RAM role: "
                     << e.GetErrorCode();
    }
}

// Test: Thread safety - Multiple threads calling getCredentials() concurrently
// Verifies that the provider is thread-safe
TEST_F(OSSCredentialProviderTest, TestThreadSafety) {
    LOG(INFO) << "Testing thread safety with concurrent credential requests";

    ECSMetadataCredentialsProvider provider;

    // Skip test if not on ECS - do a quick check first
    try {
        auto test_creds = provider.getCredentials();
        if (test_creds.AccessKeyId().empty()) {
            GTEST_SKIP() << "Not running on ECS instance with RAM role";
        }
    } catch (...) {
        GTEST_SKIP() << "Not running on ECS instance with RAM role";
    }

    const int num_threads = 10;
    const int calls_per_thread = 5;

    std::vector<std::thread> threads;
    std::atomic<int> success_count(0);
    std::atomic<int> failure_count(0);

    // Launch multiple threads that all try to get credentials
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&provider, &success_count, &failure_count, calls_per_thread]() {
            for (int j = 0; j < calls_per_thread; ++j) {
                try {
                    auto creds = provider.getCredentials();
                    if (!creds.AccessKeyId().empty() && !creds.AccessKeySecret().empty() &&
                        !creds.SessionToken().empty()) {
                        success_count++;
                    } else {
                        failure_count++;
                    }
                } catch (const AlibabaCloud::OSS::OssException& e) {
                    failure_count++;
                    LOG(WARNING) << "Thread failed to get credentials: " << e.GetErrorCode();
                }
            }
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    LOG(INFO) << "Thread safety test completed: " << success_count << " successes, "
              << failure_count << " failures out of " << (num_threads * calls_per_thread)
              << " total calls";

    // All calls should succeed (or most of them, allowing for transient network issues)
    EXPECT_GT(success_count, num_threads * calls_per_thread * 0.9)
            << "At least 90% of concurrent credential requests should succeed";
}

// Test: Multiple provider instances
// Verifies that multiple provider instances can coexist
TEST_F(OSSCredentialProviderTest, TestMultipleProviderInstances) {
    LOG(INFO) << "Testing multiple credential provider instances";

    try {
        ECSMetadataCredentialsProvider provider1;
        ECSMetadataCredentialsProvider provider2;

        auto creds1 = provider1.getCredentials();
        auto creds2 = provider2.getCredentials();

        // Both should get valid credentials
        EXPECT_FALSE(creds1.AccessKeyId().empty());
        EXPECT_FALSE(creds2.AccessKeyId().empty());

        // They should get the same credentials (from ECS metadata)
        EXPECT_EQ(creds1.AccessKeyId(), creds2.AccessKeyId())
                << "Different provider instances should get same credentials from ECS metadata";

        LOG(INFO) << "Multiple provider instances verified successfully";
    } catch (const AlibabaCloud::OSS::OssException& e) {
        GTEST_SKIP() << "Skipping test - not running on ECS instance with RAM role: "
                     << e.GetErrorCode();
    }
}

// Test: Credential refresh behavior
// NOTE: This is a long-running test that waits for credentials to expire
// Disabled by default - enable manually for integration testing
TEST_F(OSSCredentialProviderTest, DISABLED_TestCredentialRefresh) {
    LOG(INFO) << "Testing credential auto-refresh (long-running test)";

    ECSMetadataCredentialsProvider provider;

    try {
        // Get initial credentials
        auto creds1 = provider.getCredentials();
        std::string initial_access_key = creds1.AccessKeyId();
        LOG(INFO) << "Initial AccessKeyId: " << initial_access_key.substr(0, 8) << "...";

        // Wait for credentials to expire (typically 1 hour)
        // For testing, we wait just 6 minutes (credentials refresh 5 min before expiry)
        LOG(INFO) << "Waiting 6 minutes for credential refresh...";
        std::this_thread::sleep_for(std::chrono::minutes(6));

        // Get credentials again - should trigger refresh
        auto creds2 = provider.getCredentials();
        std::string refreshed_access_key = creds2.AccessKeyId();
        LOG(INFO) << "Refreshed AccessKeyId: " << refreshed_access_key.substr(0, 8) << "...";

        // Credentials should still be valid
        EXPECT_FALSE(creds2.AccessKeyId().empty());
        EXPECT_FALSE(creds2.AccessKeySecret().empty());
        EXPECT_FALSE(creds2.SessionToken().empty());

        // In a real scenario, credentials might be refreshed (different AccessKeyId)
        // or might be the same if still valid
        LOG(INFO) << "Credentials " << (initial_access_key == refreshed_access_key ? "same"
                                                                                    : "refreshed");
        LOG(INFO) << "Credential refresh test completed";
    } catch (const AlibabaCloud::OSS::OssException& e) {
        GTEST_SKIP() << "Skipping test - not running on ECS instance with RAM role: "
                     << e.GetErrorCode();
    }
}

// Test: Error handling - Invalid metadata service (simulated by using wrong host)
// This test verifies that the provider handles network errors gracefully
TEST_F(OSSCredentialProviderTest, DISABLED_TestMetadataServiceUnreachable) {
    // NOTE: This test would require mocking the HTTP calls or using a custom provider
    // that allows injecting failures. Disabled for now.
    LOG(INFO) << "Test for metadata service unreachable is disabled (requires mocking)";
    GTEST_SKIP() << "Test requires HTTP mocking infrastructure";
}

} // namespace doris

// Main function for running tests
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    // Initialize logging
    google::InitGoogleLogging(argv[0]);
    google::SetStderrLogging(google::INFO);

    LOG(INFO) << "Starting OSS Credential Provider unit tests";

    int ret = RUN_ALL_TESTS();

    LOG(INFO) << "OSS Credential Provider unit tests completed";

    return ret;
}
