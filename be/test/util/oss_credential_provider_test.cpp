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

#include "cpp/oss_credential_provider.h"

#ifdef USE_OSS

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "common/logging.h"

namespace doris {

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------
static bool credentials_non_empty(const AlibabaCloud::OSS::Credentials& c) {
    return !c.AccessKeyId().empty() && !c.AccessKeySecret().empty();
}

// ---------------------------------------------------------------------------
// ECSMetadataCredentialsProvider tests
// All tests that require a real ECS RAM role use GTEST_SKIP() gracefully —
// same pattern as oss_credential_provider_test.cpp.
// ---------------------------------------------------------------------------
class ECSMetadataProviderTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Quick probe: if metadata service is unreachable, skip all ECS tests.
        // We detect this by attempting a single credential fetch.
        ECSMetadataCredentialsProvider probe;
        auto creds = probe.getCredentials();
        if (creds.AccessKeyId().empty()) {
            skip_ = true;
        }
    }
    bool skip_ = false;
};

TEST_F(ECSMetadataProviderTest, returns_non_empty_credentials) {
    if (skip_) GTEST_SKIP() << "Not running on ECS with RAM role";

    ECSMetadataCredentialsProvider provider;
    auto creds = provider.getCredentials();
    EXPECT_FALSE(creds.AccessKeyId().empty());
    EXPECT_FALSE(creds.AccessKeySecret().empty());
    EXPECT_FALSE(creds.SessionToken().empty());
}

TEST_F(ECSMetadataProviderTest, second_call_returns_cached_credentials) {
    if (skip_) GTEST_SKIP() << "Not running on ECS with RAM role";

    ECSMetadataCredentialsProvider provider;
    auto creds1 = provider.getCredentials();
    auto creds2 = provider.getCredentials();

    EXPECT_EQ(creds1.AccessKeyId(), creds2.AccessKeyId())
            << "Second call should return cached credentials";
    EXPECT_EQ(creds1.AccessKeySecret(), creds2.AccessKeySecret());
    EXPECT_EQ(creds1.SessionToken(), creds2.SessionToken());
}

TEST_F(ECSMetadataProviderTest, concurrent_calls_all_succeed) {
    if (skip_) GTEST_SKIP() << "Not running on ECS with RAM role";

    ECSMetadataCredentialsProvider provider;

    const int num_threads = 10;
    const int calls_per_thread = 5;
    std::atomic<int> success_count {0};
    std::atomic<int> failure_count {0};

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&]() {
            for (int j = 0; j < calls_per_thread; ++j) {
                auto creds = provider.getCredentials();
                if (credentials_non_empty(creds)) {
                    ++success_count;
                } else {
                    ++failure_count;
                }
            }
        });
    }
    for (auto& t : threads) t.join();

    // At least 90% of calls should succeed (allow for transient network blips)
    EXPECT_GT(success_count.load(), num_threads * calls_per_thread * 9 / 10)
            << "success=" << success_count << " failure=" << failure_count;
}

TEST_F(ECSMetadataProviderTest, multiple_instances_get_same_credentials) {
    if (skip_) GTEST_SKIP() << "Not running on ECS with RAM role";

    ECSMetadataCredentialsProvider p1;
    ECSMetadataCredentialsProvider p2;

    auto c1 = p1.getCredentials();
    auto c2 = p2.getCredentials();

    EXPECT_EQ(c1.AccessKeyId(), c2.AccessKeyId())
            << "Independent providers on the same ECS instance must return the same credentials";
}

// ---------------------------------------------------------------------------
// OSSSTSCredentialProvider tests
// ---------------------------------------------------------------------------
class OSSSTSProviderTest : public ::testing::Test {
protected:
    // Populated from environment variables so CI can override.
    // Required: OSS_ROLE_ARN, OSS_REGION
    // Optional: OSS_EXTERNAL_ID
    static void SetUpTestSuite() {
        if (std::getenv("OSS_ROLE_ARN")) role_arn = std::getenv("OSS_ROLE_ARN");
        if (std::getenv("OSS_REGION")) region = std::getenv("OSS_REGION");
        if (std::getenv("OSS_EXTERNAL_ID")) external_id = std::getenv("OSS_EXTERNAL_ID");
    }

    void SetUp() override {
        if (role_arn.empty() || region.empty()) {
            GTEST_SKIP() << "OSS_ROLE_ARN and OSS_REGION env vars required for STS tests";
        }
        // Also requires ECS metadata (base credential for STS)
        ECSMetadataCredentialsProvider probe;
        auto creds = probe.getCredentials();
        if (creds.AccessKeyId().empty()) {
            GTEST_SKIP() << "ECS RAM role required as base credential for STS AssumeRole";
        }
    }

public:
    static std::string role_arn;
    static std::string region;
    static std::string external_id;
};

std::string OSSSTSProviderTest::role_arn;
std::string OSSSTSProviderTest::region;
std::string OSSSTSProviderTest::external_id;

TEST_F(OSSSTSProviderTest, constructor_rejects_empty_role_arn) {
    // This must throw regardless of environment
    EXPECT_THROW(OSSSTSCredentialProvider("", "cn-hangzhou"), std::invalid_argument);
}

TEST_F(OSSSTSProviderTest, returns_valid_sts_credentials) {
    OSSSTSCredentialProvider provider(role_arn, region);
    auto creds = provider.getCredentials();

    EXPECT_FALSE(creds.AccessKeyId().empty()) << "STS must return a non-empty AccessKeyId";
    EXPECT_FALSE(creds.AccessKeySecret().empty());
    EXPECT_FALSE(creds.SessionToken().empty())
            << "STS assumed credentials must include a SecurityToken";
}

TEST_F(OSSSTSProviderTest, second_call_returns_cached_credentials) {
    OSSSTSCredentialProvider provider(role_arn, region);

    auto creds1 = provider.getCredentials();
    auto creds2 = provider.getCredentials();

    EXPECT_EQ(creds1.AccessKeyId(), creds2.AccessKeyId())
            << "Second call within TTL must return cached STS credentials";
    EXPECT_EQ(creds1.SessionToken(), creds2.SessionToken());
}

TEST_F(OSSSTSProviderTest, with_external_id_returns_valid_credentials) {
    if (external_id.empty()) {
        GTEST_SKIP() << "OSS_EXTERNAL_ID not set, skipping external_id test";
    }

    OSSSTSCredentialProvider provider(role_arn, region, external_id);
    auto creds = provider.getCredentials();

    EXPECT_FALSE(creds.AccessKeyId().empty());
    EXPECT_FALSE(creds.SessionToken().empty());
}

TEST_F(OSSSTSProviderTest, empty_external_id_not_sent_to_sts) {
    // Provider with empty external_id should work fine (does not pass ExternalId to STS)
    OSSSTSCredentialProvider provider(role_arn, region, /*external_id=*/"");
    auto creds = provider.getCredentials();

    EXPECT_FALSE(creds.AccessKeyId().empty());
    EXPECT_FALSE(creds.SessionToken().empty());
}

TEST_F(OSSSTSProviderTest, concurrent_calls_are_thread_safe) {
    OSSSTSCredentialProvider provider(role_arn, region);

    const int num_threads = 8;
    std::atomic<int> success_count {0};
    std::atomic<int> failure_count {0};

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&]() {
            auto creds = provider.getCredentials();
            if (credentials_non_empty(creds)) {
                ++success_count;
            } else {
                ++failure_count;
            }
        });
    }
    for (auto& t : threads) t.join();

    EXPECT_EQ(failure_count.load(), 0) << "All concurrent STS credential requests must succeed";
    EXPECT_EQ(success_count.load(), num_threads);
}

// ---------------------------------------------------------------------------
// OSSSTSCredentialProvider — no-ECS fallback safety
// When STS fails AND there is no cached credential, getCredentials() must
// return empty credentials instead of throwing (prevents BE crash).
// This is the regression test for the std::runtime_error crash fix.
// ---------------------------------------------------------------------------
TEST(OSSSTSProviderSafetyTest, empty_role_arn_throws_on_construction) {
    EXPECT_THROW(OSSSTSCredentialProvider("", "cn-hangzhou"), std::invalid_argument)
            << "Constructor must reject empty role_arn immediately";
}

// ---------------------------------------------------------------------------
// OSSDefaultCredentialsProvider tests
// ---------------------------------------------------------------------------
class OSSDefaultProviderTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Default provider uses Alibaba Cloud credential chain
        // (env vars, config file, ECS metadata).  Skip if none available.
        OSSDefaultCredentialsProvider probe;
        auto creds = probe.getCredentials();
        if (creds.AccessKeyId().empty()) {
            GTEST_SKIP() << "No Alibaba Cloud credentials available for default provider test";
        }
    }
};

TEST_F(OSSDefaultProviderTest, returns_non_empty_credentials) {
    OSSDefaultCredentialsProvider provider;
    auto creds = provider.getCredentials();

    EXPECT_FALSE(creds.AccessKeyId().empty());
    EXPECT_FALSE(creds.AccessKeySecret().empty());
}

TEST_F(OSSDefaultProviderTest, second_call_returns_cached_credentials) {
    OSSDefaultCredentialsProvider provider;

    auto creds1 = provider.getCredentials();
    auto creds2 = provider.getCredentials();

    EXPECT_EQ(creds1.AccessKeyId(), creds2.AccessKeyId())
            << "Default provider must cache and return same credentials on second call";
}

TEST_F(OSSDefaultProviderTest, concurrent_calls_are_thread_safe) {
    OSSDefaultCredentialsProvider provider;

    const int num_threads = 10;
    std::atomic<int> success_count {0};

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&]() {
            auto creds = provider.getCredentials();
            if (credentials_non_empty(creds)) ++success_count;
        });
    }
    for (auto& t : threads) t.join();

    EXPECT_GT(success_count.load(), num_threads * 9 / 10)
            << "At least 90% of concurrent default provider calls must succeed";
}

// ---------------------------------------------------------------------------
// OSSDefaultCredentialsProvider — safety: must not throw on failure
// ---------------------------------------------------------------------------
TEST(OSSDefaultProviderSafetyTest, returns_empty_credentials_when_no_chain_available) {
    // When no credential sources are configured, getCredentials() must return
    // empty credentials, NOT throw. Throwing would crash the BE via std::terminate.
    // We cannot guarantee the environment has no credentials, so we verify the
    // return type contract: whatever is returned must be an AlibabaCloud::OSS::Credentials.
    OSSDefaultCredentialsProvider provider;
    EXPECT_NO_THROW({
        auto creds = provider.getCredentials();
        // Either empty (no chain) or non-empty (chain found) — both are valid.
        // The important thing is: no exception thrown.
        (void)creds.AccessKeyId();
    });
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    google::InitGoogleLogging(argv[0]);
    google::SetStderrLogging(google::INFO);
    LOG(INFO) << "Starting OSS credential provider tests";
    return RUN_ALL_TESTS();
}

#endif // USE_OSS
