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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>

#include "cpp/sync_point.h"
#include "io/fs/file_reader.h"
#include "io/fs/s3_file_system.h"
#include "util/defer_op.h"
#include "util/s3_util.h"

#ifdef USE_AZURE

namespace doris {

// Exercise the real factory and cache. The legacy test creator deliberately
// bypasses caching, so it must not be used to prove cache lifetime or identity.
// SDK client construction is lazy and makes no storage request in these tests.
class AzureClientFactoryCacheTest : public testing::Test {
protected:
    void SetUp() override {
        auto& factory = S3ClientFactory::instance();
        {
            std::lock_guard lock(factory._lock);
            factory._azure_cache.clear();
            original_capacity = factory._azure_cache_capacity;
            factory._azure_cache_capacity = 2;
            factory._azure_cache_clock = 0;
        }
        auto* sync = SyncPoint::get_instance();
        was_enabled = sync->get_enable();
        sync->set_call_back(
                "S3ClientFactory::azure_client_time",
                [this](auto&& args) { *try_any_cast<int64_t*>(args[0]) = now_ms.load(); },
                &clock_callback);
        sync->enable_processing();
    }

    void TearDown() override {
        auto* sync = SyncPoint::get_instance();
        clock_callback = SyncPoint::CallbackGuard {};
        if (!was_enabled) {
            sync->disable_processing();
        }
        auto& factory = S3ClientFactory::instance();
        std::lock_guard lock(factory._lock);
        factory._azure_cache.clear();
        factory._azure_cache_capacity = original_capacity;
    }

    size_t cache_size() {
        auto& factory = S3ClientFactory::instance();
        std::lock_guard lock(factory._lock);
        return factory._azure_cache.size();
    }

    S3ClientConf sas_conf(std::string signature) {
        S3ClientConf conf;
        conf.provider = io::ObjStorageProvider::AZURE;
        conf.endpoint = "https://account.blob.core.windows.net";
        conf.bucket = "cache-test";
        conf.azure_credentials.type = AzureCredentialType::SAS;
        conf.azure_credentials.account_name = "account";
        conf.azure_credentials.sas_token = "sv=2024-01-01&sr=c&sig=" + signature;
        return conf;
    }

    std::shared_ptr<io::ObjStorageClient> client(const S3ClientConf& conf) {
        auto result = S3ClientFactory::instance().create(conf);
        if (!result.has_value()) {
            ADD_FAILURE() << result.error();
            return nullptr;
        }
        return std::move(result).value();
    }

    // 2100-01-01T00:00:00Z: future even for SDK construction's real clock.
    static constexpr int64_t TOKEN_EXPIRY_MS = 4102444800000LL;
    std::atomic<int64_t> now_ms {TOKEN_EXPIRY_MS - 60000};

private:
    size_t original_capacity = 0;
    bool was_enabled = false;
    SyncPoint::CallbackGuard clock_callback;
};

TEST_F(AzureClientFactoryCacheTest, TokenExpiryRejectsHitAndReleasesOnlyCachedOwnership) {
    auto conf = sas_conf("token-expiry");
    conf.azure_credentials.sas_token += "&se=2100-01-01T00%3A00%3A00Z";
    conf.azure_credentials.sas_expiration_time_ms = TOKEN_EXPIRY_MS + 60000;
    auto active_reader_client = client(conf);
    ASSERT_NE(active_reader_client, nullptr);
    EXPECT_EQ(client(conf), active_reader_client);
    std::weak_ptr<io::ObjStorageClient> reference = active_reader_client;

    now_ms = TOKEN_EXPIRY_MS;
    auto expired = S3ClientFactory::instance().create(conf);
    ASSERT_FALSE(expired.has_value());
    EXPECT_NE(expired.error().to_string().find("expired"), std::string::npos);
    EXPECT_EQ(expired.error().to_string().find("token-expiry"), std::string::npos);
    EXPECT_EQ(cache_size(), 0);
    EXPECT_FALSE(reference.expired());
    active_reader_client.reset();
    EXPECT_TRUE(reference.expired());
}

TEST_F(AzureClientFactoryCacheTest, ExplicitEarlierExpiryControlsEviction) {
    auto conf = sas_conf("explicit-expiry");
    conf.azure_credentials.sas_token += "&se=2100-01-01T00%3A00%3A00Z";
    conf.azure_credentials.sas_expiration_time_ms = TOKEN_EXPIRY_MS - 30000;
    std::weak_ptr<io::ObjStorageClient> reference = client(conf);
    EXPECT_FALSE(reference.expired());

    now_ms = TOKEN_EXPIRY_MS - 30000;
    auto expired = S3ClientFactory::instance().create(conf);
    ASSERT_FALSE(expired.has_value());
    EXPECT_EQ(cache_size(), 0);
    EXPECT_TRUE(reference.expired());
}

TEST_F(AzureClientFactoryCacheTest, ReusedFileSystemRejectsExpiredSasBeforeOpeningKnownSizeReader) {
    auto conf = sas_conf("reused-reader");
    conf.azure_credentials.sas_token += "&se=2100-01-01T00%3A00%3A00Z";
    S3Conf fs_conf {.bucket = conf.bucket, .prefix = {}, .client_conf = conf};
    auto fs_result = io::S3FileSystem::create(std::move(fs_conf), "azure-access-expiry");
    ASSERT_TRUE(fs_result.has_value());
    auto fs = std::move(fs_result).value();
    io::FileReaderOptions options;
    options.file_size = 1; // Skip HEAD: the expiry check must not depend on a service error.
    io::FileReaderSPtr active_reader;
    const std::string path = "abfss://cache-test@account.dfs.core.windows.net/object";
    ASSERT_TRUE(fs->open_file(path, &active_reader, &options).ok());

    now_ms = TOKEN_EXPIRY_MS;
    io::FileReaderSPtr rejected_reader;
    auto status = fs->open_file(path, &rejected_reader, &options);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("expired"), std::string::npos);
    EXPECT_EQ(status.to_string().find("reused-reader"), std::string::npos);
    EXPECT_EQ(rejected_reader, nullptr);
    EXPECT_FALSE(active_reader->closed());
    EXPECT_EQ(active_reader->size(), 1);

    auto refreshed = sas_conf("refreshed-reader");
    refreshed.azure_credentials.sas_expiration_time_ms = TOKEN_EXPIRY_MS + 60000;
    ASSERT_TRUE(fs->client_holder()->reset(refreshed).ok());
    io::FileReaderSPtr refreshed_reader;
    EXPECT_TRUE(fs->open_file(path, &refreshed_reader, &options).ok());
    EXPECT_NE(refreshed_reader, nullptr);
}

TEST_F(AzureClientFactoryCacheTest, ReaderAdmissionChecksCurrentCredentialAfterConcurrentReset) {
    auto long_lived = sas_conf("admission-old");
    long_lived.azure_credentials.sas_expiration_time_ms = TOKEN_EXPIRY_MS + 60000;
    S3Conf fs_conf {.bucket = long_lived.bucket, .prefix = {}, .client_conf = long_lived};
    auto fs_result = io::S3FileSystem::create(std::move(fs_conf), "azure-concurrent-admission");
    ASSERT_TRUE(fs_result.has_value());
    auto fs = std::move(fs_result).value();
    io::FileReaderOptions options;
    options.file_size = 1; // Neither the successful nor the rejected path may issue HEAD.
    io::FileReaderSPtr reader;
    Status open_status;
    std::mutex mutex;
    std::condition_variable changed;
    bool opener_paused = false;
    bool release_opener = false;
    SyncPoint::CallbackGuard admission_callback;
    SyncPoint::get_instance()->set_call_back(
            "ObjClientHolder::validate_for_access:before_lock",
            [&](auto&&) {
                std::unique_lock lock(mutex);
                opener_paused = true;
                changed.notify_all();
                EXPECT_TRUE(changed.wait_for(lock, std::chrono::seconds(10),
                                             [&] { return release_opener; }));
            },
            &admission_callback);
    {
        std::thread opener([&] {
            open_status = fs->open_file("abfss://cache-test@account.dfs.core.windows.net/object",
                                        &reader, &options);
        });
        // Fatal assertions below must also release the callback and join before
        // its captured state and the file system go out of scope.
        Defer release_and_join = [&] {
            {
                std::lock_guard lock(mutex);
                release_opener = true;
            }
            changed.notify_all();
            opener.join();
        };
        {
            std::unique_lock lock(mutex);
            ASSERT_TRUE(changed.wait_for(lock, std::chrono::seconds(10),
                                         [&] { return opener_paused; }));
        }
        auto short_lived = sas_conf("admission-new");
        short_lived.azure_credentials.sas_expiration_time_ms = TOKEN_EXPIRY_MS;
        ASSERT_TRUE(fs->client_holder()->reset(short_lived).ok());
        now_ms = TOKEN_EXPIRY_MS;
        // The stale A snapshot would still pass; only the holder's current B
        // credential can make this admission fail after the opener resumes.
        EXPECT_TRUE(S3ClientFactory::validate_credentials_for_access(long_lived).ok());
    }
    EXPECT_FALSE(open_status.ok());
    EXPECT_NE(open_status.to_string().find("expired"), std::string::npos);
    EXPECT_EQ(open_status.to_string().find("admission-old"), std::string::npos);
    EXPECT_EQ(open_status.to_string().find("admission-new"), std::string::npos);
    EXPECT_EQ(reader, nullptr);
}

TEST_F(AzureClientFactoryCacheTest, UnknownExpiryRotationsUseBoundedLru) {
    auto first_conf = sas_conf("rotation-first");
    auto second_conf = sas_conf("rotation-second");
    auto third_conf = sas_conf("rotation-third");
    auto first = client(first_conf);
    auto second = client(second_conf);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(client(first_conf), first); // Refresh LRU order.
    auto third = client(third_conf);
    ASSERT_NE(third, nullptr);
    EXPECT_EQ(cache_size(), 2);
    EXPECT_EQ(client(first_conf), first);
    // Eviction does not invalidate a reader holding the old identity's client.
    auto recreated_second = client(second_conf);
    ASSERT_NE(recreated_second, nullptr);
    EXPECT_NE(recreated_second, second);
    EXPECT_EQ(cache_size(), 2);
}

TEST_F(AzureClientFactoryCacheTest, ExpiryDuringConstructionDoesNotPublishClient) {
    auto conf = sas_conf("expires-during-construction");
    conf.azure_credentials.sas_expiration_time_ms = TOKEN_EXPIRY_MS;
    int built_clients = 0;
    SyncPoint::CallbackGuard built_callback;
    SyncPoint::get_instance()->set_call_back(
            "S3ClientFactory::azure_client_built",
            [&](auto&&) {
                ++built_clients;
                now_ms = TOKEN_EXPIRY_MS;
            },
            &built_callback);

    auto expired = S3ClientFactory::instance().create(conf);
    ASSERT_FALSE(expired.has_value());
    EXPECT_EQ(built_clients, 1);
    EXPECT_NE(expired.error().to_string().find("expired"), std::string::npos);
    EXPECT_EQ(expired.error().to_string().find("expires-during-construction"), std::string::npos);
    EXPECT_EQ(cache_size(), 0);
}

TEST_F(AzureClientFactoryCacheTest, AzureRotationsDoNotEvictS3Clients) {
    // Even an anonymous S3 client's SDK configuration can consult IMDS. Disable
    // that lookup for this offline test, without changing the caller's environment.
    std::optional<std::string> original_metadata_disabled;
    if (const char* value = std::getenv("AWS_EC2_METADATA_DISABLED")) {
        original_metadata_disabled = value;
    }
    Defer restore_environment = [&] {
        if (original_metadata_disabled.has_value()) {
            EXPECT_EQ(setenv("AWS_EC2_METADATA_DISABLED", original_metadata_disabled->c_str(), 1),
                      0);
        } else {
            EXPECT_EQ(unsetenv("AWS_EC2_METADATA_DISABLED"), 0);
        }
    };
    ASSERT_EQ(setenv("AWS_EC2_METADATA_DISABLED", "true", 1), 0);
    S3ClientConf s3_conf;
    s3_conf.endpoint = "azure-cache-s3-regression.example.com";
    s3_conf.region = "us-east-1";
    s3_conf.cred_provider_type = CredProviderType::Anonymous;
    auto s3_client = client(s3_conf);
    ASSERT_NE(s3_client, nullptr);
    for (int index = 0; index < 8; ++index) {
        ASSERT_NE(client(sas_conf("rotation-" + std::to_string(index))), nullptr);
    }
    EXPECT_EQ(cache_size(), 2);
    EXPECT_EQ(client(s3_conf), s3_client);
}

} // namespace doris

#endif
