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

#include "util/s3_client_cache.h"

#include <gtest/gtest.h>

#include <atomic>
#include <condition_variable>
#include <future>
#include <stdexcept>
#include <thread>
#include <vector>

#include "cpp/obj-client/s3_obj_storage_client.h"
#include "cpp/sync_point.h"

namespace doris {
namespace {
S3ClientConf rotating_conf(std::string token) {
    S3ClientConf conf;
    conf.endpoint = "s3.example.com";
    conf.region = "us-east-1";
    conf.bucket = "bucket";
    conf.ak = "access-key";
    conf.sk = "secret-key";
    conf.token = std::move(token);
    return conf;
}

S3ClientCache::Client make_client() {
    return std::make_shared<io::S3ObjStorageClient>(std::shared_ptr<Aws::S3::S3Client> {});
}

// Deterministically hold the creator until every other caller has joined its flight.
class WaiterBarrier {
public:
    explicit WaiterBarrier(size_t count)
            : _remaining(count), _was_enabled(SyncPoint::get_instance()->get_enable()) {
        SyncPoint::get_instance()->set_call_back("s3_client_cache::wait", [&](auto&&) {
            std::lock_guard lock(_mutex);
            --_remaining;
            _cv.notify_all();
        });
        SyncPoint::get_instance()->enable_processing();
    }
    ~WaiterBarrier() {
        SyncPoint::get_instance()->clear_call_back("s3_client_cache::wait");
        if (!_was_enabled) {
            SyncPoint::get_instance()->disable_processing();
        }
    }
    void wait() {
        std::unique_lock lock(_mutex);
        EXPECT_TRUE(_cv.wait_for(lock, std::chrono::seconds(10), [&] { return _remaining == 0; }));
    }

private:
    std::mutex _mutex;
    std::condition_variable _cv;
    size_t _remaining;
    bool _was_enabled;
};
} // namespace

TEST(S3ClientCacheTest, ContinuousCredentialRotationReleasesOldClients) {
    std::atomic<int> live = 0;
    int created = 0;
    int64_t now = 0;
    S3ClientCache cache(8, 600, [&] { return now; });
    auto creator = [&]() -> Result<S3ClientCache::Client> {
        ++created;
        ++live;
        return S3ClientCache::Client(
                new io::S3ObjStorageClient(std::shared_ptr<Aws::S3::S3Client> {}),
                [&](io::ObjStorageClient* client) {
                    // Eviction must release SDK ownership outside the cache lock.
                    EXPECT_LE(cache.size(), 8);
                    delete client;
                    --live;
                });
    };
    auto active = cache.get_or_create(rotating_conf("first-token"), creator).value();
    std::weak_ptr<io::ObjStorageClient> active_weak = active;
    for (int i = 0; i < 1000; ++i) {
        auto conf = rotating_conf(std::to_string(i));
        auto current = cache.get_or_create(conf, creator);
        ASSERT_TRUE(current.has_value()) << current.error();
        EXPECT_EQ(cache.get_or_create(conf, creator).value(), current.value());
        EXPECT_LE(cache.size(), 8);
        EXPECT_LE(live.load(), 9); // At most eight cached clients plus the old active reader.
    }
    EXPECT_EQ(created, 1001);
    EXPECT_EQ(live, 9);
    EXPECT_FALSE(active_weak.expired());
    active.reset();
    EXPECT_TRUE(active_weak.expired());
    EXPECT_EQ(live, 8);
    now = 600;
    EXPECT_EQ(cache.prune_stale(), 8);
    EXPECT_EQ(cache.size(), 0);
    EXPECT_EQ(live, 0);
}

TEST(S3ClientCacheTest, CapacityEvictionUsesLastAccess) {
    S3ClientCache cache(2, 600, [] { return 0; });
    int created = 0;
    auto creator = [&]() -> Result<S3ClientCache::Client> {
        ++created;
        return make_client();
    };
    auto a = rotating_conf("a");
    auto b = rotating_conf("b");
    auto c = rotating_conf("c");
    auto first = cache.get_or_create(a, creator).value();
    auto second = cache.get_or_create(b, creator).value();
    EXPECT_EQ(cache.get_or_create(a, creator).value(), first);
    EXPECT_TRUE(cache.get_or_create(c, creator).has_value());
    EXPECT_EQ(cache.get_or_create(a, creator).value(), first);
    EXPECT_NE(cache.get_or_create(b, creator).value(), second);
    EXPECT_EQ(created, 4);
    EXPECT_EQ(cache.size(), 2);
    EXPECT_EQ(cache.clear(), 2);
    EXPECT_EQ(cache.size(), 0);
}

TEST(S3ClientCacheTest, IdleTtlRefreshesOnHitAndExpiresWithoutNewRequests) {
    int64_t now = 0;
    S3ClientCache cache(8, 10, [&] { return now; });
    auto conf = rotating_conf("token");
    auto creator = []() -> Result<S3ClientCache::Client> { return make_client(); };
    auto client = cache.get_or_create(conf, creator).value();
    now = 9;
    EXPECT_EQ(cache.get_or_create(conf, creator).value(), client);
    now = 10;
    EXPECT_EQ(cache.prune_stale(), 0);
    now = 19;
    EXPECT_EQ(cache.prune_stale(), 1);
    auto replacement = cache.get_or_create(conf, creator).value();
    EXPECT_NE(replacement, client);
    now = 29;
    // Expiry is also enforced on lookup, before the periodic GC has run.
    EXPECT_NE(cache.get_or_create(conf, creator).value(), replacement);
    EXPECT_EQ(cache.size(), 1);
}

TEST(S3ClientCacheTest, ConcurrentSameKeyCreatesOneClient) {
    constexpr int threads = 16;
    WaiterBarrier barrier(threads - 1);
    S3ClientCache cache(8, 600, [] { return 0; });
    std::atomic<int> created = 0;
    auto conf = rotating_conf("token");
    std::vector<std::future<Result<S3ClientCache::Client>>> results;
    for (int i = 0; i < threads; ++i) {
        results.push_back(std::async(std::launch::async, [&] {
            return cache.get_or_create(conf, [&]() -> Result<S3ClientCache::Client> {
                ++created;
                barrier.wait();
                return make_client();
            });
        }));
    }
    auto first = results.front().get().value();
    for (int i = 1; i < threads; ++i) {
        EXPECT_EQ(results[i].get().value(), first);
    }
    EXPECT_EQ(created, 1);
    EXPECT_EQ(cache.size(), 1);
}

TEST(S3ClientCacheTest, FailedFlightCompletesAllWaitersAndCanRetry) {
    constexpr int threads = 8;
    WaiterBarrier barrier(threads - 1);
    S3ClientCache cache(8, 600, [] { return 0; });
    std::atomic<int> created = 0;
    auto conf = rotating_conf("token");
    std::vector<std::future<Result<S3ClientCache::Client>>> results;
    for (int i = 0; i < threads; ++i) {
        results.push_back(std::async(std::launch::async, [&] {
            return cache.get_or_create(conf, [&]() -> Result<S3ClientCache::Client> {
                ++created;
                barrier.wait();
                return ResultError(Status::InternalError<false>("test creation failure"));
            });
        }));
    }
    for (auto& result : results) {
        auto value = result.get();
        ASSERT_FALSE(value.has_value());
        EXPECT_TRUE(value.error().to_string().find("test creation failure") != std::string::npos);
    }
    EXPECT_EQ(created, 1);
    EXPECT_EQ(cache.size(), 0);
    EXPECT_TRUE(cache.get_or_create(conf, []() -> Result<S3ClientCache::Client> {
                         return make_client();
                     }).has_value());
}

TEST(S3ClientCacheTest, ThrowingCreatorCompletesWaitersAndCanRetry) {
    WaiterBarrier barrier(1);
    S3ClientCache cache(8, 600, [] { return 0; });
    auto conf = rotating_conf("token");
    auto call = [&] {
        return cache.get_or_create(conf, [&]() -> Result<S3ClientCache::Client> {
            barrier.wait();
            throw std::runtime_error("test SDK exception");
        });
    };
    auto first = std::async(std::launch::async, call);
    auto second = std::async(std::launch::async, call);
    EXPECT_THROW(first.get(), std::runtime_error);
    EXPECT_THROW(second.get(), std::runtime_error);
    EXPECT_EQ(cache.size(), 0);
    EXPECT_TRUE(cache.get_or_create(conf, []() -> Result<S3ClientCache::Client> {
                         return make_client();
                     }).has_value());
}

TEST(S3ClientCacheTest, SlowCreationDoesNotBlockDifferentCredentials) {
    S3ClientCache cache(8, 600, [] { return 0; });
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future();
    auto slow = std::async(std::launch::async, [&] {
        return cache.get_or_create(rotating_conf("slow"), [&]() -> Result<S3ClientCache::Client> {
            entered.set_value();
            released.wait();
            return make_client();
        });
    });
    entered.get_future().wait();
    auto fast = std::async(std::launch::async, [&] {
        return cache.get_or_create(rotating_conf("fast"),
                                   []() -> Result<S3ClientCache::Client> { return make_client(); });
    });
    EXPECT_EQ(fast.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    release.set_value();
    EXPECT_TRUE(fast.get().has_value());
    EXPECT_TRUE(slow.get().has_value());
}

TEST(S3ClientCacheTest, PruningDuringCreationPreservesFlightAndActiveOwnership) {
    S3ClientCache cache(1, 600, [] { return 0; });
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future();
    auto conf = rotating_conf("token");
    auto creating = std::async(std::launch::async, [&] {
        return cache.get_or_create(conf, [&]() -> Result<S3ClientCache::Client> {
            entered.set_value();
            released.wait();
            return make_client();
        });
    });
    entered.get_future().wait();
    EXPECT_EQ(cache.clear(), 0);
    EXPECT_EQ(cache.prune_stale(), 0);
    release.set_value();
    auto client = creating.get().value();
    std::weak_ptr<io::ObjStorageClient> weak = client;
    EXPECT_EQ(cache.clear(), 1);
    EXPECT_FALSE(weak.expired());
    client.reset();
    EXPECT_TRUE(weak.expired());
}

TEST(S3ClientCacheTest, ZeroCapacityDoesNotRetainClients) {
    S3ClientCache cache(0, 600, [] { return 0; });
    auto conf = rotating_conf("token");
    auto creator = []() -> Result<S3ClientCache::Client> { return make_client(); };
    auto first = cache.get_or_create(conf, creator).value();
    EXPECT_NE(cache.get_or_create(conf, creator).value(), first);
    EXPECT_EQ(cache.size(), 0);
}

} // namespace doris
