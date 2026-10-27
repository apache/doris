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
#include <chrono>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "cpp/obj-client/rate_limited_obj_storage_client.h"
#include "cpp/obj-client/s3_obj_storage_client.h"
#include "runtime/memory/cache_manager.h"
#include "runtime/memory/cache_policy.h"
#include "runtime/thread_context.h"
#include "util/s3_util.h"

namespace doris {
namespace {

S3ClientConf cache_conf(std::string token = "token") {
    S3ClientConf conf;
    conf.endpoint = "s3-cache-test.example.com";
    conf.region = "us-east-1";
    conf.token = std::move(token);
    return conf;
}

S3ClientCache::Client fake_client() {
    return std::make_shared<S3ObjStorageClient>(std::shared_ptr<Aws::S3::S3Client> {});
}

S3ClientCache::Creator fake_creator() {
    return [](const S3ClientConf&) -> Result<S3ClientCache::Client> { return fake_client(); };
}

class AllowAllPolicy final : public ObjStorageRateLimitPolicy {
public:
    ObjStorageAdmission acquire(S3RateLimitType, size_t) const override { return {}; }
};

// Bound scheduling waits without sleeping or leaving blocked futures behind.
bool await(const std::function<bool()>& predicate) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        std::this_thread::yield();
    }
    return true;
}

} // namespace

TEST(S3ClientCacheTest, ExpiryPreservesRequestsAndReusesLiveClient) {
    int64_t now = 0;
    S3ClientCache cache(1, 10, [&] { return now; });
    auto first = cache.get_or_create(cache_conf(), fake_creator()).value();
    std::weak_ptr<ObjStorageClient> weak = first;
    now = 10;
    cache.prune_stale();
    EXPECT_EQ(cache.stats().entries, 0);
    EXPECT_EQ(cache.stats().live, 1);
    EXPECT_FALSE(weak.expired());

    auto second = cache.get_or_create(cache_conf(), fake_creator()).value();
    EXPECT_EQ(first, second);
    EXPECT_EQ(cache.stats().created, 1);
    EXPECT_EQ(cache.stats().weak_hits, 1);
    cache.prune_all();
    first.reset();
    EXPECT_FALSE(weak.expired());
    second.reset();
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(cache.stats().destroyed, 1);
    cache.prune_stale();
    EXPECT_EQ(cache.stats().indexed, 0);
}

TEST(S3ClientCacheTest, LazyIteratorKeepsOriginalOwnerIndexed) {
    for (bool decorated : {false, true}) {
        S3ClientCache cache(1, 10);
        S3ClientCache::Creator creator =
                [decorated](const S3ClientConf&) -> Result<S3ClientCache::Client> {
            auto client = fake_client();
            if (decorated) {
                return std::make_shared<RateLimitedObjStorageClient>(
                        std::move(client), std::make_shared<AllowAllPolicy>());
            }
            return client;
        };
        auto client = cache.get_or_create(cache_conf(), creator).value();
        auto original_owner = client->shared_from_this();
        EXPECT_FALSE(client.owner_before(original_owner));
        EXPECT_FALSE(original_owner.owner_before(client));
        std::weak_ptr<ObjStorageClient> weak = client;
        auto iterator = client->list_objects({.bucket = "bucket"});

        cache.prune_all();
        client.reset();
        original_owner.reset();
        // The iterator now is the only strong owner. It must still be visible
        // through the factory index and must not count as a destroyed client.
        EXPECT_FALSE(weak.expired());
        EXPECT_EQ(cache.stats().live, 1);
        EXPECT_EQ(cache.stats().destroyed, 0);
        auto reused = cache.get_or_create(cache_conf(), creator).value();
        EXPECT_EQ(reused, weak.lock());
        EXPECT_EQ(cache.stats().created, 1);
        EXPECT_EQ(cache.stats().weak_hits, 1);

        cache.prune_all();
        reused.reset();
        EXPECT_EQ(cache.stats().live, 1);
        iterator.reset();
        EXPECT_TRUE(weak.expired());
        EXPECT_EQ(cache.stats().live, 0);
        EXPECT_EQ(cache.stats().destroyed, 1);
        cache.prune_stale();
        EXPECT_EQ(cache.stats().indexed, 0);
    }
}

TEST(S3ClientCacheTest, LazyIteratorCanOutliveCache) {
    std::unique_ptr<ObjStorageListIterator> iterator;
    std::weak_ptr<ObjStorageClient> weak;
    {
        S3ClientCache cache(1, 10);
        auto client = cache.get_or_create(cache_conf(), fake_creator()).value();
        weak = client;
        iterator = client->list_objects({.bucket = "bucket"});
        client.reset();
    }
    EXPECT_FALSE(weak.expired());
    iterator.reset();
    EXPECT_TRUE(weak.expired());
}

TEST(S3ClientCacheTest, LifetimeGuardOutlivesSdkMembers) {
    for (bool decorated : {false, true}) {
        S3ClientCache cache(1, 10);
        int sdk_releases = 0;
        S3ClientCache::Creator creator = [&](const S3ClientConf&) -> Result<S3ClientCache::Client> {
            // The null SDK pointer has an owning control block so its deleter
            // observes the real member teardown without constructing an SDK client.
            auto sdk = std::shared_ptr<Aws::S3::S3Client>(nullptr, [&](Aws::S3::S3Client*) {
                EXPECT_EQ(cache.stats().live, 1);
                EXPECT_EQ(cache.stats().destroyed, 0);
                ++sdk_releases;
            });
            S3ClientCache::Client client = std::make_shared<S3ObjStorageClient>(std::move(sdk));
            if (decorated) {
                client = std::make_shared<RateLimitedObjStorageClient>(
                        std::move(client), std::make_shared<AllowAllPolicy>());
            }
            return client;
        };
        auto client = cache.get_or_create(cache_conf(), creator).value();
        auto iterator = client->list_objects({.bucket = "bucket"});
        cache.close();
        client.reset();
        EXPECT_EQ(sdk_releases, 0);
        iterator.reset();
        EXPECT_EQ(sdk_releases, 1);
        EXPECT_EQ(cache.stats().live, 0);
        EXPECT_EQ(cache.stats().destroyed, 1);
    }
}

TEST(S3ClientCacheTest, LruAndHitRefreshIdleTime) {
    int64_t now = 0;
    S3ClientCache cache(2, 10, [&] { return now; });
    std::weak_ptr<ObjStorageClient> a =
            cache.get_or_create(cache_conf("a"), fake_creator()).value();
    now = 1;
    std::weak_ptr<ObjStorageClient> b =
            cache.get_or_create(cache_conf("b"), fake_creator()).value();
    now = 5;
    EXPECT_EQ(cache.get_or_create(cache_conf("a"), fake_creator()).value(), a.lock());
    now = 6;
    std::weak_ptr<ObjStorageClient> c =
            cache.get_or_create(cache_conf("c"), fake_creator()).value();
    EXPECT_TRUE(b.expired());
    EXPECT_FALSE(a.expired());
    EXPECT_FALSE(c.expired());
    now = 14;
    cache.prune_stale();
    EXPECT_FALSE(a.expired());
    now = 15;
    cache.prune_stale();
    EXPECT_TRUE(a.expired());
    EXPECT_FALSE(c.expired());
    now = 16;
    cache.prune_stale();
    EXPECT_TRUE(c.expired());
    EXPECT_EQ(cache.stats().live, 0);
}

TEST(S3ClientCacheTest, RotationBoundsRetentionAndReclaimsWeakKeys) {
    S3ClientCache cache(4, 600);
    auto pinned = cache.get_or_create(cache_conf("pinned"), fake_creator()).value();
    for (int i = 0; i < 1000; ++i) {
        auto client = cache.get_or_create(cache_conf(std::to_string(i)), fake_creator());
        ASSERT_TRUE(client.has_value());
        EXPECT_LE(cache.stats().entries, 4);
    }
    // Dead keys must not grow with all 1,000 rotations even before periodic GC.
    EXPECT_LE(cache.stats().indexed, 128);
    cache.prune_stale();
    EXPECT_EQ(cache.stats().entries, 4);
    EXPECT_EQ(cache.stats().indexed, 5);
    EXPECT_EQ(cache.stats().live, 5);
    EXPECT_EQ(cache.get_or_create(cache_conf("pinned"), fake_creator()).value(), pinned);
    cache.prune_all();
    EXPECT_EQ(cache.stats().live, 1);
    pinned.reset();
    cache.prune_stale();
    EXPECT_EQ(cache.stats().indexed, 0);
    EXPECT_EQ(cache.stats().created, cache.stats().destroyed);
}

TEST(S3ClientCacheTest, ZeroCapacityStillReusesExternalOwners) {
    S3ClientCache cache(0, 10);
    auto first = cache.get_or_create(cache_conf(), fake_creator()).value();
    auto second = cache.get_or_create(cache_conf(), fake_creator()).value();
    EXPECT_EQ(first, second);
    EXPECT_EQ(cache.stats().entries, 0);
    first.reset();
    second.reset();
    EXPECT_EQ(cache.stats().live, 0);
    auto next = cache.get_or_create(cache_conf(), fake_creator()).value();
    EXPECT_EQ(cache.stats().created, 2);
    cache.set_capacity(1);
    EXPECT_EQ(cache.get_or_create(cache_conf(), fake_creator()).value(), next);
    EXPECT_EQ(cache.stats().entries, 1);
    cache.set_capacity(0);
    EXPECT_EQ(cache.stats().entries, 0);
    EXPECT_EQ(cache.stats().live, 1);
}

TEST(S3ClientCacheTest, FullConfigurationEqualitySurvivesHashCollisions) {
    auto a = cache_conf();
    auto b = a;
    b.is_internal_bucket = !a.is_internal_bucket;
    b.use_virtual_addressing = !a.use_virtual_addressing;
    ASSERT_EQ(a.get_hash(), b.get_hash());
    ASSERT_NE(a, b);
    S3ClientCache cache(2, 600);
    auto first = cache.get_or_create(a, fake_creator()).value();
    auto second = cache.get_or_create(b, fake_creator()).value();
    EXPECT_NE(first, second);
    EXPECT_EQ(cache.get_or_create(a, fake_creator()).value(), first);
    EXPECT_EQ(cache.get_or_create(b, fake_creator()).value(), second);
    b = a;
    b.token = "rotated-token";
    EXPECT_NE(cache.get_or_create(b, fake_creator()).value(), first);
}

TEST(S3ClientCacheTest, SingleFlightAndDifferentKeysCreateIndependently) {
    S3ClientCache cache(2, 600);
    std::promise<void> entered;
    std::promise<void> resume;
    auto gate = resume.get_future().share();
    std::atomic<int> calls = 0;
    S3ClientCache::Creator creator =
            [&](const S3ClientConf& conf) -> Result<S3ClientCache::Client> {
        ++calls;
        if (conf.token == "slow") {
            entered.set_value();
            gate.wait();
        }
        return fake_client();
    };
    auto leader = std::async(std::launch::async, [&] {
        SCOPED_INIT_THREAD_CONTEXT();
        return cache.get_or_create(cache_conf("slow"), creator);
    });
    entered.get_future().wait();
    std::vector<std::future<Result<S3ClientCache::Client>>> followers;
    for (int i = 0; i < 15; ++i) {
        followers.push_back(std::async(std::launch::async, [&] {
            SCOPED_INIT_THREAD_CONTEXT();
            return cache.get_or_create(cache_conf("slow"), creator);
        }));
    }
    EXPECT_TRUE(await([&] { return cache.stats().waits == 15; }));
    auto fast = std::async(std::launch::async, [&] {
        SCOPED_INIT_THREAD_CONTEXT();
        return cache.get_or_create(cache_conf("fast"), creator);
    });
    EXPECT_EQ(fast.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    // Pruning never deletes an in-flight task or invalidates its waiters.
    cache.prune_all();
    resume.set_value();
    auto client = leader.get().value();
    for (auto& follower : followers) {
        EXPECT_EQ(follower.get().value(), client);
    }
    EXPECT_TRUE(fast.get().has_value());
    EXPECT_EQ(calls, 2);
    EXPECT_EQ(cache.stats().inflight, 0);
}

TEST(S3ClientCacheTest, FailureAndExceptionWakeFollowersAndAllowRetry) {
    for (bool throws : {false, true}) {
        S3ClientCache cache(1, 600);
        std::promise<void> entered;
        std::promise<void> resume;
        auto gate = resume.get_future().share();
        S3ClientCache::Creator creator = [&](const S3ClientConf&) -> Result<S3ClientCache::Client> {
            entered.set_value();
            gate.wait();
            if (throws) {
                throw std::runtime_error("injected creation failure");
            }
            return ResultError(Status::InternalError("injected creation failure"));
        };
        auto acquire = [&] {
            SCOPED_INIT_THREAD_CONTEXT();
            return cache.get_or_create(cache_conf(), creator);
        };
        auto leader = std::async(std::launch::async, acquire);
        entered.get_future().wait();
        auto follower = std::async(std::launch::async, acquire);
        EXPECT_TRUE(await([&] { return cache.stats().waits == 1; }));
        resume.set_value();
        if (throws) {
            EXPECT_THROW(leader.get(), std::runtime_error);
            EXPECT_THROW(follower.get(), std::runtime_error);
        } else {
            EXPECT_FALSE(leader.get().has_value());
            EXPECT_FALSE(follower.get().has_value());
        }
        EXPECT_EQ(cache.stats().inflight, 0);
        EXPECT_EQ(cache.stats().failures, 1);
        EXPECT_TRUE(cache.get_or_create(cache_conf(), fake_creator()).has_value());
    }
}

TEST(S3ClientCacheTest, DestructionRunsOutsideCacheAndManagerLocks) {
    S3ClientCache cache(1, 600);
    auto policy = cache.create_cache_policy();
    auto* manager = CacheManager::instance();
    int destroyed = 0;
    S3ClientCache::Creator creator = [&](const S3ClientConf&) -> Result<S3ClientCache::Client> {
        return S3ClientCache::Client(
                new S3ObjStorageClient(std::shared_ptr<Aws::S3::S3Client> {}),
                [&](ObjStorageClient* p) {
                    // These re-entries deadlock if either lock is still held.
                    EXPECT_EQ(cache.stats().entries, 0);
                    manager->unregister_cache(CachePolicy::CacheType::S3_CLIENT_CACHE);
                    // BE_TEST unregister_cache() is a no-op, so explicitly take
                    // the manager mutex as well to test the production lock contract.
                    std::lock_guard manager_lock(manager->_caches_lock);
                    ++destroyed;
                    delete p;
                });
    };
    EXPECT_TRUE(cache.get_or_create(cache_conf(), creator).has_value());
    manager->cache_prune_all(CachePolicy::CacheType::S3_CLIENT_CACHE, true);
    EXPECT_EQ(destroyed, 1);
    // BE_TEST unregister is intentionally disabled; remove this local policy's
    // registration before its lifetime ends so later GC cannot call a stale pointer.
    {
        std::lock_guard lock(manager->_caches_lock);
        manager->_caches.erase(CachePolicy::CacheType::S3_CLIENT_CACHE);
    }
}

TEST(S3ClientCacheTest, CloseWaitsForCreationAndPreservesReturnedOwner) {
    S3ClientCache cache(1, 600);
    std::promise<void> entered;
    std::promise<void> resume;
    auto gate = resume.get_future().share();
    auto create = std::async(std::launch::async, [&] {
        SCOPED_INIT_THREAD_CONTEXT();
        return cache.get_or_create(cache_conf(),
                                   [&](const S3ClientConf&) -> Result<S3ClientCache::Client> {
                                       entered.set_value();
                                       gate.wait();
                                       return fake_client();
                                   });
    });
    entered.get_future().wait();
    auto close = std::async(std::launch::async, [&] {
        SCOPED_INIT_THREAD_CONTEXT();
        cache.close();
    });
    EXPECT_TRUE(await([&] { return cache.stats().closed; }));
    EXPECT_FALSE(cache.get_or_create(cache_conf("new"), fake_creator()).has_value());
    resume.set_value();
    auto client = create.get().value();
    close.get();
    EXPECT_EQ(cache.stats().entries, 0);
    EXPECT_EQ(cache.stats().indexed, 0);
    EXPECT_EQ(cache.stats().live, 1);
    client.reset();
    EXPECT_EQ(cache.stats().live, 0);
}

TEST(S3ClientCacheTest, ExternalOwnerCanOutliveCache) {
    std::weak_ptr<ObjStorageClient> weak;
    S3ClientCache::Client client;
    {
        S3ClientCache cache(1, 600);
        client = cache.get_or_create(cache_conf(), fake_creator()).value();
        weak = client;
    }
    EXPECT_FALSE(weak.expired());
    client.reset();
    EXPECT_TRUE(weak.expired());
}

} // namespace doris
