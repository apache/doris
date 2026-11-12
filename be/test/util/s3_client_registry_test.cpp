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

#include "util/s3_client_registry.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <new>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "cpp/obj-client/rate_limited_obj_storage_client.h"
#include "cpp/obj-client/s3_obj_storage_client.h"
#include "cpp/sync_point.h"
#include "io/file_factory.h"
#include "io/fs/packed_file_manager.h"
#include "io/fs/s3_file_reader.h"
#include "io/fs/s3_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/memory/cache_manager.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"
#include "util/s3_util.h"

namespace doris {
namespace {

S3ClientConf client_conf(std::string token = "token") {
    S3ClientConf conf;
    conf.endpoint = "s3-registry-test.example.com";
    conf.region = "us-east-1";
    conf.token = std::move(token);
    return conf;
}

S3ClientRegistry::Client fake_client() {
    return std::make_shared<S3ObjStorageClient>(std::shared_ptr<Aws::S3::S3Client> {});
}

S3ClientRegistry::Creator fake_creator() {
    return [](const S3ClientConf&) -> Result<S3ClientRegistry::Client> { return fake_client(); };
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

TEST(S3ClientRegistryTest, LastOwnerReleasesClientAndWeakEntryImmediately) {
    S3ClientRegistry registry;
    auto first = registry.get_or_create(client_conf(), fake_creator()).value();
    std::weak_ptr<ObjStorageClient> weak = first;
    EXPECT_EQ(registry.stats().live, 1);
    EXPECT_FALSE(weak.expired());

    auto second = registry.get_or_create(client_conf(), fake_creator()).value();
    EXPECT_EQ(first, second);
    EXPECT_EQ(registry.stats().created, 1);
    EXPECT_EQ(registry.stats().hits, 1);
    first.reset();
    EXPECT_FALSE(weak.expired());
    second.reset();
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(registry.stats().destroyed, 1);
    EXPECT_EQ(registry.stats().indexed, 0);
}

TEST(S3ClientRegistryTest, LazyIteratorKeepsOriginalOwnerIndexed) {
    for (bool decorated : {false, true}) {
        S3ClientRegistry registry;
        S3ClientRegistry::Creator creator =
                [decorated](const S3ClientConf&) -> Result<S3ClientRegistry::Client> {
            auto client = fake_client();
            if (decorated) {
                return std::make_shared<RateLimitedObjStorageClient>(
                        std::move(client), std::make_shared<AllowAllPolicy>());
            }
            return client;
        };
        auto client = registry.get_or_create(client_conf(), creator).value();
        auto original_owner = client->shared_from_this();
        EXPECT_FALSE(client.owner_before(original_owner));
        EXPECT_FALSE(original_owner.owner_before(client));
        std::weak_ptr<ObjStorageClient> weak = client;
        auto iterator = client->list_objects({.bucket = "bucket"});

        client.reset();
        original_owner.reset();
        // The iterator now is the only strong owner. It must still be visible
        // through the factory index and must not count as a destroyed client.
        EXPECT_FALSE(weak.expired());
        EXPECT_EQ(registry.stats().live, 1);
        EXPECT_EQ(registry.stats().destroyed, 0);
        auto reused = registry.get_or_create(client_conf(), creator).value();
        EXPECT_EQ(reused, weak.lock());
        EXPECT_EQ(registry.stats().created, 1);
        EXPECT_EQ(registry.stats().hits, 1);

        reused.reset();
        EXPECT_EQ(registry.stats().live, 1);
        iterator.reset();
        EXPECT_TRUE(weak.expired());
        EXPECT_EQ(registry.stats().live, 0);
        EXPECT_EQ(registry.stats().destroyed, 1);
        EXPECT_EQ(registry.stats().indexed, 0);
    }
}

TEST(S3ClientRegistryTest, LazyIteratorCanOutliveRegistry) {
    std::unique_ptr<ObjStorageListIterator> iterator;
    std::weak_ptr<ObjStorageClient> weak;
    {
        S3ClientRegistry registry;
        auto client = registry.get_or_create(client_conf(), fake_creator()).value();
        weak = client;
        iterator = client->list_objects({.bucket = "bucket"});
        client.reset();
    }
    EXPECT_FALSE(weak.expired());
    iterator.reset();
    EXPECT_TRUE(weak.expired());
}

TEST(S3ClientRegistryTest, LifetimeGuardOutlivesSdkMembers) {
    for (bool decorated : {false, true}) {
        S3ClientRegistry registry;
        int sdk_releases = 0;
        S3ClientRegistry::Creator creator =
                [&](const S3ClientConf&) -> Result<S3ClientRegistry::Client> {
            // The null SDK pointer has an owning control block so its deleter
            // observes the real member teardown without constructing an SDK client.
            auto sdk = std::shared_ptr<Aws::S3::S3Client>(nullptr, [&](Aws::S3::S3Client*) {
                EXPECT_EQ(registry.stats().live, 1);
                EXPECT_EQ(registry.stats().destroyed, 0);
                ++sdk_releases;
            });
            S3ClientRegistry::Client client = std::make_shared<S3ObjStorageClient>(std::move(sdk));
            if (decorated) {
                client = std::make_shared<RateLimitedObjStorageClient>(
                        std::move(client), std::make_shared<AllowAllPolicy>());
            }
            return client;
        };
        auto client = registry.get_or_create(client_conf(), creator).value();
        auto iterator = client->list_objects({.bucket = "bucket"});
        registry.close();
        client.reset();
        EXPECT_EQ(sdk_releases, 0);
        iterator.reset();
        EXPECT_EQ(sdk_releases, 1);
        EXPECT_EQ(registry.stats().live, 0);
        EXPECT_EQ(registry.stats().destroyed, 1);
    }
}

TEST(S3ClientRegistryTest, TokenRotationReleasesEveryObsoleteClientAndKey) {
    S3ClientRegistry registry;
    auto pinned = registry.get_or_create(client_conf("pinned"), fake_creator()).value();
    for (int i = 0; i < 1000; ++i) {
        {
            auto client = registry.get_or_create(client_conf(std::to_string(i)), fake_creator());
            ASSERT_TRUE(client.has_value());
            EXPECT_EQ(registry.stats().indexed, 2);
            EXPECT_EQ(registry.stats().live, 2);
        }
        // No timer, capacity threshold or explicit pruning is needed.
        EXPECT_EQ(registry.stats().indexed, 1);
        EXPECT_EQ(registry.stats().live, 1);
        EXPECT_EQ(registry.stats().destroyed, i + 1);
    }
    EXPECT_EQ(registry.get_or_create(client_conf("pinned"), fake_creator()).value(), pinned);
    pinned.reset();
    EXPECT_EQ(registry.stats().indexed, 0);
    EXPECT_EQ(registry.stats().live, 0);
    EXPECT_EQ(registry.stats().created, registry.stats().destroyed);
}

TEST(S3ClientRegistryTest, PublicationFailureCleansPendingAndReleasesClientOutsideLock) {
    S3ClientRegistry registry;
    auto first = registry.get_or_create(client_conf("first"), fake_creator()).value();
    {
        auto* sync_point = SyncPoint::get_instance();
        sync_point->enable_processing();
        Defer disable_sync_point([&] { sync_point->disable_processing(); });
        SyncPoint::CallbackGuard guard;
        sync_point->set_call_back(
                "S3ClientRegistry::before_publication", [](auto&&) { throw std::bad_alloc {}; },
                &guard);
        EXPECT_THROW(static_cast<void>(registry.get_or_create(client_conf(), fake_creator())),
                     std::bad_alloc);
    }
    EXPECT_EQ(registry.stats().inflight, 0);
    EXPECT_EQ(registry.stats().indexed, 1);
    EXPECT_EQ(registry.stats().live, 1);
    EXPECT_EQ(registry.stats().destroyed, 1);
    EXPECT_EQ(registry.stats().failures, 1);
    EXPECT_TRUE(registry.get_or_create(client_conf(), fake_creator()).has_value());
    EXPECT_EQ(registry.stats().indexed, 1);
}

TEST(S3ClientRegistryTest, DestructionRunsOutsideRegistryLock) {
    S3ClientRegistry registry;
    int destroyed = 0;
    S3ClientRegistry::Creator creator =
            [&](const S3ClientConf&) -> Result<S3ClientRegistry::Client> {
        return S3ClientRegistry::Client(
                new S3ObjStorageClient(std::shared_ptr<Aws::S3::S3Client> {}),
                [&](ObjStorageClient* p) {
                    // SDK deleters can re-enter the registry without deadlocking.
                    EXPECT_EQ(registry.stats().inflight, 0);
                    ++destroyed;
                    delete p;
                });
    };
    auto client = registry.get_or_create(client_conf(), creator).value();
    client.reset();
    EXPECT_EQ(destroyed, 1);
    EXPECT_EQ(registry.stats().indexed, 0);
}

TEST(S3ClientRegistryTest, OldSdkDestructorCannotEraseReplacementRegistration) {
    S3ClientRegistry registry;
    std::promise<void> destroying;
    std::promise<void> resume;
    auto gate = resume.get_future().share();
    auto old = registry.get_or_create(
                               client_conf(),
                               [&](const S3ClientConf&) {
                                   auto sdk = std::shared_ptr<Aws::S3::S3Client>(
                                           nullptr, [&](Aws::S3::S3Client*) {
                                               destroying.set_value();
                                               gate.wait();
                                           });
                                   return Result<S3ClientRegistry::Client>(
                                           std::make_shared<S3ObjStorageClient>(std::move(sdk)));
                               })
                       .value();
    auto release = std::async(std::launch::async, [client = std::move(old)]() mutable {
        SCOPED_INIT_THREAD_CONTEXT();
        client.reset();
    });
    destroying.get_future().wait();
    auto replacement = registry.get_or_create(client_conf(), fake_creator()).value();
    resume.set_value();
    release.get();
    EXPECT_EQ(registry.stats().indexed, 1);
    EXPECT_EQ(registry.stats().live, 1);
    EXPECT_EQ(registry.get_or_create(client_conf(), fake_creator()).value(), replacement);
    replacement.reset();
    EXPECT_EQ(registry.stats().indexed, 0);
    EXPECT_EQ(registry.stats().live, 0);
}

TEST(S3ClientRegistryTest, AsyncRequestOwnsClientUntilCompletion) {
    S3ClientRegistry registry;
    auto client = registry.get_or_create(client_conf(), fake_creator()).value();
    std::promise<void> entered;
    std::promise<void> resume;
    auto gate = resume.get_future().share();
    auto task = std::async(std::launch::async, [owner = client, &entered, gate]() mutable {
        SCOPED_INIT_THREAD_CONTEXT();
        entered.set_value();
        gate.wait();
        owner.reset();
    });
    entered.get_future().wait();
    client.reset();
    EXPECT_EQ(registry.stats().live, 1);
    EXPECT_EQ(registry.stats().indexed, 1);
    resume.set_value();
    task.get();
    EXPECT_EQ(registry.stats().live, 0);
    EXPECT_EQ(registry.stats().indexed, 0);
}

TEST(S3ClientRegistryTest, FullConfigurationEqualitySurvivesHashCollisions) {
    auto a = client_conf();
    auto b = a;
    b.is_internal_bucket = !a.is_internal_bucket;
    b.use_virtual_addressing = !a.use_virtual_addressing;
    ASSERT_EQ(a.get_hash(), b.get_hash());
    ASSERT_NE(a, b);
    S3ClientRegistry registry;
    auto first = registry.get_or_create(a, fake_creator()).value();
    auto second = registry.get_or_create(b, fake_creator()).value();
    EXPECT_NE(first, second);
    EXPECT_EQ(registry.get_or_create(a, fake_creator()).value(), first);
    EXPECT_EQ(registry.get_or_create(b, fake_creator()).value(), second);
    b = a;
    b.token = "rotated-token";
    EXPECT_NE(registry.get_or_create(b, fake_creator()).value(), first);
}

TEST(S3ClientRegistryTest, ConfigurationHashIsNothrowAndOrdersCredentials) {
    auto conf = client_conf(std::string(4096, 't'));
    conf.ak = std::string(32, 'a');
    conf.sk = std::string(40, 's');
    conf.endpoint = std::string(256, 'e');
    conf.region = std::string(64, 'r');
    conf.bucket = std::string(128, 'b');
    conf.role_arn = std::string(256, 'n');
    conf.external_id = std::string(256, 'x');
    static_assert(noexcept(conf.get_hash()));
    static_assert(noexcept(S3ClientConfHash {}(conf)));
    EXPECT_EQ(conf.get_hash(), S3ClientConfHash {}(conf));

    auto swapped = conf;
    std::swap(swapped.ak, swapped.sk);
    EXPECT_NE(conf.get_hash(), swapped.get_hash());
    auto native = conf;
    native.credential =
            GcpCredentialConfig {.impersonation_service_account = std::string(256, 'g')};
    EXPECT_NE(conf.get_hash(), native.get_hash());
}

TEST(S3ClientRegistryTest, ExecEnvReleasesPackedFileOwnersBeforeSdkShutdown) {
    // A manager initialized before the first S3 vault must release its owners
    // explicitly; relying on reverse static destruction can shut down AWS first.
    io::PackedFileManager manager;
    S3ClientRegistry registry;
    auto client = registry.get_or_create(client_conf(), fake_creator()).value();
    std::weak_ptr<ObjStorageClient> weak = client;
    auto fs = std::make_shared<io::S3FileSystem>(S3Conf {}, "packed-shutdown-test");
    fs->_client->_client = std::move(client);
    manager._default_file_system = fs;
    manager._file_systems["vault"] = fs;
    auto make_context = [&] {
        auto context = std::make_unique<io::PackedFileManager::PackedFileContext>();
        context->file_system = fs;
        return context;
    };
    manager._current_packed_files["vault"] = make_context();
    manager._uploading_packed_files["uploading"] = make_context();
    manager._uploaded_packed_files["uploaded"] = make_context();
    fs.reset();
    EXPECT_EQ(registry.stats().live, 1);

    const bool was_ready = ExecEnv::ready();
    Defer restore_ready([was_ready] { ExecEnv::_s_ready = was_ready; });
    ExecEnv env;
    env._packed_file_manager = &manager;
    env.set_ready();
    env.destroy();

    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(registry.stats().live, 0);
    EXPECT_EQ(registry.stats().destroyed, 1);
    EXPECT_TRUE(manager._current_packed_files.empty());
    EXPECT_TRUE(manager._uploading_packed_files.empty());
    EXPECT_TRUE(manager._uploaded_packed_files.empty());
    EXPECT_TRUE(manager._file_systems.empty());
    EXPECT_EQ(manager._default_file_system, nullptr);
}

TEST(S3ClientRegistryTest, SingleFlightAndDifferentKeysCreateIndependently) {
    S3ClientRegistry registry;
    std::promise<void> entered;
    std::promise<void> resume;
    auto gate = resume.get_future().share();
    std::atomic<int> calls = 0;
    S3ClientRegistry::Creator creator =
            [&](const S3ClientConf& conf) -> Result<S3ClientRegistry::Client> {
        ++calls;
        if (conf.token == "slow") {
            entered.set_value();
            gate.wait();
        }
        return fake_client();
    };
    auto leader = std::async(std::launch::async, [&] {
        SCOPED_INIT_THREAD_CONTEXT();
        return registry.get_or_create(client_conf("slow"), creator);
    });
    entered.get_future().wait();
    std::vector<std::future<Result<S3ClientRegistry::Client>>> followers;
    for (int i = 0; i < 15; ++i) {
        followers.push_back(std::async(std::launch::async, [&] {
            SCOPED_INIT_THREAD_CONTEXT();
            return registry.get_or_create(client_conf("slow"), creator);
        }));
    }
    EXPECT_TRUE(await([&] { return registry.stats().waits == 15; }));
    auto fast = std::async(std::launch::async, [&] {
        SCOPED_INIT_THREAD_CONTEXT();
        return registry.get_or_create(client_conf("fast"), creator);
    });
    EXPECT_EQ(fast.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    resume.set_value();
    auto client = leader.get().value();
    for (auto& follower : followers) {
        EXPECT_EQ(follower.get().value(), client);
    }
    EXPECT_TRUE(fast.get().has_value());
    EXPECT_EQ(calls, 2);
    EXPECT_EQ(registry.stats().inflight, 0);
    EXPECT_EQ(registry.stats().indexed, 1);
    client.reset();
    EXPECT_EQ(registry.stats().live, 0);
    EXPECT_EQ(registry.stats().indexed, 0);
    EXPECT_EQ(registry.stats().destroyed, 2);
}

TEST(S3ClientRegistryTest, FailureAndExceptionWakeFollowersAndAllowRetry) {
    for (bool throws : {false, true}) {
        S3ClientRegistry registry;
        std::promise<void> entered;
        std::promise<void> resume;
        auto gate = resume.get_future().share();
        S3ClientRegistry::Creator creator =
                [&](const S3ClientConf&) -> Result<S3ClientRegistry::Client> {
            entered.set_value();
            gate.wait();
            if (throws) {
                throw std::runtime_error("injected creation failure");
            }
            return ResultError(Status::InternalError("injected creation failure"));
        };
        auto acquire = [&] {
            SCOPED_INIT_THREAD_CONTEXT();
            return registry.get_or_create(client_conf(), creator);
        };
        auto leader = std::async(std::launch::async, acquire);
        entered.get_future().wait();
        auto follower = std::async(std::launch::async, acquire);
        EXPECT_TRUE(await([&] { return registry.stats().waits == 1; }));
        resume.set_value();
        if (throws) {
            EXPECT_THROW(leader.get(), std::runtime_error);
            EXPECT_THROW(follower.get(), std::runtime_error);
        } else {
            EXPECT_FALSE(leader.get().has_value());
            EXPECT_FALSE(follower.get().has_value());
        }
        EXPECT_EQ(registry.stats().inflight, 0);
        EXPECT_EQ(registry.stats().failures, 1);
        EXPECT_TRUE(registry.get_or_create(client_conf(), fake_creator()).has_value());
    }
}

TEST(S3ClientRegistryTest, CloseWaitsForCreationAndPreservesReturnedOwner) {
    S3ClientRegistry registry;
    std::promise<void> entered;
    std::promise<void> resume;
    auto gate = resume.get_future().share();
    auto create = std::async(std::launch::async, [&] {
        SCOPED_INIT_THREAD_CONTEXT();
        return registry.get_or_create(client_conf(),
                                      [&](const S3ClientConf&) -> Result<S3ClientRegistry::Client> {
                                          entered.set_value();
                                          gate.wait();
                                          return fake_client();
                                      });
    });
    entered.get_future().wait();
    auto close = std::async(std::launch::async, [&] {
        SCOPED_INIT_THREAD_CONTEXT();
        registry.close();
    });
    EXPECT_TRUE(await([&] { return registry.stats().closed; }));
    EXPECT_FALSE(registry.get_or_create(client_conf("new"), fake_creator()).has_value());
    resume.set_value();
    auto client = create.get().value();
    close.get();
    EXPECT_EQ(registry.stats().indexed, 0);
    EXPECT_EQ(registry.stats().live, 1);
    client.reset();
    EXPECT_EQ(registry.stats().live, 0);
}

TEST(S3ClientRegistryTest, ExternalOwnerCanOutliveRegistry) {
    std::weak_ptr<ObjStorageClient> weak;
    S3ClientRegistry::Client client;
    {
        S3ClientRegistry registry;
        client = registry.get_or_create(client_conf(), fake_creator()).value();
        weak = client;
    }
    EXPECT_FALSE(weak.expired());
    client.reset();
    EXPECT_TRUE(weak.expired());
}

// CacheManager intentionally leaves registrations in place in BE_TEST. Restore
// the process cache's entry after each temporary policy used by these tests.
class S3ClientCacheTest : public testing::Test {
protected:
    void SetUp() override {
        auto* manager = CacheManager::instance();
        std::lock_guard lock(manager->_caches_lock);
        auto it = manager->_caches.find(CachePolicy::CacheType::S3_CLIENT_CACHE);
        if (it != manager->_caches.end()) {
            saved_policy = it->second;
        }
        auto* sync = SyncPoint::get_instance();
        sync->enable_processing();
        sync->set_call_back(
                "S3ClientRegistry::cache_clock",
                [this](auto&& args) { *try_any_cast<int64_t*>(args[0]) = clock.load(); },
                &clock_guard);
    }

    void TearDown() override {
        clock_guard = {};
        SyncPoint::get_instance()->disable_processing();
        auto* manager = CacheManager::instance();
        std::lock_guard lock(manager->_caches_lock);
        manager->_caches.erase(CachePolicy::CacheType::S3_CLIENT_CACHE);
        if (saved_policy) {
            manager->_caches.emplace(CachePolicy::CacheType::S3_CLIENT_CACHE, saved_policy);
        }
    }

    void sweep() {
        CacheManager::instance()->for_each_cache_prune_stale_wrap([](CachePolicy* policy) {
            if (policy->type() == CachePolicy::CacheType::S3_CLIENT_CACHE) {
                policy->prune_stale();
            }
        });
    }

    void prune_all() {
        CacheManager::instance()->cache_prune_all(CachePolicy::CacheType::S3_CLIENT_CACHE, true);
    }

    std::atomic<int64_t> clock = 0;
    SyncPoint::CallbackGuard clock_guard;
    CachePolicy* saved_policy = nullptr;
};

TEST_F(S3ClientCacheTest, ReusesAcrossSequentialQueriesAndReclaimsOnGc) {
    S3ClientRegistry registry;
    registry.init_cache(4, 300);
    std::weak_ptr<ObjStorageClient> weak;
    for (int query = 0; query < 64; ++query) {
        auto client = registry.get_or_create(client_conf(), fake_creator()).value();
        if (query > 0) {
            EXPECT_EQ(weak.lock(), client);
        }
        weak = client;
    }
    EXPECT_EQ(registry.stats().created, 1);
    EXPECT_EQ(registry.stats().cached, 1);
    EXPECT_EQ(registry.stats().live, 1);
    prune_all();
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(registry.stats().indexed, 0);
    EXPECT_EQ(registry.stats().cached, 0);
    EXPECT_EQ(registry.stats().destroyed, 1);
    EXPECT_TRUE(registry.get_or_create(client_conf(), fake_creator()).has_value());
    EXPECT_EQ(registry.stats().created, 2);
}

TEST_F(S3ClientCacheTest, ThousandCredentialRotationsStayBounded) {
    S3ClientRegistry registry;
    registry.init_cache(3, 300);
    auto active = registry.get_or_create(client_conf("active"), fake_creator()).value();
    for (int rotation = 0; rotation < 1000; ++rotation) {
        auto client = registry.get_or_create(client_conf(std::to_string(rotation)), fake_creator());
        ASSERT_TRUE(client.has_value()) << client.error();
        EXPECT_LE(registry.stats().cached, 3);
        EXPECT_LE(registry.stats().live, 4);
    }
    EXPECT_EQ(registry.stats().live, 4);
    EXPECT_EQ(registry.stats().cached, 3);
    EXPECT_EQ(registry.stats().destroyed, 997);
    // LRU eviction must not hide an active client from same-configuration users.
    EXPECT_EQ(registry.get_or_create(client_conf("active"), fake_creator()).value(), active);
    EXPECT_EQ(active->shared_from_this(), active);
    active.reset();
    registry.close();
    EXPECT_EQ(registry.stats().cached, 0);
    EXPECT_EQ(registry.stats().live, 0);
    EXPECT_EQ(registry.stats().created, registry.stats().destroyed);
}

TEST_F(S3ClientCacheTest, EvictsLeastRecentlyUsedConfiguration) {
    S3ClientRegistry registry;
    registry.init_cache(2, 300);
    std::weak_ptr<ObjStorageClient> a =
            registry.get_or_create(client_conf("a"), fake_creator()).value();
    std::weak_ptr<ObjStorageClient> b =
            registry.get_or_create(client_conf("b"), fake_creator()).value();
    EXPECT_EQ(registry.get_or_create(client_conf("a"), fake_creator()).value(), a.lock());
    std::weak_ptr<ObjStorageClient> c =
            registry.get_or_create(client_conf("c"), fake_creator()).value();
    EXPECT_FALSE(a.expired());
    EXPECT_TRUE(b.expired());
    EXPECT_FALSE(c.expired());
    EXPECT_EQ(registry.stats().cached, 2);
}

TEST_F(S3ClientCacheTest, IdleExpiryRenewsOnHitAndNeedsNoNewAcquisition) {
    S3ClientRegistry registry;
    registry.init_cache(4, 1);
    std::weak_ptr<ObjStorageClient> weak =
            registry.get_or_create(client_conf(), fake_creator()).value();
    clock = 999;
    EXPECT_EQ(registry.get_or_create(client_conf(), fake_creator()).value(), weak.lock());
    clock = 1000;
    sweep();
    EXPECT_FALSE(weak.expired());
    clock = 1999;
    sweep();
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(registry.stats().live, 0);
    EXPECT_EQ(registry.stats().cached, 0);
    EXPECT_EQ(registry.stats().indexed, 0);
}

TEST_F(S3ClientCacheTest, ExpiryAndMemoryPressurePreserveActiveOwners) {
    S3ClientRegistry registry;
    registry.init_cache(4, 1);
    auto active = registry.get_or_create(client_conf(), fake_creator()).value();
    clock = 1000;
    sweep();
    EXPECT_EQ(registry.stats().cached, 0);
    EXPECT_EQ(registry.stats().live, 1);
    EXPECT_EQ(registry.get_or_create(client_conf(), fake_creator()).value(), active);
    prune_all();
    EXPECT_EQ(registry.stats().cached, 0);
    EXPECT_EQ(registry.stats().live, 1);
    active.reset();
    EXPECT_EQ(registry.stats().live, 0);
    EXPECT_EQ(registry.stats().indexed, 0);
}

TEST_F(S3ClientCacheTest, FullConfigurationSeparatesCollisionsAndCredentialVersions) {
    S3ClientRegistry registry;
    registry.init_cache(4, 300);
    auto a = client_conf();
    auto b = a;
    b.is_internal_bucket = !a.is_internal_bucket;
    b.use_virtual_addressing = !a.use_virtual_addressing;
    ASSERT_EQ(a.get_hash(), b.get_hash());
    auto first = registry.get_or_create(a, fake_creator()).value();
    auto second = registry.get_or_create(b, fake_creator()).value();
    EXPECT_NE(first, second);
    auto c = a;
    c.token = "new-credentials";
    auto third = registry.get_or_create(c, fake_creator()).value();
    EXPECT_NE(third, first);
    EXPECT_EQ(registry.get_or_create(a, fake_creator()).value(), first);
    EXPECT_EQ(registry.get_or_create(b, fake_creator()).value(), second);
    EXPECT_EQ(registry.stats().cached, 3);
}

TEST_F(S3ClientCacheTest, ConcurrentQueriesRetainOneOriginalClient) {
    S3ClientRegistry registry;
    registry.init_cache(4, 300);
    std::promise<void> entered;
    std::promise<void> resume;
    auto gate = resume.get_future().share();
    auto creator = [&](const S3ClientConf&) -> Result<S3ClientRegistry::Client> {
        entered.set_value();
        gate.wait();
        return fake_client();
    };
    auto acquire = [&] {
        SCOPED_INIT_THREAD_CONTEXT();
        return registry.get_or_create(client_conf(), creator);
    };
    auto leader = std::async(std::launch::async, acquire);
    entered.get_future().wait();
    std::vector<std::future<Result<S3ClientRegistry::Client>>> followers;
    for (int i = 0; i < 15; ++i) {
        followers.push_back(std::async(std::launch::async, acquire));
    }
    EXPECT_TRUE(await([&] { return registry.stats().waits == 15; }));
    resume.set_value();
    auto owner = leader.get().value();
    for (auto& follower : followers) {
        auto client = follower.get().value();
        EXPECT_EQ(client, owner);
        EXPECT_FALSE(owner.owner_before(client));
        EXPECT_FALSE(client.owner_before(owner));
    }
    owner.reset();
    EXPECT_EQ(registry.stats().created, 1);
    EXPECT_EQ(registry.stats().cached, 1);
    EXPECT_EQ(registry.stats().live, 1);
    registry.close();
    EXPECT_EQ(registry.stats().live, 0);
}

TEST_F(S3ClientCacheTest, EvictedSdkDestructionCanReenterAcquisition) {
    S3ClientRegistry registry;
    registry.init_cache(1, 300);
    int destroyed = 0;
    auto creator = [&](const S3ClientConf&) -> Result<S3ClientRegistry::Client> {
        return S3ClientRegistry::Client(
                new S3ObjStorageClient(std::shared_ptr<Aws::S3::S3Client> {}),
                [&](ObjStorageClient* client) {
                    ++destroyed;
                    EXPECT_TRUE(registry.get_or_create(client_conf("reentrant"), fake_creator())
                                        .has_value());
                    delete client;
                });
    };
    EXPECT_TRUE(registry.get_or_create(client_conf("original"), creator).has_value());
    EXPECT_TRUE(registry.get_or_create(client_conf("replacement"), fake_creator()).has_value());
    EXPECT_EQ(destroyed, 1);
    EXPECT_LE(registry.stats().cached, 1);
    registry.close();
    EXPECT_EQ(registry.stats().live, 0);
}

TEST_F(S3ClientCacheTest, CloseDrainsCacheAndWaitsForBlockedCreators) {
    S3ClientRegistry registry;
    registry.init_cache(4, 300);
    EXPECT_TRUE(registry.get_or_create(client_conf("cached"), fake_creator()).has_value());
    std::promise<void> entered;
    std::promise<void> resume;
    auto gate = resume.get_future().share();
    auto created = std::async(std::launch::async, [&] {
        SCOPED_INIT_THREAD_CONTEXT();
        return registry.get_or_create(client_conf("blocked"),
                                      [&](const S3ClientConf&) -> Result<S3ClientRegistry::Client> {
                                          entered.set_value();
                                          gate.wait();
                                          return fake_client();
                                      });
    });
    entered.get_future().wait();
    auto closed = std::async(std::launch::async, [&] {
        SCOPED_INIT_THREAD_CONTEXT();
        registry.close();
    });
    EXPECT_TRUE(await([&] { return registry.stats().closed; }));
    resume.set_value();
    auto owner = created.get().value();
    closed.get();
    EXPECT_EQ(registry.stats().cached, 0);
    EXPECT_EQ(registry.stats().live, 1);
    EXPECT_FALSE(registry.get_or_create(client_conf(), fake_creator()).has_value());
    owner.reset();
    EXPECT_EQ(registry.stats().live, 0);
}

TEST_F(S3ClientCacheTest, CapacityAdjustmentAndDisabledRetention) {
    S3ClientRegistry registry;
    registry.init_cache(4, 300);
    for (int i = 0; i < 4; ++i) {
        EXPECT_TRUE(
                registry.get_or_create(client_conf(std::to_string(i)), fake_creator()).has_value());
    }
    auto* policy = CacheManager::instance()->_caches[CachePolicy::CacheType::S3_CLIENT_CACHE];
    policy->adjust_capacity_weighted(0.5);
    EXPECT_EQ(registry.stats().cached, 2);
    EXPECT_EQ(registry.stats().live, 2);
    policy->reset_initial_capacity(2);
    EXPECT_EQ(policy->initial_capacity(), 4);
    EXPECT_EQ(policy->get_capacity(), 4);
    policy->adjust_capacity_weighted(2);
    EXPECT_EQ(policy->get_capacity(), 4);
    policy->adjust_capacity_weighted(0);
    EXPECT_TRUE(registry.get_or_create(client_conf("uncached"), fake_creator()).has_value());
    EXPECT_EQ(registry.stats().cached, 0);
    EXPECT_EQ(registry.stats().live, 0);
    registry.release_cache();
    registry.init_cache(0, 300);
    EXPECT_TRUE(registry.get_or_create(client_conf(), fake_creator()).has_value());
    EXPECT_EQ(registry.stats().cached, 0);
    EXPECT_EQ(registry.stats().live, 0);
}

TEST_F(S3ClientCacheTest, FactoryReusesRealSdkAcrossQueriesAndExpiresAfterReadersFinish) {
    auto& factory = S3ClientFactory::instance();
    auto original = std::move(factory._registry);
    factory._registry = std::make_unique<S3ClientRegistry>();
    Defer restore_factory([&] { factory._registry.swap(original); });
    factory.init_client_cache();
    std::weak_ptr<ObjStorageClient> weak;
    io::FileReaderSPtr active_reader;
    for (int query = 0; query < 4; ++query) {
        io::FileSystemProperties properties;
        properties.system_type = TFileType::FILE_S3;
        properties.properties = {{"AWS_ENDPOINT", "http://127.0.0.1:19000"},
                                 {"AWS_REGION", "us-east-1"},
                                 {"AWS_ACCESS_KEY", "bounded-ak"},
                                 {"AWS_SECRET_KEY", "bounded-sk"}};
        for (int file = 0; file < 64; ++file) {
            io::FileDescription description;
            description.path = "s3://bounded-bucket/file-" + std::to_string(file);
            description.file_size = 32;
            auto result = FileFactory::create_file_reader(properties, description,
                                                          io::FileReaderOptions::DEFAULT, nullptr);
            ASSERT_TRUE(result.has_value()) << result.error();
            const auto& reader = static_cast<const io::S3FileReader&>(*result.value());
            auto client = reader._client->get();
            if (query != 0 || file != 0) {
                EXPECT_EQ(weak.lock(), client);
            }
            weak = client;
            if (query == 3 && file == 63) {
                active_reader = result.value();
            }
        }
        EXPECT_EQ(factory._registry->stats().created, 1);
        EXPECT_EQ(factory._registry->stats().cached, 1);
        EXPECT_EQ(factory._registry->stats().destroyed, 0);
    }
    clock = int64_t(config::s3_client_cache_stale_sweep_time_sec) * 1000;
    sweep();
    EXPECT_EQ(factory._registry->stats().cached, 0);
    EXPECT_FALSE(weak.expired());
    EXPECT_EQ(factory._registry->stats().live, 1);
    EXPECT_EQ(factory._registry->stats().destroyed, 0);
    ASSERT_TRUE(active_reader->close().ok());
    EXPECT_FALSE(weak.expired());
    active_reader.reset();
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(factory._registry->stats().live, 0);
    EXPECT_EQ(factory._registry->stats().destroyed, 1);
    factory.release_client_cache();
}

} // namespace doris
