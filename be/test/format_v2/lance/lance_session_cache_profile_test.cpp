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

#include "format_v2/lance/lance_session_cache_profile.h"

#include <gtest/gtest.h>
#include <lance/lance.h>

#include <atomic>
#include <barrier>
#include <chrono>
#include <future>
#include <thread>

#include "util/defer_op.h"

namespace doris::format::lance {

TEST(LanceSessionCacheProfileTest, EarlyLimitSnapshotsWhileAnotherWorkerIsInNativeRead) {
    std::unique_ptr<LanceSession, decltype(&lance_session_close)> session(
            lance_session_new(64 * 1024 * 1024, 64 * 1024 * 1024), lance_session_close);
    ASSERT_NE(nullptr, session);
    auto snapshot = [&](LanceSessionManager::CacheStatistics* stats) {
        LanceSessionCacheStats native {};
        if (lance_session_get_cache_stats(session.get(), &native) != 0) {
            return Status::InternalError("native snapshot unavailable");
        }
        *stats = {native.index_cache_misses, native.metadata_cache_misses};
        return Status::OK();
    };
    RuntimeProfile profile("early_limit");
    auto* index = profile.add_counter("index", TUnit::UNIT);
    auto* metadata = profile.add_counter("metadata", TUnit::UNIT);
    LanceSessionCacheProfile slow(&profile, index, metadata, snapshot);
    LanceSessionCacheProfile fast(&profile, index, metadata, snapshot);
    LanceSessionManager::CacheStatistics before_slow;
    ASSERT_TRUE(snapshot(&before_slow).ok());
    slow.start();
    const char* uri = "./be/test/format_v2/table/lance/data/all_types.lance";
    std::unique_ptr<LanceDataset, decltype(&lance_dataset_close)> slow_dataset(
            lance_dataset_open_with_session(uri, nullptr, 0, session.get()), lance_dataset_close);
    ASSERT_NE(nullptr, slow_dataset);
    LanceSessionManager::CacheStatistics before_fast;
    ASSERT_TRUE(snapshot(&before_fast).ok());
    fast.start();
    std::unique_ptr<LanceDataset, decltype(&lance_dataset_close)> fast_dataset(
            lance_dataset_open_with_session(uri, nullptr, 0, session.get()), lance_dataset_close);
    ASSERT_NE(nullptr, fast_dataset);
    const char* columns[] = {"row_id", nullptr};
    std::unique_ptr<LanceScanner, decltype(&lance_scanner_close)> slow_scanner(
            lance_scanner_new(slow_dataset.get(), columns, nullptr), lance_scanner_close);
    ASSERT_NE(nullptr, slow_scanner);
    struct BlockedRead {
        std::promise<void> entered;
        std::shared_future<void> released;
        std::atomic<bool> held = false;
    };
    std::promise<void> release;
    BlockedRead blocked;
    blocked.released = release.get_future().share();
    auto entered = blocked.entered.get_future();
    ASSERT_EQ(0, lance_scanner_set_statistics_callback(
                         slow_scanner.get(),
                         [](void* ctx, const LanceScanStatistics*) {
                             auto* blocked_read = static_cast<BlockedRead*>(ctx);
                             if (!blocked_read->held.exchange(true)) {
                                 // This callback runs inside native execution while the
                                 // scanner worker is still waiting for its read to finish.
                                 blocked_read->entered.set_value();
                                 blocked_read->released.wait();
                             }
                         },
                         &blocked));
    std::thread worker([&] {
        LanceBatch* batch = nullptr;
        int32_t result;
        while ((result = lance_scanner_next(slow_scanner.get(), &batch)) == 0) {
            lance_batch_free(batch);
        }
        EXPECT_EQ(1, result);
        slow_scanner.reset();
        slow.stop();
    });
    Defer join {[&] {
        release.set_value();
        worker.join();
    }};
    ASSERT_EQ(std::future_status::ready, entered.wait_for(std::chrono::seconds(10)));
    std::unique_ptr<LanceScanner, decltype(&lance_scanner_close)> fast_scanner(
            lance_scanner_new(fast_dataset.get(), columns, nullptr), lance_scanner_close);
    ASSERT_NE(nullptr, fast_scanner);
    ASSERT_EQ(0, lance_scanner_set_limit(fast_scanner.get(), 1));
    LanceBatch* batch = nullptr;
    ASSERT_EQ(0, lance_scanner_next(fast_scanner.get(), &batch));
    lance_batch_free(batch);
    fast_scanner.reset();
    fast.stop();
    LanceSessionManager::CacheStatistics current;
    ASSERT_TRUE(snapshot(&current).ok());
    ASSERT_GT(current.metadata_cache_misses, before_slow.metadata_cache_misses);
    TRuntimeProfileTree saved;
    profile.to_thrift(&saved);
    auto restored = RuntimeProfile::from_thrift(saved);
    EXPECT_EQ("true", saved.nodes[0].info_strings.at("LanceSessionCacheStatsAvailable"));
    EXPECT_GE(restored->get_counter("metadata")->value(),
              (current.metadata_cache_misses - before_slow.metadata_cache_misses) +
                      (current.metadata_cache_misses - before_fast.metadata_cache_misses));
}

TEST(LanceSessionCacheProfileTest, SnapshotOpenWindowsAndDoNotRecountOnClose) {
    RuntimeProfile profile("session");
    auto* index = profile.add_counter("index", TUnit::UNIT);
    auto* metadata = profile.add_counter("metadata", TUnit::UNIT);
    LanceSessionManager::CacheStatistics current {10, 20};
    auto snapshot = [&](auto* stats) {
        *stats = current;
        return Status::OK();
    };
    LanceSessionCacheProfile first(&profile, index, metadata, snapshot);
    LanceSessionCacheProfile second(&profile, index, metadata, snapshot);
    first.start();
    current = {12, 23};
    second.start();
    current = {15, 27};
    first.stop();

    TRuntimeProfileTree saved;
    profile.to_thrift(&saved);
    auto restored = RuntimeProfile::from_thrift(saved);
    EXPECT_EQ(8, restored->get_counter("index")->value());
    EXPECT_EQ(11, restored->get_counter("metadata")->value());
    EXPECT_EQ("true", saved.nodes[0].info_strings.at("LanceSessionCacheStatsAvailable"));
    profile.to_thrift(&saved);
    second.stop();
    second.stop();
    EXPECT_EQ(8, index->value());
    EXPECT_EQ(11, metadata->value());

    current = {100, 200};
    profile.to_thrift(&saved);
    EXPECT_EQ(8, index->value());
    second.start();
    current = {102, 205};
    second.stop();
    EXPECT_EQ(10, index->value());
    EXPECT_EQ(16, metadata->value());
}

TEST(LanceSessionCacheProfileTest, ConcurrentSnapshotAndCloseCountEachDeltaOnce) {
    RuntimeProfile profile("session");
    auto* index = profile.add_counter("index", TUnit::UNIT);
    auto* metadata = profile.add_counter("metadata", TUnit::UNIT);
    std::atomic<uint64_t> current = 0;
    LanceSessionCacheProfile window(&profile, index, metadata, [&](auto* stats) {
        const auto value = current.load();
        *stats = {value, value};
        return Status::OK();
    });
    std::barrier rendezvous(2);
    std::thread reporter([&] {
        for (int i = 0; i < 100; ++i) {
            rendezvous.arrive_and_wait();
            TRuntimeProfileTree saved;
            profile.to_thrift(&saved);
            rendezvous.arrive_and_wait();
        }
    });
    for (int i = 0; i < 100; ++i) {
        window.start();
        current.fetch_add(1);
        rendezvous.arrive_and_wait();
        window.stop();
        rendezvous.arrive_and_wait();
    }
    reporter.join();
    EXPECT_EQ(100, index->value());
    EXPECT_EQ(100, metadata->value());
}

TEST(LanceSessionCacheProfileTest, FailedSnapshotsStayUnavailableAndExpiredWindowsAreSafe) {
    RuntimeProfile profile("session");
    auto* index = profile.add_counter("index", TUnit::UNIT);
    auto* metadata = profile.add_counter("metadata", TUnit::UNIT);
    bool fail = false;
    {
        LanceSessionCacheProfile window(&profile, index, metadata, [&](auto* stats) {
            *stats = {1, 1};
            return fail ? Status::InternalError("snapshot unavailable") : Status::OK();
        });
        window.start();
        fail = true;
        TRuntimeProfileTree saved;
        profile.to_thrift(&saved);
        EXPECT_EQ("false", saved.nodes[0].info_strings.at("LanceSessionCacheStatsAvailable"));
        fail = false;
        profile.to_thrift(&saved);
        EXPECT_EQ("false", saved.nodes[0].info_strings.at("LanceSessionCacheStatsAvailable"));
    }
    TRuntimeProfileTree saved;
    profile.to_thrift(&saved);
    EXPECT_EQ(0, index->value());
    EXPECT_EQ(0, metadata->value());
}

} // namespace doris::format::lance
