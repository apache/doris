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

#include "io/cache/inflight_write_buffer_index.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <thread>
#include <vector>

#include "io/cache/block_file_cache.h"
#include "util/time.h"

namespace doris::io {
namespace {

std::shared_ptr<InflightWriteBufferEntry> make_entry(uint64_t epoch, size_t offset = 0,
                                                     size_t size = 4096) {
    return std::make_shared<InflightWriteBufferEntry>(nullptr, offset, size, MonotonicMicros(),
                                                      epoch);
}

TEST(InflightWriteBufferIndexTest, InsertLookupAndConditionalRemove) {
    InflightWriteBufferIndex index(8, "inflight_index_basic_test");
    const auto hash = BlockFileCache::hash("basic");
    auto entry = make_entry(7);
    const int64_t lock_wait_count = index._lock_wait_latency_metric->count();
    const int64_t lock_hold_count = index._lock_hold_latency_metric->count();

    EXPECT_EQ(index.insert_if_absent(hash, 0, entry), nullptr);
    EXPECT_EQ(index.size(), 1);
    EXPECT_EQ(index.lookup(hash, 0, 7), entry);

    auto unexpected = make_entry(7);
    EXPECT_FALSE(index.remove_if(hash, 0, unexpected));
    EXPECT_EQ(index.lookup(hash, 0, 7), entry);
    EXPECT_TRUE(index.remove_if(hash, 0, entry));
    EXPECT_EQ(index.size(), 0);
    EXPECT_EQ(index.lookup(hash, 0, 7), nullptr);
    EXPECT_EQ(index._lock_wait_latency_metric->count() - lock_wait_count, 6);
    EXPECT_EQ(index._lock_hold_latency_metric->count() - lock_hold_count, 6);
}

TEST(InflightWriteBufferIndexTest, LookupAllSupportsPartialHit) {
    InflightWriteBufferIndex index(8, "inflight_index_lookup_all_test");
    const auto hash = BlockFileCache::hash("lookup_all");
    auto first = make_entry(3, 0);
    auto third = make_entry(3, 8192);
    ASSERT_EQ(index.insert_if_absent(hash, 0, first), nullptr);
    ASSERT_EQ(index.insert_if_absent(hash, 8192, third), nullptr);

    auto results = index.lookup_all(hash, {0, 4096, 8192}, 3);
    ASSERT_EQ(results.size(), 3);
    EXPECT_EQ(results[0].block_offset, 0);
    EXPECT_EQ(results[0].entry, first);
    EXPECT_EQ(results[1].block_offset, 4096);
    EXPECT_EQ(results[1].entry, nullptr);
    EXPECT_EQ(results[2].block_offset, 8192);
    EXPECT_EQ(results[2].entry, third);
}

TEST(InflightWriteBufferIndexTest, NewEpochReplacesOldWithoutOldCallbackDeletingNew) {
    InflightWriteBufferIndex index(8, "inflight_index_epoch_test");
    const auto hash = BlockFileCache::hash("epoch");
    auto old_entry = make_entry(10);
    auto new_entry = make_entry(11);
    ASSERT_EQ(index.insert_if_absent(hash, 0, old_entry), nullptr);

    EXPECT_EQ(index.lookup(hash, 0, 11), nullptr);
    EXPECT_EQ(index.size(), 0);
    ASSERT_EQ(index.insert_if_absent(hash, 0, old_entry), nullptr);
    EXPECT_EQ(index.insert_if_absent(hash, 0, new_entry), nullptr);
    EXPECT_EQ(index.size(), 1);
    EXPECT_FALSE(index.remove_if(hash, 0, old_entry));
    EXPECT_EQ(index.lookup(hash, 0, 11), new_entry);

    auto stale_candidate = make_entry(10);
    EXPECT_EQ(index.insert_if_absent(hash, 0, stale_candidate), new_entry);
    EXPECT_EQ(index.lookup(hash, 0, 11), new_entry);
}

TEST(InflightWriteBufferIndexTest, ConcurrentInsertPublishesExactlyOneEntry) {
    InflightWriteBufferIndex index(64, "inflight_index_concurrent_test");
    const auto hash = BlockFileCache::hash("concurrent");
    constexpr size_t thread_count = 32;
    std::vector<std::shared_ptr<InflightWriteBufferEntry>> candidates;
    candidates.reserve(thread_count);
    for (size_t i = 0; i < thread_count; ++i) {
        candidates.emplace_back(make_entry(20));
    }

    std::atomic<size_t> inserted_count {0};
    std::vector<std::thread> threads;
    threads.reserve(thread_count);
    for (size_t i = 0; i < thread_count; ++i) {
        threads.emplace_back([&, i]() {
            if (index.insert_if_absent(hash, 0, candidates[i]) == nullptr) {
                inserted_count.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(inserted_count.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(index.size(), 1);
    auto published = index.lookup(hash, 0, 20);
    ASSERT_NE(published, nullptr);
    EXPECT_TRUE(std::find(candidates.begin(), candidates.end(), published) != candidates.end());
}

TEST(InflightWriteBufferIndexTest, ConcurrentNewEpochReplacementSurvivesOldRemoval) {
    InflightWriteBufferIndex index(16, "inflight_index_concurrent_replace_test");
    const auto hash = BlockFileCache::hash("concurrent_replace");
    auto old_entry = make_entry(30);
    auto new_entry = make_entry(31);
    ASSERT_EQ(index.insert_if_absent(hash, 0, old_entry), nullptr);

    std::atomic<bool> start {false};
    std::vector<std::thread> threads;
    threads.emplace_back([&]() {
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        EXPECT_EQ(index.insert_if_absent(hash, 0, new_entry), nullptr);
    });
    for (size_t thread_index = 0; thread_index < 16; ++thread_index) {
        threads.emplace_back([&]() {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            index.remove_if(hash, 0, old_entry);
        });
    }
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(index.size(), 1);
    EXPECT_EQ(index.lookup(hash, 0, 31), new_entry);
    EXPECT_FALSE(index.remove_if(hash, 0, old_entry));
    EXPECT_EQ(index.lookup(hash, 0, 31), new_entry);
}

} // namespace
} // namespace doris::io
