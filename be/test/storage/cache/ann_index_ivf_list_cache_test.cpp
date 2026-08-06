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

#include "storage/cache/ann_index_ivf_list_cache.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <atomic>
#include <barrier>
#include <cstring>
#include <thread>
#include <vector>

#include "gtest/gtest_pred_impl.h"

namespace doris {

static constexpr uint32_t kNumShards = AnnIndexIVFListCache::kDefaultNumShards;

class AnnIndexIVFListCacheTest : public testing::Test {
public:
    AnnIndexIVFListCacheTest() {
        mem_tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::GLOBAL,
                                                       "AnnIndexIVFListCacheTest");
    }

protected:
    void SetUp() override {
        cache_ = std::make_unique<AnnIndexIVFListCache>(kNumShards * 1024 * 1024, kNumShards);
    }

    void TearDown() override { cache_.reset(); }

    std::shared_ptr<MemTrackerLimiter> mem_tracker;
    std::unique_ptr<AnnIndexIVFListCache> cache_;
};

TEST_F(AnnIndexIVFListCacheTest, basic_insert_and_lookup) {
    AnnIndexIVFListCache::CacheKey key("test_file", 4096, 0);

    PageCacheHandle handle;
    EXPECT_FALSE(cache_->lookup(key, &handle));

    auto* page = new DataPage(1024, mem_tracker);
    std::memset(page->data(), 0xAB, 1024);

    cache_->insert(key, page, &handle);
    Slice data = handle.data();
    EXPECT_EQ(data.data, page->data());

    PageCacheHandle lookup_handle;
    EXPECT_TRUE(cache_->lookup(key, &lookup_handle));
    Slice lookup_data = lookup_handle.data();
    EXPECT_EQ(static_cast<unsigned char>(lookup_data.data[0]), 0xAB);
}

TEST_F(AnnIndexIVFListCacheTest, cache_miss_different_key) {
    AnnIndexIVFListCache::CacheKey key("file_a", 4096, 0);

    auto* page = new DataPage(512, mem_tracker);
    PageCacheHandle handle;
    cache_->insert(key, page, &handle);

    {
        PageCacheHandle miss_handle;
        AnnIndexIVFListCache::CacheKey miss_key("file_b", 4096, 0);
        EXPECT_FALSE(cache_->lookup(miss_key, &miss_handle));
    }

    {
        PageCacheHandle miss_handle;
        AnnIndexIVFListCache::CacheKey miss_key("file_a", 8192, 0);
        EXPECT_FALSE(cache_->lookup(miss_key, &miss_handle));
    }

    {
        PageCacheHandle miss_handle;
        AnnIndexIVFListCache::CacheKey miss_key("file_a", 4096, 100);
        EXPECT_FALSE(cache_->lookup(miss_key, &miss_handle));
    }
}

TEST_F(AnnIndexIVFListCacheTest, multiple_entries) {
    constexpr int kCount = 32;
    std::vector<PageCacheHandle> handles(kCount);
    for (int i = 0; i < kCount; ++i) {
        AnnIndexIVFListCache::CacheKey key("multi", 4096, i * 1024);
        auto* page = new DataPage(256, mem_tracker);
        std::memset(page->data(), static_cast<char>(i), 256);
        cache_->insert(key, page, &handles[i]);
    }

    for (int i = 0; i < kCount; ++i) {
        AnnIndexIVFListCache::CacheKey key("multi", 4096, i * 1024);
        PageCacheHandle handle;
        EXPECT_TRUE(cache_->lookup(key, &handle));
        Slice data = handle.data();
        EXPECT_EQ(static_cast<unsigned char>(data.data[0]), static_cast<unsigned char>(i));
    }
}

// Simulates the stampede protection pattern from CachedRandomAccessReader::borrow().
//
// The test exercises the double-check locking path: N threads concurrently
// miss the fast-path cache lookup for the same key; only the first thread
// through the mutex performs the "disk I/O" (here: allocate + fill a DataPage);
// the remaining N-1 threads find the entry on re-check and skip the I/O.
TEST_F(AnnIndexIVFListCacheTest, stampede_protection) {
    constexpr int kThreads = 8;
    const AnnIndexIVFListCache::CacheKey key("stampede_file", 4096, 0);

    std::atomic<int> io_count {0};
    std::mutex io_mutex;
    std::barrier sync_point(kThreads);

    auto simulate_borrow = [&]() {
        PageCacheHandle handle;
        if (cache_->lookup(key, &handle)) {
            return;
        }

        sync_point.arrive_and_wait();

        std::lock_guard<std::mutex> lock(io_mutex);

        if (cache_->lookup(key, &handle)) {
            return;
        }

        auto* page = new DataPage(1024, mem_tracker);
        std::memset(page->data(), 0xCD, 1024);
        io_count.fetch_add(1, std::memory_order_relaxed);
        cache_->insert(key, page, &handle);
    };

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back(simulate_borrow);
    }
    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(io_count.load(), 1);

    PageCacheHandle verify_handle;
    EXPECT_TRUE(cache_->lookup(key, &verify_handle));
    Slice data = verify_handle.data();
    EXPECT_EQ(static_cast<unsigned char>(data.data[0]), 0xCD);
}

// Variant: multiple distinct keys under contention.  Each key should be
// loaded exactly once even when many threads race on each key.
TEST_F(AnnIndexIVFListCacheTest, stampede_protection_multiple_keys) {
    constexpr int kKeys = 4;
    constexpr int kThreadsPerKey = 4;
    constexpr int kTotalThreads = kKeys * kThreadsPerKey;

    std::atomic<int> io_counts[kKeys];
    for (auto& c : io_counts) {
        c.store(0);
    }

    std::mutex io_mutex;
    std::barrier sync_point(kTotalThreads);

    auto simulate_borrow = [&](int key_idx) {
        AnnIndexIVFListCache::CacheKey key("multi_stampede", 4096, key_idx * 1024);

        PageCacheHandle handle;
        if (cache_->lookup(key, &handle)) {
            return;
        }

        sync_point.arrive_and_wait();

        std::lock_guard<std::mutex> lock(io_mutex);

        if (cache_->lookup(key, &handle)) {
            return;
        }

        auto* page = new DataPage(512, mem_tracker);
        std::memset(page->data(), static_cast<char>(key_idx), 512);
        io_counts[key_idx].fetch_add(1, std::memory_order_relaxed);
        cache_->insert(key, page, &handle);
    };

    std::vector<std::thread> threads;
    threads.reserve(kTotalThreads);
    for (int k = 0; k < kKeys; ++k) {
        for (int t = 0; t < kThreadsPerKey; ++t) {
            threads.emplace_back(simulate_borrow, k);
        }
    }
    for (auto& th : threads) {
        th.join();
    }

    for (int k = 0; k < kKeys; ++k) {
        EXPECT_EQ(io_counts[k].load(), 1) << "key_idx=" << k;

        AnnIndexIVFListCache::CacheKey key("multi_stampede", 4096, k * 1024);
        PageCacheHandle handle;
        EXPECT_TRUE(cache_->lookup(key, &handle));
        Slice data = handle.data();
        EXPECT_EQ(static_cast<unsigned char>(data.data[0]), static_cast<unsigned char>(k));
    }
}

TEST_F(AnnIndexIVFListCacheTest, singleton_lifecycle) {
    EXPECT_EQ(AnnIndexIVFListCache::instance(), nullptr);

    auto* inst = AnnIndexIVFListCache::create_global_cache(kNumShards * 2048, kNumShards);
    EXPECT_NE(inst, nullptr);
    EXPECT_EQ(AnnIndexIVFListCache::instance(), inst);

    AnnIndexIVFListCache::destroy_global_cache();
    EXPECT_EQ(AnnIndexIVFListCache::instance(), nullptr);
}

} // namespace doris
