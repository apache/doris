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

// Whether a cached block is protected by its tablet's TTL is computed from the tablet's current
// policy (TabletCachePolicyRegistry), never stored on the block. These tests change the policy
// behind the cache's back and check that the queues follow, that eviction never mistakes a
// protected block for an unprotected one, and that a restart recovers the right queues from
// what was persisted.

#include <gtest/gtest.h>

#include "io/cache/block_file_cache_test_common.h"
#include "io/cache/cache_block_meta_store.h"
#include "io/cache/tablet_cache_policy_registry.h"
#include "util/defer_op.h"

namespace doris::io {

namespace {

constexpr size_t kBlockSize = 100000;

class TtlPolicyTestGuard {
public:
    TtlPolicyTestGuard() {
        _sweep_interval_ms = config::file_cache_queue_sweep_interval_ms;
        _evict_in_advance = config::enable_evict_file_cache_in_advance;
        _refresh_batch = config::file_cache_tablet_policy_refresh_batch;
        // The tests drive the queue sweep, the eviction and the policy refresh themselves.
        config::file_cache_queue_sweep_interval_ms = 3600 * 1000;
        config::enable_evict_file_cache_in_advance = false;
        config::file_cache_tablet_policy_refresh_batch = 0;
        auto* registry = TabletCachePolicyRegistry::instance();
        registry->clear();
        // Policies only change when a test says so.
        registry->set_fetcher([](int64_t, TabletCachePolicy*) {
            return Status::InternalError("no meta service in this test");
        });
    }
    ~TtlPolicyTestGuard() {
        config::file_cache_queue_sweep_interval_ms = _sweep_interval_ms;
        config::enable_evict_file_cache_in_advance = _evict_in_advance;
        config::file_cache_tablet_policy_refresh_batch = _refresh_batch;
        auto* registry = TabletCachePolicyRegistry::instance();
        registry->set_fetcher(nullptr);
        registry->clear();
    }

private:
    int64_t _sweep_interval_ms;
    bool _evict_in_advance;
    int64_t _refresh_batch;
};

FileCacheSettings ttl_policy_test_settings() {
    FileCacheSettings settings;
    settings.ttl_queue_size = 5000000;
    settings.ttl_queue_elements = 50000;
    settings.query_queue_size = 3000000;
    settings.query_queue_elements = 30000;
    settings.index_queue_size = 1000000;
    settings.index_queue_elements = 10000;
    settings.disposable_queue_size = 1000000;
    settings.disposable_queue_elements = 10000;
    settings.capacity = 10000000;
    settings.max_file_block_size = kBlockSize;
    settings.max_query_cache_size = 30;
    return settings;
}

CacheContext context_of(int64_t tablet_id, FileCacheType cache_class, int64_t legacy_deadline,
                        ReadStatistics* stats) {
    CacheContext context;
    context.stats = stats;
    context.tablet_id = tablet_id;
    context.cache_type = cache_class;
    context.expiration_time = legacy_deadline;
    return context;
}

// Caches `blocks` downloaded blocks of `key` with the given context.
void fill(BlockFileCache& cache, const std::string& key, size_t blocks, CacheContext context) {
    auto hash = BlockFileCache::hash(key);
    for (size_t i = 0; i < blocks; ++i) {
        auto holder = cache.get_or_set(hash, i * kBlockSize, kBlockSize, context);
        auto file_blocks = fromHolder(holder);
        ASSERT_EQ(file_blocks.size(), 1);
        ASSERT_TRUE(file_blocks[0]->get_or_set_downloader() == FileBlock::get_caller_id());
        download(file_blocks[0]);
    }
}

FileBlockSPtr block_of(BlockFileCache& cache, const std::string& key, size_t index) {
    std::lock_guard<std::mutex> cache_lock(cache._mutex);
    auto* cell = cache.get_cell(BlockFileCache::hash(key), index * kBlockSize, cache_lock);
    DORIS_CHECK(cell != nullptr);
    return cell->file_block;
}

size_t queue_size(BlockFileCache& cache, FileCacheType type) {
    std::lock_guard<std::mutex> cache_lock(cache._mutex);
    return cache.get_queue(type).get_capacity(cache_lock);
}

// Runs the queue sweep to completion.
void sweep_all(BlockFileCache& cache, int64_t now) {
    cache.start_queue_sweeps(now);
    std::lock_guard<std::mutex> cache_lock(cache._mutex);
    for (auto type : {FileCacheType::TTL, FileCacheType::INDEX, FileCacheType::NORMAL,
                      FileCacheType::DISPOSABLE}) {
        while (cache.sweep_queue(type, 1000, now, cache_lock) > 0) {
        }
    }
}

void open_cache(BlockFileCache& cache) {
    ASSERT_TRUE(cache.initialize());
    wait_until_cache_ready(cache);
}

bool is_cached(BlockFileCache& cache, const std::string& key, size_t index) {
    std::lock_guard<std::mutex> cache_lock(cache._mutex);
    return cache.get_cell(BlockFileCache::hash(key), index * kBlockSize, cache_lock) != nullptr;
}

} // namespace

TEST_F(BlockFileCacheTest, ttl_policy_decides_queue_on_write) {
    TtlPolicyTestGuard guard;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    BlockFileCache cache(cache_base_path, ttl_policy_test_settings());
    open_cache(cache);
    auto* registry = TabletCachePolicyRegistry::instance();
    const int64_t now = UnixSeconds();
    ReadStatistics stats;

    // Known and protected: the TTL queue, whatever deadline the reader stamped.
    registry->upsert(1, {3600, now});
    fill(cache, "protected", 2, context_of(1, FileCacheType::NORMAL, 0, &stats));
    // Known with no TTL: its class queue, even with a stale stamped deadline.
    registry->upsert(2, {0, now});
    fill(cache, "revoked", 2, context_of(2, FileCacheType::NORMAL, now + 3600, &stats));
    // Unknown: judged by the stamped deadline until its policy is known.
    fill(cache, "unknown", 2, context_of(3, FileCacheType::NORMAL, now + 3600, &stats));
    // Outside any tablet: no TTL to go by but the stamped one.
    fill(cache, "external", 2, context_of(0, FileCacheType::NORMAL, 0, &stats));

    EXPECT_EQ(block_of(cache, "protected", 0)->cache_type(), FileCacheType::TTL);
    EXPECT_EQ(block_of(cache, "revoked", 0)->cache_type(), FileCacheType::NORMAL);
    EXPECT_EQ(block_of(cache, "unknown", 0)->cache_type(), FileCacheType::TTL);
    EXPECT_EQ(block_of(cache, "external", 0)->cache_type(), FileCacheType::NORMAL);
    EXPECT_EQ(queue_size(cache, FileCacheType::TTL), 4 * kBlockSize);
    EXPECT_EQ(queue_size(cache, FileCacheType::NORMAL), 4 * kBlockSize);
}

// An index block of a TTL tablet stays an index block.
TEST_F(BlockFileCacheTest, ttl_policy_keeps_block_class) {
    TtlPolicyTestGuard guard;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    BlockFileCache cache(cache_base_path, ttl_policy_test_settings());
    open_cache(cache);
    auto* registry = TabletCachePolicyRegistry::instance();
    const int64_t now = UnixSeconds();
    ReadStatistics stats;

    registry->upsert(1, {3600, now});
    fill(cache, "index", 2, context_of(1, FileCacheType::INDEX, 0, &stats));
    fill(cache, "disposable", 1, context_of(1, FileCacheType::DISPOSABLE, 0, &stats));
    EXPECT_EQ(block_of(cache, "index", 0)->cache_type(), FileCacheType::TTL);
    EXPECT_EQ(block_of(cache, "index", 0)->cache_class(), FileCacheType::INDEX);

    // The TTL is removed: the blocks go back to the queue of their own class.
    registry->upsert(1, {0, now});
    sweep_all(cache, now);
    EXPECT_EQ(block_of(cache, "index", 0)->cache_type(), FileCacheType::INDEX);
    EXPECT_EQ(block_of(cache, "disposable", 0)->cache_type(), FileCacheType::DISPOSABLE);
    EXPECT_EQ(queue_size(cache, FileCacheType::TTL), 0);
    EXPECT_EQ(queue_size(cache, FileCacheType::INDEX), 2 * kBlockSize);
}

// The queues follow ALTER and expiry, in both directions.
TEST_F(BlockFileCacheTest, ttl_policy_sweep_follows_policy_changes) {
    TtlPolicyTestGuard guard;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    BlockFileCache cache(cache_base_path, ttl_policy_test_settings());
    open_cache(cache);
    auto* registry = TabletCachePolicyRegistry::instance();
    const int64_t now = UnixSeconds();
    ReadStatistics stats;

    registry->upsert(1, {0, now - 100});
    fill(cache, "t1", 3, context_of(1, FileCacheType::NORMAL, 0, &stats));
    EXPECT_EQ(queue_size(cache, FileCacheType::TTL), 0);

    // ALTER adds a TTL.
    registry->upsert(1, {3600, now - 100});
    sweep_all(cache, now);
    EXPECT_EQ(queue_size(cache, FileCacheType::TTL), 3 * kBlockSize);
    EXPECT_EQ(queue_size(cache, FileCacheType::NORMAL), 0);

    // Nothing changed: a sweep is not started at all.
    cache.start_queue_sweeps(now);
    EXPECT_FALSE(cache._queue_sweep.active[file_cache_type_index(FileCacheType::TTL)]);
    EXPECT_FALSE(cache._queue_sweep.active[file_cache_type_index(FileCacheType::NORMAL)]);

    // ALTER shortens it so that it has already run out.
    registry->upsert(1, {50, now - 100});
    sweep_all(cache, now);
    EXPECT_EQ(queue_size(cache, FileCacheType::TTL), 0);
    EXPECT_EQ(queue_size(cache, FileCacheType::NORMAL), 3 * kBlockSize);

    // Extended again after it expired.
    registry->upsert(1, {3600, now - 100});
    sweep_all(cache, now);
    EXPECT_EQ(queue_size(cache, FileCacheType::TTL), 3 * kBlockSize);

    // The deadline passes without any change to the policy.
    sweep_all(cache, now + 3600);
    EXPECT_EQ(queue_size(cache, FileCacheType::TTL), 0);
    EXPECT_EQ(queue_size(cache, FileCacheType::NORMAL), 3 * kBlockSize);
}

// A block whose tablet gained a TTL is never evicted as unprotected data, even before the sweep
// moved it; the eviction scan moves it to the TTL queue instead.
TEST_F(BlockFileCacheTest, ttl_policy_eviction_skips_and_relocates_protected_blocks) {
    TtlPolicyTestGuard guard;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    BlockFileCache cache(cache_base_path, ttl_policy_test_settings());
    open_cache(cache);
    auto* registry = TabletCachePolicyRegistry::instance();
    const int64_t now = UnixSeconds();
    ReadStatistics stats;

    // The coldest blocks belong to tablet 1, which has no TTL yet.
    registry->upsert(1, {0, now});
    fill(cache, "t1", 3, context_of(1, FileCacheType::NORMAL, 0, &stats));
    fill(cache, "t2", 3, context_of(2, FileCacheType::NORMAL, 0, &stats));
    registry->upsert(1, {3600, now});

    std::vector<FileBlockCell*> to_evict;
    {
        std::lock_guard<std::mutex> cache_lock(cache._mutex);
        size_t removed_size = 0;
        size_t cur_removed_size = 0;
        cache.find_evict_candidates(FileCacheType::NORMAL, 2 * kBlockSize, cache._cur_cache_size,
                                    removed_size, to_evict, cache_lock, cur_removed_size, true);
        ASSERT_EQ(removed_size, 2 * kBlockSize);
    }
    ASSERT_EQ(to_evict.size(), 2);
    for (auto* cell : to_evict) {
        EXPECT_EQ(cell->file_block->tablet_id(), 2);
    }
    EXPECT_EQ(block_of(cache, "t1", 0)->cache_type(), FileCacheType::TTL);
    EXPECT_EQ(queue_size(cache, FileCacheType::TTL), 3 * kBlockSize);
}

// What is persisted is the class and the deadline the block was written with; a restart puts
// the blocks in the queues their tablets' policies call for, using the policies this path kept.
TEST_F(BlockFileCacheTest, ttl_policy_restart_recomputes_queues) {
    TtlPolicyTestGuard guard;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    auto* registry = TabletCachePolicyRegistry::instance();
    const int64_t now = UnixSeconds();
    ReadStatistics stats;
    {
        BlockFileCache cache(cache_base_path, ttl_policy_test_settings());
        open_cache(cache);
        registry->upsert(1, {3600, now});
        registry->upsert(2, {3600, now});
        fill(cache, "t1", 2, context_of(1, FileCacheType::INDEX, now + 3600, &stats));
        fill(cache, "t2", 2, context_of(2, FileCacheType::NORMAL, now + 3600, &stats));
        // The TTL of tablet 2 is removed while the BE is up.
        registry->upsert(2, {0, now});

        auto* fs_storage = dynamic_cast<FSFileCacheStorage*>(cache._storage.get());
        ASSERT_NE(fs_storage, nullptr);
        auto meta =
                fs_storage->get_meta_store()->get(BlockMetaKey(1, BlockFileCache::hash("t1"), 0));
        for (int i = 0; i < 100 && !meta.has_value(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            meta = fs_storage->get_meta_store()->get(
                    BlockMetaKey(1, BlockFileCache::hash("t1"), 0));
        }
        ASSERT_TRUE(meta.has_value());
        EXPECT_EQ(meta->type, FileCacheType::INDEX);
        EXPECT_EQ(meta->ttl, now + 3600);
    }

    // The restart only knows what the cache path kept.
    registry->clear();
    BlockFileCache cache(cache_base_path, ttl_policy_test_settings());
    open_cache(cache);
    EXPECT_EQ(registry->deadline(1), now + 3600);
    EXPECT_EQ(registry->deadline(2), 0);

    auto t1 = block_of(cache, "t1", 0);
    EXPECT_EQ(t1->cache_type(), FileCacheType::TTL);
    EXPECT_EQ(t1->cache_class(), FileCacheType::INDEX);
    EXPECT_EQ(t1->tablet_id(), 1);
    EXPECT_EQ(block_of(cache, "t2", 0)->cache_type(), FileCacheType::NORMAL);

    // Tablet 2 gets a TTL again while blocks wait in its class queue.
    registry->upsert(2, {3600, now});
    sweep_all(cache, now);
    EXPECT_EQ(block_of(cache, "t2", 0)->cache_type(), FileCacheType::TTL);
}

// The TTL queue is dumped and restored like the other queues. A restored block only gets its
// tablet and class when its metadata is loaded, and then goes where its tablet's policy says.
TEST_F(BlockFileCacheTest, ttl_policy_restored_ttl_queue_follows_policy) {
    TtlPolicyTestGuard guard;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    const auto original_tail_record_num = config::file_cache_background_lru_dump_tail_record_num;
    Defer restore_tail_record_num {[&] {
        config::file_cache_background_lru_dump_tail_record_num = original_tail_record_num;
    }};
    config::file_cache_background_lru_dump_tail_record_num = 1000;
    auto* registry = TabletCachePolicyRegistry::instance();
    const int64_t now = UnixSeconds();
    ReadStatistics stats;
    {
        BlockFileCache cache(cache_base_path, ttl_policy_test_settings());
        open_cache(cache);
        registry->upsert(1, {3600, now});
        registry->upsert(2, {3600, now});
        fill(cache, "t1", 2, context_of(1, FileCacheType::INDEX, 0, &stats));
        fill(cache, "t2", 2, context_of(2, FileCacheType::NORMAL, 0, &stats));
        cache.replay_lru_logs_once();
        cache.dump_lru_queues(true);
        EXPECT_TRUE(fs::exists(cache_base_path + "/lru_dump_ttl.tail"));
        // Tablet 2 loses its TTL before the restart.
        registry->upsert(2, {0, now});
    }

    registry->clear();
    BlockFileCache cache(cache_base_path, ttl_policy_test_settings());
    open_cache(cache);
    auto t1 = block_of(cache, "t1", 0);
    EXPECT_EQ(t1->cache_type(), FileCacheType::TTL);
    EXPECT_EQ(t1->cache_class(), FileCacheType::INDEX);
    EXPECT_EQ(t1->tablet_id(), 1);
    auto t2 = block_of(cache, "t2", 0);
    EXPECT_EQ(t2->cache_type(), FileCacheType::NORMAL);
    EXPECT_EQ(t2->tablet_id(), 2);
}

// A cache path keeps count of the tablets it holds blocks of: only their policies are fetched.
TEST_F(BlockFileCacheTest, ttl_policy_counts_cached_tablets) {
    TtlPolicyTestGuard guard;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    BlockFileCache cache(cache_base_path, ttl_policy_test_settings());
    open_cache(cache);
    ReadStatistics stats;
    fill(cache, "t1", 3, context_of(1, FileCacheType::NORMAL, 0, &stats));
    fill(cache, "t2", 2, context_of(2, FileCacheType::INDEX, 0, &stats));
    fill(cache, "external", 2, context_of(0, FileCacheType::NORMAL, 0, &stats));
    {
        std::lock_guard<std::mutex> cache_lock(cache._mutex);
        EXPECT_EQ(cache._cached_tablets.size(), 2);
        EXPECT_EQ(cache._cached_tablets[1], 3);
        EXPECT_EQ(cache._cached_tablets[2], 2);
    }
    cache.remove_if_cached(BlockFileCache::hash("t1"));
    std::lock_guard<std::mutex> cache_lock(cache._mutex);
    EXPECT_FALSE(cache._cached_tablets.contains(1));
    EXPECT_EQ(cache._cached_tablets[2], 2);
}

// Which queue a block is in can be derived again at any time, so losing it costs nothing: after
// every block is put in a wrong queue, one pass of the sweep puts each back where it belongs.
TEST_F(BlockFileCacheTest, ttl_policy_sweep_repairs_shuffled_queues) {
    TtlPolicyTestGuard guard;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    BlockFileCache cache(cache_base_path, ttl_policy_test_settings());
    open_cache(cache);
    auto* registry = TabletCachePolicyRegistry::instance();
    const int64_t now = UnixSeconds();
    ReadStatistics stats;

    registry->upsert(1, {3600, now});
    registry->upsert(2, {0, now});
    fill(cache, "t1_index", 2, context_of(1, FileCacheType::INDEX, 0, &stats));
    fill(cache, "t1_normal", 2, context_of(1, FileCacheType::NORMAL, 0, &stats));
    fill(cache, "t2_disposable", 2, context_of(2, FileCacheType::DISPOSABLE, 0, &stats));
    fill(cache, "t2_index", 2, context_of(2, FileCacheType::INDEX, 0, &stats));
    fill(cache, "external", 2, context_of(0, FileCacheType::NORMAL, 0, &stats));
    const std::map<std::string, FileCacheType> expected {
            {"t1_index", FileCacheType::TTL},
            {"t1_normal", FileCacheType::TTL},
            {"t2_disposable", FileCacheType::DISPOSABLE},
            {"t2_index", FileCacheType::INDEX},
            {"external", FileCacheType::NORMAL}};
    // The policy changes are swept, so nothing but the daily pass would look at the queues again.
    sweep_all(cache, now);

    {
        constexpr std::array<FileCacheType, 4> types {FileCacheType::TTL, FileCacheType::INDEX,
                                                      FileCacheType::NORMAL,
                                                      FileCacheType::DISPOSABLE};
        std::lock_guard<std::mutex> cache_lock(cache._mutex);
        size_t next = 0;
        for (const auto& [key, want] : expected) {
            for (size_t index = 0; index < 2; ++index) {
                auto* cell =
                        cache.get_cell(BlockFileCache::hash(key), index * kBlockSize, cache_lock);
                ASSERT_EQ(cell->file_block->cache_type(), want) << key;
                FileCacheType wrong = types[next++ % types.size()];
                if (wrong == want) {
                    wrong = types[next++ % types.size()];
                }
                cache.move_cell_to_queue(*cell, wrong, cache_lock);
            }
        }
    }

    cache._queue_sweep.last_backstop_at = 0;
    sweep_all(cache, now);
    for (const auto& [key, want] : expected) {
        for (size_t index = 0; index < 2; ++index) {
            EXPECT_EQ(block_of(cache, key, index)->cache_type(), want) << key << " " << index;
        }
    }
    EXPECT_EQ(queue_size(cache, FileCacheType::TTL), 4 * kBlockSize);
    EXPECT_EQ(queue_size(cache, FileCacheType::INDEX), 2 * kBlockSize);
    EXPECT_EQ(queue_size(cache, FileCacheType::NORMAL), 2 * kBlockSize);
    EXPECT_EQ(queue_size(cache, FileCacheType::DISPOSABLE), 2 * kBlockSize);
}

// Protection is a budget, not a guarantee: once protected data outgrows the TTL queue's share, a
// write that needs the space evicts the coldest protected block, and every such byte is counted.
// A block whose protection already ended is evicted first and is not counted.
TEST_F(BlockFileCacheTest, ttl_policy_counts_protected_bytes_evicted_over_budget) {
    TtlPolicyTestGuard guard;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    BlockFileCache cache(cache_base_path, ttl_policy_test_settings());
    open_cache(cache);
    auto* registry = TabletCachePolicyRegistry::instance();
    const int64_t now = UnixSeconds();
    ReadStatistics stats;

    registry->upsert(1, {3600, now});
    registry->upsert(2, {3600, now});
    registry->upsert(3, {0, now});
    // The TTL queue holds exactly its share, the class queues fill the rest of the cache.
    fill(cache, "revoked", 1, context_of(2, FileCacheType::NORMAL, 0, &stats));
    fill(cache, "t1", 49, context_of(1, FileCacheType::NORMAL, 0, &stats));
    fill(cache, "normal", 30, context_of(3, FileCacheType::NORMAL, 0, &stats));
    fill(cache, "index", 10, context_of(3, FileCacheType::INDEX, 0, &stats));
    fill(cache, "disposable", 10, context_of(3, FileCacheType::DISPOSABLE, 0, &stats));
    ASSERT_EQ(cache._cur_cache_size, 10000000);
    // Tablet 2 loses its TTL; its block is still in the TTL queue, the coldest one there.
    registry->upsert(2, {0, now});

    const size_t evicted_before = cache._ttl_protected_evicted_bytes->get_value();
    fill(cache, "t1_more", 2, context_of(1, FileCacheType::NORMAL, 0, &stats));
    EXPECT_EQ(cache._ttl_protected_evicted_bytes->get_value() - evicted_before, kBlockSize);
    EXPECT_FALSE(is_cached(cache, "revoked", 0));
    EXPECT_FALSE(is_cached(cache, "t1", 0));
    EXPECT_TRUE(is_cached(cache, "t1", 1));
    EXPECT_EQ(queue_size(cache, FileCacheType::TTL), 5000000);
    EXPECT_EQ(queue_size(cache, FileCacheType::NORMAL), 3000000);
    EXPECT_EQ(queue_size(cache, FileCacheType::INDEX), 1000000);
    EXPECT_EQ(queue_size(cache, FileCacheType::DISPOSABLE), 1000000);
}

// Evicting in advance only frees unprotected data, even when protected data has grown past the
// TTL queue's share.
TEST_F(BlockFileCacheTest, ttl_policy_evict_in_advance_leaves_protected_blocks) {
    TtlPolicyTestGuard guard;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    BlockFileCache cache(cache_base_path, ttl_policy_test_settings());
    open_cache(cache);
    auto* registry = TabletCachePolicyRegistry::instance();
    const int64_t now = UnixSeconds();
    ReadStatistics stats;

    registry->upsert(1, {3600, now});
    registry->upsert(2, {0, now});
    fill(cache, "t1", 60, context_of(1, FileCacheType::NORMAL, 0, &stats));
    fill(cache, "t2", 30, context_of(2, FileCacheType::NORMAL, 0, &stats));
    ASSERT_EQ(queue_size(cache, FileCacheType::TTL), 6000000);

    {
        std::lock_guard<std::mutex> cache_lock(cache._mutex);
        cache.try_evict_in_advance(1000000, cache_lock);
    }
    EXPECT_EQ(queue_size(cache, FileCacheType::TTL), 6000000);
    EXPECT_EQ(queue_size(cache, FileCacheType::NORMAL), 2000000);
}

// The first start of this version has no policies kept: a block is judged by the deadline it
// was written with, as every block was before, until its tablet's policy is fetched. Data that
// never recorded its tablet (v2 cache files) only ever has the written deadline.
TEST_F(BlockFileCacheTest, ttl_policy_first_start_judges_by_written_deadlines) {
    TtlPolicyTestGuard guard;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    auto* registry = TabletCachePolicyRegistry::instance();
    const int64_t now = UnixSeconds();
    ReadStatistics stats;
    {
        BlockFileCache cache(cache_base_path, ttl_policy_test_settings());
        open_cache(cache);
        fill(cache, "t1", 2, context_of(1, FileCacheType::INDEX, now + 3600, &stats));
        fill(cache, "legacy", 2, context_of(0, FileCacheType::NORMAL, now + 1800, &stats));
        fill(cache, "t2", 2, context_of(2, FileCacheType::NORMAL, 0, &stats));
        auto* fs_storage = dynamic_cast<FSFileCacheStorage*>(cache._storage.get());
        ASSERT_NE(fs_storage, nullptr);
        for (int i = 0;
             i < 100 && !fs_storage->get_meta_store()
                                 ->get(BlockMetaKey(2, BlockFileCache::hash("t2"), kBlockSize))
                                 .has_value();
             ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    registry->clear();
    BlockFileCache cache(cache_base_path, ttl_policy_test_settings());
    open_cache(cache);
    EXPECT_EQ(registry->size(), 0);
    EXPECT_EQ(block_of(cache, "t1", 0)->cache_type(), FileCacheType::TTL);
    EXPECT_EQ(block_of(cache, "legacy", 0)->cache_type(), FileCacheType::TTL);
    EXPECT_EQ(block_of(cache, "t2", 0)->cache_type(), FileCacheType::NORMAL);
    sweep_all(cache, now);
    EXPECT_EQ(cache._ttl_legacy_v2_protected_bytes->get_value(), 2 * kBlockSize);

    // The tablets the cache ran into get their policies fetched: tablet 1 no longer has a TTL.
    registry->set_fetcher([&](int64_t tablet_id, TabletCachePolicy* policy) {
        *policy = {tablet_id == 2 ? 3600 : 0, now};
        return Status::OK();
    });
    registry->refresh(10);
    EXPECT_EQ(registry->deadline(1), 0);
    EXPECT_EQ(registry->deadline(2), now + 3600);
    sweep_all(cache, now);
    EXPECT_EQ(block_of(cache, "t1", 0)->cache_type(), FileCacheType::INDEX);
    EXPECT_EQ(block_of(cache, "t2", 0)->cache_type(), FileCacheType::TTL);
    EXPECT_EQ(block_of(cache, "legacy", 0)->cache_type(), FileCacheType::TTL);

    // No policy change tells when the written deadline passes: a pass starts once it has.
    sweep_all(cache, now + 1800 - 1);
    EXPECT_EQ(block_of(cache, "legacy", 0)->cache_type(), FileCacheType::TTL);
    sweep_all(cache, now + 1800 + 1);
    EXPECT_EQ(block_of(cache, "legacy", 0)->cache_type(), FileCacheType::NORMAL);
    EXPECT_EQ(block_of(cache, "t2", 0)->cache_type(), FileCacheType::TTL);
    EXPECT_EQ(cache._ttl_legacy_v2_protected_bytes->get_value(), 0);
}

// Blocks touched or added during a pass go to the hot end of the queue, behind the cursor's way;
// however busy the queue, they cannot keep the pass from ending.
TEST_F(BlockFileCacheTest, ttl_policy_sweep_pass_ends_on_a_busy_queue) {
    TtlPolicyTestGuard guard;
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    BlockFileCache cache(cache_base_path, ttl_policy_test_settings());
    open_cache(cache);
    auto* registry = TabletCachePolicyRegistry::instance();
    const int64_t now = UnixSeconds();
    ReadStatistics stats;

    registry->upsert(1, {3600, now});
    fill(cache, "t1", 10, context_of(1, FileCacheType::NORMAL, 0, &stats));
    cache._queue_sweep.last_backstop_at = 0;
    cache.start_queue_sweeps(now);
    auto& active = cache._queue_sweep.active[file_cache_type_index(FileCacheType::TTL)];
    ASSERT_TRUE(active);

    std::lock_guard<std::mutex> cache_lock(cache._mutex);
    size_t walked = 0;
    for (int round = 0; round < 100 && active; ++round) {
        walked += cache.sweep_queue(FileCacheType::TTL, 3, now, cache_lock);
        // Every block is read again between two rounds.
        for (size_t i = 0; i < 10; ++i) {
            auto* cell = cache.get_cell(BlockFileCache::hash("t1"), i * kBlockSize, cache_lock);
            cache._ttl_queue.move_to_end(*cell->queue_iterator, cache_lock);
        }
    }
    EXPECT_FALSE(active);
    EXPECT_EQ(walked, 10);
}

TEST_F(BlockFileCacheTest, ttl_policy_meta_store_keeps_tablet_policies) {
    if (fs::exists(cache_base_path)) {
        fs::remove_all(cache_base_path);
    }
    fs::create_directories(cache_base_path);
    const std::string db_path = cache_base_path + "/meta";
    {
        CacheBlockMetaStore store(db_path);
        store.put_tablet_policy(1, {3600, 1000});
        store.put_tablet_policy(2, {0, 1000});
        store.put_tablet_policy(3, {60, 1000});
        store.delete_tablet_policy(3);
        // Block metadata and tablet policies do not mix.
        store.put(BlockMetaKey(1, BlockFileCache::hash("key"), 0),
                  BlockMeta(FileCacheType::NORMAL, 100, 0));
    }
    CacheBlockMetaStore store(db_path);
    std::map<int64_t, TabletCachePolicy> policies;
    store.for_each_tablet_policy([&](int64_t tablet_id, const TabletCachePolicy& policy) {
        policies.emplace(tablet_id, policy);
    });
    ASSERT_EQ(policies.size(), 2);
    EXPECT_EQ(policies[1], (TabletCachePolicy {3600, 1000}));
    EXPECT_EQ(policies[2], (TabletCachePolicy {0, 1000}));
    size_t blocks = 0;
    for (auto it = store.get_all(); it->valid(); it->next()) {
        it->key();
        EXPECT_TRUE(it->get_last_key_error().ok());
        ++blocks;
    }
    EXPECT_EQ(blocks, 1);
}

} // namespace doris::io
