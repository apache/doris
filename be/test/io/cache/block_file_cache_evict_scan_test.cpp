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

// Eviction scans under the cache lock: in-use entries an eviction skips must not be examined
// again by the next reservation, and a scan must stop at file_cache_evict_max_scan_entries.

#include <bvar/bvar.h>

#include <chrono>
#include <vector>

#include "io/cache/block_file_cache_test_common.h"
#include "util/defer_op.h"

namespace doris::io {

extern bvar::Adder<uint64_t> g_file_cache_evict_scanned_entries;
extern bvar::Adder<uint64_t> g_file_cache_evict_rotated_in_use;
extern bvar::Adder<uint64_t> g_file_cache_evict_scan_truncated;

namespace {

constexpr size_t kBlockSize = 10;
constexpr size_t kBlocks = 10;

FileCacheSettings normal_only_settings() {
    FileCacheSettings settings;
    settings.index_queue_elements = 0;
    settings.index_queue_size = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.ttl_queue_size = 0;
    settings.ttl_queue_elements = 0;
    settings.query_queue_size = kBlocks * kBlockSize;
    settings.query_queue_elements = kBlocks + 1;
    settings.max_file_block_size = kBlockSize;
    settings.max_query_cache_size = kBlocks * kBlockSize;
    settings.capacity = kBlocks * kBlockSize;
    return settings;
}

// Five blocks each for the disposable and normal queues; a normal reservation into the full cache
// first tries to take cold disposable blocks.
FileCacheSettings disposable_and_normal_settings() {
    FileCacheSettings settings = normal_only_settings();
    settings.disposable_queue_size = 5 * kBlockSize;
    settings.disposable_queue_elements = 6;
    settings.query_queue_size = 5 * kBlockSize;
    settings.query_queue_elements = 6;
    return settings;
}

// Inserts one block of `hash` at `offset`, downloads it and returns its holder.
FileBlocksHolder insert_block(BlockFileCache& cache, const UInt128Wrapper& hash, size_t offset,
                              FileCacheType type = FileCacheType::NORMAL) {
    CacheContext context;
    ReadStatistics rstats;
    context.stats = &rstats;
    context.cache_type = type;
    auto holder = cache.get_or_set(hash, offset, kBlockSize, context);
    for (auto& block : holder.file_blocks) {
        if (block->state() == FileBlock::State::EMPTY &&
            block->get_or_set_downloader() == FileBlock::get_caller_id()) {
            std::string data(block->range().size(), '0');
            EXPECT_TRUE(block->append(Slice(data.data(), data.size())).ok());
            EXPECT_TRUE(block->finalize().ok());
        }
    }
    return holder;
}

class EvictScanConfigGuard {
public:
    EvictScanConfigGuard(int64_t max_scan, bool rotate)
            : _max_scan(config::file_cache_evict_max_scan_entries),
              _rotate(config::enable_file_cache_evict_rotate_in_use),
              _in_advance(config::enable_evict_file_cache_in_advance) {
        config::file_cache_evict_max_scan_entries = max_scan;
        config::enable_file_cache_evict_rotate_in_use = rotate;
        config::enable_evict_file_cache_in_advance = false;
    }
    ~EvictScanConfigGuard() {
        config::file_cache_evict_max_scan_entries = _max_scan;
        config::enable_file_cache_evict_rotate_in_use = _rotate;
        config::enable_evict_file_cache_in_advance = _in_advance;
    }

private:
    int64_t _max_scan;
    bool _rotate;
    bool _in_advance;
};

// Fills the cache: the first `held` blocks of `key` stay held (in use, at the LRU head), the
// rest are released.
std::vector<FileBlocksHolder> fill_with_held_head(BlockFileCache& cache, const UInt128Wrapper& key,
                                                  size_t held) {
    std::vector<FileBlocksHolder> holders;
    for (size_t i = 0; i < kBlocks; ++i) {
        auto holder = insert_block(cache, key, i * kBlockSize);
        if (i < held) {
            holders.push_back(std::move(holder));
        }
    }
    return holders;
}

} // namespace

TEST_F(BlockFileCacheTest, evict_scan_rotates_in_use_entries) {
    EvictScanConfigGuard guard(/*max_scan=*/0, /*rotate=*/true);
    auto base = (caches_dir / "evict_scan_rotate" / "").string();
    fs::remove_all(base);
    fs::create_directories(base);
    Defer cleanup {[&] { fs::remove_all(base); }};
    BlockFileCache cache(base, normal_only_settings());
    ASSERT_TRUE(cache.initialize());
    wait_until_cache_ready(cache);

    auto key = BlockFileCache::hash("evict_scan_rotate_key");
    auto held = fill_with_held_head(cache, key, /*held=*/5);

    // The first eviction skips the 5 held blocks and evicts block 5.
    auto scanned_before = g_file_cache_evict_scanned_entries.get_value();
    auto rotated_before = g_file_cache_evict_rotated_in_use.get_value();
    auto other1 = insert_block(cache, BlockFileCache::hash("evict_scan_rotate_other1"), 0);
    EXPECT_EQ(g_file_cache_evict_scanned_entries.get_value() - scanned_before, 6);
    EXPECT_EQ(g_file_cache_evict_rotated_in_use.get_value() - rotated_before, 5);
    auto blocks = cache.get_blocks_by_key(key);
    EXPECT_FALSE(blocks.contains(5 * kBlockSize));
    for (size_t i = 0; i < 5; ++i) {
        EXPECT_TRUE(blocks.contains(i * kBlockSize)) << i;
    }

    // The held blocks were moved behind block 6, so the next eviction examines one entry.
    scanned_before = g_file_cache_evict_scanned_entries.get_value();
    rotated_before = g_file_cache_evict_rotated_in_use.get_value();
    auto other2 = insert_block(cache, BlockFileCache::hash("evict_scan_rotate_other2"), 0);
    EXPECT_EQ(g_file_cache_evict_scanned_entries.get_value() - scanned_before, 1);
    EXPECT_EQ(g_file_cache_evict_rotated_in_use.get_value() - rotated_before, 0);
    EXPECT_FALSE(cache.get_blocks_by_key(key).contains(6 * kBlockSize));
}

TEST_F(BlockFileCacheTest, evict_scan_without_rotation_rescans_in_use_entries) {
    EvictScanConfigGuard guard(/*max_scan=*/0, /*rotate=*/false);
    auto base = (caches_dir / "evict_scan_no_rotate" / "").string();
    fs::remove_all(base);
    fs::create_directories(base);
    Defer cleanup {[&] { fs::remove_all(base); }};
    BlockFileCache cache(base, normal_only_settings());
    ASSERT_TRUE(cache.initialize());
    wait_until_cache_ready(cache);

    auto key = BlockFileCache::hash("evict_scan_no_rotate_key");
    auto held = fill_with_held_head(cache, key, /*held=*/5);
    auto other1 = insert_block(cache, BlockFileCache::hash("evict_scan_no_rotate_other1"), 0);

    auto scanned_before = g_file_cache_evict_scanned_entries.get_value();
    auto other2 = insert_block(cache, BlockFileCache::hash("evict_scan_no_rotate_other2"), 0);
    EXPECT_EQ(g_file_cache_evict_scanned_entries.get_value() - scanned_before, 6);
}

TEST_F(BlockFileCacheTest, evict_scan_stops_at_bound_and_skips_cache) {
    EvictScanConfigGuard guard(/*max_scan=*/3, /*rotate=*/true);
    auto base = (caches_dir / "evict_scan_bound" / "").string();
    fs::remove_all(base);
    fs::create_directories(base);
    Defer cleanup {[&] { fs::remove_all(base); }};
    BlockFileCache cache(base, normal_only_settings());
    ASSERT_TRUE(cache.initialize());
    wait_until_cache_ready(cache);

    auto key = BlockFileCache::hash("evict_scan_bound_key");
    auto held = fill_with_held_head(cache, key, /*held=*/5);

    auto truncated_before = g_file_cache_evict_scan_truncated.get_value();
    auto scanned_before = g_file_cache_evict_scanned_entries.get_value();
    auto other = insert_block(cache, BlockFileCache::hash("evict_scan_bound_other"), 0);
    EXPECT_GE(g_file_cache_evict_scan_truncated.get_value() - truncated_before, 1);
    // No single scan examines more than the bound.
    auto scanned = g_file_cache_evict_scanned_entries.get_value() - scanned_before;
    auto scans = g_file_cache_evict_scan_truncated.get_value() - truncated_before;
    EXPECT_LE(scanned, scans * 3);
    ASSERT_EQ(other.file_blocks.size(), 1);
    EXPECT_EQ(other.file_blocks.front()->state(), FileBlock::State::SKIP_CACHE);
    EXPECT_EQ(cache.get_blocks_by_key(key).size(), kBlocks);
}

TEST_F(BlockFileCacheTest, evict_scan_by_time_rotates_in_use_entries) {
    EvictScanConfigGuard guard(/*max_scan=*/0, /*rotate=*/true);
    auto base = (caches_dir / "evict_scan_by_time" / "").string();
    fs::remove_all(base);
    fs::create_directories(base);
    Defer cleanup {[&] { fs::remove_all(base); }};
    BlockFileCache cache(base, disposable_and_normal_settings());
    ASSERT_TRUE(cache.initialize());
    wait_until_cache_ready(cache);

    // The first 3 disposable blocks stay held; all 5 are older than the queue's hot interval.
    auto disposable_key = BlockFileCache::hash("evict_scan_by_time_disposable");
    std::vector<FileBlocksHolder> held;
    for (size_t i = 0; i < 5; ++i) {
        auto holder =
                insert_block(cache, disposable_key, i * kBlockSize, FileCacheType::DISPOSABLE);
        if (i < 3) {
            held.push_back(std::move(holder));
        }
    }
    auto normal_key = BlockFileCache::hash("evict_scan_by_time_normal");
    for (size_t i = 0; i < 5; ++i) {
        insert_block(cache, normal_key, i * kBlockSize);
    }
    {
        std::lock_guard cache_lock(cache._mutex);
        const int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                                    std::chrono::steady_clock::now().time_since_epoch())
                                    .count();
        for (auto& [offset, cell] : cache._files.at(disposable_key)) {
            cell.atime = now - 2 * 60 * 60;
        }
    }

    // The time-interval scan of the disposable queue skips the 3 held blocks and evicts block 3.
    auto scanned_before = g_file_cache_evict_scanned_entries.get_value();
    auto rotated_before = g_file_cache_evict_rotated_in_use.get_value();
    auto other1 = insert_block(cache, BlockFileCache::hash("evict_scan_by_time_other1"), 0);
    EXPECT_EQ(g_file_cache_evict_scanned_entries.get_value() - scanned_before, 4);
    EXPECT_EQ(g_file_cache_evict_rotated_in_use.get_value() - rotated_before, 3);
    auto blocks = cache.get_blocks_by_key(disposable_key);
    EXPECT_FALSE(blocks.contains(3 * kBlockSize));
    EXPECT_EQ(cache.get_blocks_by_key(normal_key).size(), 5);

    // The held blocks were moved behind block 4, so the next eviction examines one entry.
    scanned_before = g_file_cache_evict_scanned_entries.get_value();
    rotated_before = g_file_cache_evict_rotated_in_use.get_value();
    auto other2 = insert_block(cache, BlockFileCache::hash("evict_scan_by_time_other2"), 0);
    EXPECT_EQ(g_file_cache_evict_scanned_entries.get_value() - scanned_before, 1);
    EXPECT_EQ(g_file_cache_evict_rotated_in_use.get_value() - rotated_before, 0);
    blocks = cache.get_blocks_by_key(disposable_key);
    EXPECT_FALSE(blocks.contains(4 * kBlockSize));
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_TRUE(blocks.contains(i * kBlockSize)) << i;
    }
}

} // namespace doris::io
