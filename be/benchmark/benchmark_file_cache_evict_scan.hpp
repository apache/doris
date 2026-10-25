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

#pragma once

#include <benchmark/benchmark.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/file_cache_common.h"

namespace doris {

// One reservation into a full file cache whose LRU head is held (in use), as when concurrent
// reads and write-through keep many blocks pinned. Each iteration inserts a new block through
// get_or_set, which evicts one released block under the cache lock, downloads it and releases
// it, so the queue keeps its shape: `held` pinned blocks plus kEvictFileCacheReleased released
// ones.
//
// Arg 0: number of held blocks. Arg 1: mode.
//   0 before:      no scan bound, no rotation (the code path before the fix)
//   1 cap only:    file_cache_evict_max_scan_entries = 4096, no rotation
//   2 rotate only: no scan bound, rotation on
//   3 after:       scan bound 4096 and rotation on (the defaults)
// Counters per reservation: scanned (LRU entries examined under the lock), truncated (scans
// that hit the bound) and skip_cache (reservations that failed, so the block was not cached).

constexpr size_t kEvictFileCacheBlock = 1024;
constexpr size_t kEvictFileCacheReleased = 1024;

class EvictFileCacheBench {
public:
    EvictFileCacheBench(size_t held, int64_t max_scan, bool rotate)
            : _max_scan(config::file_cache_evict_max_scan_entries),
              _rotate(config::enable_file_cache_evict_rotate_in_use),
              _in_advance(config::enable_evict_file_cache_in_advance),
              _async_touch(config::enable_file_cache_async_touch_on_get_or_set) {
        config::file_cache_evict_max_scan_entries = max_scan;
        config::enable_file_cache_evict_rotate_in_use = rotate;
        config::enable_evict_file_cache_in_advance = false;
        config::enable_file_cache_async_touch_on_get_or_set = false;

        const size_t blocks = held + kEvictFileCacheReleased;
        io::FileCacheSettings settings;
        settings.storage = "memory";
        settings.capacity = blocks * kEvictFileCacheBlock;
        settings.max_file_block_size = kEvictFileCacheBlock;
        settings.max_query_cache_size = 0;
        settings.query_queue_size = settings.capacity;
        settings.query_queue_elements = blocks + 1;
        settings.index_queue_size = 0;
        settings.index_queue_elements = 0;
        settings.disposable_queue_size = 0;
        settings.disposable_queue_elements = 0;
        settings.ttl_queue_size = 0;
        settings.ttl_queue_elements = 0;
        _cache = std::make_unique<io::BlockFileCache>("memory", settings);
        CHECK(_cache->initialize().ok());
        while (!_cache->get_async_open_success()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        // Held blocks first, so they sit at the LRU head.
        for (size_t i = 0; i < held; ++i) {
            _held.push_back(insert(io::BlockFileCache::hash("held_" + std::to_string(i))));
        }
        for (size_t i = 0; i < kEvictFileCacheReleased; ++i) {
            insert(io::BlockFileCache::hash("released_" + std::to_string(i)));
        }
    }

    ~EvictFileCacheBench() {
        _held.clear();
        _cache.reset();
        config::file_cache_evict_max_scan_entries = _max_scan;
        config::enable_file_cache_evict_rotate_in_use = _rotate;
        config::enable_evict_file_cache_in_advance = _in_advance;
        config::enable_file_cache_async_touch_on_get_or_set = _async_touch;
    }

    // Reserves, downloads and releases one new block. Returns false if the reservation failed.
    bool insert_one() {
        auto holder = insert(io::BlockFileCache::hash("new_" + std::to_string(_next++)));
        CHECK_EQ(holder.file_blocks.size(), 1);
        return holder.file_blocks.front()->state() != io::FileBlock::State::SKIP_CACHE;
    }

    uint64_t scanned() const { return _cache->_evict_scanned_entries_metrics->get_value(); }
    uint64_t truncated() const { return _cache->_evict_scan_truncated_metrics->get_value(); }

private:
    io::FileBlocksHolder insert(const io::UInt128Wrapper& hash) {
        io::CacheContext context;
        io::ReadStatistics stats;
        context.stats = &stats;
        context.cache_type = io::FileCacheType::NORMAL;
        auto holder = _cache->get_or_set(hash, 0, kEvictFileCacheBlock, context);
        for (auto& block : holder.file_blocks) {
            if (block->state() == io::FileBlock::State::EMPTY &&
                block->get_or_set_downloader() == io::FileBlock::get_caller_id()) {
                CHECK(block->append(Slice(_payload.data(), _payload.size())).ok());
                CHECK(block->finalize().ok());
            }
        }
        return holder;
    }

    int64_t _max_scan;
    bool _rotate;
    bool _in_advance;
    bool _async_touch;
    std::string _payload = std::string(kEvictFileCacheBlock, 'x');
    std::unique_ptr<io::BlockFileCache> _cache;
    std::vector<io::FileBlocksHolder> _held;
    size_t _next = 0;
};

static void BM_FileCacheReserveWithHeldHead(benchmark::State& state) {
    const auto held = static_cast<size_t>(state.range(0));
    const auto mode = state.range(1);
    const int64_t max_scan = (mode == 1 || mode == 3) ? 4096 : 0;
    const bool rotate = mode >= 2;
    EvictFileCacheBench bench(held, max_scan, rotate);

    const uint64_t scanned_before = bench.scanned();
    const uint64_t truncated_before = bench.truncated();
    int64_t skipped = 0;
    for (auto _ : state) {
        skipped += bench.insert_one() ? 0 : 1;
    }
    const auto per_op = benchmark::Counter::kAvgIterations;
    state.counters["scanned"] =
            benchmark::Counter(static_cast<double>(bench.scanned() - scanned_before), per_op);
    state.counters["truncated"] =
            benchmark::Counter(static_cast<double>(bench.truncated() - truncated_before), per_op);
    state.counters["skip_cache"] = benchmark::Counter(static_cast<double>(skipped), per_op);
    state.SetLabel(mode == 0   ? "before"
                   : mode == 1 ? "cap_only"
                   : mode == 2 ? "rotate_only"
                               : "after");
}

BENCHMARK(BM_FileCacheReserveWithHeldHead)
        ->ArgsProduct({{1024, 8192, 65536}, {0, 1, 2, 3}})
        ->Unit(benchmark::kMicrosecond);

} // namespace doris
