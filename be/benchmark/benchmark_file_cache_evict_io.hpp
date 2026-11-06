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
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_cache_common.h"
#include "io/cache/fs_file_cache_storage.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "util/time.h"

namespace doris {

// Many threads inserting into a full file cache on the local filesystem, so every insert evicts
// one block and its file has to be deleted (the reactive eviction path; evict-in-advance is off).
// Each iteration, `threads` threads each insert kEvictIoInsertsPerThread new blocks.
// Arg 0: threads.
// Counters: items_per_second (inserts) and get_or_set_us, the mean time a get_or_set takes,
// which includes waiting for the cache lock and the eviction it does.

constexpr size_t kEvictIoBlock = 16 * 1024;
constexpr size_t kEvictIoBlocks = 2048;
constexpr int kEvictIoInsertsPerThread = 64;

class EvictIoBench {
public:
    EvictIoBench()
            : _in_advance(config::enable_evict_file_cache_in_advance),
              _resource_limit_percent(config::file_cache_enter_disk_resource_limit_mode_percent) {
        config::enable_evict_file_cache_in_advance = false;
        // Keep a nearly full host disk from switching the cache into disk-resource-limit mode.
        config::file_cache_enter_disk_resource_limit_mode_percent = 99;
        // FSFileCacheStorage reads through the process-wide FD cache that a BE creates at startup.
        if (ExecEnv::GetInstance()->file_cache_open_fd_cache() == nullptr) {
            ExecEnv::GetInstance()->set_file_cache_open_fd_cache(std::make_unique<io::FDCache>());
            _owns_fd_cache = true;
        }
        if (ExecEnv::GetInstance()->file_cache_factory() == nullptr) {
            _factory = std::make_unique<io::FileCacheFactory>();
            ExecEnv::GetInstance()->set_file_cache_factory(_factory.get());
        }

        _base = std::filesystem::temp_directory_path() /
                ("doris_evict_io_bench_" + std::to_string(::getpid()));
        std::filesystem::remove_all(_base);
        std::filesystem::create_directories(_base);

        io::FileCacheSettings settings;
        settings.capacity = kEvictIoBlocks * kEvictIoBlock;
        settings.max_file_block_size = kEvictIoBlock;
        settings.max_query_cache_size = settings.capacity;
        settings.query_queue_size = settings.capacity;
        settings.query_queue_elements = kEvictIoBlocks + 1;
        settings.index_queue_size = 0;
        settings.index_queue_elements = 0;
        settings.disposable_queue_size = 0;
        settings.disposable_queue_elements = 0;
        settings.ttl_queue_size = 0;
        settings.ttl_queue_elements = 0;
        _cache = std::make_unique<io::BlockFileCache>(_base.string() + "/", settings);
        CHECK(_cache->initialize().ok());
        while (!_cache->get_async_open_success()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        for (size_t i = 0; i < kEvictIoBlocks; ++i) {
            insert("fill_" + std::to_string(i));
        }
    }

    ~EvictIoBench() {
        _cache.reset();
        std::filesystem::remove_all(_base);
        if (_factory != nullptr) {
            ExecEnv::GetInstance()->set_file_cache_factory(nullptr);
        }
        if (_owns_fd_cache) {
            ExecEnv::GetInstance()->set_file_cache_open_fd_cache(nullptr);
        }
        config::enable_evict_file_cache_in_advance = _in_advance;
        config::file_cache_enter_disk_resource_limit_mode_percent = _resource_limit_percent;
    }

    // Inserts and downloads one new block; returns the nanoseconds spent in get_or_set.
    int64_t insert(const std::string& name) {
        io::CacheContext context;
        io::ReadStatistics stats;
        context.stats = &stats;
        context.cache_type = io::FileCacheType::NORMAL;
        auto hash = io::BlockFileCache::hash(name);
        const int64_t start = MonotonicNanos();
        auto holder = _cache->get_or_set(hash, 0, kEvictIoBlock, context);
        const int64_t elapsed = MonotonicNanos() - start;
        for (auto& block : holder.file_blocks) {
            if (block->state() == io::FileBlock::State::EMPTY &&
                block->get_or_set_downloader() == io::FileBlock::get_caller_id()) {
                CHECK(block->append(Slice(_payload.data(), _payload.size())).ok());
                CHECK(block->finalize().ok());
            }
        }
        return elapsed;
    }

private:
    bool _in_advance;
    int32_t _resource_limit_percent;
    bool _owns_fd_cache = false;
    std::unique_ptr<io::FileCacheFactory> _factory;
    std::filesystem::path _base;
    std::string _payload = std::string(kEvictIoBlock, 'x');
    std::unique_ptr<io::BlockFileCache> _cache;
};

static void BM_FileCacheFullInsertEvictsFiles(benchmark::State& state) {
    const int threads = static_cast<int>(state.range(0));
    EvictIoBench bench;
    std::atomic<int64_t> next {0};
    int64_t get_or_set_ns = 0;
    int64_t inserts = 0;
    for (auto _ : state) {
        std::atomic<int64_t> round_ns {0};
        std::vector<std::thread> workers;
        workers.reserve(threads);
        for (int t = 0; t < threads; ++t) {
            workers.emplace_back([&] {
                SCOPED_INIT_THREAD_CONTEXT();
                int64_t local_ns = 0;
                for (int i = 0; i < kEvictIoInsertsPerThread; ++i) {
                    local_ns += bench.insert("new_" + std::to_string(next.fetch_add(1)));
                }
                round_ns += local_ns;
            });
        }
        for (auto& worker : workers) {
            worker.join();
        }
        get_or_set_ns += round_ns.load();
        inserts += static_cast<int64_t>(threads) * kEvictIoInsertsPerThread;
    }
    state.SetItemsProcessed(inserts);
    state.counters["get_or_set_us"] =
            static_cast<double>(get_or_set_ns) / static_cast<double>(inserts) / 1000.0;
}

BENCHMARK(BM_FileCacheFullInsertEvictsFiles)
        ->Arg(1)
        ->Arg(16)
        ->Arg(64)
        ->Unit(benchmark::kMillisecond)
        ->UseRealTime();

} // namespace doris
