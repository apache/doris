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

// Storage removal of evicted file cache blocks: it must run outside the cache lock, must never
// delete a block re-created at the same position, and must treat missing files as removed.

#include <atomic>
#include <cstdlib>
#include <future>

#include "io/cache/block_file_cache_test_common.h"
#include "util/defer_op.h"

namespace doris::io {

namespace {

constexpr size_t kBlockSize = 10;

std::string evict_io_cache_path(const std::string& name) {
    return (caches_dir / ("evict_io_" + name) / "").string();
}

FileCacheSettings normal_only_settings(size_t capacity, size_t block_size) {
    FileCacheSettings settings;
    settings.index_queue_elements = 0;
    settings.index_queue_size = 0;
    settings.disposable_queue_size = 0;
    settings.disposable_queue_elements = 0;
    settings.ttl_queue_size = 0;
    settings.ttl_queue_elements = 0;
    settings.query_queue_size = capacity;
    settings.query_queue_elements = capacity / block_size + 1;
    settings.max_file_block_size = block_size;
    settings.max_query_cache_size = capacity;
    settings.capacity = capacity;
    return settings;
}

std::string block_path(const std::string& base, const UInt128Wrapper& hash, size_t offset) {
    auto key_str = hash.to_string();
    return (fs::path(base) / key_str.substr(0, 3) / (key_str + "_0") / std::to_string(offset))
            .string();
}

// Inserts [offset, offset + size) of hash, downloads it with `fill` and releases the holder.
void insert_block(BlockFileCache& cache, const UInt128Wrapper& hash, size_t offset, size_t size,
                  char fill = '0') {
    CacheContext context;
    ReadStatistics rstats;
    context.stats = &rstats;
    context.cache_type = FileCacheType::NORMAL;
    auto holder = cache.get_or_set(hash, offset, size, context);
    for (auto& block : holder.file_blocks) {
        if (block->state() != FileBlock::State::EMPTY ||
            block->get_or_set_downloader() != FileBlock::get_caller_id()) {
            continue;
        }
        std::string data(block->range().size(), fill);
        ASSERT_TRUE(block->append(Slice(data.data(), data.size())).ok());
        ASSERT_TRUE(block->finalize().ok());
    }
}

// Whether `mutex` is held by any thread. try_lock from the owning thread is undefined, so the
// probe runs on its own thread.
bool is_locked_elsewhere(std::mutex& mutex) {
    bool locked = false;
    std::thread probe([&] {
        if (mutex.try_lock()) {
            mutex.unlock();
        } else {
            locked = true;
        }
    });
    probe.join();
    return locked;
}

class EvictIoConfigGuard {
public:
    explicit EvictIoConfigGuard(bool value) : _old(config::enable_file_cache_async_evict_io) {
        config::enable_file_cache_async_evict_io = value;
    }
    ~EvictIoConfigGuard() { config::enable_file_cache_async_evict_io = _old; }

private:
    bool _old;
};

void run_eviction_lock_probe(bool async_evict_io, bool expect_lock_held) {
    EvictIoConfigGuard config_guard(async_evict_io);
    auto base = evict_io_cache_path(async_evict_io ? "lock_on" : "lock_off");
    fs::remove_all(base);
    fs::create_directories(base);
    Defer cleanup {[&] { fs::remove_all(base); }};

    BlockFileCache cache(base, normal_only_settings(3 * kBlockSize, kBlockSize));
    ASSERT_TRUE(cache.initialize());
    wait_until_cache_ready(cache);

    auto key1 = BlockFileCache::hash("evict_io_lock_key1");
    auto key2 = BlockFileCache::hash("evict_io_lock_key2");
    insert_block(cache, key1, 0, 3 * kBlockSize);
    ASSERT_TRUE(fs::exists(block_path(base, key1, 0)));

    std::atomic<int> removes {0};
    std::atomic<int> removes_under_lock {0};
    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    Defer disable_sp {[&] { sp->disable_processing(); }};
    SyncPoint::CallbackGuard guard;
    sp->set_call_back(
            "BlockFileCache::run_storage_remove",
            [&](auto&& args) {
                auto* key = try_any_cast<FileCacheKey*>(args[0]);
                if (key->hash != key1) {
                    return;
                }
                ++removes;
                if (is_locked_elsewhere(cache._mutex)) {
                    ++removes_under_lock;
                }
            },
            &guard);

    // The cache is full: reserving the new block evicts key1's LRU head synchronously.
    insert_block(cache, key2, 0, kBlockSize);

    EXPECT_EQ(removes.load(), 1);
    EXPECT_EQ(removes_under_lock.load(), expect_lock_held ? 1 : 0);
    // The removal is synchronous for the caller either way: it is done when get_or_set returns.
    EXPECT_FALSE(fs::exists(block_path(base, key1, 0)));
    EXPECT_TRUE(fs::exists(block_path(base, key1, kBlockSize)));
    EXPECT_EQ(cache._pending_storage_removes.size(), 0U);
}

} // namespace

TEST_F(BlockFileCacheTest, evict_io_storage_remove_runs_after_cache_lock_released) {
    run_eviction_lock_probe(/*async_evict_io=*/true, /*expect_lock_held=*/false);
}

TEST_F(BlockFileCacheTest, evict_io_storage_remove_under_lock_when_disabled) {
    // Proves the probe detects a held lock, and keeps the legacy mode covered.
    run_eviction_lock_probe(/*async_evict_io=*/false, /*expect_lock_held=*/true);
}

TEST_F(BlockFileCacheTest, evict_io_remove_if_cached_removes_after_unlock) {
    EvictIoConfigGuard config_guard(true);
    auto base = evict_io_cache_path("remove_if_cached");
    fs::remove_all(base);
    fs::create_directories(base);
    Defer cleanup {[&] { fs::remove_all(base); }};

    BlockFileCache cache(base, normal_only_settings(10 * kBlockSize, kBlockSize));
    ASSERT_TRUE(cache.initialize());
    wait_until_cache_ready(cache);
    auto key = BlockFileCache::hash("evict_io_remove_if_cached");
    insert_block(cache, key, 0, 3 * kBlockSize);

    std::atomic<int> removes_under_lock {0};
    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    Defer disable_sp {[&] { sp->disable_processing(); }};
    SyncPoint::CallbackGuard guard;
    sp->set_call_back(
            "BlockFileCache::run_storage_remove",
            [&](auto&&) {
                if (is_locked_elsewhere(cache._mutex)) {
                    ++removes_under_lock;
                }
            },
            &guard);

    cache.remove_if_cached(key);
    EXPECT_EQ(removes_under_lock.load(), 0);
    for (size_t offset = 0; offset < 3 * kBlockSize; offset += kBlockSize) {
        EXPECT_FALSE(fs::exists(block_path(base, key, offset))) << offset;
    }
    auto key_str = key.to_string();
    EXPECT_FALSE(fs::exists(fs::path(base) / key_str.substr(0, 3) / (key_str + "_0")));
}

// While an evicted block's storage removal is stalled, unrelated cache lookups do not wait for
// it: the removal runs after the cache lock is released.
TEST_F(BlockFileCacheTest, evict_io_lookup_proceeds_while_remove_stalls) {
    EvictIoConfigGuard config_guard(true);
    auto base = evict_io_cache_path("stalled_remove");
    fs::remove_all(base);
    fs::create_directories(base);
    Defer cleanup {[&] { fs::remove_all(base); }};

    BlockFileCache cache(base, normal_only_settings(2 * kBlockSize, kBlockSize));
    ASSERT_TRUE(cache.initialize());
    wait_until_cache_ready(cache);

    auto victim = BlockFileCache::hash("evict_io_stall_victim");
    auto other = BlockFileCache::hash("evict_io_stall_other");
    auto newcomer = BlockFileCache::hash("evict_io_stall_newcomer");
    insert_block(cache, victim, 0, kBlockSize);
    insert_block(cache, other, 0, kBlockSize);

    std::promise<void> remove_entered;
    std::promise<void> release_remove;
    auto release_future = release_remove.get_future().share();
    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    Defer disable_sp {[&] { sp->disable_processing(); }};
    SyncPoint::CallbackGuard guard;
    sp->set_call_back(
            "BlockFileCache::run_storage_remove",
            [&](auto&& args) {
                auto* key = try_any_cast<FileCacheKey*>(args[0]);
                if (key->hash != victim) {
                    return;
                }
                remove_entered.set_value();
                release_future.wait();
            },
            &guard);

    // Inserting into the full cache evicts the victim; its removal then stalls on this thread.
    std::thread evictor([&] { insert_block(cache, newcomer, 0, kBlockSize); });
    Defer join_evictor {[&] {
        release_remove.set_value();
        evictor.join();
    }};
    ASSERT_EQ(remove_entered.get_future().wait_for(std::chrono::seconds(10)),
              std::future_status::ready);

    auto lookup = std::async(std::launch::async, [&] {
        CacheContext context;
        ReadStatistics rstats;
        context.stats = &rstats;
        context.cache_type = FileCacheType::NORMAL;
        auto holder = cache.get_or_set(other, 0, kBlockSize, context);
        return holder.file_blocks.size() == 1 &&
               holder.file_blocks.front()->state() == FileBlock::State::DOWNLOADED;
    });
    ASSERT_EQ(lookup.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    EXPECT_TRUE(lookup.get());
    EXPECT_FALSE(is_locked_elsewhere(cache._mutex));
}

// A block is evicted and its removal stalls; meanwhile the same position is downloaded again.
// The new download must wait for the stale removal, so the new file survives.
TEST_F(BlockFileCacheTest, evict_io_recreated_block_survives_stale_remove) {
    EvictIoConfigGuard config_guard(true);
    auto base = evict_io_cache_path("recreate");
    fs::remove_all(base);
    fs::create_directories(base);
    Defer cleanup {[&] { fs::remove_all(base); }};

    BlockFileCache cache(base, normal_only_settings(2 * kBlockSize, kBlockSize));
    ASSERT_TRUE(cache.initialize());
    wait_until_cache_ready(cache);

    auto victim = BlockFileCache::hash("evict_io_victim");
    auto other = BlockFileCache::hash("evict_io_other");
    auto newcomer = BlockFileCache::hash("evict_io_newcomer");
    insert_block(cache, victim, 0, kBlockSize, 'a');
    insert_block(cache, other, 0, kBlockSize, 'b');

    std::promise<void> remove_entered;
    std::promise<void> release_remove;
    auto release_future = release_remove.get_future().share();
    std::atomic<bool> entered_once {false};
    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    Defer disable_sp {[&] { sp->disable_processing(); }};
    SyncPoint::CallbackGuard guard;
    sp->set_call_back(
            "BlockFileCache::run_storage_remove",
            [&](auto&& args) {
                auto* key = try_any_cast<FileCacheKey*>(args[0]);
                if (key->hash != victim || entered_once.exchange(true)) {
                    return;
                }
                remove_entered.set_value();
                release_future.wait();
            },
            &guard);

    // Evicts the victim; its storage removal blocks in the sync point after the lock is released.
    auto evictor = std::async(std::launch::async, [&] {
        SCOPED_INIT_THREAD_CONTEXT();
        insert_block(cache, newcomer, 0, kBlockSize, 'c');
    });
    remove_entered.get_future().wait();

    // Download the victim position again while the stale removal is in flight.
    std::atomic<bool> recreated {false};
    auto recreator = std::async(std::launch::async, [&] {
        SCOPED_INIT_THREAD_CONTEXT();
        insert_block(cache, victim, 0, kBlockSize, 'd');
        recreated = true;
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_FALSE(recreated.load()) << "the new download must wait for the stale removal";

    release_remove.set_value();
    evictor.get();
    recreator.get();
    ASSERT_TRUE(recreated.load());

    auto path = block_path(base, victim, 0);
    ASSERT_TRUE(fs::exists(path));
    std::ifstream in(path);
    std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    EXPECT_EQ(content, std::string(kBlockSize, 'd'));
    EXPECT_EQ(cache._pending_storage_removes.size(), 0U);
}

// A removal that is queued but not yet claimed is run by the downloader of the replacement
// block before it writes, so the queued item later finds nothing to do.
TEST_F(BlockFileCacheTest, evict_io_queued_remove_drained_by_new_download) {
    auto base = evict_io_cache_path("queued");
    fs::remove_all(base);
    fs::create_directories(base);
    Defer cleanup {[&] { fs::remove_all(base); }};

    auto old_interval = config::file_cache_background_gc_interval_ms;
    // Keep the background GC thread from draining the queue during the test.
    config::file_cache_background_gc_interval_ms = 3600 * 1000;
    Defer restore {[&] { config::file_cache_background_gc_interval_ms = old_interval; }};

    BlockFileCache cache(base, normal_only_settings(10 * kBlockSize, kBlockSize));
    ASSERT_TRUE(cache.initialize());
    wait_until_cache_ready(cache);
    auto key = BlockFileCache::hash("evict_io_queued");
    insert_block(cache, key, 0, kBlockSize, 'a');

    cache.remove_if_cached_async(key);
    ASSERT_TRUE(cache._pending_storage_removes.contains(key, 0));
    ASSERT_TRUE(fs::exists(block_path(base, key, 0)));

    insert_block(cache, key, 0, kBlockSize, 'b');
    EXPECT_FALSE(cache._pending_storage_removes.contains(key, 0));

    // The queued item no longer owns the position.
    FileCacheKey queued;
    ASSERT_TRUE(cache._recycle_keys.try_dequeue(queued));
    cache.run_storage_remove(queued.hash, queued.offset, nullptr);
    auto path = block_path(base, key, 0);
    ASSERT_TRUE(fs::exists(path));
    std::ifstream in(path);
    std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    EXPECT_EQ(content, std::string(kBlockSize, 'b'));
}

// Many threads reserve, evict and re-insert a small set of hot positions. Every block the cache
// believes is downloaded must still have its file once all removals have drained.
TEST_F(BlockFileCacheTest, evict_io_concurrent_reserve_evict_reinsert) {
    EvictIoConfigGuard config_guard(true);
    auto base = evict_io_cache_path("concurrent");
    fs::remove_all(base);
    fs::create_directories(base);
    Defer cleanup {[&] { fs::remove_all(base); }};

    constexpr size_t kBlocksInCache = 8;
    BlockFileCache cache(base, normal_only_settings(kBlocksInCache * kBlockSize, kBlockSize));
    ASSERT_TRUE(cache.initialize());
    wait_until_cache_ready(cache);

    constexpr int kThreads = 16;
    constexpr int kIterations = 400;
    constexpr int kKeys = 6;
    constexpr int kOffsets = 4;
    std::vector<UInt128Wrapper> keys;
    for (int i = 0; i < kKeys; ++i) {
        keys.push_back(BlockFileCache::hash("evict_io_concurrent_" + std::to_string(i)));
    }
    // Reads populate the process-wide FDCache; later tests assert on its size.
    Defer clear_fd_cache {[&] {
        for (const auto& hash : keys) {
            for (int o = 0; o < kOffsets; ++o) {
                FDCache::instance()->remove_file_reader(std::make_pair(hash, o * kBlockSize));
            }
        }
    }};
    std::atomic<int> read_failures {0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            SCOPED_INIT_THREAD_CONTEXT();
            std::mt19937 rng(t);
            for (int i = 0; i < kIterations; ++i) {
                const auto& hash = keys[rng() % kKeys];
                size_t offset = (rng() % kOffsets) * kBlockSize;
                CacheContext context;
                ReadStatistics rstats;
                context.stats = &rstats;
                context.cache_type = FileCacheType::NORMAL;
                auto holder = cache.get_or_set(hash, offset, kBlockSize, context);
                for (auto& block : holder.file_blocks) {
                    if (block->state() == FileBlock::State::EMPTY &&
                        block->get_or_set_downloader() == FileBlock::get_caller_id()) {
                        std::string data(block->range().size(), 'x');
                        if (!block->append(Slice(data.data(), data.size())).ok() ||
                            !block->finalize().ok()) {
                            continue;
                        }
                    }
                    if (block->state() == FileBlock::State::DOWNLOADED) {
                        std::string buf(block->range().size(), '\0');
                        if (!block->read(Slice(buf.data(), buf.size()), 0).ok()) {
                            ++read_failures;
                        }
                    }
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(read_failures.load(), 0);

    // Drain whatever the background GC has not reached yet.
    FileCacheKey queued;
    while (cache._recycle_keys.try_dequeue(queued)) {
        cache.run_storage_remove(queued.hash, queued.offset, nullptr);
    }
    EXPECT_EQ(cache._pending_storage_removes.size(), 0U);

    std::lock_guard lock(cache._mutex);
    size_t downloaded = 0;
    for (const auto& [hash, cells] : cache._files) {
        for (const auto& [offset, cell] : cells) {
            if (cell.file_block->state_unsafe() == FileBlock::State::DOWNLOADED) {
                ++downloaded;
                EXPECT_TRUE(fs::exists(block_path(base, hash, offset)))
                        << hash.to_string() << " " << offset;
            }
        }
    }
    EXPECT_GT(downloaded, 0U);
}

TEST_F(BlockFileCacheTest, evict_io_remove_missing_file_is_ok) {
    auto base = evict_io_cache_path("missing");
    fs::remove_all(base);
    fs::create_directories(base);
    Defer cleanup {[&] { fs::remove_all(base); }};

    BlockFileCache cache(base, normal_only_settings(10 * kBlockSize, kBlockSize));
    ASSERT_TRUE(cache.initialize());
    wait_until_cache_ready(cache);

    // Never written: neither the file nor its directory exists.
    FileCacheKey key;
    key.hash = BlockFileCache::hash("evict_io_never_written");
    key.offset = 0;
    key.meta.type = FileCacheType::NORMAL;
    key.meta.expiration_time = 0;
    EXPECT_TRUE(cache._storage->remove(key).ok());

    // Written, then the file disappears behind the cache's back before eviction.
    auto hash = BlockFileCache::hash("evict_io_vanished");
    insert_block(cache, hash, 0, 2 * kBlockSize);
    ASSERT_TRUE(fs::remove(block_path(base, hash, 0)));
    cache.remove_if_cached(hash);
    EXPECT_FALSE(fs::exists(block_path(base, hash, kBlockSize)));
    EXPECT_EQ(cache._pending_storage_removes.size(), 0U);
    EXPECT_EQ(cache.get_used_cache_size(FileCacheType::NORMAL), 0U);

    // Removing it again finds nothing and still succeeds.
    key.hash = hash;
    EXPECT_TRUE(cache._storage->remove(key).ok());
    key.offset = kBlockSize;
    EXPECT_TRUE(cache._storage->remove(key).ok());
}

TEST(PendingStorageRemovesTest, mark_claim_finish) {
    PendingStorageRemoves pending;
    FileCacheKey key;
    key.hash = BlockFileCache::hash("pending");
    key.offset = 42;
    FileCacheKey claimed;

    EXPECT_FALSE(pending.claim(key.hash, key.offset, &claimed));
    pending.mark(key);
    EXPECT_TRUE(pending.contains(key.hash, key.offset));
    ASSERT_TRUE(pending.claim(key.hash, key.offset, &claimed));
    EXPECT_EQ(claimed.offset, 42U);
    // A running removal cannot be claimed twice.
    EXPECT_FALSE(pending.claim(key.hash, key.offset, &claimed));
    EXPECT_FALSE(pending.finish(key.hash, key.offset));
    EXPECT_EQ(pending.size(), 0U);

    // Marked again while running: finish hands the removal back to the caller.
    pending.mark(key);
    ASSERT_TRUE(pending.claim(key.hash, key.offset, &claimed));
    pending.mark(key);
    EXPECT_TRUE(pending.finish(key.hash, key.offset));
    ASSERT_TRUE(pending.claim(key.hash, key.offset, &claimed));
    EXPECT_FALSE(pending.finish(key.hash, key.offset));
    EXPECT_EQ(pending.size(), 0U);
}

TEST(PendingStorageRemovesTest, wait_and_claim_waits_for_running_remove) {
    PendingStorageRemoves pending;
    FileCacheKey key;
    key.hash = BlockFileCache::hash("pending_wait");
    key.offset = 0;
    FileCacheKey claimed;
    pending.mark(key);
    ASSERT_TRUE(pending.claim(key.hash, key.offset, &claimed));

    std::atomic<bool> returned {false};
    bool waiter_claimed = true;
    std::thread waiter([&] {
        FileCacheKey k;
        waiter_claimed = pending.wait_and_claim(key.hash, key.offset, &k);
        returned = true;
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_FALSE(returned.load());
    EXPECT_FALSE(pending.finish(key.hash, key.offset));
    waiter.join();
    EXPECT_TRUE(returned.load());
    // The running removal finished the job; nothing is left to claim.
    EXPECT_FALSE(waiter_claimed);
}

// Throughput of many threads inserting into a full cache, where every insert evicts. Run with
// DORIS_FILE_CACHE_EVICT_BENCH=1; it compares removal under the cache lock with removal after it.
TEST_F(BlockFileCacheTest, evict_io_bench_full_cache_insert) {
    if (std::getenv("DORIS_FILE_CACHE_EVICT_BENCH") == nullptr) {
        GTEST_SKIP() << "set DORIS_FILE_CACHE_EVICT_BENCH=1 to run";
    }
    const int threads_num =
            std::getenv("BENCH_THREADS") ? std::atoi(std::getenv("BENCH_THREADS")) : 64;
    const int seconds = std::getenv("BENCH_SECONDS") ? std::atoi(std::getenv("BENCH_SECONDS")) : 5;
    constexpr size_t kBenchBlock = 64 * 1024;
    constexpr size_t kBenchBlocks = 2048;

    for (bool async_evict_io : {false, true, false, true}) {
        EvictIoConfigGuard config_guard(async_evict_io);
        auto base = evict_io_cache_path("bench");
        fs::remove_all(base);
        fs::create_directories(base);
        Defer cleanup {[&] { fs::remove_all(base); }};
        BlockFileCache cache(base, normal_only_settings(kBenchBlocks * kBenchBlock, kBenchBlock));
        ASSERT_TRUE(cache.initialize());
        wait_until_cache_ready(cache);
        std::string payload(kBenchBlock, 'p');
        auto insert = [&](const std::string& name, int64_t* get_or_set_ns) {
            CacheContext context;
            ReadStatistics rstats;
            context.stats = &rstats;
            context.cache_type = FileCacheType::NORMAL;
            auto hash = BlockFileCache::hash(name);
            int64_t start = MonotonicNanos();
            auto holder = cache.get_or_set(hash, 0, kBenchBlock, context);
            *get_or_set_ns += MonotonicNanos() - start;
            for (auto& block : holder.file_blocks) {
                if (block->get_or_set_downloader() == FileBlock::get_caller_id()) {
                    static_cast<void>(block->append(Slice(payload.data(), payload.size())));
                    static_cast<void>(block->finalize());
                }
            }
        };
        int64_t unused = 0;
        for (size_t i = 0; i < kBenchBlocks; ++i) {
            insert("bench_fill_" + std::to_string(i), &unused);
        }

        std::atomic<bool> stop {false};
        std::atomic<int64_t> ops {0};
        std::atomic<int64_t> total_get_or_set_ns {0};
        std::vector<std::thread> workers;
        for (int t = 0; t < threads_num; ++t) {
            workers.emplace_back([&, t] {
                SCOPED_INIT_THREAD_CONTEXT();
                int64_t local_ops = 0;
                int64_t local_ns = 0;
                while (!stop.load(std::memory_order_relaxed)) {
                    insert("bench_" + std::to_string(t) + "_" + std::to_string(local_ops),
                           &local_ns);
                    ++local_ops;
                }
                ops += local_ops;
                total_get_or_set_ns += local_ns;
            });
        }
        std::this_thread::sleep_for(std::chrono::seconds(seconds));
        stop = true;
        for (auto& worker : workers) {
            worker.join();
        }
        LOG(INFO) << "evict_io_bench async_evict_io=" << async_evict_io
                  << " threads=" << threads_num << " inserts_per_sec=" << ops.load() / seconds
                  << " avg_get_or_set_us="
                  << (ops.load() == 0 ? 0 : total_get_or_set_ns.load() / ops.load() / 1000);
        std::cout << "evict_io_bench async_evict_io=" << async_evict_io
                  << " threads=" << threads_num << " inserts_per_sec=" << ops.load() / seconds
                  << " avg_get_or_set_us="
                  << (ops.load() == 0 ? 0 : total_get_or_set_ns.load() / ops.load() / 1000)
                  << std::endl;
    }
}

} // namespace doris::io
