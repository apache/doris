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

#include <condition_variable>
#include <functional>
#include <limits>
#include <mutex>
#include <thread>

#include "io/cache/block_file_cache_factory.h"
#include "io/cache/block_file_cache_test_common.h"

namespace doris::io {

class BlockFileCacheMemModeTest : public BlockFileCacheTest {
public:
    void SetUp() override {
        const auto* test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        _cache_path = caches_dir / test_info->name() / "";
        fs::remove_all(_cache_path);
        fs::create_directories(_cache_path);

        _origin_enter_need_evict = config::file_cache_enter_need_evict_cache_in_advance_percent;
        _origin_exit_need_evict = config::file_cache_exit_need_evict_cache_in_advance_percent;
        _origin_enter_disk_limit = config::file_cache_enter_disk_resource_limit_mode_percent;
        _origin_exit_disk_limit = config::file_cache_exit_disk_resource_limit_mode_percent;
        _origin_enable_evict_in_advance = config::enable_evict_file_cache_in_advance;
        _origin_enable_query_limit = config::enable_file_cache_query_limit;
        _origin_lru_dump_tail_record_num = config::file_cache_background_lru_dump_tail_record_num;
    }

    void TearDown() override {
        config::file_cache_enter_need_evict_cache_in_advance_percent = _origin_enter_need_evict;
        config::file_cache_exit_need_evict_cache_in_advance_percent = _origin_exit_need_evict;
        config::file_cache_enter_disk_resource_limit_mode_percent = _origin_enter_disk_limit;
        config::file_cache_exit_disk_resource_limit_mode_percent = _origin_exit_disk_limit;
        config::enable_evict_file_cache_in_advance = _origin_enable_evict_in_advance;
        config::enable_file_cache_query_limit = _origin_enable_query_limit;
        config::file_cache_background_lru_dump_tail_record_num = _origin_lru_dump_tail_record_num;

        auto* sync_point = SyncPoint::get_instance();
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();

        fs::remove_all(_cache_path);
    }

protected:
    FileCacheSettings make_settings(const std::string& storage = "memory") const {
        FileCacheSettings settings;
        settings.storage = storage;
        settings.capacity = 100_mb;
        settings.max_file_block_size = 10_mb;
        settings.max_query_cache_size = 100_mb;
        settings.query_queue_size = 100_mb;
        settings.query_queue_elements = 16;
        settings.index_queue_size = 30_mb;
        settings.index_queue_elements = 8;
        settings.disposable_queue_size = 30_mb;
        settings.disposable_queue_elements = 8;
        settings.ttl_queue_size = 30_mb;
        settings.ttl_queue_elements = 8;
        return settings;
    }

    void wait_async_open(BlockFileCache& cache) const {
        for (int i = 0; i < 100; ++i) {
            if (cache.get_async_open_success()) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        FAIL() << "cache async open did not finish";
    }

    bool wait_until(const std::function<bool()>& predicate, int64_t timeout_ms = 2000) const {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
        while (std::chrono::steady_clock::now() < deadline) {
            if (predicate()) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        return predicate();
    }

    void fill_memory_cache(BlockFileCache& cache, size_t total_size) const {
        TUniqueId query_id;
        query_id.hi = 1;
        query_id.lo = 1;
        CacheContext context;
        ReadStatistics rstats;
        context.stats = &rstats;
        context.cache_type = FileCacheType::NORMAL;
        context.query_id = query_id;
        auto key = BlockFileCache::hash("mem-mode-key");

        for (size_t offset = 0; offset < total_size; offset += 10_mb) {
            auto holder = cache.get_or_set(key, offset, 10_mb, context);
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 1);
            ASSERT_TRUE(blocks[0]->get_or_set_downloader() == FileBlock::get_caller_id());
            download_into_memory(blocks[0]);
        }
    }

    void fill_cache(BlockFileCache& cache, const UInt128Wrapper& key, size_t total_size,
                    bool use_memory_download) const {
        TUniqueId query_id;
        query_id.hi = 11;
        query_id.lo = 11;
        CacheContext context;
        ReadStatistics rstats;
        context.stats = &rstats;
        context.cache_type = FileCacheType::NORMAL;
        context.query_id = query_id;

        for (size_t offset = 0; offset < total_size; offset += 10_mb) {
            auto holder = cache.get_or_set(key, offset, 10_mb, context);
            auto blocks = fromHolder(holder);
            ASSERT_EQ(blocks.size(), 1);
            ASSERT_TRUE(blocks[0]->get_or_set_downloader() == FileBlock::get_caller_id());
            if (use_memory_download) {
                download_into_memory(blocks[0]);
            } else {
                download(blocks[0]);
            }
        }
    }

    void download_memory_block(const FileBlockSPtr& block, char value) const {
        std::string data(block->range().size(), value);
        ASSERT_TRUE(block->append(Slice(data.data(), data.size())).ok());
        ASSERT_TRUE(block->finalize().ok());
    }

    fs::path _cache_path;
    int64_t _origin_enter_need_evict = 0;
    int64_t _origin_exit_need_evict = 0;
    int64_t _origin_enter_disk_limit = 0;
    int64_t _origin_exit_disk_limit = 0;
    bool _origin_enable_evict_in_advance = false;
    bool _origin_enable_query_limit = false;
    int64_t _origin_lru_dump_tail_record_num = 0;
};

TEST_F(BlockFileCacheMemModeTest, check_need_evict_memory_enter_exit) {
    auto settings = make_settings();
    BlockFileCache cache(_cache_path.string(), settings);

    config::file_cache_enter_need_evict_cache_in_advance_percent = 70;
    config::file_cache_exit_need_evict_cache_in_advance_percent = 65;

    cache._cur_cache_size = 80_mb;
    cache.check_need_evict_cache_in_advance();
    ASSERT_TRUE(cache._need_evict_cache_in_advance);

    cache._cur_cache_size = 60_mb;
    cache.check_need_evict_cache_in_advance();
    ASSERT_FALSE(cache._need_evict_cache_in_advance);
}

TEST_F(BlockFileCacheMemModeTest, zero_capacity_memory_should_keep_monitor_and_stats_available) {
    auto settings = make_settings();
    settings.capacity = 0;
    BlockFileCache cache(_cache_path.string(), settings);

    cache.check_disk_resource_limit();
    cache.check_need_evict_cache_in_advance();
    EXPECT_FALSE(cache._disk_resource_limit_mode.load(std::memory_order_relaxed));
    EXPECT_FALSE(cache._need_evict_cache_in_advance.load(std::memory_order_relaxed));
    EXPECT_EQ(cache.get_stats().at("size_percentage"), 0);
    EXPECT_EQ(cache.get_stats_unsafe().at("size_percentage"), 0);

    ASSERT_TRUE(cache.initialize().ok());
    wait_async_open(cache);
    EXPECT_EQ(cache.get_stats().at("size_percentage"), 0);
}

TEST_F(BlockFileCacheMemModeTest,
       tiny_reset_with_held_blocks_should_keep_pressure_checks_available) {
    auto settings = make_settings();
    BlockFileCache cache(_cache_path.string(), settings);
    ASSERT_TRUE(cache.initialize().ok());
    wait_async_open(cache);

    CacheContext context;
    ReadStatistics rstats;
    context.stats = &rstats;
    context.cache_type = FileCacheType::NORMAL;
    auto key = BlockFileCache::hash("mem-held-tiny-reset-key");
    auto holder = cache.get_or_set(key, 0, 30_mb, context);
    auto blocks = fromHolder(holder);
    ASSERT_EQ(blocks.size(), 3);
    for (const auto& block : blocks) {
        ASSERT_TRUE(block->get_or_set_downloader() == FileBlock::get_caller_id());
        download_into_memory(block);
    }

    auto message = cache.reset_capacity(1);
    ASSERT_FALSE(message.empty());
    ASSERT_EQ(cache._cur_cache_size, 30_mb);
    {
        std::lock_guard cache_lock(cache._mutex);
        ASSERT_GT(cache.calc_size_percentage_unlocked(cache_lock), std::numeric_limits<int>::max());
    }

    cache.check_disk_resource_limit();
    cache.check_need_evict_cache_in_advance();
    EXPECT_TRUE(cache._disk_resource_limit_mode.load(std::memory_order_relaxed));
    EXPECT_TRUE(cache._need_evict_cache_in_advance.load(std::memory_order_relaxed));
}

TEST_F(BlockFileCacheMemModeTest, pressure_mode_should_account_query_by_actual_block_size) {
    auto settings = make_settings();
    config::enable_file_cache_query_limit = false;
    config::file_cache_enter_disk_resource_limit_mode_percent = 90;
    config::file_cache_exit_disk_resource_limit_mode_percent = 80;
    BlockFileCache cache(_cache_path.string(), settings);
    ASSERT_TRUE(cache.initialize().ok());
    wait_async_open(cache);

    fill_memory_cache(cache, 100_mb);
    cache.clear_need_update_lru_blocks();
    {
        std::lock_guard cache_lock(cache._mutex);
        // Keep the prefilled blocks cold regardless of the CI worker's uptime.
        for (auto& file : cache._files) {
            for (auto& offset_cell : file.second) {
                offset_cell.second.atime = std::numeric_limits<int64_t>::min();
            }
        }
    }
    cache.check_disk_resource_limit();
    ASSERT_TRUE(cache._disk_resource_limit_mode.load(std::memory_order_relaxed));

    config::enable_file_cache_query_limit = true;
    TUniqueId query_id;
    query_id.hi = 19;
    query_id.lo = 19;
    auto query_context_holder = cache.get_query_context_holder(query_id, 10);
    ASSERT_NE(query_context_holder, nullptr);
    ASSERT_NE(query_context_holder->context, nullptr);

    CacheContext context;
    ReadStatistics rstats;
    context.stats = &rstats;
    context.cache_type = FileCacheType::DISPOSABLE;
    context.query_id = query_id;
    auto admit_block = [&](const std::string& key_name) {
        auto key = BlockFileCache::hash(key_name);
        auto holder = cache.get_or_set(key, 0, 10_mb, context);
        auto blocks = fromHolder(holder);
        ASSERT_EQ(blocks.size(), 1);
        ASSERT_TRUE(blocks[0]->get_or_set_downloader() == FileBlock::get_caller_id());
        download_into_memory(blocks[0]);
    };

    admit_block("query-pressure-key-1");
    cache.clear_need_update_lru_blocks();
    {
        std::lock_guard cache_lock(cache._mutex);
        EXPECT_EQ(query_context_holder->context->get_cache_size(cache_lock), 10_mb);
    }

    admit_block("query-pressure-key-2");
    cache.clear_need_update_lru_blocks();
    {
        std::lock_guard cache_lock(cache._mutex);
        EXPECT_EQ(query_context_holder->context->get_cache_size(cache_lock), 10_mb);
    }
}

TEST_F(BlockFileCacheMemModeTest, check_need_evict_memory_config_fallback) {
    auto settings = make_settings();
    BlockFileCache cache(_cache_path.string(), settings);

    config::file_cache_enter_need_evict_cache_in_advance_percent = 70;
    config::file_cache_exit_need_evict_cache_in_advance_percent = 75;

    cache._cur_cache_size = 80_mb;
    cache.check_need_evict_cache_in_advance();

    ASSERT_EQ(config::file_cache_enter_need_evict_cache_in_advance_percent, 78);
    ASSERT_EQ(config::file_cache_exit_need_evict_cache_in_advance_percent, 75);
    ASSERT_TRUE(cache._need_evict_cache_in_advance);
}

TEST_F(BlockFileCacheMemModeTest, check_disk_mode_memory_enter_exit) {
    auto settings = make_settings();
    BlockFileCache cache(_cache_path.string(), settings);

    config::file_cache_enter_disk_resource_limit_mode_percent = 90;
    config::file_cache_exit_disk_resource_limit_mode_percent = 80;

    cache._cur_cache_size = 95_mb;
    cache.check_disk_resource_limit();
    ASSERT_TRUE(cache._disk_resource_limit_mode);

    cache._cur_cache_size = 70_mb;
    cache.check_disk_resource_limit();
    ASSERT_FALSE(cache._disk_resource_limit_mode);
}

TEST_F(BlockFileCacheMemModeTest, check_disk_mode_disk_compat) {
    auto settings = make_settings("disk");
    BlockFileCache cache(_cache_path.string(), settings);

    config::file_cache_enter_disk_resource_limit_mode_percent = 90;
    config::file_cache_exit_disk_resource_limit_mode_percent = 80;

    auto* sync_point = SyncPoint::get_instance();
    sync_point->set_call_back("BlockFileCache::disk_used_percentage:1",
                              [](std::vector<std::any>&& values) {
                                  auto* percent = try_any_cast<std::pair<int, int>*>(values.back());
                                  percent->first = 95;
                                  percent->second = 70;
                              });
    sync_point->enable_processing();
    cache.check_disk_resource_limit();
    ASSERT_TRUE(cache._disk_resource_limit_mode);

    sync_point->clear_all_call_backs();
    sync_point->set_call_back("BlockFileCache::disk_used_percentage:1",
                              [](std::vector<std::any>&& values) {
                                  auto* percent = try_any_cast<std::pair<int, int>*>(values.back());
                                  percent->first = 70;
                                  percent->second = 70;
                              });
    cache.check_disk_resource_limit();
    ASSERT_FALSE(cache._disk_resource_limit_mode);
}

TEST_F(BlockFileCacheMemModeTest, reset_capacity_memory_should_work_and_mode_converge) {
    auto settings = make_settings();
    BlockFileCache cache(_cache_path.string(), settings);
    ASSERT_TRUE(cache.initialize().ok());
    wait_async_open(cache);

    fill_memory_cache(cache, 60_mb);
    ASSERT_EQ(cache._cur_cache_size, 60_mb);

    cache._disk_resource_limit_mode = false;
    cache._disk_limit_mode_metrics->set_value(0);

    auto message = cache.reset_capacity(30_mb);
    ASSERT_FALSE(message.empty());
    ASSERT_LE(cache._cur_cache_size, 30_mb);
    ASSERT_FALSE(cache._disk_resource_limit_mode);

    config::file_cache_enter_disk_resource_limit_mode_percent = 90;
    config::file_cache_exit_disk_resource_limit_mode_percent = 80;
    cache.check_disk_resource_limit();
    ASSERT_TRUE(cache._disk_resource_limit_mode);
}

TEST_F(BlockFileCacheMemModeTest, reset_capacity_memory_should_not_overcount_unreleasable_blocks) {
    auto settings = make_settings();
    BlockFileCache cache(_cache_path.string(), settings);
    ASSERT_TRUE(cache.initialize().ok());
    wait_async_open(cache);

    TUniqueId query_id;
    query_id.hi = 7;
    query_id.lo = 7;
    CacheContext context;
    ReadStatistics rstats;
    context.stats = &rstats;
    context.cache_type = FileCacheType::NORMAL;
    context.query_id = query_id;
    auto key = BlockFileCache::hash("mem-held-key");

    auto holder = cache.get_or_set(key, 0, 10_mb, context);
    auto blocks = fromHolder(holder);
    ASSERT_EQ(blocks.size(), 1);
    ASSERT_TRUE(blocks[0]->get_or_set_downloader() == FileBlock::get_caller_id());
    download_into_memory(blocks[0]);

    fill_memory_cache(cache, 30_mb);
    ASSERT_EQ(cache._cur_cache_size, 40_mb);

    auto message = cache.reset_capacity(15_mb);
    ASSERT_FALSE(message.empty());
    ASSERT_LE(cache._cur_cache_size, 15_mb);
    ASSERT_EQ(cache._cur_cache_size, 10_mb);

    blocks.clear();
    holder.file_blocks.clear();
}

TEST_F(BlockFileCacheMemModeTest, reset_capacity_should_not_miss_last_holder_release) {
    auto settings = make_settings();
    BlockFileCache cache(_cache_path.string(), settings);
    ASSERT_TRUE(cache.initialize().ok());
    wait_async_open(cache);

    CacheContext context;
    ReadStatistics rstats;
    context.stats = &rstats;
    context.cache_type = FileCacheType::NORMAL;
    auto key = BlockFileCache::hash("mem-reset-last-holder-key");

    auto holder = std::make_unique<FileBlocksHolder>(cache.get_or_set(key, 0, 1_mb, context));
    auto blocks = fromHolder(*holder);
    ASSERT_EQ(blocks.size(), 1);
    ASSERT_EQ(blocks[0]->get_or_set_downloader(), FileBlock::get_caller_id());
    download_memory_block(blocks[0], '0');
    blocks.clear();

    std::mutex barrier_mutex;
    std::condition_variable barrier_cv;
    bool holder_before_release = false;
    bool reset_before_block_lock = false;
    bool release_holder = false;

    auto* sync_point = SyncPoint::get_instance();
    sync_point->set_call_back("FileBlocksHolder::~FileBlocksHolder::before_cached_block_release",
                              [&](auto&&) {
                                  std::unique_lock lock(barrier_mutex);
                                  holder_before_release = true;
                                  barrier_cv.notify_all();
                                  barrier_cv.wait(lock, [&] { return release_holder; });
                              });
    sync_point->set_call_back("BlockFileCache::reset_capacity::before_lock_busy_block",
                              [&](auto&&) {
                                  std::lock_guard lock(barrier_mutex);
                                  reset_before_block_lock = true;
                                  barrier_cv.notify_all();
                              });
    sync_point->enable_processing();

    std::thread holder_thread([&] { holder.reset(); });
    {
        std::unique_lock lock(barrier_mutex);
        barrier_cv.wait(lock, [&] { return holder_before_release; });
    }

    std::thread reset_thread([&] { cache.reset_capacity(1); });
    {
        std::unique_lock lock(barrier_mutex);
        barrier_cv.wait(lock, [&] { return reset_before_block_lock; });
        release_holder = true;
        barrier_cv.notify_all();
    }

    holder_thread.join();
    reset_thread.join();

    EXPECT_EQ(cache._cur_cache_size, 0);
    EXPECT_EQ(cache.get_file_blocks_num(FileCacheType::NORMAL), 0);
}

TEST_F(BlockFileCacheMemModeTest, reset_holder_cleanup_should_not_remove_redownloaded_block) {
    auto settings = make_settings();
    BlockFileCache cache(_cache_path.string(), settings);
    ASSERT_TRUE(cache.initialize().ok());
    wait_async_open(cache);

    CacheContext context;
    ReadStatistics rstats;
    context.stats = &rstats;
    context.cache_type = FileCacheType::NORMAL;
    auto key = BlockFileCache::hash("mem-reset-holder-redownload-key");

    auto old_holder = std::make_unique<FileBlocksHolder>(cache.get_or_set(key, 0, 1_mb, context));
    auto old_blocks = fromHolder(*old_holder);
    ASSERT_EQ(old_blocks.size(), 1);
    ASSERT_EQ(old_blocks[0]->get_or_set_downloader(), FileBlock::get_caller_id());
    download_memory_block(old_blocks[0], '0');
    old_blocks.clear();

    std::unique_lock gc_pause(cache._close_mtx);
    ASSERT_FALSE(cache.reset_capacity(1).empty());
    old_holder.reset();

    ASSERT_FALSE(cache.reset_capacity(100_mb).empty());
    auto new_holder = std::make_unique<FileBlocksHolder>(cache.get_or_set(key, 0, 1_mb, context));
    auto new_blocks = fromHolder(*new_holder);
    ASSERT_EQ(new_blocks.size(), 1);
    ASSERT_EQ(new_blocks[0]->get_or_set_downloader(), FileBlock::get_caller_id());
    download_memory_block(new_blocks[0], '1');

    gc_pause.unlock();
    ASSERT_TRUE(wait_until([&cache] { return cache._recycle_keys.size_approx() == 0; }));

    std::string actual(1_mb, '\0');
    ASSERT_TRUE(new_blocks[0]->read(Slice(actual.data(), actual.size()), 0).ok());
    EXPECT_EQ(actual, std::string(1_mb, '1'));
}

TEST_F(BlockFileCacheMemModeTest, run_background_monitor_memory_mode_flip) {
    auto settings = make_settings();
    config::enable_evict_file_cache_in_advance = true;
    config::file_cache_enter_need_evict_cache_in_advance_percent = 70;
    config::file_cache_exit_need_evict_cache_in_advance_percent = 65;
    config::file_cache_enter_disk_resource_limit_mode_percent = 90;
    config::file_cache_exit_disk_resource_limit_mode_percent = 80;

    auto* sync_point = SyncPoint::get_instance();
    sync_point->set_call_back("BlockFileCache::set_sleep_time",
                              [](auto&& args) { *try_any_cast<int64_t*>(args[0]) = 10; });
    sync_point->enable_processing();

    BlockFileCache cache(_cache_path.string(), settings);
    ASSERT_TRUE(cache.initialize().ok());
    wait_async_open(cache);

    {
        std::lock_guard lock(cache._mutex);
        cache._cur_cache_size = 95_mb;
    }
    ASSERT_TRUE(wait_until([&cache] {
        return cache._need_evict_cache_in_advance && cache._disk_resource_limit_mode;
    }));

    {
        std::lock_guard lock(cache._mutex);
        cache._cur_cache_size = 60_mb;
    }
    ASSERT_TRUE(wait_until([&cache] {
        return !cache._need_evict_cache_in_advance && !cache._disk_resource_limit_mode;
    }));
}

TEST_F(BlockFileCacheMemModeTest, stats_memory_contains_storage_type_and_size_percentage) {
    auto settings = make_settings();
    BlockFileCache cache(_cache_path.string(), settings);

    cache._cur_cache_size = 25_mb;
    cache._cur_cache_size_metrics->set_value(cache._cur_cache_size);

    auto stats = cache.get_stats();
    ASSERT_TRUE(stats.contains("size_percentage"));
    ASSERT_TRUE(stats.contains("storage_type"));
    ASSERT_EQ(stats["size_percentage"], 25);
    ASSERT_EQ(stats["storage_type"], FileCacheStorageType::MEMORY);
}

TEST_F(BlockFileCacheMemModeTest, memory_storage_should_skip_lru_restore_and_dump) {
    config::file_cache_background_lru_dump_tail_record_num = 100;
    auto settings = make_settings();
    auto key = BlockFileCache::hash("memory-lru-restore-key");
    auto memory_dump_dir = fs::current_path() / "memory";
    fs::remove_all(memory_dump_dir);
    fs::create_directories(memory_dump_dir);

    {
        BlockFileCache cache(_cache_path.string(), settings);
        ASSERT_TRUE(cache.initialize().ok());
        wait_async_open(cache);

        fill_cache(cache, key, 20_mb, true);
        ASSERT_EQ(cache._cur_cache_size, 20_mb);
        cache.dump_lru_queues(true);
    }

    auto dump_file = memory_dump_dir / "lru_dump_normal.tail";
    ASSERT_FALSE(fs::exists(dump_file));

    std::ofstream(dump_file, std::ios::binary) << "stale-memory-dump";
    ASSERT_TRUE(fs::exists(dump_file));

    BlockFileCache cache2(_cache_path.string(), settings);
    ASSERT_TRUE(cache2.initialize().ok());
    wait_async_open(cache2);

    ASSERT_EQ(cache2._cur_cache_size, 0);
    ASSERT_EQ(cache2._normal_queue.get_elements_num_unsafe(), 0);
    ASSERT_TRUE(cache2.get_blocks_by_key(key).empty());

    fs::remove_all(memory_dump_dir);
}

TEST_F(BlockFileCacheMemModeTest, memory_storage_should_skip_lru_queue_recorder) {
    config::file_cache_background_lru_dump_tail_record_num = 100;
    auto settings = make_settings();
    BlockFileCache cache(_cache_path.string(), settings);
    ASSERT_TRUE(cache.initialize().ok());
    wait_async_open(cache);

    auto key = BlockFileCache::hash("memory-storage-skip-lru-recorder");
    fill_cache(cache, key, 10_mb, true);

    EXPECT_EQ(cache._lru_recorder->get_lru_queue_update_cnt_from_last_dump(FileCacheType::NORMAL),
              0);
}

TEST_F(BlockFileCacheMemModeTest, disk_storage_should_keep_lru_restore) {
    config::file_cache_background_lru_dump_tail_record_num = 100;
    auto settings = make_settings("disk");
    auto key = BlockFileCache::hash("disk-lru-restore-key");
    auto origin_cache_base_path = cache_base_path;
    cache_base_path = _cache_path.string();

    {
        BlockFileCache cache(_cache_path.string(), settings);
        ASSERT_TRUE(cache.initialize().ok());
        wait_async_open(cache);

        fill_cache(cache, key, 10_mb, false);
        cache.dump_lru_queues(true);
    }

    BlockFileCache cache2(_cache_path.string(), settings);
    ASSERT_TRUE(cache2.initialize().ok());
    wait_async_open(cache2);

    ASSERT_EQ(cache2._cur_cache_size, 10_mb);
    ASSERT_EQ(cache2._normal_queue.get_elements_num_unsafe(), 1);
    auto blocks = cache2.get_blocks_by_key(key);
    ASSERT_EQ(blocks.size(), 1);
    ASSERT_TRUE(blocks.contains(0));

    cache_base_path = origin_cache_base_path;
}

TEST_F(BlockFileCacheMemModeTest, factory_should_reset_memory_without_disk_validation) {
    FileCacheFactory factory;
    auto memory_settings = make_settings();
    auto disk_settings = make_settings("disk");
    auto disk_path = (_cache_path / "disk").string();

    ASSERT_TRUE(factory.create_file_cache("memory", memory_settings).ok());
    ASSERT_TRUE(factory.create_file_cache(disk_path, disk_settings).ok());
    ASSERT_EQ(factory.get_capacity(), 200_mb);

    auto message = factory.reset_capacity("memory", 60_mb);
    EXPECT_FALSE(message.empty());
    EXPECT_EQ(factory.get_by_path("memory")->capacity(), 60_mb);
    EXPECT_EQ(factory.get_capacity(), 160_mb);

    message = factory.reset_capacity("", 40_mb);
    EXPECT_FALSE(message.empty());
    EXPECT_EQ(factory.get_by_path("memory")->capacity(), 40_mb);
    EXPECT_EQ(factory.get_by_path(disk_path)->capacity(), 40_mb);
    EXPECT_EQ(factory.get_capacity(), 80_mb);
}

TEST_F(BlockFileCacheMemModeTest, factory_should_prevalidate_all_resets_before_mutation) {
    FileCacheFactory factory;
    auto settings = make_settings("disk");
    auto disk_path_a = (_cache_path / "disk_a").string();
    auto disk_path_b = (_cache_path / "disk_b").string();

    ASSERT_TRUE(factory.create_file_cache(disk_path_a, settings).ok());
    ASSERT_TRUE(factory.create_file_cache(disk_path_b, settings).ok());
    auto paths = factory.get_base_paths();
    ASSERT_EQ(paths.size(), 2);
    fs::remove_all(paths.back());

    auto message = factory.reset_capacity("", 60_mb);
    EXPECT_NE(message.find("statfs error"), std::string::npos);
    EXPECT_EQ(factory.get_by_path(disk_path_a)->capacity(), 100_mb);
    EXPECT_EQ(factory.get_by_path(disk_path_b)->capacity(), 100_mb);
    EXPECT_EQ(factory.get_capacity(), 200_mb);
}

} // namespace doris::io
