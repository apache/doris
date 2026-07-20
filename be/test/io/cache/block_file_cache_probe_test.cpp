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

#include <gtest/gtest.h>

#include <condition_variable>
#include <iterator>
#include <mutex>
#include <thread>

#include "io/cache/block_file_cache_test_common.h"
#include "util/defer_op.h"

namespace doris::io {
namespace {

FileCacheSettings probe_cache_settings() {
    FileCacheSettings settings;
    settings.query_queue_size = 4_mb;
    settings.query_queue_elements = 1024;
    settings.index_queue_size = 1_mb;
    settings.index_queue_elements = 256;
    settings.disposable_queue_size = 1_mb;
    settings.disposable_queue_elements = 256;
    settings.capacity = 8_mb;
    settings.max_file_block_size = 4096;
    settings.max_query_cache_size = 0;
    return settings;
}

TEST_F(BlockFileCacheTest, ProbeMissDoesNotCreateCacheCell) {
    const auto path = caches_dir / "block_file_cache_probe_miss";
    std::error_code error;
    fs::remove_all(path, error);
    fs::create_directories(path);
    {
        BlockFileCache cache(path.string(), probe_cache_settings());
        ASSERT_TRUE(cache.initialize().ok());
        wait_until_cache_ready(cache);
        const auto hash = BlockFileCache::hash("probe_miss");
        ReadStatistics stats;
        CacheContext context;
        context.stats = &stats;
        const size_t cache_size_before = cache._cur_cache_size;
        const int64_t probe_latency_count = cache._probe_latency_us->count();
        const int64_t cache_lock_wait_count = cache._cache_lock_wait_time_us->count();

        auto result = cache.probe(hash, 0, 4096, context);
        ASSERT_EQ(result.file_blocks.size(), 1);
        EXPECT_EQ(result.file_blocks[0], nullptr);
        EXPECT_EQ(cache._cur_cache_size, cache_size_before);
        EXPECT_EQ(cache._probe_latency_us->count() - probe_latency_count, 1);
        EXPECT_EQ(cache._cache_lock_wait_time_us->count() - cache_lock_wait_count, 1);
        std::lock_guard cache_lock(cache._mutex);
        EXPECT_EQ(cache._files.find(hash), cache._files.end());
    }
    fs::remove_all(path, error);
}

TEST_F(BlockFileCacheTest, ProbeReturnsDownloadedBlockAndNullMissSlot) {
    const auto path = caches_dir / "block_file_cache_probe_mixed";
    std::error_code error;
    fs::remove_all(path, error);
    fs::create_directories(path);
    {
        BlockFileCache cache(path.string(), probe_cache_settings());
        ASSERT_TRUE(cache.initialize().ok());
        wait_until_cache_ready(cache);
        const auto hash = BlockFileCache::hash("probe_mixed");
        ReadStatistics stats;
        CacheContext context;
        context.stats = &stats;
        {
            auto holder = cache.get_or_set(hash, 0, 4096, context);
            ASSERT_EQ(holder.file_blocks.size(), 1);
            const auto& block = holder.file_blocks.front();
            ASSERT_EQ(block->get_or_set_downloader(), FileBlock::get_caller_id());
            const std::string payload(4096, 'p');
            ASSERT_TRUE(block->append(Slice(payload.data(), payload.size())).ok());
            ASSERT_TRUE(block->finalize().ok());
        }
        const size_t cache_size_before = cache._cur_cache_size;
        {
            std::lock_guard cache_lock(cache._mutex);
            cache._files.at(hash).begin()->second.atime = 123;
        }

        auto result = cache.probe(hash, 0, 8192, context);
        ASSERT_EQ(result.file_blocks.size(), 2);
        ASSERT_NE(result.file_blocks[0], nullptr);
        EXPECT_EQ(result.file_blocks[0]->state(), FileBlock::State::DOWNLOADED);
        EXPECT_EQ(result.file_blocks[0]->range().left, 0);
        EXPECT_EQ(result.file_blocks[0]->range().right, 4095);
        EXPECT_EQ(result.file_blocks[1], nullptr);
        EXPECT_EQ(cache._cur_cache_size, cache_size_before);
        {
            std::lock_guard cache_lock(cache._mutex);
            EXPECT_EQ(cache._files.at(hash).begin()->second.atime, 123);
        }
    }
    fs::remove_all(path, error);
}

TEST_F(BlockFileCacheTest, ProbeAcceptsPreallocatedBlockCoveringFileTail) {
    const auto path = caches_dir / "block_file_cache_probe_preallocated_file_tail";
    std::error_code error;
    fs::remove_all(path, error);
    fs::create_directories(path);
    {
        BlockFileCache cache(path.string(), probe_cache_settings());
        ASSERT_TRUE(cache.initialize().ok());
        wait_until_cache_ready(cache);
        const auto hash = BlockFileCache::hash("probe_preallocated_file_tail");
        ReadStatistics stats;
        CacheContext context;
        context.stats = &stats;

        // File writers allocate a full block before the final buffer size is known. During that
        // window, the last cache block can extend beyond the actual file tail passed to probe().
        auto holder = cache.get_or_set(hash, 0, 8192, context);
        ASSERT_EQ(holder.file_blocks.size(), 2);
        const auto& preallocated_tail = *std::next(holder.file_blocks.begin());
        ASSERT_EQ(preallocated_tail->range().left, 4096);
        ASSERT_EQ(preallocated_tail->range().right, 8191);

        auto result = cache.probe(hash, 0, 6144, context);
        ASSERT_EQ(result.file_blocks.size(), 2);
        ASSERT_EQ(result.file_blocks[1], preallocated_tail);
        EXPECT_EQ(result.file_blocks[1]->range().left, 4096);
        EXPECT_EQ(result.file_blocks[1]->range().right, 8191);
    }
    fs::remove_all(path, error);
}

TEST_F(BlockFileCacheTest, ProbeTouchAndRemovalRevalidateTheRetainedBlock) {
    const auto path = caches_dir / "block_file_cache_probe_touch_remove";
    std::error_code error;
    fs::remove_all(path, error);
    fs::create_directories(path);
    const bool old_async_touch = config::enable_file_cache_async_touch_on_get_or_set;
    config::enable_file_cache_async_touch_on_get_or_set = false;
    Defer restore_config {
            [&]() { config::enable_file_cache_async_touch_on_get_or_set = old_async_touch; }};
    {
        BlockFileCache cache(path.string(), probe_cache_settings());
        ASSERT_TRUE(cache.initialize().ok());
        wait_until_cache_ready(cache);
        const auto hash = BlockFileCache::hash("probe_touch_remove");
        ReadStatistics stats;
        CacheContext context;
        context.stats = &stats;
        {
            auto holder = cache.get_or_set(hash, 0, 4096, context);
            ASSERT_EQ(holder.file_blocks.size(), 1);
            const auto& block = holder.file_blocks.front();
            ASSERT_EQ(block->get_or_set_downloader(), FileBlock::get_caller_id());
            const std::string payload(4096, 't');
            ASSERT_TRUE(block->append(Slice(payload.data(), payload.size())).ok());
            ASSERT_TRUE(block->finalize().ok());
        }
        {
            std::lock_guard cache_lock(cache._mutex);
            cache._files.at(hash).begin()->second.atime = 123;
        }

        fs::path cache_file;
        const uint64_t old_epoch = cache.async_write_service()->current_write_epoch();
        {
            auto probe_result = cache.probe(hash, 0, 4096, context);
            ASSERT_EQ(probe_result.file_blocks.size(), 1);
            const auto& block = probe_result.file_blocks[0];
            ASSERT_NE(block, nullptr);
            cache_file = block->get_cache_file();
            {
                std::lock_guard cache_lock(cache._mutex);
                EXPECT_EQ(cache._files.at(hash).begin()->second.atime, 123);
            }

            EXPECT_FALSE(cache.is_block_deleting(block));
            cache.touch_probe_block_if_cached(block, context);
            {
                std::lock_guard cache_lock(cache._mutex);
                EXPECT_NE(cache._files.at(hash).begin()->second.atime, 123);
            }

            cache.remove_if_cached(hash);
            EXPECT_EQ(cache.async_write_service()->current_write_epoch(), old_epoch + 1);
            EXPECT_TRUE(cache.is_block_deleting(block));
            cache.touch_probe_block_if_cached(block, context);
        }

        bool removed = false;
        for (int attempt = 0; attempt < 5000; ++attempt) {
            bool metadata_removed = false;
            {
                auto probe_result = cache.probe(hash, 0, 4096, context);
                metadata_removed = probe_result.file_blocks.size() == 1 &&
                                   probe_result.file_blocks[0] == nullptr;
            }
            if (metadata_removed && !fs::exists(cache_file)) {
                removed = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        EXPECT_TRUE(removed);
    }
    fs::remove_all(path, error);
}

TEST_F(BlockFileCacheTest, ProbeObservesDownloadingAndEmptyBlocksWithoutTakingDownloader) {
    const auto path = caches_dir / "block_file_cache_probe_states";
    std::error_code error;
    fs::remove_all(path, error);
    fs::create_directories(path);
    {
        BlockFileCache cache(path.string(), probe_cache_settings());
        ASSERT_TRUE(cache.initialize().ok());
        wait_until_cache_ready(cache);
        const auto hash = BlockFileCache::hash("probe_states");
        ReadStatistics stats;
        CacheContext context;
        context.stats = &stats;
        std::mutex mutex;
        std::condition_variable cv;
        bool downloader_ready = false;
        bool release_downloader = false;

        std::thread downloader([&]() {
            ReadStatistics downloader_stats;
            CacheContext downloader_context;
            downloader_context.stats = &downloader_stats;
            auto holder = cache.get_or_set(hash, 0, 6144, downloader_context);
            DORIS_CHECK(holder.file_blocks.size() == 2);
            DORIS_CHECK(holder.file_blocks.front()->get_or_set_downloader() ==
                        FileBlock::get_caller_id());
            {
                std::lock_guard lock(mutex);
                downloader_ready = true;
            }
            cv.notify_all();
            std::unique_lock lock(mutex);
            cv.wait(lock, [&]() { return release_downloader; });
        });
        Defer stop_downloader {[&]() {
            {
                std::lock_guard lock(mutex);
                release_downloader = true;
            }
            cv.notify_all();
            if (downloader.joinable()) {
                downloader.join();
            }
        }};
        bool ready = false;
        {
            std::unique_lock lock(mutex);
            ready = cv.wait_for(lock, std::chrono::seconds(5), [&]() { return downloader_ready; });
        }
        if (!ready) {
            FAIL() << "downloader thread did not become ready";
        }

        {
            auto result = cache.probe(hash, 0, 6144, context);
            ASSERT_EQ(result.file_blocks.size(), 2);
            ASSERT_NE(result.file_blocks[0], nullptr);
            ASSERT_NE(result.file_blocks[1], nullptr);
            EXPECT_EQ(result.file_blocks[0]->state(), FileBlock::State::DOWNLOADING);
            EXPECT_EQ(result.file_blocks[0]->range().left, 0);
            EXPECT_EQ(result.file_blocks[0]->range().right, 4095);
            EXPECT_EQ(result.file_blocks[1]->state(), FileBlock::State::EMPTY);
            EXPECT_EQ(result.file_blocks[1]->range().left, 4096);
            EXPECT_EQ(result.file_blocks[1]->range().right, 6143);
        }
    }
    fs::remove_all(path, error);
}

} // namespace
} // namespace doris::io
