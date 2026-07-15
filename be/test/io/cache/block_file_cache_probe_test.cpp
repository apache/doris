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

        auto result = cache.probe(hash, 1024, 4096, context);
        EXPECT_TRUE(result.holder.file_blocks.empty());
        ASSERT_EQ(result.gaps.size(), 1);
        EXPECT_EQ(result.gaps.front().left, 1024);
        EXPECT_EQ(result.gaps.front().right, 5119);
        EXPECT_EQ(cache._cur_cache_size, cache_size_before);
        std::lock_guard cache_lock(cache._mutex);
        EXPECT_EQ(cache._files.find(hash), cache._files.end());
    }
    fs::remove_all(path, error);
}

TEST_F(BlockFileCacheTest, ProbeReturnsDownloadedBlockAndIndependentGap) {
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
        ASSERT_EQ(result.holder.file_blocks.size(), 1);
        EXPECT_EQ(result.holder.file_blocks.front()->state(), FileBlock::State::DOWNLOADED);
        EXPECT_EQ(result.holder.file_blocks.front()->range().left, 0);
        EXPECT_EQ(result.holder.file_blocks.front()->range().right, 4095);
        ASSERT_EQ(result.gaps.size(), 1);
        EXPECT_EQ(result.gaps.front().left, 4096);
        EXPECT_EQ(result.gaps.front().right, 8191);
        EXPECT_EQ(cache._cur_cache_size, cache_size_before);
        {
            std::lock_guard cache_lock(cache._mutex);
            EXPECT_EQ(cache._files.at(hash).begin()->second.atime, 123);
        }
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
            auto holder = cache.get_or_set(hash, 0, 8192, downloader_context);
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
            auto result = cache.probe(hash, 0, 8192, context);
            ASSERT_EQ(result.holder.file_blocks.size(), 2);
            auto iterator = result.holder.file_blocks.begin();
            EXPECT_EQ((*iterator)->state(), FileBlock::State::DOWNLOADING);
            ++iterator;
            EXPECT_EQ((*iterator)->state(), FileBlock::State::EMPTY);
            EXPECT_TRUE(result.gaps.empty());
        }
    }
    fs::remove_all(path, error);
}

} // namespace
} // namespace doris::io
