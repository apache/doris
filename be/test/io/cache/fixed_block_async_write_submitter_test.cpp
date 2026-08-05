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

#include "io/cache/fixed_block_async_write_submitter.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include "common/config.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache_test_common.h"
#include "io/cache/inflight_write_buffer_index.h"
#include "util/time.h"

namespace doris::io {
namespace {

constexpr size_t BLOCK_SIZE = 4096;

FileCacheSettings submitter_cache_settings() {
    FileCacheSettings settings;
    settings.query_queue_size = 4_mb;
    settings.query_queue_elements = 1024;
    settings.index_queue_size = 1_mb;
    settings.index_queue_elements = 256;
    settings.disposable_queue_size = 1_mb;
    settings.disposable_queue_elements = 256;
    settings.capacity = 8_mb;
    settings.max_file_block_size = BLOCK_SIZE;
    settings.max_query_cache_size = 0;
    return settings;
}

class FixedBlockAsyncWriteSubmitterTest : public BlockFileCacheTest {
protected:
    void SetUp() override {
        _old_block_size = config::file_cache_each_block_size;
        _old_inflight_enabled = config::enable_async_file_cache_write_inflight_write_buffer_index;
        config::file_cache_each_block_size = BLOCK_SIZE;
        config::enable_async_file_cache_write_inflight_write_buffer_index = true;
    }

    void TearDown() override {
        config::file_cache_each_block_size = _old_block_size;
        config::enable_async_file_cache_write_inflight_write_buffer_index = _old_inflight_enabled;
        for (const auto& path : _paths) {
            std::error_code error;
            std::filesystem::remove_all(path, error);
        }
    }

    std::unique_ptr<BlockFileCache> create_cache(const std::string& name,
                                                 bool start_manager = true) {
        auto path = caches_dir / name;
        std::error_code error;
        std::filesystem::remove_all(path, error);
        std::filesystem::create_directories(path);
        _paths.emplace_back(path);
        auto cache = std::make_unique<BlockFileCache>(path.string(), submitter_cache_settings());
        EXPECT_TRUE(cache->initialize().ok());
        wait_until_cache_ready(*cache);
        if (start_manager) {
            EXPECT_TRUE(cache->async_write_manager()->start().ok());
        }
        return cache;
    }

    FixedBlockSubmitRequest request(BlockFileCache* cache, const std::string& key,
                                    const std::string& payload, size_t block_offset = 0,
                                    size_t file_size = BLOCK_SIZE) {
        const auto cache_hash = BlockFileCache::hash(key);
        return FixedBlockSubmitRequest {
                .cache = cache,
                .cache_hash = cache_hash,
                .block_offset = block_offset,
                .valid_size = payload.size(),
                .file_size = file_size,
                .complete_payload = Slice(payload.data(), payload.size()),
                .admission_ctx = {},
                .write_epoch = cache->async_write_manager()->current_write_epoch(cache_hash),
        };
    }

    static void wait_for_writes(BlockFileCache* cache) {
        for (size_t attempt = 0;
             attempt < 500 && cache->async_write_manager()->pending_count() != 0; ++attempt) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        ASSERT_EQ(cache->async_write_manager()->pending_count(), 0);
    }

private:
    int64_t _old_block_size = 0;
    bool _old_inflight_enabled = false;
    std::vector<std::filesystem::path> _paths;
};

TEST_F(FixedBlockAsyncWriteSubmitterTest, SubmitsFullAndShortEofBlocks) {
    auto cache = create_cache("fixed_block_submitter_full_and_eof");
    const std::string full_payload(BLOCK_SIZE, 'a');
    auto full_request = request(cache.get(), "full", full_payload);
    EXPECT_EQ(FixedBlockAsyncWriteSubmitter::try_submit(full_request),
              FixedBlockSubmitResult::SUBMITTED);

    constexpr size_t eof_size = 777;
    const std::string eof_payload(eof_size, 'b');
    auto eof_request = request(cache.get(), "eof", eof_payload, BLOCK_SIZE, BLOCK_SIZE + eof_size);
    EXPECT_EQ(FixedBlockAsyncWriteSubmitter::try_submit(eof_request),
              FixedBlockSubmitResult::SUBMITTED);
    wait_for_writes(cache.get());
    EXPECT_EQ(cache->inflight_write_buffer_index()->count(), 0);

    ReadStatistics read_stats;
    CacheContext cache_context;
    cache_context.stats = &read_stats;
    FileBlocks blocks;
    bool fully_covered = false;
    ASSERT_TRUE(cache->get_downloaded_blocks_if_fully_covered(full_request.cache_hash, 0,
                                                              BLOCK_SIZE, cache_context, &blocks,
                                                              &fully_covered)
                        .ok());
    ASSERT_TRUE(fully_covered);
    std::string actual_full(BLOCK_SIZE, '\0');
    ASSERT_TRUE(blocks.front()->read(Slice(actual_full.data(), actual_full.size()), 0).ok());
    EXPECT_EQ(actual_full, full_payload);

    blocks.clear();
    fully_covered = false;
    ASSERT_TRUE(cache->get_downloaded_blocks_if_fully_covered(eof_request.cache_hash, BLOCK_SIZE,
                                                              eof_size, cache_context, &blocks,
                                                              &fully_covered)
                        .ok());
    ASSERT_TRUE(fully_covered);
    std::string actual_eof(eof_size, '\0');
    ASSERT_TRUE(blocks.front()->read(Slice(actual_eof.data(), actual_eof.size()), 0).ok());
    EXPECT_EQ(actual_eof, eof_payload);
}

TEST_F(FixedBlockAsyncWriteSubmitterTest, RejectsStaleAndExistingInflightOwnership) {
    auto cache = create_cache("fixed_block_submitter_epoch_and_inflight");
    const std::string payload(BLOCK_SIZE, 'c');
    auto stale_request = request(cache.get(), "stale", payload);
    cache->async_write_manager()->invalidate_pending_writes(stale_request.cache_hash);
    EXPECT_EQ(FixedBlockAsyncWriteSubmitter::try_submit(stale_request),
              FixedBlockSubmitResult::STALE_EPOCH);

    auto existing_request = request(cache.get(), "existing", payload);
    AsyncCacheWriteBufferPtr existing_buffer;
    ASSERT_TRUE(cache->async_write_manager()
                        ->allocate_tracked_buffer(BLOCK_SIZE, &existing_buffer)
                        .ok());
    auto existing_entry = std::make_shared<InflightWriteBufferEntry>(existing_buffer, 0, BLOCK_SIZE,
                                                                     MonotonicMicros());
    ASSERT_EQ(cache->inflight_write_buffer_index()->insert_if_absent(existing_request.cache_hash, 0,
                                                                     existing_entry),
              nullptr);
    EXPECT_EQ(FixedBlockAsyncWriteSubmitter::try_submit(existing_request),
              FixedBlockSubmitResult::EXISTING_INFLIGHT);
    EXPECT_EQ(cache->async_write_manager()->pending_count(), 0);
    EXPECT_TRUE(cache->inflight_write_buffer_index()->remove_if(existing_request.cache_hash, 0,
                                                                existing_entry));
}

TEST_F(FixedBlockAsyncWriteSubmitterTest, FinalProbeSkipsDownloadedAndDownloadingBlocks) {
    auto cache = create_cache("fixed_block_submitter_probe_states");
    const std::string payload(BLOCK_SIZE, 'd');
    auto downloaded_request = request(cache.get(), "downloaded", payload);
    ReadStatistics read_stats;
    CacheContext cache_context;
    cache_context.stats = &read_stats;
    auto downloaded_holder =
            cache->get_or_set(downloaded_request.cache_hash, 0, BLOCK_SIZE, cache_context);
    complete_into_memory(downloaded_holder);
    EXPECT_EQ(FixedBlockAsyncWriteSubmitter::try_submit(downloaded_request),
              FixedBlockSubmitResult::ALREADY_DOWNLOADED);

    auto downloading_request = request(cache.get(), "downloading", payload);
    auto downloading_holder =
            cache->get_or_set(downloading_request.cache_hash, 0, BLOCK_SIZE, cache_context);
    ASSERT_EQ(downloading_holder.file_blocks.size(), 1);
    ASSERT_EQ(downloading_holder.file_blocks.front()->get_or_set_downloader(),
              FileBlock::get_caller_id());
    EXPECT_EQ(FixedBlockAsyncWriteSubmitter::try_submit(downloading_request),
              FixedBlockSubmitResult::CACHE_DOWNLOADING);
    EXPECT_EQ(cache->async_write_manager()->pending_count(), 0);
}

TEST_F(FixedBlockAsyncWriteSubmitterTest, AllocationFailureAndBackpressurePublishNoInflightEntry) {
    auto cache = create_cache("fixed_block_submitter_failures");
    const std::string payload(BLOCK_SIZE, 'e');
    auto allocation_request = request(cache.get(), "allocation_failure", payload);
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "AsyncCacheWriteManager::allocate_tracked_buffer:inject_failure",
            [&](auto&& values) {
                auto* status = try_any_cast<Status*>(values.back());
                *status = Status::MemoryAllocFailed("injected allocation failure");
            },
            &guard);
    sync_point->enable_processing();
    EXPECT_EQ(FixedBlockAsyncWriteSubmitter::try_submit(allocation_request),
              FixedBlockSubmitResult::ALLOC_FAILED);
    sync_point->disable_processing();
    sync_point->clear_all_call_backs();
    EXPECT_EQ(cache->inflight_write_buffer_index()->count(), 0);

    auto stopped_cache = create_cache("fixed_block_submitter_backpressure", false);
    auto backpressure_request = request(stopped_cache.get(), "backpressure", payload);
    EXPECT_EQ(FixedBlockAsyncWriteSubmitter::try_submit(backpressure_request),
              FixedBlockSubmitResult::BACKPRESSURE);
    EXPECT_EQ(stopped_cache->inflight_write_buffer_index()->count(), 0);
}

} // namespace
} // namespace doris::io
