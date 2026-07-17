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

#include "io/cache/async_cache_write_service.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <condition_variable>
#include <cstring>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache_test_common.h"
#include "util/defer_op.h"
#include "util/time.h"

namespace doris::io {
namespace {

FileCacheSettings async_write_cache_settings() {
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

class AsyncCacheWriteServiceTest : public BlockFileCacheTest {
protected:
    std::unique_ptr<BlockFileCache> create_cache(const std::string& name) {
        auto path = caches_dir / name;
        std::error_code error;
        fs::remove_all(path, error);
        fs::create_directories(path);
        _paths.emplace_back(path);
        auto cache = std::make_unique<BlockFileCache>(path.string(), async_write_cache_settings());
        EXPECT_TRUE(cache->initialize().ok());
        wait_until_cache_ready(*cache);
        EXPECT_TRUE(cache->async_write_service()->start().ok());
        return cache;
    }

    void TearDown() override {
        for (const auto& path : _paths) {
            std::error_code error;
            fs::remove_all(path, error);
        }
    }

private:
    std::vector<fs::path> _paths;
};

TEST_F(AsyncCacheWriteServiceTest, TaskWritesDownloadedBlockAndCleansInflightEntry) {
    auto cache = create_cache("async_write_service_single_task");
    auto* service = cache->async_write_service();
    auto* index = cache->inflight_write_buffer_index();
    ASSERT_NE(service, nullptr);
    ASSERT_NE(index, nullptr);

    constexpr size_t block_size = 4096;
    const auto hash = BlockFileCache::hash("async_single_task");
    const std::string payload(block_size, 'a');
    const int64_t baseline_memory = service->buffer_memory_bytes();
    AsyncCacheWriteBufferPtr buffer;
    ASSERT_TRUE(service->allocate_tracked_buffer(block_size, &buffer).ok());
    memcpy(buffer->data(), payload.data(), payload.size());
    EXPECT_GE(service->buffer_memory_bytes(), baseline_memory + static_cast<int64_t>(block_size));

    const uint64_t epoch = service->current_write_epoch();
    auto entry = std::make_shared<InflightWriteBufferEntry>(buffer, 0, block_size,
                                                            MonotonicMicros(), epoch);
    ASSERT_EQ(index->insert_if_absent(hash, 0, entry), nullptr);
    std::promise<void> finished;
    auto finished_future = finished.get_future();
    AsyncCacheWriteTask task {
            .cache_hash = hash,
            .file_offset = 0,
            .file_size = block_size,
            .buffer = buffer,
            .admission_ctx = {},
            .submit_ts_us = MonotonicMicros(),
            .write_epoch = epoch,
            .on_finalized =
                    [index, hash, entry, &finished](const AsyncCacheWriteTask&) {
                        index->remove_if(hash, 0, entry);
                        finished.set_value();
                    },
    };
    ASSERT_TRUE(service->try_submit(std::move(task)));
    ASSERT_EQ(finished_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_EQ(service->pending_count(), 0);
    EXPECT_EQ(index->lookup(hash, 0, epoch), nullptr);

    ReadStatistics read_stats;
    CacheContext context;
    context.stats = &read_stats;
    FileBlocks blocks;
    bool fully_covered = false;
    ASSERT_TRUE(cache->get_downloaded_blocks_if_fully_covered(hash, 0, block_size, context, &blocks,
                                                              &fully_covered)
                        .ok());
    ASSERT_TRUE(fully_covered);
    ASSERT_EQ(blocks.size(), 1);
    std::string actual(block_size, '\0');
    ASSERT_TRUE(blocks.front()->read(Slice(actual.data(), actual.size()), 0).ok());
    EXPECT_EQ(actual, payload);

    blocks.clear();
    entry.reset();
    buffer.reset();
    for (int attempt = 0; attempt < 100 && service->buffer_memory_bytes() != baseline_memory;
         ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(service->buffer_memory_bytes(), baseline_memory);
}

TEST_F(AsyncCacheWriteServiceTest, PendingLimitRejectsWhileWorkerIsActive) {
    auto cache = create_cache("async_write_service_backpressure");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);
    auto options = service->options();
    options.max_pending_tasks = 1;
    ASSERT_TRUE(service->update_options(options).ok());

    std::mutex mutex;
    std::condition_variable cv;
    bool worker_entered = false;
    bool release_worker = false;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "AsyncCacheWriteService::_write_one:before_get_or_set",
            [&](auto&&) {
                std::unique_lock lock(mutex);
                worker_entered = true;
                cv.notify_all();
                cv.wait(lock, [&]() { return release_worker; });
            },
            &guard);
    sync_point->enable_processing();
    Defer clear_sync_point {[&]() {
        {
            std::lock_guard lock(mutex);
            release_worker = true;
        }
        cv.notify_all();
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    }};

    AsyncCacheWriteBufferPtr first_buffer;
    ASSERT_TRUE(service->allocate_tracked_buffer(4096, &first_buffer).ok());
    memset(first_buffer->data(), 'b', first_buffer->size());
    std::promise<void> first_finished;
    auto first_future = first_finished.get_future();
    AsyncCacheWriteTask first_task {
            .cache_hash = BlockFileCache::hash("backpressure_first"),
            .file_offset = 0,
            .file_size = first_buffer->size(),
            .buffer = first_buffer,
            .admission_ctx = {},
            .submit_ts_us = MonotonicMicros(),
            .write_epoch = service->current_write_epoch(),
            .on_finalized =
                    [&first_finished](const AsyncCacheWriteTask&) { first_finished.set_value(); },
    };
    ASSERT_TRUE(service->try_submit(std::move(first_task)));
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&]() { return worker_entered; }));
    }

    AsyncCacheWriteBufferPtr rejected_buffer;
    ASSERT_TRUE(service->allocate_tracked_buffer(4096, &rejected_buffer).ok());
    AsyncCacheWriteTask rejected_task {
            .cache_hash = BlockFileCache::hash("backpressure_rejected"),
            .file_offset = 0,
            .file_size = rejected_buffer->size(),
            .buffer = rejected_buffer,
            .admission_ctx = {},
            .submit_ts_us = MonotonicMicros(),
            .write_epoch = service->current_write_epoch(),
            .on_finalized = nullptr,
    };
    EXPECT_FALSE(service->try_submit(std::move(rejected_task)));
    EXPECT_EQ(service->pending_count(), 1);
    EXPECT_GE(service->stats().rejected, 1);

    {
        std::lock_guard lock(mutex);
        release_worker = true;
    }
    cv.notify_all();
    ASSERT_EQ(first_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_EQ(service->pending_count(), 0);
}

TEST_F(AsyncCacheWriteServiceTest, WatchdogDropsExpiredTaskAndCleansInflightEntry) {
    auto cache = create_cache("async_write_service_watchdog");
    auto* service = cache->async_write_service();
    auto* index = cache->inflight_write_buffer_index();
    ASSERT_NE(service, nullptr);
    ASSERT_NE(index, nullptr);
    auto options = service->options();
    options.watchdog_warn_secs = 0;
    options.watchdog_drop_secs = 1;
    ASSERT_TRUE(service->update_options(options).ok());

    const auto hash = BlockFileCache::hash("watchdog_drop");
    AsyncCacheWriteBufferPtr buffer;
    ASSERT_TRUE(service->allocate_tracked_buffer(4096, &buffer).ok());
    memset(buffer->data(), 'w', buffer->size());
    const uint64_t epoch = service->current_write_epoch();
    auto entry = std::make_shared<InflightWriteBufferEntry>(buffer, 0, buffer->size(),
                                                            MonotonicMicros(), epoch);
    ASSERT_EQ(index->insert_if_absent(hash, 0, entry), nullptr);
    std::promise<void> finished;
    auto finished_future = finished.get_future();
    AsyncCacheWriteTask task {
            .cache_hash = hash,
            .file_offset = 0,
            .file_size = buffer->size(),
            .buffer = buffer,
            .admission_ctx = {},
            .submit_ts_us = MonotonicMicros() - 2 * 1000 * 1000,
            .write_epoch = epoch,
            .on_finalized =
                    [index, hash, entry, &finished](const AsyncCacheWriteTask&) {
                        index->remove_if(hash, 0, entry);
                        finished.set_value();
                    },
    };
    ASSERT_TRUE(service->try_submit(std::move(task)));
    ASSERT_EQ(finished_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_EQ(service->pending_count(), 0);
    EXPECT_EQ(index->lookup(hash, 0, epoch), nullptr);
    EXPECT_GE(service->stats().watchdog_timeout, 1);

    ReadStatistics read_stats;
    CacheContext context;
    context.stats = &read_stats;
    auto probe_result = cache->probe(hash, 0, 4096, context);
    ASSERT_EQ(probe_result.file_blocks.size(), 1);
    EXPECT_EQ(probe_result.file_blocks[0], nullptr);
}

TEST_F(AsyncCacheWriteServiceTest, ShutdownDrainsAcceptedTask) {
    auto cache = create_cache("async_write_service_shutdown_drain");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);

    const auto hash = BlockFileCache::hash("shutdown_drain");
    AsyncCacheWriteBufferPtr buffer;
    ASSERT_TRUE(service->allocate_tracked_buffer(4096, &buffer).ok());
    memset(buffer->data(), 's', buffer->size());
    std::promise<void> finished;
    auto finished_future = finished.get_future();
    AsyncCacheWriteTask task {
            .cache_hash = hash,
            .file_offset = 0,
            .file_size = buffer->size(),
            .buffer = buffer,
            .admission_ctx = {},
            .submit_ts_us = MonotonicMicros(),
            .write_epoch = service->current_write_epoch(),
            .on_finalized = [&finished](const AsyncCacheWriteTask&) { finished.set_value(); },
    };
    ASSERT_TRUE(service->try_submit(std::move(task)));
    service->shutdown();
    ASSERT_EQ(finished_future.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_EQ(service->pending_count(), 0);

    ReadStatistics read_stats;
    CacheContext context;
    context.stats = &read_stats;
    FileBlocks blocks;
    bool fully_covered = false;
    ASSERT_TRUE(cache->get_downloaded_blocks_if_fully_covered(hash, 0, 4096, context, &blocks,
                                                              &fully_covered)
                        .ok());
    EXPECT_TRUE(fully_covered);
    ASSERT_EQ(blocks.size(), 1);
    std::string actual(4096, '\0');
    ASSERT_TRUE(blocks.front()->read(Slice(actual.data(), actual.size()), 0).ok());
    EXPECT_EQ(actual, std::string(4096, 's'));
}

TEST_F(AsyncCacheWriteServiceTest, OneTaskWritesMultipleContainedCells) {
    auto cache = create_cache("async_write_service_multiple_cells");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);

    constexpr size_t cell_size = 4096;
    constexpr size_t task_size = cell_size * 2;
    const auto hash = BlockFileCache::hash("multiple_cells");
    AsyncCacheWriteBufferPtr buffer;
    ASSERT_TRUE(service->allocate_tracked_buffer(task_size, &buffer).ok());
    memset(buffer->data(), 'm', cell_size);
    memset(buffer->data() + cell_size, 'n', cell_size);
    std::promise<void> finished;
    auto finished_future = finished.get_future();
    AsyncCacheWriteTask task {
            .cache_hash = hash,
            .file_offset = 0,
            .file_size = task_size,
            .buffer = buffer,
            .admission_ctx = {},
            .submit_ts_us = MonotonicMicros(),
            .write_epoch = service->current_write_epoch(),
            .on_finalized = [&finished](const AsyncCacheWriteTask&) { finished.set_value(); },
    };
    ASSERT_TRUE(service->try_submit(std::move(task)));
    ASSERT_EQ(finished_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);

    ReadStatistics read_stats;
    CacheContext context;
    context.stats = &read_stats;
    FileBlocks blocks;
    bool fully_covered = false;
    ASSERT_TRUE(cache->get_downloaded_blocks_if_fully_covered(hash, 0, task_size, context, &blocks,
                                                              &fully_covered)
                        .ok());
    EXPECT_TRUE(fully_covered);
    ASSERT_EQ(blocks.size(), 2);
    auto iterator = blocks.begin();
    std::string first(cell_size, '\0');
    ASSERT_TRUE((*iterator)->read(Slice(first.data(), first.size()), 0).ok());
    EXPECT_EQ(first, std::string(cell_size, 'm'));
    ++iterator;
    std::string second(cell_size, '\0');
    ASSERT_TRUE((*iterator)->read(Slice(second.data(), second.size()), 0).ok());
    EXPECT_EQ(second, std::string(cell_size, 'n'));
}

TEST_F(AsyncCacheWriteServiceTest, ExistingAndDeletingCellsKeepTheirOwners) {
    auto cache = create_cache("async_write_service_existing_states");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);

    constexpr size_t cell_size = 4096;
    constexpr size_t task_size = cell_size * 3;
    const auto hash = BlockFileCache::hash("existing_states");
    ReadStatistics read_stats;
    CacheContext context;
    context.stats = &read_stats;
    auto holder = cache->get_or_set(hash, 0, task_size, context);
    ASSERT_EQ(holder.file_blocks.size(), 3);
    auto iterator = holder.file_blocks.begin();
    const auto downloaded_block = *iterator;
    ++iterator;
    const auto downloading_block = *iterator;
    ++iterator;
    const auto deleting_block = *iterator;

    ASSERT_EQ(downloaded_block->get_or_set_downloader(), FileBlock::get_caller_id());
    const std::string original(cell_size, 'x');
    ASSERT_TRUE(downloaded_block->append(Slice(original.data(), original.size())).ok());
    ASSERT_TRUE(downloaded_block->finalize().ok());
    ASSERT_EQ(downloading_block->get_or_set_downloader(), FileBlock::get_caller_id());
    deleting_block->set_deleting();

    AsyncCacheWriteBufferPtr buffer;
    ASSERT_TRUE(service->allocate_tracked_buffer(task_size, &buffer).ok());
    memset(buffer->data(), 'y', buffer->size());
    std::promise<void> finished;
    auto finished_future = finished.get_future();
    AsyncCacheWriteTask task {
            .cache_hash = hash,
            .file_offset = 0,
            .file_size = task_size,
            .buffer = buffer,
            .admission_ctx = {},
            .submit_ts_us = MonotonicMicros(),
            .write_epoch = service->current_write_epoch(),
            .on_finalized = [&finished](const AsyncCacheWriteTask&) { finished.set_value(); },
    };
    ASSERT_TRUE(service->try_submit(std::move(task)));
    ASSERT_EQ(finished_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_EQ(service->pending_count(), 0);
    EXPECT_GE(service->stats().skip_downloaded, 1);
    EXPECT_GE(service->stats().skip_downloading, 1);
    EXPECT_GE(service->stats().skip_deleting, 1);

    std::string actual(cell_size, '\0');
    ASSERT_TRUE(downloaded_block->read(Slice(actual.data(), actual.size()), 0).ok());
    EXPECT_EQ(actual, original);
    EXPECT_EQ(downloading_block->state(), FileBlock::State::DOWNLOADING);
    EXPECT_EQ(downloading_block->get_downloader(), FileBlock::get_caller_id());
    EXPECT_TRUE(cache->is_block_deleting(deleting_block));
}

TEST_F(AsyncCacheWriteServiceTest, RemoveDuringAppendDoesNotLeaveResurrectedCacheData) {
    auto cache = create_cache("async_write_service_remove_during_append");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);

    std::mutex mutex;
    std::condition_variable cv;
    bool before_append = false;
    bool release_worker = false;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "AsyncCacheWriteService::_write_one:before_append",
            [&](auto&&) {
                std::unique_lock lock(mutex);
                before_append = true;
                cv.notify_all();
                cv.wait(lock, [&]() { return release_worker; });
            },
            &guard);
    sync_point->enable_processing();
    Defer clear_sync_point {[&]() {
        {
            std::lock_guard lock(mutex);
            release_worker = true;
        }
        cv.notify_all();
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    }};

    const auto hash = BlockFileCache::hash("remove_during_append");
    AsyncCacheWriteBufferPtr buffer;
    ASSERT_TRUE(service->allocate_tracked_buffer(4096, &buffer).ok());
    memset(buffer->data(), 'r', buffer->size());
    std::promise<void> finished;
    auto finished_future = finished.get_future();
    AsyncCacheWriteTask task {
            .cache_hash = hash,
            .file_offset = 0,
            .file_size = buffer->size(),
            .buffer = buffer,
            .admission_ctx = {},
            .submit_ts_us = MonotonicMicros(),
            .write_epoch = service->current_write_epoch(),
            .on_finalized = [&finished](const AsyncCacheWriteTask&) { finished.set_value(); },
    };
    ASSERT_TRUE(service->try_submit(std::move(task)));
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&]() { return before_append; }));
    }

    fs::path cache_file;
    {
        ReadStatistics probe_stats;
        CacheContext probe_context;
        probe_context.stats = &probe_stats;
        auto probe_result = cache->probe(hash, 0, 4096, probe_context);
        ASSERT_EQ(probe_result.file_blocks.size(), 1);
        ASSERT_NE(probe_result.file_blocks[0], nullptr);
        EXPECT_EQ(probe_result.file_blocks[0]->state(), FileBlock::State::DOWNLOADING);
        cache_file = probe_result.file_blocks[0]->get_cache_file();
    }
    const uint64_t old_epoch = service->current_write_epoch();
    cache->remove_if_cached_async(hash);
    EXPECT_EQ(service->current_write_epoch(), old_epoch + 1);
    {
        std::lock_guard lock(mutex);
        release_worker = true;
    }
    cv.notify_all();
    ASSERT_EQ(finished_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_EQ(service->pending_count(), 0);

    bool removed = false;
    for (int attempt = 0; attempt < 5000; ++attempt) {
        ReadStatistics probe_stats;
        CacheContext probe_context;
        probe_context.stats = &probe_stats;
        bool metadata_removed = false;
        {
            auto probe_result = cache->probe(hash, 0, 4096, probe_context);
            metadata_removed =
                    probe_result.file_blocks.size() == 1 && probe_result.file_blocks[0] == nullptr;
        }
        if (metadata_removed && !fs::exists(cache_file)) {
            removed = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_TRUE(removed);
}

TEST_F(AsyncCacheWriteServiceTest, PartialOverlapIsSkippedWithoutOutOfBoundsWrite) {
    auto cache = create_cache("async_write_service_partial_overlap");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);

    constexpr size_t existing_size = 4096;
    const auto hash = BlockFileCache::hash("partial_overlap");
    ReadStatistics read_stats;
    CacheContext context;
    context.stats = &read_stats;
    {
        auto holder = cache->get_or_set(hash, 0, existing_size, context);
        ASSERT_EQ(holder.file_blocks.size(), 1);
        const auto& block = holder.file_blocks.front();
        ASSERT_EQ(block->get_or_set_downloader(), FileBlock::get_caller_id());
        const std::string payload(existing_size, 'x');
        ASSERT_TRUE(block->append(Slice(payload.data(), payload.size())).ok());
        ASSERT_TRUE(block->finalize().ok());
    }

    constexpr size_t task_offset = 1024;
    constexpr size_t task_size = 4096;
    AsyncCacheWriteBufferPtr buffer;
    ASSERT_TRUE(service->allocate_tracked_buffer(task_size, &buffer).ok());
    memset(buffer->data(), 'y', buffer->size());
    std::promise<void> finished;
    auto finished_future = finished.get_future();
    AsyncCacheWriteTask task {
            .cache_hash = hash,
            .file_offset = task_offset,
            .file_size = task_size,
            .buffer = buffer,
            .admission_ctx = {},
            .submit_ts_us = MonotonicMicros(),
            .write_epoch = service->current_write_epoch(),
            .on_finalized = [&finished](const AsyncCacheWriteTask&) { finished.set_value(); },
    };
    ASSERT_TRUE(service->try_submit(std::move(task)));
    ASSERT_EQ(finished_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_GE(service->stats().skip_partial_overlap, 1);

    FileBlocks blocks;
    bool fully_covered = false;
    ASSERT_TRUE(cache->get_downloaded_blocks_if_fully_covered(hash, 0, task_offset + task_size,
                                                              context, &blocks, &fully_covered)
                        .ok());
    EXPECT_TRUE(fully_covered);
    ASSERT_EQ(blocks.size(), 2);
    auto iterator = blocks.begin();
    std::string existing(existing_size, '\0');
    ASSERT_TRUE((*iterator)->read(Slice(existing.data(), existing.size()), 0).ok());
    EXPECT_EQ(existing, std::string(existing_size, 'x'));
    ++iterator;
    std::string tail(task_offset, '\0');
    ASSERT_TRUE((*iterator)->read(Slice(tail.data(), tail.size()), 0).ok());
    EXPECT_EQ(tail, std::string(task_offset, 'y'));
}

TEST_F(AsyncCacheWriteServiceTest, RemoveInvalidatesActiveAndQueuedTasksAndCleansEmptyCells) {
    auto cache = create_cache("async_write_service_epoch");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);
    auto options = service->options();
    options.worker_count = 1;
    ASSERT_TRUE(service->update_options(options).ok());

    std::mutex mutex;
    std::condition_variable cv;
    bool worker_entered = false;
    bool release_worker = false;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "AsyncCacheWriteService::_write_one:before_get_or_set",
            [&](auto&&) {
                std::unique_lock lock(mutex);
                worker_entered = true;
                cv.notify_all();
                cv.wait(lock, [&]() { return release_worker; });
            },
            &guard);
    sync_point->enable_processing();
    Defer clear_sync_point {[&]() {
        {
            std::lock_guard lock(mutex);
            release_worker = true;
        }
        cv.notify_all();
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    }};

    const auto active_hash = BlockFileCache::hash("epoch_drop_active");
    AsyncCacheWriteBufferPtr active_buffer;
    ASSERT_TRUE(service->allocate_tracked_buffer(4096, &active_buffer).ok());
    memset(active_buffer->data(), 'a', active_buffer->size());
    std::promise<void> active_finished;
    auto active_future = active_finished.get_future();
    AsyncCacheWriteTask active_task {
            .cache_hash = active_hash,
            .file_offset = 0,
            .file_size = active_buffer->size(),
            .buffer = active_buffer,
            .admission_ctx = {},
            .submit_ts_us = MonotonicMicros(),
            .write_epoch = service->current_write_epoch(),
            .on_finalized =
                    [&active_finished](const AsyncCacheWriteTask&) { active_finished.set_value(); },
    };
    ASSERT_TRUE(service->try_submit(std::move(active_task)));
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&]() { return worker_entered; }));
    }

    const auto queued_hash = BlockFileCache::hash("epoch_drop_queued");
    AsyncCacheWriteBufferPtr queued_buffer;
    ASSERT_TRUE(service->allocate_tracked_buffer(4096, &queued_buffer).ok());
    memset(queued_buffer->data(), 'q', queued_buffer->size());
    std::promise<void> queued_finished;
    auto queued_future = queued_finished.get_future();
    AsyncCacheWriteTask queued_task {
            .cache_hash = queued_hash,
            .file_offset = 0,
            .file_size = queued_buffer->size(),
            .buffer = queued_buffer,
            .admission_ctx = {},
            .submit_ts_us = MonotonicMicros(),
            .write_epoch = service->current_write_epoch(),
            .on_finalized =
                    [&queued_finished](const AsyncCacheWriteTask&) { queued_finished.set_value(); },
    };
    ASSERT_TRUE(service->try_submit(std::move(queued_task)));
    ASSERT_EQ(service->pending_count(), 2);

    const uint64_t old_epoch = service->current_write_epoch();
    cache->remove_if_cached_async(active_hash);
    EXPECT_EQ(service->current_write_epoch(), old_epoch + 1);
    {
        std::lock_guard lock(mutex);
        release_worker = true;
    }
    cv.notify_all();
    ASSERT_EQ(active_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    ASSERT_EQ(queued_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_EQ(service->pending_count(), 0);
    EXPECT_GE(service->stats().drop_stale_epoch, 2);

    ReadStatistics read_stats;
    CacheContext context;
    context.stats = &read_stats;
    const auto expect_cache_gap = [&](const UInt128Wrapper& hash) {
        auto probe_result = cache->probe(hash, 0, 4096, context);
        ASSERT_EQ(probe_result.file_blocks.size(), 1);
        EXPECT_EQ(probe_result.file_blocks[0], nullptr);
    };
    expect_cache_gap(active_hash);
    expect_cache_gap(queued_hash);
}

TEST_F(AsyncCacheWriteServiceTest, UpdateOptionsValidatesAndAppliesAtRuntime) {
    auto cache = create_cache("async_write_service_resize");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);

    auto options = service->options();
    auto invalid_options = options;
    invalid_options.worker_count = 0;
    EXPECT_TRUE(service->update_options(invalid_options).is<ErrorCode::INVALID_ARGUMENT>());
    invalid_options = options;
    invalid_options.max_pending_tasks = 0;
    EXPECT_TRUE(service->update_options(invalid_options).is<ErrorCode::INVALID_ARGUMENT>());
    invalid_options = options;
    invalid_options.batch_size = 0;
    EXPECT_TRUE(service->update_options(invalid_options).is<ErrorCode::INVALID_ARGUMENT>());
    invalid_options = options;
    invalid_options.watchdog_drop_secs = invalid_options.watchdog_warn_secs;
    EXPECT_TRUE(service->update_options(invalid_options).is<ErrorCode::INVALID_ARGUMENT>());

    options.worker_count = 3;
    options.max_pending_tasks = 7;
    options.batch_size = 2;
    options.watchdog_warn_secs = 1;
    options.watchdog_drop_secs = 2;
    ASSERT_TRUE(service->update_options(options).ok());
    const auto updated = service->options();
    EXPECT_EQ(updated.worker_count, 3);
    EXPECT_EQ(updated.max_pending_tasks, 7);
    EXPECT_EQ(updated.batch_size, 2);
    EXPECT_EQ(updated.watchdog_warn_secs, 1);
    EXPECT_EQ(updated.watchdog_drop_secs, 2);
    EXPECT_EQ(service->_desired_worker_count.load(std::memory_order_acquire), 3);

    options.worker_count = 1;
    ASSERT_TRUE(service->update_options(options).ok());
    EXPECT_EQ(service->_desired_worker_count.load(std::memory_order_acquire), 1);
}

TEST_F(AsyncCacheWriteServiceTest, RuntimeGrowAndShrinkPreserveConcurrentTasks) {
    auto cache = create_cache("async_write_service_mpmc_consumers");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);

    constexpr size_t worker_count = 8;
    auto options = service->options();
    options.worker_count = 1;
    options.max_pending_tasks = worker_count;
    options.batch_size = 1;
    ASSERT_TRUE(service->update_options(options).ok());
    options.worker_count = worker_count;
    ASSERT_TRUE(service->update_options(options).ok());

    std::mutex mutex;
    std::condition_variable cv;
    size_t entered_workers = 0;
    size_t finished_tasks = 0;
    bool release_workers = false;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "AsyncCacheWriteService::_write_one:before_get_or_set",
            [&](auto&&) {
                std::unique_lock lock(mutex);
                ++entered_workers;
                cv.notify_all();
                cv.wait(lock, [&]() { return release_workers; });
            },
            &guard);
    sync_point->enable_processing();
    Defer clear_sync_point {[&]() {
        {
            std::lock_guard lock(mutex);
            release_workers = true;
        }
        cv.notify_all();
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    }};

    for (size_t task_id = 0; task_id < worker_count; ++task_id) {
        AsyncCacheWriteBufferPtr buffer;
        ASSERT_TRUE(service->allocate_tracked_buffer(4096, &buffer).ok());
        memset(buffer->data(), static_cast<int>('a' + task_id), buffer->size());
        AsyncCacheWriteTask task {
                .cache_hash = BlockFileCache::hash("mpmc_consumer_" + std::to_string(task_id)),
                .file_offset = 0,
                .file_size = buffer->size(),
                .buffer = buffer,
                .admission_ctx = {},
                .submit_ts_us = MonotonicMicros(),
                .write_epoch = service->current_write_epoch(),
                .on_finalized =
                        [&](const AsyncCacheWriteTask&) {
                            std::lock_guard lock(mutex);
                            ++finished_tasks;
                            cv.notify_all();
                        },
        };
        ASSERT_TRUE(service->try_submit(std::move(task)));
    }

    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5),
                                [&]() { return entered_workers == worker_count; }));
        EXPECT_EQ(service->pending_count(), worker_count);
    }

    auto shrink_options = service->options();
    shrink_options.worker_count = 1;
    auto shrink_future = std::async(std::launch::async, [service, shrink_options]() {
        return service->update_options(shrink_options);
    });
    for (int attempt = 0;
         attempt < 5000 && service->_desired_worker_count.load(std::memory_order_acquire) != 1;
         ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(service->_desired_worker_count.load(std::memory_order_acquire), 1);
    EXPECT_EQ(shrink_future.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
    {
        std::lock_guard lock(mutex);
        release_workers = true;
    }
    cv.notify_all();
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5),
                                [&]() { return finished_tasks == worker_count; }));
    }
    ASSERT_TRUE(shrink_future.get().ok());
    EXPECT_EQ(service->pending_count(), 0);
    EXPECT_EQ(service->options().worker_count, 1);
}

TEST_F(AsyncCacheWriteServiceTest, DynamicMpmcQueueGrowthRejectionAndDrain) {
    auto cache = create_cache("async_write_service_dynamic_backpressure");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);

    constexpr size_t producer_count = 4;
    constexpr size_t fill_wave_count = 4;
    constexpr size_t fill_tasks_per_producer = 4;
    constexpr size_t accepted_producer_tasks =
            producer_count * fill_wave_count * fill_tasks_per_producer;
    constexpr size_t rejected_tasks_per_producer = 32;
    constexpr size_t rejected_producer_tasks = producer_count * rejected_tasks_per_producer;
    constexpr size_t total_producer_tasks = accepted_producer_tasks + rejected_producer_tasks;
    constexpr size_t max_pending_tasks = accepted_producer_tasks + 1;
    constexpr size_t fast_worker_count = 4;
    constexpr size_t block_size = 4096;

    auto options = service->options();
    options.worker_count = 1;
    options.max_pending_tasks = max_pending_tasks;
    options.batch_size = 1;
    ASSERT_TRUE(service->update_options(options).ok());

    std::mutex mutex;
    std::condition_variable cv;
    size_t consumer_entries = 0;
    size_t finished_tasks = 0;
    bool slow_consumers = true;
    bool record_drain_samples = false;
    std::vector<size_t> drain_queue_sizes;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "AsyncCacheWriteService::_write_one:before_get_or_set",
            [&](auto&&) {
                std::unique_lock lock(mutex);
                ++consumer_entries;
                cv.notify_all();
                cv.wait(lock, [&]() { return !slow_consumers; });
                if (record_drain_samples) {
                    // This excludes active tasks, unlike pending_count(), and therefore observes
                    // the actual MPMC backlog after each dequeue.
                    drain_queue_sizes.emplace_back(service->_queue.size_approx());
                }
            },
            &guard);
    sync_point->enable_processing();
    Defer clear_sync_point {[&]() {
        {
            std::lock_guard lock(mutex);
            slow_consumers = false;
        }
        cv.notify_all();
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
        service->shutdown();
    }};

    std::vector<AsyncCacheWriteTask> tasks;
    tasks.reserve(total_producer_tasks + 1);
    for (size_t task_id = 0; task_id <= total_producer_tasks; ++task_id) {
        AsyncCacheWriteBufferPtr buffer;
        ASSERT_TRUE(service->allocate_tracked_buffer(block_size, &buffer).ok());
        memset(buffer->data(), static_cast<int>('a' + task_id % 26), buffer->size());
        tasks.emplace_back(AsyncCacheWriteTask {
                .cache_hash =
                        BlockFileCache::hash("dynamic_backpressure_" + std::to_string(task_id)),
                .file_offset = 0,
                .file_size = buffer->size(),
                .buffer = std::move(buffer),
                .admission_ctx = {},
                .submit_ts_us = MonotonicMicros(),
                .write_epoch = service->current_write_epoch(),
                .on_finalized =
                        [&](const AsyncCacheWriteTask&) {
                            std::lock_guard lock(mutex);
                            ++finished_tasks;
                            cv.notify_all();
                        },
        });
    }

    const auto baseline_stats = service->stats();
    ASSERT_TRUE(service->try_submit(std::move(tasks[0])));
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5),
                                [&]() { return consumer_entries == 1; }));
    }

    std::atomic<size_t> producer_accepts {0};
    std::atomic<size_t> producer_rejects {0};
    // Each producer owns a contiguous task slice beginning at first_task; tasks_per_producer
    // controls whether this invocation is one fill wave or the final rejection burst.
    const auto submit_wave = [&](size_t first_task, size_t tasks_per_producer) {
        std::vector<std::thread> producers;
        producers.reserve(producer_count);
        std::barrier producer_start(producer_count);
        for (size_t producer_id = 0; producer_id < producer_count; ++producer_id) {
            producers.emplace_back([&, producer_id]() {
                producer_start.arrive_and_wait();
                const size_t producer_first = first_task + producer_id * tasks_per_producer;
                for (size_t task_offset = 0; task_offset < tasks_per_producer; ++task_offset) {
                    if (service->try_submit(std::move(tasks[producer_first + task_offset]))) {
                        producer_accepts.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        producer_rejects.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            });
        }
        for (auto& producer : producers) {
            producer.join();
        }
    };

    // With the only consumer blocked, every producer continuously submits several tasks per wave.
    // The stable snapshots make backlog growth deterministic instead of depending on polling
    // timing.
    for (size_t wave = 0; wave < fill_wave_count; ++wave) {
        submit_wave(1 + wave * producer_count * fill_tasks_per_producer, fill_tasks_per_producer);
        EXPECT_EQ(service->_queue.size_approx(),
                  (wave + 1) * producer_count * fill_tasks_per_producer);
        EXPECT_EQ(service->pending_count(),
                  1 + (wave + 1) * producer_count * fill_tasks_per_producer);
    }

    // The queue is now bounded by max_pending_tasks. A larger concurrent burst must be dropped by
    // backpressure without changing either the queued or active task count.
    submit_wave(1 + accepted_producer_tasks, rejected_tasks_per_producer);
    EXPECT_EQ(producer_accepts.load(std::memory_order_relaxed), accepted_producer_tasks);
    EXPECT_EQ(producer_rejects.load(std::memory_order_relaxed), rejected_producer_tasks);
    EXPECT_GT(producer_rejects.load(std::memory_order_relaxed),
              producer_accepts.load(std::memory_order_relaxed));
    EXPECT_EQ(service->_queue.size_approx(), accepted_producer_tasks);
    EXPECT_EQ(service->pending_count(), max_pending_tasks);
    const auto pressured_stats = service->stats();
    EXPECT_EQ(pressured_stats.submitted - baseline_stats.submitted, max_pending_tasks);
    EXPECT_EQ(pressured_stats.rejected - baseline_stats.rejected, rejected_producer_tasks);

    // Producers have stopped. Grow from one to four consumers while writes remain blocked, then
    // remove the artificial delay and increase the batch size to model faster disk consumption.
    options.worker_count = fast_worker_count;
    options.batch_size = 4;
    ASSERT_TRUE(service->update_options(options).ok());
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5),
                                [&]() { return consumer_entries == fast_worker_count; }));
        EXPECT_EQ(service->_queue.size_approx(), accepted_producer_tasks - (fast_worker_count - 1));
        record_drain_samples = true;
        slow_consumers = false;
    }
    cv.notify_all();

    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(10),
                                [&]() { return finished_tasks == max_pending_tasks; }));
    }
    EXPECT_EQ(service->pending_count(), 0);
    EXPECT_EQ(service->_queue.size_approx(), 0);
    EXPECT_TRUE(std::any_of(drain_queue_sizes.begin(), drain_queue_sizes.end(), [&](size_t size) {
        return size > 0 && size <= accepted_producer_tasks / 2;
    }));
    EXPECT_TRUE(std::any_of(drain_queue_sizes.begin(), drain_queue_sizes.end(),
                            [](size_t size) { return size == 0; }));
}

TEST_F(AsyncCacheWriteServiceTest, MutableConfigUpdatesServicesExplicitly) {
    auto* factory = FileCacheFactory::instance();
    factory->_caches.clear();
    factory->_path_to_cache.clear();
    factory->_capacity = 0;

    const bool old_enable = config::enable_async_file_cache_write;
    const int32_t old_workers = config::async_file_cache_write_workers_per_disk;
    const int64_t old_max_pending = config::async_file_cache_write_max_pending_tasks_per_disk;
    const int32_t old_batch_size = config::async_file_cache_write_batch_size;
    const int64_t old_warn_secs = config::async_file_cache_write_watchdog_warn_secs;
    const int64_t old_drop_secs = config::async_file_cache_write_watchdog_drop_secs;
    const auto path = caches_dir / "async_write_service_config_update";
    std::error_code error;
    Defer restore {[&]() {
        EXPECT_TRUE(config::set_config("async_file_cache_write_workers_per_disk",
                                       std::to_string(old_workers))
                            .ok());
        EXPECT_TRUE(config::set_config("async_file_cache_write_max_pending_tasks_per_disk",
                                       std::to_string(old_max_pending))
                            .ok());
        EXPECT_TRUE(config::set_config("async_file_cache_write_batch_size",
                                       std::to_string(old_batch_size))
                            .ok());
        EXPECT_TRUE(config::set_config("async_file_cache_write_watchdog_warn_secs",
                                       std::to_string(old_warn_secs))
                            .ok());
        EXPECT_TRUE(config::set_config("async_file_cache_write_watchdog_drop_secs",
                                       std::to_string(old_drop_secs))
                            .ok());
        EXPECT_TRUE(
                config::set_config("enable_async_file_cache_write", old_enable ? "true" : "false")
                        .ok());
        factory->_caches.clear();
        factory->_path_to_cache.clear();
        factory->_capacity = 0;
        fs::remove_all(path, error);
    }};

    ASSERT_TRUE(config::set_config("enable_async_file_cache_write", "false").ok());
    fs::remove_all(path, error);
    fs::create_directories(path);
    ASSERT_TRUE(factory->create_file_cache(path.string(), async_write_cache_settings()).ok());
    auto* cache = factory->get_by_path(path.string());
    ASSERT_NE(cache, nullptr);
    wait_until_cache_ready(*cache);
    ASSERT_FALSE(cache->async_write_service()->_started.load(std::memory_order_acquire));
    AsyncCacheWriteBufferPtr disabled_buffer;
    ASSERT_TRUE(cache->async_write_service()->allocate_tracked_buffer(4096, &disabled_buffer).ok());
    AsyncCacheWriteTask disabled_task {
            .cache_hash = BlockFileCache::hash("disabled_async_write_service"),
            .file_offset = 0,
            .file_size = disabled_buffer->size(),
            .buffer = disabled_buffer,
            .admission_ctx = {},
            .submit_ts_us = MonotonicMicros(),
            .write_epoch = cache->async_write_service()->current_write_epoch(),
            .on_finalized = nullptr,
    };
    EXPECT_FALSE(cache->async_write_service()->try_submit(std::move(disabled_task)));
    EXPECT_EQ(cache->async_write_service()->pending_count(), 0);

    const int32_t new_workers = old_workers == 1 ? 2 : 1;
    const int64_t new_max_pending = old_max_pending + 1;
    const int32_t new_batch_size = old_batch_size + 1;
    const int64_t new_warn_secs = old_warn_secs + 1;
    const int64_t new_drop_secs = std::max(old_drop_secs + 1, new_warn_secs + 1);
    ASSERT_TRUE(config::set_config("async_file_cache_write_workers_per_disk",
                                   std::to_string(new_workers))
                        .ok());
    ASSERT_TRUE(config::set_config("async_file_cache_write_max_pending_tasks_per_disk",
                                   std::to_string(new_max_pending))
                        .ok());
    ASSERT_TRUE(
            config::set_config("async_file_cache_write_batch_size", std::to_string(new_batch_size))
                    .ok());
    ASSERT_TRUE(config::set_config("async_file_cache_write_watchdog_drop_secs",
                                   std::to_string(new_drop_secs))
                        .ok());
    ASSERT_TRUE(config::set_config("async_file_cache_write_watchdog_warn_secs",
                                   std::to_string(new_warn_secs))
                        .ok());
    ASSERT_TRUE(config::set_config("enable_async_file_cache_write", "true").ok());

    const auto updated = cache->async_write_service()->options();
    EXPECT_TRUE(cache->async_write_service()->_started.load(std::memory_order_acquire));
    EXPECT_EQ(updated.worker_count, new_workers);
    EXPECT_EQ(updated.max_pending_tasks, new_max_pending);
    EXPECT_EQ(updated.batch_size, new_batch_size);
    EXPECT_EQ(updated.watchdog_warn_secs, new_warn_secs);
    EXPECT_EQ(updated.watchdog_drop_secs, new_drop_secs);
}

TEST_F(AsyncCacheWriteServiceTest, ShutdownWaitsForRegisteredSubmitterAndRejectsItsTask) {
    auto cache = create_cache("async_write_service_shutdown");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);

    std::mutex mutex;
    std::condition_variable cv;
    bool submitter_registered = false;
    bool release_submitter = false;
    std::promise<void> shutdown_stopped_accepting;
    auto shutdown_stopped_accepting_future = shutdown_stopped_accepting.get_future();
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard submit_guard;
    SyncPoint::CallbackGuard shutdown_guard;
    sync_point->set_call_back(
            "AsyncCacheWriteService::try_submit:after_register",
            [&](auto&&) {
                std::unique_lock lock(mutex);
                submitter_registered = true;
                cv.notify_all();
                cv.wait(lock, [&]() { return release_submitter; });
            },
            &submit_guard);
    sync_point->set_call_back(
            "AsyncCacheWriteService::shutdown:after_stop_accepting",
            [&](auto&&) { shutdown_stopped_accepting.set_value(); }, &shutdown_guard);
    sync_point->enable_processing();
    std::future<bool> submit_future;
    std::future<void> shutdown_future;
    Defer clear_sync_point {[&]() {
        {
            std::lock_guard lock(mutex);
            release_submitter = true;
        }
        cv.notify_all();
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    }};

    AsyncCacheWriteBufferPtr buffer;
    ASSERT_TRUE(service->allocate_tracked_buffer(4096, &buffer).ok());
    AsyncCacheWriteTask task {
            .cache_hash = BlockFileCache::hash("shutdown_submitter"),
            .file_offset = 0,
            .file_size = buffer->size(),
            .buffer = buffer,
            .admission_ctx = {},
            .submit_ts_us = MonotonicMicros(),
            .write_epoch = service->current_write_epoch(),
            .on_finalized = nullptr,
    };
    submit_future = std::async(std::launch::async, [service, task = std::move(task)]() mutable {
        return service->try_submit(std::move(task));
    });
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(
                cv.wait_for(lock, std::chrono::seconds(5), [&]() { return submitter_registered; }));
    }

    shutdown_future = std::async(std::launch::async, [service]() { service->shutdown(); });
    ASSERT_EQ(shutdown_stopped_accepting_future.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    EXPECT_EQ(shutdown_future.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
    {
        std::lock_guard lock(mutex);
        release_submitter = true;
    }
    cv.notify_all();

    EXPECT_FALSE(submit_future.get());
    ASSERT_EQ(shutdown_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_EQ(service->pending_count(), 0);
}

} // namespace
} // namespace doris::io
