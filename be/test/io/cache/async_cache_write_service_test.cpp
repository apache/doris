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

#include <atomic>
#include <condition_variable>
#include <cstring>
#include <future>
#include <memory>
#include <mutex>
#include <string>

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
    const int64_t old_max_pending = config::async_file_cache_write_max_pending_tasks_per_disk;
    config::async_file_cache_write_max_pending_tasks_per_disk = 1;
    Defer restore_config {
            [&]() { config::async_file_cache_write_max_pending_tasks_per_disk = old_max_pending; }};
    auto cache = create_cache("async_write_service_backpressure");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);

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
    const int64_t old_warn_secs = config::async_file_cache_write_watchdog_warn_secs;
    const int64_t old_drop_secs = config::async_file_cache_write_watchdog_drop_secs;
    config::async_file_cache_write_watchdog_warn_secs = 0;
    config::async_file_cache_write_watchdog_drop_secs = 1;
    Defer restore_config {[&]() {
        config::async_file_cache_write_watchdog_warn_secs = old_warn_secs;
        config::async_file_cache_write_watchdog_drop_secs = old_drop_secs;
    }};
    auto cache = create_cache("async_write_service_watchdog");
    auto* service = cache->async_write_service();
    auto* index = cache->inflight_write_buffer_index();
    ASSERT_NE(service, nullptr);
    ASSERT_NE(index, nullptr);

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
    EXPECT_TRUE(probe_result.holder.file_blocks.empty());
    ASSERT_EQ(probe_result.gaps.size(), 1);
    EXPECT_EQ(probe_result.gaps.front().left, 0);
    EXPECT_EQ(probe_result.gaps.front().right, 4095);
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

TEST_F(AsyncCacheWriteServiceTest, EpochChangeAfterFirstCheckDropsTaskAndCleansEmptyCell) {
    auto cache = create_cache("async_write_service_epoch");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);

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

    const auto hash = BlockFileCache::hash("epoch_drop");
    AsyncCacheWriteBufferPtr buffer;
    ASSERT_TRUE(service->allocate_tracked_buffer(4096, &buffer).ok());
    memset(buffer->data(), 'c', buffer->size());
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
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&]() { return worker_entered; }));
    }
    service->invalidate_pending_writes();
    {
        std::lock_guard lock(mutex);
        release_worker = true;
    }
    cv.notify_all();
    ASSERT_EQ(finished_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_GE(service->stats().drop_stale_epoch, 1);

    ReadStatistics read_stats;
    CacheContext context;
    context.stats = &read_stats;
    auto probe_result = cache->probe(hash, 0, 4096, context);
    EXPECT_TRUE(probe_result.holder.file_blocks.empty());
    ASSERT_EQ(probe_result.gaps.size(), 1);
    EXPECT_EQ(probe_result.gaps.front().left, 0);
    EXPECT_EQ(probe_result.gaps.front().right, 4095);
}

TEST_F(AsyncCacheWriteServiceTest, ResizeWorkersAtRuntime) {
    auto cache = create_cache("async_write_service_resize");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);

    ASSERT_TRUE(service->resize_workers(3).ok());
    EXPECT_EQ(service->_desired_worker_count.load(std::memory_order_acquire), 3);
    ASSERT_TRUE(service->resize_workers(1).ok());
    EXPECT_EQ(service->_desired_worker_count.load(std::memory_order_acquire), 1);
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
