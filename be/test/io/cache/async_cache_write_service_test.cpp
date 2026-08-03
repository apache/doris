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
#include <limits>
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

AsyncCacheWriteTask make_async_write_task(
        AsyncCacheWriteService* service, const std::string& key, char fill,
        std::function<void(const AsyncCacheWriteTask&)> on_finalized = nullptr,
        int64_t submit_ts_us = MonotonicMicros()) {
    DORIS_CHECK(service != nullptr);
    AsyncCacheWriteBufferPtr buffer;
    EXPECT_TRUE(service->allocate_tracked_buffer(4096, &buffer).ok());
    DORIS_CHECK(buffer != nullptr);
    memset(buffer->data(), fill, buffer->size());
    return AsyncCacheWriteTask {
            .cache_hash = BlockFileCache::hash(key),
            .file_offset = 0,
            .write_size = buffer->size(),
            .buffer = std::move(buffer),
            .admission_ctx = {},
            .submit_ts_us = submit_ts_us,
            .write_epoch = service->current_write_epoch(),
            .on_finalized = std::move(on_finalized),
    };
}

bool is_cache_range_downloaded(BlockFileCache* cache, const UInt128Wrapper& hash, size_t offset = 0,
                               size_t size = 4096) {
    DORIS_CHECK(cache != nullptr);
    ReadStatistics read_stats;
    CacheContext context;
    context.stats = &read_stats;
    FileBlocks blocks;
    bool fully_covered = false;
    DORIS_CHECK(cache->get_downloaded_blocks_if_fully_covered(hash, offset, size, context, &blocks,
                                                              &fully_covered)
                        .ok());
    return fully_covered;
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
    const uint64_t baseline_submitted = service->_submitted_metric->get_value();
    const uint64_t baseline_submitted_bytes = service->_submitted_bytes_metric->get_value();
    const uint64_t baseline_finished = service->_finished_metric->get_value();
    const uint64_t baseline_finished_bytes = service->_finished_bytes_metric->get_value();
    const uint64_t baseline_worker_finished_bytes =
            service->_worker_finished_bytes_metric->get_value();
    const uint64_t baseline_persisted_blocks = service->_persisted_blocks_metric->get_value();
    const uint64_t baseline_persisted_bytes = service->_persisted_bytes_metric->get_value();
    const int64_t baseline_submit_latency_count = service->_submit_latency_metric->count();
    const int64_t baseline_buffer_alloc_latency_count =
            service->_buffer_alloc_latency_metric->count();
    const int64_t baseline_queue_wait_latency_count = service->_queue_wait_latency_metric->count();
    const int64_t baseline_worker_task_latency_count =
            service->_worker_task_latency_metric->count();
    const int64_t baseline_get_or_set_latency_count = service->_get_or_set_latency_metric->count();
    const int64_t baseline_append_latency_count = service->_append_latency_metric->count();
    const int64_t baseline_finalize_latency_count = service->_finalize_latency_metric->count();

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
            .write_size = block_size,
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
    EXPECT_EQ(service->pending_bytes(), 0);
    EXPECT_EQ(service->queued_count(), 0);
    EXPECT_EQ(service->queued_bytes(), 0);
    EXPECT_EQ(service->active_task_count(), 0);
    EXPECT_EQ(service->active_bytes(), 0);
    EXPECT_EQ(index->lookup(hash, 0, epoch), nullptr);
    EXPECT_EQ(service->_submitted_metric->get_value() - baseline_submitted, 1);
    EXPECT_EQ(service->_submitted_bytes_metric->get_value() - baseline_submitted_bytes, block_size);
    EXPECT_EQ(service->_finished_metric->get_value() - baseline_finished, 1);
    EXPECT_EQ(service->_finished_bytes_metric->get_value() - baseline_finished_bytes, block_size);
    EXPECT_EQ(service->_worker_finished_bytes_metric->get_value() - baseline_worker_finished_bytes,
              block_size);
    EXPECT_EQ(service->_persisted_blocks_metric->get_value() - baseline_persisted_blocks, 1);
    EXPECT_EQ(service->_persisted_bytes_metric->get_value() - baseline_persisted_bytes, block_size);
    EXPECT_EQ(service->_submit_latency_metric->count() - baseline_submit_latency_count, 1);
    EXPECT_EQ(service->_buffer_alloc_latency_metric->count() - baseline_buffer_alloc_latency_count,
              1);
    EXPECT_EQ(service->_queue_wait_latency_metric->count() - baseline_queue_wait_latency_count, 1);
    EXPECT_EQ(service->_worker_task_latency_metric->count() - baseline_worker_task_latency_count,
              1);
    EXPECT_EQ(service->_get_or_set_latency_metric->count() - baseline_get_or_set_latency_count, 1);
    EXPECT_EQ(service->_append_latency_metric->count() - baseline_append_latency_count, 1);
    EXPECT_EQ(service->_finalize_latency_metric->count() - baseline_finalize_latency_count, 1);

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

TEST_F(AsyncCacheWriteServiceTest, LockedQueuePreservesFifoWithSingleWorker) {
    auto cache = create_cache("async_write_service_locked_fifo");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);
    auto options = service->options();
    options.worker_count = 1;
    options.max_pending_bytes = 4 * 4096;
    ASSERT_TRUE(service->update_options(options).ok());

    const std::vector<UInt128Wrapper> hashes {BlockFileCache::hash("locked_fifo_a"),
                                              BlockFileCache::hash("locked_fifo_b"),
                                              BlockFileCache::hash("locked_fifo_c")};
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<size_t> take_order;
    size_t finalized = 0;
    bool release_worker = false;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "AsyncCacheWriteService::_write_one:before_get_or_set",
            [&](auto&& args) {
                const auto* task = try_any_cast<const AsyncCacheWriteTask*>(args[0]);
                const auto iterator = std::find(hashes.begin(), hashes.end(), task->cache_hash);
                DORIS_CHECK(iterator != hashes.end());
                std::unique_lock lock(mutex);
                take_order.emplace_back(static_cast<size_t>(iterator - hashes.begin()));
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

    const auto on_finalized = [&](const AsyncCacheWriteTask&) {
        std::lock_guard lock(mutex);
        ++finalized;
        cv.notify_all();
    };
    ASSERT_TRUE(service->try_submit(
            make_async_write_task(service, "locked_fifo_a", 'a', on_finalized)));
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5),
                                [&]() { return take_order.size() == 1; }));
    }
    ASSERT_TRUE(service->try_submit(
            make_async_write_task(service, "locked_fifo_b", 'b', on_finalized)));
    ASSERT_TRUE(service->try_submit(
            make_async_write_task(service, "locked_fifo_c", 'c', on_finalized)));
    EXPECT_EQ(service->queued_count(), 2);
    {
        std::lock_guard lock(mutex);
        release_worker = true;
    }
    cv.notify_all();
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&]() { return finalized == 3; }));
    }
    EXPECT_EQ(take_order, (std::vector<size_t> {0, 1, 2}));
    EXPECT_EQ(service->pending_count(), 0);
}

TEST_F(AsyncCacheWriteServiceTest, RejectsWhenAllPendingTasksAreActive) {
    auto cache = create_cache("async_write_service_backpressure");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);
    auto options = service->options();
    options.max_pending_bytes = 4096;
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
            .write_size = first_buffer->size(),
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
            .write_size = rejected_buffer->size(),
            .buffer = rejected_buffer,
            .admission_ctx = {},
            .submit_ts_us = MonotonicMicros(),
            .write_epoch = service->current_write_epoch(),
            .on_finalized = nullptr,
    };
    EXPECT_FALSE(service->try_submit(std::move(rejected_task)));
    EXPECT_EQ(service->pending_count(), 1);
    EXPECT_EQ(service->pending_bytes(), 4096);
    EXPECT_EQ(service->queued_count(), 0);
    EXPECT_EQ(service->queued_bytes(), 0);
    EXPECT_EQ(service->active_task_count(), 1);
    EXPECT_EQ(service->active_bytes(), 4096);
    EXPECT_EQ(service->_active_get_or_set_count.load(std::memory_order_relaxed), 1);
    EXPECT_GE(service->_reject_backpressure_metric->get_value(), 1);
    EXPECT_EQ(service->_evicted_oldest_metric->get_value(), 0);

    {
        std::lock_guard lock(mutex);
        release_worker = true;
    }
    cv.notify_all();
    ASSERT_EQ(first_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_EQ(service->pending_count(), 0);
    EXPECT_EQ(service->pending_bytes(), 0);
}

TEST_F(AsyncCacheWriteServiceTest, RejectsTaskLargerThanPendingMemoryLimit) {
    auto cache = create_cache("async_write_service_task_too_large");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);
    auto options = service->options();
    options.max_pending_bytes = 4095;
    ASSERT_TRUE(service->update_options(options).ok());

    const uint64_t baseline_rejected = service->_rejected_metric->get_value();
    const uint64_t baseline_backpressure = service->_reject_backpressure_metric->get_value();
    size_t finalized = 0;
    EXPECT_FALSE(service->try_submit(make_async_write_task(
            service, "task_too_large", 'l', [&](const AsyncCacheWriteTask&) { ++finalized; })));
    EXPECT_EQ(service->pending_count(), 0);
    EXPECT_EQ(service->pending_bytes(), 0);
    EXPECT_EQ(service->_rejected_metric->get_value() - baseline_rejected, 1);
    EXPECT_EQ(service->_reject_backpressure_metric->get_value() - baseline_backpressure, 1);
    EXPECT_EQ(finalized, 0);
}

TEST_F(AsyncCacheWriteServiceTest,
       DropOldestReplacesOnlyOldestQueuedTaskAndKeepsInflightReaderAlive) {
    auto cache = create_cache("async_write_service_drop_oldest");
    auto* service = cache->async_write_service();
    auto* index = cache->inflight_write_buffer_index();
    ASSERT_NE(service, nullptr);
    ASSERT_NE(index, nullptr);
    auto options = service->options();
    options.worker_count = 1;
    options.max_pending_bytes = 3 * 4096;
    ASSERT_TRUE(service->update_options(options).ok());
    const int64_t baseline_memory = service->buffer_memory_bytes();

    const std::vector<std::string> keys {"drop_oldest_a", "drop_oldest_b", "drop_oldest_c",
                                         "drop_oldest_d"};
    std::vector<UInt128Wrapper> hashes;
    hashes.reserve(keys.size());
    for (const auto& key : keys) {
        hashes.emplace_back(BlockFileCache::hash(key));
    }

    std::mutex mutex;
    std::condition_variable cv;
    bool worker_entered = false;
    bool release_worker = false;
    std::vector<size_t> finalized(keys.size(), 0);
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

    const auto record_finalized = [&](size_t task_id) {
        return [&, task_id](const AsyncCacheWriteTask&) {
            std::lock_guard lock(mutex);
            ++finalized[task_id];
            cv.notify_all();
        };
    };
    ASSERT_TRUE(
            service->try_submit(make_async_write_task(service, keys[0], 'a', record_finalized(0))));
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&]() { return worker_entered; }));
    }

    std::vector<std::shared_ptr<InflightWriteBufferEntry>> entries(keys.size());
    const auto make_indexed_task = [&](size_t task_id, char fill) {
        AsyncCacheWriteBufferPtr buffer;
        EXPECT_TRUE(service->allocate_tracked_buffer(4096, &buffer).ok());
        DORIS_CHECK(buffer != nullptr);
        memset(buffer->data(), fill, buffer->size());
        const int64_t submit_ts_us = MonotonicMicros();
        entries[task_id] = std::make_shared<InflightWriteBufferEntry>(
                buffer, 0, buffer->size(), submit_ts_us, service->current_write_epoch());
        DORIS_CHECK(index->insert_if_absent(hashes[task_id], 0, entries[task_id]) == nullptr);
        return AsyncCacheWriteTask {
                .cache_hash = hashes[task_id],
                .file_offset = 0,
                .write_size = buffer->size(),
                .buffer = std::move(buffer),
                .admission_ctx = {},
                .submit_ts_us = submit_ts_us,
                .write_epoch = service->current_write_epoch(),
                .on_finalized =
                        [&, task_id](const AsyncCacheWriteTask&) {
                            index->remove_if(hashes[task_id], 0, entries[task_id]);
                            std::lock_guard lock(mutex);
                            ++finalized[task_id];
                            cv.notify_all();
                        },
        };
    };

    ASSERT_TRUE(service->try_submit(make_indexed_task(1, 'b')));
    ASSERT_TRUE(service->try_submit(make_indexed_task(2, 'c')));
    ASSERT_EQ(service->pending_count(), 3);
    ASSERT_EQ(service->pending_bytes(), 3 * 4096);
    ASSERT_EQ(service->queued_count(), 2);
    ASSERT_EQ(service->queued_bytes(), 2 * 4096);
    ASSERT_EQ(service->active_task_count(), 1);
    ASSERT_EQ(service->active_bytes(), 4096);
    auto concurrent_reader = index->lookup(hashes[1], 0, service->current_write_epoch());
    ASSERT_NE(concurrent_reader, nullptr);
    const uint64_t baseline_evicted = service->_evicted_oldest_metric->get_value();
    const uint64_t baseline_evicted_bytes = service->_evicted_oldest_bytes_metric->get_value();
    const int64_t baseline_evicted_age_count = service->_evicted_oldest_age_metric->count();

    ASSERT_TRUE(service->try_submit(make_indexed_task(3, 'd')));
    EXPECT_EQ(service->pending_count(), 3);
    EXPECT_EQ(service->pending_bytes(), 3 * 4096);
    EXPECT_EQ(service->queued_count(), 2);
    EXPECT_EQ(service->queued_bytes(), 2 * 4096);
    EXPECT_EQ(service->active_task_count(), 1);
    EXPECT_EQ(service->active_bytes(), 4096);
    EXPECT_EQ(finalized[0], 0);
    EXPECT_EQ(finalized[1], 1);
    EXPECT_EQ(finalized[2], 0);
    EXPECT_EQ(finalized[3], 0);
    EXPECT_EQ(service->_evicted_oldest_metric->get_value() - baseline_evicted, 1);
    EXPECT_EQ(service->_evicted_oldest_bytes_metric->get_value() - baseline_evicted_bytes, 4096);
    EXPECT_EQ(service->_evicted_oldest_age_metric->count() - baseline_evicted_age_count, 1);
    EXPECT_EQ(index->lookup(hashes[1], 0, service->current_write_epoch()), nullptr);
    EXPECT_NE(index->lookup(hashes[2], 0, service->current_write_epoch()), nullptr);
    EXPECT_NE(index->lookup(hashes[3], 0, service->current_write_epoch()), nullptr);
    ASSERT_NE(concurrent_reader->buffer, nullptr);
    EXPECT_EQ(concurrent_reader->buffer->data()[0], 'b');
    EXPECT_EQ(concurrent_reader->buffer->data()[4095], 'b');

    {
        std::lock_guard lock(mutex);
        release_worker = true;
    }
    cv.notify_all();
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&]() {
            return finalized[0] == 1 && finalized[1] == 1 && finalized[2] == 1 && finalized[3] == 1;
        }));
    }
    EXPECT_TRUE(is_cache_range_downloaded(cache.get(), hashes[0]));
    EXPECT_FALSE(is_cache_range_downloaded(cache.get(), hashes[1]));
    EXPECT_TRUE(is_cache_range_downloaded(cache.get(), hashes[2]));
    EXPECT_TRUE(is_cache_range_downloaded(cache.get(), hashes[3]));
    EXPECT_EQ(service->pending_count(), 0);
    EXPECT_EQ(service->pending_bytes(), 0);
    EXPECT_EQ(index->count(), 0);

    concurrent_reader.reset();
    entries.clear();
    for (int attempt = 0; attempt < 100 && service->buffer_memory_bytes() != baseline_memory;
         ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(service->buffer_memory_bytes(), baseline_memory);
}

TEST_F(AsyncCacheWriteServiceTest, OldEpochVictimCallbackDoesNotDeleteReplacement) {
    auto cache = create_cache("async_write_service_old_epoch_victim");
    auto* service = cache->async_write_service();
    auto* index = cache->inflight_write_buffer_index();
    ASSERT_NE(service, nullptr);
    ASSERT_NE(index, nullptr);
    auto options = service->options();
    options.worker_count = 1;
    options.max_pending_bytes = 2 * 4096;
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

    ASSERT_TRUE(service->try_submit(make_async_write_task(service, "old_epoch_active", 'a')));
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&]() { return worker_entered; }));
    }

    const auto hash = BlockFileCache::hash("old_epoch_same_key");
    const uint64_t old_epoch = service->current_write_epoch();
    AsyncCacheWriteBufferPtr old_buffer;
    ASSERT_TRUE(service->allocate_tracked_buffer(4096, &old_buffer).ok());
    memset(old_buffer->data(), 'o', old_buffer->size());
    auto old_entry = std::make_shared<InflightWriteBufferEntry>(old_buffer, 0, old_buffer->size(),
                                                                MonotonicMicros(), old_epoch);
    ASSERT_EQ(index->insert_if_absent(hash, 0, old_entry), nullptr);
    size_t old_callback_count = 0;
    AsyncCacheWriteTask old_task {
            .cache_hash = hash,
            .file_offset = 0,
            .write_size = old_buffer->size(),
            .buffer = old_buffer,
            .admission_ctx = {},
            .submit_ts_us = MonotonicMicros(),
            .write_epoch = old_epoch,
            .on_finalized =
                    [&, old_entry](const AsyncCacheWriteTask&) {
                        index->remove_if(hash, 0, old_entry);
                        ++old_callback_count;
                    },
    };
    ASSERT_TRUE(service->try_submit(std::move(old_task)));

    const uint64_t new_epoch = service->invalidate_pending_writes();
    AsyncCacheWriteBufferPtr replacement_buffer;
    ASSERT_TRUE(service->allocate_tracked_buffer(4096, &replacement_buffer).ok());
    memset(replacement_buffer->data(), 'n', replacement_buffer->size());
    auto replacement_entry = std::make_shared<InflightWriteBufferEntry>(
            replacement_buffer, 0, replacement_buffer->size(), MonotonicMicros(), new_epoch);
    ASSERT_EQ(index->insert_if_absent(hash, 0, replacement_entry), nullptr);
    ASSERT_EQ(index->lookup(hash, 0, new_epoch), replacement_entry);

    ASSERT_TRUE(service->try_submit(make_async_write_task(service, "old_epoch_replacer", 'r')));
    EXPECT_EQ(old_callback_count, 1);
    EXPECT_EQ(index->lookup(hash, 0, new_epoch), replacement_entry);
    EXPECT_EQ(service->_evicted_oldest_metric->get_value(), 1);

    {
        std::lock_guard lock(mutex);
        release_worker = true;
    }
    cv.notify_all();
    for (int attempt = 0; attempt < 5000 && service->pending_count() != 0; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(service->pending_count(), 0);
    EXPECT_EQ(old_callback_count, 1);
    EXPECT_TRUE(index->remove_if(hash, 0, replacement_entry));
}

TEST_F(AsyncCacheWriteServiceTest, EvictedCallbackRunsOutsideQueueMutex) {
    auto cache = create_cache("async_write_service_callback_outside_lock");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);
    auto options = service->options();
    options.worker_count = 1;
    options.max_pending_bytes = 2 * 4096;
    ASSERT_TRUE(service->update_options(options).ok());

    const auto active_hash = BlockFileCache::hash("callback_outside_active");
    const auto replacement_hash = BlockFileCache::hash("callback_outside_replacement");
    std::mutex mutex;
    std::condition_variable cv;
    bool active_entered = false;
    bool release_active = false;
    bool replacement_entered = false;
    bool victim_callback_entered = false;
    bool release_victim_callback = false;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "AsyncCacheWriteService::_write_one:before_get_or_set",
            [&](auto&& args) {
                const auto* task = try_any_cast<const AsyncCacheWriteTask*>(args[0]);
                std::unique_lock lock(mutex);
                if (task->cache_hash == active_hash) {
                    active_entered = true;
                    cv.notify_all();
                    cv.wait(lock, [&]() { return release_active; });
                } else if (task->cache_hash == replacement_hash) {
                    replacement_entered = true;
                    cv.notify_all();
                }
            },
            &guard);
    sync_point->enable_processing();
    Defer clear_sync_point {[&]() {
        {
            std::lock_guard lock(mutex);
            release_active = true;
            release_victim_callback = true;
        }
        cv.notify_all();
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    }};

    ASSERT_TRUE(
            service->try_submit(make_async_write_task(service, "callback_outside_active", 'a')));
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&]() { return active_entered; }));
    }
    ASSERT_TRUE(service->try_submit(make_async_write_task(
            service, "callback_outside_victim", 'v', [&](const AsyncCacheWriteTask&) {
                std::unique_lock lock(mutex);
                victim_callback_entered = true;
                cv.notify_all();
                cv.wait(lock, [&]() { return release_victim_callback; });
            })));

    auto replacement_future = std::async(std::launch::async, [&]() {
        SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->orphan_mem_tracker());
        return service->try_submit(
                make_async_write_task(service, "callback_outside_replacement", 'r'));
    });
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5),
                                [&]() { return victim_callback_entered; }));
        release_active = true;
    }
    cv.notify_all();
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(
                cv.wait_for(lock, std::chrono::seconds(5), [&]() { return replacement_entered; }));
    }
    EXPECT_EQ(replacement_future.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);
    {
        std::lock_guard lock(mutex);
        release_victim_callback = true;
    }
    cv.notify_all();
    ASSERT_TRUE(replacement_future.get());
}

TEST_F(AsyncCacheWriteServiceTest, OldQueuedTaskIsStillWrittenAndCleansInflightEntry) {
    auto cache = create_cache("async_write_service_old_queued_task");
    auto* service = cache->async_write_service();
    auto* index = cache->inflight_write_buffer_index();
    ASSERT_NE(service, nullptr);
    ASSERT_NE(index, nullptr);

    const auto hash = BlockFileCache::hash("old_queued_task");
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
            .write_size = buffer->size(),
            .buffer = buffer,
            .admission_ctx = {},
            .submit_ts_us = MonotonicMicros() - 60LL * 60 * 1000 * 1000,
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
    EXPECT_TRUE(is_cache_range_downloaded(cache.get(), hash));
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
            .write_size = buffer->size(),
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
            .write_size = task_size,
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
            .write_size = task_size,
            .buffer = buffer,
            .admission_ctx = {},
            .submit_ts_us = MonotonicMicros(),
            .write_epoch = service->current_write_epoch(),
            .on_finalized = [&finished](const AsyncCacheWriteTask&) { finished.set_value(); },
    };
    ASSERT_TRUE(service->try_submit(std::move(task)));
    ASSERT_EQ(finished_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_EQ(service->pending_count(), 0);
    EXPECT_GE(service->_skip_downloaded_metric->get_value(), 1);
    EXPECT_GE(service->_skip_downloading_metric->get_value(), 1);
    EXPECT_GE(service->_skip_deleting_metric->get_value(), 1);

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
            .write_size = buffer->size(),
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
    EXPECT_EQ(service->_active_append_count.load(std::memory_order_relaxed), 1);

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
    EXPECT_EQ(service->_active_append_count.load(std::memory_order_relaxed), 0);

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
            .write_size = task_size,
            .buffer = buffer,
            .admission_ctx = {},
            .submit_ts_us = MonotonicMicros(),
            .write_epoch = service->current_write_epoch(),
            .on_finalized = [&finished](const AsyncCacheWriteTask&) { finished.set_value(); },
    };
    ASSERT_TRUE(service->try_submit(std::move(task)));
    ASSERT_EQ(finished_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_GE(service->_skip_partial_overlap_metric->get_value(), 1);

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
            .write_size = active_buffer->size(),
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
            .write_size = queued_buffer->size(),
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
    EXPECT_GE(service->_drop_stale_epoch_metric->get_value(), 2);

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

TEST_F(AsyncCacheWriteServiceTest, PendingLimitDecreaseKeepsReplacingOldestQueuedTask) {
    auto cache = create_cache("async_write_service_limit_decrease");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);
    auto options = service->options();
    options.worker_count = 1;
    options.max_pending_bytes = 4 * 4096;
    ASSERT_TRUE(service->update_options(options).ok());

    std::mutex mutex;
    std::condition_variable cv;
    size_t worker_entries = 0;
    size_t released_entries = 0;
    std::vector<size_t> finalized(7, 0);
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "AsyncCacheWriteService::_write_one:before_get_or_set",
            [&](auto&&) {
                std::unique_lock lock(mutex);
                const size_t current_entry = ++worker_entries;
                cv.notify_all();
                cv.wait(lock, [&]() { return released_entries >= current_entry; });
            },
            &guard);
    sync_point->enable_processing();
    Defer clear_sync_point {[&]() {
        {
            std::lock_guard lock(mutex);
            released_entries = std::numeric_limits<size_t>::max();
        }
        cv.notify_all();
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    }};

    const auto finalizer = [&](size_t task_id) {
        return [&, task_id](const AsyncCacheWriteTask&) {
            std::lock_guard lock(mutex);
            ++finalized[task_id];
            cv.notify_all();
        };
    };
    ASSERT_TRUE(service->try_submit(
            make_async_write_task(service, "limit_decrease_a", 'a', finalizer(0))));
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(
                cv.wait_for(lock, std::chrono::seconds(5), [&]() { return worker_entries == 1; }));
    }
    const auto first_evicted_hash = BlockFileCache::hash("limit_decrease_b");
    ASSERT_TRUE(service->try_submit(
            make_async_write_task(service, "limit_decrease_b", 'b', finalizer(1))));
    ASSERT_TRUE(service->try_submit(
            make_async_write_task(service, "limit_decrease_c", 'c', finalizer(2))));
    const auto second_evicted_hash = BlockFileCache::hash("limit_decrease_d");
    ASSERT_TRUE(service->try_submit(
            make_async_write_task(service, "limit_decrease_d", 'd', finalizer(3))));
    ASSERT_EQ(service->pending_count(), 4);
    ASSERT_EQ(service->pending_bytes(), 4 * 4096);
    ASSERT_EQ(service->queued_count(), 3);
    ASSERT_EQ(service->queued_bytes(), 3 * 4096);

    options.max_pending_bytes = 2 * 4096;
    ASSERT_TRUE(service->update_options(options).ok());
    const uint64_t baseline_evicted = service->_evicted_oldest_metric->get_value();
    ASSERT_TRUE(service->try_submit(
            make_async_write_task(service, "limit_decrease_e", 'e', finalizer(4))));
    EXPECT_EQ(finalized[1], 1);
    EXPECT_EQ(service->_evicted_oldest_metric->get_value() - baseline_evicted, 1);
    EXPECT_EQ(service->pending_count(), 4);
    EXPECT_EQ(service->pending_bytes(), 4 * 4096);
    EXPECT_EQ(service->queued_count(), 3);
    EXPECT_EQ(service->queued_bytes(), 3 * 4096);

    {
        std::lock_guard lock(mutex);
        released_entries = 1;
    }
    cv.notify_all();
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(
                cv.wait_for(lock, std::chrono::seconds(5), [&]() { return worker_entries == 2; }));
    }
    EXPECT_EQ(service->pending_count(), 3);
    EXPECT_EQ(service->pending_bytes(), 3 * 4096);
    const auto third_evicted_hash = BlockFileCache::hash("limit_decrease_g");
    ASSERT_TRUE(service->try_submit(
            make_async_write_task(service, "limit_decrease_g", 'g', finalizer(5))));
    EXPECT_EQ(finalized[3], 1);
    EXPECT_EQ(service->_evicted_oldest_metric->get_value() - baseline_evicted, 2);
    EXPECT_EQ(service->pending_count(), 3);
    EXPECT_EQ(service->pending_bytes(), 3 * 4096);
    EXPECT_EQ(service->queued_count(), 2);
    EXPECT_EQ(service->queued_bytes(), 2 * 4096);

    {
        std::lock_guard lock(mutex);
        released_entries = 2;
    }
    cv.notify_all();
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(
                cv.wait_for(lock, std::chrono::seconds(5), [&]() { return worker_entries == 3; }));
    }
    ASSERT_EQ(service->pending_count(), 2);
    ASSERT_EQ(service->pending_bytes(), 2 * 4096);
    ASSERT_EQ(service->queued_count(), 1);
    ASSERT_EQ(service->queued_bytes(), 4096);
    ASSERT_EQ(service->active_task_count(), 1);
    ASSERT_EQ(service->active_bytes(), 4096);

    const auto replacement_hash = BlockFileCache::hash("limit_decrease_f");
    ASSERT_TRUE(service->try_submit(
            make_async_write_task(service, "limit_decrease_f", 'f', finalizer(6))));
    EXPECT_EQ(service->pending_count(), 2);
    EXPECT_EQ(service->pending_bytes(), 2 * 4096);
    EXPECT_EQ(service->queued_count(), 1);
    EXPECT_EQ(service->queued_bytes(), 4096);
    EXPECT_EQ(finalized[5], 1);
    EXPECT_EQ(service->_evicted_oldest_metric->get_value() - baseline_evicted, 3);

    {
        std::lock_guard lock(mutex);
        released_entries = std::numeric_limits<size_t>::max();
    }
    cv.notify_all();
    for (int attempt = 0; attempt < 5000 && service->pending_count() != 0; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(service->pending_count(), 0);
    ASSERT_EQ(service->pending_bytes(), 0);
    for (size_t finalized_count : finalized) {
        EXPECT_EQ(finalized_count, 1);
    }
    EXPECT_FALSE(is_cache_range_downloaded(cache.get(), first_evicted_hash));
    EXPECT_FALSE(is_cache_range_downloaded(cache.get(), second_evicted_hash));
    EXPECT_FALSE(is_cache_range_downloaded(cache.get(), third_evicted_hash));
    EXPECT_TRUE(is_cache_range_downloaded(cache.get(), replacement_hash));
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
    invalid_options.max_pending_bytes = 0;
    EXPECT_TRUE(service->update_options(invalid_options).is<ErrorCode::INVALID_ARGUMENT>());
    options.worker_count = 3;
    options.max_pending_bytes = 7 * 4096;
    ASSERT_TRUE(service->update_options(options).ok());
    const auto updated = service->options();
    EXPECT_EQ(updated.worker_count, 3);
    EXPECT_EQ(updated.max_pending_bytes, 7 * 4096);

    options.worker_count = 1;
    ASSERT_TRUE(service->update_options(options).ok());
    EXPECT_EQ(service->options().worker_count, 1);
}

TEST_F(AsyncCacheWriteServiceTest, ResizeWorkersPreservesActiveTaskOwnership) {
    auto cache = create_cache("async_write_service_resize_workers");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);

    constexpr size_t worker_count = 8;
    auto options = service->options();
    options.worker_count = 1;
    options.max_pending_bytes = worker_count * 4096;
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
                .cache_hash = BlockFileCache::hash("resize_worker_" + std::to_string(task_id)),
                .file_offset = 0,
                .write_size = buffer->size(),
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
    for (int attempt = 0; attempt < 5000 && service->options().worker_count != 1; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(service->options().worker_count, 1);
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

TEST_F(AsyncCacheWriteServiceTest, ConcurrentDropOldestMaintainsCounterConservation) {
    auto cache = create_cache("async_write_service_concurrent_drop_oldest");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);

    constexpr size_t producer_count = 4;
    constexpr size_t tasks_per_producer = 64;
    constexpr size_t producer_tasks = producer_count * tasks_per_producer;
    constexpr size_t total_tasks = producer_tasks + 1;
    constexpr size_t max_pending_blocks = 16;
    auto options = service->options();
    options.worker_count = 1;
    options.max_pending_bytes = max_pending_blocks * 4096;
    ASSERT_TRUE(service->update_options(options).ok());

    const auto active_hash = BlockFileCache::hash("concurrent_drop_oldest_0");
    std::mutex mutex;
    std::condition_variable cv;
    bool active_entered = false;
    bool release_active = false;
    size_t worker_tasks = 0;
    std::vector<size_t> finalized(total_tasks, 0);
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "AsyncCacheWriteService::_write_one:before_get_or_set",
            [&](auto&& args) {
                const auto* task = try_any_cast<const AsyncCacheWriteTask*>(args[0]);
                std::unique_lock lock(mutex);
                ++worker_tasks;
                if (task->cache_hash == active_hash) {
                    active_entered = true;
                    cv.notify_all();
                    cv.wait(lock, [&]() { return release_active; });
                }
            },
            &guard);
    sync_point->enable_processing();
    Defer clear_sync_point {[&]() {
        {
            std::lock_guard lock(mutex);
            release_active = true;
        }
        cv.notify_all();
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    }};

    std::vector<AsyncCacheWriteTask> tasks;
    tasks.reserve(total_tasks);
    for (size_t task_id = 0; task_id < total_tasks; ++task_id) {
        tasks.emplace_back(make_async_write_task(
                service, "concurrent_drop_oldest_" + std::to_string(task_id),
                static_cast<char>('a' + task_id % 26), [&, task_id](const AsyncCacheWriteTask&) {
                    std::lock_guard lock(mutex);
                    ++finalized[task_id];
                    cv.notify_all();
                }));
    }

    const uint64_t baseline_submitted = service->_submitted_metric->get_value();
    const uint64_t baseline_submitted_bytes = service->_submitted_bytes_metric->get_value();
    const uint64_t baseline_finished = service->_finished_metric->get_value();
    const uint64_t baseline_finished_bytes = service->_finished_bytes_metric->get_value();
    const uint64_t baseline_worker_finished = service->_worker_finished_metric->get_value();
    const uint64_t baseline_worker_finished_bytes =
            service->_worker_finished_bytes_metric->get_value();
    const uint64_t baseline_evicted = service->_evicted_oldest_metric->get_value();
    const uint64_t baseline_evicted_bytes = service->_evicted_oldest_bytes_metric->get_value();
    const uint64_t baseline_rejected = service->_rejected_metric->get_value();
    ASSERT_TRUE(service->try_submit(std::move(tasks[0])));
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&]() { return active_entered; }));
    }

    std::atomic<size_t> accepted {0};
    std::vector<std::thread> producers;
    producers.reserve(producer_count);
    std::barrier start_barrier(producer_count);
    for (size_t producer_id = 0; producer_id < producer_count; ++producer_id) {
        producers.emplace_back([&, producer_id]() {
            start_barrier.arrive_and_wait();
            const size_t first_task = 1 + producer_id * tasks_per_producer;
            for (size_t offset = 0; offset < tasks_per_producer; ++offset) {
                if (service->try_submit(std::move(tasks[first_task + offset]))) {
                    accepted.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& producer : producers) {
        producer.join();
    }

    EXPECT_EQ(accepted.load(std::memory_order_relaxed), producer_tasks);
    EXPECT_EQ(service->pending_count(), max_pending_blocks);
    EXPECT_EQ(service->pending_bytes(), max_pending_blocks * 4096);
    EXPECT_EQ(service->queued_count(), max_pending_blocks - 1);
    EXPECT_EQ(service->queued_bytes(), (max_pending_blocks - 1) * 4096);
    EXPECT_EQ(service->active_task_count(), 1);
    EXPECT_EQ(service->active_bytes(), 4096);
    EXPECT_EQ(service->_submitted_metric->get_value() - baseline_submitted, total_tasks);
    EXPECT_EQ(service->_submitted_bytes_metric->get_value() - baseline_submitted_bytes,
              total_tasks * 4096);
    EXPECT_EQ(service->_rejected_metric->get_value() - baseline_rejected, 0);
    EXPECT_EQ(service->_evicted_oldest_metric->get_value() - baseline_evicted,
              total_tasks - max_pending_blocks);
    EXPECT_EQ(service->_evicted_oldest_bytes_metric->get_value() - baseline_evicted_bytes,
              (total_tasks - max_pending_blocks) * 4096);

    {
        std::lock_guard lock(mutex);
        release_active = true;
    }
    cv.notify_all();
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(10), [&]() {
            return std::all_of(finalized.begin(), finalized.end(),
                               [](size_t count) { return count == 1; });
        }));
    }
    EXPECT_EQ(service->pending_count(), 0);
    EXPECT_EQ(service->pending_bytes(), 0);
    EXPECT_EQ(service->queued_count(), 0);
    EXPECT_EQ(service->queued_bytes(), 0);
    EXPECT_EQ(service->active_task_count(), 0);
    EXPECT_EQ(service->active_bytes(), 0);
    EXPECT_EQ(worker_tasks, max_pending_blocks);
    EXPECT_EQ(service->_finished_metric->get_value() - baseline_finished, total_tasks);
    EXPECT_EQ(service->_finished_bytes_metric->get_value() - baseline_finished_bytes,
              total_tasks * 4096);
    EXPECT_EQ(service->_worker_finished_metric->get_value() - baseline_worker_finished,
              max_pending_blocks);
    EXPECT_EQ(service->_worker_finished_bytes_metric->get_value() - baseline_worker_finished_bytes,
              max_pending_blocks * 4096);
    EXPECT_EQ((service->_worker_finished_metric->get_value() - baseline_worker_finished) +
                      (service->_evicted_oldest_metric->get_value() - baseline_evicted),
              service->_submitted_metric->get_value() - baseline_submitted);
}

TEST_F(AsyncCacheWriteServiceTest, MutableConfigUpdatesServicesExplicitly) {
    auto* factory = FileCacheFactory::instance();
    factory->_caches.clear();
    factory->_path_to_cache.clear();
    factory->_capacity = 0;

    const bool old_enable = config::enable_async_file_cache_write;
    const int32_t old_workers = config::async_file_cache_write_workers_per_disk;
    const int64_t old_max_pending_bytes = config::async_file_cache_write_max_pending_bytes_per_disk;
    const auto path = caches_dir / "async_write_service_config_update";
    std::error_code error;
    Defer restore {[&]() {
        EXPECT_TRUE(config::set_config("async_file_cache_write_workers_per_disk",
                                       std::to_string(old_workers))
                            .ok());
        EXPECT_TRUE(config::set_config("async_file_cache_write_max_pending_bytes_per_disk",
                                       std::to_string(old_max_pending_bytes))
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
            .write_size = disabled_buffer->size(),
            .buffer = disabled_buffer,
            .admission_ctx = {},
            .submit_ts_us = MonotonicMicros(),
            .write_epoch = cache->async_write_service()->current_write_epoch(),
            .on_finalized = nullptr,
    };
    EXPECT_FALSE(cache->async_write_service()->try_submit(std::move(disabled_task)));
    EXPECT_EQ(cache->async_write_service()->pending_count(), 0);

    const int32_t new_workers = old_workers == 1 ? 2 : 1;
    const int64_t new_max_pending_bytes = old_max_pending_bytes + 4096;
    ASSERT_TRUE(config::set_config("async_file_cache_write_workers_per_disk",
                                   std::to_string(new_workers))
                        .ok());
    ASSERT_TRUE(config::set_config("async_file_cache_write_max_pending_bytes_per_disk",
                                   std::to_string(new_max_pending_bytes))
                        .ok());
    ASSERT_TRUE(config::set_config("enable_async_file_cache_write", "true").ok());

    const auto updated = cache->async_write_service()->options();
    EXPECT_TRUE(cache->async_write_service()->_started.load(std::memory_order_acquire));
    EXPECT_EQ(updated.worker_count, new_workers);
    EXPECT_EQ(updated.max_pending_bytes, new_max_pending_bytes);
}

TEST_F(AsyncCacheWriteServiceTest, ShutdownWaitsForConcurrentReplacementAndDrains) {
    auto cache = create_cache("async_write_service_shutdown_replacement");
    auto* service = cache->async_write_service();
    ASSERT_NE(service, nullptr);
    auto options = service->options();
    options.worker_count = 1;
    options.max_pending_bytes = 2 * 4096;
    ASSERT_TRUE(service->update_options(options).ok());

    const auto active_hash = BlockFileCache::hash("shutdown_replacement_active");
    std::mutex mutex;
    std::condition_variable cv;
    bool active_entered = false;
    bool release_active = false;
    bool victim_callback_entered = false;
    bool release_victim_callback = false;
    std::vector<size_t> finalized(3, 0);
    std::promise<void> shutdown_stopped_accepting;
    auto shutdown_stopped_accepting_future = shutdown_stopped_accepting.get_future();
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard worker_guard;
    SyncPoint::CallbackGuard shutdown_guard;
    sync_point->set_call_back(
            "AsyncCacheWriteService::_write_one:before_get_or_set",
            [&](auto&& args) {
                const auto* task = try_any_cast<const AsyncCacheWriteTask*>(args[0]);
                if (task->cache_hash != active_hash) {
                    return;
                }
                std::unique_lock lock(mutex);
                active_entered = true;
                cv.notify_all();
                cv.wait(lock, [&]() { return release_active; });
            },
            &worker_guard);
    sync_point->set_call_back(
            "AsyncCacheWriteService::shutdown:after_stop_accepting",
            [&](auto&&) { shutdown_stopped_accepting.set_value(); }, &shutdown_guard);
    sync_point->enable_processing();
    Defer clear_sync_point {[&]() {
        {
            std::lock_guard lock(mutex);
            release_active = true;
            release_victim_callback = true;
        }
        cv.notify_all();
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    }};

    const auto finalizer = [&](size_t task_id) {
        return [&, task_id](const AsyncCacheWriteTask&) {
            std::lock_guard lock(mutex);
            ++finalized[task_id];
            cv.notify_all();
        };
    };
    ASSERT_TRUE(service->try_submit(
            make_async_write_task(service, "shutdown_replacement_active", 'a', finalizer(0))));
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&]() { return active_entered; }));
    }
    ASSERT_TRUE(service->try_submit(make_async_write_task(
            service, "shutdown_replacement_victim", 'v', [&](const AsyncCacheWriteTask&) {
                std::unique_lock lock(mutex);
                ++finalized[1];
                victim_callback_entered = true;
                cv.notify_all();
                cv.wait(lock, [&]() { return release_victim_callback; });
            })));

    auto replacement_future = std::async(std::launch::async, [&]() {
        SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->orphan_mem_tracker());
        return service->try_submit(
                make_async_write_task(service, "shutdown_replacement_new", 'n', finalizer(2)));
    });
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5),
                                [&]() { return victim_callback_entered; }));
    }

    auto shutdown_future = std::async(std::launch::async, [service]() { service->shutdown(); });
    ASSERT_EQ(shutdown_stopped_accepting_future.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    EXPECT_EQ(shutdown_future.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
    {
        std::lock_guard lock(mutex);
        release_victim_callback = true;
    }
    cv.notify_all();
    ASSERT_TRUE(replacement_future.get());
    EXPECT_EQ(shutdown_future.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);

    {
        std::lock_guard lock(mutex);
        release_active = true;
    }
    cv.notify_all();
    ASSERT_EQ(shutdown_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    shutdown_future.get();
    EXPECT_EQ(finalized, (std::vector<size_t> {1, 1, 1}));
    EXPECT_EQ(service->pending_count(), 0);
    EXPECT_EQ(service->queued_count(), 0);
    EXPECT_EQ(service->active_task_count(), 0);
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
            .write_size = buffer->size(),
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
