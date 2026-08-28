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

#include "io/cache/partial_block_writeback_manager.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cpp/sync_point.h"
#include "io/cache/block_file_cache_test_common.h"
#include "io/cache/inflight_write_buffer_index.h"
#include "io/fs/path.h"
#include "util/defer_op.h"

namespace doris::io {
namespace {

using namespace std::chrono_literals;

constexpr size_t kBlockSize = 4096;

FileCacheSettings partial_writeback_cache_settings() {
    FileCacheSettings settings;
    settings.query_queue_size = 4_mb;
    settings.query_queue_elements = 1024;
    settings.index_queue_size = 1_mb;
    settings.index_queue_elements = 256;
    settings.disposable_queue_size = 1_mb;
    settings.disposable_queue_elements = 256;
    settings.capacity = 8_mb;
    settings.max_file_block_size = kBlockSize;
    settings.max_query_cache_size = 0;
    return settings;
}

PartialBlockWritebackOptions partial_writeback_options(size_t workers = 2,
                                                       size_t pending_blocks = 4) {
    return PartialBlockWritebackOptions {
            .block_size = kBlockSize,
            .worker_count = workers,
            .max_pending_bytes = pending_blocks * kBlockSize,
            .hole_fill_coalesce =
                    {
                            .max_gap_bytes = 128,
                            .max_range_bytes = kBlockSize,
                            .max_read_amplification_ratio = 2.0,
                    },
    };
}

std::string patterned_content(char base) {
    std::string content(kBlockSize, '\0');
    for (size_t index = 0; index < content.size(); ++index) {
        content[index] = static_cast<char>(base + index % 13);
    }
    return content;
}

struct ObservedRead {
    size_t offset {0};
    size_t size {0};
    bool bypass_peer_read {false};
    bool should_stop {true};
    std::optional<CacheWriteMode> cache_write_mode;
};

class ControlledFileReader final : public FileReader {
public:
    explicit ControlledFileReader(std::string content, bool block_reads = false)
            : _content(std::move(content)), _block_reads(block_reads) {}

    Status close() override {
        std::lock_guard lock(_mutex);
        _closed = true;
        return Status::OK();
    }

    const Path& path() const override { return _path; }
    size_t size() const override { return _content.size(); }

    bool closed() const override {
        std::lock_guard lock(_mutex);
        return _closed;
    }

    int64_t mtime() const override { return 0; }

    void fail_at(size_t offset) {
        std::lock_guard lock(_mutex);
        _failed_offsets.insert(offset);
    }

    void return_short_at(size_t offset) {
        std::lock_guard lock(_mutex);
        _short_offsets.insert(offset);
    }

    bool wait_for_entered(size_t count) {
        std::unique_lock lock(_mutex);
        return _cv.wait_for(lock, 5s, [&]() { return _entered >= count; });
    }

    void release_reads() {
        {
            std::lock_guard lock(_mutex);
            _release_reads = true;
        }
        _cv.notify_all();
    }

    size_t read_calls() const {
        std::lock_guard lock(_mutex);
        return _reads.size();
    }

    size_t max_active_reads() const {
        std::lock_guard lock(_mutex);
        return _max_active_reads;
    }

    std::vector<ObservedRead> reads() const {
        std::lock_guard lock(_mutex);
        return _reads;
    }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_context) override {
        DORIS_CHECK(offset <= _content.size());
        DORIS_CHECK(result.size <= _content.size() - offset);
        bool fail = false;
        bool return_short = false;
        {
            std::unique_lock lock(_mutex);
            ++_entered;
            ++_active_reads;
            _max_active_reads = std::max(_max_active_reads, _active_reads);
            fail = _failed_offsets.contains(offset);
            return_short = _short_offsets.contains(offset);
            ObservedRead observed {
                    .offset = offset,
                    .size = result.size,
                    .bypass_peer_read = false,
                    .should_stop = true,
                    .cache_write_mode = std::nullopt,
            };
            if (io_context != nullptr) {
                observed.bypass_peer_read = io_context->bypass_peer_read;
                observed.should_stop = io_context->should_stop;
                observed.cache_write_mode = io_context->cache_write_mode_override;
            }
            _reads.push_back(observed);
            _cv.notify_all();
            if (_block_reads) {
                _cv.wait(lock, [&]() { return _release_reads; });
            }
            --_active_reads;
        }

        if (fail) {
            *bytes_read = 0;
            return Status::IOError("injected hole-fill read failure at {}", offset);
        }
        size_t read_size = result.size;
        if (return_short) {
            DORIS_CHECK(read_size > 0);
            --read_size;
        }
        std::memcpy(result.data, _content.data() + offset, read_size);
        *bytes_read = read_size;
        return Status::OK();
    }

private:
    const Path _path {"partial_block_source"};
    const std::string _content;
    const bool _block_reads;
    mutable std::mutex _mutex;
    std::condition_variable _cv;
    bool _closed {false};
    bool _release_reads {false};
    size_t _entered {0};
    size_t _active_reads {0};
    size_t _max_active_reads {0};
    std::set<size_t> _failed_offsets;
    std::set<size_t> _short_offsets;
    std::vector<ObservedRead> _reads;
};

class OneShotSyncPointGate {
public:
    void arrive_and_wait() {
        std::unique_lock lock(_mutex);
        if (_arrived) {
            return;
        }
        _arrived = true;
        _cv.notify_all();
        _cv.wait(lock, [&]() { return _released; });
    }

    bool wait_until_arrived() {
        std::unique_lock lock(_mutex);
        return _cv.wait_for(lock, 5s, [&]() { return _arrived; });
    }

    void release() {
        {
            std::lock_guard lock(_mutex);
            _released = true;
        }
        _cv.notify_all();
    }

private:
    std::mutex _mutex;
    std::condition_variable _cv;
    bool _arrived {false};
    bool _released {false};
};

template <typename Predicate>
bool wait_until(Predicate predicate) {
    for (int attempt = 0; attempt < 5000; ++attempt) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    return false;
}

bool cache_range_downloaded(BlockFileCache* cache, const UInt128Wrapper& hash) {
    ReadStatistics stats;
    CacheContext context;
    context.stats = &stats;
    FileBlocks blocks;
    bool fully_covered = false;
    DORIS_CHECK(cache->get_downloaded_blocks_if_fully_covered(hash, 0, kBlockSize, context, &blocks,
                                                              &fully_covered)
                        .ok());
    return fully_covered;
}

std::string read_cached_block(BlockFileCache* cache, const UInt128Wrapper& hash) {
    ReadStatistics stats;
    CacheContext context;
    context.stats = &stats;
    FileBlocks blocks;
    bool fully_covered = false;
    DORIS_CHECK(cache->get_downloaded_blocks_if_fully_covered(hash, 0, kBlockSize, context, &blocks,
                                                              &fully_covered)
                        .ok());
    DORIS_CHECK(fully_covered);
    DORIS_CHECK(blocks.size() == 1);
    std::string result(kBlockSize, '\0');
    DORIS_CHECK(blocks.front()->read(Slice(result), 0).ok());
    return result;
}

PartialBlockWritebackRequest make_request(AsyncCacheWriteManager* write_manager,
                                          InflightWriteBufferIndex* inflight_index,
                                          FileReaderSPtr reader, const UInt128Wrapper& hash,
                                          const std::string& content, size_t fragment_offset,
                                          size_t fragment_size) {
    IOContext io_context;
    return PartialBlockWritebackRequest {
            .write_manager = write_manager,
            .inflight_index = inflight_index,
            .source_reader = std::move(reader),
            .cache_hash = hash,
            .block_offset = 0,
            .block_valid_size = kBlockSize,
            .fragment_offset = fragment_offset,
            .data = Slice(content.data() + fragment_offset, fragment_size),
            .admission_ctx = {},
            .write_epoch = write_manager->current_write_epoch(hash),
            .io_context = FileRangeReadIOContext::from_caller(io_context),
    };
}

class PartialBlockWritebackManagerTest : public BlockFileCacheTest {
protected:
    std::unique_ptr<BlockFileCache> create_cache(const std::string& name,
                                                 size_t cache_writer_pending_blocks = 8) {
        auto path = caches_dir / name;
        std::error_code error;
        fs::remove_all(path, error);
        fs::create_directories(path);
        _paths.emplace_back(path);
        auto cache =
                std::make_unique<BlockFileCache>(path.string(), partial_writeback_cache_settings());
        EXPECT_TRUE(cache->initialize().ok());
        wait_until_cache_ready(*cache);
        auto* manager = cache->async_write_manager();
        EXPECT_NE(manager, nullptr);
        EXPECT_TRUE(manager->start().ok());
        auto options = manager->options();
        options.worker_count = 1;
        options.max_pending_bytes = cache_writer_pending_blocks * kBlockSize;
        EXPECT_TRUE(manager->update_options(options).ok());
        return cache;
    }

    std::unique_ptr<PartialBlockWritebackManager> create_manager(
            const PartialBlockWritebackOptions& options = partial_writeback_options()) {
        std::unique_ptr<PartialBlockWritebackManager> manager;
        EXPECT_TRUE(PartialBlockWritebackManager::create(options, &manager).ok());
        return manager;
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

TEST_F(PartialBlockWritebackManagerTest, MergesQueuedFragmentsAndWaitsForCacheWriterCapacity) {
    auto cache = create_cache("partial_block_merge", 1);
    auto* cache_writer = cache->async_write_manager();
    auto* index = cache->inflight_write_buffer_index();
    auto manager = create_manager(partial_writeback_options(2, 4));

    OneShotSyncPointGate cache_writer_gate;
    OneShotSyncPointGate merge_copy_gate;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard cache_writer_guard;
    SyncPoint::CallbackGuard merge_copy_guard;
    sync_point->set_call_back(
            "AsyncCacheWriteManager::_persist_task:before_get_or_set",
            [&](auto&&) { cache_writer_gate.arrive_and_wait(); }, &cache_writer_guard);
    sync_point->set_call_back(
            "PartialBlockWritebackManager::try_submit:before_merge_copy",
            [&](auto&&) { merge_copy_gate.arrive_and_wait(); }, &merge_copy_guard);
    sync_point->enable_processing();
    Defer clear_sync_point {[&]() {
        cache_writer_gate.release();
        merge_copy_gate.release();
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    }};

    const auto blocker_hash = BlockFileCache::hash("partial_block_merge_blocker");
    const std::string blocker(kBlockSize, 'x');
    ASSERT_EQ(cache_writer->try_submit_block(AsyncCacheWriteBlockRequest {
                      .cache_hash = blocker_hash,
                      .file_offset = 0,
                      .data = Slice(blocker),
                      .buffer_size = kBlockSize,
                      .admission_ctx = {},
                      .write_epoch = cache_writer->current_write_epoch(blocker_hash),
                      .inflight_index = index,
              }),
              AsyncCacheWriteBlockSubmitResult::SUBMITTED);
    ASSERT_TRUE(cache_writer_gate.wait_until_arrived());

    const std::string content = patterned_content('a');
    auto reader = std::make_shared<ControlledFileReader>(content);
    auto unrelated_reader = std::make_shared<ControlledFileReader>(content);
    const auto hash = BlockFileCache::hash("partial_block_merge_target");
    const auto unrelated_hash = BlockFileCache::hash("partial_block_merge_unrelated");
    EXPECT_EQ(
            manager->try_submit(make_request(cache_writer, index, reader, hash, content, 0, 1024)),
            PartialBlockSubmitResult::QUEUED);
    auto merge = std::async(std::launch::async, [&]() {
        return manager->try_submit(
                make_request(cache_writer, index, reader, hash, content, 2048, 2048));
    });
    ASSERT_TRUE(merge_copy_gate.wait_until_arrived());
    auto unrelated = std::async(std::launch::async, [&]() {
        return manager->try_submit(make_request(cache_writer, index, unrelated_reader,
                                                unrelated_hash, content, 0, 1024));
    });
    const auto unrelated_wait = unrelated.wait_for(1s);
    merge_copy_gate.release();
    ASSERT_EQ(merge.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(merge.get(), PartialBlockSubmitResult::MERGED);
    ASSERT_EQ(unrelated.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(unrelated.get(), PartialBlockSubmitResult::QUEUED);
    EXPECT_EQ(unrelated_wait, std::future_status::ready);
    EXPECT_EQ(manager->pending_count(), 2);
    EXPECT_EQ(manager->pending_bytes(), 2 * kBlockSize);
    EXPECT_EQ(manager->queued_count(), 2);
    std::this_thread::sleep_for(30ms);
    EXPECT_EQ(reader->read_calls(), 0);

    cache_writer_gate.release();
    ASSERT_TRUE(wait_until([&]() { return manager->pending_count() == 0; }));
    ASSERT_TRUE(wait_until([&]() { return cache_writer->pending_count() == 0; }));
    ASSERT_TRUE(cache_range_downloaded(cache.get(), hash));
    ASSERT_TRUE(cache_range_downloaded(cache.get(), unrelated_hash));
    EXPECT_EQ(read_cached_block(cache.get(), hash), content);
    EXPECT_EQ(read_cached_block(cache.get(), unrelated_hash), content);
    const auto reads = reader->reads();
    ASSERT_EQ(reads.size(), 1);
    EXPECT_EQ(reads[0].offset, 1024);
    EXPECT_EQ(reads[0].size, 1024);
    EXPECT_TRUE(reads[0].bypass_peer_read);
    EXPECT_FALSE(reads[0].should_stop);
    EXPECT_EQ(reads[0].cache_write_mode, CacheWriteMode::NO_WRITE);
}

TEST_F(PartialBlockWritebackManagerTest, IncludesMergeThatRacesWithWorkerActivation) {
    auto cache = create_cache("partial_block_merge_activation", 1);
    auto* cache_writer = cache->async_write_manager();
    auto* index = cache->inflight_write_buffer_index();
    auto manager = create_manager(partial_writeback_options(1, 1));

    OneShotSyncPointGate cache_writer_gate;
    OneShotSyncPointGate merge_copy_gate;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard cache_writer_guard;
    SyncPoint::CallbackGuard merge_copy_guard;
    sync_point->set_call_back(
            "AsyncCacheWriteManager::_persist_task:before_get_or_set",
            [&](auto&&) { cache_writer_gate.arrive_and_wait(); }, &cache_writer_guard);
    sync_point->set_call_back(
            "PartialBlockWritebackManager::try_submit:before_merge_copy",
            [&](auto&&) { merge_copy_gate.arrive_and_wait(); }, &merge_copy_guard);
    sync_point->enable_processing();
    Defer clear_sync_point {[&]() {
        cache_writer_gate.release();
        merge_copy_gate.release();
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    }};

    const auto blocker_hash = BlockFileCache::hash("partial_block_merge_activation_blocker");
    const std::string blocker(kBlockSize, 'x');
    ASSERT_EQ(cache_writer->try_submit_block(AsyncCacheWriteBlockRequest {
                      .cache_hash = blocker_hash,
                      .file_offset = 0,
                      .data = Slice(blocker),
                      .buffer_size = kBlockSize,
                      .admission_ctx = {},
                      .write_epoch = cache_writer->current_write_epoch(blocker_hash),
                      .inflight_index = index,
              }),
              AsyncCacheWriteBlockSubmitResult::SUBMITTED);
    ASSERT_TRUE(cache_writer_gate.wait_until_arrived());

    const std::string content = patterned_content('m');
    auto reader = std::make_shared<ControlledFileReader>(content);
    const auto hash = BlockFileCache::hash("partial_block_merge_activation_target");
    ASSERT_EQ(
            manager->try_submit(make_request(cache_writer, index, reader, hash, content, 0, 1024)),
            PartialBlockSubmitResult::QUEUED);

    auto merge = std::async(std::launch::async, [&]() {
        return manager->try_submit(
                make_request(cache_writer, index, reader, hash, content, 2048, 2048));
    });
    ASSERT_TRUE(merge_copy_gate.wait_until_arrived());

    cache_writer_gate.release();
    ASSERT_TRUE(wait_until([&]() { return manager->active_count() == 1; }));
    merge_copy_gate.release();
    ASSERT_EQ(merge.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(merge.get(), PartialBlockSubmitResult::MERGED);

    ASSERT_TRUE(wait_until([&]() { return manager->pending_count() == 0; }));
    ASSERT_TRUE(wait_until([&]() { return cache_writer->pending_count() == 0; }));
    ASSERT_TRUE(cache_range_downloaded(cache.get(), hash));
    EXPECT_EQ(read_cached_block(cache.get(), hash), content);
    const auto reads = reader->reads();
    ASSERT_EQ(reads.size(), 1);
    EXPECT_EQ(reads[0].offset, 1024);
    EXPECT_EQ(reads[0].size, 1024);
}

TEST_F(PartialBlockWritebackManagerTest, ReplacesQueuedTaskAfterEpochInvalidation) {
    auto cache = create_cache("partial_block_replace_stale", 8);
    auto* cache_writer = cache->async_write_manager();
    auto* index = cache->inflight_write_buffer_index();
    auto manager = create_manager(partial_writeback_options(1, 2));

    const std::string blocker_content = patterned_content('b');
    auto blocker_reader = std::make_shared<ControlledFileReader>(blocker_content, true);
    Defer release_blocker {[&]() { blocker_reader->release_reads(); }};
    const auto blocker_hash = BlockFileCache::hash("partial_block_replace_stale_blocker");
    ASSERT_EQ(manager->try_submit(make_request(cache_writer, index, blocker_reader, blocker_hash,
                                               blocker_content, 0, 1024)),
              PartialBlockSubmitResult::QUEUED);
    ASSERT_TRUE(blocker_reader->wait_for_entered(1));

    const std::string old_content = patterned_content('o');
    const std::string new_content = patterned_content('n');
    auto old_reader = std::make_shared<ControlledFileReader>(old_content);
    auto new_reader = std::make_shared<ControlledFileReader>(new_content);
    const auto hash = BlockFileCache::hash("partial_block_replace_stale_target");
    ASSERT_EQ(manager->try_submit(
                      make_request(cache_writer, index, old_reader, hash, old_content, 0, 1024)),
              PartialBlockSubmitResult::QUEUED);
    ASSERT_EQ(manager->active_count(), 1);
    ASSERT_EQ(manager->queued_count(), 1);

    cache_writer->invalidate_pending_writes(hash);
    EXPECT_EQ(manager->try_submit(
                      make_request(cache_writer, index, new_reader, hash, new_content, 2048, 1024)),
              PartialBlockSubmitResult::QUEUED);
    EXPECT_EQ(manager->pending_count(), 2);
    EXPECT_EQ(manager->queued_count(), 1);
    EXPECT_EQ(old_reader->read_calls(), 0);

    blocker_reader->release_reads();
    ASSERT_TRUE(wait_until([&]() { return manager->pending_count() == 0; }));
    ASSERT_TRUE(wait_until([&]() { return cache_writer->pending_count() == 0; }));
    ASSERT_TRUE(cache_range_downloaded(cache.get(), hash));
    EXPECT_EQ(read_cached_block(cache.get(), hash), new_content);
    EXPECT_EQ(old_reader->read_calls(), 0);
    EXPECT_EQ(new_reader->read_calls(), 2);
}

TEST_F(PartialBlockWritebackManagerTest, UsesReadWorkersConcurrentlyAndDeduplicatesActiveTask) {
    auto cache = create_cache("partial_block_concurrent", 8);
    auto* cache_writer = cache->async_write_manager();
    auto* index = cache->inflight_write_buffer_index();
    auto manager = create_manager(partial_writeback_options(2, 2));
    const std::string content = patterned_content('c');
    auto reader = std::make_shared<ControlledFileReader>(content, true);
    const auto first_hash = BlockFileCache::hash("partial_block_concurrent_first");
    const auto second_hash = BlockFileCache::hash("partial_block_concurrent_second");
    const auto rejected_hash = BlockFileCache::hash("partial_block_concurrent_rejected");

    EXPECT_EQ(manager->try_submit(
                      make_request(cache_writer, index, reader, first_hash, content, 0, 1024)),
              PartialBlockSubmitResult::QUEUED);
    EXPECT_EQ(manager->try_submit(
                      make_request(cache_writer, index, reader, second_hash, content, 0, 1024)),
              PartialBlockSubmitResult::QUEUED);
    ASSERT_TRUE(reader->wait_for_entered(2));
    EXPECT_EQ(manager->active_count(), 2);
    EXPECT_EQ(reader->max_active_reads(), 2);

    EXPECT_EQ(manager->try_submit(
                      make_request(cache_writer, index, reader, first_hash, content, 1024, 512)),
              PartialBlockSubmitResult::ACTIVE_DEDUPLICATED);
    EXPECT_EQ(manager->try_submit(
                      make_request(cache_writer, index, reader, rejected_hash, content, 0, 1024)),
              PartialBlockSubmitResult::REJECTED);
    EXPECT_EQ(manager->pending_count(), 2);

    reader->release_reads();
    ASSERT_TRUE(wait_until([&]() { return manager->pending_count() == 0; }));
    ASSERT_TRUE(wait_until([&]() { return cache_writer->pending_count() == 0; }));
    EXPECT_TRUE(cache_range_downloaded(cache.get(), first_hash));
    EXPECT_TRUE(cache_range_downloaded(cache.get(), second_hash));
    EXPECT_FALSE(cache_range_downloaded(cache.get(), rejected_hash));
}

TEST_F(PartialBlockWritebackManagerTest, ResizesWorkersWithoutInterruptingActiveReads) {
    auto cache = create_cache("partial_block_resize_workers", 8);
    auto* cache_writer = cache->async_write_manager();
    auto* index = cache->inflight_write_buffer_index();
    auto manager = create_manager(partial_writeback_options(1, 2));
    ASSERT_TRUE(wait_until([&]() { return manager->running_worker_count() == 1; }));
    EXPECT_EQ(manager->worker_count(), 1);

    const std::string content = patterned_content('d');
    auto reader = std::make_shared<ControlledFileReader>(content, true);
    const auto first_hash = BlockFileCache::hash("partial_block_resize_first");
    const auto second_hash = BlockFileCache::hash("partial_block_resize_second");
    ASSERT_EQ(manager->try_submit(
                      make_request(cache_writer, index, reader, first_hash, content, 0, 1024)),
              PartialBlockSubmitResult::QUEUED);
    ASSERT_TRUE(reader->wait_for_entered(1));

    ASSERT_TRUE(manager->resize_workers(2).ok());
    ASSERT_TRUE(wait_until([&]() { return manager->running_worker_count() == 2; }));
    EXPECT_EQ(manager->worker_count(), 2);
    ASSERT_EQ(manager->try_submit(
                      make_request(cache_writer, index, reader, second_hash, content, 0, 1024)),
              PartialBlockSubmitResult::QUEUED);
    ASSERT_TRUE(reader->wait_for_entered(2));
    EXPECT_EQ(reader->max_active_reads(), 2);

    auto shrink = std::async(std::launch::async, [&]() { return manager->resize_workers(1); });
    EXPECT_EQ(shrink.wait_for(50ms), std::future_status::timeout);
    reader->release_reads();
    ASSERT_EQ(shrink.wait_for(5s), std::future_status::ready);
    EXPECT_TRUE(shrink.get().ok());
    ASSERT_TRUE(wait_until([&]() { return manager->running_worker_count() == 1; }));
    EXPECT_EQ(manager->worker_count(), 1);
    EXPECT_FALSE(manager->resize_workers(0).ok());

    ASSERT_TRUE(wait_until([&]() { return manager->pending_count() == 0; }));
    ASSERT_TRUE(wait_until([&]() { return cache_writer->pending_count() == 0; }));
    EXPECT_TRUE(cache_range_downloaded(cache.get(), first_hash));
    EXPECT_TRUE(cache_range_downloaded(cache.get(), second_hash));
}

TEST_F(PartialBlockWritebackManagerTest, EvictsOldestQueuedTask) {
    auto cache = create_cache("partial_block_evict_oldest", 1);
    auto* cache_writer = cache->async_write_manager();
    auto* index = cache->inflight_write_buffer_index();
    auto manager = create_manager(partial_writeback_options(1, 2));

    OneShotSyncPointGate cache_writer_gate;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "AsyncCacheWriteManager::_persist_task:before_get_or_set",
            [&](auto&&) { cache_writer_gate.arrive_and_wait(); }, &guard);
    sync_point->enable_processing();
    Defer clear_sync_point {[&]() {
        cache_writer_gate.release();
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    }};

    const auto blocker_hash = BlockFileCache::hash("partial_block_evict_blocker");
    const std::string blocker(kBlockSize, 'x');
    ASSERT_EQ(cache_writer->try_submit_block(AsyncCacheWriteBlockRequest {
                      .cache_hash = blocker_hash,
                      .file_offset = 0,
                      .data = Slice(blocker),
                      .buffer_size = kBlockSize,
                      .admission_ctx = {},
                      .write_epoch = cache_writer->current_write_epoch(blocker_hash),
                      .inflight_index = index,
              }),
              AsyncCacheWriteBlockSubmitResult::SUBMITTED);
    ASSERT_TRUE(cache_writer_gate.wait_until_arrived());

    const std::string content = patterned_content('e');
    auto reader = std::make_shared<ControlledFileReader>(content);
    const auto first_hash = BlockFileCache::hash("partial_block_evict_first");
    const auto second_hash = BlockFileCache::hash("partial_block_evict_second");
    const auto third_hash = BlockFileCache::hash("partial_block_evict_third");
    EXPECT_EQ(manager->try_submit(
                      make_request(cache_writer, index, reader, first_hash, content, 0, 1024)),
              PartialBlockSubmitResult::QUEUED);
    EXPECT_EQ(manager->try_submit(
                      make_request(cache_writer, index, reader, second_hash, content, 0, 1024)),
              PartialBlockSubmitResult::QUEUED);
    EXPECT_EQ(manager->try_submit(
                      make_request(cache_writer, index, reader, third_hash, content, 0, 1024)),
              PartialBlockSubmitResult::QUEUED);
    EXPECT_EQ(manager->pending_count(), 2);
    EXPECT_EQ(manager->queued_count(), 2);

    cache_writer_gate.release();
    ASSERT_TRUE(wait_until([&]() { return manager->pending_count() == 0; }));
    ASSERT_TRUE(wait_until([&]() { return cache_writer->pending_count() == 0; }));
    EXPECT_FALSE(cache_range_downloaded(cache.get(), first_hash));
    EXPECT_TRUE(cache_range_downloaded(cache.get(), second_hash));
    EXPECT_TRUE(cache_range_downloaded(cache.get(), third_hash));
}

TEST_F(PartialBlockWritebackManagerTest, DropsFailedAndShortReads) {
    auto cache = create_cache("partial_block_read_failures", 8);
    auto* cache_writer = cache->async_write_manager();
    auto* index = cache->inflight_write_buffer_index();
    auto manager = create_manager(partial_writeback_options(2, 2));
    const std::string content = patterned_content('f');
    auto failed_reader = std::make_shared<ControlledFileReader>(content);
    auto short_reader = std::make_shared<ControlledFileReader>(content);
    failed_reader->fail_at(1024);
    short_reader->return_short_at(1024);
    const auto failed_hash = BlockFileCache::hash("partial_block_failed_read");
    const auto short_hash = BlockFileCache::hash("partial_block_short_read");

    EXPECT_EQ(manager->try_submit(make_request(cache_writer, index, failed_reader, failed_hash,
                                               content, 0, 1024)),
              PartialBlockSubmitResult::QUEUED);
    EXPECT_EQ(manager->try_submit(make_request(cache_writer, index, short_reader, short_hash,
                                               content, 0, 1024)),
              PartialBlockSubmitResult::QUEUED);
    ASSERT_TRUE(wait_until([&]() { return manager->pending_count() == 0; }));
    EXPECT_FALSE(cache_range_downloaded(cache.get(), failed_hash));
    EXPECT_FALSE(cache_range_downloaded(cache.get(), short_hash));
}

TEST_F(PartialBlockWritebackManagerTest, ShutdownWaitsForActiveRemoteRead) {
    auto cache = create_cache("partial_block_shutdown", 8);
    auto* cache_writer = cache->async_write_manager();
    auto* index = cache->inflight_write_buffer_index();
    auto manager = create_manager(partial_writeback_options(1, 1));
    const std::string content = patterned_content('s');
    auto reader = std::make_shared<ControlledFileReader>(content, true);
    const auto hash = BlockFileCache::hash("partial_block_shutdown_target");

    EXPECT_EQ(
            manager->try_submit(make_request(cache_writer, index, reader, hash, content, 0, 1024)),
            PartialBlockSubmitResult::QUEUED);
    ASSERT_TRUE(reader->wait_for_entered(1));
    auto shutdown = std::async(std::launch::async, [&]() { manager->shutdown(); });
    EXPECT_EQ(shutdown.wait_for(50ms), std::future_status::timeout);
    reader->release_reads();
    EXPECT_EQ(shutdown.wait_for(5s), std::future_status::ready);
    EXPECT_FALSE(manager->accepting());
    EXPECT_EQ(manager->pending_count(), 0);
}

TEST_F(PartialBlockWritebackManagerTest, RejectsStoppedCacheWriterWithoutQueueing) {
    auto cache = create_cache("partial_block_stopped_cache_writer", 8);
    auto* cache_writer = cache->async_write_manager();
    auto* index = cache->inflight_write_buffer_index();
    auto manager = create_manager();
    const std::string content = patterned_content('t');
    auto reader = std::make_shared<ControlledFileReader>(content);
    const auto hash = BlockFileCache::hash("partial_block_stopped_cache_writer_target");
    cache_writer->shutdown();

    EXPECT_EQ(
            manager->try_submit(make_request(cache_writer, index, reader, hash, content, 0, 1024)),
            PartialBlockSubmitResult::REJECTED);
    EXPECT_EQ(manager->pending_count(), 0);
    EXPECT_EQ(reader->read_calls(), 0);
}

TEST_F(PartialBlockWritebackManagerTest, DropsQueuedTaskWhenCacheWriterStops) {
    auto cache = create_cache("partial_block_cache_writer_stops", 1);
    auto* cache_writer = cache->async_write_manager();
    auto* index = cache->inflight_write_buffer_index();
    auto manager = create_manager();

    OneShotSyncPointGate cache_writer_gate;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "AsyncCacheWriteManager::_persist_task:before_get_or_set",
            [&](auto&&) { cache_writer_gate.arrive_and_wait(); }, &guard);
    sync_point->enable_processing();
    Defer clear_sync_point {[&]() {
        cache_writer_gate.release();
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    }};

    const auto blocker_hash = BlockFileCache::hash("partial_block_cache_writer_stops_blocker");
    const std::string blocker(kBlockSize, 'x');
    ASSERT_EQ(cache_writer->try_submit_block(AsyncCacheWriteBlockRequest {
                      .cache_hash = blocker_hash,
                      .file_offset = 0,
                      .data = Slice(blocker),
                      .buffer_size = kBlockSize,
                      .admission_ctx = {},
                      .write_epoch = cache_writer->current_write_epoch(blocker_hash),
                      .inflight_index = index,
              }),
              AsyncCacheWriteBlockSubmitResult::SUBMITTED);
    ASSERT_TRUE(cache_writer_gate.wait_until_arrived());

    const std::string content = patterned_content('u');
    auto reader = std::make_shared<ControlledFileReader>(content);
    const auto hash = BlockFileCache::hash("partial_block_cache_writer_stops_target");
    ASSERT_EQ(
            manager->try_submit(make_request(cache_writer, index, reader, hash, content, 0, 1024)),
            PartialBlockSubmitResult::QUEUED);
    ASSERT_EQ(manager->queued_count(), 1);

    auto shutdown = std::async(std::launch::async, [&]() { cache_writer->shutdown(); });
    ASSERT_TRUE(wait_until([&]() { return !cache_writer->accepting(); }));
    ASSERT_TRUE(wait_until([&]() { return manager->pending_count() == 0; }));
    EXPECT_EQ(reader->read_calls(), 0);

    cache_writer_gate.release();
    EXPECT_EQ(shutdown.wait_for(5s), std::future_status::ready);
}

TEST(PartialBlockWritebackOptionsTest, RejectsInvalidLimits) {
    auto options = partial_writeback_options();
    options.worker_count = 0;
    EXPECT_FALSE(options.validate().ok());
    options = partial_writeback_options();
    options.worker_count = 129;
    EXPECT_FALSE(options.validate().ok());
    options = partial_writeback_options();
    options.max_pending_bytes = kBlockSize - 1;
    EXPECT_FALSE(options.validate().ok());
}

TEST(PartialBlockWritebackOptionsTest, AllowsHoleLargerThanCoalesceLimit) {
    auto options = partial_writeback_options();
    options.hole_fill_coalesce.max_range_bytes = kBlockSize / 2;
    EXPECT_TRUE(options.validate().ok());
}

TEST(PartialBlockWritebackOptionsTest, AcceptsProductionDefaults) {
    PartialBlockWritebackOptions options {
            .block_size = 1_mb,
            .worker_count = 32,
            .max_pending_bytes = 256_mb,
            .hole_fill_coalesce =
                    {
                            .max_gap_bytes = 32_kb,
                            .max_range_bytes = 1_mb,
                            .max_read_amplification_ratio = 2.0,
                    },
    };
    EXPECT_TRUE(options.validate().ok());
}

} // namespace
} // namespace doris::io
