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

#include "common/config.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache_test_common.h"
#include "io/cache/inflight_write_buffer_index.h"
#include "io/fs/path.h"
#include "io/fs/read_ahead_metrics.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

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
                                                       size_t pending_blocks = 4,
                                                       int remote_read_threads = 2) {
    return PartialBlockWritebackOptions {
            .block_size = kBlockSize,
            .worker_count = workers,
            .remote_read_thread_count = remote_read_threads,
            .merge_delay_ms = 0,
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
    std::chrono::steady_clock::time_point started_at;
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

    void release_read(size_t offset) {
        {
            std::lock_guard lock(_mutex);
            _released_offsets.insert(offset);
        }
        _cv.notify_all();
    }

    bool wait_for_completed(size_t count) {
        std::unique_lock lock(_mutex);
        return _cv.wait_for(lock, 5s, [&]() { return _completed >= count; });
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
        auto result = _reads;
        // GETs may enter in any order; callers compare ranges in file order.
        std::ranges::sort(result, {}, &ObservedRead::offset);
        return result;
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
                    .started_at = std::chrono::steady_clock::now(),
            };
            if (io_context != nullptr) {
                observed.bypass_peer_read = io_context->bypass_peer_read;
                observed.should_stop = io_context->should_stop;
                observed.cache_write_mode = io_context->cache_write_mode_override;
            }
            _reads.push_back(observed);
            _cv.notify_all();
            if (_block_reads) {
                _cv.wait(lock,
                         [&]() { return _release_reads || _released_offsets.contains(offset); });
            }
        }
        Defer completed {[&]() {
            std::lock_guard lock(_mutex);
            --_active_reads;
            ++_completed;
            _cv.notify_all();
        }};

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
    size_t _completed {0};
    size_t _active_reads {0};
    size_t _max_active_reads {0};
    std::set<size_t> _failed_offsets;
    std::set<size_t> _short_offsets;
    std::set<size_t> _released_offsets;
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

    void cache_block(BlockFileCache* cache, const UInt128Wrapper& hash,
                     const std::string& content) {
        ReadStatistics stats;
        CacheContext context;
        context.stats = &stats;
        auto holder = cache->get_or_set(hash, 0, content.size(), context);
        ASSERT_EQ(holder.file_blocks.size(), 1);
        auto block = holder.file_blocks.front();
        ASSERT_EQ(block->get_or_set_downloader(), FileBlock::get_caller_id());
        ASSERT_TRUE(block->append(Slice(content)).ok());
        ASSERT_TRUE(block->finalize().ok());
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

TEST_F(PartialBlockWritebackManagerTest, SkipsDownloadedBlockWithoutAllocating) {
    auto cache = create_cache("partial_block_already_cached");
    auto* writer = cache->async_write_manager();
    auto manager = create_manager();
    const auto hash = BlockFileCache::hash("partial_block_already_cached");
    const auto content = patterned_content('a');
    auto reader = std::make_shared<ControlledFileReader>(content);
    cache_block(cache.get(), hash, content);

    EXPECT_EQ(manager->try_submit(make_request(writer, cache->inflight_write_buffer_index(), reader,
                                               hash, content, 1024, 1024)),
              PartialBlockSubmitResult::CACHE_BLOCK_PRESENT);
    EXPECT_EQ(manager->pending_count(), 0);
    EXPECT_EQ(writer->buffer_memory_bytes(), 0);
    EXPECT_EQ(reader->read_calls(), 0);
    EXPECT_EQ(read_cached_block(cache.get(), hash), content);
}

TEST_F(PartialBlockWritebackManagerTest, SkipsInflightBlockWithoutProbingCache) {
    auto cache = create_cache("partial_block_inflight_before_probe");
    auto* writer = cache->async_write_manager();
    auto* index = cache->inflight_write_buffer_index();
    auto manager = create_manager();
    const auto hash = BlockFileCache::hash("partial_block_inflight_before_probe");
    const auto content = patterned_content('i');
    auto reader = std::make_shared<ControlledFileReader>(content);
    AsyncCacheWriteBufferPtr buffer;
    ASSERT_TRUE(writer->allocate_tracked_buffer(kBlockSize, &buffer).ok());
    std::memcpy(buffer->data(), content.data(), content.size());
    auto entry = std::make_shared<InflightWriteBufferEntry>(buffer, 0, kBlockSize, 0);
    ASSERT_EQ(index->insert_if_absent(hash, 0, entry), nullptr);
    Defer remove_entry {[&]() { index->remove_if(hash, 0, entry); }};

    std::future<PartialBlockSubmitResult> submission;
    {
        // An inflight hit must complete even while the cache-probe mutex is held.
        std::lock_guard cache_lock(cache->_mutex);
        submission = std::async(std::launch::async, [&]() {
            return manager->try_submit(make_request(writer, index, reader, hash, content, 0, 1024));
        });
        EXPECT_EQ(submission.wait_for(1s), std::future_status::ready);
    }
    EXPECT_EQ(submission.get(), PartialBlockSubmitResult::CACHE_BLOCK_PRESENT);
    EXPECT_EQ(reader->read_calls(), 0);
    EXPECT_EQ(manager->pending_count(), 0);
    EXPECT_EQ(writer->buffer_memory_bytes(), kBlockSize);
}

TEST_F(PartialBlockWritebackManagerTest, SkipsDownloadedEofBlock) {
    auto cache = create_cache("partial_block_cached_eof");
    auto* writer = cache->async_write_manager();
    auto manager = create_manager();
    const auto hash = BlockFileCache::hash("partial_block_cached_eof");
    const std::string content(1500, 'e');
    auto reader = std::make_shared<ControlledFileReader>(content);
    cache_block(cache.get(), hash, content);
    auto request = make_request(writer, nullptr, reader, hash, content, 1000, 500);
    request.block_valid_size = content.size();

    EXPECT_EQ(manager->try_submit(std::move(request)),
              PartialBlockSubmitResult::CACHE_BLOCK_PRESENT);
    EXPECT_EQ(manager->pending_count(), 0);
    EXPECT_EQ(writer->buffer_memory_bytes(), 0);
    EXPECT_EQ(reader->read_calls(), 0);
}

TEST_F(PartialBlockWritebackManagerTest, SkipsBlockWithExistingDownloader) {
    auto cache = create_cache("partial_block_downloading");
    auto* writer = cache->async_write_manager();
    auto manager = create_manager();
    const auto hash = BlockFileCache::hash("partial_block_downloading");
    const auto content = patterned_content('d');
    auto reader = std::make_shared<ControlledFileReader>(content);
    ReadStatistics stats;
    CacheContext context;
    context.stats = &stats;
    auto holder = cache->get_or_set(hash, 0, kBlockSize, context);
    auto block = holder.file_blocks.front();
    ASSERT_EQ(block->get_or_set_downloader(), FileBlock::get_caller_id());

    EXPECT_EQ(manager->try_submit(make_request(writer, nullptr, reader, hash, content, 0, 1024)),
              PartialBlockSubmitResult::CACHE_BLOCK_PRESENT);
    EXPECT_EQ(reader->read_calls(), 0);
    EXPECT_EQ(manager->pending_count(), 0);
    EXPECT_EQ(block->state(), FileBlock::State::DOWNLOADING);
    EXPECT_EQ(block->get_downloader(), FileBlock::get_caller_id());
}

TEST_F(PartialBlockWritebackManagerTest, FillsExistingEmptyBlock) {
    auto cache = create_cache("partial_block_empty_cell");
    auto* writer = cache->async_write_manager();
    auto options = partial_writeback_options();
    options.merge_delay_ms = 50;
    auto manager = create_manager(options);
    const auto hash = BlockFileCache::hash("partial_block_empty_cell");
    const auto content = patterned_content('e');
    auto reader = std::make_shared<ControlledFileReader>(content);
    ReadStatistics stats;
    CacheContext context;
    context.stats = &stats;
    auto holder = cache->get_or_set(hash, 0, kBlockSize, context);
    ASSERT_EQ(holder.file_blocks.front()->state(), FileBlock::State::EMPTY);

    const auto submitted_at = std::chrono::steady_clock::now();
    ASSERT_EQ(manager->try_submit(make_request(writer, nullptr, reader, hash, content, 0, 1024)),
              PartialBlockSubmitResult::QUEUED);
    ASSERT_TRUE(wait_until([&]() { return manager->pending_count() == 0; }));
    ASSERT_TRUE(wait_until([&]() { return writer->pending_count() == 0; }));
    const auto reads = reader->reads();
    ASSERT_EQ(reads.size(), 1);
    EXPECT_GE(reads[0].started_at - submitted_at, 50ms);
    EXPECT_EQ(read_cached_block(cache.get(), hash), content);
}

TEST_F(PartialBlockWritebackManagerTest, SkipsBlockCachedWhileQueued) {
    auto cache = create_cache("partial_block_cached_while_queued");
    auto* writer = cache->async_write_manager();
    auto manager = create_manager(partial_writeback_options(1, 2));
    const auto content = patterned_content('q');
    auto blocker = std::make_shared<ControlledFileReader>(content, true);
    auto reader = std::make_shared<ControlledFileReader>(content);
    Defer release {[&]() { blocker->release_reads(); }};
    const auto blocker_hash = BlockFileCache::hash("partial_block_queued_blocker");
    const auto hash = BlockFileCache::hash("partial_block_cached_while_queued");
    ASSERT_EQ(manager->try_submit(
                      make_request(writer, nullptr, blocker, blocker_hash, content, 0, 1024)),
              PartialBlockSubmitResult::QUEUED);
    ASSERT_TRUE(blocker->wait_for_entered(1));
    ASSERT_EQ(manager->try_submit(make_request(writer, nullptr, reader, hash, content, 0, 1024)),
              PartialBlockSubmitResult::QUEUED);
    ASSERT_EQ(manager->queued_count(), 1);

    cache_block(cache.get(), hash, content);
    blocker->release_reads();
    ASSERT_TRUE(wait_until([&]() { return manager->pending_count() == 0; }));
    ASSERT_TRUE(wait_until([&]() { return writer->pending_count() == 0; }));
    EXPECT_EQ(reader->read_calls(), 0);
    EXPECT_EQ(read_cached_block(cache.get(), hash), content);
}

TEST_F(PartialBlockWritebackManagerTest, CompletesPlannedReadsWhenBlockIsCachedDuringRead) {
    auto cache = create_cache("partial_block_cached_between_reads");
    auto* writer = cache->async_write_manager();
    auto manager = create_manager();
    const auto hash = BlockFileCache::hash("partial_block_cached_between_reads");
    const auto content = patterned_content('c');
    auto reader = std::make_shared<ControlledFileReader>(content, true);
    Defer release {[&]() { reader->release_reads(); }};
    ASSERT_EQ(manager->try_submit(make_request(writer, nullptr, reader, hash, content, 1024, 1024)),
              PartialBlockSubmitResult::QUEUED);
    ASSERT_TRUE(reader->wait_for_entered(1));

    cache_block(cache.get(), hash, content);
    reader->release_reads();
    ASSERT_TRUE(wait_until([&]() { return manager->pending_count() == 0; }));
    ASSERT_TRUE(wait_until([&]() { return writer->pending_count() == 0; }));
    const auto reads = reader->reads();
    ASSERT_EQ(reads.size(), 2);
    EXPECT_EQ(reads.front().offset, 0);
    EXPECT_EQ(reads.front().size, 1024);
    EXPECT_EQ(reads.back().offset, 2048);
    EXPECT_EQ(reads.back().size, kBlockSize - 2048);
    EXPECT_EQ(read_cached_block(cache.get(), hash), content);
}

TEST_F(PartialBlockWritebackManagerTest,
       CompletesPlannedReadsWhenInflightBufferIsPublishedDuringRead) {
    auto cache = create_cache("partial_block_inflight_between_reads");
    auto* writer = cache->async_write_manager();
    auto* index = cache->inflight_write_buffer_index();
    auto manager = create_manager();
    const auto hash = BlockFileCache::hash("partial_block_inflight_between_reads");
    const auto content = patterned_content('i');
    auto reader = std::make_shared<ControlledFileReader>(content, true);
    Defer release {[&]() { reader->release_reads(); }};
    ASSERT_EQ(manager->try_submit(make_request(writer, index, reader, hash, content, 1024, 1024)),
              PartialBlockSubmitResult::QUEUED);
    ASSERT_TRUE(reader->wait_for_entered(1));

    AsyncCacheWriteBufferPtr buffer;
    ASSERT_TRUE(writer->allocate_tracked_buffer(kBlockSize, &buffer).ok());
    std::memcpy(buffer->data(), content.data(), content.size());
    auto entry = std::make_shared<InflightWriteBufferEntry>(buffer, 0, kBlockSize, 0);
    ASSERT_EQ(index->insert_if_absent(hash, 0, entry), nullptr);
    Defer remove_entry {[&]() { index->remove_if(hash, 0, entry); }};
    reader->release_reads();
    ASSERT_TRUE(wait_until([&]() { return manager->pending_count() == 0; }));
    EXPECT_EQ(reader->read_calls(), 2);
    EXPECT_EQ(writer->pending_count(), 0);
    EXPECT_FALSE(cache_range_downloaded(cache.get(), hash));
}

TEST_F(PartialBlockWritebackManagerTest, MergesQueuedFragmentsAndWaitsForCacheWriterCapacity) {
    auto cache = create_cache("partial_block_merge", 1);
    auto* cache_writer = cache->async_write_manager();
    auto* index = cache->inflight_write_buffer_index();
    auto options = partial_writeback_options(2, 4);
    options.merge_delay_ms = 60000;
    auto manager = create_manager(options);
    const auto next_wakeup = [&]() {
        PartialBlockWritebackManager::Queue discarded;
        auto deadline = std::chrono::steady_clock::time_point::max();
        std::lock_guard lock(manager->_mutex);
        EXPECT_EQ(manager->_take_runnable_task_locked(&discarded, &deadline), nullptr);
        EXPECT_TRUE(discarded.empty());
        return deadline;
    };

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
    const auto submitted_at = std::chrono::steady_clock::now();
    EXPECT_EQ(
            manager->try_submit(make_request(cache_writer, index, reader, hash, content, 0, 1024)),
            PartialBlockSubmitResult::QUEUED);
    const auto original_deadline = next_wakeup();
    EXPECT_GE(original_deadline - submitted_at, 60s);
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
    EXPECT_EQ(manager->active_count(), 0);
    EXPECT_EQ(next_wakeup(), original_deadline);
    manager->set_merge_delay_ms(120000);
    EXPECT_EQ(next_wakeup(), original_deadline + 60s);
    std::this_thread::sleep_for(30ms);
    EXPECT_EQ(reader->read_calls(), 0);

    cache_writer_gate.release();
    ASSERT_TRUE(wait_until([&]() { return cache_writer->pending_count() == 0; }));
    EXPECT_EQ(manager->queued_count(), 2);
    EXPECT_EQ(reader->read_calls(), 0);
    // Disable the delay for existing tasks and wake workers waiting on the old deadline.
    manager->set_merge_delay_ms(0);
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
    auto& metrics = read_ahead_bvars();
    const auto pending_before = metrics.hole_fill_pending_bytes.get_value();
    const auto active_before = metrics.hole_fill_active_blocks.get_value();
    const auto requests_before = metrics.hole_fill_remote_requests.get_value();
    const auto bytes_before = metrics.hole_fill_remote_bytes.get_value();
    const auto submitted_before = metrics.hole_fill_write_submitted_blocks.get_value();
    auto cache = create_cache("partial_block_concurrent", 8);
    auto* cache_writer = cache->async_write_manager();
    auto* index = cache->inflight_write_buffer_index();
    // Single-range reads use block workers independently of the remote-read pool.
    auto manager = create_manager(partial_writeback_options(2, 2, 1));
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
    manager->set_merge_delay_ms(60000);
    EXPECT_EQ(manager->active_count(), 2);
    EXPECT_EQ(reader->max_active_reads(), 2);
    EXPECT_EQ(metrics.hole_fill_pending_bytes.get_value() - pending_before, 2 * kBlockSize);
    EXPECT_EQ(metrics.hole_fill_active_blocks.get_value() - active_before, 2);
    EXPECT_EQ(metrics.hole_fill_remote_requests.get_value() - requests_before, 2);

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
    EXPECT_EQ(metrics.hole_fill_pending_bytes.get_value(), pending_before);
    EXPECT_EQ(metrics.hole_fill_active_blocks.get_value(), active_before);
    EXPECT_EQ(metrics.hole_fill_remote_bytes.get_value() - bytes_before, 2 * (kBlockSize - 1024));
    EXPECT_EQ(metrics.hole_fill_write_submitted_blocks.get_value() - submitted_before, 2);
}

TEST_F(PartialBlockWritebackManagerTest, ReadsHolesConcurrentlyWithinOneBlock) {
    auto& metrics = read_ahead_bvars();
    const auto requests_before = metrics.hole_fill_remote_requests.get_value();
    const auto bytes_before = metrics.hole_fill_remote_bytes.get_value();
    auto cache = create_cache("partial_block_parallel_holes");
    auto* writer = cache->async_write_manager();
    auto manager = create_manager(partial_writeback_options(1, 1, 2));
    const auto hash = BlockFileCache::hash("partial_block_parallel_holes");
    const auto content = patterned_content('p');
    auto reader = std::make_shared<ControlledFileReader>(content, true);
    Defer release {[&]() { reader->release_reads(); }};

    ASSERT_EQ(manager->try_submit(make_request(writer, nullptr, reader, hash, content, 1024, 1024)),
              PartialBlockSubmitResult::QUEUED);
    ASSERT_TRUE(reader->wait_for_entered(2));
    EXPECT_EQ(reader->max_active_reads(), 2);
    EXPECT_EQ(manager->active_count(), 1);
    EXPECT_EQ(writer->pending_count(), 0);
    EXPECT_EQ(writer->buffer_memory_bytes(), kBlockSize);

    reader->release_reads();
    ASSERT_TRUE(wait_until([&]() { return manager->pending_count() == 0; }));
    ASSERT_TRUE(wait_until([&]() { return writer->pending_count() == 0; }));
    EXPECT_EQ(read_cached_block(cache.get(), hash), content);
    EXPECT_EQ(metrics.hole_fill_remote_requests.get_value() - requests_before, 2);
    EXPECT_EQ(metrics.hole_fill_remote_bytes.get_value() - bytes_before, kBlockSize - 1024);
}

TEST_F(PartialBlockWritebackManagerTest, SharesRemoteReadPoolLimitAcrossBlocks) {
    auto cache = create_cache("partial_block_shared_read_pool", 8);
    auto* writer = cache->async_write_manager();
    auto manager = create_manager(partial_writeback_options(2, 2, 1));
    const auto content = patterned_content('l');
    auto reader = std::make_shared<ControlledFileReader>(content, true);
    Defer release {[&]() { reader->release_reads(); }};
    for (const auto* name : {"shared_read_pool_first", "shared_read_pool_second"}) {
        ASSERT_EQ(
                manager->try_submit(make_request(writer, nullptr, reader,
                                                 BlockFileCache::hash(name), content, 1024, 1024)),
                PartialBlockSubmitResult::QUEUED);
    }
    ASSERT_TRUE(reader->wait_for_entered(1));
    ASSERT_TRUE(wait_until([&]() { return manager->_remote_read_pool->get_queue_size() == 3; }));
    EXPECT_EQ(manager->active_count(), 2);
    EXPECT_EQ(reader->read_calls(), 1);
    EXPECT_EQ(reader->max_active_reads(), 1);
    reader->release_reads();
    ASSERT_TRUE(wait_until([&]() { return manager->pending_count() == 0; }));
    ASSERT_TRUE(wait_until([&]() { return writer->pending_count() == 0; }));
    EXPECT_EQ(reader->read_calls(), 4);
    EXPECT_EQ(reader->max_active_reads(), 1);
}

TEST_F(PartialBlockWritebackManagerTest, ResizesRemoteReadPoolWithoutInterruptingActiveReads) {
    auto cache = create_cache("partial_block_resize_read_pool");
    auto* writer = cache->async_write_manager();
    auto manager = create_manager(partial_writeback_options(1, 1, 1));
    const auto content = patterned_content('r');
    auto reader = std::make_shared<ControlledFileReader>(content, true);
    Defer release {[&]() { reader->release_reads(); }};
    const auto hash = BlockFileCache::hash("partial_block_resize_read_pool");
    ASSERT_EQ(manager->try_submit(make_request(writer, nullptr, reader, hash, content, 1024, 1024)),
              PartialBlockSubmitResult::QUEUED);
    ASSERT_TRUE(reader->wait_for_entered(1));
    ASSERT_TRUE(wait_until([&]() { return manager->_remote_read_pool->get_queue_size() == 1; }));
    ASSERT_TRUE(manager->resize_remote_read_threads(2).ok());
    ASSERT_TRUE(reader->wait_for_entered(2));
    EXPECT_EQ(reader->max_active_reads(), 2);
    ASSERT_TRUE(manager->resize_remote_read_threads(1).ok());
    EXPECT_EQ(manager->_remote_read_pool->max_threads(), 1);
    EXPECT_EQ(writer->pending_count(), 0);
    reader->release_reads();
    ASSERT_TRUE(wait_until([&]() { return manager->pending_count() == 0; }));
    ASSERT_TRUE(wait_until([&]() { return writer->pending_count() == 0; }));
    EXPECT_EQ(read_cached_block(cache.get(), hash), content);
    ASSERT_TRUE(wait_until([&]() { return manager->_remote_read_pool->num_threads() <= 1; }));
    EXPECT_FALSE(manager->resize_remote_read_threads(0).ok());
    EXPECT_FALSE(manager->resize_remote_read_threads(-1).ok());
    manager->shutdown();
    EXPECT_FALSE(manager->resize_remote_read_threads(2).ok());
}

TEST_F(PartialBlockWritebackManagerTest, WaitsForOtherHolesAfterReadFailure) {
    for (bool short_read : {false, true}) {
        auto& metrics = read_ahead_bvars();
        const auto failed_before = metrics.hole_fill_failed_blocks.get_value();
        auto cache =
                create_cache(short_read ? "parallel_hole_short_read" : "parallel_hole_failure");
        auto* writer = cache->async_write_manager();
        auto manager = create_manager(partial_writeback_options(1, 1, 2));
        const auto content = patterned_content('f');
        auto reader = std::make_shared<ControlledFileReader>(content, true);
        Defer release {[&]() { reader->release_reads(); }};
        if (short_read) {
            reader->return_short_at(0);
        } else {
            reader->fail_at(0);
        }
        const auto hash = BlockFileCache::hash("parallel_hole_failure");
        ASSERT_EQ(manager->try_submit(
                          make_request(writer, nullptr, reader, hash, content, 1024, 1024)),
                  PartialBlockSubmitResult::QUEUED);
        ASSERT_TRUE(reader->wait_for_entered(2));
        reader->release_read(0);
        ASSERT_TRUE(reader->wait_for_completed(1));
        EXPECT_EQ(manager->pending_count(), 1);
        EXPECT_EQ(writer->pending_count(), 0);
        EXPECT_EQ(writer->buffer_memory_bytes(), kBlockSize);
        reader->release_reads();
        ASSERT_TRUE(wait_until([&]() { return manager->pending_count() == 0; }));
        EXPECT_EQ(writer->pending_count(), 0);
        EXPECT_FALSE(cache_range_downloaded(cache.get(), hash));
        EXPECT_EQ(metrics.hole_fill_failed_blocks.get_value() - failed_before, 1);
        ASSERT_TRUE(wait_until([&]() { return writer->buffer_memory_bytes() == 0; }));
    }
}

TEST_F(PartialBlockWritebackManagerTest, JoinsAcceptedReadsWhenSubmissionFails) {
    auto& metrics = read_ahead_bvars();
    const auto failed_before = metrics.hole_fill_failed_blocks.get_value();
    auto cache = create_cache("partial_block_read_submit_failure");
    auto* writer = cache->async_write_manager();
    auto manager = create_manager(partial_writeback_options(1, 1, 1));
    {
        std::lock_guard lifecycle_lock(manager->_lifecycle_mutex);
        // Release the worker's token before replacing its remote-read pool.
        manager->_stop_workers_locked(0);
        // One blocked GET occupies the only slot; the next submission must be rejected.
        ASSERT_TRUE(ThreadPoolBuilder("HoleFillRemoteReadRejectTest")
                            .set_min_threads(1)
                            .set_max_threads(1)
                            .set_max_queue_size(0)
                            .build(&manager->_remote_read_pool)
                            .ok());
        ASSERT_TRUE(manager->_resize_workers_locked(1).ok());
    }
    const auto content = patterned_content('j');
    auto reader = std::make_shared<ControlledFileReader>(content, true);
    Defer release {[&]() { reader->release_reads(); }};
    const auto hash = BlockFileCache::hash("partial_block_read_submit_failure");
    ASSERT_EQ(manager->try_submit(make_request(writer, nullptr, reader, hash, content, 1024, 1024)),
              PartialBlockSubmitResult::QUEUED);
    ASSERT_TRUE(reader->wait_for_entered(1));
    ASSERT_TRUE(wait_until(
            [&]() { return manager->_remote_read_pool->thread_pool_submit_failed->value() == 1; }));
    EXPECT_EQ(manager->pending_count(), 1);
    EXPECT_EQ(writer->buffer_memory_bytes(), kBlockSize);
    reader->release_reads();
    ASSERT_TRUE(wait_until([&]() { return manager->pending_count() == 0; }));
    EXPECT_EQ(reader->read_calls(), 1);
    EXPECT_EQ(writer->pending_count(), 0);
    EXPECT_FALSE(cache_range_downloaded(cache.get(), hash));
    EXPECT_EQ(metrics.hole_fill_failed_blocks.get_value() - failed_before, 1);
    ASSERT_TRUE(wait_until([&]() { return writer->buffer_memory_bytes() == 0; }));
}

TEST_F(PartialBlockWritebackManagerTest, ShutdownWaitsForActiveAndQueuedHoleReads) {
    auto cache = create_cache("partial_block_shutdown_holes");
    auto* writer = cache->async_write_manager();
    auto manager = create_manager(partial_writeback_options(1, 1, 1));
    const auto content = patterned_content('s');
    auto reader = std::make_shared<ControlledFileReader>(content, true);
    std::future<void> shutdown;
    Defer release {[&]() { reader->release_reads(); }};
    const auto hash = BlockFileCache::hash("partial_block_shutdown_holes");
    ASSERT_EQ(manager->try_submit(make_request(writer, nullptr, reader, hash, content, 1024, 1024)),
              PartialBlockSubmitResult::QUEUED);
    ASSERT_TRUE(reader->wait_for_entered(1));
    ASSERT_TRUE(wait_until([&]() { return manager->_remote_read_pool->get_queue_size() == 1; }));
    shutdown = std::async(std::launch::async, [&]() { manager->shutdown(); });
    EXPECT_EQ(shutdown.wait_for(50ms), std::future_status::timeout);
    reader->release_read(0);
    ASSERT_TRUE(reader->wait_for_entered(2));
    EXPECT_EQ(shutdown.wait_for(50ms), std::future_status::timeout);
    reader->release_reads();
    ASSERT_EQ(shutdown.wait_for(5s), std::future_status::ready);
    shutdown.get();
    EXPECT_EQ(reader->read_calls(), 2);
    EXPECT_EQ(manager->pending_count(), 0);
    EXPECT_FALSE(manager->accepting());
    ASSERT_TRUE(wait_until([&]() { return writer->pending_count() == 0; }));
    EXPECT_EQ(read_cached_block(cache.get(), hash), content);
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
    auto& metrics = read_ahead_bvars();
    const auto failed_before = metrics.hole_fill_failed_blocks.get_value();
    const auto pending_before = metrics.hole_fill_pending_bytes.get_value();
    const auto dropped_before = metrics.hole_fill_dropped_blocks.get_value();
    auto cache = create_cache("partial_block_evict_oldest", 1);
    auto* cache_writer = cache->async_write_manager();
    auto* index = cache->inflight_write_buffer_index();
    auto manager = create_manager(partial_writeback_options(1, 2));
    manager->set_merge_delay_ms(60000);

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

    EXPECT_EQ(metrics.hole_fill_pending_bytes.get_value() - pending_before, 2 * kBlockSize);
    EXPECT_EQ(metrics.hole_fill_dropped_blocks.get_value() - dropped_before, 1);
    EXPECT_EQ(metrics.hole_fill_failed_blocks.get_value(), failed_before);

    manager->set_merge_delay_ms(0);
    cache_writer_gate.release();
    ASSERT_TRUE(wait_until([&]() { return manager->pending_count() == 0; }));
    ASSERT_TRUE(wait_until([&]() { return cache_writer->pending_count() == 0; }));
    EXPECT_FALSE(cache_range_downloaded(cache.get(), first_hash));
    EXPECT_TRUE(cache_range_downloaded(cache.get(), second_hash));
    EXPECT_TRUE(cache_range_downloaded(cache.get(), third_hash));
    EXPECT_EQ(metrics.hole_fill_pending_bytes.get_value(), pending_before);
}

TEST_F(PartialBlockWritebackManagerTest, DropsFailedAndShortReads) {
    auto& metrics = read_ahead_bvars();
    const auto failed_before = metrics.hole_fill_failed_blocks.get_value();
    const auto read_time_before = metrics.hole_fill_remote_read_time_ns.get_value();
    const auto dropped_before = metrics.hole_fill_dropped_blocks.get_value();
    const auto submitted_before = metrics.hole_fill_write_submitted_blocks.get_value();
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
    EXPECT_EQ(metrics.hole_fill_dropped_blocks.get_value() - dropped_before, 2);
    EXPECT_EQ(metrics.hole_fill_failed_blocks.get_value() - failed_before, 2);
    EXPECT_GT(metrics.hole_fill_remote_read_time_ns.get_value(), read_time_before);
    EXPECT_EQ(metrics.hole_fill_write_submitted_blocks.get_value(), submitted_before);
}

TEST_F(PartialBlockWritebackManagerTest, ShutdownWaitsForActiveRemoteRead) {
    auto cache = create_cache("partial_block_shutdown", 8);
    auto* cache_writer = cache->async_write_manager();
    auto* index = cache->inflight_write_buffer_index();
    auto manager = create_manager(partial_writeback_options(1, 2));
    const std::string content = patterned_content('s');
    auto reader = std::make_shared<ControlledFileReader>(content, true);
    const auto hash = BlockFileCache::hash("partial_block_shutdown_target");

    EXPECT_EQ(
            manager->try_submit(make_request(cache_writer, index, reader, hash, content, 0, 1024)),
            PartialBlockSubmitResult::QUEUED);
    ASSERT_TRUE(reader->wait_for_entered(1));
    manager->set_merge_delay_ms(60000);
    auto queued_reader = std::make_shared<ControlledFileReader>(content);
    const auto queued_hash = BlockFileCache::hash("partial_block_shutdown_delayed");
    EXPECT_EQ(manager->try_submit(make_request(cache_writer, index, queued_reader, queued_hash,
                                               content, 0, 1024)),
              PartialBlockSubmitResult::QUEUED);
    auto shutdown = std::async(std::launch::async, [&]() { manager->shutdown(); });
    EXPECT_EQ(shutdown.wait_for(50ms), std::future_status::timeout);
    reader->release_reads();
    EXPECT_EQ(shutdown.wait_for(5s), std::future_status::ready);
    EXPECT_FALSE(manager->accepting());
    EXPECT_EQ(manager->pending_count(), 0);
    EXPECT_EQ(queued_reader->read_calls(), 0);
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
    options.remote_read_thread_count = 0;
    EXPECT_FALSE(options.validate().ok());
    options.remote_read_thread_count = -1;
    EXPECT_FALSE(options.validate().ok());
    options = partial_writeback_options();
    options.merge_delay_ms = -1;
    EXPECT_FALSE(options.validate().ok());
    options.merge_delay_ms = 0;
    EXPECT_TRUE(options.validate().ok());
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
            .remote_read_thread_count = 64,
            .max_pending_bytes = 256_mb,
            .hole_fill_coalesce =
                    {
                            .max_gap_bytes = 32_kb,
                            .max_range_bytes = 1_mb,
                            .max_read_amplification_ratio = 2.0,
                    },
    };
    EXPECT_EQ(options.merge_delay_ms, 10);
    EXPECT_TRUE(options.validate().ok());
}

TEST(PartialBlockWritebackOptionsTest, WorkerConfigIsMutableAndBounded) {
    const int32_t old_worker_count = config::hole_fill_workers_per_be;
    const int32_t old_merge_delay = config::hole_fill_merge_delay_ms;
    Defer restore_worker_count {[&]() {
        EXPECT_TRUE(config::set_config("hole_fill_workers_per_be", std::to_string(old_worker_count))
                            .ok());
        EXPECT_TRUE(config::set_config("hole_fill_merge_delay_ms", std::to_string(old_merge_delay))
                            .ok());
    }};

    EXPECT_FALSE(config::set_config("hole_fill_workers_per_be", "0").ok());
    EXPECT_FALSE(config::set_config("hole_fill_workers_per_be", "129").ok());
    EXPECT_EQ(config::hole_fill_workers_per_be, old_worker_count);
    const int32_t new_worker_count = old_worker_count == 1 ? 2 : 1;
    EXPECT_TRUE(
            config::set_config("hole_fill_workers_per_be", std::to_string(new_worker_count)).ok());
    EXPECT_EQ(config::hole_fill_workers_per_be, new_worker_count);
    EXPECT_FALSE(config::set_config("hole_fill_merge_delay_ms", "-1").ok());
    EXPECT_EQ(config::hole_fill_merge_delay_ms, old_merge_delay);
    EXPECT_TRUE(config::set_config("hole_fill_merge_delay_ms", "0").ok());
    EXPECT_EQ(config::hole_fill_merge_delay_ms, 0);
    EXPECT_TRUE(config::set_config("hole_fill_merge_delay_ms", "20").ok());
    EXPECT_EQ(config::hole_fill_merge_delay_ms, 20);
}

TEST(PartialBlockWritebackOptionsTest, RemoteReadThreadConfigIsMutableAndPositive) {
    const int32_t old_count = config::hole_fill_remote_read_threads_per_be;
    Defer restore {[&]() {
        EXPECT_TRUE(config::set_config("hole_fill_remote_read_threads_per_be",
                                       std::to_string(old_count))
                            .ok());
    }};
    EXPECT_FALSE(config::set_config("hole_fill_remote_read_threads_per_be", "0").ok());
    EXPECT_FALSE(config::set_config("hole_fill_remote_read_threads_per_be", "-1").ok());
    EXPECT_EQ(config::hole_fill_remote_read_threads_per_be, old_count);
    const int32_t new_count = old_count == 1 ? 2 : 1;
    EXPECT_TRUE(
            config::set_config("hole_fill_remote_read_threads_per_be", std::to_string(new_count))
                    .ok());
    EXPECT_EQ(config::hole_fill_remote_read_threads_per_be, new_count);
}

} // namespace
} // namespace doris::io
