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

#include <future>

#include "block_file_cache_test_common.h"
#include "cloud/config.h"

namespace doris::io {
namespace {

FileCacheSettings async_reader_cache_settings() {
    FileCacheSettings settings;
    settings.query_queue_size = 8_mb;
    settings.query_queue_elements = 16;
    settings.index_queue_size = 2_mb;
    settings.index_queue_elements = 4;
    settings.disposable_queue_size = 2_mb;
    settings.disposable_queue_elements = 4;
    settings.capacity = 16_mb;
    settings.max_file_block_size = 1_mb;
    settings.max_query_cache_size = 0;
    return settings;
}

void reset_async_reader_cache_factory() {
    FileCacheFactory::instance()->_caches.clear();
    FileCacheFactory::instance()->_path_to_cache.clear();
    FileCacheFactory::instance()->_capacity = 0;
}

class CountingFileReader final : public FileReader {
public:
    explicit CountingFileReader(FileReaderSPtr delegate) : _delegate(std::move(delegate)) {}

    Status close() override { return _delegate->close(); }
    const Path& path() const override { return _delegate->path(); }
    size_t size() const override { return _delegate->size(); }
    bool closed() const override { return _delegate->closed(); }
    int64_t mtime() const override { return _delegate->mtime(); }

    size_t read_count() const { return _read_count; }
    size_t last_offset() const { return _last_offset; }
    size_t last_size() const { return _last_size; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override {
        ++_read_count;
        _last_offset = offset;
        _last_size = result.size;
        return _delegate->read_at(offset, result, bytes_read, io_ctx);
    }

private:
    FileReaderSPtr _delegate;
    size_t _read_count {0};
    size_t _last_offset {0};
    size_t _last_size {0};
};

// Provides one isolated cache disk with phase-one async-read settings for concise end-to-end
// reader tests. Every test gets a fresh FileCacheFactory entry and restores global knobs afterward.
class AsyncCachedRemoteFileReaderTest : public BlockFileCacheTest {
protected:
    void SetUp() override {
        _old_enable_async = config::enable_async_file_cache_write;
        _old_enable_inflight = config::enable_inflight_write_buffer_index;
        _old_enable_direct = config::enable_read_cache_file_directly;
        _old_enable_peer = config::enable_cache_read_from_peer;
        _old_block_size = config::file_cache_each_block_size;
        _old_wait_timeout_ms = config::block_cache_wait_timeout_ms;

        config::enable_async_file_cache_write = true;
        config::enable_inflight_write_buffer_index = true;
        config::enable_read_cache_file_directly = false;
        config::enable_cache_read_from_peer = false;
        config::file_cache_each_block_size = 1_mb;
        reset_async_reader_cache_factory();
    }

    void TearDown() override {
        reset_async_reader_cache_factory();
        if (!_cache_path.empty()) {
            std::error_code error;
            fs::remove_all(_cache_path, error);
        }
        config::enable_async_file_cache_write = _old_enable_async;
        config::enable_inflight_write_buffer_index = _old_enable_inflight;
        config::enable_read_cache_file_directly = _old_enable_direct;
        config::enable_cache_read_from_peer = _old_enable_peer;
        config::file_cache_each_block_size = _old_block_size;
        config::block_cache_wait_timeout_ms = _old_wait_timeout_ms;
    }

    // Create and initialize the single cache disk used by the current test.
    void create_cache(std::string_view name) {
        _cache_path = caches_dir / name;
        std::error_code error;
        fs::remove_all(_cache_path, error);
        fs::create_directories(_cache_path);
        ASSERT_TRUE(FileCacheFactory::instance()
                            ->create_file_cache(_cache_path.string(), async_reader_cache_settings())
                            .ok());
        _cache = FileCacheFactory::instance()->_path_to_cache[_cache_path.string()];
        ASSERT_NE(_cache, nullptr);
        wait_until_cache_ready(*_cache);
    }

    // Open the deterministic 10MB fixture file used as the remote source in cache tests.
    FileReaderSPtr open_remote_file() {
        FileReaderSPtr reader;
        EXPECT_TRUE(global_local_filesystem()->open_file(tmp_file, &reader).ok());
        return reader;
    }

    // Wrap a supplied remote source with the normal file-block-cache reader options.
    std::shared_ptr<CachedRemoteFileReader> create_reader(FileReaderSPtr remote_reader) {
        FileReaderOptions options;
        options.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
        options.is_doris_table = true;
        options.tablet_id = 10086;
        return std::make_shared<CachedRemoteFileReader>(std::move(remote_reader), options);
    }

    // Wait until accepted writes and their inflight payload entries have both been finalized.
    void wait_for_async_writes() {
        ASSERT_NE(_cache, nullptr);
        for (int attempt = 0;
             attempt < 5000 && (_cache->async_write_service()->pending_count() != 0 ||
                                _cache->inflight_write_buffer_index()->size() != 0);
             ++attempt) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        EXPECT_EQ(_cache->async_write_service()->pending_count(), 0);
        EXPECT_EQ(_cache->inflight_write_buffer_index()->size(), 0);
    }

    BlockFileCache* cache() const { return _cache; }

private:
    fs::path _cache_path;
    BlockFileCache* _cache = nullptr;
    bool _old_enable_async = false;
    bool _old_enable_inflight = false;
    bool _old_enable_direct = false;
    bool _old_enable_peer = false;
    int64_t _old_block_size = 0;
    int64_t _old_wait_timeout_ms = 0;
};

} // namespace

TEST_F(AsyncCachedRemoteFileReaderTest,
       missing_cache_file_falls_back_and_removes_the_complete_cache_hash) {
    create_cache("cached_remote_reader_async_self_heal");
    auto counting_reader = std::make_shared<CountingFileReader>(open_remote_file());
    auto reader = create_reader(counting_reader);

    std::string initial_result(2_mb, '\0');
    FileCacheStatistics initial_stats;
    IOContext initial_context;
    initial_context.file_cache_stats = &initial_stats;
    size_t bytes_read = 0;
    ASSERT_TRUE(reader->read_at(0, Slice(initial_result.data(), initial_result.size()), &bytes_read,
                                &initial_context)
                        .ok());
    ASSERT_EQ(bytes_read, initial_result.size());
    wait_for_async_writes();

    std::vector<fs::path> cache_files;
    {
        ReadStatistics probe_stats;
        CacheContext probe_context;
        probe_context.stats = &probe_stats;
        auto probe_result = cache()->probe(reader->_cache_hash, 0, 2_mb, probe_context);
        ASSERT_EQ(probe_result.file_blocks.size(), 2);
        for (const auto& block : probe_result.file_blocks) {
            ASSERT_NE(block, nullptr);
            ASSERT_EQ(block->state(), FileBlock::State::DOWNLOADED);
            cache_files.emplace_back(block->get_cache_file());
        }
    }
    ASSERT_EQ(cache_files.size(), 2);
    ASSERT_NE(cache_files[0], cache_files[1]);
    std::error_code remove_error;
    ASSERT_TRUE(fs::remove(cache_files[0], remove_error));
    ASSERT_FALSE(remove_error);

    const uint64_t epoch_before_self_heal = cache()->async_write_service()->current_write_epoch();
    std::string fallback_result(4096, '\0');
    FileCacheStatistics fallback_stats;
    IOContext fallback_context;
    fallback_context.file_cache_stats = &fallback_stats;
    bytes_read = 0;
    ASSERT_TRUE(reader->read_at(0, Slice(fallback_result.data(), fallback_result.size()),
                                &bytes_read, &fallback_context)
                        .ok());
    EXPECT_EQ(bytes_read, fallback_result.size());
    EXPECT_EQ(fallback_result, std::string(fallback_result.size(), '0'));
    EXPECT_EQ(counting_reader->read_count(), 2);
    EXPECT_EQ(fallback_stats.bytes_read_from_remote, fallback_result.size());
    EXPECT_EQ(fallback_stats.async_cache_write_submitted, 0);
    EXPECT_EQ(cache()->async_write_service()->current_write_epoch(), epoch_before_self_heal + 1);

    bool hash_removed = false;
    for (int attempt = 0; attempt < 5000; ++attempt) {
        ReadStatistics probe_stats;
        CacheContext probe_context;
        probe_context.stats = &probe_stats;
        bool metadata_removed = false;
        {
            auto probe_result = cache()->probe(reader->_cache_hash, 0, 2_mb, probe_context);
            metadata_removed =
                    probe_result.file_blocks.size() == 2 &&
                    std::all_of(probe_result.file_blocks.begin(), probe_result.file_blocks.end(),
                                [](const auto& block) { return block == nullptr; });
        }
        const bool files_removed =
                std::none_of(cache_files.begin(), cache_files.end(),
                             [](const fs::path& path) { return fs::exists(path); });
        if (metadata_removed && files_removed) {
            hash_removed = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_TRUE(hash_removed);
}

TEST_F(AsyncCachedRemoteFileReaderTest,
       downloading_cache_block_timeout_falls_back_without_duplicate_submission) {
    create_cache("cached_remote_reader_async_wait_timeout");
    config::block_cache_wait_timeout_ms = 10;
    auto counting_reader = std::make_shared<CountingFileReader>(open_remote_file());
    auto reader = create_reader(counting_reader);

    std::mutex mutex;
    std::condition_variable cv;
    bool downloader_ready = false;
    bool release_downloader = false;
    std::thread downloader([&]() {
        ReadStatistics cache_stats;
        CacheContext cache_context;
        cache_context.stats = &cache_stats;
        auto holder = cache()->get_or_set(reader->_cache_hash, 0, 1_mb, cache_context);
        DORIS_CHECK(holder.file_blocks.size() == 1);
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
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&]() { return downloader_ready; }));
    }

    std::string result(4096, '\0');
    FileCacheStatistics stats;
    IOContext context;
    context.file_cache_stats = &stats;
    size_t bytes_read = 0;
    ASSERT_TRUE(
            reader->read_at(0, Slice(result.data(), result.size()), &bytes_read, &context).ok());
    EXPECT_EQ(result, std::string(result.size(), '0'));
    EXPECT_EQ(bytes_read, result.size());
    EXPECT_EQ(counting_reader->read_count(), 1);
    EXPECT_EQ(counting_reader->last_offset(), 0);
    EXPECT_EQ(counting_reader->last_size(), 1_mb);
    EXPECT_EQ(stats.probe_downloading_hit, 1);
    EXPECT_EQ(stats.block_wait_success, 0);
    EXPECT_EQ(stats.block_wait_timeout, 1);
    EXPECT_EQ(stats.bytes_read_from_remote, result.size());
    EXPECT_EQ(stats.async_cache_write_submitted, 0);
}

TEST_F(AsyncCachedRemoteFileReaderTest,
       direct_cache_prefix_is_preserved_while_async_path_fills_the_unread_suffix) {
    create_cache("cached_remote_reader_async_direct_prefix");
    auto counting_reader = std::make_shared<CountingFileReader>(open_remote_file());
    auto reader = create_reader(counting_reader);

    ReadStatistics cache_stats;
    CacheContext cache_context;
    cache_context.stats = &cache_stats;
    auto holder = cache()->get_or_set(reader->_cache_hash, 0, 1_mb, cache_context);
    ASSERT_EQ(holder.file_blocks.size(), 1);
    const auto& prefix_block = holder.file_blocks.front();
    ASSERT_EQ(prefix_block->get_or_set_downloader(), FileBlock::get_caller_id());
    const std::string cached_prefix(1_mb, 'x');
    ASSERT_TRUE(prefix_block->append(Slice(cached_prefix.data(), cached_prefix.size())).ok());
    ASSERT_TRUE(prefix_block->finalize().ok());
    config::enable_read_cache_file_directly = true;
    reader->_insert_file_reader(prefix_block);

    std::string result(1_mb + 4096, '\0');
    FileCacheStatistics stats;
    IOContext context;
    context.file_cache_stats = &stats;
    size_t bytes_read = 0;
    ASSERT_TRUE(
            reader->read_at(0, Slice(result.data(), result.size()), &bytes_read, &context).ok());
    EXPECT_EQ(bytes_read, result.size());
    EXPECT_EQ(result.substr(0, 1_mb), cached_prefix);
    EXPECT_EQ(result.substr(1_mb), std::string(4096, '1'));
    EXPECT_EQ(counting_reader->read_count(), 1);
    EXPECT_EQ(counting_reader->last_offset(), 1_mb);
    EXPECT_EQ(counting_reader->last_size(), 1_mb);
    EXPECT_EQ(stats.bytes_read_from_local, 1_mb);
    EXPECT_EQ(stats.bytes_read_from_remote, 4096);
    EXPECT_EQ(stats.async_cache_write_submitted, 1);
    wait_for_async_writes();
}

TEST_F(AsyncCachedRemoteFileReaderTest, concurrent_cold_reads_publish_only_one_async_write_task) {
    create_cache("cached_remote_reader_async_concurrent_dedup");
    auto first_counting_reader = std::make_shared<CountingFileReader>(open_remote_file());
    auto second_counting_reader = std::make_shared<CountingFileReader>(open_remote_file());
    auto first_reader = create_reader(first_counting_reader);
    auto second_reader = create_reader(second_counting_reader);

    std::mutex insert_mutex;
    std::condition_variable insert_cv;
    size_t insert_arrivals = 0;
    bool release_inserters = false;
    std::mutex worker_mutex;
    std::condition_variable worker_cv;
    bool worker_entered = false;
    bool release_worker = false;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard insert_guard;
    SyncPoint::CallbackGuard worker_guard;
    sync_point->set_call_back(
            "CachedRemoteFileReader::_submit_async_write_tasks:before_inflight_insert",
            [&](auto&&) {
                std::unique_lock lock(insert_mutex);
                ++insert_arrivals;
                if (insert_arrivals == 2) {
                    release_inserters = true;
                    insert_cv.notify_all();
                }
                insert_cv.wait(lock, [&]() { return release_inserters; });
            },
            &insert_guard);
    sync_point->set_call_back(
            "AsyncCacheWriteService::_write_one:before_get_or_set",
            [&](auto&&) {
                std::unique_lock lock(worker_mutex);
                worker_entered = true;
                worker_cv.notify_all();
                worker_cv.wait(lock, [&]() { return release_worker; });
            },
            &worker_guard);
    sync_point->enable_processing();
    Defer clear_sync_point {[&]() {
        {
            std::lock_guard lock(insert_mutex);
            release_inserters = true;
        }
        insert_cv.notify_all();
        {
            std::lock_guard lock(worker_mutex);
            release_worker = true;
        }
        worker_cv.notify_all();
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    }};

    std::string first_result(4096, '\0');
    std::string second_result(4096, '\0');
    FileCacheStatistics first_stats;
    FileCacheStatistics second_stats;
    IOContext first_context;
    IOContext second_context;
    first_context.file_cache_stats = &first_stats;
    second_context.file_cache_stats = &second_stats;
    size_t first_bytes_read = 0;
    size_t second_bytes_read = 0;
    auto first_future = std::async(std::launch::async, [&]() {
        SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->orphan_mem_tracker());
        return first_reader->read_at(0, Slice(first_result.data(), first_result.size()),
                                     &first_bytes_read, &first_context);
    });
    auto second_future = std::async(std::launch::async, [&]() {
        SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->orphan_mem_tracker());
        return second_reader->read_at(0, Slice(second_result.data(), second_result.size()),
                                      &second_bytes_read, &second_context);
    });
    const auto first_status = first_future.wait_for(std::chrono::seconds(5));
    const auto second_status = second_future.wait_for(std::chrono::seconds(5));
    if (first_status != std::future_status::ready || second_status != std::future_status::ready) {
        {
            std::lock_guard lock(insert_mutex);
            release_inserters = true;
        }
        insert_cv.notify_all();
        {
            std::lock_guard lock(worker_mutex);
            release_worker = true;
        }
        worker_cv.notify_all();
    }
    ASSERT_EQ(first_status, std::future_status::ready);
    ASSERT_EQ(second_status, std::future_status::ready);
    ASSERT_TRUE(first_future.get().ok());
    ASSERT_TRUE(second_future.get().ok());
    EXPECT_EQ(first_result, std::string(first_result.size(), '0'));
    EXPECT_EQ(second_result, std::string(second_result.size(), '0'));
    EXPECT_EQ(first_counting_reader->read_count(), 1);
    EXPECT_EQ(second_counting_reader->read_count(), 1);
    EXPECT_EQ(first_stats.async_cache_write_submitted + second_stats.async_cache_write_submitted,
              1);
    EXPECT_EQ(cache()->async_write_service()->pending_count(), 1);
    EXPECT_EQ(cache()->inflight_write_buffer_index()->size(), 1);
    {
        std::unique_lock lock(worker_mutex);
        ASSERT_TRUE(worker_cv.wait_for(lock, std::chrono::seconds(5),
                                       [&]() { return worker_entered; }));
        release_worker = true;
    }
    worker_cv.notify_all();
    wait_for_async_writes();
}

TEST_F(AsyncCachedRemoteFileReaderTest,
       async_buffer_allocation_failure_returns_remote_data_without_publishing_state) {
    create_cache("cached_remote_reader_async_allocation_failure");
    auto counting_reader = std::make_shared<CountingFileReader>(open_remote_file());
    auto reader = create_reader(counting_reader);

    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "AsyncCacheWriteService::allocate_tracked_buffer:inject_failure",
            [&](auto&& values) {
                auto* status = try_any_cast<Status*>(values.back());
                *status = Status::MemoryAllocFailed("injected async write buffer failure");
            },
            &guard);
    sync_point->enable_processing();
    Defer clear_sync_point {[&]() {
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    }};

    std::string result(4096, '\0');
    FileCacheStatistics stats;
    IOContext context;
    context.file_cache_stats = &stats;
    size_t bytes_read = 0;
    ASSERT_TRUE(
            reader->read_at(0, Slice(result.data(), result.size()), &bytes_read, &context).ok());
    EXPECT_EQ(result, std::string(result.size(), '0'));
    EXPECT_EQ(bytes_read, result.size());
    EXPECT_EQ(counting_reader->read_count(), 1);
    EXPECT_EQ(stats.async_cache_write_buffer_alloc_fail, 1);
    EXPECT_EQ(stats.async_cache_write_submitted, 0);
    EXPECT_EQ(cache()->async_write_service()->pending_count(), 0);
    EXPECT_EQ(cache()->inflight_write_buffer_index()->size(), 0);
    EXPECT_GE(cache()->async_write_service()->stats().buffer_alloc_fail, 1);
}

TEST_F(AsyncCachedRemoteFileReaderTest,
       external_table_reader_uses_async_write_and_reuses_downloaded_cache) {
    create_cache("cached_remote_reader_async_external_table");
    auto first_remote = std::make_shared<CountingFileReader>(open_remote_file());
    FileReaderOptions options;
    options.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    options.cache_base_path = cache()->get_base_path();
    options.mtime = 123;
    auto first_reader = std::make_shared<CachedRemoteFileReader>(first_remote, options);

    std::string first_result(4096, '\0');
    FileCacheStatistics first_stats;
    IOContext first_context;
    first_context.file_cache_stats = &first_stats;
    size_t bytes_read = 0;
    ASSERT_TRUE(first_reader
                        ->read_at(0, Slice(first_result.data(), first_result.size()), &bytes_read,
                                  &first_context)
                        .ok());
    EXPECT_EQ(first_result, std::string(first_result.size(), '0'));
    EXPECT_EQ(first_remote->read_count(), 1);
    EXPECT_EQ(first_stats.async_cache_write_submitted, 1);
    wait_for_async_writes();

    auto second_remote = std::make_shared<CountingFileReader>(open_remote_file());
    auto second_reader = std::make_shared<CachedRemoteFileReader>(second_remote, options);
    std::string second_result(4096, '\0');
    FileCacheStatistics second_stats;
    IOContext second_context;
    second_context.file_cache_stats = &second_stats;
    bytes_read = 0;
    ASSERT_TRUE(second_reader
                        ->read_at(4096, Slice(second_result.data(), second_result.size()),
                                  &bytes_read, &second_context)
                        .ok());
    EXPECT_EQ(second_result, std::string(second_result.size(), '0'));
    EXPECT_EQ(second_remote->read_count(), 0);
    EXPECT_EQ(second_stats.probe_downloaded_hit, 1);
    EXPECT_EQ(second_stats.async_cache_write_submitted, 0);
}

TEST_F(BlockFileCacheTest,
       direct_partial_hit_with_downloaded_remainder_should_not_read_remote_again) {
    std::string local_cache_base_path =
            caches_dir / "cache_direct_partial_downloaded_no_remote_read" / "";
    config::enable_read_cache_file_directly = true;
    if (fs::exists(local_cache_base_path)) {
        fs::remove_all(local_cache_base_path);
    }
    fs::create_directories(local_cache_base_path);

    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 1;

    io::FileCacheSettings settings;
    settings.query_queue_size = 6291456;
    settings.query_queue_elements = 6;
    settings.index_queue_size = 1048576;
    settings.index_queue_elements = 1;
    settings.disposable_queue_size = 1048576;
    settings.disposable_queue_elements = 1;
    settings.capacity = 8388608;
    settings.max_file_block_size = 1048576;
    settings.max_query_cache_size = 0;

    io::CacheContext context;
    ReadStatistics rstats;
    context.stats = &rstats;
    context.query_id = query_id;
    ASSERT_TRUE(
            FileCacheFactory::instance()->create_file_cache(local_cache_base_path, settings).ok());

    io::FileReaderOptions opts;
    opts.cache_type = io::cache_type_from_string("file_block_cache");
    opts.is_doris_table = true;
    opts.tablet_id = 10086;

    {
        FileReaderSPtr local_reader;
        ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &local_reader));
        auto seed_reader = std::make_shared<CachedRemoteFileReader>(local_reader, opts);
        std::string buffer(64_kb, '\0');
        IOContext io_ctx;
        FileCacheStatistics stats;
        io_ctx.file_cache_stats = &stats;
        size_t bytes_read {0};
        ASSERT_TRUE(
                seed_reader->read_at(100, Slice(buffer.data(), buffer.size()), &bytes_read, &io_ctx)
                        .ok());
        EXPECT_EQ(bytes_read, 64_kb);
        EXPECT_EQ(std::string(64_kb, '0'), buffer);
    }

    FileReaderSPtr stale_local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &stale_local_reader));
    auto stale_reader = std::make_shared<CachedRemoteFileReader>(stale_local_reader, opts);
    EXPECT_EQ(stale_reader->_cache_file_readers.size(), 1);

    {
        FileReaderSPtr local_reader;
        ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &local_reader));
        auto updater_reader = std::make_shared<CachedRemoteFileReader>(local_reader, opts);
        std::string buffer(64_kb, '\0');
        IOContext io_ctx;
        FileCacheStatistics stats;
        io_ctx.file_cache_stats = &stats;
        size_t bytes_read {0};
        ASSERT_TRUE(updater_reader
                            ->read_at(1_mb + 100, Slice(buffer.data(), buffer.size()), &bytes_read,
                                      &io_ctx)
                            .ok());
        EXPECT_EQ(bytes_read, 64_kb);
        EXPECT_EQ(std::string(64_kb, '1'), buffer);
    }

    EXPECT_EQ(stale_reader->_cache_file_readers.size(), 1);

    std::string cross_block_buffer(64_kb, '\0');
    IOContext io_ctx;
    FileCacheStatistics stats;
    io_ctx.file_cache_stats = &stats;
    size_t bytes_read {0};
    ASSERT_TRUE(stale_reader
                        ->read_at(1_mb - 100,
                                  Slice(cross_block_buffer.data(), cross_block_buffer.size()),
                                  &bytes_read, &io_ctx)
                        .ok());
    EXPECT_EQ(bytes_read, 64_kb);
    EXPECT_EQ(std::string(100, '0') + std::string(64_kb - 100, '1'), cross_block_buffer);
    EXPECT_EQ(stats.bytes_read_from_remote, 0);

    EXPECT_TRUE(stale_reader->close().ok());
    EXPECT_TRUE(stale_reader->closed());
    std::this_thread::sleep_for(std::chrono::seconds(1));
    if (fs::exists(local_cache_base_path)) {
        fs::remove_all(local_cache_base_path);
    }
    FileCacheFactory::instance()->_caches.clear();
    FileCacheFactory::instance()->_path_to_cache.clear();
    FileCacheFactory::instance()->_capacity = 0;
    config::enable_read_cache_file_directly = false;
}

TEST_F(BlockFileCacheTest, async_write_reuses_inflight_buffer_then_reads_downloaded_block) {
    const bool old_enable_async = config::enable_async_file_cache_write;
    const bool old_enable_inflight = config::enable_inflight_write_buffer_index;
    const bool old_enable_direct = config::enable_read_cache_file_directly;
    const bool old_enable_peer = config::enable_cache_read_from_peer;
    config::enable_async_file_cache_write = true;
    config::enable_inflight_write_buffer_index = true;
    config::enable_read_cache_file_directly = false;
    config::enable_cache_read_from_peer = false;
    Defer restore_config {[&]() {
        config::enable_async_file_cache_write = old_enable_async;
        config::enable_inflight_write_buffer_index = old_enable_inflight;
        config::enable_read_cache_file_directly = old_enable_direct;
        config::enable_cache_read_from_peer = old_enable_peer;
    }};

    reset_async_reader_cache_factory();
    const auto cache_path = caches_dir / "cached_remote_reader_async_write";
    std::error_code error;
    fs::remove_all(cache_path, error);
    fs::create_directories(cache_path);
    Defer cleanup_cache {[&]() {
        reset_async_reader_cache_factory();
        std::error_code cleanup_error;
        fs::remove_all(cache_path, cleanup_error);
    }};
    ASSERT_TRUE(FileCacheFactory::instance()
                        ->create_file_cache(cache_path.string(), async_reader_cache_settings())
                        .ok());
    auto* cache = FileCacheFactory::instance()->_path_to_cache[cache_path.string()];
    ASSERT_NE(cache, nullptr);
    wait_until_cache_ready(*cache);

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

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &local_reader).ok());
    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.tablet_id = 10086;
    auto reader = std::make_shared<CachedRemoteFileReader>(local_reader, opts);

    std::string first_page(4096, '\0');
    FileCacheStatistics first_stats;
    IOContext first_ctx;
    first_ctx.file_cache_stats = &first_stats;
    size_t bytes_read = 0;
    ASSERT_TRUE(
            reader->read_at(0, Slice(first_page.data(), first_page.size()), &bytes_read, &first_ctx)
                    .ok());
    EXPECT_EQ(bytes_read, first_page.size());
    EXPECT_EQ(first_page, std::string(first_page.size(), '0'));
    EXPECT_EQ(first_stats.async_cache_write_submitted, 1);
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&]() { return worker_entered; }));
    }

    std::string second_page(4096, '\0');
    FileCacheStatistics second_stats;
    IOContext second_ctx;
    second_ctx.file_cache_stats = &second_stats;
    bytes_read = 0;
    std::future<Status> second_read;
    {
        // Full inflight coverage must complete while the BlockFileCache mutex is unavailable,
        // proving that this fast path does not call BlockFileCache::probe.
        std::lock_guard cache_lock(cache->_mutex);
        second_read = std::async(std::launch::async, [&]() {
            SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->orphan_mem_tracker());
            return reader->read_at(4096, Slice(second_page.data(), second_page.size()), &bytes_read,
                                   &second_ctx);
        });
        EXPECT_EQ(second_read.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    }
    ASSERT_TRUE(second_read.get().ok());
    EXPECT_EQ(bytes_read, second_page.size());
    EXPECT_EQ(second_page, std::string(second_page.size(), '0'));
    EXPECT_EQ(second_stats.inflight_write_buffer_index_hit, 1);
    EXPECT_EQ(second_stats.bytes_read_from_remote, 0);
    EXPECT_EQ(second_stats.async_cache_write_submitted, 0);

    {
        std::lock_guard lock(mutex);
        release_worker = true;
    }
    cv.notify_all();
    for (int attempt = 0; attempt < 500 && cache->async_write_service()->pending_count() != 0;
         ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(cache->async_write_service()->pending_count(), 0);
    for (int attempt = 0; attempt < 500 && cache->inflight_write_buffer_index()->size() != 0;
         ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(cache->inflight_write_buffer_index()->size(), 0);

    std::string third_page(4096, '\0');
    FileCacheStatistics third_stats;
    IOContext third_ctx;
    third_ctx.file_cache_stats = &third_stats;
    bytes_read = 0;
    ASSERT_TRUE(reader->read_at(8192, Slice(third_page.data(), third_page.size()), &bytes_read,
                                &third_ctx)
                        .ok());
    EXPECT_EQ(bytes_read, third_page.size());
    EXPECT_EQ(third_page, std::string(third_page.size(), '0'));
    EXPECT_EQ(third_stats.probe_downloaded_hit, 1);
    EXPECT_EQ(third_stats.bytes_read_from_remote, 0);
}

TEST_F(BlockFileCacheTest, async_write_waits_for_downloading_block_outside_remote_span) {
    const bool old_enable_async = config::enable_async_file_cache_write;
    const bool old_enable_inflight = config::enable_inflight_write_buffer_index;
    const bool old_enable_direct = config::enable_read_cache_file_directly;
    const bool old_enable_peer = config::enable_cache_read_from_peer;
    const int64_t old_block_size = config::file_cache_each_block_size;
    config::enable_async_file_cache_write = true;
    config::enable_inflight_write_buffer_index = true;
    config::enable_read_cache_file_directly = false;
    config::enable_cache_read_from_peer = false;
    config::file_cache_each_block_size = 1_mb;
    Defer restore_config {[&]() {
        config::enable_async_file_cache_write = old_enable_async;
        config::enable_inflight_write_buffer_index = old_enable_inflight;
        config::enable_read_cache_file_directly = old_enable_direct;
        config::enable_cache_read_from_peer = old_enable_peer;
        config::file_cache_each_block_size = old_block_size;
    }};

    reset_async_reader_cache_factory();
    const auto cache_path = caches_dir / "cached_remote_reader_wait_downloading";
    std::error_code error;
    fs::remove_all(cache_path, error);
    fs::create_directories(cache_path);
    Defer cleanup_cache {[&]() {
        reset_async_reader_cache_factory();
        std::error_code cleanup_error;
        fs::remove_all(cache_path, cleanup_error);
    }};
    ASSERT_TRUE(FileCacheFactory::instance()
                        ->create_file_cache(cache_path.string(), async_reader_cache_settings())
                        .ok());
    auto* cache = FileCacheFactory::instance()->_path_to_cache[cache_path.string()];
    ASSERT_NE(cache, nullptr);
    wait_until_cache_ready(*cache);

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &local_reader).ok());
    auto counting_reader = std::make_shared<CountingFileReader>(local_reader);
    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.tablet_id = 10086;
    auto reader = std::make_shared<CachedRemoteFileReader>(counting_reader, opts);

    ReadStatistics cache_stats;
    CacheContext cache_context;
    cache_context.stats = &cache_stats;
    auto holder = cache->get_or_set(reader->_cache_hash, 0, 1_mb, cache_context);
    ASSERT_EQ(holder.file_blocks.size(), 1);
    const auto& downloading_block = holder.file_blocks.front();
    ASSERT_EQ(downloading_block->get_or_set_downloader(), FileBlock::get_caller_id());

    std::string result(4096, '\0');
    FileCacheStatistics read_stats;
    IOContext context;
    context.file_cache_stats = &read_stats;
    size_t bytes_read = 0;
    auto read_future = std::async(std::launch::async, [&]() {
        SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->orphan_mem_tracker());
        return reader->read_at(0, Slice(result.data(), result.size()), &bytes_read, &context);
    });
    EXPECT_EQ(read_future.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);

    const std::string payload(1_mb, '0');
    ASSERT_TRUE(downloading_block->append(Slice(payload.data(), payload.size())).ok());
    ASSERT_TRUE(downloading_block->finalize().ok());
    ASSERT_TRUE(read_future.get().ok());

    EXPECT_EQ(bytes_read, result.size());
    EXPECT_EQ(result, std::string(result.size(), '0'));
    EXPECT_EQ(counting_reader->read_count(), 0);
    EXPECT_EQ(read_stats.bytes_read_from_remote, 0);
    EXPECT_EQ(read_stats.probe_downloading_hit, 1);
    EXPECT_EQ(read_stats.block_wait_success, 1);
    EXPECT_EQ(read_stats.block_wait_timeout, 0);
    EXPECT_EQ(read_stats.async_cache_write_submitted, 0);
}

TEST_F(BlockFileCacheTest, async_write_reads_only_middle_span_between_cached_sides) {
    const bool old_enable_async = config::enable_async_file_cache_write;
    const bool old_enable_inflight = config::enable_inflight_write_buffer_index;
    const bool old_enable_direct = config::enable_read_cache_file_directly;
    const bool old_enable_peer = config::enable_cache_read_from_peer;
    const int64_t old_block_size = config::file_cache_each_block_size;
    config::enable_async_file_cache_write = true;
    config::enable_inflight_write_buffer_index = true;
    config::enable_read_cache_file_directly = false;
    config::enable_cache_read_from_peer = false;
    config::file_cache_each_block_size = 1_mb;
    Defer restore_config {[&]() {
        config::enable_async_file_cache_write = old_enable_async;
        config::enable_inflight_write_buffer_index = old_enable_inflight;
        config::enable_read_cache_file_directly = old_enable_direct;
        config::enable_cache_read_from_peer = old_enable_peer;
        config::file_cache_each_block_size = old_block_size;
    }};

    reset_async_reader_cache_factory();
    const auto cache_path = caches_dir / "cached_remote_reader_middle_span";
    std::error_code error;
    fs::remove_all(cache_path, error);
    fs::create_directories(cache_path);
    Defer cleanup_cache {[&]() {
        reset_async_reader_cache_factory();
        std::error_code cleanup_error;
        fs::remove_all(cache_path, cleanup_error);
    }};
    ASSERT_TRUE(FileCacheFactory::instance()
                        ->create_file_cache(cache_path.string(), async_reader_cache_settings())
                        .ok());
    auto* cache = FileCacheFactory::instance()->_path_to_cache[cache_path.string()];
    ASSERT_NE(cache, nullptr);
    wait_until_cache_ready(*cache);

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &local_reader).ok());
    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.tablet_id = 10086;
    auto reader = std::make_shared<CachedRemoteFileReader>(local_reader, opts);

    ReadStatistics cache_stats;
    CacheContext cache_context;
    cache_context.stats = &cache_stats;
    auto populate_block = [&](size_t offset, char value) {
        auto holder = cache->get_or_set(reader->_cache_hash, offset, 1_mb, cache_context);
        ASSERT_EQ(holder.file_blocks.size(), 1);
        const auto& block = holder.file_blocks.front();
        ASSERT_EQ(block->get_or_set_downloader(), FileBlock::get_caller_id());
        const std::string payload(1_mb, value);
        ASSERT_TRUE(block->append(Slice(payload.data(), payload.size())).ok());
        ASSERT_TRUE(block->finalize().ok());
    };
    populate_block(0, '0');
    populate_block(2_mb, '2');

    std::string result(3_mb, '\0');
    FileCacheStatistics read_stats;
    IOContext context;
    context.file_cache_stats = &read_stats;
    size_t bytes_read = 0;
    ASSERT_TRUE(
            reader->read_at(0, Slice(result.data(), result.size()), &bytes_read, &context).ok());
    EXPECT_EQ(bytes_read, result.size());
    EXPECT_EQ(result.substr(0, 1_mb), std::string(1_mb, '0'));
    EXPECT_EQ(result.substr(1_mb, 1_mb), std::string(1_mb, '1'));
    EXPECT_EQ(result.substr(2_mb, 1_mb), std::string(1_mb, '2'));
    EXPECT_EQ(read_stats.bytes_read_from_local, 2_mb);
    EXPECT_EQ(read_stats.bytes_read_from_remote, 1_mb);
    EXPECT_EQ(read_stats.probe_downloaded_hit, 2);
    EXPECT_EQ(read_stats.probe_miss, 1);
    EXPECT_EQ(read_stats.async_cache_write_submitted, 1);

    for (int attempt = 0; attempt < 5000 && (cache->async_write_service()->pending_count() != 0 ||
                                             cache->inflight_write_buffer_index()->size() != 0);
         ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(cache->async_write_service()->pending_count(), 0);
    EXPECT_EQ(cache->inflight_write_buffer_index()->size(), 0);
}

TEST_F(BlockFileCacheTest, async_write_middle_hit_uses_one_remote_span_and_submits_only_misses) {
    const bool old_enable_async = config::enable_async_file_cache_write;
    const bool old_enable_inflight = config::enable_inflight_write_buffer_index;
    const bool old_enable_direct = config::enable_read_cache_file_directly;
    const bool old_enable_peer = config::enable_cache_read_from_peer;
    const int64_t old_block_size = config::file_cache_each_block_size;
    config::enable_async_file_cache_write = true;
    config::enable_inflight_write_buffer_index = true;
    config::enable_read_cache_file_directly = false;
    config::enable_cache_read_from_peer = false;
    config::file_cache_each_block_size = 1_mb;
    Defer restore_config {[&]() {
        config::enable_async_file_cache_write = old_enable_async;
        config::enable_inflight_write_buffer_index = old_enable_inflight;
        config::enable_read_cache_file_directly = old_enable_direct;
        config::enable_cache_read_from_peer = old_enable_peer;
        config::file_cache_each_block_size = old_block_size;
    }};

    reset_async_reader_cache_factory();
    const auto cache_path = caches_dir / "cached_remote_reader_middle_hit";
    std::error_code error;
    fs::remove_all(cache_path, error);
    fs::create_directories(cache_path);
    Defer cleanup_cache {[&]() {
        reset_async_reader_cache_factory();
        std::error_code cleanup_error;
        fs::remove_all(cache_path, cleanup_error);
    }};
    ASSERT_TRUE(FileCacheFactory::instance()
                        ->create_file_cache(cache_path.string(), async_reader_cache_settings())
                        .ok());
    auto* cache = FileCacheFactory::instance()->_path_to_cache[cache_path.string()];
    ASSERT_NE(cache, nullptr);
    wait_until_cache_ready(*cache);

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &local_reader).ok());
    auto counting_reader = std::make_shared<CountingFileReader>(local_reader);
    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.tablet_id = 10086;
    auto reader = std::make_shared<CachedRemoteFileReader>(counting_reader, opts);

    ReadStatistics cache_stats;
    CacheContext cache_context;
    cache_context.stats = &cache_stats;
    auto holder = cache->get_or_set(reader->_cache_hash, 1_mb, 1_mb, cache_context);
    ASSERT_EQ(holder.file_blocks.size(), 1);
    const auto& middle_block = holder.file_blocks.front();
    ASSERT_EQ(middle_block->get_or_set_downloader(), FileBlock::get_caller_id());
    const std::string payload(1_mb, '1');
    ASSERT_TRUE(middle_block->append(Slice(payload.data(), payload.size())).ok());
    ASSERT_TRUE(middle_block->finalize().ok());
    {
        std::lock_guard cache_lock(cache->_mutex);
        cache->_files.at(reader->_cache_hash).at(1_mb).atime = 123;
    }

    std::string result(3_mb, '\0');
    FileCacheStatistics read_stats;
    IOContext context;
    context.file_cache_stats = &read_stats;
    size_t bytes_read = 0;
    ASSERT_TRUE(
            reader->read_at(0, Slice(result.data(), result.size()), &bytes_read, &context).ok());
    EXPECT_EQ(bytes_read, result.size());
    EXPECT_EQ(result.substr(0, 1_mb), std::string(1_mb, '0'));
    EXPECT_EQ(result.substr(1_mb, 1_mb), std::string(1_mb, '1'));
    EXPECT_EQ(result.substr(2_mb, 1_mb), std::string(1_mb, '2'));
    EXPECT_EQ(counting_reader->read_count(), 1);
    EXPECT_EQ(counting_reader->last_offset(), 0);
    EXPECT_EQ(counting_reader->last_size(), 3_mb);
    EXPECT_EQ(read_stats.bytes_read_from_local, 0);
    EXPECT_EQ(read_stats.bytes_read_from_remote, 3_mb);
    EXPECT_EQ(read_stats.probe_downloaded_hit, 1);
    EXPECT_EQ(read_stats.probe_miss, 2);
    EXPECT_EQ(read_stats.async_cache_write_submitted, 2);
    {
        std::lock_guard cache_lock(cache->_mutex);
        EXPECT_EQ(cache->_files.at(reader->_cache_hash).at(1_mb).atime, 123);
    }

    for (int attempt = 0; attempt < 5000 && (cache->async_write_service()->pending_count() != 0 ||
                                             cache->inflight_write_buffer_index()->size() != 0);
         ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(cache->async_write_service()->pending_count(), 0);
    EXPECT_EQ(cache->inflight_write_buffer_index()->size(), 0);
}

TEST_F(BlockFileCacheTest, async_write_backpressure_rolls_back_inflight_entry) {
    const bool old_enable_async = config::enable_async_file_cache_write;
    const bool old_enable_inflight = config::enable_inflight_write_buffer_index;
    const bool old_enable_direct = config::enable_read_cache_file_directly;
    const bool old_enable_peer = config::enable_cache_read_from_peer;
    const int64_t old_block_size = config::file_cache_each_block_size;
    config::enable_async_file_cache_write = true;
    config::enable_inflight_write_buffer_index = true;
    config::enable_read_cache_file_directly = false;
    config::enable_cache_read_from_peer = false;
    config::file_cache_each_block_size = 1_mb;
    Defer restore_config {[&]() {
        config::enable_async_file_cache_write = old_enable_async;
        config::enable_inflight_write_buffer_index = old_enable_inflight;
        config::enable_read_cache_file_directly = old_enable_direct;
        config::enable_cache_read_from_peer = old_enable_peer;
        config::file_cache_each_block_size = old_block_size;
    }};

    reset_async_reader_cache_factory();
    const auto cache_path = caches_dir / "cached_remote_reader_backpressure";
    std::error_code error;
    fs::remove_all(cache_path, error);
    fs::create_directories(cache_path);
    Defer cleanup_cache {[&]() {
        reset_async_reader_cache_factory();
        std::error_code cleanup_error;
        fs::remove_all(cache_path, cleanup_error);
    }};
    ASSERT_TRUE(FileCacheFactory::instance()
                        ->create_file_cache(cache_path.string(), async_reader_cache_settings())
                        .ok());
    auto* cache = FileCacheFactory::instance()->_path_to_cache[cache_path.string()];
    ASSERT_NE(cache, nullptr);
    wait_until_cache_ready(*cache);
    auto async_write_options = cache->async_write_service()->options();
    async_write_options.max_pending_tasks = 1;
    ASSERT_TRUE(cache->async_write_service()->update_options(async_write_options).ok());

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

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &local_reader).ok());
    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.tablet_id = 10086;
    auto reader = std::make_shared<CachedRemoteFileReader>(local_reader, opts);

    std::string first_page(4096, '\0');
    FileCacheStatistics first_stats;
    IOContext first_context;
    first_context.file_cache_stats = &first_stats;
    size_t bytes_read = 0;
    ASSERT_TRUE(reader->read_at(0, Slice(first_page.data(), first_page.size()), &bytes_read,
                                &first_context)
                        .ok());
    EXPECT_EQ(first_page, std::string(first_page.size(), '0'));
    EXPECT_EQ(first_stats.async_cache_write_submitted, 1);
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&]() { return worker_entered; }));
    }

    std::string second_page(4096, '\0');
    FileCacheStatistics second_stats;
    IOContext second_context;
    second_context.file_cache_stats = &second_stats;
    bytes_read = 0;
    ASSERT_TRUE(reader->read_at(1_mb, Slice(second_page.data(), second_page.size()), &bytes_read,
                                &second_context)
                        .ok());
    EXPECT_EQ(second_page, std::string(second_page.size(), '1'));
    EXPECT_EQ(second_stats.async_cache_write_rejected, 1);
    EXPECT_EQ(second_stats.bytes_read_from_remote, 4096);
    EXPECT_EQ(cache->async_write_service()->pending_count(), 1);
    EXPECT_EQ(
            cache->inflight_write_buffer_index()->lookup(
                    reader->_cache_hash, 1_mb, cache->async_write_service()->current_write_epoch()),
            nullptr);
    EXPECT_EQ(cache->inflight_write_buffer_index()->size(), 1);

    {
        std::lock_guard lock(mutex);
        release_worker = true;
    }
    cv.notify_all();
    for (int attempt = 0; attempt < 5000 && (cache->async_write_service()->pending_count() != 0 ||
                                             cache->inflight_write_buffer_index()->size() != 0);
         ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(cache->async_write_service()->pending_count(), 0);
    EXPECT_EQ(cache->inflight_write_buffer_index()->size(), 0);
}

TEST_F(BlockFileCacheTest, cache_write_mode_is_resolved_for_each_read_context) {
    const bool old_enable_async = config::enable_async_file_cache_write;
    const bool old_enable_peer = config::enable_cache_read_from_peer;
    const std::string old_deploy_mode = config::deploy_mode;
    config::enable_cache_read_from_peer = false;
    Defer restore_config {[&]() {
        config::enable_async_file_cache_write = old_enable_async;
        config::enable_cache_read_from_peer = old_enable_peer;
        config::deploy_mode = old_deploy_mode;
    }};

    reset_async_reader_cache_factory();
    const auto cache_path = caches_dir / "cached_remote_reader_mode_resolution";
    std::error_code error;
    fs::remove_all(cache_path, error);
    fs::create_directories(cache_path);
    Defer cleanup_cache {[&]() {
        reset_async_reader_cache_factory();
        std::error_code cleanup_error;
        fs::remove_all(cache_path, cleanup_error);
    }};
    ASSERT_TRUE(FileCacheFactory::instance()
                        ->create_file_cache(cache_path.string(), async_reader_cache_settings())
                        .ok());

    FileReaderSPtr local_reader;
    ASSERT_TRUE(global_local_filesystem()->open_file(tmp_file, &local_reader).ok());
    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.tablet_id = 10086;
    auto reader = std::make_shared<CachedRemoteFileReader>(local_reader, opts);
    IOContext context;

    config::enable_async_file_cache_write = false;
    EXPECT_EQ(reader->_resolve_cache_write_mode(&context), CacheWriteMode::SYNC_WRITE);
    config::enable_async_file_cache_write = true;
    EXPECT_EQ(reader->_resolve_cache_write_mode(&context), CacheWriteMode::ASYNC_WRITE);

    context.cache_write_mode_override = CacheWriteMode::SYNC_WRITE;
    EXPECT_EQ(reader->_resolve_cache_write_mode(&context), CacheWriteMode::SYNC_WRITE);
    context.cache_write_mode_override = CacheWriteMode::ASYNC_WRITE;
    context.is_dryrun = true;
    EXPECT_EQ(reader->_resolve_cache_write_mode(&context), CacheWriteMode::SYNC_WRITE);
    context.is_dryrun = false;
    context.is_warmup = true;
    EXPECT_EQ(reader->_resolve_cache_write_mode(&context), CacheWriteMode::SYNC_WRITE);

    context.is_warmup = false;
    config::deploy_mode = "cloud";
    config::enable_cache_read_from_peer = true;
    EXPECT_EQ(reader->_resolve_cache_write_mode(&context), CacheWriteMode::SYNC_WRITE);
    context.bypass_peer_read = true;
    EXPECT_EQ(reader->_resolve_cache_write_mode(&context), CacheWriteMode::ASYNC_WRITE);

    context.bypass_peer_read = false;
    config::enable_cache_read_from_peer = false;
    context.cache_write_mode_override.reset();
    opts.cache_write_mode = CacheWriteMode::SYNC_WRITE;
    auto sync_reader = std::make_shared<CachedRemoteFileReader>(local_reader, opts);
    EXPECT_EQ(sync_reader->_resolve_cache_write_mode(&context), CacheWriteMode::SYNC_WRITE);
    context.cache_write_mode_override = CacheWriteMode::ASYNC_WRITE;
    EXPECT_EQ(sync_reader->_resolve_cache_write_mode(&context), CacheWriteMode::ASYNC_WRITE);
}

} // namespace doris::io
