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

} // namespace

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
    config::enable_cache_read_from_peer = false;
    Defer restore_config {[&]() {
        config::enable_async_file_cache_write = old_enable_async;
        config::enable_cache_read_from_peer = old_enable_peer;
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
    context.cache_write_mode_override.reset();
    context.is_dryrun = true;
    EXPECT_EQ(reader->_resolve_cache_write_mode(&context), CacheWriteMode::SYNC_WRITE);
    context.is_dryrun = false;
    context.is_warmup = true;
    EXPECT_EQ(reader->_resolve_cache_write_mode(&context), CacheWriteMode::SYNC_WRITE);

    context.is_warmup = false;
    opts.cache_write_mode = CacheWriteMode::SYNC_WRITE;
    auto sync_reader = std::make_shared<CachedRemoteFileReader>(local_reader, opts);
    EXPECT_EQ(sync_reader->_resolve_cache_write_mode(&context), CacheWriteMode::SYNC_WRITE);
    context.cache_write_mode_override = CacheWriteMode::ASYNC_WRITE;
    EXPECT_EQ(sync_reader->_resolve_cache_write_mode(&context), CacheWriteMode::ASYNC_WRITE);
}

} // namespace doris::io
