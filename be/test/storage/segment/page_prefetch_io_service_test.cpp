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

#include "storage/segment/page_prefetch_io_service.h"

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "cloud/config.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache_test_common.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/cache/remote_scan_cache_write_limiter.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "storage/segment/file_cache_writeback_coordinator.h"
#include "storage/segment/page_prefetcher.h"
#include "testutil/mock/mock_query_context.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace doris::segment_v2 {
namespace {

constexpr PagePrefetchBudgetLimits kWideLimits {.max_ranges = 16, .max_bytes = 1 << 20};

struct RangeFixture {
    std::shared_ptr<PagePrefetchQueryContext> query;
    std::shared_ptr<PagePrefetchGlobalBudget> global;
    std::shared_ptr<MemTrackerLimiter> tracker;
    std::shared_ptr<PrefetchRange> range;
};

PageFetchRangeSpec make_spec(size_t size = 64) {
    return PageFetchRangeSpec {
            .offset = 100,
            .size = size,
            .requested_page_bytes = 24,
            .coalesced_gap_bytes = size - 24,
            .block_fill_bytes = 0,
            .pages = {{.page_index = 7, .page_offset = 108, .page_size = 16, .buffer_offset = 8}},
            .complete_blocks = {},
    };
}

RangeFixture make_range() {
    RangeFixture fixture;
    fixture.query = std::make_shared<PagePrefetchQueryContext>(kWideLimits);
    fixture.global = std::make_shared<PagePrefetchGlobalBudget>(kWideLimits);
    fixture.tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::CACHE,
                                                       "PagePrefetchIOServiceTest");
    PagePrefetchRejectReason reject_reason = PagePrefetchRejectReason::NONE;
    auto reservation = PagePrefetchReservation::try_reserve_range(fixture.query, fixture.global, 64,
                                                                  &reject_reason);
    DORIS_CHECK(reservation.has_value());
    DORIS_CHECK(reject_reason == PagePrefetchRejectReason::NONE);
    std::shared_ptr<PagePrefetchBuffer> buffer;
    DORIS_CHECK(
            PagePrefetchBuffer::create(64, fixture.tracker, std::move(*reservation), &buffer).ok());
    std::memset(buffer->data(), 'p', buffer->size());
    fixture.range = std::make_shared<PrefetchRange>(make_spec(), std::move(buffer));
    return fixture;
}

PageFetchRangeSpec make_service_spec(size_t offset, size_t size) {
    DORIS_CHECK(size <= std::numeric_limits<uint32_t>::max());
    return PageFetchRangeSpec {
            .offset = offset,
            .size = size,
            .requested_page_bytes = size,
            .coalesced_gap_bytes = 0,
            .block_fill_bytes = 0,
            .pages = {{.page_index = 11,
                       .page_offset = offset,
                       .page_size = static_cast<uint32_t>(size),
                       .buffer_offset = 0}},
            .complete_blocks = {},
    };
}

PageFetchRangeSpec make_complete_block_service_spec(size_t block_size) {
    DORIS_CHECK(block_size <= std::numeric_limits<uint32_t>::max());
    return PageFetchRangeSpec {
            .offset = 0,
            .size = 2 * block_size,
            .requested_page_bytes = 2 * block_size,
            .coalesced_gap_bytes = 0,
            .block_fill_bytes = 0,
            .pages = {{.page_index = 11,
                       .page_offset = 0,
                       .page_size = static_cast<uint32_t>(block_size),
                       .buffer_offset = 0},
                      {.page_index = 12,
                       .page_offset = block_size,
                       .page_size = static_cast<uint32_t>(block_size),
                       .buffer_offset = block_size}},
            .complete_blocks = {{.block_offset = 0,
                                 .valid_size = block_size,
                                 .buffer_offset = 0,
                                 .source_page_indexes = {11}},
                                {.block_offset = block_size,
                                 .valid_size = block_size,
                                 .buffer_offset = block_size,
                                 .source_page_indexes = {12}}},
    };
}

TUniqueId make_query_id(int64_t hi, int64_t lo) {
    TUniqueId query_id;
    query_id.hi = hi;
    query_id.lo = lo;
    return query_id;
}

io::FileCacheSettings service_cache_settings() {
    io::FileCacheSettings settings;
    settings.query_queue_size = 8 * 1024 * 1024;
    settings.query_queue_elements = 16;
    settings.index_queue_size = 2 * 1024 * 1024;
    settings.index_queue_elements = 4;
    settings.disposable_queue_size = 2 * 1024 * 1024;
    settings.disposable_queue_elements = 4;
    settings.capacity = 16 * 1024 * 1024;
    settings.max_file_block_size = 1024 * 1024;
    settings.max_query_cache_size = 0;
    return settings;
}

void reset_service_cache_factory() {
    io::FileCacheFactory::instance()->_caches.clear();
    io::FileCacheFactory::instance()->_path_to_cache.clear();
    io::FileCacheFactory::instance()->_capacity = 0;
}

struct ObservedIOContext {
    size_t offset = 0;
    size_t size = 0;
    size_t read_count = 0;
    const TUniqueId* query_id_pointer = nullptr;
    std::optional<TUniqueId> query_id_value;
    io::FileCacheStatistics* file_cache_stats = nullptr;
    io::FileReaderStats* file_reader_stats = nullptr;
    io::RemoteScanCacheWriteLimiter* limiter = nullptr;
    bool is_index_data = true;
    bool is_inverted_index = true;
    bool is_dryrun = true;
    bool is_warmup = true;
    bool bypass_peer_read = false;
    std::optional<io::CacheAlignMode> align_mode;
    std::optional<io::CacheWriteMode> write_mode;
    io::FileCacheMissPolicy miss_policy = io::FileCacheMissPolicy::READ_THROUGH_AND_WRITE_BACK;
};

class InspectingFileReader final : public io::FileReader {
public:
    InspectingFileReader(io::FileReaderSPtr delegate, bool block, bool fail)
            : _delegate(std::move(delegate)), _released(!block), _fail(fail) {}

    Status close() override { return _delegate->close(); }
    const io::Path& path() const override { return _delegate->path(); }
    size_t size() const override { return _delegate->size(); }
    bool closed() const override { return _delegate->closed(); }
    int64_t mtime() const override { return _delegate->mtime(); }

    bool wait_until_entered(std::chrono::milliseconds timeout) {
        std::unique_lock lock(_mutex);
        return _cv.wait_for(lock, timeout, [this]() { return _entered; });
    }

    void release() {
        {
            std::lock_guard lock(_mutex);
            _released = true;
        }
        _cv.notify_all();
    }

    ObservedIOContext observed() const {
        std::lock_guard lock(_mutex);
        return _observed;
    }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override {
        DORIS_CHECK(io_ctx != nullptr);
        {
            std::unique_lock lock(_mutex);
            _observed.offset = offset;
            _observed.size = result.size;
            ++_observed.read_count;
            _observed.query_id_pointer = io_ctx->query_id;
            if (io_ctx->query_id != nullptr) {
                _observed.query_id_value = *io_ctx->query_id;
            }
            _observed.file_cache_stats = io_ctx->file_cache_stats;
            _observed.file_reader_stats = io_ctx->file_reader_stats;
            _observed.limiter = io_ctx->remote_scan_cache_write_limiter;
            _observed.is_index_data = io_ctx->is_index_data;
            _observed.is_inverted_index = io_ctx->is_inverted_index;
            _observed.is_dryrun = io_ctx->is_dryrun;
            _observed.is_warmup = io_ctx->is_warmup;
            _observed.bypass_peer_read = io_ctx->bypass_peer_read;
            _observed.align_mode = io_ctx->cache_align_mode_override;
            _observed.write_mode = io_ctx->cache_write_mode_override;
            _observed.miss_policy = io_ctx->file_cache_miss_policy;
            _entered = true;
            _cv.notify_all();
            _cv.wait(lock, [this]() { return _released; });
        }
        if (_fail) {
            *bytes_read = 0;
            return Status::IOError("injected page prefetch read failure");
        }
        return _delegate->read_at(offset, result, bytes_read, io_ctx);
    }

private:
    const io::FileReaderSPtr _delegate;
    mutable std::mutex _mutex;
    std::condition_variable _cv;
    ObservedIOContext _observed;
    bool _entered = false;
    bool _released = false;
    const bool _fail;
};

class PagePrefetchIOServiceTest : public io::BlockFileCacheTest {
protected:
    void SetUp() override {
        _old_enable_async = config::enable_async_file_cache_write;
        _old_enable_page_prefetch = config::enable_query_page_prefetch;
        _old_enable_inflight = config::enable_async_file_cache_write_inflight_write_buffer_index;
        _old_enable_direct = config::enable_read_cache_file_directly;
        _old_enable_peer = config::enable_cache_read_from_peer;
        _old_block_size = config::file_cache_each_block_size;

        config::enable_async_file_cache_write = true;
        config::enable_query_page_prefetch = true;
        config::enable_async_file_cache_write_inflight_write_buffer_index = true;
        config::enable_read_cache_file_directly = false;
        config::enable_cache_read_from_peer = false;
        config::file_cache_each_block_size = 1024 * 1024;
        reset_service_cache_factory();
        ExecEnv::GetInstance()->set_file_cache_open_fd_cache(std::make_unique<io::FDCache>());
    }

    void TearDown() override {
        _pool.reset();
        reset_service_cache_factory();
        if (!_cache_path.empty()) {
            std::error_code error;
            std::filesystem::remove_all(_cache_path, error);
        }
        config::enable_async_file_cache_write = _old_enable_async;
        config::enable_query_page_prefetch = _old_enable_page_prefetch;
        config::enable_async_file_cache_write_inflight_write_buffer_index = _old_enable_inflight;
        config::enable_read_cache_file_directly = _old_enable_direct;
        config::enable_cache_read_from_peer = _old_enable_peer;
        config::file_cache_each_block_size = _old_block_size;
    }

    void create_cache(std::string_view name) {
        _cache_path = io::caches_dir / name;
        std::error_code error;
        std::filesystem::remove_all(_cache_path, error);
        std::filesystem::create_directories(_cache_path);
        ASSERT_TRUE(io::FileCacheFactory::instance()
                            ->create_file_cache(_cache_path.string(), service_cache_settings())
                            .ok());
        _cache = io::FileCacheFactory::instance()->_path_to_cache[_cache_path.string()];
        ASSERT_NE(_cache, nullptr);
        io::wait_until_cache_ready(*_cache);
    }

    void create_pool(int max_queue_size) {
        ASSERT_TRUE(ThreadPoolBuilder("PagePrefetchIOServiceTestPool")
                            .set_min_threads(1)
                            .set_max_threads(1)
                            .set_max_queue_size(max_queue_size)
                            .build(&_pool)
                            .ok());
    }

    io::FileReaderSPtr open_remote_file() {
        io::FileReaderSPtr reader;
        EXPECT_TRUE(io::global_local_filesystem()->open_file(io::tmp_file, &reader).ok());
        return reader;
    }

    std::shared_ptr<io::CachedRemoteFileReader> create_reader(io::FileReaderSPtr remote_reader) {
        io::FileReaderOptions options;
        options.cache_type = io::FileCachePolicy::FILE_BLOCK_CACHE;
        options.is_doris_table = true;
        options.tablet_id = kTabletId;
        return std::make_shared<io::CachedRemoteFileReader>(std::move(remote_reader), options);
    }

    PagePrefetchSafeIOContext make_safe_context(
            const TUniqueId& query_id, io::FileCacheStatistics* file_cache_stats,
            io::FileReaderStats* file_reader_stats,
            io::RemoteScanCacheWriteLimiter* limiter = nullptr) {
        io::IOContext source;
        source.reader_type = ReaderType::READER_QUERY;
        source.query_id = &query_id;
        source.file_cache_stats = file_cache_stats;
        source.file_reader_stats = file_reader_stats;
        source.remote_scan_cache_write_limiter = limiter;
        return PagePrefetchSafeIOContext::from_query_thread(source, kTabletId);
    }

    PagePrefetchIOServiceOptions service_options() const {
        return PagePrefetchIOServiceOptions {
                .query_limits = {.max_ranges = 16, .max_bytes = 2 * 1024 * 1024},
                .global_limits = {.max_ranges = 64, .max_bytes = 8 * 1024 * 1024},
        };
    }

    static constexpr int64_t kTabletId = 10086;
    std::filesystem::path _cache_path;
    io::BlockFileCache* _cache = nullptr;
    std::unique_ptr<ThreadPool> _pool;
    bool _old_enable_async = false;
    bool _old_enable_page_prefetch = false;
    bool _old_enable_inflight = false;
    bool _old_enable_direct = false;
    bool _old_enable_peer = false;
    int64_t _old_block_size = 0;
};

TEST(PagePrefetchSafeIOContextTest, OwnsValuesAndStripsQueryThreadPointers) {
    TUniqueId query_id = make_query_id(17, 19);
    const TUniqueId captured_query_id = query_id;
    io::FileCacheStatistics query_file_cache_stats;
    io::FileReaderStats query_file_reader_stats;
    io::RemoteScanCacheWriteLimiter limiter(query_id, 0);
    io::IOContext source;
    source.reader_type = ReaderType::READER_QUERY;
    source.is_disposable = true;
    source.is_index_data = true;
    source.expiration_time = 12345;
    source.query_id = &query_id;
    source.file_cache_stats = &query_file_cache_stats;
    source.file_reader_stats = &query_file_reader_stats;
    source.is_inverted_index = true;
    source.is_dryrun = true;
    source.is_warmup = true;
    source.condition_cache_filtered_rows = 31;
    source.predicate_filtered_rows = 37;
    source.remote_scan_cache_write_limiter = &limiter;

    auto safe = PagePrefetchSafeIOContext::from_query_thread(source, 10086);
    query_id.hi = 23;
    query_id.lo = 29;

    ASSERT_TRUE(safe.query_id_value.has_value());
    EXPECT_EQ(*safe.query_id_value, captured_query_id);
    EXPECT_EQ(safe.io_ctx.query_id, &*safe.query_id_value);
    EXPECT_NE(safe.io_ctx.query_id, source.query_id);
    EXPECT_EQ(safe.io_ctx.file_cache_stats, nullptr);
    EXPECT_EQ(safe.io_ctx.file_reader_stats, nullptr);
    EXPECT_EQ(safe.io_ctx.remote_scan_cache_write_limiter, nullptr);
    EXPECT_EQ(safe.io_ctx.reader_type, ReaderType::READER_QUERY);
    EXPECT_TRUE(safe.io_ctx.is_disposable);
    EXPECT_EQ(safe.io_ctx.expiration_time, 12345);
    EXPECT_FALSE(safe.io_ctx.is_index_data);
    EXPECT_FALSE(safe.io_ctx.is_inverted_index);
    EXPECT_FALSE(safe.io_ctx.is_dryrun);
    EXPECT_FALSE(safe.io_ctx.is_warmup);
    EXPECT_EQ(safe.io_ctx.condition_cache_filtered_rows, 0);
    EXPECT_EQ(safe.io_ctx.predicate_filtered_rows, 0);
    EXPECT_TRUE(safe.io_ctx.bypass_peer_read);
    EXPECT_EQ(safe.io_ctx.cache_align_mode_override, io::CacheAlignMode::UNALIGNED);
    EXPECT_EQ(safe.io_ctx.cache_write_mode_override, io::CacheWriteMode::NO_WRITE);
    EXPECT_EQ(safe.io_ctx.file_cache_miss_policy, io::FileCacheMissPolicy::REMOTE_ONLY_ON_MISS);
    EXPECT_TRUE(safe.remote_only_on_miss);
    EXPECT_EQ(safe.admission_ctx.query_id, captured_query_id);
    EXPECT_EQ(safe.admission_ctx.cache_type, io::FileCacheType::TTL);
    EXPECT_EQ(safe.admission_ctx.expiration_time, 12345);
    EXPECT_EQ(safe.admission_ctx.tablet_id, 10086);
    EXPECT_FALSE(safe.admission_ctx.is_warmup);

    PagePrefetchSafeIOContext copied = safe;
    EXPECT_EQ(copied.io_ctx.query_id, &*copied.query_id_value);
    EXPECT_NE(copied.io_ctx.query_id, safe.io_ctx.query_id);
    PagePrefetchSafeIOContext moved = std::move(copied);
    EXPECT_EQ(copied.io_ctx.query_id, nullptr);
    EXPECT_EQ(moved.io_ctx.query_id, &*moved.query_id_value);
    EXPECT_EQ(*moved.query_id_value, captured_query_id);
}

TEST_F(PagePrefetchIOServiceTest, ReadsExactRangeWithWorkerOwnedContextAndBuffer) {
    create_cache("page_prefetch_io_service_exact_range");
    create_pool(4);
    PagePrefetchIOService service(_pool.get(), service_options());
    auto inspecting_reader =
            std::make_shared<InspectingFileReader>(open_remote_file(), false, false);
    auto reader = create_reader(inspecting_reader);
    TUniqueId query_id = make_query_id(41, 43);
    io::FileCacheStatistics query_file_cache_stats;
    io::FileReaderStats query_file_reader_stats;
    auto safe_context =
            make_safe_context(query_id, &query_file_cache_stats, &query_file_reader_stats);
    auto runtime_query_context = MockQueryContext::create(query_id);
    auto query_context = service.get_or_create_query_context(query_id, runtime_query_context);
    EXPECT_EQ(service.get_or_create_query_context(query_id, runtime_query_context), query_context);

    constexpr size_t read_offset = 1024 * 1024 + 123;
    constexpr size_t read_size = 4096;
    auto submit = service.try_submit(make_service_spec(read_offset, read_size), reader,
                                     std::move(safe_context), query_context);
    ASSERT_EQ(submit.reject_reason, PagePrefetchRejectReason::NONE);
    ASSERT_NE(submit.range, nullptr);
    auto range = std::move(submit.range);
    ASSERT_TRUE(range->wait_for_consume().ok());
    _pool->wait();

    EXPECT_EQ(range->state(), PrefetchRange::State::READY);
    Slice page = range->page_slice(0);
    EXPECT_EQ(std::string_view(page.data, page.size), std::string(read_size, '1'));
    const auto stats = range->read_stats();
    EXPECT_EQ(stats.cache_or_inflight_bytes, 0);
    EXPECT_EQ(stats.remote_bytes, read_size);
    EXPECT_EQ(query_file_cache_stats.bytes_read_from_remote, 0);
    EXPECT_EQ(query_file_reader_stats.read_calls, 0);
    EXPECT_EQ(query_context->inflight_ranges(), 0);
    EXPECT_EQ(query_context->resident_bytes(), read_size);
    EXPECT_EQ(service.global_budget()->inflight_ranges(), 0);
    EXPECT_EQ(service.global_budget()->resident_bytes(), read_size);
    EXPECT_EQ(service.outstanding_tasks(), 0);

    const auto observed = inspecting_reader->observed();
    EXPECT_EQ(observed.offset, read_offset);
    EXPECT_EQ(observed.size, read_size);
    EXPECT_EQ(observed.read_count, 1);
    ASSERT_TRUE(observed.query_id_value.has_value());
    EXPECT_EQ(*observed.query_id_value, query_id);
    EXPECT_NE(observed.query_id_pointer, &query_id);
    EXPECT_NE(observed.file_cache_stats, nullptr);
    EXPECT_NE(observed.file_cache_stats, &query_file_cache_stats);
    EXPECT_NE(observed.file_reader_stats, nullptr);
    EXPECT_NE(observed.file_reader_stats, &query_file_reader_stats);
    EXPECT_EQ(observed.limiter, nullptr);
    EXPECT_FALSE(observed.is_index_data);
    EXPECT_FALSE(observed.is_inverted_index);
    EXPECT_FALSE(observed.is_dryrun);
    EXPECT_FALSE(observed.is_warmup);
    EXPECT_TRUE(observed.bypass_peer_read);
    EXPECT_EQ(observed.align_mode, io::CacheAlignMode::UNALIGNED);
    EXPECT_EQ(observed.write_mode, io::CacheWriteMode::NO_WRITE);

    range.reset();
    EXPECT_EQ(query_context->resident_bytes(), 0);
    EXPECT_EQ(service.global_budget()->resident_bytes(), 0);
    EXPECT_EQ(service.mem_tracker()->consumption(), 0);
    service.shutdown();
}

TEST_F(PagePrefetchIOServiceTest, PagePrefetcherTracksWindowConsumptionAndSkippedLookahead) {
    create_cache("page_prefetcher_window_state");
    create_pool(4);
    PagePrefetchIOService service(_pool.get(), service_options());
    auto reader = create_reader(open_remote_file());
    TUniqueId query_id = make_query_id(211, 223);
    io::FileCacheStatistics query_file_cache_stats;
    io::FileReaderStats query_file_reader_stats;
    auto runtime_query_context = MockQueryContext::create(query_id);
    auto query_context = service.get_or_create_query_context(query_id, runtime_query_context);
    PagePrefetcher prefetcher({
            .io_service = &service,
            .reader = reader,
            .query_context = query_context,
            .io_context =
                    make_safe_context(query_id, &query_file_cache_stats, &query_file_reader_stats),
            .pages = {{.page_index = 0,
                       .first_ordinal = 0,
                       .last_ordinal = 9,
                       .offset = 0,
                       .size = 16},
                      {.page_index = 1,
                       .first_ordinal = 10,
                       .last_ordinal = 19,
                       .offset = 32,
                       .size = 16},
                      {.page_index = 2,
                       .first_ordinal = 20,
                       .last_ordinal = 29,
                       .offset = 64,
                       .size = 16}},
            .file_size = reader->size(),
            .options = {.window_pages = 2,
                        .min_window_pages = 1,
                        .max_window_pages = 2,
                        .max_gap_bytes = 16,
                        .max_range_bytes = 128,
                        .max_pages_per_range = 4,
                        .max_read_amplification_ratio = 2.0,
                        .writeback_min_block_coverage = 0.5,
                        .adaptive_window = false},
            .page_cache_probe = [](const PageCandidate&) { return false; },
    });

    ASSERT_TRUE(prefetcher
                        .prepare({.kind = PagePrefetchRequest::Kind::ORDINAL_RANGE,
                                  .first_ordinal = 0,
                                  .ordinal_count = 10,
                                  .is_forward = true})
                        .ok());
    _pool->wait();
    auto first = prefetcher.acquire(0);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(first->has_value());
    EXPECT_EQ((*first)->data, Slice(std::string(16, '0')));
    prefetcher.mark_consumed(0);

    ASSERT_TRUE(prefetcher
                        .prepare({.kind = PagePrefetchRequest::Kind::ORDINAL_RANGE,
                                  .first_ordinal = 20,
                                  .ordinal_count = 10,
                                  .is_forward = true})
                        .ok());
    auto skipped = prefetcher.acquire(1);
    ASSERT_TRUE(skipped.has_value());
    EXPECT_FALSE(skipped->has_value());
    _pool->wait();
    auto third = prefetcher.acquire(2);
    ASSERT_TRUE(third.has_value());
    ASSERT_TRUE(third->has_value());
    EXPECT_EQ((*third)->data, Slice(std::string(16, '0')));
    prefetcher.mark_consumed(2);

    const auto& statistics = prefetcher.statistics();
    EXPECT_EQ(statistics.candidate_pages, 3);
    EXPECT_EQ(statistics.submitted_pages, 3);
    EXPECT_EQ(statistics.consumed_pages, 2);
    EXPECT_EQ(statistics.ready_hits, 2);
    EXPECT_EQ(statistics.submitted_ranges, 2);
    EXPECT_EQ(statistics.throttled_ranges, 0);
    EXPECT_EQ(statistics.fallback_pages, 0);
    EXPECT_EQ(statistics.requested_page_bytes, 48);
    EXPECT_EQ(statistics.fetched_bytes, 64);
    EXPECT_EQ(statistics.coalesced_gap_bytes, 16);
    EXPECT_EQ(statistics.remote_bytes, 64);
    service.shutdown();
}

TEST_F(PagePrefetchIOServiceTest, PagePrefetcherAdmissionRejectionFallsBackWithoutWaiting) {
    create_cache("page_prefetcher_admission_fallback");
    create_pool(4);
    auto options = service_options();
    options.query_limits.max_ranges = 1;
    PagePrefetchIOService service(_pool.get(), options);
    auto blocking_remote = std::make_shared<InspectingFileReader>(open_remote_file(), true, false);
    auto reader = create_reader(blocking_remote);
    TUniqueId query_id = make_query_id(227, 229);
    io::FileCacheStatistics query_file_cache_stats;
    io::FileReaderStats query_file_reader_stats;
    auto runtime_query_context = MockQueryContext::create(query_id);
    auto query_context = service.get_or_create_query_context(query_id, runtime_query_context);
    PagePrefetcher prefetcher({
            .io_service = &service,
            .reader = reader,
            .query_context = query_context,
            .io_context =
                    make_safe_context(query_id, &query_file_cache_stats, &query_file_reader_stats),
            .pages = {{.page_index = 0,
                       .first_ordinal = 0,
                       .last_ordinal = 9,
                       .offset = 0,
                       .size = 16},
                      {.page_index = 1,
                       .first_ordinal = 10,
                       .last_ordinal = 19,
                       .offset = 32,
                       .size = 16}},
            .file_size = reader->size(),
            .options = {.window_pages = 1,
                        .min_window_pages = 1,
                        .max_window_pages = 1,
                        .max_gap_bytes = 16,
                        .max_range_bytes = 128,
                        .max_pages_per_range = 1,
                        .max_read_amplification_ratio = 2.0,
                        .writeback_min_block_coverage = 0.5,
                        .adaptive_window = false},
            .page_cache_probe = [](const PageCandidate&) { return false; },
    });

    ASSERT_TRUE(prefetcher
                        .prepare({.kind = PagePrefetchRequest::Kind::ORDINAL_RANGE,
                                  .first_ordinal = 0,
                                  .ordinal_count = 1,
                                  .is_forward = true})
                        .ok());
    ASSERT_TRUE(blocking_remote->wait_until_entered(std::chrono::seconds(5)));
    const auto prepare_start = std::chrono::steady_clock::now();
    ASSERT_TRUE(prefetcher
                        .prepare({.kind = PagePrefetchRequest::Kind::ORDINAL_RANGE,
                                  .first_ordinal = 10,
                                  .ordinal_count = 1,
                                  .is_forward = true})
                        .ok());
    EXPECT_LT(std::chrono::steady_clock::now() - prepare_start, std::chrono::seconds(1));
    auto fallback = prefetcher.acquire(1);
    ASSERT_TRUE(fallback.has_value());
    EXPECT_FALSE(fallback->has_value());
    EXPECT_EQ(prefetcher.statistics().throttled_ranges, 1);
    EXPECT_EQ(prefetcher.statistics().fallback_pages, 1);

    blocking_remote->release();
    _pool->wait();
    service.shutdown();
}

TEST_F(PagePrefetchIOServiceTest, ConsumedPageWritesOnlyItsAssociatedValidCompleteBlock) {
    create_cache("page_prefetch_writeback_usefulness");
    create_pool(4);
    const size_t block_size = static_cast<size_t>(config::file_cache_each_block_size);
    auto options = service_options();
    options.query_limits.max_bytes = 4 * block_size;
    PagePrefetchIOService service(_pool.get(), options);
    auto reader = create_reader(open_remote_file());
    const auto cache_hash = reader->cache_hash();
    TUniqueId query_id = make_query_id(107, 109);
    io::FileCacheStatistics query_file_cache_stats;
    io::FileReaderStats query_file_reader_stats;
    auto runtime_query_context = MockQueryContext::create(query_id);
    auto query_context = service.get_or_create_query_context(query_id, runtime_query_context);
    auto submit = service.try_submit(
            make_complete_block_service_spec(block_size), reader,
            make_safe_context(query_id, &query_file_cache_stats, &query_file_reader_stats),
            query_context);
    ASSERT_EQ(submit.reject_reason, PagePrefetchRejectReason::NONE);
    ASSERT_NE(submit.range, nullptr);
    auto range = std::move(submit.range);
    ASSERT_TRUE(range->wait_for_consume().ok());
    _pool->wait();

    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    std::mutex writeback_mutex;
    std::condition_variable writeback_cv;
    bool writeback_entered = false;
    bool release_writeback = false;
    sync_point->set_call_back(
            "PagePrefetchIOService::_execute_writeback_copy:before_fixed_submit",
            [&](auto&&) {
                std::unique_lock lock(writeback_mutex);
                writeback_entered = true;
                writeback_cv.notify_all();
                writeback_cv.wait(lock, [&]() { return release_writeback; });
            },
            &guard);
    sync_point->enable_processing();
    Defer clear_sync_point {[&]() {
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    }};
    Defer release_blocked_writeback {[&]() {
        {
            std::lock_guard lock(writeback_mutex);
            release_writeback = true;
        }
        writeback_cv.notify_all();
    }};

    FileCacheWritebackCoordinator coordinator(&service);
    coordinator.mark_page_consumed(range, 11);
    {
        std::unique_lock lock(writeback_mutex);
        ASSERT_TRUE(writeback_cv.wait_for(lock, std::chrono::seconds(5),
                                          [&]() { return writeback_entered; }));
    }
    EXPECT_EQ(query_context->resident_bytes(), 3 * block_size);
    EXPECT_EQ(service.global_budget()->resident_bytes(), 3 * block_size);
    EXPECT_EQ(service.outstanding_tasks(), 1);

    coordinator.invalidate_page(range, 12);
    coordinator.mark_page_consumed(range, 12);
    {
        std::lock_guard lock(writeback_mutex);
        release_writeback = true;
    }
    writeback_cv.notify_all();
    _pool->wait();
    EXPECT_EQ(query_context->resident_bytes(), 2 * block_size);
    EXPECT_EQ(service.global_budget()->resident_bytes(), 2 * block_size);
    EXPECT_EQ(service.outstanding_tasks(), 0);

    coordinator.mark_page_consumed(range, 11);
    EXPECT_EQ(service.outstanding_tasks(), 0);
    for (size_t attempt = 0; attempt < 500 && _cache->async_write_service()->pending_count() != 0;
         ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(_cache->async_write_service()->pending_count(), 0);

    io::ReadStatistics read_stats;
    io::CacheContext cache_context;
    cache_context.stats = &read_stats;
    io::FileBlocks blocks;
    bool fully_covered = false;
    ASSERT_TRUE(_cache->get_downloaded_blocks_if_fully_covered(
                              cache_hash, 0, block_size, cache_context, &blocks, &fully_covered)
                        .ok());
    ASSERT_TRUE(fully_covered);
    ASSERT_EQ(blocks.size(), 1);
    std::string first_block(block_size, '\0');
    ASSERT_TRUE(blocks.front()->read(Slice(first_block.data(), first_block.size()), 0).ok());
    EXPECT_EQ(first_block, std::string(block_size, '0'));

    blocks.clear();
    fully_covered = false;
    ASSERT_TRUE(_cache->get_downloaded_blocks_if_fully_covered(cache_hash, block_size, block_size,
                                                               cache_context, &blocks,
                                                               &fully_covered)
                        .ok());
    EXPECT_FALSE(fully_covered);

    range.reset();
    EXPECT_EQ(query_context->resident_bytes(), 0);
    EXPECT_EQ(service.global_budget()->resident_bytes(), 0);
    EXPECT_EQ(service.mem_tracker()->consumption(), 0);
    service.shutdown();
}

TEST_F(PagePrefetchIOServiceTest, PoolRejectionRollsBackEveryRejectedRangeResource) {
    create_cache("page_prefetch_io_service_pool_rejection");
    create_pool(0);
    PagePrefetchIOService service(_pool.get(), service_options());
    auto inspecting_reader =
            std::make_shared<InspectingFileReader>(open_remote_file(), true, false);
    Defer release_reader {[&]() { inspecting_reader->release(); }};
    auto reader = create_reader(inspecting_reader);
    TUniqueId query_id = make_query_id(47, 53);
    io::FileCacheStatistics query_file_cache_stats;
    io::FileReaderStats query_file_reader_stats;
    auto runtime_query_context = MockQueryContext::create(query_id);
    auto query_context = service.get_or_create_query_context(query_id, runtime_query_context);

    constexpr size_t read_offset = 2 * 1024 * 1024 + 17;
    constexpr size_t read_size = 4096;
    auto first = service.try_submit(
            make_service_spec(read_offset, read_size), reader,
            make_safe_context(query_id, &query_file_cache_stats, &query_file_reader_stats),
            query_context);
    ASSERT_NE(first.range, nullptr);
    ASSERT_TRUE(inspecting_reader->wait_until_entered(std::chrono::seconds(5)));
    auto rejected = service.try_submit(
            make_service_spec(read_offset + read_size, read_size), reader,
            make_safe_context(query_id, &query_file_cache_stats, &query_file_reader_stats),
            query_context);

    EXPECT_EQ(rejected.range, nullptr);
    EXPECT_EQ(rejected.reject_reason, PagePrefetchRejectReason::THREAD_POOL_REJECTED);
    EXPECT_EQ(query_context->inflight_ranges(), 1);
    EXPECT_EQ(query_context->resident_bytes(), read_size);
    EXPECT_EQ(service.global_budget()->inflight_ranges(), 1);
    EXPECT_EQ(service.global_budget()->resident_bytes(), read_size);
    EXPECT_EQ(service.outstanding_tasks(), 1);

    inspecting_reader->release();
    ASSERT_TRUE(first.range->wait_for_consume().ok());
    _pool->wait();
    first.range.reset();
    EXPECT_EQ(query_context->inflight_ranges(), 0);
    EXPECT_EQ(query_context->resident_bytes(), 0);
    EXPECT_EQ(service.global_budget()->inflight_ranges(), 0);
    EXPECT_EQ(service.global_budget()->resident_bytes(), 0);
    EXPECT_EQ(service.outstanding_tasks(), 0);
    EXPECT_EQ(service.mem_tracker()->consumption(), 0);
    service.shutdown();
}

TEST_F(PagePrefetchIOServiceTest, ShutdownCancelsWaitersAndWaitsForRunningIOWithoutOwningPool) {
    create_cache("page_prefetch_io_service_shutdown");
    create_pool(4);
    PagePrefetchIOService service(_pool.get(), service_options());
    auto inspecting_reader =
            std::make_shared<InspectingFileReader>(open_remote_file(), true, false);
    Defer release_reader {[&]() { inspecting_reader->release(); }};
    auto reader = create_reader(inspecting_reader);
    TUniqueId query_id = make_query_id(59, 61);
    io::FileCacheStatistics query_file_cache_stats;
    io::FileReaderStats query_file_reader_stats;
    auto runtime_query_context = MockQueryContext::create(query_id);
    auto query_context = service.get_or_create_query_context(query_id, runtime_query_context);
    auto submit = service.try_submit(
            make_service_spec(3 * 1024 * 1024 + 71, 4096), reader,
            make_safe_context(query_id, &query_file_cache_stats, &query_file_reader_stats),
            query_context);
    ASSERT_NE(submit.range, nullptr);
    ASSERT_TRUE(inspecting_reader->wait_until_entered(std::chrono::seconds(5)));
    auto submitted_range = submit.range;
    auto waiter = std::async(std::launch::async,
                             [submitted_range]() { return submitted_range->wait_for_consume(); });
    auto shutdown_future = std::async(std::launch::async, [&service]() { service.shutdown(); });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (service.accepting() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    EXPECT_FALSE(service.accepting());
    EXPECT_EQ(waiter.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    EXPECT_EQ(shutdown_future.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    EXPECT_EQ(submit.range->state(), PrefetchRange::State::RUNNING);
    EXPECT_TRUE(submit.range->cancel_requested());

    auto rejected = service.try_submit(
            make_service_spec(4 * 1024 * 1024 + 73, 4096), reader,
            make_safe_context(query_id, &query_file_cache_stats, &query_file_reader_stats),
            query_context);
    EXPECT_EQ(rejected.range, nullptr);
    EXPECT_EQ(rejected.reject_reason, PagePrefetchRejectReason::SHUTTING_DOWN);

    inspecting_reader->release();
    shutdown_future.get();
    const Status wait_status = waiter.get();
    EXPECT_TRUE(wait_status.is<ErrorCode::CANCELLED>());
    EXPECT_EQ(submit.range->state(), PrefetchRange::State::CANCELLED);
    EXPECT_EQ(service.outstanding_tasks(), 0);

    auto pool_task_done = std::make_shared<std::promise<void>>();
    auto pool_task_future = pool_task_done->get_future();
    ASSERT_TRUE(_pool->submit_func([pool_task_done]() { pool_task_done->set_value(); }).ok());
    EXPECT_EQ(pool_task_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
}

TEST_F(PagePrefetchIOServiceTest, ReadFailurePublishesStableErrorAndReleasesActiveSlot) {
    create_cache("page_prefetch_io_service_read_failure");
    create_pool(4);
    PagePrefetchIOService service(_pool.get(), service_options());
    auto inspecting_reader =
            std::make_shared<InspectingFileReader>(open_remote_file(), false, true);
    auto reader = create_reader(inspecting_reader);
    TUniqueId query_id = make_query_id(67, 71);
    io::FileCacheStatistics query_file_cache_stats;
    io::FileReaderStats query_file_reader_stats;
    auto runtime_query_context = MockQueryContext::create(query_id);
    auto query_context = service.get_or_create_query_context(query_id, runtime_query_context);
    auto submit = service.try_submit(
            make_service_spec(5 * 1024 * 1024 + 79, 4096), reader,
            make_safe_context(query_id, &query_file_cache_stats, &query_file_reader_stats),
            query_context);
    ASSERT_NE(submit.range, nullptr);

    const Status first = submit.range->wait_for_consume();
    const Status second = submit.range->wait_for_consume();
    _pool->wait();
    EXPECT_TRUE(first.is<ErrorCode::IO_ERROR>());
    EXPECT_EQ(first, second);
    EXPECT_EQ(submit.range->state(), PrefetchRange::State::FAILED);
    EXPECT_EQ(query_file_cache_stats.bytes_read_from_remote, 0);
    EXPECT_EQ(query_file_reader_stats.read_calls, 0);
    EXPECT_EQ(query_context->inflight_ranges(), 0);
    EXPECT_EQ(query_context->resident_bytes(), 4096);
    EXPECT_EQ(service.outstanding_tasks(), 0);

    submit.range.reset();
    EXPECT_EQ(query_context->resident_bytes(), 0);
    EXPECT_EQ(service.global_budget()->resident_bytes(), 0);
    EXPECT_EQ(service.mem_tracker()->consumption(), 0);
    service.shutdown();
}

TEST_F(PagePrefetchIOServiceTest, DynamicLimitsRejectNewReservationsWithoutRevokingExistingOnes) {
    create_pool(4);
    PagePrefetchIOService service(_pool.get(), service_options());
    const TUniqueId query_id = make_query_id(83, 89);
    auto runtime_query_context = MockQueryContext::create(query_id);
    auto query_context = service.get_or_create_query_context(query_id, runtime_query_context);
    PagePrefetchRejectReason reject_reason = PagePrefetchRejectReason::NONE;
    auto existing = PagePrefetchReservation::try_reserve_writeback(
            query_context, service.global_budget(), 4096, &reject_reason);
    ASSERT_TRUE(existing.has_value());

    const PagePrefetchIOServiceOptions reduced_options {
            .query_limits = {.max_ranges = 2, .max_bytes = 2048},
            .global_limits = {.max_ranges = 4, .max_bytes = 4096},
    };
    ASSERT_TRUE(service.update_options(reduced_options).ok());
    EXPECT_EQ(service.options().query_limits.max_ranges, 2);
    EXPECT_EQ(service.options().query_limits.max_bytes, 2048);
    EXPECT_EQ(service.options().global_limits.max_ranges, 4);
    EXPECT_EQ(service.options().global_limits.max_bytes, 4096);
    EXPECT_EQ(query_context->limits().max_ranges, 2);
    EXPECT_EQ(query_context->limits().max_bytes, 2048);
    EXPECT_EQ(service.global_budget()->limits().max_ranges, 4);
    EXPECT_EQ(service.global_budget()->limits().max_bytes, 4096);
    EXPECT_EQ(query_context->resident_bytes(), 4096);
    EXPECT_EQ(service.global_budget()->resident_bytes(), 4096);

    auto rejected = PagePrefetchReservation::try_reserve_writeback(
            query_context, service.global_budget(), 1, &reject_reason);
    EXPECT_FALSE(rejected.has_value());
    EXPECT_EQ(reject_reason, PagePrefetchRejectReason::QUERY_BYTE_LIMIT);
    EXPECT_EQ(query_context->resident_bytes(), 4096);
    EXPECT_EQ(service.global_budget()->resident_bytes(), 4096);

    existing.reset();
    auto accepted = PagePrefetchReservation::try_reserve_writeback(
            query_context, service.global_budget(), 1024, &reject_reason);
    ASSERT_TRUE(accepted.has_value());
    EXPECT_EQ(reject_reason, PagePrefetchRejectReason::NONE);

    const TUniqueId second_query_id = make_query_id(97, 101);
    auto second_runtime_query_context = MockQueryContext::create(second_query_id);
    auto second_query_context =
            service.get_or_create_query_context(second_query_id, second_runtime_query_context);
    EXPECT_EQ(second_query_context->limits().max_ranges, 2);
    EXPECT_EQ(second_query_context->limits().max_bytes, 2048);

    auto invalid_options = reduced_options;
    invalid_options.query_limits.max_ranges = 5;
    EXPECT_TRUE(service.update_options(invalid_options).is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_EQ(service.options().query_limits.max_ranges, 2);
    EXPECT_EQ(query_context->limits().max_ranges, 2);
    EXPECT_EQ(service.global_budget()->limits().max_ranges, 4);

    accepted.reset();
    service.shutdown();
}

TEST(PagePrefetchAdmissionTest, QueryRangeAndByteLimitsRollbackCompletely) {
    auto global = std::make_shared<PagePrefetchGlobalBudget>(kWideLimits);
    auto range_limited_query = std::make_shared<PagePrefetchQueryContext>(
            PagePrefetchBudgetLimits {.max_ranges = 1, .max_bytes = 64});
    PagePrefetchRejectReason reject_reason = PagePrefetchRejectReason::NONE;
    auto first = PagePrefetchReservation::try_reserve_range(range_limited_query, global, 16,
                                                            &reject_reason);
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(reject_reason, PagePrefetchRejectReason::NONE);
    auto rejected = PagePrefetchReservation::try_reserve_range(range_limited_query, global, 16,
                                                               &reject_reason);
    EXPECT_FALSE(rejected.has_value());
    EXPECT_EQ(reject_reason, PagePrefetchRejectReason::QUERY_RANGE_LIMIT);
    EXPECT_EQ(range_limited_query->inflight_ranges(), 1);
    EXPECT_EQ(range_limited_query->resident_bytes(), 16);
    EXPECT_EQ(global->inflight_ranges(), 1);
    EXPECT_EQ(global->resident_bytes(), 16);
    first.reset();
    EXPECT_EQ(range_limited_query->inflight_ranges(), 0);
    EXPECT_EQ(range_limited_query->resident_bytes(), 0);
    EXPECT_EQ(global->inflight_ranges(), 0);
    EXPECT_EQ(global->resident_bytes(), 0);

    auto byte_limited_query = std::make_shared<PagePrefetchQueryContext>(
            PagePrefetchBudgetLimits {.max_ranges = 2, .max_bytes = 8});
    auto within_limit = PagePrefetchReservation::try_reserve_range(byte_limited_query, global, 6,
                                                                   &reject_reason);
    ASSERT_TRUE(within_limit.has_value());
    rejected = PagePrefetchReservation::try_reserve_range(byte_limited_query, global, 3,
                                                          &reject_reason);
    EXPECT_FALSE(rejected.has_value());
    EXPECT_EQ(reject_reason, PagePrefetchRejectReason::QUERY_BYTE_LIMIT);
    EXPECT_EQ(byte_limited_query->inflight_ranges(), 1);
    EXPECT_EQ(byte_limited_query->resident_bytes(), 6);
    EXPECT_EQ(global->inflight_ranges(), 1);
    EXPECT_EQ(global->resident_bytes(), 6);
}

TEST(PagePrefetchAdmissionTest, ExpiredRuntimeQueryRejectsNewReservations) {
    const TUniqueId query_id = make_query_id(73, 79);
    auto runtime_query_context = MockQueryContext::create(query_id);
    auto query = std::make_shared<PagePrefetchQueryContext>(query_id, runtime_query_context,
                                                            kWideLimits);
    auto global = std::make_shared<PagePrefetchGlobalBudget>(kWideLimits);
    EXPECT_FALSE(query->cancelled());

    runtime_query_context.reset();
    EXPECT_TRUE(query->cancelled());
    PagePrefetchRejectReason reject_reason = PagePrefetchRejectReason::NONE;
    auto rejected = PagePrefetchReservation::try_reserve_range(query, global, 64, &reject_reason);
    EXPECT_FALSE(rejected.has_value());
    EXPECT_EQ(reject_reason, PagePrefetchRejectReason::QUERY_CANCELLED);
    EXPECT_EQ(query->inflight_ranges(), 0);
    EXPECT_EQ(query->resident_bytes(), 0);
    EXPECT_EQ(global->inflight_ranges(), 0);
    EXPECT_EQ(global->resident_bytes(), 0);
}

TEST(PagePrefetchAdmissionTest, GlobalRangeAndByteLimitsRollbackQueryReservation) {
    PagePrefetchRejectReason reject_reason = PagePrefetchRejectReason::NONE;
    auto query = std::make_shared<PagePrefetchQueryContext>(kWideLimits);
    auto range_limited_global = std::make_shared<PagePrefetchGlobalBudget>(
            PagePrefetchBudgetLimits {.max_ranges = 1, .max_bytes = 64});
    auto first = PagePrefetchReservation::try_reserve_range(query, range_limited_global, 16,
                                                            &reject_reason);
    ASSERT_TRUE(first.has_value());
    auto rejected = PagePrefetchReservation::try_reserve_range(query, range_limited_global, 16,
                                                               &reject_reason);
    EXPECT_FALSE(rejected.has_value());
    EXPECT_EQ(reject_reason, PagePrefetchRejectReason::GLOBAL_RANGE_LIMIT);
    EXPECT_EQ(query->inflight_ranges(), 1);
    EXPECT_EQ(query->resident_bytes(), 16);

    first.reset();
    auto byte_limited_global = std::make_shared<PagePrefetchGlobalBudget>(
            PagePrefetchBudgetLimits {.max_ranges = 2, .max_bytes = 8});
    auto within_limit = PagePrefetchReservation::try_reserve_range(query, byte_limited_global, 6,
                                                                   &reject_reason);
    ASSERT_TRUE(within_limit.has_value());
    rejected = PagePrefetchReservation::try_reserve_range(query, byte_limited_global, 3,
                                                          &reject_reason);
    EXPECT_FALSE(rejected.has_value());
    EXPECT_EQ(reject_reason, PagePrefetchRejectReason::GLOBAL_BYTE_LIMIT);
    EXPECT_EQ(query->inflight_ranges(), 1);
    EXPECT_EQ(query->resident_bytes(), 6);
    EXPECT_EQ(byte_limited_global->inflight_ranges(), 1);
    EXPECT_EQ(byte_limited_global->resident_bytes(), 6);
}

TEST(PagePrefetchAdmissionTest, MoveOnlyReservationSeparatesRangeAndResidentLifetimes) {
    auto query = std::make_shared<PagePrefetchQueryContext>(kWideLimits);
    auto global = std::make_shared<PagePrefetchGlobalBudget>(kWideLimits);
    PagePrefetchRejectReason reject_reason = PagePrefetchRejectReason::NONE;
    auto reservation =
            PagePrefetchReservation::try_reserve_range(query, global, 32, &reject_reason);
    ASSERT_TRUE(reservation.has_value());
    PagePrefetchReservation moved = std::move(*reservation);
    EXPECT_TRUE(moved.valid());
    EXPECT_FALSE(reservation->valid());
    EXPECT_EQ(query->inflight_ranges(), 1);
    EXPECT_EQ(query->resident_bytes(), 32);

    moved.release_range_slot();
    EXPECT_EQ(query->inflight_ranges(), 0);
    EXPECT_EQ(global->inflight_ranges(), 0);
    EXPECT_EQ(query->resident_bytes(), 32);
    EXPECT_EQ(global->resident_bytes(), 32);

    auto replacement =
            PagePrefetchReservation::try_reserve_writeback(query, global, 8, &reject_reason);
    ASSERT_TRUE(replacement.has_value());
    moved = std::move(*replacement);
    EXPECT_EQ(query->resident_bytes(), 8);
    EXPECT_EQ(global->resident_bytes(), 8);
}

TEST(PagePrefetchAdmissionTest, BufferAllocationFailureRollsBackEveryReservation) {
    auto query = std::make_shared<PagePrefetchQueryContext>(kWideLimits);
    auto global = std::make_shared<PagePrefetchGlobalBudget>(kWideLimits);
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::CACHE,
                                                    "PagePrefetchAllocationFailureTest");
    PagePrefetchRejectReason reject_reason = PagePrefetchRejectReason::NONE;
    auto reservation =
            PagePrefetchReservation::try_reserve_range(query, global, 64, &reject_reason);
    ASSERT_TRUE(reservation.has_value());

    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "PagePrefetchBuffer::create:inject_failure",
            [](auto&& values) {
                *try_any_cast<Status*>(values.back()) =
                        Status::MemoryAllocFailed("injected page prefetch allocation failure");
            },
            &guard);
    sync_point->enable_processing();
    Defer clear_sync_point {[&]() {
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    }};

    std::shared_ptr<PagePrefetchBuffer> buffer;
    Status status = PagePrefetchBuffer::create(64, tracker, std::move(*reservation), &buffer);
    EXPECT_TRUE(status.is<ErrorCode::MEM_ALLOC_FAILED>());
    EXPECT_EQ(buffer, nullptr);
    EXPECT_EQ(query->inflight_ranges(), 0);
    EXPECT_EQ(query->resident_bytes(), 0);
    EXPECT_EQ(global->inflight_ranges(), 0);
    EXPECT_EQ(global->resident_bytes(), 0);
    EXPECT_EQ(tracker->consumption(), 0);
}

TEST(PagePrefetchRangeTest, ReadyWakesAllWaitersAndExposesOwnedPageSlice) {
    auto fixture = make_range();
    fixture.range->mark_queued();
    ASSERT_TRUE(fixture.range->mark_running());

    std::vector<std::future<Status>> waiters;
    for (size_t i = 0; i < 8; ++i) {
        waiters.emplace_back(std::async(std::launch::async, [range = fixture.range]() {
            return range->wait_for_consume();
        }));
    }
    RangeReadStats stats {.cache_or_inflight_bytes = 16,
                          .remote_bytes = 48,
                          .remote_io_time_ns = 1234,
                          .self_heal_count = 0};
    fixture.range->publish_ready(stats);
    for (auto& waiter : waiters) {
        EXPECT_TRUE(waiter.get().ok());
    }
    EXPECT_TRUE(fixture.range->wait_for_consume().ok());
    EXPECT_EQ(fixture.range->state(), PrefetchRange::State::READY);
    EXPECT_EQ(fixture.query->inflight_ranges(), 0);
    EXPECT_EQ(fixture.query->resident_bytes(), 64);

    Slice page = fixture.range->page_slice(0);
    ASSERT_EQ(page.size, 16);
    EXPECT_EQ(std::string_view(page.data, page.size), std::string_view("pppppppppppppppp"));
    RangeReadStats merged;
    EXPECT_TRUE(fixture.range->take_read_stats_once(&merged));
    EXPECT_EQ(merged.remote_bytes, 48);
    EXPECT_FALSE(fixture.range->take_read_stats_once(&merged));

    fixture.range->request_cancel();
    EXPECT_EQ(fixture.range->state(), PrefetchRange::State::READY);
    EXPECT_TRUE(fixture.range->wait_for_consume().ok());
    fixture.range.reset();
    EXPECT_EQ(fixture.query->resident_bytes(), 0);
    EXPECT_EQ(fixture.global->resident_bytes(), 0);
    EXPECT_EQ(fixture.tracker->consumption(), 0);
}

TEST(PagePrefetchRangeTest, RunningCancellationWakesWaiterBeforeWorkerFinishes) {
    auto fixture = make_range();
    fixture.range->mark_queued();
    ASSERT_TRUE(fixture.range->mark_running());
    auto waiter = std::async(std::launch::async,
                             [range = fixture.range]() { return range->wait_for_consume(); });

    fixture.range->request_cancel();
    ASSERT_EQ(waiter.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_TRUE(waiter.get().is<ErrorCode::CANCELLED>());
    EXPECT_EQ(fixture.range->state(), PrefetchRange::State::RUNNING);
    EXPECT_EQ(fixture.query->inflight_ranges(), 1);

    fixture.range->publish_ready({.cache_or_inflight_bytes = 0,
                                  .remote_bytes = 64,
                                  .remote_io_time_ns = 0,
                                  .self_heal_count = 0});
    EXPECT_EQ(fixture.range->state(), PrefetchRange::State::CANCELLED);
    EXPECT_TRUE(fixture.range->wait_for_consume().is<ErrorCode::CANCELLED>());
    EXPECT_EQ(fixture.query->inflight_ranges(), 0);
    EXPECT_EQ(fixture.query->resident_bytes(), 64);
}

TEST(PagePrefetchRangeTest, QueuedCancellationFinalizesWhenWorkerTakesTask) {
    auto fixture = make_range();
    fixture.range->mark_queued();
    fixture.range->request_cancel();
    EXPECT_TRUE(fixture.range->wait_for_consume().is<ErrorCode::CANCELLED>());
    EXPECT_EQ(fixture.range->state(), PrefetchRange::State::QUEUED);
    EXPECT_FALSE(fixture.range->mark_running());
    EXPECT_EQ(fixture.range->state(), PrefetchRange::State::CANCELLED);
    EXPECT_EQ(fixture.query->inflight_ranges(), 0);
}

TEST(PagePrefetchRangeTest, FailedRangePublishesStableStatusAndStats) {
    auto fixture = make_range();
    fixture.range->mark_queued();
    ASSERT_TRUE(fixture.range->mark_running());
    fixture.range->publish_failed(Status::IOError("injected prefetch read failure"),
                                  {.cache_or_inflight_bytes = 16,
                                   .remote_bytes = 0,
                                   .remote_io_time_ns = 55,
                                   .self_heal_count = 1});
    Status first = fixture.range->wait_for_consume();
    Status second = fixture.range->wait_for_consume();
    EXPECT_TRUE(first.is<ErrorCode::IO_ERROR>());
    EXPECT_EQ(first, second);
    EXPECT_EQ(fixture.range->state(), PrefetchRange::State::FAILED);
    EXPECT_EQ(fixture.range->read_stats().self_heal_count, 1);
    EXPECT_EQ(fixture.query->inflight_ranges(), 0);
}

TEST(PagePrefetchRangeTest, QueryCancellationCancelsRegisteredRangeAndNewReservations) {
    auto fixture = make_range();
    fixture.range->mark_queued();
    ASSERT_TRUE(fixture.range->mark_running());
    fixture.query->register_range(fixture.range);
    fixture.query->cancel();
    EXPECT_TRUE(fixture.range->wait_for_consume().is<ErrorCode::CANCELLED>());
    EXPECT_EQ(fixture.range->state(), PrefetchRange::State::RUNNING);

    PagePrefetchRejectReason reject_reason = PagePrefetchRejectReason::NONE;
    auto rejected = PagePrefetchReservation::try_reserve_range(fixture.query, fixture.global, 1,
                                                               &reject_reason);
    EXPECT_FALSE(rejected.has_value());
    EXPECT_EQ(reject_reason, PagePrefetchRejectReason::QUERY_CANCELLED);
    fixture.range->publish_cancelled();
    EXPECT_EQ(fixture.range->state(), PrefetchRange::State::CANCELLED);
}

TEST(PagePrefetchRangeTest, RejectedRangeReleasesActiveSlotAndKeepsBufferUntilDestruction) {
    auto fixture = make_range();
    fixture.range->mark_rejected(Status::TooManyTasks("thread pool rejected range"));
    Status status = fixture.range->wait_for_consume();
    EXPECT_TRUE(status.is<ErrorCode::TOO_MANY_TASKS>());
    EXPECT_EQ(fixture.range->state(), PrefetchRange::State::REJECTED);
    EXPECT_EQ(fixture.query->inflight_ranges(), 0);
    EXPECT_EQ(fixture.query->resident_bytes(), 64);
    fixture.range.reset();
    EXPECT_EQ(fixture.query->resident_bytes(), 0);
    EXPECT_EQ(fixture.global->resident_bytes(), 0);
}

} // namespace
} // namespace doris::segment_v2
