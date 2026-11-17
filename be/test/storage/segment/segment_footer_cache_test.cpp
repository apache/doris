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

#include <atomic>
#include <random>
#include <thread>

#include "core/field.h"
#include "cpp/sync_point.h"
#include "io/cache/remote_scan_cache_write_limiter.h"
#include "runtime/memory/cache_manager.h"
#include "runtime/thread_context.h"
#include "storage/cache/page_cache.h"
#include "storage/segment/segment.h"
#include "storage/segment/vertical_segment_writer.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_schema_helper.h"

namespace doris {

TabletColumnPtr create_int_sequence_value(int32_t id, bool is_nullable = true,
                                          bool is_bf_column = false);

TabletSchemaSPtr create_schema(const std::vector<TabletColumnPtr>& columns,
                               KeysType keys_type = UNIQUE_KEYS);

using Generator = std::function<void(size_t rid, int cid, Field& field)>;

void build_segment(VerticalSegmentWriterOptions opts, TabletSchemaSPtr build_schema,
                   size_t segment_id, TabletSchemaSPtr query_schema, size_t nrows,
                   Generator generator, std::shared_ptr<Segment>* res, std::string segment_dir);

static std::string segment_footer_cache_test_segment_dir = "./ut_dir/segment_footer_cache_test";

class SegmentFooterCacheTest : public ::testing::Test {
    using Segments = std::vector<std::shared_ptr<segment_v2::Segment>>;
    Segments create(size_t const num_segments, size_t const max_rows_per_segment,
                    size_t const num_key_columns, bool has_sequence_col,
                    size_t const num_value_columns, int const random_seed, int const min_value,
                    int const max_value) {
        Segments segments(num_segments);
        segment_v2::VerticalSegmentWriterOptions opts;
        opts.enable_unique_key_merge_on_write = true;

        size_t const num_columns = num_key_columns + has_sequence_col + num_value_columns;
        size_t const seq_col_idx = has_sequence_col ? num_key_columns : -1;

        std::vector<TabletColumnPtr> columns;

        for (int i = 0; i < num_key_columns; ++i) {
            columns.emplace_back(create_int_key(i));
        }
        if (has_sequence_col) {
            columns.emplace_back(create_int_sequence_value(num_key_columns));
        }
        for (int i = 0; i < num_value_columns; ++i) {
            columns.emplace_back(create_int_value(num_key_columns + has_sequence_col));
        }

        TabletSchemaSPtr tablet_schema = create_schema(columns, UNIQUE_KEYS);

        std::mt19937 rng(random_seed);
        std::uniform_int_distribution<int> gen(min_value, max_value);

        std::vector<std::vector<std::vector<int>>> datas(num_segments);
        std::map<std::pair<size_t, size_t>, std::vector<int>> data_map;
        // each flat_data of data will be a tuple of (column1, column2, ..., segment_id, row_id)
        std::vector<std::vector<int>> flat_data;
        size_t seq_counter = 0;

        // Generate random data, ensuring that there are no identical keys within each segment
        // and the keys within each segment are ordered.
        // Also, ensure that the sequence values are not equal.
        for (size_t sid = 0; sid < num_segments; ++sid) {
            auto& segment_data = datas[sid];
            for (size_t rid = 0; rid < max_rows_per_segment; ++rid) {
                std::vector<int> row;
                for (size_t cid = 0; cid < num_columns; ++cid) {
                    if (cid == seq_col_idx) {
                        row.emplace_back(++seq_counter);
                    } else {
                        row.emplace_back(gen(rng));
                    }
                }
                segment_data.emplace_back(row);
            }
            std::sort(segment_data.begin(), segment_data.end());
            segment_data.erase(
                    std::unique(segment_data.begin(), segment_data.end(),
                                [&](std::vector<int> const& lhs, std::vector<int> const& rhs) {
                                    return std::vector<int>(lhs.begin(),
                                                            lhs.begin() + num_key_columns) ==
                                           std::vector<int>(rhs.begin(),
                                                            rhs.begin() + num_key_columns);
                                }),
                    segment_data.end());
            for (size_t rid = 0; rid < segment_data.size(); ++rid) {
                data_map[{sid, rid}] = segment_data[rid];
                auto row = segment_data[rid];
                row.emplace_back(sid);
                row.emplace_back(rid);
                flat_data.emplace_back(row);
            }
        }

        // Construct segments using the data generated before.
        for (size_t sid = 0; sid < num_segments; ++sid) {
            auto& segment = segments[sid];
            std::vector<int> row_data;
            auto generator = [&](size_t rid, int cid, Field& field) {
                field = Field::create_field<TYPE_INT>(int32_t(data_map[{sid, rid}][cid]));
            };
            build_segment(opts, tablet_schema, sid, tablet_schema, datas[sid].size(), generator,
                          &segment, segment_footer_cache_test_segment_dir);
        }

        return segments;
    }

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(
                segment_footer_cache_test_segment_dir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(segment_footer_cache_test_segment_dir);
        ASSERT_TRUE(st.ok()) << st;
        ExecEnv::GetInstance()->set_storage_engine(
                std::make_unique<StorageEngine>(EngineOptions {}));
        _segments = create(2, 10, 2, false, 1, 4933, 1, 3);
    }

    void TearDown() override {
        _segments.clear();
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        EXPECT_TRUE(io::global_local_filesystem()
                            ->delete_directory(segment_footer_cache_test_segment_dir)
                            .ok());
    }

private:
    Segments _segments;
};

namespace {

// Under BE_TEST, CacheManager::register_cache() replaces same-type entries and
// unregister_cache() is a no-op, so constructing any StoragePageCache hijacks the
// CacheManager registrations and leaves them dangling after that cache is
// destroyed. This restores the current global cache's registrations (its policy
// members are private; BE UT builds with -fno-access-control).
void reregister_global_storage_page_cache_policies() {
    auto* global_cache = ExecEnv::GetInstance()->get_storage_page_cache();
    auto* cache_manager = CacheManager::instance();
    cache_manager->register_cache(global_cache->_data_page_cache.get());
    cache_manager->register_cache(global_cache->_index_page_cache.get());
    cache_manager->register_cache(global_cache->_pk_index_page_cache.get());
}

// Restores the global cache's CacheManager registrations on destruction, including
// on ASSERT failure (see reregister_global_storage_page_cache_policies()).
class ScopedGlobalCachePolicyRegistration {
public:
    ScopedGlobalCachePolicyRegistration() = default;
    ~ScopedGlobalCachePolicyRegistration() { reregister_global_storage_page_cache_policies(); }

    ScopedGlobalCachePolicyRegistration(const ScopedGlobalCachePolicyRegistration&) = delete;
    ScopedGlobalCachePolicyRegistration& operator=(const ScopedGlobalCachePolicyRegistration&) =
            delete;
};

// Swaps the global storage page cache for the lifetime of the guard, so a test can
// run against a cache with controlled capacity. The destructor restores the
// previous global cache and its CacheManager registrations (see
// reregister_global_storage_page_cache_policies()).
class ScopedStoragePageCache {
public:
    explicit ScopedStoragePageCache(StoragePageCache* cache)
            : _original(ExecEnv::GetInstance()->get_storage_page_cache()) {
        DCHECK(_original != nullptr);
        ExecEnv::GetInstance()->set_storage_page_cache(cache);
    }
    ~ScopedStoragePageCache() {
        ExecEnv::GetInstance()->set_storage_page_cache(_original);
        reregister_global_storage_page_cache_policies();
    }

    ScopedStoragePageCache(const ScopedStoragePageCache&) = delete;
    ScopedStoragePageCache& operator=(const ScopedStoragePageCache&) = delete;

private:
    StoragePageCache* _original;
};

// Enables SyncPoint processing for the lifetime of the guard; without it, an
// ASSERT failure would leave processing enabled for subsequent tests.
class ScopedSyncPointProcessing {
public:
    ScopedSyncPointProcessing() { SyncPoint::get_instance()->enable_processing(); }
    ~ScopedSyncPointProcessing() { SyncPoint::get_instance()->disable_processing(); }

    ScopedSyncPointProcessing(const ScopedSyncPointProcessing&) = delete;
    ScopedSyncPointProcessing& operator=(const ScopedSyncPointProcessing&) = delete;
};

} // namespace

TEST_F(SegmentFooterCacheTest, TestGetSegmentFooter) {
    for (auto segment_ptr : _segments) {
        std::shared_ptr<segment_v2::SegmentFooterPB> footer;
        Status st = segment_ptr->_get_segment_footer(footer, nullptr);
        ASSERT_TRUE(st.ok());
    }

    for (auto segment_ptr : _segments) {
        std::shared_ptr<segment_v2::SegmentFooterPB> footer;
        Status st = segment_ptr->_get_segment_footer(footer, nullptr);
        ASSERT_TRUE(st.ok());
    }
}

// The segment footer is owned by the index page cache and every access goes through the cache
// lookup: a miss re-parses the footer and re-inserts it, while other threads keep reading it
// (variant sub-path readers call _get_segment_footer() once per path). A similar concurrent
// eviction + access workload exposed a heap-use-after-free in the old weak_ptr implementation
// (#69811); this test checks that every caller gets a valid, identical footer regardless of
// concurrent evictions.
TEST_F(SegmentFooterCacheTest, ConcurrentGetSegmentFooterWithEviction) {
    auto segment_ptr = _segments.front();
    auto* segment_footer_cache = ExecEnv::GetInstance()->get_storage_page_cache();
    ASSERT_NE(segment_footer_cache, nullptr);
    const auto cache_key = segment_ptr->get_segment_footer_cache_key();

    std::shared_ptr<segment_v2::SegmentFooterPB> baseline;
    ASSERT_TRUE(segment_ptr->_get_segment_footer(baseline, nullptr).ok());
    ASSERT_NE(baseline, nullptr);
    const auto expected_num_rows = baseline->num_rows();
    const auto expected_size = baseline->ByteSizeLong();
    baseline.reset();

    constexpr int kThreads = 8;
    constexpr int kIterations = 2000;
    std::atomic<int> failures {0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            // Reading the footer from the file needs the thread context a scanner thread has.
            SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->orphan_mem_tracker());
            for (int i = 0; i < kIterations; ++i) {
                if (t % 2 == 0) {
                    // simulate the LRU eviction of the footer page
                    segment_footer_cache->erase(cache_key, segment_v2::PageTypePB::INDEX_PAGE);
                }
                std::shared_ptr<segment_v2::SegmentFooterPB> footer;
                Status st = segment_ptr->_get_segment_footer(footer, nullptr);
                if (!st.ok() || footer == nullptr || footer->num_rows() != expected_num_rows ||
                    footer->ByteSizeLong() != expected_size) {
                    ++failures;
                }
            }
        });
    }
    for (auto& th : threads) {
        th.join();
    }
    EXPECT_EQ(failures.load(), 0);
}

// Every footer access goes through StoragePageCache, so repeated accesses must
// keep the footer page recent in the LRU order. The test runs against a
// single-shard index cache that fits exactly two footer pages plus half a page of
// slack: after A and B are loaded, A is re-accessed (refreshing its recency,
// served from the cache), and a third footer-sized page is inserted. The insertion
// must evict B — the least recently used entry — and keep A, proving that the
// repeated access refreshed A's position.
TEST_F(SegmentFooterCacheTest, RepeatedFooterAccessesRefreshLruOrder) {
    auto segment_a = _segments[0];
    auto segment_b = _segments[1];
    const auto key_a = segment_a->get_segment_footer_cache_key();
    const auto key_b = segment_b->get_segment_footer_cache_key();
    const StoragePageCache::CacheKey pressure_key("footer_lru_pressure_test", 0, 0);

    // Measure the cache space (charge + handle overhead) of the entries involved:
    // footer A, footers A+B, and the pressure page. The pressure footer object is
    // reused in the scenario phase, so its charge is identical by construction.
    int64_t usage_a = 0;
    int64_t usage_ab = 0;
    int64_t usage_abp = 0;
    std::shared_ptr<segment_v2::SegmentFooterPB> pressure_footer;
    {
        StoragePageCache measure_cache(1 << 20, 100, 0, 1);
        ScopedStoragePageCache scoped_cache(&measure_cache);
        std::shared_ptr<segment_v2::SegmentFooterPB> footer;
        ASSERT_TRUE(segment_a->_get_segment_footer(footer, nullptr).ok());
        usage_a = measure_cache._get_page_cache(segment_v2::PageTypePB::INDEX_PAGE)->get_usage();
        ASSERT_TRUE(segment_b->_get_segment_footer(footer, nullptr).ok());
        usage_ab = measure_cache._get_page_cache(segment_v2::PageTypePB::INDEX_PAGE)->get_usage();
        pressure_footer = std::make_shared<segment_v2::SegmentFooterPB>(*footer);
        {
            PageCacheHandle handle;
            measure_cache.insert(pressure_key, pressure_footer, pressure_footer->ByteSizeLong(),
                                 &handle, segment_v2::PageTypePB::INDEX_PAGE);
        }
        usage_abp = measure_cache._get_page_cache(segment_v2::PageTypePB::INDEX_PAGE)->get_usage();
        ASSERT_GT(usage_ab, usage_a);
        ASSERT_GT(usage_abp, usage_ab);
    }
    const int64_t weight_a = usage_a;
    const int64_t weight_b = usage_ab - usage_a;
    const int64_t weight_pressure = usage_abp - usage_ab;

    // Capacity for exactly two footer pages plus half of A's page as slack. The
    // slack guarantees — verified here explicitly — that the pressure insert both
    // exceeds the capacity (it must evict) and fits again after evicting exactly
    // one entry (it must evict no more than the least recently used page).
    const int64_t capacity = usage_ab + weight_a / 2;
    ASSERT_GT(weight_a + weight_b + weight_pressure, capacity);
    ASSERT_LE(weight_a + weight_pressure, capacity);

    StoragePageCache small_cache(capacity, 100, 0, 1);
    ScopedStoragePageCache scoped_cache(&small_cache);

    ScopedSyncPointProcessing scoped_sync_point;
    int parse_count = 0;
    SyncPoint::CallbackGuard guard;
    SyncPoint::get_instance()->set_call_back(
            "Segment::_parse_footer::io_ctx", [&](auto&&) { ++parse_count; }, &guard);

    std::shared_ptr<segment_v2::SegmentFooterPB> footer;

    // Load A and B: both are cache misses and are parsed; A is now the least
    // recently used footer page.
    ASSERT_TRUE(segment_a->_get_segment_footer(footer, nullptr).ok());
    ASSERT_TRUE(segment_b->_get_segment_footer(footer, nullptr).ok());
    ASSERT_EQ(parse_count, 2);

    // Repeated access of A: served from the cache (no additional parse) and
    // refreshes A as the most recently used footer page.
    ASSERT_TRUE(segment_a->_get_segment_footer(footer, nullptr).ok());
    ASSERT_NE(footer, nullptr);
    ASSERT_EQ(parse_count, 2);

    // Insert the third footer-sized index page: it does not fit, so the cache
    // evicts the least recently used page, which must be B — not the re-accessed A.
    {
        PageCacheHandle pressure_handle;
        small_cache.insert(pressure_key, pressure_footer, pressure_footer->ByteSizeLong(),
                           &pressure_handle, segment_v2::PageTypePB::INDEX_PAGE);
    }

    {
        PageCacheHandle probe;
        EXPECT_TRUE(small_cache.lookup(key_a, &probe, segment_v2::PageTypePB::INDEX_PAGE));
    }
    {
        PageCacheHandle probe;
        EXPECT_FALSE(small_cache.lookup(key_b, &probe, segment_v2::PageTypePB::INDEX_PAGE));
    }
    {
        PageCacheHandle probe;
        EXPECT_TRUE(small_cache.lookup(pressure_key, &probe, segment_v2::PageTypePB::INDEX_PAGE));
    }
    ASSERT_EQ(parse_count, 2);
}

// VariantExternalMetaReader keeps the footer shared_ptr returned by
// _get_segment_footer() alive for the reader's whole lifetime. This test pins the
// lifetime and cache-accounting semantics under eviction: after the cached entry
// is dropped, the held copy stays valid; the next access re-parses the footer
// (counted via sync point) into a footer that is valid and content-identical; and
// the index cache tracker is charged exactly while the entry is cached (footer
// bytes plus entry overhead), uncharged at eviction, re-charged identically at
// reload, and unaffected by holders that outlive the entry. This verifies
// cache-tracker bookkeeping and the retained footer's validity, not actual heap
// deallocation.
TEST_F(SegmentFooterCacheTest, HeldFooterStaysValidAcrossCacheEvictionAndReload) {
    auto* segment_footer_cache = ExecEnv::GetInstance()->get_storage_page_cache();
    ASSERT_NE(segment_footer_cache, nullptr);
    auto segment_ptr = _segments.front();
    const auto cache_key = segment_ptr->get_segment_footer_cache_key();
    const auto tracker = segment_footer_cache->mem_tracker(segment_v2::PageTypePB::INDEX_PAGE);

    // Drop whatever Segment::open() during SetUp may have left in the cache.
    segment_footer_cache->erase(cache_key, segment_v2::PageTypePB::INDEX_PAGE);

    ScopedSyncPointProcessing scoped_sync_point;
    int parse_count = 0;
    SyncPoint::CallbackGuard guard;
    SyncPoint::get_instance()->set_call_back(
            "Segment::_parse_footer::io_ctx", [&](auto&&) { ++parse_count; }, &guard);

    // First access: the footer is parsed; the cache charges the entry — the
    // footer's own bytes plus the cache entry overhead (handle + key).
    const int64_t usage_before = tracker->consumption();
    std::shared_ptr<segment_v2::SegmentFooterPB> held;
    ASSERT_TRUE(segment_ptr->_get_segment_footer(held, nullptr).ok());
    ASSERT_NE(held, nullptr);
    ASSERT_EQ(parse_count, 1);
    const auto num_rows = held->num_rows();
    const auto byte_size = held->ByteSizeLong();
    const int64_t tracked_bytes = tracker->consumption() - usage_before;
    ASSERT_GT(tracked_bytes, static_cast<int64_t>(byte_size));

    // Evict the entry: the cache uncharges it while the held copy keeps the footer
    // object alive (the reader's lifetime guarantee).
    segment_footer_cache->erase(cache_key, segment_v2::PageTypePB::INDEX_PAGE);
    EXPECT_EQ(tracker->consumption(), usage_before);

    // The next access re-parses the footer (a new parse is counted) and charges
    // the same measured amount again. Whether the reloaded footer is the same
    // object as the held one is an implementation detail; what matters is that it
    // is valid and content-identical.
    std::shared_ptr<segment_v2::SegmentFooterPB> reloaded;
    ASSERT_TRUE(segment_ptr->_get_segment_footer(reloaded, nullptr).ok());
    ASSERT_NE(reloaded, nullptr);
    ASSERT_EQ(parse_count, 2);
    EXPECT_EQ(tracker->consumption() - usage_before, tracked_bytes);
    EXPECT_EQ(reloaded->num_rows(), num_rows);
    EXPECT_EQ(reloaded->ByteSizeLong(), byte_size);
    EXPECT_EQ(held->SerializeAsString(), reloaded->SerializeAsString());

    // The held copy stays valid across the eviction and the reload.
    EXPECT_EQ(held->num_rows(), num_rows);
    EXPECT_EQ(held->ByteSizeLong(), byte_size);

    // Releasing the held copy frees that reference without touching the cache's
    // accounting (the cached copy stays charged).
    held.reset();
    EXPECT_EQ(tracker->consumption() - usage_before, tracked_bytes);
    ASSERT_EQ(parse_count, 2);
}

TEST_F(SegmentFooterCacheTest, GetSegmentFooterPropagatesIoContext) {
    auto segment_ptr = _segments.front();
    auto* segment_footer_cache = ExecEnv::GetInstance()->get_storage_page_cache();
    ASSERT_NE(segment_footer_cache, nullptr);
    segment_footer_cache->erase(segment_ptr->get_segment_footer_cache_key(),
                                segment_v2::PageTypePB::INDEX_PAGE);

    TUniqueId query_id;
    query_id.hi = 100;
    query_id.lo = 200;
    io::RemoteScanCacheWriteLimiter limiter(query_id, 1024);
    OlapReaderStatistics stats;
    io::IOContext io_ctx;
    io_ctx.reader_type = ReaderType::READER_QUERY;
    io_ctx.query_id = &query_id;
    io_ctx.file_cache_miss_policy = io::FileCacheMissPolicy::REMOTE_ONLY_ON_MISS;
    io_ctx.remote_scan_cache_write_limiter = &limiter;

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    bool observed_parse_footer_io_ctx = false;
    SyncPoint::CallbackGuard guard;
    sp->set_call_back(
            "Segment::_parse_footer::io_ctx",
            [&](auto&& args) {
                auto* actual = try_any_cast<io::IOContext*>(args[0]);
                observed_parse_footer_io_ctx = true;
                EXPECT_EQ(actual->reader_type, ReaderType::READER_QUERY);
                EXPECT_EQ(actual->query_id, &query_id);
                EXPECT_EQ(actual->file_cache_stats, &stats.file_cache_stats);
                EXPECT_EQ(actual->file_cache_miss_policy,
                          io::FileCacheMissPolicy::REMOTE_ONLY_ON_MISS);
                EXPECT_EQ(actual->remote_scan_cache_write_limiter, &limiter);
                EXPECT_TRUE(actual->is_index_data);
                EXPECT_FALSE(actual->is_inverted_index);
            },
            &guard);

    std::shared_ptr<segment_v2::SegmentFooterPB> footer;
    Status st = segment_ptr->_get_segment_footer(footer, &stats, &io_ctx);
    sp->disable_processing();

    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_NE(footer, nullptr);
    EXPECT_TRUE(observed_parse_footer_io_ctx);
}

TEST_F(SegmentFooterCacheTest, OpenPropagatesIoContextToFooter) {
    auto segment_ptr = _segments.front();
    auto* segment_footer_cache = ExecEnv::GetInstance()->get_storage_page_cache();
    ASSERT_NE(segment_footer_cache, nullptr);
    segment_footer_cache->erase(segment_ptr->get_segment_footer_cache_key(),
                                segment_v2::PageTypePB::INDEX_PAGE);

    TUniqueId query_id;
    query_id.hi = 300;
    query_id.lo = 400;
    io::RemoteScanCacheWriteLimiter limiter(query_id, 1024);
    OlapReaderStatistics stats;
    io::IOContext io_ctx;
    io_ctx.reader_type = ReaderType::READER_QUERY;
    io_ctx.query_id = &query_id;
    io_ctx.file_cache_miss_policy = io::FileCacheMissPolicy::REMOTE_ONLY_ON_MISS;
    io_ctx.remote_scan_cache_write_limiter = &limiter;

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    bool observed_parse_footer_io_ctx = false;
    SyncPoint::CallbackGuard guard;
    sp->set_call_back(
            "Segment::_parse_footer::io_ctx",
            [&](auto&& args) {
                auto* actual = try_any_cast<io::IOContext*>(args[0]);
                observed_parse_footer_io_ctx = true;
                EXPECT_EQ(actual->reader_type, ReaderType::READER_QUERY);
                EXPECT_EQ(actual->query_id, &query_id);
                EXPECT_EQ(actual->file_cache_stats, &stats.file_cache_stats);
                EXPECT_EQ(actual->file_cache_miss_policy,
                          io::FileCacheMissPolicy::REMOTE_ONLY_ON_MISS);
                EXPECT_EQ(actual->remote_scan_cache_write_limiter, &limiter);
                EXPECT_TRUE(actual->is_index_data);
                EXPECT_FALSE(actual->is_inverted_index);
            },
            &guard);

    std::shared_ptr<segment_v2::Segment> opened_segment;
    io::FileReaderOptions reader_options;
    reader_options.file_size = segment_ptr->file_reader()->size();
    auto st = segment_v2::Segment::open(
            io::global_local_filesystem(), segment_ptr->file_reader()->path().native(),
            /*tablet_id=*/1, segment_ptr->id(), segment_ptr->rowset_id(),
            segment_ptr->tablet_schema(), reader_options, &opened_segment, {}, &stats, &io_ctx);
    sp->disable_processing();

    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_NE(opened_segment, nullptr);
    EXPECT_TRUE(observed_parse_footer_io_ctx);
}

TEST_F(SegmentFooterCacheTest, TestGetSegmentFooterCacheKey) {
    for (auto segment_ptr : _segments) {
        StoragePageCache::CacheKey cache_key = segment_ptr->get_segment_footer_cache_key();
        std::string path_native = segment_ptr->_file_reader->path().native();
        size_t fsize = segment_ptr->_file_reader->size();
        size_t offset = fsize - 12;
        std::cout << "cache_key: " << cache_key.encode() << std::endl;
        ASSERT_EQ(path_native, cache_key.fname);
        ASSERT_EQ(fsize, cache_key.fsize);
        ASSERT_EQ(offset, cache_key.offset);
    }
}

TEST_F(SegmentFooterCacheTest, TestSemgnetFooterPBPage) {
    // The local cache below registers its policies with CacheManager (BE_TEST
    // unregister is a no-op); restore the global cache's registrations on exit,
    // including on ASSERT failure.
    ScopedGlobalCachePolicyRegistration registration_guard;
    StoragePageCache cache(16 * 2048, 0, 0, 16);
    for (auto segment_ptr : _segments) {
        std::shared_ptr<segment_v2::SegmentFooterPB> footer;
        Status st = segment_ptr->_get_segment_footer(footer, nullptr);
        ASSERT_TRUE(st.ok());
        PageCacheHandle cache_handle;
        cache.insert(segment_ptr->get_segment_footer_cache_key(), footer, footer->ByteSizeLong(),
                     &cache_handle, segment_v2::PageTypePB::DATA_PAGE);

        EXPECT_EQ(cache_handle.get<std::shared_ptr<segment_v2::SegmentFooterPB>>(), footer);
        auto found = cache.lookup(segment_ptr->get_segment_footer_cache_key(), &cache_handle,
                                  segment_v2::PageTypePB::DATA_PAGE);
        ASSERT_TRUE(found);
    }
}

} // namespace doris
