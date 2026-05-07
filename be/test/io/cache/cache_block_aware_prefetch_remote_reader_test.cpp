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

#include "io/cache/cache_block_aware_prefetch_remote_reader.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "io/cache/block_file_cache_test_common.h"
#include "runtime/exec_env.h"
#include "storage/index/ordinal_page_index.h"
#include "storage/segment/page_pointer.h"
#include "storage/segment/segment_file_access_range_builder.h"
#include "util/defer_op.h"
#include "util/threadpool.h"

namespace doris::io {

using segment_v2::rowid_t;

class CountingRemoteFileReader final : public FileReader {
public:
    CountingRemoteFileReader(Path path, size_t size) : _path(std::move(path)), _size(size) {}

    Status close() override {
        _closed = true;
        return Status::OK();
    }

    const Path& path() const override { return _path; }

    size_t size() const override { return _size; }

    bool closed() const override { return _closed; }

    int64_t mtime() const override { return 0; }

    std::vector<CacheBlockRange> read_ranges() const {
        std::lock_guard lock(_mutex);
        return _read_ranges;
    }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override {
        std::lock_guard lock(_mutex);
        _read_ranges.push_back({offset, result.size});
        if (offset >= _size) {
            *bytes_read = 0;
            return Status::OK();
        }
        *bytes_read = std::min(result.size, _size - offset);
        for (size_t i = 0; i < *bytes_read; ++i) {
            result.data[i] = static_cast<char>('a' + ((offset + i) % 26));
        }
        return Status::OK();
    }

private:
    Path _path;
    size_t _size;
    bool _closed = false;
    mutable std::mutex _mutex;
    std::vector<CacheBlockRange> _read_ranges;
};

bool wait_until(const std::function<bool()>& pred) {
    for (int i = 0; i < 200; ++i) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

std::unique_ptr<segment_v2::OrdinalIndexReader> make_ordinal_index_for_prefetch_test() {
    auto reader = std::make_shared<CountingRemoteFileReader>(Path("/tmp/ordinal_index_test"), 512);
    auto ordinal_index = std::make_unique<segment_v2::OrdinalIndexReader>(
            reader, 400, segment_v2::OrdinalIndexPB());
    ordinal_index->_num_pages = 4;
    ordinal_index->_ordinals = {0, 100, 200, 300, 400};
    ordinal_index->_pages = {
            segment_v2::PagePointer(0, 50),
            segment_v2::PagePointer(100, 50),
            segment_v2::PagePointer(200, 50),
            segment_v2::PagePointer(300, 50),
    };
    return ordinal_index;
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest, segment_file_access_range_builder_from_rowids) {
    auto ordinal_index = make_ordinal_index_for_prefetch_test();
    segment_v2::SegmentFileAccessRangeBuilder builder(ordinal_index.get(), true);
    std::vector<rowid_t> rowids {5, 30, 115, 250};
    builder.add_rowids(rowids.data(), static_cast<uint32_t>(rowids.size()));

    auto ranges = builder.finish_by_rowids();

    ASSERT_EQ(ranges.size(), 3);
    EXPECT_EQ(ranges[0].offset, 0);
    EXPECT_EQ(ranges[0].size, 50);
    EXPECT_EQ(ranges[1].offset, 100);
    EXPECT_EQ(ranges[2].offset, 200);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest,
     segment_file_access_range_builder_from_rowids_backward) {
    auto ordinal_index = make_ordinal_index_for_prefetch_test();
    segment_v2::SegmentFileAccessRangeBuilder builder(ordinal_index.get(), false);
    std::vector<rowid_t> rowids {5, 30, 115, 250};
    builder.add_rowids(rowids.data(), static_cast<uint32_t>(rowids.size()));

    auto ranges = builder.finish_by_rowids();

    ASSERT_EQ(ranges.size(), 3);
    EXPECT_EQ(ranges[0].offset, 200);
    EXPECT_EQ(ranges[1].offset, 100);
    EXPECT_EQ(ranges[2].offset, 0);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest, segment_file_access_range_builder_all_data) {
    auto ordinal_index = make_ordinal_index_for_prefetch_test();
    segment_v2::SegmentFileAccessRangeBuilder builder(ordinal_index.get(), false);

    auto ranges = builder.build_all_data_ranges();

    ASSERT_EQ(ranges.size(), 4);
    EXPECT_EQ(ranges[0].offset, 300);
    EXPECT_EQ(ranges[1].offset, 200);
    EXPECT_EQ(ranges[2].offset, 100);
    EXPECT_EQ(ranges[3].offset, 0);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest,
     segment_file_access_range_builder_preserves_large_page_range) {
    auto reader =
            std::make_shared<CountingRemoteFileReader>(Path("/tmp/large_page_ordinal_index"), 512);
    auto ordinal_index = std::make_unique<segment_v2::OrdinalIndexReader>(
            reader, 200, segment_v2::OrdinalIndexPB());
    ordinal_index->_num_pages = 2;
    ordinal_index->_ordinals = {0, 100, 200};
    ordinal_index->_pages = {
            segment_v2::PagePointer(50, 260),
            segment_v2::PagePointer(400, 40),
    };
    segment_v2::SegmentFileAccessRangeBuilder builder(ordinal_index.get(), true);
    std::vector<rowid_t> rowids {10, 120};
    builder.add_rowids(rowids.data(), static_cast<uint32_t>(rowids.size()));

    auto ranges = builder.finish_by_rowids();

    ASSERT_EQ(ranges.size(), 2);
    EXPECT_EQ(ranges[0].offset, 50);
    EXPECT_EQ(ranges[0].size, 260);
    EXPECT_EQ(ranges[1].offset, 400);
    EXPECT_EQ(ranges[1].size, 40);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest, build_block_sequence_forward) {
    CacheBlockReadPattern pattern {
            .direction = CacheBlockReadDirection::FORWARD,
            .ranges =
                    {
                            {.offset = 205, .size = 30},
                            {.offset = 0, .size = 90},
                            {.offset = 90, .size = 40},
                            {.offset = 310, .size = 10},
                    },
    };

    auto sequence = CacheBlockAwarePrefetchRemoteReader::_build_block_sequence(pattern, 100);

    ASSERT_EQ(sequence.size(), 4);
    EXPECT_EQ(sequence[0].block_id, 0);
    EXPECT_EQ(sequence[0].trigger_offset, 0);
    EXPECT_EQ(sequence[1].block_id, 1);
    EXPECT_EQ(sequence[1].trigger_offset, 90);
    EXPECT_EQ(sequence[2].block_id, 2);
    EXPECT_EQ(sequence[2].trigger_offset, 205);
    EXPECT_EQ(sequence[3].block_id, 3);
    EXPECT_EQ(sequence[3].trigger_offset, 310);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest, build_block_sequence_backward) {
    CacheBlockReadPattern pattern {
            .direction = CacheBlockReadDirection::BACKWARD,
            .ranges =
                    {
                            {.offset = 0, .size = 20},
                            {.offset = 250, .size = 80},
                            {.offset = 90, .size = 120},
                    },
    };

    auto sequence = CacheBlockAwarePrefetchRemoteReader::_build_block_sequence(pattern, 100);

    ASSERT_EQ(sequence.size(), 4);
    EXPECT_EQ(sequence[0].block_id, 3);
    EXPECT_EQ(sequence[0].trigger_offset, 250);
    EXPECT_EQ(sequence[1].block_id, 2);
    EXPECT_EQ(sequence[1].trigger_offset, 250);
    EXPECT_EQ(sequence[2].block_id, 1);
    EXPECT_EQ(sequence[2].trigger_offset, 90);
    EXPECT_EQ(sequence[3].block_id, 0);
    EXPECT_EQ(sequence[3].trigger_offset, 90);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest,
     build_block_sequence_expands_cross_block_ranges_and_ignores_duplicates) {
    CacheBlockReadPattern pattern {
            .direction = CacheBlockReadDirection::FORWARD,
            .ranges =
                    {
                            {.offset = 50, .size = 260},
                            {.offset = 100, .size = 100},
                            {.offset = 400, .size = 100},
                            {.offset = 500, .size = 0},
                    },
    };

    auto sequence = CacheBlockAwarePrefetchRemoteReader::_build_block_sequence(pattern, 100);

    ASSERT_EQ(sequence.size(), 5);
    EXPECT_EQ(sequence[0].block_id, 0);
    EXPECT_EQ(sequence[0].trigger_offset, 50);
    EXPECT_EQ(sequence[1].block_id, 1);
    EXPECT_EQ(sequence[1].trigger_offset, 50);
    EXPECT_EQ(sequence[2].block_id, 2);
    EXPECT_EQ(sequence[2].trigger_offset, 50);
    EXPECT_EQ(sequence[3].block_id, 3);
    EXPECT_EQ(sequence[3].trigger_offset, 50);
    EXPECT_EQ(sequence[4].block_id, 4);
    EXPECT_EQ(sequence[4].trigger_offset, 400);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest, build_block_sequence_backward_cross_block_range) {
    CacheBlockReadPattern pattern {
            .direction = CacheBlockReadDirection::BACKWARD,
            .ranges =
                    {
                            {.offset = 50, .size = 260},
                            {.offset = 0, .size = 50},
                    },
    };

    auto sequence = CacheBlockAwarePrefetchRemoteReader::_build_block_sequence(pattern, 100);

    ASSERT_EQ(sequence.size(), 4);
    EXPECT_EQ(sequence[0].block_id, 3);
    EXPECT_EQ(sequence[0].trigger_offset, 50);
    EXPECT_EQ(sequence[1].block_id, 2);
    EXPECT_EQ(sequence[1].trigger_offset, 50);
    EXPECT_EQ(sequence[2].block_id, 1);
    EXPECT_EQ(sequence[2].trigger_offset, 50);
    EXPECT_EQ(sequence[3].block_id, 0);
    EXPECT_EQ(sequence[3].trigger_offset, 50);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest, prefetch_window_advances_without_duplicates) {
    CacheBlockAwarePrefetchRemoteReader::ReadPatternState state {
            .direction = CacheBlockReadDirection::FORWARD,
            .policy = {.max_prefetch_blocks = 2, .cache_block_size = 100},
            .block_sequence =
                    {
                            {.block_id = 0, .trigger_offset = 0},
                            {.block_id = 1, .trigger_offset = 100},
                            {.block_id = 2, .trigger_offset = 200},
                            {.block_id = 3, .trigger_offset = 300},
                    },
    };

    auto ranges = CacheBlockAwarePrefetchRemoteReader::_next_prefetch_ranges(&state, 0);
    ASSERT_EQ(ranges.size(), 2);
    EXPECT_EQ(ranges[0].offset, 0);
    EXPECT_EQ(ranges[1].offset, 100);

    ranges = CacheBlockAwarePrefetchRemoteReader::_next_prefetch_ranges(&state, 0);
    EXPECT_TRUE(ranges.empty());

    ranges = CacheBlockAwarePrefetchRemoteReader::_next_prefetch_ranges(&state, 100);
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges[0].offset, 200);

    ranges = CacheBlockAwarePrefetchRemoteReader::_next_prefetch_ranges(&state, 300);
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges[0].offset, 300);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest, prefetch_window_does_not_split_large_file_range) {
    CacheBlockAwarePrefetchRemoteReader::ReadPatternState state {
            .direction = CacheBlockReadDirection::FORWARD,
            .policy = {.max_prefetch_blocks = 2, .cache_block_size = 100},
            .block_sequence =
                    {
                            {.block_id = 0, .trigger_offset = 50},
                            {.block_id = 1, .trigger_offset = 50},
                            {.block_id = 2, .trigger_offset = 50},
                            {.block_id = 3, .trigger_offset = 50},
                            {.block_id = 4, .trigger_offset = 400},
                    },
    };

    auto ranges = CacheBlockAwarePrefetchRemoteReader::_next_prefetch_ranges(&state, 50);
    ASSERT_EQ(ranges.size(), 4);
    EXPECT_EQ(ranges[0].offset, 0);
    EXPECT_EQ(ranges[1].offset, 100);
    EXPECT_EQ(ranges[2].offset, 200);
    EXPECT_EQ(ranges[3].offset, 300);

    ranges = CacheBlockAwarePrefetchRemoteReader::_next_prefetch_ranges(&state, 400);
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges[0].offset, 400);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest,
     backward_prefetch_window_advances_without_duplicates) {
    CacheBlockAwarePrefetchRemoteReader::ReadPatternState state {
            .direction = CacheBlockReadDirection::BACKWARD,
            .policy = {.max_prefetch_blocks = 2, .cache_block_size = 100},
            .block_sequence =
                    {
                            {.block_id = 3, .trigger_offset = 300},
                            {.block_id = 2, .trigger_offset = 200},
                            {.block_id = 1, .trigger_offset = 100},
                            {.block_id = 0, .trigger_offset = 0},
                    },
    };

    auto ranges = CacheBlockAwarePrefetchRemoteReader::_next_prefetch_ranges(&state, 300);
    ASSERT_EQ(ranges.size(), 2);
    EXPECT_EQ(ranges[0].offset, 300);
    EXPECT_EQ(ranges[1].offset, 200);

    ranges = CacheBlockAwarePrefetchRemoteReader::_next_prefetch_ranges(&state, 200);
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges[0].offset, 100);

    ranges = CacheBlockAwarePrefetchRemoteReader::_next_prefetch_ranges(&state, 0);
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges[0].offset, 0);
}

TEST_F(BlockFileCacheTest, usage_example_registers_independent_column_patterns) {
    std::string test_cache_base_path = caches_dir / "cache_block_prefetch_usage_example" / "";
    auto cleanup = [&] {
        if (fs::exists(test_cache_base_path)) {
            fs::remove_all(test_cache_base_path);
        }
        FileCacheFactory::instance()->_caches.clear();
        FileCacheFactory::instance()->_path_to_cache.clear();
        FileCacheFactory::instance()->_capacity = 0;
    };
    cleanup();
    Defer defer {cleanup};

    fs::create_directories(test_cache_base_path);
    FileCacheSettings settings;
    settings.query_queue_size = 8_mb;
    settings.query_queue_elements = 8;
    settings.index_queue_size = 1_mb;
    settings.index_queue_elements = 1;
    settings.disposable_queue_size = 1_mb;
    settings.disposable_queue_elements = 1;
    settings.capacity = 10_mb;
    settings.max_file_block_size = config::file_cache_each_block_size;
    settings.max_query_cache_size = 0;
    ASSERT_TRUE(
            FileCacheFactory::instance()->create_file_cache(test_cache_base_path, settings).ok());
    auto cache = FileCacheFactory::instance()->_path_to_cache[test_cache_base_path];
    ASSERT_TRUE(wait_until([&] { return cache->get_async_open_success(); }));

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.tablet_id = 10086;

    auto remote_reader = std::make_shared<CountingRemoteFileReader>(
            Path("/tmp/cache_block_prefetch_usage_example"), 5_mb);
    auto shared_reader = std::make_shared<CacheBlockAwarePrefetchRemoteReader>(remote_reader, opts);
    CacheBlockPrefetchPolicy policy {
            .max_prefetch_blocks = 2,
            .cache_block_size = static_cast<size_t>(config::file_cache_each_block_size),
    };

    auto first_column_pattern = shared_reader->register_read_pattern(
            CacheBlockReadPattern {
                    .direction = CacheBlockReadDirection::FORWARD,
                    .ranges =
                            {
                                    {.offset = 0, .size = 1},
                                    {.offset = 1_mb, .size = 1},
                                    {.offset = 2_mb, .size = 1},
                            },
            },
            policy);
    ASSERT_TRUE(first_column_pattern.has_value()) << first_column_pattern.error();
    auto first_column = std::move(first_column_pattern.value());

    auto second_column_pattern = shared_reader->register_read_pattern(
            CacheBlockReadPattern {
                    .direction = CacheBlockReadDirection::FORWARD,
                    .ranges =
                            {
                                    {.offset = 3_mb, .size = 1},
                                    {.offset = 4_mb, .size = 1},
                            },
            },
            policy);
    ASSERT_TRUE(second_column_pattern.has_value()) << second_column_pattern.error();
    auto second_column = std::move(second_column_pattern.value());

    ASSERT_TRUE(first_column);
    ASSERT_TRUE(second_column);
    ASSERT_NE(first_column._id, second_column._id);

    const auto first_column_pattern_id = first_column._id;
    const auto second_column_pattern_id = second_column._id;
    first_column.prefetch(0);
    EXPECT_EQ(shared_reader->_patterns.at(first_column_pattern_id).prefetched_index, 1);
    EXPECT_EQ(shared_reader->_patterns.at(second_column_pattern_id).prefetched_index, -1);
    EXPECT_EQ(shared_reader->_patterns.at(second_column_pattern_id).current_block_index, 0);

    second_column.prefetch(3_mb);
    EXPECT_EQ(shared_reader->_patterns.at(first_column_pattern_id).prefetched_index, 1);
    EXPECT_EQ(shared_reader->_patterns.at(second_column_pattern_id).prefetched_index, 1);

    first_column.reset();
    EXPECT_FALSE(first_column);
    EXPECT_FALSE(shared_reader->_patterns.contains(first_column_pattern_id));
    EXPECT_TRUE(shared_reader->_patterns.contains(second_column_pattern_id));
}

TEST_F(BlockFileCacheTest, cache_block_aware_prefetch_remote_reader_prefetches_cache_blocks) {
    std::string test_cache_base_path = caches_dir / "cache_block_aware_prefetch_remote_reader" / "";
    auto cleanup = [&] {
        ExecEnv::GetInstance()->_segment_prefetch_thread_pool.reset();
        if (fs::exists(test_cache_base_path)) {
            fs::remove_all(test_cache_base_path);
        }
        FileCacheFactory::instance()->_caches.clear();
        FileCacheFactory::instance()->_path_to_cache.clear();
        FileCacheFactory::instance()->_capacity = 0;
    };
    cleanup();
    Defer defer {cleanup};

    fs::create_directories(test_cache_base_path);
    FileCacheSettings settings;
    settings.query_queue_size = 8_mb;
    settings.query_queue_elements = 8;
    settings.index_queue_size = 1_mb;
    settings.index_queue_elements = 1;
    settings.disposable_queue_size = 1_mb;
    settings.disposable_queue_elements = 1;
    settings.capacity = 10_mb;
    settings.max_file_block_size = config::file_cache_each_block_size;
    settings.max_query_cache_size = 0;
    ASSERT_TRUE(
            FileCacheFactory::instance()->create_file_cache(test_cache_base_path, settings).ok());
    auto cache = FileCacheFactory::instance()->_path_to_cache[test_cache_base_path];
    ASSERT_TRUE(wait_until([&] { return cache->get_async_open_success(); }));

    auto raw_reader =
            std::make_shared<CountingRemoteFileReader>(Path("/tmp/create_cached_reader"), 1024);
    FileReaderOptions cached_reader_opts;
    cached_reader_opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    cached_reader_opts.is_doris_table = true;
    cached_reader_opts.tablet_id = 10086;

    auto cached_reader = create_cached_file_reader(raw_reader, cached_reader_opts);
    ASSERT_TRUE(cached_reader.has_value());
    EXPECT_NE(dynamic_cast<CachedRemoteFileReader*>(cached_reader.value().get()), nullptr);
    EXPECT_EQ(dynamic_cast<CacheBlockAwarePrefetchRemoteReader*>(cached_reader.value().get()),
              nullptr);

    cached_reader_opts.enable_cache_block_prefetch = true;
    auto prefetch_raw_reader =
            std::make_shared<CountingRemoteFileReader>(Path("/tmp/create_prefetch_reader"), 1024);
    cached_reader = create_cached_file_reader(prefetch_raw_reader, cached_reader_opts);
    ASSERT_TRUE(cached_reader.has_value());
    EXPECT_NE(dynamic_cast<CacheBlockAwarePrefetchRemoteReader*>(cached_reader.value().get()),
              nullptr);

    auto shared_reader_remote = std::make_shared<CountingRemoteFileReader>(
            Path("/tmp/cache_block_aware_prefetch_remote_reader_shared"), 4_mb);
    auto shared_reader = std::make_shared<CacheBlockAwarePrefetchRemoteReader>(shared_reader_remote,
                                                                               cached_reader_opts);
    CacheBlockPrefetchPolicy shared_policy {
            .max_prefetch_blocks = 2,
            .cache_block_size = static_cast<size_t>(config::file_cache_each_block_size),
    };
    auto first_pattern = shared_reader->register_read_pattern(
            CacheBlockReadPattern {
                    .direction = CacheBlockReadDirection::FORWARD,
                    .ranges =
                            {
                                    {.offset = 0, .size = 1},
                                    {.offset = 1_mb, .size = 1},
                                    {.offset = 2_mb, .size = 1},
                            },
            },
            shared_policy);
    ASSERT_TRUE(first_pattern.has_value()) << first_pattern.error();
    auto first_column = std::move(first_pattern.value());

    auto second_pattern = shared_reader->register_read_pattern(
            CacheBlockReadPattern {
                    .direction = CacheBlockReadDirection::FORWARD,
                    .ranges =
                            {
                                    {.offset = 3_mb, .size = 1},
                                    {.offset = 4_mb, .size = 1},
                            },
            },
            shared_policy);
    ASSERT_TRUE(second_pattern.has_value()) << second_pattern.error();
    auto second_column = std::move(second_pattern.value());
    ASSERT_NE(first_column._id, second_column._id);

    first_column.prefetch(0);
    EXPECT_EQ(shared_reader->_patterns.at(first_column._id).prefetched_index, 1);
    EXPECT_EQ(shared_reader->_patterns.at(second_column._id).prefetched_index, -1);
    EXPECT_EQ(shared_reader->_patterns.at(second_column._id).current_block_index, 0);

    second_column.prefetch(3_mb);
    EXPECT_EQ(shared_reader->_patterns.at(first_column._id).prefetched_index, 1);
    EXPECT_EQ(shared_reader->_patterns.at(second_column._id).prefetched_index, 1);

    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("CacheBlockAwarePrefetchRemoteReaderTest")
                        .set_min_threads(2)
                        .set_max_threads(4)
                        .build(&pool)
                        .ok());
    ExecEnv::GetInstance()->_segment_prefetch_thread_pool = std::move(pool);

    auto remote_reader = std::make_shared<CountingRemoteFileReader>(
            Path("/tmp/cache_block_aware_prefetch_remote_reader_file"), 4_mb);
    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.tablet_id = 10086;
    auto reader = std::make_shared<CacheBlockAwarePrefetchRemoteReader>(remote_reader, opts);

    CacheBlockReadPattern pattern {
            .direction = CacheBlockReadDirection::FORWARD,
            .ranges =
                    {
                            {.offset = 0, .size = 1},
                            {.offset = 1_mb, .size = 1},
                            {.offset = 2_mb, .size = 1},
                            {.offset = 3_mb, .size = 1},
                    },
    };
    CacheBlockPrefetchPolicy policy {
            .max_prefetch_blocks = 2,
            .cache_block_size = static_cast<size_t>(config::file_cache_each_block_size),
    };
    auto pattern_handle_result = reader->register_read_pattern(std::move(pattern), policy);
    ASSERT_TRUE(pattern_handle_result.has_value()) << pattern_handle_result.error();
    auto pattern_handle = std::move(pattern_handle_result.value());
    ASSERT_TRUE(pattern_handle);

    pattern_handle.prefetch(0);
    ASSERT_TRUE(wait_until([&] { return remote_reader->read_ranges().size() >= 2; }));
    auto read_ranges = remote_reader->read_ranges();
    std::set<size_t> offsets;
    for (const auto& range : read_ranges) {
        offsets.emplace(range.offset);
    }
    EXPECT_TRUE(offsets.contains(0));
    EXPECT_TRUE(offsets.contains(1_mb));

    pattern_handle.prefetch(1_mb);
    ASSERT_TRUE(wait_until([&] { return remote_reader->read_ranges().size() >= 3; }));
    read_ranges = remote_reader->read_ranges();
    offsets.clear();
    for (const auto& range : read_ranges) {
        offsets.emplace(range.offset);
    }
    EXPECT_TRUE(offsets.contains(2_mb));

    pattern_handle.prefetch(1_mb);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(remote_reader->read_ranges().size(), 3);

    auto key = BlockFileCache::hash("cache_block_aware_prefetch_remote_reader_file");
    CacheContext context;
    ReadStatistics stats;
    context.stats = &stats;
    context.cache_type = FileCacheType::NORMAL;
    auto holder = cache->get_or_set(key, 0, 2_mb, context);
    auto blocks = fromHolder(holder);
    ASSERT_EQ(blocks.size(), 2);
    EXPECT_EQ(blocks[0]->state(), FileBlock::State::DOWNLOADED);
    EXPECT_EQ(blocks[1]->state(), FileBlock::State::DOWNLOADED);

    pattern_handle.reset();
}

} // namespace doris::io
