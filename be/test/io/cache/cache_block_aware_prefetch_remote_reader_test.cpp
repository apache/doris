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
#include <initializer_list>
#include <mutex>
#include <set>
#include <span>
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

void expect_plan_entry(std::span<const detail::CacheBlockPrefetchPlanEntry> entries, size_t index,
                       size_t cache_block_offset, size_t trigger_offset, size_t trigger_size) {
    ASSERT_LT(index, entries.size());
    const auto& entry = entries[index];
    EXPECT_EQ(entry.cache_block_range.offset, cache_block_offset);
    EXPECT_EQ(entry.trigger_file_range.offset, trigger_offset);
    EXPECT_EQ(entry.trigger_file_range.size, trigger_size);
}

void expect_plan_cache_block_offsets(std::span<const detail::CacheBlockPrefetchPlanEntry> entries,
                                     std::initializer_list<size_t> expected_offsets) {
    ASSERT_EQ(entries.size(), expected_offsets.size());
    size_t index = 0;
    for (const size_t expected_offset : expected_offsets) {
        EXPECT_EQ(entries[index].cache_block_range.offset, expected_offset) << "index=" << index;
        ++index;
    }
}

void expect_cache_block_offsets(const std::vector<CacheBlockRange>& ranges,
                                std::initializer_list<size_t> expected_offsets) {
    ASSERT_EQ(ranges.size(), expected_offsets.size());
    size_t index = 0;
    for (const size_t expected_offset : expected_offsets) {
        EXPECT_EQ(ranges[index].offset, expected_offset) << "index=" << index;
        ++index;
    }
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest, segment_file_access_range_builder_from_rowids) {
    auto ordinal_index = make_ordinal_index_for_prefetch_test();
    segment_v2::SegmentFileAccessRangeBuilder builder(ordinal_index.get(),
                                                      CacheBlockReadDirection::FORWARD);
    std::vector<rowid_t> rowids {5, 30, 115, 250};
    builder.add_ascending_rowids(rowids);

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
    segment_v2::SegmentFileAccessRangeBuilder builder(ordinal_index.get(),
                                                      CacheBlockReadDirection::BACKWARD);
    std::vector<rowid_t> rowids {5, 30, 115, 250};
    builder.add_ascending_rowids(rowids);

    auto ranges = builder.finish_by_rowids();

    ASSERT_EQ(ranges.size(), 3);
    EXPECT_EQ(ranges[0].offset, 200);
    EXPECT_EQ(ranges[1].offset, 100);
    EXPECT_EQ(ranges[2].offset, 0);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest, segment_file_access_range_builder_all_data) {
    auto ordinal_index = make_ordinal_index_for_prefetch_test();
    segment_v2::SegmentFileAccessRangeBuilder builder(ordinal_index.get(),
                                                      CacheBlockReadDirection::BACKWARD);

    auto ranges = builder.build_all_data_page_ranges();

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
    segment_v2::SegmentFileAccessRangeBuilder builder(ordinal_index.get(),
                                                      CacheBlockReadDirection::FORWARD);
    std::vector<rowid_t> rowids {10, 120};
    builder.add_ascending_rowids(rowids);

    auto ranges = builder.finish_by_rowids();

    ASSERT_EQ(ranges.size(), 2);
    EXPECT_EQ(ranges[0].offset, 50);
    EXPECT_EQ(ranges[0].size, 260);
    EXPECT_EQ(ranges[1].offset, 400);
    EXPECT_EQ(ranges[1].size, 40);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest,
     segment_file_access_range_builder_keeps_many_sparse_pages_across_large_span) {
    auto reader = std::make_shared<CountingRemoteFileReader>(Path("/tmp/sparse_page_ordinal_index"),
                                                             128_mb);
    auto ordinal_index = std::make_unique<segment_v2::OrdinalIndexReader>(
            reader, 120, segment_v2::OrdinalIndexPB());
    ordinal_index->_num_pages = 12;
    for (rowid_t ordinal = 0; ordinal <= 120; ordinal += 10) {
        ordinal_index->_ordinals.push_back(ordinal);
    }
    ordinal_index->_pages = {
            segment_v2::PagePointer(64_kb, 32_kb),
            segment_v2::PagePointer(1_mb + 128_kb, 64_kb),
            segment_v2::PagePointer(4_mb + 900_kb, 300_kb),
            segment_v2::PagePointer(9_mb + 16_kb, 1_mb + 128_kb),
            segment_v2::PagePointer(16_mb + 512_kb, 64_kb),
            segment_v2::PagePointer(31_mb + 768_kb, 2_mb + 512_kb),
            segment_v2::PagePointer(48_mb + 128_kb, 32_kb),
            segment_v2::PagePointer(64_mb + 900_kb, 300_kb),
            segment_v2::PagePointer(80_mb + 32_kb, 64_kb),
            segment_v2::PagePointer(96_mb + 512_kb, 1_mb + 1),
            segment_v2::PagePointer(112_mb + 128_kb, 128_kb),
            segment_v2::PagePointer(127_mb, 512_kb),
    };
    std::vector<rowid_t> rowids {0, 1, 22, 35, 36, 58, 77, 78, 94, 119};

    segment_v2::SegmentFileAccessRangeBuilder forward_builder(ordinal_index.get(),
                                                              CacheBlockReadDirection::FORWARD);
    forward_builder.add_ascending_rowids(rowids);
    auto forward_ranges = forward_builder.finish_by_rowids();
    ASSERT_EQ(forward_ranges.size(), 7);
    EXPECT_EQ(forward_ranges[0].offset, 64_kb);
    EXPECT_EQ(forward_ranges[0].size, 32_kb);
    EXPECT_EQ(forward_ranges[1].offset, 4_mb + 900_kb);
    EXPECT_EQ(forward_ranges[1].size, 300_kb);
    EXPECT_EQ(forward_ranges[2].offset, 9_mb + 16_kb);
    EXPECT_EQ(forward_ranges[2].size, 1_mb + 128_kb);
    EXPECT_EQ(forward_ranges[3].offset, 31_mb + 768_kb);
    EXPECT_EQ(forward_ranges[3].size, 2_mb + 512_kb);
    EXPECT_EQ(forward_ranges[4].offset, 64_mb + 900_kb);
    EXPECT_EQ(forward_ranges[4].size, 300_kb);
    EXPECT_EQ(forward_ranges[5].offset, 96_mb + 512_kb);
    EXPECT_EQ(forward_ranges[5].size, 1_mb + 1);
    EXPECT_EQ(forward_ranges[6].offset, 127_mb);
    EXPECT_EQ(forward_ranges[6].size, 512_kb);

    segment_v2::SegmentFileAccessRangeBuilder backward_builder(ordinal_index.get(),
                                                               CacheBlockReadDirection::BACKWARD);
    backward_builder.add_ascending_rowids(rowids);
    auto backward_ranges = backward_builder.finish_by_rowids();
    ASSERT_EQ(backward_ranges.size(), forward_ranges.size());
    for (size_t i = 0; i < backward_ranges.size(); ++i) {
        EXPECT_EQ(backward_ranges[i].offset, forward_ranges[forward_ranges.size() - i - 1].offset);
        EXPECT_EQ(backward_ranges[i].size, forward_ranges[forward_ranges.size() - i - 1].size);
    }
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest,
     segment_file_access_range_builder_consumes_bitmap_for_multiple_builders) {
    auto first_ordinal_index = make_ordinal_index_for_prefetch_test();
    segment_v2::SegmentFileAccessRangeBuilder first_builder(first_ordinal_index.get(),
                                                            CacheBlockReadDirection::FORWARD);
    std::vector<rowid_t> stale_rowids {5};
    first_builder.add_ascending_rowids(stale_rowids);

    auto reader =
            std::make_shared<CountingRemoteFileReader>(Path("/tmp/second_ordinal_index"), 4096);
    auto second_ordinal_index = std::make_unique<segment_v2::OrdinalIndexReader>(
            reader, 400, segment_v2::OrdinalIndexPB());
    second_ordinal_index->_num_pages = 3;
    second_ordinal_index->_ordinals = {0, 150, 260, 400};
    second_ordinal_index->_pages = {
            segment_v2::PagePointer(1000, 50),
            segment_v2::PagePointer(2000, 50),
            segment_v2::PagePointer(3000, 50),
    };
    segment_v2::SegmentFileAccessRangeBuilder second_builder(second_ordinal_index.get(),
                                                             CacheBlockReadDirection::FORWARD);

    roaring::Roaring row_bitmap;
    row_bitmap.add(5);
    row_bitmap.add(160);
    row_bitmap.add(270);
    row_bitmap.add(350);
    std::vector<segment_v2::SegmentFileAccessRangeBuilder*> builders {
            &first_builder,
            &second_builder,
    };
    segment_v2::SegmentFileAccessRangeBuilder::add_rowids_from_bitmap(row_bitmap, builders);

    auto first_ranges = first_builder.finish_by_rowids();
    ASSERT_EQ(first_ranges.size(), 4);
    EXPECT_EQ(first_ranges[0].offset, 0);
    EXPECT_EQ(first_ranges[1].offset, 100);
    EXPECT_EQ(first_ranges[2].offset, 200);
    EXPECT_EQ(first_ranges[3].offset, 300);

    auto second_ranges = second_builder.finish_by_rowids();
    ASSERT_EQ(second_ranges.size(), 3);
    EXPECT_EQ(second_ranges[0].offset, 1000);
    EXPECT_EQ(second_ranges[1].offset, 2000);
    EXPECT_EQ(second_ranges[2].offset, 3000);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest,
     segment_file_access_ranges_drive_prefetch_plan_and_cursor) {
    auto reader = std::make_shared<CountingRemoteFileReader>(
            Path("/tmp/prefetch_chain_ordinal_index"), 512);
    auto ordinal_index = std::make_unique<segment_v2::OrdinalIndexReader>(
            reader, 200, segment_v2::OrdinalIndexPB());
    ordinal_index->_num_pages = 2;
    ordinal_index->_ordinals = {0, 100, 200};
    ordinal_index->_pages = {
            segment_v2::PagePointer(50, 260),
            segment_v2::PagePointer(400, 40),
    };
    segment_v2::SegmentFileAccessRangeBuilder builder(ordinal_index.get(),
                                                      CacheBlockReadDirection::FORWARD);
    std::vector<rowid_t> rowids {10, 120};
    builder.add_ascending_rowids(rowids);

    auto file_ranges = builder.finish_by_rowids();
    detail::CacheBlockPrefetchCursor cursor {
            detail::CacheBlockPrefetchPlan::from_read_pattern(
                    CacheBlockReadPattern {
                            .direction = CacheBlockReadDirection::FORWARD,
                            .ranges = std::move(file_ranges),
                    },
                    100),
            2};

    auto cache_block_ranges = cursor.next_touch_ranges(50);
    ASSERT_EQ(cache_block_ranges.size(), 4);
    EXPECT_EQ(cache_block_ranges[0].offset, 0);
    EXPECT_EQ(cache_block_ranges[1].offset, 100);
    EXPECT_EQ(cache_block_ranges[2].offset, 200);
    EXPECT_EQ(cache_block_ranges[3].offset, 300);

    cache_block_ranges = cursor.next_touch_ranges(400);
    ASSERT_EQ(cache_block_ranges.size(), 1);
    EXPECT_EQ(cache_block_ranges[0].offset, 400);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest, build_prefetch_plan_forward) {
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

    auto plan = detail::CacheBlockPrefetchPlan::from_read_pattern(pattern, 100);
    const auto entries = plan.entries();

    ASSERT_EQ(entries.size(), 4);
    expect_plan_entry(entries, 0, 0, 0, 90);
    expect_plan_entry(entries, 1, 100, 90, 40);
    expect_plan_entry(entries, 2, 200, 205, 30);
    expect_plan_entry(entries, 3, 300, 310, 10);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest, build_prefetch_plan_backward) {
    CacheBlockReadPattern pattern {
            .direction = CacheBlockReadDirection::BACKWARD,
            .ranges =
                    {
                            {.offset = 0, .size = 20},
                            {.offset = 250, .size = 80},
                            {.offset = 90, .size = 120},
                    },
    };

    auto plan = detail::CacheBlockPrefetchPlan::from_read_pattern(pattern, 100);
    const auto entries = plan.entries();

    ASSERT_EQ(entries.size(), 4);
    expect_plan_entry(entries, 0, 300, 250, 80);
    expect_plan_entry(entries, 1, 200, 250, 80);
    expect_plan_entry(entries, 2, 100, 90, 120);
    expect_plan_entry(entries, 3, 0, 90, 120);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest,
     build_prefetch_plan_expands_cross_block_ranges_and_ignores_duplicates) {
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

    auto plan = detail::CacheBlockPrefetchPlan::from_read_pattern(pattern, 100);
    const auto entries = plan.entries();

    ASSERT_EQ(entries.size(), 5);
    expect_plan_entry(entries, 0, 0, 50, 260);
    expect_plan_entry(entries, 1, 100, 50, 260);
    expect_plan_entry(entries, 2, 200, 50, 260);
    expect_plan_entry(entries, 3, 300, 50, 260);
    expect_plan_entry(entries, 4, 400, 400, 100);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest,
     build_prefetch_plan_handles_many_sparse_pages_across_many_cache_blocks) {
    CacheBlockReadPattern pattern {
            .direction = CacheBlockReadDirection::FORWARD,
            .ranges =
                    {
                            {.offset = 96_mb + 256_kb, .size = 128_kb},
                            {.offset = 64_kb, .size = 128_kb},
                            {.offset = 2_mb + 900_kb, .size = 300_kb},
                            {.offset = 7_mb + 16_kb, .size = 64_kb},
                            {.offset = 7_mb + 512_kb, .size = 128_kb},
                            {.offset = 32_mb + 768_kb, .size = 8_mb + 512_kb},
                            {.offset = 48_mb + 64_kb, .size = 0},
                    },
    };

    auto plan =
            detail::CacheBlockPrefetchPlan::from_read_pattern(pattern, static_cast<size_t>(1_mb));
    const auto entries = plan.entries();

    expect_plan_cache_block_offsets(entries, {
                                                     0,
                                                     2_mb,
                                                     3_mb,
                                                     7_mb,
                                                     32_mb,
                                                     33_mb,
                                                     34_mb,
                                                     35_mb,
                                                     36_mb,
                                                     37_mb,
                                                     38_mb,
                                                     39_mb,
                                                     40_mb,
                                                     41_mb,
                                                     96_mb,
                                             });
    expect_plan_entry(entries, 0, 0, 64_kb, 128_kb);
    expect_plan_entry(entries, 1, 2_mb, 2_mb + 900_kb, 300_kb);
    expect_plan_entry(entries, 2, 3_mb, 2_mb + 900_kb, 300_kb);
    expect_plan_entry(entries, 3, 7_mb, 7_mb + 16_kb, 64_kb);
    expect_plan_entry(entries, 4, 32_mb, 32_mb + 768_kb, 8_mb + 512_kb);
    expect_plan_entry(entries, 13, 41_mb, 32_mb + 768_kb, 8_mb + 512_kb);
    expect_plan_entry(entries, 14, 96_mb, 96_mb + 256_kb, 128_kb);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest,
     build_backward_prefetch_plan_handles_many_sparse_pages_across_many_cache_blocks) {
    CacheBlockReadPattern pattern {
            .direction = CacheBlockReadDirection::BACKWARD,
            .ranges =
                    {
                            {.offset = 96_mb + 256_kb, .size = 128_kb},
                            {.offset = 64_kb, .size = 128_kb},
                            {.offset = 2_mb + 900_kb, .size = 300_kb},
                            {.offset = 7_mb + 16_kb, .size = 64_kb},
                            {.offset = 7_mb + 512_kb, .size = 128_kb},
                            {.offset = 32_mb + 768_kb, .size = 8_mb + 512_kb},
                    },
    };

    auto plan =
            detail::CacheBlockPrefetchPlan::from_read_pattern(pattern, static_cast<size_t>(1_mb));
    const auto entries = plan.entries();

    expect_plan_cache_block_offsets(entries, {
                                                     96_mb,
                                                     41_mb,
                                                     40_mb,
                                                     39_mb,
                                                     38_mb,
                                                     37_mb,
                                                     36_mb,
                                                     35_mb,
                                                     34_mb,
                                                     33_mb,
                                                     32_mb,
                                                     7_mb,
                                                     3_mb,
                                                     2_mb,
                                                     0,
                                             });
    expect_plan_entry(entries, 0, 96_mb, 96_mb + 256_kb, 128_kb);
    expect_plan_entry(entries, 1, 41_mb, 32_mb + 768_kb, 8_mb + 512_kb);
    expect_plan_entry(entries, 10, 32_mb, 32_mb + 768_kb, 8_mb + 512_kb);
    expect_plan_entry(entries, 11, 7_mb, 7_mb + 512_kb, 128_kb);
    expect_plan_entry(entries, 12, 3_mb, 2_mb + 900_kb, 300_kb);
    expect_plan_entry(entries, 13, 2_mb, 2_mb + 900_kb, 300_kb);
    expect_plan_entry(entries, 14, 0, 64_kb, 128_kb);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest, build_prefetch_plan_backward_cross_block_range) {
    CacheBlockReadPattern pattern {
            .direction = CacheBlockReadDirection::BACKWARD,
            .ranges =
                    {
                            {.offset = 50, .size = 260},
                            {.offset = 0, .size = 50},
                    },
    };

    auto plan = detail::CacheBlockPrefetchPlan::from_read_pattern(pattern, 100);
    const auto entries = plan.entries();

    ASSERT_EQ(entries.size(), 4);
    expect_plan_entry(entries, 0, 300, 50, 260);
    expect_plan_entry(entries, 1, 200, 50, 260);
    expect_plan_entry(entries, 2, 100, 50, 260);
    expect_plan_entry(entries, 3, 0, 50, 260);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest, prefetch_window_advances_without_duplicates) {
    detail::CacheBlockPrefetchCursor cursor {
            detail::CacheBlockPrefetchPlan::from_read_pattern(
                    CacheBlockReadPattern {
                            .direction = CacheBlockReadDirection::FORWARD,
                            .ranges =
                                    {
                                            {.offset = 0, .size = 1},
                                            {.offset = 100, .size = 1},
                                            {.offset = 200, .size = 1},
                                            {.offset = 300, .size = 1},
                                    },
                    },
                    100),
            2};

    auto ranges = cursor.next_touch_ranges(0);
    ASSERT_EQ(ranges.size(), 2);
    EXPECT_EQ(ranges[0].offset, 0);
    EXPECT_EQ(ranges[1].offset, 100);

    ranges = cursor.next_touch_ranges(0);
    EXPECT_TRUE(ranges.empty());

    ranges = cursor.next_touch_ranges(100);
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges[0].offset, 200);

    ranges = cursor.next_touch_ranges(300);
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges[0].offset, 300);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest, initial_touch_window_advances_before_read_at) {
    detail::CacheBlockPrefetchCursor cursor {
            detail::CacheBlockPrefetchPlan::from_read_pattern(
                    CacheBlockReadPattern {
                            .direction = CacheBlockReadDirection::FORWARD,
                            .ranges =
                                    {
                                            {.offset = 0, .size = 1},
                                            {.offset = 100, .size = 1},
                                            {.offset = 200, .size = 1},
                                    },
                    },
                    100),
            2};

    auto ranges = cursor.next_initial_touch_ranges();
    ASSERT_EQ(ranges.size(), 2);
    EXPECT_EQ(ranges[0].offset, 0);
    EXPECT_EQ(ranges[1].offset, 100);

    ranges = cursor.next_touch_ranges(0);
    EXPECT_TRUE(ranges.empty());

    ranges = cursor.next_touch_ranges(100);
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges[0].offset, 200);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest, prefetch_window_does_not_split_large_file_range) {
    detail::CacheBlockPrefetchCursor cursor {
            detail::CacheBlockPrefetchPlan::from_read_pattern(
                    CacheBlockReadPattern {
                            .direction = CacheBlockReadDirection::FORWARD,
                            .ranges =
                                    {
                                            {.offset = 50, .size = 260},
                                            {.offset = 400, .size = 100},
                                    },
                    },
                    100),
            2};

    auto ranges = cursor.next_touch_ranges(50);
    ASSERT_EQ(ranges.size(), 4);
    EXPECT_EQ(ranges[0].offset, 0);
    EXPECT_EQ(ranges[1].offset, 100);
    EXPECT_EQ(ranges[2].offset, 200);
    EXPECT_EQ(ranges[3].offset, 300);

    ranges = cursor.next_touch_ranges(400);
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges[0].offset, 400);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest,
     prefetch_cursor_handles_sparse_large_span_and_many_cache_blocks) {
    detail::CacheBlockPrefetchCursor cursor {
            detail::CacheBlockPrefetchPlan::from_read_pattern(
                    CacheBlockReadPattern {
                            .direction = CacheBlockReadDirection::FORWARD,
                            .ranges =
                                    {
                                            {.offset = 0, .size = 1},
                                            {.offset = 5_mb + 64_kb, .size = 64_kb},
                                            {.offset = 20_mb + 256_kb, .size = 7_mb + 512_kb},
                                            {.offset = 40_mb + 32_kb, .size = 64_kb},
                                            {.offset = 41_mb + 64_kb, .size = 64_kb},
                                            {.offset = 88_mb + 128_kb, .size = 64_kb},
                                    },
                    },
                    static_cast<size_t>(1_mb)),
            3};

    auto ranges = cursor.next_touch_ranges(0);
    expect_cache_block_offsets(ranges, {
                                               0,
                                               5_mb,
                                               20_mb,
                                               21_mb,
                                               22_mb,
                                               23_mb,
                                               24_mb,
                                               25_mb,
                                               26_mb,
                                               27_mb,
                                       });

    ranges = cursor.next_touch_ranges(20_mb + 256_kb);
    EXPECT_TRUE(ranges.empty());

    ranges = cursor.next_touch_ranges(40_mb + 32_kb);
    expect_cache_block_offsets(ranges, {
                                               40_mb,
                                               41_mb,
                                               88_mb,
                                       });

    ranges = cursor.next_touch_ranges(88_mb + 128_kb);
    EXPECT_TRUE(ranges.empty());
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest,
     prefetch_window_triggers_when_read_starts_inside_file_range) {
    detail::CacheBlockPrefetchCursor forward_cursor {
            detail::CacheBlockPrefetchPlan::from_read_pattern(
                    CacheBlockReadPattern {
                            .direction = CacheBlockReadDirection::FORWARD,
                            .ranges = {{.offset = 50, .size = 260}},
                    },
                    100),
            2};

    auto ranges = forward_cursor.next_touch_ranges(180);
    ASSERT_EQ(ranges.size(), 4);
    EXPECT_EQ(ranges[0].offset, 0);
    EXPECT_EQ(ranges[1].offset, 100);
    EXPECT_EQ(ranges[2].offset, 200);
    EXPECT_EQ(ranges[3].offset, 300);

    detail::CacheBlockPrefetchCursor backward_cursor {
            detail::CacheBlockPrefetchPlan::from_read_pattern(
                    CacheBlockReadPattern {
                            .direction = CacheBlockReadDirection::BACKWARD,
                            .ranges = {{.offset = 50, .size = 260}},
                    },
                    100),
            2};

    ranges = backward_cursor.next_touch_ranges(180);
    ASSERT_EQ(ranges.size(), 4);
    EXPECT_EQ(ranges[0].offset, 300);
    EXPECT_EQ(ranges[1].offset, 200);
    EXPECT_EQ(ranges[2].offset, 100);
    EXPECT_EQ(ranges[3].offset, 0);
}

TEST(CacheBlockAwarePrefetchRemoteReaderTest,
     backward_prefetch_window_advances_without_duplicates) {
    detail::CacheBlockPrefetchCursor cursor {
            detail::CacheBlockPrefetchPlan::from_read_pattern(
                    CacheBlockReadPattern {
                            .direction = CacheBlockReadDirection::BACKWARD,
                            .ranges =
                                    {
                                            {.offset = 300, .size = 1},
                                            {.offset = 200, .size = 1},
                                            {.offset = 100, .size = 1},
                                            {.offset = 0, .size = 1},
                                    },
                    },
                    100),
            2};

    auto ranges = cursor.next_touch_ranges(300);
    ASSERT_EQ(ranges.size(), 2);
    EXPECT_EQ(ranges[0].offset, 300);
    EXPECT_EQ(ranges[1].offset, 200);

    ranges = cursor.next_touch_ranges(200);
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges[0].offset, 100);

    ranges = cursor.next_touch_ranges(0);
    ASSERT_EQ(ranges.size(), 1);
    EXPECT_EQ(ranges[0].offset, 0);
}

TEST_F(BlockFileCacheTest, usage_example_read_at_automatically_prefetches_single_pattern) {
    // This is the intended integration pattern for segment readers:
    // create one cache-aware reader per physical iterator, install one monotonic
    // file-offset pattern, then let ordinary read_at() calls advance prefetch
    // automatically. No outer code needs to keep a pattern id or manually
    // trigger cache-block prefetch before each page read.
    std::string test_cache_base_path = caches_dir / "cache_block_prefetch_usage_example" / "";
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

    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("CacheBlockAwarePrefetchRemoteReaderUsageTest")
                        .set_min_threads(2)
                        .set_max_threads(4)
                        .build(&pool)
                        .ok());
    ExecEnv::GetInstance()->_segment_prefetch_thread_pool = std::move(pool);

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.tablet_id = 10086;

    auto first_remote_reader = std::make_shared<CountingRemoteFileReader>(
            Path("/tmp/cache_block_prefetch_usage_example_first"), 5_mb);
    auto first_reader =
            std::make_shared<CacheBlockAwarePrefetchRemoteReader>(first_remote_reader, opts);
    auto second_remote_reader = std::make_shared<CountingRemoteFileReader>(
            Path("/tmp/cache_block_prefetch_usage_example_second"), 5_mb);
    auto second_reader =
            std::make_shared<CacheBlockAwarePrefetchRemoteReader>(second_remote_reader, opts);
    CacheBlockPrefetchPolicy policy {
            .max_prefetch_blocks = 2,
            .cache_block_size = static_cast<size_t>(config::file_cache_each_block_size),
    };

    // The two readers stand for two independent column iterators. Their
    // patterns are intentionally different to show that progress is isolated by
    // the reader object instead of by an external read-pattern handle.
    ASSERT_TRUE(first_reader
                        ->set_read_pattern(
                                CacheBlockReadPattern {
                                        .direction = CacheBlockReadDirection::FORWARD,
                                        .ranges =
                                                {
                                                        {.offset = 0, .size = 1},
                                                        {.offset = 1_mb, .size = 1},
                                                        {.offset = 2_mb, .size = 1},
                                                },
                                },
                                policy)
                        .ok());
    ASSERT_TRUE(second_reader
                        ->set_read_pattern(
                                CacheBlockReadPattern {
                                        .direction = CacheBlockReadDirection::FORWARD,
                                        .ranges =
                                                {
                                                        {.offset = 3_mb, .size = 1},
                                                        {.offset = 4_mb, .size = 1},
                                                },
                                },
                                policy)
                        .ok());
    ASSERT_TRUE(first_reader->has_read_pattern());
    ASSERT_TRUE(second_reader->has_read_pattern());

    char buf;
    size_t bytes_read = 0;
    IOContext io_ctx;
    auto read_offsets = [](const std::shared_ptr<CountingRemoteFileReader>& reader) {
        std::set<size_t> offsets;
        for (const auto& range : reader->read_ranges()) {
            offsets.emplace(range.offset);
        }
        return offsets;
    };

    // A normal read on the first reader touches the first reader's window only.
    // The second reader remains idle until its own read_at() observes file
    // offset 3 MiB.
    ASSERT_TRUE(first_reader->read_at(0, Slice(&buf, 1), &bytes_read, &io_ctx).ok());
    EXPECT_EQ(bytes_read, 1);
    ASSERT_TRUE(wait_until([&] {
        auto offsets = read_offsets(first_remote_reader);
        return offsets.contains(0) && offsets.contains(1_mb);
    }));
    EXPECT_TRUE(second_remote_reader->read_ranges().empty());

    // Reading the second iterator advances only the second pattern. This mirrors
    // segment scans where each physical column iterator owns its own
    // CacheBlockAwarePrefetchRemoteReader.
    ASSERT_TRUE(second_reader->read_at(3_mb, Slice(&buf, 1), &bytes_read, &io_ctx).ok());
    EXPECT_EQ(bytes_read, 1);
    ASSERT_TRUE(wait_until([&] {
        auto offsets = read_offsets(second_remote_reader);
        return offsets.contains(3_mb) && offsets.contains(4_mb);
    }));

    first_reader->clear_read_pattern();
    EXPECT_FALSE(first_reader->has_read_pattern());
    EXPECT_TRUE(second_reader->has_read_pattern());
}

TEST_F(BlockFileCacheTest,
       cache_block_aware_prefetch_remote_reader_touches_initial_window_before_read_at) {
    std::string test_cache_base_path = caches_dir / "initial_prefetch_window" / "";
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

    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("CacheBlockAwarePrefetchRemoteReaderInitialWindowTest")
                        .set_min_threads(2)
                        .set_max_threads(4)
                        .build(&pool)
                        .ok());
    ExecEnv::GetInstance()->_segment_prefetch_thread_pool = std::move(pool);

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.tablet_id = 10086;

    auto remote_reader = std::make_shared<CountingRemoteFileReader>(
            Path("/tmp/cache_block_prefetch_initial_window"), 4_mb);
    auto reader = std::make_shared<CacheBlockAwarePrefetchRemoteReader>(remote_reader, opts);
    CacheBlockPrefetchPolicy policy {
            .max_prefetch_blocks = 2,
            .cache_block_size = static_cast<size_t>(config::file_cache_each_block_size),
    };
    ASSERT_TRUE(reader->set_read_pattern(
                              CacheBlockReadPattern {
                                      .direction = CacheBlockReadDirection::FORWARD,
                                      .ranges =
                                              {
                                                      {.offset = 0, .size = 1},
                                                      {.offset = 1_mb, .size = 1},
                                                      {.offset = 2_mb, .size = 1},
                                              },
                              },
                              policy)
                        .ok());

    // Predicate columns in SegmentIterator use this path after their read pattern is installed:
    // the first window is touched before PageIO issues the first foreground read_at(), because
    // those ranges are guaranteed to be consumed by predicate evaluation.
    IOContext io_ctx;
    reader->async_touch_initial_window(&io_ctx);
    ASSERT_TRUE(wait_until([&] {
        std::set<size_t> offsets;
        for (const auto& range : remote_reader->read_ranges()) {
            offsets.emplace(range.offset);
        }
        return offsets.contains(0) && offsets.contains(1_mb);
    }));

    auto key = BlockFileCache::hash("cache_block_prefetch_initial_window");
    CacheContext context;
    ReadStatistics stats;
    context.stats = &stats;
    context.cache_type = FileCacheType::NORMAL;
    ASSERT_TRUE(wait_until([&] {
        auto holder = cache->get_or_set(key, 0, 2_mb, context);
        auto blocks = fromHolder(holder);
        return blocks.size() == 2 && blocks[0]->state() == FileBlock::State::DOWNLOADED &&
               blocks[1]->state() == FileBlock::State::DOWNLOADED;
    }));

    // The initial touch advances the same cursor used by read_at()-triggered prefetch, so calling
    // it again without scan progress is idempotent and does not resubmit the already touched
    // cache blocks.
    const auto read_count = remote_reader->read_ranges().size();
    reader->async_touch_initial_window(&io_ctx);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(remote_reader->read_ranges().size(), read_count);
}

TEST_F(BlockFileCacheTest, cached_remote_file_reader_async_touch_local_cache_downloads_range) {
    std::string test_cache_base_path = caches_dir / "async_touch_local_cache" / "";
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

    std::unique_ptr<ThreadPool> pool;
    ASSERT_TRUE(ThreadPoolBuilder("CachedRemoteFileReaderAsyncTouchTest")
                        .set_min_threads(2)
                        .set_max_threads(4)
                        .build(&pool)
                        .ok());
    ExecEnv::GetInstance()->_segment_prefetch_thread_pool = std::move(pool);

    FileReaderOptions opts;
    opts.cache_type = FileCachePolicy::FILE_BLOCK_CACHE;
    opts.is_doris_table = true;
    opts.tablet_id = 10086;

    auto remote_reader = std::make_shared<CountingRemoteFileReader>(
            Path("/tmp/cached_remote_async_touch_local_cache"), 2_mb);
    auto reader = std::make_shared<CachedRemoteFileReader>(remote_reader, opts);

    reader->async_touch_local_cache(0, 1_mb);
    ASSERT_TRUE(wait_until([&] { return !remote_reader->read_ranges().empty(); }));
    auto read_ranges = remote_reader->read_ranges();
    ASSERT_EQ(read_ranges[0].offset, 0);

    auto key = BlockFileCache::hash("cached_remote_async_touch_local_cache");
    CacheContext context;
    ReadStatistics stats;
    context.stats = &stats;
    context.cache_type = FileCacheType::NORMAL;
    ASSERT_TRUE(wait_until([&] {
        auto holder = cache->get_or_set(key, 0, 1_mb, context);
        auto blocks = fromHolder(holder);
        return blocks.size() == 1 && blocks[0]->state() == FileBlock::State::DOWNLOADED;
    }));
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
    ASSERT_TRUE(reader->set_read_pattern(std::move(pattern), policy).ok());
    ASSERT_TRUE(reader->has_read_pattern());

    char buf;
    size_t bytes_read = 0;
    IOContext io_ctx;
    ASSERT_TRUE(reader->read_at(0, Slice(&buf, 1), &bytes_read, &io_ctx).ok());
    EXPECT_EQ(bytes_read, 1);
    ASSERT_TRUE(wait_until([&] { return remote_reader->read_ranges().size() >= 2; }));
    auto read_ranges = remote_reader->read_ranges();
    std::set<size_t> offsets;
    for (const auto& range : read_ranges) {
        offsets.emplace(range.offset);
    }
    EXPECT_TRUE(offsets.contains(0));
    EXPECT_TRUE(offsets.contains(1_mb));

    ASSERT_TRUE(reader->read_at(1_mb, Slice(&buf, 1), &bytes_read, &io_ctx).ok());
    EXPECT_EQ(bytes_read, 1);
    ASSERT_TRUE(wait_until([&] { return remote_reader->read_ranges().size() >= 3; }));
    read_ranges = remote_reader->read_ranges();
    offsets.clear();
    for (const auto& range : read_ranges) {
        offsets.emplace(range.offset);
    }
    EXPECT_TRUE(offsets.contains(2_mb));

    const auto read_count = remote_reader->read_ranges().size();
    ASSERT_TRUE(reader->read_at(1_mb, Slice(&buf, 1), &bytes_read, &io_ctx).ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(remote_reader->read_ranges().size(), read_count);

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
}

} // namespace doris::io
