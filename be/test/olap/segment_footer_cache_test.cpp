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

#include <random>

#include "olap/page_cache.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/storage_engine.h"
#include "olap/tablet_schema_helper.h"

namespace doris {

TabletColumnPtr create_int_sequence_value(int32_t id, bool is_nullable = true,
                                          bool is_bf_column = false, bool has_bitmap_index = false);

TabletSchemaSPtr create_schema(const std::vector<TabletColumnPtr>& columns,
                               KeysType keys_type = UNIQUE_KEYS);

using Generator = std::function<void(size_t rid, int cid, RowCursorCell& cell)>;

void build_segment(SegmentWriterOptions opts, TabletSchemaSPtr build_schema, size_t segment_id,
                   TabletSchemaSPtr query_schema, size_t nrows, Generator generator,
                   std::shared_ptr<Segment>* res, std::string segment_dir);

static std::string segment_footer_cache_test_segment_dir = "./ut_dir/segment_footer_cache_test";

class SegmentFooterCacheTest : public ::testing::Test {
    using Segments = std::vector<std::shared_ptr<segment_v2::Segment>>;
    Segments create(size_t const num_segments, size_t const max_rows_per_segment,
                    size_t const num_key_columns, bool has_sequence_col,
                    size_t const num_value_columns, int const random_seed, int const min_value,
                    int const max_value) {
        Segments segments(num_segments);
        segment_v2::SegmentWriterOptions opts;
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
            auto generator = [&](size_t rid, int cid, RowCursorCell& cell) {
                cell.set_not_null();
                *(int*)cell.mutable_cell_ptr() = data_map[{sid, rid}][cid];
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
        EXPECT_TRUE(io::global_local_filesystem()
                            ->delete_directory(segment_footer_cache_test_segment_dir)
                            .ok());
    }

private:
    Segments _segments;
};

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
