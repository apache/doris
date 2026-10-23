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

#include <memory>
#include <vector>

#include "format/parquet/parquet_common.h"
#include "format/parquet/vparquet_column_reader.h"

namespace doris {

// The nested-level projection a lazy-read batch's filter map goes through for a complex column
// (ScalarColumnReader::gen_filter_map). The first two cases are the crash shape of a selective
// predicate plus a struct sub-field in the projection: a batch whose rows were all filtered by the
// predicate columns reaches the nested reader as a filter-all map, with or without data.
class ParquetNestedFilterMapTest : public testing::Test {
protected:
    using Reader = ScalarColumnReader<false, true>;

    // three top-level rows: [r0, r0.x, r0.y], [r1, r1.x], [r2]
    const std::vector<level_t> _rep_levels = {0, 1, 1, 0, 1, 0};
};

TEST_F(ParquetNestedFilterMapTest, filter_all_without_data_filters_every_level) {
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(nullptr, 3, true).ok());
    ASSERT_EQ(filter_map.filter_map_data(), nullptr);

    std::vector<uint8_t> nested_data;
    std::unique_ptr<FilterMap> nested;
    ASSERT_TRUE(Reader::gen_nested_filter_map(filter_map, _rep_levels, 0, 0, _rep_levels.size(),
                                              nested_data, &nested)
                        .ok());

    EXPECT_EQ(nested_data, (std::vector<uint8_t> {0, 0, 0, 0, 0, 0}));
    ASSERT_NE(nested, nullptr);
    EXPECT_TRUE(nested->has_filter());
    EXPECT_TRUE(nested->filter_all());
    EXPECT_EQ(nested->filter_map_size(), 6);
}

TEST_F(ParquetNestedFilterMapTest, filter_all_with_zero_data_filters_every_level) {
    // FilterMap::init detects an all-zero map and flags it filter-all itself
    std::vector<uint8_t> zeros(3, 0);
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(zeros.data(), zeros.size(), false).ok());
    ASSERT_TRUE(filter_map.filter_all());

    std::vector<uint8_t> nested_data;
    std::unique_ptr<FilterMap> nested;
    ASSERT_TRUE(Reader::gen_nested_filter_map(filter_map, _rep_levels, 0, 0, _rep_levels.size(),
                                              nested_data, &nested)
                        .ok());

    EXPECT_EQ(nested_data, (std::vector<uint8_t> {0, 0, 0, 0, 0, 0}));
    EXPECT_TRUE(nested->filter_all());
}

TEST_F(ParquetNestedFilterMapTest, filter_all_on_a_level_window_sizes_by_the_window) {
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(nullptr, 3, true).ok());

    std::vector<uint8_t> nested_data;
    std::unique_ptr<FilterMap> nested;
    // the cross-page tail of a row: levels [3, 6) only
    ASSERT_TRUE(Reader::gen_nested_filter_map(filter_map, _rep_levels, 1, 3, _rep_levels.size(),
                                              nested_data, &nested)
                        .ok());

    EXPECT_EQ(nested_data, (std::vector<uint8_t> {0, 0, 0}));
    EXPECT_TRUE(nested->filter_all());
    EXPECT_EQ(nested->filter_map_size(), 3);
}

TEST_F(ParquetNestedFilterMapTest, selective_map_is_projected_by_repetition_levels) {
    std::vector<uint8_t> row_filter = {1, 0, 1};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(row_filter.data(), row_filter.size(), false).ok());
    ASSERT_FALSE(filter_map.filter_all());

    std::vector<uint8_t> nested_data;
    std::unique_ptr<FilterMap> nested;
    ASSERT_TRUE(Reader::gen_nested_filter_map(filter_map, _rep_levels, 0, 0, _rep_levels.size(),
                                              nested_data, &nested)
                        .ok());

    // row 0 keeps its three levels, row 1 loses its two, row 2 keeps its one
    EXPECT_EQ(nested_data, (std::vector<uint8_t> {1, 1, 1, 0, 0, 1}));
    EXPECT_TRUE(nested->has_filter());
    EXPECT_FALSE(nested->filter_all());
    EXPECT_DOUBLE_EQ(nested->filter_ratio(), 2.0 / 6.0);
}

TEST_F(ParquetNestedFilterMapTest, selective_map_window_starts_at_the_given_row) {
    std::vector<uint8_t> row_filter = {1, 0, 1};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(row_filter.data(), row_filter.size(), false).ok());

    std::vector<uint8_t> nested_data;
    std::unique_ptr<FilterMap> nested;
    // levels [3, 6) belong to rows 1 and 2; filter_loc names row 1
    ASSERT_TRUE(Reader::gen_nested_filter_map(filter_map, _rep_levels, 1, 3, _rep_levels.size(),
                                              nested_data, &nested)
                        .ok());

    EXPECT_EQ(nested_data, (std::vector<uint8_t> {0, 0, 1}));
    EXPECT_TRUE(nested->has_filter());
    EXPECT_FALSE(nested->filter_all());
}

} // namespace doris
