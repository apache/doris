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

#include "format/parquet/parquet_common.h"

#include <gtest/gtest.h>

namespace doris {

// ============= FilterMap Tests =============
class FilterMapTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// Basic initialization test
TEST_F(FilterMapTest, test_basic_init) {
    std::vector<uint8_t> filter_data = {1, 0, 1, 0};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());

    EXPECT_TRUE(filter_map.has_filter());
    EXPECT_FALSE(filter_map.filter_all());
    EXPECT_EQ(filter_map.filter_map_size(), 4);
    EXPECT_DOUBLE_EQ(filter_map.filter_ratio(), 0.5);
}

// Empty filter test
TEST_F(FilterMapTest, test_empty_filter) {
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(nullptr, 0, false).ok());

    EXPECT_FALSE(filter_map.has_filter());
    EXPECT_FALSE(filter_map.filter_all());
    EXPECT_DOUBLE_EQ(filter_map.filter_ratio(), 0.0);
}

// Test filter all
TEST_F(FilterMapTest, test_filter_all) {
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(nullptr, 0, true).ok());

    EXPECT_TRUE(filter_map.has_filter());
    EXPECT_TRUE(filter_map.filter_all());
    EXPECT_DOUBLE_EQ(filter_map.filter_ratio(), 1.0);
}

// Test all zero filter
TEST_F(FilterMapTest, test_all_zero_filter) {
    std::vector<uint8_t> filter_data(100, 0); // Large data test
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());

    EXPECT_TRUE(filter_map.has_filter());
    EXPECT_TRUE(filter_map.filter_all());
    EXPECT_DOUBLE_EQ(filter_map.filter_ratio(), 1.0);
}

// Test all one filter
TEST_F(FilterMapTest, test_all_one_filter) {
    std::vector<uint8_t> filter_data(100, 1);
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());

    EXPECT_FALSE(filter_map.has_filter());
    EXPECT_FALSE(filter_map.filter_all());
    EXPECT_DOUBLE_EQ(filter_map.filter_ratio(), 0.0);
}

// Basic nested filter map generation test
TEST_F(FilterMapTest, test_generate_nested_filter_map_basic) {
    std::vector<uint8_t> filter_data = {1, 0, 1};
    std::vector<level_t> rep_levels = {0, 1, 1, 0, 1, 0};

    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());

    std::vector<uint8_t> nested_filter_map_data;
    std::unique_ptr<FilterMap> nested_filter_map;
    size_t current_row = 0;

    ASSERT_TRUE(filter_map
                        .generate_nested_filter_map(rep_levels, nested_filter_map_data,
                                                    &nested_filter_map, &current_row, 0)
                        .ok());

    std::vector<uint8_t> expected = {1, 1, 1, 0, 0, 1};
    EXPECT_EQ(nested_filter_map_data, expected);
    EXPECT_EQ(current_row, 2);
}

// Empty rep_levels test
TEST_F(FilterMapTest, test_generate_nested_filter_map_empty_rep_levels) {
    std::vector<uint8_t> filter_data = {1, 0, 1};
    std::vector<level_t> rep_levels;

    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());

    std::vector<uint8_t> nested_filter_map_data;
    std::unique_ptr<FilterMap> nested_filter_map;
    size_t current_row = 0;

    ASSERT_TRUE(filter_map
                        .generate_nested_filter_map(rep_levels, nested_filter_map_data,
                                                    &nested_filter_map, &current_row, 0)
                        .ok());

    EXPECT_TRUE(nested_filter_map_data.empty());
    EXPECT_EQ(current_row, 0);
}

// Test nested filter map generation with start index
TEST_F(FilterMapTest, test_generate_nested_filter_map_with_start_index) {
    std::vector<uint8_t> filter_data = {1, 0, 1};
    std::vector<level_t> rep_levels = {0, 1, 1, 0, 1, 0};
    // 011, 01, 0
    // 111, 00, 1

    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());

    std::vector<uint8_t> nested_filter_map_data;
    std::unique_ptr<FilterMap> nested_filter_map;
    size_t current_row = 1;

    ASSERT_TRUE(filter_map
                        .generate_nested_filter_map(rep_levels, nested_filter_map_data,
                                                    &nested_filter_map, &current_row, 3)
                        .ok());

    std::vector<uint8_t> expected(6); // Initialize with zeros
    expected[5] = 1;                  // Last value should be 1
    EXPECT_EQ(nested_filter_map_data, expected);
    EXPECT_EQ(current_row, 2);
}

// Test filter map boundary check
TEST_F(FilterMapTest, test_generate_nested_filter_map_boundary) {
    std::vector<uint8_t> filter_data = {1};
    std::vector<level_t> rep_levels = {0, 1, 1, 0}; // Needs 2 rows but filter_data only has 1

    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());

    std::vector<uint8_t> nested_filter_map_data;
    std::unique_ptr<FilterMap> nested_filter_map;
    size_t current_row = 0;

    // Should return error
    auto status = filter_map.generate_nested_filter_map(rep_levels, nested_filter_map_data,
                                                        &nested_filter_map, &current_row, 0);
    EXPECT_FALSE(status.ok());
}

// Test can_filter_all functionality
TEST_F(FilterMapTest, test_can_filter_all) {
    std::vector<uint8_t> filter_data = {0, 0, 1, 0, 0, 1, 0};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());

    EXPECT_TRUE(filter_map.can_filter_all(2, 0));  // First two are 0
    EXPECT_FALSE(filter_map.can_filter_all(3, 0)); // First three include 1
    EXPECT_TRUE(filter_map.can_filter_all(2, 3));  // Two values starting at index 3 are 0
    EXPECT_FALSE(filter_map.can_filter_all(2, 5)); // Index 5 contains 1
    EXPECT_TRUE(filter_map.can_filter_all(1, 6));  // Last value is 0
}

// Test can_filter_all when filter_all is true
TEST_F(FilterMapTest, test_can_filter_all_when_filter_all) {
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(nullptr, 100, true).ok());

    EXPECT_TRUE(filter_map.can_filter_all(50, 0));
    EXPECT_TRUE(filter_map.can_filter_all(100, 0));
}

class CrossPageTest : public testing::Test {
protected:
    void SetUp() override {
        filter_data = {1, 0, 1, 0, 1};

        // 1111 00
        page1_rep_levels = {0, 1, 1, 1, 0, 1};
        // 00 11 000 1
        page2_rep_levels = {1, 1, 0, 1, 0, 1, 1, 0};
    }

    std::vector<uint8_t> filter_data;
    std::vector<level_t> page1_rep_levels;
    std::vector<level_t> page2_rep_levels;
};

TEST_F(CrossPageTest, test_basic1) {
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());

    std::vector<uint8_t> nested_filter_map_data;
    std::unique_ptr<FilterMap> nested_filter_map;
    size_t current_row = 0;
    std::vector<level_t> rep_levels;
    rep_levels.insert(rep_levels.end(), page1_rep_levels.begin(), page1_rep_levels.end());
    rep_levels.insert(rep_levels.end(), page2_rep_levels.begin(), page2_rep_levels.end());

    ASSERT_TRUE(filter_map
                        .generate_nested_filter_map(rep_levels, nested_filter_map_data,
                                                    &nested_filter_map, &current_row, 0)
                        .ok());

    std::vector<uint8_t> expected = {1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1};

    EXPECT_EQ(nested_filter_map_data, expected);

    EXPECT_EQ(current_row, 4);
}

TEST_F(CrossPageTest, test_basic2) {
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());

    std::vector<uint8_t> nested_filter_map_data;
    std::unique_ptr<FilterMap> nested_filter_map;

    size_t current_row = 0;
    std::vector<level_t> rep_levels;
    rep_levels.insert(rep_levels.end(), page1_rep_levels.begin(), page1_rep_levels.end());

    ASSERT_TRUE(filter_map
                        .generate_nested_filter_map(rep_levels, nested_filter_map_data,
                                                    &nested_filter_map, &current_row, 0)
                        .ok());
    std::vector<uint8_t> expected1 = {1, 1, 1, 1, 0, 0};

    EXPECT_EQ(nested_filter_map_data, expected1);
    EXPECT_EQ(current_row, 1);

    rep_levels.insert(rep_levels.end(), page2_rep_levels.begin(), page2_rep_levels.end());

    size_t start_index = page1_rep_levels.size();
    ASSERT_TRUE(filter_map
                        .generate_nested_filter_map(rep_levels, nested_filter_map_data,
                                                    &nested_filter_map, &current_row, start_index)
                        .ok());

    std::vector<uint8_t> expected2 = {1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1};

    EXPECT_EQ(nested_filter_map_data, expected2);
    EXPECT_EQ(current_row, 4);
}

// ============= ColumnSelectVector Tests =============
class ColumnSelectVectorTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// Basic initialization test
TEST_F(ColumnSelectVectorTest, test_basic_init) {
    std::vector<uint16_t> run_length_null_map = {2, 1, 3}; // 2 non-null, 1 null, 3 non-null
    std::vector<uint8_t> filter_data = {1, 0, 1, 0, 1, 0};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());

    ColumnSelectVector select_vector;
    NullMap null_map;
    ASSERT_TRUE(select_vector.init(run_length_null_map, 6, &null_map, &filter_map, 0).ok());

    EXPECT_TRUE(select_vector.has_filter());
    EXPECT_EQ(select_vector.num_values(), 6);
    EXPECT_EQ(select_vector.num_nulls(), 1);
    EXPECT_EQ(select_vector.num_filtered(), 3);
}

// Test initialization without null map
TEST_F(ColumnSelectVectorTest, test_init_without_null_map) {
    std::vector<uint16_t> run_length_null_map = {2, 1, 3};
    std::vector<uint8_t> filter_data = {1, 1, 1, 1, 1, 1};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());

    ColumnSelectVector select_vector;
    ASSERT_TRUE(select_vector.init(run_length_null_map, 6, nullptr, &filter_map, 0).ok());

    EXPECT_EQ(select_vector.num_nulls(), 1);
    EXPECT_EQ(select_vector.num_filtered(), 0);
}

// Test all null values
TEST_F(ColumnSelectVectorTest, test_all_null) {
    std::vector<uint16_t> run_length_null_map = {0, 6}; // All null
    std::vector<uint8_t> filter_data = {1, 1, 1, 1, 1, 1};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());

    ColumnSelectVector select_vector;
    NullMap null_map;
    ASSERT_TRUE(select_vector.init(run_length_null_map, 6, &null_map, &filter_map, 0).ok());

    EXPECT_EQ(select_vector.num_nulls(), 6);
    EXPECT_EQ(select_vector.num_filtered(), 0);

    // Verify null_map
    EXPECT_EQ(null_map.size(), 6);
    for (size_t i = 0; i < 6; i++) {
        EXPECT_EQ(null_map[i], 1);
    }
}

// Test no null values
TEST_F(ColumnSelectVectorTest, test_no_null) {
    std::vector<uint16_t> run_length_null_map = {6}; // All non-null
    std::vector<uint8_t> filter_data = {1, 1, 1, 1, 1, 1};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());

    ColumnSelectVector select_vector;
    NullMap null_map;
    ASSERT_TRUE(select_vector.init(run_length_null_map, 6, &null_map, &filter_map, 0).ok());

    EXPECT_EQ(select_vector.num_nulls(), 0);
    EXPECT_EQ(select_vector.num_filtered(), 0);

    // Verify null_map
    EXPECT_EQ(null_map.size(), 6);
    for (size_t i = 0; i < 6; i++) {
        EXPECT_EQ(null_map[i], 0);
    }
}

// Test get_next_run with filter
TEST_F(ColumnSelectVectorTest, test_get_next_run_with_filter) {
    std::vector<uint16_t> run_length_null_map = {2, 1, 3}; // 1, 1, 0, 1, 1, 1
    std::vector<uint8_t> filter_data = {1, 1, 0, 1, 1, 0};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());

    ColumnSelectVector select_vector;
    NullMap null_map;
    ASSERT_TRUE(select_vector.init(run_length_null_map, 6, &null_map, &filter_map, 0).ok());

    ColumnSelectVector::DataReadType type;

    // Verify read sequence
    EXPECT_EQ(select_vector.get_next_run<true>(&type), 2);
    EXPECT_EQ(type, ColumnSelectVector::CONTENT);

    EXPECT_EQ(select_vector.get_next_run<true>(&type), 1);
    EXPECT_EQ(type, ColumnSelectVector::FILTERED_NULL);

    EXPECT_EQ(select_vector.get_next_run<true>(&type), 2);
    EXPECT_EQ(type, ColumnSelectVector::CONTENT);

    EXPECT_EQ(select_vector.get_next_run<true>(&type), 1);
    EXPECT_EQ(type, ColumnSelectVector::FILTERED_CONTENT);
}

// Test get_next_run without filter
TEST_F(ColumnSelectVectorTest, test_get_next_run_without_filter) {
    std::vector<uint16_t> run_length_null_map = {2, 1, 3};
    std::vector<uint8_t> filter_data = {1, 1, 1, 1, 1, 1};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(nullptr, 0, false).ok());

    ColumnSelectVector select_vector;
    NullMap null_map;
    ASSERT_TRUE(select_vector.init(run_length_null_map, 6, &null_map, &filter_map, 0).ok());

    ColumnSelectVector::DataReadType type;

    // Verify read sequence
    EXPECT_EQ(select_vector.get_next_run<false>(&type), 2);
    EXPECT_EQ(type, ColumnSelectVector::CONTENT);

    EXPECT_EQ(select_vector.get_next_run<false>(&type), 1);
    EXPECT_EQ(type, ColumnSelectVector::NULL_DATA);

    EXPECT_EQ(select_vector.get_next_run<false>(&type), 3);
    EXPECT_EQ(type, ColumnSelectVector::CONTENT);

    EXPECT_EQ(select_vector.get_next_run<false>(&type), 0);
}

// Test complex null pattern
TEST_F(ColumnSelectVectorTest, test_complex_null_pattern) {
    // Alternating null and non-null values
    std::vector<uint16_t> run_length_null_map = {1, 1, 1, 1, 1, 1}; // 1, 0, 1, 0, 1, 0
    std::vector<uint8_t> filter_data = {1, 0, 1, 0, 1, 0};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());

    ColumnSelectVector select_vector;
    NullMap null_map;
    ASSERT_TRUE(select_vector.init(run_length_null_map, 6, &null_map, &filter_map, 0).ok());

    EXPECT_EQ(select_vector.num_nulls(), 3);
    EXPECT_EQ(select_vector.num_filtered(), 3);

    ColumnSelectVector::DataReadType type;

    // Verify alternating read pattern
    EXPECT_EQ(select_vector.get_next_run<true>(&type), 1);
    EXPECT_EQ(type, ColumnSelectVector::CONTENT);

    EXPECT_EQ(select_vector.get_next_run<true>(&type), 1);
    EXPECT_EQ(type, ColumnSelectVector::FILTERED_NULL);

    EXPECT_EQ(select_vector.get_next_run<true>(&type), 1);
    EXPECT_EQ(type, ColumnSelectVector::CONTENT);

    EXPECT_EQ(select_vector.get_next_run<true>(&type), 1);
    EXPECT_EQ(type, ColumnSelectVector::FILTERED_NULL);

    EXPECT_EQ(select_vector.get_next_run<true>(&type), 1);
    EXPECT_EQ(type, ColumnSelectVector::CONTENT);

    EXPECT_EQ(select_vector.get_next_run<true>(&type), 1);
    EXPECT_EQ(type, ColumnSelectVector::FILTERED_NULL);
}

// Test filter_map_index
TEST_F(ColumnSelectVectorTest, test_filter_map_index) {
    std::vector<uint16_t> run_length_null_map = {0, 1, 3}; // 0, 1, 1, 1
    std::vector<uint8_t> filter_data = {0, 0, 1, 1, 1, 1};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());

    ColumnSelectVector select_vector;
    NullMap null_map;
    ASSERT_TRUE(select_vector.init(run_length_null_map, 4, &null_map, &filter_map, 2).ok());

    EXPECT_EQ(select_vector.num_filtered(), 0);

    ColumnSelectVector::DataReadType type;
    EXPECT_EQ(select_vector.get_next_run<true>(&type), 1);
    EXPECT_EQ(type, ColumnSelectVector::NULL_DATA);

    EXPECT_EQ(select_vector.get_next_run<true>(&type), 3);
    EXPECT_EQ(type, ColumnSelectVector::CONTENT);
}

TEST_F(ColumnSelectVectorTest, test_index_selection_merges_selection_and_null_runs) {
    const std::vector<uint16_t> null_runs = {2, 2, 3, 1};
    const std::vector<uint16_t> selection = {0, 2, 3, 6};
    NullMap null_map = {9};
    ColumnSelectVector select_vector;

    ASSERT_TRUE(select_vector
                        .init_from_selection(null_runs, 8, &null_map, selection.data(),
                                             selection.size())
                        .ok());
    EXPECT_TRUE(select_vector.has_filter());
    EXPECT_EQ(select_vector.num_values(), 8);
    EXPECT_EQ(select_vector.num_nulls(), 3);
    EXPECT_EQ(select_vector.num_filtered(), 4);
    EXPECT_EQ(null_map, (NullMap {9, 0, 1, 1, 0}));

    using ReadType = ColumnSelectVector::DataReadType;
    const std::vector<std::pair<size_t, ReadType>> expected_runs = {
            {1, ReadType::CONTENT},   {1, ReadType::FILTERED_CONTENT},
            {2, ReadType::NULL_DATA}, {2, ReadType::FILTERED_CONTENT},
            {1, ReadType::CONTENT},   {1, ReadType::FILTERED_NULL},
    };
    for (const auto& [expected_length, expected_type] : expected_runs) {
        ReadType type;
        EXPECT_EQ(select_vector.get_next_run<true>(&type), expected_length);
        EXPECT_EQ(type, expected_type);
    }
    ReadType type;
    EXPECT_EQ(select_vector.get_next_run<true>(&type), 0);
}

TEST_F(ColumnSelectVectorTest, test_dense_index_selection_uses_null_runs_directly) {
    const std::vector<uint16_t> null_runs = {2, 0, 1, 2, 1};
    NullMap null_map;
    ColumnSelectVector select_vector;

    ASSERT_TRUE(select_vector.init_from_selection(null_runs, 6, &null_map, nullptr, 6).ok());
    EXPECT_FALSE(select_vector.has_filter());
    EXPECT_EQ(null_map, (NullMap {0, 0, 0, 1, 1, 0}));

    using ReadType = ColumnSelectVector::DataReadType;
    ReadType type;
    EXPECT_EQ(select_vector.get_next_run<false>(&type), 2);
    EXPECT_EQ(type, ReadType::CONTENT);
    EXPECT_EQ(select_vector.get_next_run<false>(&type), 1);
    EXPECT_EQ(type, ReadType::CONTENT);
    EXPECT_EQ(select_vector.get_next_run<false>(&type), 2);
    EXPECT_EQ(type, ReadType::NULL_DATA);
    EXPECT_EQ(select_vector.get_next_run<false>(&type), 1);
    EXPECT_EQ(type, ReadType::CONTENT);
    EXPECT_EQ(select_vector.get_next_run<false>(&type), 0);
}

TEST_F(ColumnSelectVectorTest, test_empty_index_selection_skips_content_and_nulls) {
    const std::vector<uint16_t> null_runs = {2, 2, 3, 1};
    NullMap null_map = {7};
    ColumnSelectVector select_vector;

    ASSERT_TRUE(select_vector.init_from_selection(null_runs, 8, &null_map, nullptr, 0).ok());
    EXPECT_TRUE(select_vector.has_filter());
    EXPECT_EQ(select_vector.num_filtered(), 8);
    EXPECT_EQ(null_map, (NullMap {7}));

    using ReadType = ColumnSelectVector::DataReadType;
    ReadType type;
    EXPECT_EQ(select_vector.get_next_run<true>(&type), 2);
    EXPECT_EQ(type, ReadType::FILTERED_CONTENT);
    EXPECT_EQ(select_vector.get_next_run<true>(&type), 2);
    EXPECT_EQ(type, ReadType::FILTERED_NULL);
    EXPECT_EQ(select_vector.get_next_run<true>(&type), 3);
    EXPECT_EQ(type, ReadType::FILTERED_CONTENT);
    EXPECT_EQ(select_vector.get_next_run<true>(&type), 1);
    EXPECT_EQ(type, ReadType::FILTERED_NULL);
    EXPECT_EQ(select_vector.get_next_run<true>(&type), 0);
}

TEST_F(ColumnSelectVectorTest, test_empty_null_runs_mean_all_non_null) {
    const std::vector<uint16_t> null_runs;
    const std::vector<uint16_t> selection = {1, 4};
    NullMap null_map;
    ColumnSelectVector select_vector;

    ASSERT_TRUE(select_vector
                        .init_from_selection(null_runs, 6, &null_map, selection.data(),
                                             selection.size())
                        .ok());
    EXPECT_EQ(null_map, (NullMap {0, 0}));

    using ReadType = ColumnSelectVector::DataReadType;
    const std::vector<std::pair<size_t, ReadType>> expected_runs = {
            {1, ReadType::FILTERED_CONTENT}, {1, ReadType::CONTENT},
            {2, ReadType::FILTERED_CONTENT}, {1, ReadType::CONTENT},
            {1, ReadType::FILTERED_CONTENT},
    };
    for (const auto& [expected_length, expected_type] : expected_runs) {
        ReadType type;
        EXPECT_EQ(select_vector.get_next_run<true>(&type), expected_length);
        EXPECT_EQ(type, expected_type);
    }
}

TEST_F(ColumnSelectVectorTest, test_index_selection_rejects_invalid_contracts) {
    ColumnSelectVector select_vector;
    const std::vector<uint16_t> null_runs = {4};
    const std::vector<uint16_t> duplicate = {1, 1};
    const std::vector<uint16_t> descending = {2, 1};
    const std::vector<uint16_t> outside = {4};

    EXPECT_FALSE(select_vector.init_from_selection(null_runs, 4, nullptr, nullptr, 2).ok());
    EXPECT_FALSE(
            select_vector
                    .init_from_selection(null_runs, 4, nullptr, duplicate.data(), duplicate.size())
                    .ok());
    EXPECT_FALSE(select_vector
                         .init_from_selection(null_runs, 4, nullptr, descending.data(),
                                              descending.size())
                         .ok());
    EXPECT_FALSE(
            select_vector.init_from_selection(null_runs, 4, nullptr, outside.data(), outside.size())
                    .ok());
    EXPECT_FALSE(select_vector.init_from_selection(null_runs, 4, nullptr, nullptr, 5).ok());
    EXPECT_FALSE(
            select_vector.init_from_selection(std::vector<uint16_t> {3}, 4, nullptr, nullptr, 4)
                    .ok());
    EXPECT_FALSE(
            select_vector.init_from_selection(std::vector<uint16_t> {5}, 4, nullptr, nullptr, 4)
                    .ok());
}

TEST_F(ColumnSelectVectorTest, test_reinitialization_resets_selection_mode) {
    const std::vector<uint16_t> null_runs = {4};
    const std::vector<uint16_t> selection = {1};
    ColumnSelectVector select_vector;
    ASSERT_TRUE(
            select_vector
                    .init_from_selection(null_runs, 4, nullptr, selection.data(), selection.size())
                    .ok());

    std::vector<uint8_t> filter_data = {1, 0, 1, 0};
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ASSERT_TRUE(select_vector.init(null_runs, 4, nullptr, &filter_map, 0).ok());

    using ReadType = ColumnSelectVector::DataReadType;
    ReadType type;
    EXPECT_EQ(select_vector.get_next_run<true>(&type), 1);
    EXPECT_EQ(type, ReadType::CONTENT);
    EXPECT_EQ(select_vector.get_next_run<true>(&type), 1);
    EXPECT_EQ(type, ReadType::FILTERED_CONTENT);
}

TEST_F(ColumnSelectVectorTest, test_index_selection_exhaustive_small_batches) {
    using ReadType = ColumnSelectVector::DataReadType;
    for (size_t num_values = 1; num_values <= 6; ++num_values) {
        const size_t bitmap_count = size_t {1} << num_values;
        for (size_t null_bitmap = 0; null_bitmap < bitmap_count; ++null_bitmap) {
            std::vector<uint16_t> null_runs;
            bool current_run_is_null = false;
            uint16_t current_run_length = 0;
            for (size_t row = 0; row < num_values; ++row) {
                const bool row_is_null = ((null_bitmap >> row) & 1) != 0;
                if (row_is_null == current_run_is_null) {
                    ++current_run_length;
                    continue;
                }
                null_runs.push_back(current_run_length);
                current_run_length = 1;
                current_run_is_null = row_is_null;
            }
            null_runs.push_back(current_run_length);

            for (size_t selection_bitmap = 0; selection_bitmap < bitmap_count; ++selection_bitmap) {
                std::vector<uint16_t> selection;
                NullMap expected_null_map;
                std::vector<ReadType> expected_actions;
                for (size_t row = 0; row < num_values; ++row) {
                    const bool row_is_null = ((null_bitmap >> row) & 1) != 0;
                    const bool row_is_selected = ((selection_bitmap >> row) & 1) != 0;
                    if (row_is_selected) {
                        selection.push_back(static_cast<uint16_t>(row));
                        expected_null_map.push_back(row_is_null);
                    }
                    expected_actions.push_back(
                            row_is_selected
                                    ? (row_is_null ? ReadType::NULL_DATA : ReadType::CONTENT)
                                    : (row_is_null ? ReadType::FILTERED_NULL
                                                   : ReadType::FILTERED_CONTENT));
                }

                NullMap null_map;
                ColumnSelectVector select_vector;
                ASSERT_TRUE(select_vector
                                    .init_from_selection(null_runs, num_values, &null_map,
                                                         selection.data(), selection.size())
                                    .ok());
                EXPECT_EQ(null_map, expected_null_map);

                std::vector<ReadType> actual_actions;
                ReadType type;
                if (selection.size() == num_values) {
                    while (const size_t run_length = select_vector.get_next_run<false>(&type)) {
                        actual_actions.insert(actual_actions.end(), run_length, type);
                    }
                } else {
                    while (const size_t run_length = select_vector.get_next_run<true>(&type)) {
                        actual_actions.insert(actual_actions.end(), run_length, type);
                    }
                }
                EXPECT_EQ(actual_actions, expected_actions)
                        << "num_values=" << num_values << ", null_bitmap=" << null_bitmap
                        << ", selection_bitmap=" << selection_bitmap;
            }
        }
    }
}

} // namespace doris
