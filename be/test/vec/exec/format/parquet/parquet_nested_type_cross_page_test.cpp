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

#include "vec/exec/format/parquet/parquet_common.h"

namespace doris::vectorized {
class ParquetCrossPageTest : public testing::Test {
protected:
    void SetUp() override {
        // filter_data is row-level, corresponding to 8 rows
        filter_data = {1, 0, 0, 1, 0, 0, 1, 1}; // filter conditions for 8 rows

        // Page 1: 2 complete rows + 1 incomplete row
        page1_rep_levels = {
                0, 1, 1, // Row 1: [1,1,1]
                0, 1,    // Row 2: [1,1]
                0, 1, 1  // Row 3: [1,1,to be continued...]
        };

        // Page 2: continue Row 3 + 2 complete rows + 1 incomplete row
        page2_rep_levels = {
                1, 1,    // Continue Row 3: [...1,1]
                0, 1,    // Row 4: [1]
                0, 1, 1, // Row 5: [1,1,1]
                0, 1     // Row 6: [1,to be continued...]
        };

        // Page 3: continue Row 6 + 2 complete rows
        page3_rep_levels = {
                1, 1,    // Continue Row 6: [...1,1]
                0, 1, 1, // Row 7: [1,1]
                0, 1     // Row 8: [1]
        };
    }

    std::vector<uint8_t> filter_data; // Row-level filter conditions for all data
    std::vector<level_t> page1_rep_levels;
    std::vector<level_t> page2_rep_levels;
    std::vector<level_t> page3_rep_levels;
};

// Test complete processing of three pages
TEST_F(ParquetCrossPageTest, test_three_pages_complete) {
    size_t current_row = 0;

    // Process first page
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());

    std::vector<uint8_t> nested_data1;
    std::unique_ptr<FilterMap> nested_map1;

    ASSERT_TRUE(filter_map
                        .generate_nested_filter_map(page1_rep_levels, nested_data1, &nested_map1,
                                                    &current_row, 0)
                        .ok());

    // Verify first page results - using corresponding rows from overall filter_data
    std::vector<uint8_t> expected1 = {1, 1, 1, 0, 0, 0, 0, 0};
    EXPECT_EQ(nested_data1, expected1);
    EXPECT_EQ(current_row, 2); // Processed up to Row 3

    // Process second page - continue with same filter_map
    std::vector<uint8_t> nested_data2;
    std::unique_ptr<FilterMap> nested_map2;

    ASSERT_TRUE(filter_map
                        .generate_nested_filter_map(page2_rep_levels, nested_data2, &nested_map2,
                                                    &current_row, 0)
                        .ok());

    // Verify second page results
    std::vector<uint8_t> expected2 = {0, 0, 1, 1, 0, 0, 0, 0, 0};
    EXPECT_EQ(nested_data2, expected2);
    EXPECT_EQ(current_row, 5); // Processed up to Row 6

    // Process third page - continue with same filter_map
    std::vector<uint8_t> nested_data3;
    std::unique_ptr<FilterMap> nested_map3;

    ASSERT_TRUE(filter_map
                        .generate_nested_filter_map(page3_rep_levels, nested_data3, &nested_map3,
                                                    &current_row, 0)
                        .ok());

    // Verify third page results
    std::vector<uint8_t> expected3 = {0, 0, 1, 1, 1, 1, 1};
    EXPECT_EQ(nested_data3, expected3);
    EXPECT_EQ(current_row, 7); // Processed all 8 rows
}

// Test case where a single row spans three pages
TEST_F(ParquetCrossPageTest, test_single_row_across_three_pages) {
    // Filter for one long array row
    std::vector<uint8_t> row_filter = {1, 0}; // Only 2 rows of data

    // First page
    std::vector<level_t> rep1 = {
            0,       // Start of first row
            1, 1, 1, // 3 array elements
            1, 1     // To be continued...
    };

    // Second page
    std::vector<level_t> rep2 = {
            1, 1, 1, // Continue with 3 array elements
            1, 1, 1  // To be continued...
    };

    // Third page
    std::vector<level_t> rep3 = {
            1, 1, 1, // Last 3 array elements
            0        // Start of second row
    };

    size_t current_row = 0;

    // Use same filter_map for all pages
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(row_filter.data(), row_filter.size(), false).ok());

    // Process first page
    std::vector<uint8_t> nested_data1;
    std::unique_ptr<FilterMap> nested_map1;

    ASSERT_TRUE(
            filter_map.generate_nested_filter_map(rep1, nested_data1, &nested_map1, &current_row, 0)
                    .ok());

    // Verify first page results
    std::vector<uint8_t> expected1(rep1.size(), 1); // All use first row's filter value
    EXPECT_EQ(nested_data1, expected1);
    EXPECT_EQ(current_row, 0); // Still in first row

    // Process second page
    std::vector<uint8_t> nested_data2;
    std::unique_ptr<FilterMap> nested_map2;

    ASSERT_TRUE(
            filter_map.generate_nested_filter_map(rep2, nested_data2, &nested_map2, &current_row, 0)
                    .ok());

    // Verify second page results
    std::vector<uint8_t> expected2(rep2.size(), 1); // Still using first row's filter value
    EXPECT_EQ(nested_data2, expected2);
    EXPECT_EQ(current_row, 0); // Still in first row

    // Process third page
    std::vector<uint8_t> nested_data3;
    std::unique_ptr<FilterMap> nested_map3;

    ASSERT_TRUE(
            filter_map.generate_nested_filter_map(rep3, nested_data3, &nested_map3, &current_row, 0)
                    .ok());

    // Verify third page results
    std::vector<uint8_t> expected3(rep3.size(), 1);
    expected3.back() = 0; // Last element uses second row's filter value
    EXPECT_EQ(nested_data3, expected3);
    EXPECT_EQ(current_row, 1); // Moved to second row
}

} // namespace doris::vectorized
