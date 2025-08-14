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

#include "olap/rowset/segment_v2/virtual_column_iterator.h"

#include <gtest/gtest.h>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nothing.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"

using namespace doris::segment_v2;

namespace doris::vectorized {
class VirtualColumnIteratorTest : public testing::Test {};

// Test the default constructor with ColumnNothing
TEST_F(VirtualColumnIteratorTest, TestDefaultConstructor) {
    VirtualColumnIterator iterator;
    vectorized::MutableColumnPtr dst = vectorized::ColumnString::create();

    // Create some rowids
    rowid_t rowids[] = {0, 1, 2, 3, 4};
    size_t count = sizeof(rowids) / sizeof(rowids[0]);

    // Since default is ColumnNothing, this should return OK immediately with no changes to dst
    Status status = iterator.read_by_rowids(rowids, count, dst);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(dst->size(), 0);
}

// Test init method
TEST_F(VirtualColumnIteratorTest, TestInit) {
    VirtualColumnIterator iterator;
    ColumnIteratorOptions opts;
    Status status = iterator.init(opts);
    ASSERT_TRUE(status.ok());
}

// Test with a materialized int32_t column
TEST_F(VirtualColumnIteratorTest, ReadByRowIdsint32_tColumn) {
    VirtualColumnIterator iterator;

    // Create a materialized int32_t column with values [10, 20, 30, 40, 50]
    auto int_column = vectorized::ColumnVector<TYPE_INT>::create();
    std::unique_ptr<std::vector<uint64_t>> labels = std::make_unique<std::vector<uint64_t>>();
    for (int i = 0; i < 5; i++) {
        int_column->insert_value(10 * (i + 1));
        labels->push_back(i);
    }
    // Set the materialized column

    iterator.prepare_materialization(std::move(int_column), std::move(labels));

    // Create destination column
    vectorized::MutableColumnPtr dst = vectorized::ColumnVector<TYPE_INT>::create();

    // Select rowids 0, 2, 4 (values 10, 30, 50)
    rowid_t rowids[] = {0, 2, 4};
    size_t count = sizeof(rowids) / sizeof(rowids[0]);
    DCHECK(count == 3);
    // Read selected rows
    Status status = iterator.read_by_rowids(rowids, count, dst);
    ASSERT_TRUE(status.ok());

    // Verify results
    EXPECT_EQ(dst->size(), 3);
    EXPECT_EQ(dst->get_int(0), 10);
    EXPECT_EQ(dst->get_int(1), 30);
    EXPECT_EQ(dst->get_int(2), 50);
}

// Test with a String column
TEST_F(VirtualColumnIteratorTest, ReadByRowIdsStringColumn) {
    VirtualColumnIterator iterator;

    // Create a materialized String column
    auto string_column = vectorized::ColumnString::create();
    string_column->insert_value("apple");
    string_column->insert_value("banana");
    string_column->insert_value("cherry");
    string_column->insert_value("date");
    string_column->insert_value("elderberry");
    auto labels = std::make_unique<std::vector<uint64_t>>();
    for (int i = 0; i < 5; i++) {
        labels->push_back(i);
    }

    // Set the materialized column
    iterator.prepare_materialization(std::move(string_column), std::move(labels));

    // Create destination column
    vectorized::MutableColumnPtr dst = vectorized::ColumnString::create();

    // Select rowids 1, 3 (values "banana", "date")
    rowid_t rowids[] = {1, 3};
    size_t count = sizeof(rowids) / sizeof(rowids[0]);

    // Read selected rows
    Status status = iterator.read_by_rowids(rowids, count, dst);
    ASSERT_TRUE(status.ok());

    // Verify results
    ASSERT_EQ(dst->size(), 2);
    ASSERT_EQ(dst->get_data_at(0).to_string(), "banana");
    ASSERT_EQ(dst->get_data_at(1).to_string(), "date");
}

// Test with empty rowids array
TEST_F(VirtualColumnIteratorTest, ReadByRowIdsEmptyRowIds) {
    VirtualColumnIterator iterator;

    // Create a materialized int32_t column with values [10, 20, 30, 40, 50]
    auto int_column = vectorized::ColumnVector<TYPE_INT>::create();
    auto labels = std::make_unique<std::vector<uint64_t>>();
    for (int i = 0; i < 5; i++) {
        int_column->insert_value(10 * (i + 1));
        labels->push_back(i);
    }

    // Set the materialized column
    iterator.prepare_materialization(std::move(int_column), std::move(labels));

    // Create destination column
    vectorized::MutableColumnPtr dst = vectorized::ColumnVector<TYPE_INT>::create();

    // Empty rowids array
    rowid_t rowids[1];
    size_t count = 0;

    // Read with empty rowids
    Status status = iterator.read_by_rowids(rowids, count, dst);
    ASSERT_TRUE(status.ok());

    // Verify empty result
    ASSERT_EQ(dst->size(), 0);
}

// Test with large number of rows
TEST_F(VirtualColumnIteratorTest, TestLargeRowset) {
    VirtualColumnIterator iterator;

    // Create a large materialized int32_t column (1000 values)
    auto int_column = vectorized::ColumnVector<TYPE_INT>::create();
    auto labels = std::make_unique<std::vector<uint64_t>>();

    for (int i = 0; i < 1000; i++) {
        int_column->insert_value(i);
        labels->push_back(i);
    }

    // Set the materialized column
    iterator.prepare_materialization(std::move(int_column), std::move(labels));

    // Create destination column
    vectorized::MutableColumnPtr dst = vectorized::ColumnVector<TYPE_INT>::create();

    // Select every 100th row (0, 100, 200, ... 900)
    const int step = 100;
    std::vector<rowid_t> rowids;
    for (int i = 0; i < 1000; i += step) {
        rowids.push_back(i);
    }

    // Read selected rows
    Status status = iterator.read_by_rowids(rowids.data(), rowids.size(), dst);
    ASSERT_TRUE(status.ok());

    // Verify results
    ASSERT_EQ(dst->size(), 10);
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(dst->get_int(i), i * step);
    }
}

TEST_F(VirtualColumnIteratorTest, ReadByRowIdsNoContinueRowIds) {
    // Create a column with 1000 values (0-999)
    auto column = ColumnVector<TYPE_INT>::create();
    auto labels = std::make_unique<std::vector<uint64_t>>();

    // Generate non-consecutive row IDs by multiplying by 2 (0,2,4,...)
    for (size_t i = 0; i < 1000; i++) {
        column->insert_value(i);
        labels->push_back(i * 2); // Non-consecutive row IDs
    }

    VirtualColumnIterator iterator;
    iterator.prepare_materialization(std::move(column), std::move(labels));

    // Verify row_id_to_idx mapping is correct
    for (size_t i = 0; i < 1000; i++) {
        const auto& row_id_to_idx = iterator.get_row_id_to_idx();
        ASSERT_EQ(row_id_to_idx.find(i * 2)->second, i);
    }

    // Create destination column for results
    vectorized::MutableColumnPtr dest_col = ColumnVector<TYPE_INT>::create();

    // Test with various non-consecutive row IDs
    {
        // Select some random non-consecutive row IDs (0, 100, 500, 998)
        rowid_t rowids[] = {0, 200, 1000, 1996};
        size_t count = sizeof(rowids) / sizeof(rowids[0]);

        // Read values by row IDs
        Status status = iterator.read_by_rowids(rowids, count, dest_col);
        ASSERT_TRUE(status.ok());

        // Verify results - values should be 0, 100, 500, 998
        ASSERT_EQ(dest_col->size(), count);
        ASSERT_EQ(dest_col->get_int(0), 0);
        ASSERT_EQ(dest_col->get_int(1), 100);
        ASSERT_EQ(dest_col->get_int(2), 500);
        ASSERT_EQ(dest_col->get_int(3), 998);
    }

    // Test with reversed order row IDs
    {
        dest_col->clear();

        // Row IDs in reverse order
        rowid_t rowids[] = {1998, 1500, 1000, 500, 0};
        size_t count = sizeof(rowids) / sizeof(rowids[0]);

        Status status = iterator.read_by_rowids(rowids, count, dest_col);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(dest_col->size(), count);
        ASSERT_EQ(dest_col->get_int(4), 999);
        ASSERT_EQ(dest_col->get_int(3), 750);
        ASSERT_EQ(dest_col->get_int(2), 500);
        ASSERT_EQ(dest_col->get_int(1), 250);
        ASSERT_EQ(dest_col->get_int(0), 0);
    }

    // Test with duplicate row IDs
    {
        dest_col->clear();

        // Duplicate row IDs
        rowid_t rowids[] = {100, 100, 100};
        size_t count = sizeof(rowids) / sizeof(rowids[0]);

        Status status = iterator.read_by_rowids(rowids, count, dest_col);
        ASSERT_TRUE(status.ok());

        // Verify results - should contain 3 copies of value 50
        ASSERT_EQ(dest_col->size(), 1); // Note: filter will deduplicate the rows
        ASSERT_EQ(dest_col->get_int(0), 50);
    }

    // Test with out-of-order and scattered row IDs
    {
        dest_col->clear();

        // Scattered row IDs
        rowid_t rowids[] = {8, 24, 46, 100, 1998};
        size_t count = sizeof(rowids) / sizeof(rowids[0]);

        Status status = iterator.read_by_rowids(rowids, count, dest_col);
        ASSERT_TRUE(status.ok());

        // Verify results
        ASSERT_EQ(dest_col->size(), count);
        ASSERT_EQ(dest_col->get_int(0), 4);
        ASSERT_EQ(dest_col->get_int(1), 12);
        ASSERT_EQ(dest_col->get_int(2), 23);
        ASSERT_EQ(dest_col->get_int(3), 50);
        ASSERT_EQ(dest_col->get_int(4), 999);
    }
}

TEST_F(VirtualColumnIteratorTest, NextBatchTest1) {
    VirtualColumnIterator iterator;

    // Construct an int32 column with 100 rows, values from 0 to 99
    auto int_column = vectorized::ColumnVector<TYPE_INT>::create();
    auto labels = std::make_unique<std::vector<uint64_t>>();
    for (int i = 0; i < 100; ++i) {
        int_column->insert_value(i);
        labels->push_back(i);
    }
    iterator.prepare_materialization(std::move(int_column), std::move(labels));

    // 1. Seek to row 10, next_batch reads 10 rows
    {
        vectorized::MutableColumnPtr dst = vectorized::ColumnVector<TYPE_INT>::create();
        Status st = iterator.seek_to_ordinal(10);
        ASSERT_TRUE(st.ok());
        size_t rows_read = 10;
        bool has_null = false;
        st = iterator.next_batch(&rows_read, dst, &has_null);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(rows_read, 10);
        ASSERT_EQ(dst->size(), 10);
        for (int i = 0; i < 10; ++i) {
            ASSERT_EQ(dst->get_int(i), 10 + i);
        }
    }

    // 2. Seek to row 85, next_batch reads 10 rows (only 15 rows remaining)
    {
        vectorized::MutableColumnPtr dst = vectorized::ColumnVector<TYPE_INT>::create();
        Status st = iterator.seek_to_ordinal(85);
        ASSERT_TRUE(st.ok());
        size_t rows_read = 10;
        bool has_null = false;
        st = iterator.next_batch(&rows_read, dst, &has_null);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(rows_read, 10);
        ASSERT_EQ(dst->size(), 10);
        for (int i = 0; i < 10; ++i) {
            EXPECT_EQ(dst->get_int(i), 85 + i);
        }
    }

    // 3. Seek to row 0, next_batch reads all 100 rows
    {
        vectorized::MutableColumnPtr dst = vectorized::ColumnVector<TYPE_INT>::create();
        Status st = iterator.seek_to_ordinal(0);
        ASSERT_TRUE(st.ok());
        size_t rows_read = 100;
        bool has_null = false;
        st = iterator.next_batch(&rows_read, dst, &has_null);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(rows_read, 100);
        ASSERT_EQ(dst->size(), 100);
        for (int i = 0; i < 100; ++i) {
            ASSERT_EQ(dst->get_int(i), i);
        }
    }

    // 4. Seek to out-of-bounds position (e.g., 100), should return error
    {
        vectorized::MutableColumnPtr dst = vectorized::ColumnVector<TYPE_INT>::create();
        Status st = iterator.seek_to_ordinal(100);
        ASSERT_EQ(st.ok(), false);
    }
}

TEST_F(VirtualColumnIteratorTest, TestPrepare1) {
    VirtualColumnIterator iterator;

    // Create a materialized int32_t column with values [10, 20, 30, 40, 50]
    auto int_column = vectorized::ColumnVector<TYPE_INT>::create();
    int_column->insert_value(10);
    int_column->insert_value(20);
    int_column->insert_value(30);
    int_column->insert_value(40);
    int_column->insert_value(50);
    auto labels = std::make_unique<std::vector<uint64_t>>();
    labels->push_back(100);
    labels->push_back(11);
    labels->push_back(33);
    labels->push_back(22);
    labels->push_back(55);
    // Set the materialized column
    iterator.prepare_materialization(std::move(int_column), std::move(labels));

    // Verify row_id_to_idx mapping
    const auto& row_id_to_idx = iterator.get_row_id_to_idx();
    ASSERT_EQ(row_id_to_idx.size(), 5);
    ASSERT_EQ(row_id_to_idx.find(11)->second, 0);
    ASSERT_EQ(row_id_to_idx.find(22)->second, 1);
    ASSERT_EQ(row_id_to_idx.find(33)->second, 2);
    ASSERT_EQ(row_id_to_idx.find(55)->second, 3);
    ASSERT_EQ(row_id_to_idx.find(100)->second, 4);

    auto materialization_col = iterator.get_materialized_column();
    auto int_col_m =
            assert_cast<const vectorized::ColumnVector<TYPE_INT>*>(materialization_col.get());
    ASSERT_EQ(int_col_m->get_data()[0], 20);
    ASSERT_EQ(int_col_m->get_data()[1], 40);
    ASSERT_EQ(int_col_m->get_data()[2], 30);
    ASSERT_EQ(int_col_m->get_data()[3], 50);
    ASSERT_EQ(int_col_m->get_data()[4], 10);
}

TEST_F(VirtualColumnIteratorTest, TestColumnNothing) {
    VirtualColumnIterator iterator;

    // Create a materialized int32_t column with values [10, 20, 30, 40, 50]
    auto int_column = vectorized::ColumnVector<TYPE_INT>::create();
    int_column->insert_value(10);
    int_column->insert_value(20);
    int_column->insert_value(30);
    int_column->insert_value(40);
    int_column->insert_value(50);
    auto labels = std::make_unique<std::vector<uint64_t>>();
    labels->push_back(100);
    labels->push_back(11);
    labels->push_back(33);
    labels->push_back(22);
    labels->push_back(55);
    // Set the materialized column
    iterator.prepare_materialization(std::move(int_column), std::move(labels));

    // Create destination column
    vectorized::MutableColumnPtr dst = vectorized::ColumnNothing::create(0);

    // Read by rowids, should return empty result
    rowid_t rowids[] = {11, 22, 33};
    size_t count = sizeof(rowids) / sizeof(rowids[0]);
    Status status = iterator.read_by_rowids(rowids, count, dst);
    ASSERT_TRUE(status.ok());
    auto tmp_nothing = vectorized::check_and_get_column<vectorized::ColumnNothing>(*dst);
    ASSERT_TRUE(tmp_nothing == nullptr);
    auto tmp_col_i32 = vectorized::check_and_get_column<vectorized::ColumnVector<TYPE_INT>>(
            *iterator.get_materialized_column());
    ASSERT_TRUE(tmp_col_i32 != nullptr);
    ASSERT_EQ(dst->size(), 3);
    ASSERT_EQ(tmp_col_i32->get_data()[0], 20);
    ASSERT_EQ(tmp_col_i32->get_data()[1], 40);
    ASSERT_EQ(tmp_col_i32->get_data()[2], 30);
}

// Test the combination of seek_to_ordinal + next_batch behavior
// NOTE: next_batch only works when row IDs are consecutive
TEST_F(VirtualColumnIteratorTest, SeekAndNextBatchCombination) {
    VirtualColumnIterator iterator;

    // Create test data with CONSECUTIVE global row IDs
    // Original column values: [100, 200, 300, 400]
    // Global row IDs:         [0,   1,   2,   3]  (consecutive)
    auto int_column = vectorized::ColumnVector<TYPE_INT>::create();
    std::vector<int> values = {100, 200, 300, 400};
    std::vector<uint64_t> global_row_ids = {0, 1, 2, 3}; // Consecutive row IDs

    for (int val : values) {
        int_column->insert_value(val);
    }

    auto labels = std::make_unique<std::vector<uint64_t>>();
    for (uint64_t id : global_row_ids) {
        labels->push_back(id);
    }

    iterator.prepare_materialization(std::move(int_column), std::move(labels));

    // Row IDs are already in order: [0, 1, 2, 3]
    // Corresponding values:         [100, 200, 300, 400]
    // _row_id_to_idx mapping: {0->0, 1->1, 2->2, 3->3}

    // Test 1: Seek to ordinal 1, then read 2 rows
    {
        vectorized::MutableColumnPtr dst = vectorized::ColumnVector<TYPE_INT>::create();
        Status st = iterator.seek_to_ordinal(1);
        ASSERT_TRUE(st.ok());

        size_t rows_to_read = 2;
        bool has_null = false;
        st = iterator.next_batch(&rows_to_read, dst, &has_null);
        ASSERT_TRUE(st.ok());

        ASSERT_EQ(dst->size(), 2);
        EXPECT_EQ(dst->get_int(0), 200); // ordinal 1 -> value 200
        EXPECT_EQ(dst->get_int(1), 300); // ordinal 2 -> value 300
    }

    // Test 2: Multiple consecutive next_batch calls
    {
        Status st = iterator.seek_to_ordinal(0);
        ASSERT_TRUE(st.ok());

        // First next_batch: read 1 row
        vectorized::MutableColumnPtr dst1 = vectorized::ColumnVector<TYPE_INT>::create();
        size_t rows_to_read = 1;
        bool has_null = false;
        st = iterator.next_batch(&rows_to_read, dst1, &has_null);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(dst1->size(), 1);
        EXPECT_EQ(dst1->get_int(0), 100); // ordinal 0 -> value 100

        // Second next_batch: read 2 more rows (current_ordinal should be 1 now)
        vectorized::MutableColumnPtr dst2 = vectorized::ColumnVector<TYPE_INT>::create();
        rows_to_read = 2;
        st = iterator.next_batch(&rows_to_read, dst2, &has_null);
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_EQ(dst2->size(), 2);
        EXPECT_EQ(dst2->get_int(0), 200); // ordinal 1 -> value 200
        EXPECT_EQ(dst2->get_int(1), 300); // ordinal 2 -> value 300
    }
}

// Test read_by_rowids with different scenarios
TEST_F(VirtualColumnIteratorTest, ReadByRowidsComprehensive) {
    VirtualColumnIterator iterator;

    // Create test data with gaps in global row IDs
    // Original column values: [1000, 2000, 3000, 4000]
    // Global row IDs:         [100,  50,   200,  25]
    auto int_column = vectorized::ColumnVector<TYPE_INT>::create();
    std::vector<int> values = {1000, 2000, 3000, 4000};
    std::vector<uint64_t> global_row_ids = {100, 50, 200, 25};

    for (int val : values) {
        int_column->insert_value(val);
    }

    auto labels = std::make_unique<std::vector<uint64_t>>();
    for (uint64_t id : global_row_ids) {
        labels->push_back(id);
    }

    iterator.prepare_materialization(std::move(int_column), std::move(labels));

    // After sorting by global_row_id: [25, 50, 100, 200]
    // Corresponding original values:  [4000, 2000, 1000, 3000]
    // _row_id_to_idx mapping: {25->0, 50->1, 100->2, 200->3}

    // Test 1: Read by multiple rowids in descending order
    {
        vectorized::MutableColumnPtr dst = vectorized::ColumnVector<TYPE_INT>::create();
        rowid_t rowids[] = {200, 25, 100};
        Status status = iterator.read_by_rowids(rowids, 3, dst);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(dst->size(), 3);
        EXPECT_EQ(dst->get_int(0), 4000); // global_row_id 25 -> original value 4000
        EXPECT_EQ(dst->get_int(1), 1000); // global_row_id 100 -> original value 1000
        EXPECT_EQ(dst->get_int(2), 3000); // global_row_id 200 -> original value 3000
    }

    // Test 2: Read by duplicate rowids (should work due to filter logic)
    {
        vectorized::MutableColumnPtr dst = vectorized::ColumnVector<TYPE_INT>::create();
        rowid_t rowids[] = {100, 100, 100};
        Status status = iterator.read_by_rowids(rowids, 3, dst);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(dst->size(), 1);        // Filter deduplicates
        EXPECT_EQ(dst->get_int(0), 1000); // global_row_id 100 -> original value 1000
    }
}

// Test combination of seek_to_ordinal + next_batch + read_by_rowids
// NOTE: next_batch requires consecutive row IDs, read_by_rowids works with any row IDs
TEST_F(VirtualColumnIteratorTest, MixedOperationsCombination) {
    VirtualColumnIterator iterator;

    // Test data with consecutive row IDs for next_batch testing
    auto int_column = vectorized::ColumnVector<TYPE_INT>::create();
    std::vector<int> values = {10, 20, 30, 40};
    std::vector<uint64_t> global_row_ids = {0, 1, 2, 3}; // Consecutive for next_batch

    for (int val : values) {
        int_column->insert_value(val);
    }

    auto labels = std::make_unique<std::vector<uint64_t>>();
    for (uint64_t id : global_row_ids) {
        labels->push_back(id);
    }

    iterator.prepare_materialization(std::move(int_column), std::move(labels));

    // Row IDs are consecutive: [0, 1, 2, 3], values [10, 20, 30, 40]
    // _row_id_to_idx: {0->0, 1->1, 2->2, 3->3}

    // Operation 1: read_by_rowids with specific row IDs
    {
        vectorized::MutableColumnPtr dst = vectorized::ColumnVector<TYPE_INT>::create();
        rowid_t rowids[] = {1, 3};
        Status status = iterator.read_by_rowids(rowids, 2, dst);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(dst->size(), 2);
        EXPECT_EQ(dst->get_int(0), 20); // row_id 1 -> value 20
        EXPECT_EQ(dst->get_int(1), 40); // row_id 3 -> value 40
    }

    // Operation 2: seek and next_batch (works because row IDs are consecutive)
    {
        vectorized::MutableColumnPtr dst = vectorized::ColumnVector<TYPE_INT>::create();
        Status st = iterator.seek_to_ordinal(0);
        ASSERT_TRUE(st.ok());

        size_t rows_to_read = 2;
        bool has_null = false;
        st = iterator.next_batch(&rows_to_read, dst, &has_null);
        ASSERT_TRUE(st.ok());
        ASSERT_EQ(dst->size(), 2);
        EXPECT_EQ(dst->get_int(0), 10); // ordinal 0 -> value 10
        EXPECT_EQ(dst->get_int(1), 20); // ordinal 1 -> value 20
    }
}

// Test dst column is ColumnNothing in different scenarios
TEST_F(VirtualColumnIteratorTest, DstColumnNothingHandling) {
    VirtualColumnIterator iterator;

    // Create test data with consecutive row IDs for next_batch compatibility
    auto int_column = vectorized::ColumnVector<TYPE_INT>::create();
    std::vector<int> values = {100, 200, 300};
    std::vector<uint64_t> global_row_ids = {0, 1, 2}; // Consecutive for next_batch

    for (int val : values) {
        int_column->insert_value(val);
    }

    auto labels = std::make_unique<std::vector<uint64_t>>();
    for (uint64_t id : global_row_ids) {
        labels->push_back(id);
    }

    iterator.prepare_materialization(std::move(int_column), std::move(labels));

    // Row IDs are consecutive: [0, 1, 2] -> [100, 200, 300]

    // Test 1: next_batch with ColumnNothing dst
    {
        vectorized::MutableColumnPtr dst = vectorized::ColumnNothing::create(0);
        Status st = iterator.seek_to_ordinal(0);
        ASSERT_TRUE(st.ok());

        size_t rows_to_read = 2;
        bool has_null = false;
        st = iterator.next_batch(&rows_to_read, dst, &has_null);
        ASSERT_TRUE(st.ok());

        // dst should be replaced with materialized column data
        auto nothing_check = vectorized::check_and_get_column<vectorized::ColumnNothing>(*dst);
        EXPECT_EQ(nothing_check, nullptr); // Should not be ColumnNothing anymore
        ASSERT_EQ(dst->size(), 2);
        EXPECT_EQ(dst->get_int(0), 100); // ordinal 0 -> value 100
        EXPECT_EQ(dst->get_int(1), 200); // ordinal 1 -> value 200
    }

    // Test 2: read_by_rowids with ColumnNothing dst
    {
        vectorized::MutableColumnPtr dst = vectorized::ColumnNothing::create(0);
        rowid_t rowids[] = {1, 2};
        Status status = iterator.read_by_rowids(rowids, 2, dst);
        ASSERT_TRUE(status.ok());

        // dst should be replaced with filtered results
        auto nothing_check = vectorized::check_and_get_column<vectorized::ColumnNothing>(*dst);
        EXPECT_EQ(nothing_check, nullptr); // Should not be ColumnNothing anymore
        ASSERT_EQ(dst->size(), 2);
        EXPECT_EQ(dst->get_int(0), 200); // row_id 1 -> value 200
        EXPECT_EQ(dst->get_int(1), 300); // row_id 2 -> value 300
    }

    // Test 3: Empty read_by_rowids with ColumnNothing dst (should remain ColumnNothing)
    {
        vectorized::MutableColumnPtr dst = vectorized::ColumnNothing::create(0);
        rowid_t rowids[1]; // Empty array
        Status status = iterator.read_by_rowids(rowids, 0, dst);
        ASSERT_TRUE(status.ok());

        // dst should remain ColumnNothing for empty results
        auto nothing_check = vectorized::check_and_get_column<vectorized::ColumnNothing>(*dst);
        EXPECT_NE(nothing_check, nullptr); // Should still be ColumnNothing
        ASSERT_EQ(dst->size(), 0);
    }
}
} // namespace doris::vectorized