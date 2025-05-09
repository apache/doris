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

#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"

namespace doris {
namespace segment_v2 {

class VirtualColumnIteratorTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

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

// Test with a materialized int32_t column
TEST_F(VirtualColumnIteratorTest, TestWithint32_tColumn) {
    VirtualColumnIterator iterator;

    // Create a materialized int32_t column with values [10, 20, 30, 40, 50]
    auto int_column = vectorized::ColumnVector<int32_t>::create();
    for (int i = 0; i < 5; i++) {
        int_column->insert(10 * (i + 1));
    }

    std::cout << "Init virtual column: " << int_column->dump_structure() << std::endl;
    // Set the materialized column
    iterator.set_materialized_column(std::move(int_column));

    // Create destination column
    vectorized::MutableColumnPtr dst = vectorized::ColumnVector<int32_t>::create();

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
TEST_F(VirtualColumnIteratorTest, TestWithStringColumn) {
    VirtualColumnIterator iterator;

    // Create a materialized String column
    auto string_column = vectorized::ColumnString::create();
    string_column->insert("apple");
    string_column->insert("banana");
    string_column->insert("cherry");
    string_column->insert("date");
    string_column->insert("elderberry");

    // Set the materialized column
    iterator.set_materialized_column(std::move(string_column));

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
TEST_F(VirtualColumnIteratorTest, TestEmptyRowIds) {
    VirtualColumnIterator iterator;

    // Create a materialized int32_t column with values [10, 20, 30, 40, 50]
    auto int_column = vectorized::ColumnVector<int32_t>::create();
    for (int i = 0; i < 5; i++) {
        int_column->insert(10 * (i + 1));
    }

    // Set the materialized column
    iterator.set_materialized_column(std::move(int_column));

    // Create destination column
    vectorized::MutableColumnPtr dst = vectorized::ColumnVector<int32_t>::create();

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
    auto int_column = vectorized::ColumnVector<int32_t>::create();
    for (int i = 0; i < 1000; i++) {
        int_column->insert(i);
    }

    // Set the materialized column
    iterator.set_materialized_column(std::move(int_column));

    // Create destination column
    vectorized::MutableColumnPtr dst = vectorized::ColumnVector<int32_t>::create();

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

} // namespace segment_v2
} // namespace doris