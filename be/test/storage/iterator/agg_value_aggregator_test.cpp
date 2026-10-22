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

// Verifies that the buffer flushes of AggValueAggregator (add_row() flushing when the buffered
// rows reach `max_buffered_rows` or the source Block ends) do not corrupt the aggregate of a key
// group that spans several flush windows, and that closed groups are emitted in order.

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#include "storage/iterator/agg_key_block_iterator.h"
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "storage/schema.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

namespace {

// Builds a 2-column source block: key (Int64, `key_value` for every row) and value (Int64,
// 1..n_rows). The value column is what the aggregator sums.
std::shared_ptr<Block> make_source_block(size_t n_rows, int64_t key_value) {
    auto block = std::make_shared<Block>();
    auto type = std::make_shared<DataTypeInt64>();
    auto key_col = ColumnInt64::create();
    auto val_col = ColumnInt64::create();
    for (size_t i = 0; i < n_rows; ++i) {
        key_col->insert_value(key_value);
        val_col->insert_value(static_cast<int64_t>(i + 1));
    }
    block->insert({std::move(key_col), type, "k"});
    block->insert({std::move(val_col), type, "v"});
    return block;
}

// Read schema of an AGG_KEYS table {k BIGINT key, v BIGINT SUM}.
ReadSchemaSPtr make_read_schema() {
    auto key = std::make_shared<TabletColumn>(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                                              FieldType::OLAP_FIELD_TYPE_BIGINT, false, 0,
                                              sizeof(int64_t));
    key->set_name("k");
    key->set_is_key(true);
    auto val = std::make_shared<TabletColumn>(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM,
                                              FieldType::OLAP_FIELD_TYPE_BIGINT, false, 1,
                                              sizeof(int64_t));
    val->set_name("v");
    auto type = std::make_shared<DataTypeInt64>();
    return std::make_shared<ReadSchema>(std::vector<TabletColumnPtr> {key, val},
                                        std::vector<DataTypePtr> {type, type});
}

MutableColumns make_target_columns() {
    MutableColumns cols;
    cols.emplace_back(ColumnInt64::create()); // key (untouched by the aggregator)
    cols.emplace_back(ColumnInt64::create()); // aggregated value
    return cols;
}

int64_t read_int64(const IColumn& col, size_t row) {
    return assert_cast<const ColumnInt64&>(col).get_data()[row];
}

// An aggregator over the value column of make_read_schema() with the given buffer size.
std::unique_ptr<AggValueAggregator> make_aggregator(size_t max_buffered_rows) {
    auto aggregator = std::make_unique<AggValueAggregator>();
    EXPECT_TRUE(aggregator
                        ->init(*make_read_schema(), {1}, BeExecVersionManager::get_newest_version(),
                               max_buffered_rows)
                        .ok());
    return aggregator;
}

// Feeds rows [begin, end) of `source` as one open group; returns how many flushes add_row()
// triggered, observed as the buffered rows shrinking instead of growing by one.
int add_rows(AggValueAggregator& aggregator, const std::shared_ptr<Block>& source, size_t begin,
             size_t end, MutableColumns& out) {
    int flushes = 0;
    size_t buffered_before = aggregator._buffered_rows.size();
    for (size_t i = begin; i < end; ++i) {
        aggregator.add_row(
                IteratorRowRef {
                        .block = source, .row_pos = static_cast<int>(i), .is_same = i > begin},
                out);
        const size_t buffered_after = aggregator._buffered_rows.size();
        if (buffered_after < buffered_before + 1) {
            ++flushes;
        }
        buffered_before = buffered_after;
    }
    return flushes;
}

} // namespace

// Sanity baseline: a single group whose size is below the buffer size only sees the
// end-of-Block flush. Verifies the fixture itself is wired correctly.
TEST(AggValueAggregatorTest, NoMidGroupFlushAggregatesCorrectly) {
    constexpr size_t kMaxBufferedRows = 16;
    constexpr size_t kRows = 5;

    auto source = make_source_block(kRows, /*key_value=*/42);
    auto aggregator = make_aggregator(kMaxBufferedRows);
    auto target_columns = make_target_columns();

    EXPECT_EQ(add_rows(*aggregator, source, 0, kRows, target_columns), 1);
    // The end-of-Block flush already drained the buffer into the open group's state without
    // emitting anything.
    EXPECT_EQ(aggregator->_buffered_rows.size(), 0);
    EXPECT_EQ(aggregator->_open_group_rows, 0);
    EXPECT_EQ(target_columns[1]->size(), 0);

    aggregator->close_group();
    aggregator->flush(target_columns);

    ASSERT_EQ(target_columns[1]->size(), 1);
    EXPECT_EQ(read_int64(*target_columns[1], 0), 1 + 2 + 3 + 4 + 5);
}

// A single group of 10 rows with a buffer of 4 flushes mid-group at rows 4 and 8, plus the
// end-of-Block flush at row 10. The final result must still be the full sum 1..10 = 55.
TEST(AggValueAggregatorTest, PeriodicFlushPreservesAggregateAcrossWindows) {
    constexpr size_t kMaxBufferedRows = 4;
    constexpr size_t kRows = 10;

    auto source = make_source_block(kRows, /*key_value=*/7);
    auto aggregator = make_aggregator(kMaxBufferedRows);
    auto target_columns = make_target_columns();

    EXPECT_EQ(add_rows(*aggregator, source, 0, kRows, target_columns), 3);
    EXPECT_EQ(aggregator->_buffered_rows.size(), 0);
    EXPECT_EQ(aggregator->_open_group_rows, 0);

    aggregator->close_group();
    aggregator->flush(target_columns);

    ASSERT_EQ(target_columns[1]->size(), 1);
    EXPECT_EQ(read_int64(*target_columns[1], 0), 55);
}

// Stress: a single group long enough to trigger many full periodic flushes, followed by the
// group end. Catches off-by-one bugs in the chunked aggregation.
TEST(AggValueAggregatorTest, PeriodicFlushManyWindowsSingleGroup) {
    constexpr size_t kMaxBufferedRows = 4;
    constexpr size_t kRows = 100; // 25 full windows

    auto source = make_source_block(kRows, /*key_value=*/3);
    auto aggregator = make_aggregator(kMaxBufferedRows);
    auto target_columns = make_target_columns();

    add_rows(*aggregator, source, 0, kRows, target_columns);
    aggregator->close_group();
    aggregator->flush(target_columns);

    ASSERT_EQ(target_columns[1]->size(), 1);
    EXPECT_EQ(read_int64(*target_columns[1], 0), static_cast<int64_t>(kRows) * (kRows + 1) / 2);
}

// Several groups inside one buffer window: close_group() delimits them and flush() emits one
// result row per closed group, in order, while the still open group keeps accumulating.
TEST(AggValueAggregatorTest, ClosedGroupsAreEmittedInOrder) {
    constexpr size_t kMaxBufferedRows = 8;
    auto source = make_source_block(/*n_rows=*/6, /*key_value=*/1);
    auto aggregator = make_aggregator(kMaxBufferedRows);
    auto target_columns = make_target_columns();

    // Groups: rows [0,2) -> 1+2, rows [2,5) -> 3+4+5, rows [5,6) -> 6 (open).
    add_rows(*aggregator, source, 0, 2, target_columns);
    aggregator->close_group();
    add_rows(*aggregator, source, 2, 5, target_columns);
    aggregator->close_group();
    // Row 5 is the Block's last row: add_row() flushes, which emits the two closed groups and
    // keeps the open group's partial sum.
    add_rows(*aggregator, source, 5, 6, target_columns);
    ASSERT_EQ(target_columns[1]->size(), 2);
    EXPECT_EQ(read_int64(*target_columns[1], 0), 3);
    EXPECT_EQ(read_int64(*target_columns[1], 1), 12);

    // Closing a group whose rows were all materialized by an earlier flush still emits it.
    aggregator->close_group();
    aggregator->flush(target_columns);
    ASSERT_EQ(target_columns[1]->size(), 3);
    EXPECT_EQ(read_int64(*target_columns[1], 2), 6);
}

} // namespace doris
