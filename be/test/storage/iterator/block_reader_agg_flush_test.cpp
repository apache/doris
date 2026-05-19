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

// Verifies that BlockReader's aggregation buffer flush triggered by
// `_stored_row_ref.size() == batch_max_rows()` (block_reader.cpp:639) does not
// corrupt the final aggregated value when a single agg group spans multiple
// flush windows. Drives `_append_agg_data` / `_update_agg_data` directly.

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define private public
#define protected public
#include "storage/iterator/block_reader.h"
#undef private
#undef protected
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/config.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {

namespace {

// Builds a 2-column source block: key (Int64, all set to `key_value`) and value
// (Int64, set to 1..n_rows). The value column is what we aggregate over.
std::unique_ptr<Block> make_source_block(size_t n_rows, int64_t key_value) {
    auto block = Block::create_unique();
    auto key_type = std::make_shared<DataTypeInt64>();
    auto val_type = std::make_shared<DataTypeInt64>();

    auto key_col = ColumnInt64::create();
    auto val_col = ColumnInt64::create();
    for (size_t i = 0; i < n_rows; ++i) {
        key_col->insert_value(key_value);
        val_col->insert_value(static_cast<int64_t>(i + 1));
    }
    block->insert({std::move(key_col), key_type, "k"});
    block->insert({std::move(val_col), val_type, "v"});
    return block;
}

// Mirror BlockReader::_init_agg_state's _stored_data_columns sizing: cloned
// struct of the source block, pre-filled with `n_rows` default rows so that
// non-variable-length agg columns can be written via replace_column_data.
MutableColumns make_stored_columns(const Block& src_block, size_t n_rows) {
    return src_block.create_same_struct_block(n_rows)->mutate_columns();
}

MutableColumns make_target_columns() {
    MutableColumns cols;
    cols.push_back(ColumnInt64::create()); // key (untouched in this test)
    cols.push_back(ColumnInt64::create()); // agg result column
    return cols;
}

// Configure `reader` as if it had completed init() for an AGG_KEYS table with
// schema {key: Int64, value: Int64} and SUM aggregation over `value`.
void configure_reader_for_int64_sum(BlockReader& reader, const Block& src_block,
                                    size_t batch_max_rows) {
    reader._reader_context.batch_size = batch_max_rows;
    // Adaptive disabled so batch_max_rows() == _reader_context.batch_size.
    config::enable_adaptive_batch_size = false;

    // Column layout: [0]=key, [1]=agg value. Output layout matches input.
    reader._normal_columns_idx = {0};
    reader._agg_columns_idx = {1};
    reader._return_columns_loc = {0, 1};

    reader._stored_data_columns = make_stored_columns(src_block, batch_max_rows);
    reader._stored_has_null_tag.assign(reader._stored_data_columns.size(), false);
    reader._stored_has_variable_length_tag.assign(reader._stored_data_columns.size(), false);

    auto fn = AggregateFunctionSimpleFactory::instance().get(
            "sum", {std::make_shared<DataTypeInt64>()}, std::make_shared<DataTypeInt64>(),
            /*result_nullable=*/false, BeExecVersionManager::get_newest_version(),
            {.column_names = {}});
    ASSERT_TRUE(fn != nullptr);

    auto* place = new char[fn->size_of_data()];
    fn->create(place);
    reader._agg_functions.push_back(fn);
    reader._agg_places.push_back(place);
    // Destructor (BlockReader::~BlockReader) cleans up _agg_places.
}

int64_t read_int64(const IColumn& col, size_t row) {
    return assert_cast<const ColumnInt64&>(col).get_data()[row];
}

} // namespace

class BlockReaderAggFlushTest : public testing::Test {
protected:
    void SetUp() override { _saved_enable_adaptive = config::enable_adaptive_batch_size; }

    void TearDown() override { config::enable_adaptive_batch_size = _saved_enable_adaptive; }

    bool _saved_enable_adaptive = false;
};

// Sanity baseline: a single group whose size is below batch_max_rows triggers
// no mid-group flush. Verifies the test fixture itself is wired correctly.
TEST_F(BlockReaderAggFlushTest, NoMidGroupFlushAggregatesCorrectly) {
    constexpr size_t kBatchMaxRows = 16;
    constexpr size_t kRows = 5; // < batch_max_rows, only is_last flush fires

    BlockReader reader;
    auto src_block = make_source_block(kRows, /*key_value=*/42);
    configure_reader_for_int64_sum(reader, *src_block, kBatchMaxRows);

    auto target_columns = make_target_columns();

    for (size_t i = 0; i < kRows; ++i) {
        reader._next_row.block = std::shared_ptr<Block>(src_block.get(), [](Block*) {});
        reader._next_row.row_pos = static_cast<int>(i);
        reader._next_row.is_same = (i > 0);
        reader._append_agg_data(target_columns);
    }

    // is_last flush at i=4 already drained _stored_row_ref into the aggregator
    // without finalizing (because _last_agg_data_counter > 0).
    EXPECT_EQ(reader._stored_row_ref.size(), 0);
    EXPECT_EQ(reader._last_agg_data_counter, 0);

    // Mimic `_agg_key_next_block` end-of-group close.
    reader._agg_data_counters.push_back(reader._last_agg_data_counter);
    reader._last_agg_data_counter = 0;
    reader._update_agg_data(target_columns);

    ASSERT_EQ(target_columns[1]->size(), 1);
    EXPECT_EQ(read_int64(*target_columns[1], 0), 1 + 2 + 3 + 4 + 5);
}

// The interesting case: a single group of 10 rows with batch_max_rows=4 forces
// `_stored_row_ref.size() == batch_max_rows()` to fire mid-group at i=3 and
// i=7, plus an `is_last` flush at i=9. Final close must still emit the full
// sum 1..10 = 55.
TEST_F(BlockReaderAggFlushTest, PeriodicFlushPreservesAggregateAcrossWindows) {
    constexpr size_t kBatchMaxRows = 4;
    constexpr size_t kRows = 10;

    BlockReader reader;
    auto src_block = make_source_block(kRows, /*key_value=*/7);
    configure_reader_for_int64_sum(reader, *src_block, kBatchMaxRows);

    auto target_columns = make_target_columns();

    int flush_count = 0;
    int prev_size = 0;
    for (size_t i = 0; i < kRows; ++i) {
        reader._next_row.block = std::shared_ptr<Block>(src_block.get(), [](Block*) {});
        reader._next_row.row_pos = static_cast<int>(i);
        reader._next_row.is_same = (i > 0);
        reader._append_agg_data(target_columns);

        // A flush happens whenever _stored_row_ref shrinks (it's pushed to
        // first, then potentially cleared by _update_agg_data).
        int cur_size = static_cast<int>(reader._stored_row_ref.size());
        if (cur_size < prev_size + 1) {
            ++flush_count;
        }
        prev_size = cur_size;
    }

    // Expected flushes: at i=3 (size==4), i=7 (size==4), i=9 (is_last). The
    // final aggregated state must remain consistent across all three.
    EXPECT_GE(flush_count, 3) << "expected at least 3 mid/last flushes";
    EXPECT_EQ(reader._stored_row_ref.size(), 0);
    EXPECT_EQ(reader._last_agg_data_counter, 0);

    // Mimic `_agg_key_next_block` end-of-group close.
    reader._agg_data_counters.push_back(reader._last_agg_data_counter);
    reader._last_agg_data_counter = 0;
    reader._update_agg_data(target_columns);

    ASSERT_EQ(target_columns[1]->size(), 1);
    int64_t expected = 0;
    for (int64_t v = 1; v <= static_cast<int64_t>(kRows); ++v) {
        expected += v;
    }
    EXPECT_EQ(read_int64(*target_columns[1], 0), expected); // 55
}

// Stress: a single group long enough to trigger many full periodic flushes,
// followed by a group end. Catches off-by-one bugs in chunked aggregation.
TEST_F(BlockReaderAggFlushTest, PeriodicFlushManyWindowsSingleGroup) {
    constexpr size_t kBatchMaxRows = 4;
    constexpr size_t kRows = 100; // 25 full windows

    BlockReader reader;
    auto src_block = make_source_block(kRows, /*key_value=*/3);
    configure_reader_for_int64_sum(reader, *src_block, kBatchMaxRows);

    auto target_columns = make_target_columns();
    for (size_t i = 0; i < kRows; ++i) {
        reader._next_row.block = std::shared_ptr<Block>(src_block.get(), [](Block*) {});
        reader._next_row.row_pos = static_cast<int>(i);
        reader._next_row.is_same = (i > 0);
        reader._append_agg_data(target_columns);
    }
    reader._agg_data_counters.push_back(reader._last_agg_data_counter);
    reader._last_agg_data_counter = 0;
    reader._update_agg_data(target_columns);

    ASSERT_EQ(target_columns[1]->size(), 1);
    int64_t expected = static_cast<int64_t>(kRows) * (kRows + 1) / 2; // 5050
    EXPECT_EQ(read_int64(*target_columns[1], 0), expected);
}

} // namespace doris
