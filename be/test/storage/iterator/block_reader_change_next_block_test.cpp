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

// End-to-end branch coverage for BlockReader::_min_delta_next_block and
// BlockReader::_detail_change_next_block. Instead of standing up a real tablet /
// rowset stack, we inject a fake VCollectIterator::LevelIterator that walks a
// pre-built merged binlog block one row at a time, so we can exercise every
// min-delta fold result (SKIP / INSERT / DELETE / UPDATE_BEFORE_AFTER) and every
// detail op (INSERT / DELETE / UPDATE, including the pending-row batch split).

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define private public
#define protected public
#include "storage/iterator/block_reader.h"
#include "storage/iterator/vcollect_iterator.h"
#undef private
#undef protected
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "storage/binlog.h"
#include "storage/iterator/binlog_block_reader_utils.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"

namespace doris {

using namespace ErrorCode;

namespace {

// Merged binlog block schema used across the tests. Column layout mirrors what a
// row-binlog scan produces after merge:
//   0: key         (Int64, the primary key used to group same-key rows)
//   1: val         (Int64, the "after" value of a data column)
//   2: __BEFORE__val__ (Int64, the "before" value mirror of `val`)
//   3: __DORIS_BINLOG_TSO__ (Int64)
//   4: __DORIS_BINLOG_LSN__ (Int64)
//   5: __DORIS_BINLOG_OP__  (Int64, one of ROW_BINLOG_APPEND/UPDATE/DELETE)
constexpr int KEY_IDX = 0;
constexpr int VAL_IDX = 1;
constexpr int BEFORE_VAL_IDX = 2;
constexpr int TSO_IDX = 3;
constexpr int LSN_IDX = 4;
constexpr int OP_IDX = 5;

struct Row {
    int64_t key;
    int64_t val;
    int64_t before_val;
    int64_t tso;
    int64_t lsn;
    int64_t op;
};

std::shared_ptr<Block> make_source_block(const std::vector<Row>& rows) {
    auto block = std::make_shared<Block>();
    auto type = std::make_shared<DataTypeInt64>();

    auto key_col = ColumnInt64::create();
    auto val_col = ColumnInt64::create();
    auto before_col = ColumnInt64::create();
    auto tso_col = ColumnInt64::create();
    auto lsn_col = ColumnInt64::create();
    auto op_col = ColumnInt64::create();
    for (const auto& r : rows) {
        key_col->insert_value(r.key);
        val_col->insert_value(r.val);
        before_col->insert_value(r.before_val);
        tso_col->insert_value(r.tso);
        lsn_col->insert_value(r.lsn);
        op_col->insert_value(r.op);
    }
    block->insert({std::move(key_col), type, "key"});
    block->insert({std::move(val_col), type, "val"});
    block->insert({std::move(before_col), type, binlog::build_before_column_name("val")});
    block->insert({std::move(tso_col), type, BINLOG_TSO_COL});
    block->insert({std::move(lsn_col), type, BINLOG_LSN_COL});
    block->insert({std::move(op_col), type, BINLOG_OP_COL});
    return block;
}

// Fake merge iterator: hands out rows from `source` one at a time. `is_same` is
// derived from primary-key equality with the previous emitted row, matching the
// real merge iterator's contract that consecutive same-key rows are flagged.
class FakeLevelIterator : public VCollectIterator::LevelIterator {
public:
    FakeLevelIterator(TabletReader* reader, std::shared_ptr<Block> source)
            : LevelIterator(reader), _source(std::move(source)) {
        _ref.block = _source;
        _ref.row_pos = 0;
        _ref.is_same = false;
    }

    Status init(bool /*get_data_by_ref*/) override { return Status::OK(); }
    int64_t version() const override { return 0; }

    Status next(IteratorRowRef* ref) override {
        int prev = _ref.row_pos;
        int next_pos = prev + 1;
        if (next_pos >= _source->rows()) {
            _ref.row_pos = -1;
            *ref = _ref;
            return Status::Error<END_OF_FILE>("");
        }
        int64_t prev_key = read_i64(KEY_IDX, prev);
        int64_t cur_key = read_i64(KEY_IDX, next_pos);
        _ref.row_pos = next_pos;
        _ref.is_same = (prev_key == cur_key);
        *ref = _ref;
        return Status::OK();
    }

    Status next(Block* /*block*/) override { return Status::Error<END_OF_FILE>(""); }

    RowLocation current_row_location() override { return RowLocation(); }
    Status current_block_row_locations(std::vector<RowLocation>* /*loc*/) override {
        return Status::OK();
    }
    Status ensure_first_row_ref() override { return Status::OK(); }
    void update_profile(RuntimeProfile* /*profile*/) override {}

private:
    int64_t read_i64(int col, int row) const {
        return assert_cast<const ColumnInt64&>(*_source->get_by_position(col).column)
                .get_element(row);
    }

    std::shared_ptr<Block> _source;
};

// Wire a BlockReader as if init() had already completed for a row-binlog change
// scan over the fixed 6-column schema, then plug in the fake merge iterator.
void configure_reader(BlockReader& reader, std::shared_ptr<Block> source, size_t batch_size) {
    config::enable_adaptive_batch_size = false;
    reader._reader_context.batch_size = batch_size;

    // The fake LevelIterator base ctor dereferences reader->tablet_schema(), so a
    // schema must exist even though its contents are unused by these code paths.
    reader._tablet_schema = std::make_shared<TabletSchema>();

    // All 6 columns are "normal" columns and are returned in-place.
    reader._normal_columns_idx = {KEY_IDX, VAL_IDX, BEFORE_VAL_IDX, TSO_IDX, LSN_IDX, OP_IDX};
    reader._return_columns_loc = {0, 1, 2, 3, 4, 5};

    reader._next_row.block = source;
    reader._next_row.row_pos = 0;
    reader._next_row.is_same = false;
    reader._eof = false;

    reader._vcollect_iter._inner_iter =
            std::make_unique<FakeLevelIterator>(&reader, std::move(source));
}

// Build the empty output block matching the source schema.
Block make_output_block() {
    Block block;
    auto type = std::make_shared<DataTypeInt64>();
    block.insert({ColumnInt64::create(), type, "key"});
    block.insert({ColumnInt64::create(), type, "val"});
    block.insert({ColumnInt64::create(), type, binlog::build_before_column_name("val")});
    block.insert({ColumnInt64::create(), type, BINLOG_TSO_COL});
    block.insert({ColumnInt64::create(), type, BINLOG_LSN_COL});
    block.insert({ColumnInt64::create(), type, BINLOG_OP_COL});
    return block;
}

int64_t out_i64(const Block& block, int col, int row) {
    return assert_cast<const ColumnInt64&>(*block.get_by_position(col).column).get_element(row);
}

struct OutRow {
    int64_t key;
    int64_t val;
    int64_t op;
};

// Drain the reader across as many next_block calls as needed and collect the
// (key, val, op) triples in output order.
std::vector<OutRow> drain(BlockReader& reader,
                          Status (BlockReader::*fn)(Block*, bool*)) {
    std::vector<OutRow> result;
    bool eof = false;
    int guard = 0;
    while (!eof) {
        Block block = make_output_block();
        Status st = (reader.*fn)(&block, &eof);
        EXPECT_TRUE(st.ok()) << st;
        for (size_t r = 0; r < block.rows(); ++r) {
            result.push_back({out_i64(block, KEY_IDX, r), out_i64(block, VAL_IDX, r),
                              out_i64(block, OP_IDX, r)});
        }
        if (++guard >= 1000) {
            ADD_FAILURE() << "drain did not terminate";
            break;
        }
    }
    return result;
}

} // namespace

class BlockReaderChangeNextBlockTest : public testing::Test {
protected:
    void SetUp() override { _saved_adaptive = config::enable_adaptive_batch_size; }
    void TearDown() override { config::enable_adaptive_batch_size = _saved_adaptive; }
    bool _saved_adaptive = false;
};

// ============================================================================
// _min_delta_next_block branch coverage
// ============================================================================

// APPEND then DELETE within the window folds to SKIP: nothing is emitted.
TEST_F(BlockReaderChangeNextBlockTest, MinDeltaSkip) {
    auto source = make_source_block({
            {/*key=*/1, /*val=*/10, /*before=*/0, /*tso=*/1, /*lsn=*/1, ROW_BINLOG_APPEND},
            {/*key=*/1, /*val=*/10, /*before=*/10, /*tso=*/2, /*lsn=*/2, ROW_BINLOG_DELETE},
    });
    BlockReader reader;
    configure_reader(reader, source, /*batch_size=*/16);

    auto out = drain(reader, &BlockReader::_min_delta_next_block);
    EXPECT_TRUE(out.empty());
}

// APPEND (+ later UPDATE) folds to a single INSERT carrying the most recent value.
TEST_F(BlockReaderChangeNextBlockTest, MinDeltaInsert) {
    auto source = make_source_block({
            {1, 10, 0, 1, 1, ROW_BINLOG_APPEND},
            {1, 20, 10, 2, 2, ROW_BINLOG_UPDATE},
    });
    BlockReader reader;
    configure_reader(reader, source, 16);

    auto out = drain(reader, &BlockReader::_min_delta_next_block);
    ASSERT_EQ(out.size(), 1);
    EXPECT_EQ(out[0].op, binlog::STREAM_CHANGE_INSERT);
    EXPECT_EQ(out[0].key, 1);
    EXPECT_EQ(out[0].val, 20); // most recent value
}

// UPDATE then DELETE folds to a single DELETE carrying the first op's before value.
TEST_F(BlockReaderChangeNextBlockTest, MinDeltaDelete) {
    auto source = make_source_block({
            {1, 20, 10, 1, 1, ROW_BINLOG_UPDATE},
            {1, 20, 20, 2, 2, ROW_BINLOG_DELETE},
    });
    BlockReader reader;
    configure_reader(reader, source, 16);

    auto out = drain(reader, &BlockReader::_min_delta_next_block);
    ASSERT_EQ(out.size(), 1);
    EXPECT_EQ(out[0].op, binlog::STREAM_CHANGE_DELETE);
    EXPECT_EQ(out[0].key, 1);
    // delete uses the first op's before value (val's __BEFORE__ mirror of row 0).
    EXPECT_EQ(out[0].val, 10);
}

// UPDATE then UPDATE folds to a BEFORE + AFTER pair.
TEST_F(BlockReaderChangeNextBlockTest, MinDeltaUpdateBeforeAfter) {
    auto source = make_source_block({
            {1, 20, 10, 1, 1, ROW_BINLOG_UPDATE},
            {1, 30, 20, 2, 2, ROW_BINLOG_UPDATE},
    });
    BlockReader reader;
    configure_reader(reader, source, 16);

    auto out = drain(reader, &BlockReader::_min_delta_next_block);
    ASSERT_EQ(out.size(), 2);
    EXPECT_EQ(out[0].op, binlog::STREAM_CHANGE_UPDATE_BEFORE);
    EXPECT_EQ(out[0].val, 10); // before value from the first op
    EXPECT_EQ(out[1].op, binlog::STREAM_CHANGE_UPDATE_AFTER);
    EXPECT_EQ(out[1].val, 30); // after value from the last op
}

// Multiple distinct keys, each in its own group, are folded independently.
TEST_F(BlockReaderChangeNextBlockTest, MinDeltaMultipleKeys) {
    auto source = make_source_block({
            // key 1: APPEND -> INSERT(val=10)
            {1, 10, 0, 1, 1, ROW_BINLOG_APPEND},
            // key 2: APPEND + DELETE -> SKIP
            {2, 20, 0, 2, 2, ROW_BINLOG_APPEND},
            {2, 20, 20, 3, 3, ROW_BINLOG_DELETE},
            // key 3: UPDATE + DELETE -> DELETE(before=30)
            {3, 40, 30, 4, 4, ROW_BINLOG_UPDATE},
            {3, 40, 40, 5, 5, ROW_BINLOG_DELETE},
    });
    BlockReader reader;
    configure_reader(reader, source, 16);

    auto out = drain(reader, &BlockReader::_min_delta_next_block);
    ASSERT_EQ(out.size(), 2);
    EXPECT_EQ(out[0].key, 1);
    EXPECT_EQ(out[0].op, binlog::STREAM_CHANGE_INSERT);
    EXPECT_EQ(out[0].val, 10);
    EXPECT_EQ(out[1].key, 3);
    EXPECT_EQ(out[1].op, binlog::STREAM_CHANGE_DELETE);
    EXPECT_EQ(out[1].val, 30);
}

// The UPDATE_BEFORE_AFTER split across a batch boundary: BEFORE emitted at the
// end of one block, AFTER carried over via _pending_row_columns to the next.
TEST_F(BlockReaderChangeNextBlockTest, MinDeltaUpdatePendingRowAcrossBatch) {
    auto source = make_source_block({
            // key 1: single INSERT fills row 0 of the first batch.
            {1, 10, 0, 1, 1, ROW_BINLOG_APPEND},
            // key 2: UPDATE+UPDATE -> BEFORE lands on row 1 (== batch_size), so
            // AFTER must be parked and flushed on the next call.
            {2, 20, 15, 2, 2, ROW_BINLOG_UPDATE},
            {2, 30, 20, 3, 3, ROW_BINLOG_UPDATE},
    });
    BlockReader reader;
    configure_reader(reader, source, /*batch_size=*/2);

    bool eof = false;
    Block block1 = make_output_block();
    ASSERT_TRUE(reader._min_delta_next_block(&block1, &eof).ok());
    ASSERT_EQ(block1.rows(), 2);
    EXPECT_EQ(out_i64(block1, OP_IDX, 0), binlog::STREAM_CHANGE_INSERT);
    EXPECT_EQ(out_i64(block1, OP_IDX, 1), binlog::STREAM_CHANGE_UPDATE_BEFORE);
    EXPECT_EQ(out_i64(block1, VAL_IDX, 1), 15); // before value
    EXPECT_FALSE(eof);
    EXPECT_TRUE(reader._has_pending_row);

    Block block2 = make_output_block();
    ASSERT_TRUE(reader._min_delta_next_block(&block2, &eof).ok());
    ASSERT_EQ(block2.rows(), 1);
    EXPECT_EQ(out_i64(block2, OP_IDX, 0), binlog::STREAM_CHANGE_UPDATE_AFTER);
    EXPECT_EQ(out_i64(block2, VAL_IDX, 0), 30); // after value
    EXPECT_TRUE(eof);
}

// ============================================================================
// _detail_change_next_block branch coverage
// ============================================================================

// APPEND -> a single INSERT row with the row's value.
TEST_F(BlockReaderChangeNextBlockTest, DetailInsert) {
    auto source = make_source_block({
            {1, 10, 0, 1, 1, ROW_BINLOG_APPEND},
    });
    BlockReader reader;
    configure_reader(reader, source, 16);

    auto out = drain(reader, &BlockReader::_detail_change_next_block);
    ASSERT_EQ(out.size(), 1);
    EXPECT_EQ(out[0].op, binlog::STREAM_CHANGE_INSERT);
    EXPECT_EQ(out[0].key, 1);
    EXPECT_EQ(out[0].val, 10);
}

// DELETE -> a single DELETE row that uses the before value.
TEST_F(BlockReaderChangeNextBlockTest, DetailDelete) {
    auto source = make_source_block({
            {1, 10, 99, 1, 1, ROW_BINLOG_DELETE},
    });
    BlockReader reader;
    configure_reader(reader, source, 16);

    auto out = drain(reader, &BlockReader::_detail_change_next_block);
    ASSERT_EQ(out.size(), 1);
    EXPECT_EQ(out[0].op, binlog::STREAM_CHANGE_DELETE);
    EXPECT_EQ(out[0].key, 1);
    EXPECT_EQ(out[0].val, 99); // delete uses __BEFORE__ mirror
}

// UPDATE -> a BEFORE (before value) + AFTER (after value) pair.
TEST_F(BlockReaderChangeNextBlockTest, DetailUpdatePair) {
    auto source = make_source_block({
            {1, 20, 10, 1, 1, ROW_BINLOG_UPDATE},
    });
    BlockReader reader;
    configure_reader(reader, source, 16);

    auto out = drain(reader, &BlockReader::_detail_change_next_block);
    ASSERT_EQ(out.size(), 2);
    EXPECT_EQ(out[0].op, binlog::STREAM_CHANGE_UPDATE_BEFORE);
    EXPECT_EQ(out[0].val, 10); // before
    EXPECT_EQ(out[1].op, binlog::STREAM_CHANGE_UPDATE_AFTER);
    EXPECT_EQ(out[1].val, 20); // after
}

// Mixed ops emitted verbatim in order.
TEST_F(BlockReaderChangeNextBlockTest, DetailMixedOps) {
    auto source = make_source_block({
            {1, 10, 0, 1, 1, ROW_BINLOG_APPEND},
            {2, 25, 20, 2, 2, ROW_BINLOG_UPDATE},
            {3, 30, 30, 3, 3, ROW_BINLOG_DELETE},
    });
    BlockReader reader;
    configure_reader(reader, source, 16);

    auto out = drain(reader, &BlockReader::_detail_change_next_block);
    ASSERT_EQ(out.size(), 4); // INSERT + (BEFORE,AFTER) + DELETE
    EXPECT_EQ(out[0].op, binlog::STREAM_CHANGE_INSERT);
    EXPECT_EQ(out[0].val, 10);
    EXPECT_EQ(out[1].op, binlog::STREAM_CHANGE_UPDATE_BEFORE);
    EXPECT_EQ(out[1].val, 20);
    EXPECT_EQ(out[2].op, binlog::STREAM_CHANGE_UPDATE_AFTER);
    EXPECT_EQ(out[2].val, 25);
    EXPECT_EQ(out[3].op, binlog::STREAM_CHANGE_DELETE);
    EXPECT_EQ(out[3].val, 30);
}

// UPDATE whose BEFORE row fills the batch: AFTER is parked in _pending_row_columns
// and flushed at the start of the next call.
TEST_F(BlockReaderChangeNextBlockTest, DetailUpdatePendingRowAcrossBatch) {
    auto source = make_source_block({
            {1, 10, 0, 1, 1, ROW_BINLOG_APPEND},
            {2, 25, 20, 2, 2, ROW_BINLOG_UPDATE},
    });
    BlockReader reader;
    configure_reader(reader, source, /*batch_size=*/2);

    bool eof = false;
    Block block1 = make_output_block();
    ASSERT_TRUE(reader._detail_change_next_block(&block1, &eof).ok());
    ASSERT_EQ(block1.rows(), 2);
    EXPECT_EQ(out_i64(block1, OP_IDX, 0), binlog::STREAM_CHANGE_INSERT);
    EXPECT_EQ(out_i64(block1, OP_IDX, 1), binlog::STREAM_CHANGE_UPDATE_BEFORE);
    EXPECT_EQ(out_i64(block1, VAL_IDX, 1), 20); // before
    EXPECT_FALSE(eof);
    EXPECT_TRUE(reader._has_pending_row);

    Block block2 = make_output_block();
    ASSERT_TRUE(reader._detail_change_next_block(&block2, &eof).ok());
    ASSERT_EQ(block2.rows(), 1);
    EXPECT_EQ(out_i64(block2, OP_IDX, 0), binlog::STREAM_CHANGE_UPDATE_AFTER);
    EXPECT_EQ(out_i64(block2, VAL_IDX, 0), 25); // after
    EXPECT_TRUE(eof);
}

// Already-at-EOF with no pending row returns eof immediately with no output.
TEST_F(BlockReaderChangeNextBlockTest, DetailEofImmediately) {
    auto source = make_source_block({
            {1, 10, 0, 1, 1, ROW_BINLOG_APPEND},
    });
    BlockReader reader;
    configure_reader(reader, source, 16);

    // First call drains the only row.
    auto out = drain(reader, &BlockReader::_detail_change_next_block);
    ASSERT_EQ(out.size(), 1);

    // A subsequent call should report eof with no rows.
    bool eof = false;
    Block block = make_output_block();
    ASSERT_TRUE(reader._detail_change_next_block(&block, &eof).ok());
    EXPECT_EQ(block.rows(), 0);
    EXPECT_TRUE(eof);
}

} // namespace doris
