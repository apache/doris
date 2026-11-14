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

// The changelog folds of BlockReader over a real VHeapMergeIterator of TWO rowsets holding the
// SAME user key with DIFFERENT TSOs. Unlike block_reader_change_next_block_test.cpp (which
// injects fake, already merged rows), this test merges the rows the way BlockReader does for a
// changelog scan: decide_merge() of the read context makes the TSO the sequence column with
// the smaller value first, and the reader's cursor flags the rows of one key.
//
// The read schema is a real changelog schema: DUP_KEYS, which is what the FE actually
// generates for the hidden changelog table. With DUP_KEYS the merge drops no older version,
// so it keeps EVERY change event instead of deduplicating by
// user key. The test verifies that the two rowsets are merged into one globally
// (key, TSO)-ordered stream and that BlockReader folds it correctly:
//   * MIN_DELTA collapses each key's consecutive same-key changes into its net change,
//   * DETAIL emits every change event verbatim in ascending-TSO order,
// covering same-key/different-TSO events spread across two rowsets without dropping any.

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#include "storage/iterator/block_reader.h"
#include "storage/iterator/heap_merge_iterator.h"
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
#include "storage/changelog.h"
#include "storage/iterator/changelog_block_reader_utils.h"
#include "storage/schema.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"

namespace doris {

using namespace ErrorCode;

namespace {

// Merged changelog block layout. The leading column is the primary key used both for the
// merge-heap key comparison and for MIN_DELTA group boundaries:
//   0: key                  (Int64, the only key column)
//   1: val                  (Int64, "after" value)
//   2: __DORIS_CHANGELOG_BEFORE_val__ (Int64, "before" value mirror)
//   3: __DORIS_CHANGELOG_TSO__  (Int64, the merge sequence / order column)
//   4: __DORIS_CHANGELOG_LSN__  (Int64)
//   5: __DORIS_CHANGELOG_OP__   (Int64, ChangelogOp::APPEND/UPDATE/DELETE)
constexpr int KEY_IDX = 0;
constexpr int VAL_IDX = 1;
constexpr int BEFORE_VAL_IDX = 2;
constexpr int TSO_IDX = 3;
constexpr int LSN_IDX = 4;
constexpr int OP_IDX = 5;
constexpr int NUM_COLS = 6;

struct Row {
    int64_t key;
    int64_t val;
    int64_t before_val;
    int64_t tso;
    int64_t lsn;
    int64_t op;
};

// Changelog read schema: single leading key column, a value column plus its BEFORE
// mirror, and the three changelog meta columns. Marking CHANGELOG_TSO_COL by name makes
// TabletSchema::changelog_tso_col_idx() return its position, which decide_merge()
// uses to pick the TSO as the merge sequence column.
std::shared_ptr<TabletSchema> make_changelog_schema() {
    auto schema = std::make_shared<TabletSchema>();
    const std::string col_names[] = {"key",
                                     "val",
                                     changelog::build_before_column_name("val"),
                                     CHANGELOG_TSO_COL,
                                     CHANGELOG_LSN_COL,
                                     CHANGELOG_OP_COL};
    for (int i = 0; i < NUM_COLS; ++i) {
        TabletColumn col;
        col.set_name(col_names[i]);
        col.set_type(FieldType::OLAP_FIELD_TYPE_BIGINT);
        col.set_unique_id(i);
        col.set_is_key(i == KEY_IDX); // only the leading key column is a key
        schema->append_column(std::move(col));
    }
    // DUP_KEYS matches the schema the FE generates for the hidden changelog table; the merge
    // drops no older version of it, so every change event stays.
    schema->_keys_type = KeysType::DUP_KEYS;
    return schema;
}

// The rows of one rowset: next_batch(Block*) fills the block, built from the read schema, with
// the preset rows on the first call, then reports END_OF_FILE.
class FakeRowsIterator : public RowwiseIterator {
public:
    FakeRowsIterator(std::vector<Row> rows, ReadSchemaSPtr read_schema)
            : _rows(std::move(rows)), _read_schema(std::move(read_schema)) {}

    Status next_batch(Block* block) override {
        if (_emitted) {
            return Status::Error<END_OF_FILE>("");
        }
        _emitted = true;
        // The block is created from the read schema (all BIGINT, non-nullable) and
        // cleared before each refresh, so append straight into its Int64 columns. The
        // scoped guard writes the filled columns back into `block` on destruction.
        auto columns_guard = block->mutate_columns_scoped();
        auto& columns = columns_guard.mutable_columns();
        auto col_of = [&](int idx) -> ColumnInt64& {
            return assert_cast<ColumnInt64&>(*columns[idx]);
        };
        for (const auto& r : _rows) {
            col_of(KEY_IDX).insert_value(r.key);
            col_of(VAL_IDX).insert_value(r.val);
            col_of(BEFORE_VAL_IDX).insert_value(r.before_val);
            col_of(TSO_IDX).insert_value(r.tso);
            col_of(LSN_IDX).insert_value(r.lsn);
            col_of(OP_IDX).insert_value(r.op);
        }
        return Status::OK();
    }

    const ReadSchema& schema() const override { return *_read_schema; }

private:
    std::vector<Row> _rows;
    ReadSchemaSPtr _read_schema;
    bool _emitted = false;
};

// Wire a BlockReader as if init() had completed for a changelog MIN_DELTA/DETAIL scan whose two
// rowsets overlap, with the rows of the read merged the way BlockReader::_init_rows_iter merges
// them: one child per rowset in version order, ordered by decide_merge() of the read context.
// Nothing here hardcodes the order or the dedup; decide_merge() works them out from the
// (DUP_KEYS) changelog read schema, which keeps every event.
void configure_two_rowset_merge(BlockReader& reader, std::vector<Row> rowset0_rows,
                                std::vector<Row> rowset1_rows, size_t batch_size) {
    config::enable_adaptive_batch_size = false;
    reader._reader_context.batch_size = batch_size;
    // TabletReader reads the changes of a key in insertion order when they tie.
    reader._reader_context.read_changelog = true;
    reader._reader_context.use_insert_order_when_same = true;
    reader._reader_context.read_orderby_key_columns = nullptr;
    reader._reader_type = ReaderType::READER_BASE_COMPACTION;

    reader._tablet_schema = make_changelog_schema();
    // Identity read schema over the full changelog layout. ReadSchema derives num_key_columns()
    // from is_key() and tso_ordinal() from the CHANGELOG_TSO_COL name, which decide_merge()
    // uses to pick the TSO as the merge sequence column.
    reader._read_schema = std::make_shared<ReadSchema>(reader._tablet_schema->columns());
    reader._reader_context.read_schema = reader._read_schema;
    ASSERT_EQ(reader._read_schema->tso_ordinal(), TSO_IDX);

    std::vector<RowwiseIteratorUPtr> children;
    children.push_back(
            std::make_unique<FakeRowsIterator>(std::move(rowset0_rows), reader._read_schema));
    children.push_back(
            std::make_unique<FakeRowsIterator>(std::move(rowset1_rows), reader._read_schema));
    reader._merge = true;
    reader._rows_iter = new_heap_merge_iterator(std::move(children), reader._read_schema,
                                                decide_merge(reader._reader_context));
    StorageReadOptions opts(reader._stats);
    opts.block_row_max = static_cast<uint32_t>(batch_size);
    ASSERT_TRUE(reader._rows_iter->init(opts).ok());
    ASSERT_TRUE(reader._init_next_row().ok());
}

Block make_output_block() {
    Block block;
    auto type = std::make_shared<DataTypeInt64>();
    block.insert({ColumnInt64::create(), type, "key"});
    block.insert({ColumnInt64::create(), type, "val"});
    block.insert({ColumnInt64::create(), type, changelog::build_before_column_name("val")});
    block.insert({ColumnInt64::create(), type, CHANGELOG_TSO_COL});
    block.insert({ColumnInt64::create(), type, CHANGELOG_LSN_COL});
    block.insert({ColumnInt64::create(), type, CHANGELOG_OP_COL});
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

std::vector<OutRow> drain(BlockReader& reader, Status (BlockReader::*fn)(Block*, bool*)) {
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

class BlockReaderChangelogMergeTest : public testing::Test {
protected:
    void SetUp() override { _saved_adaptive = config::enable_adaptive_batch_size; }
    void TearDown() override { config::enable_adaptive_batch_size = _saved_adaptive; }
    bool _saved_adaptive = false;
};

// Two rowsets, same key, different TSO. rowset0 holds the APPEND (tso=1), rowset1 holds
// a later UPDATE (tso=2). With the DUP_KEYS changelog schema, the merge keeps every
// same-key row: the merge orders them by ascending TSO into one key group, and MIN_DELTA
// folds APPEND+UPDATE into a single INSERT carrying the most recent value.
TEST_F(BlockReaderChangelogMergeTest, MinDeltaSameKeyAcrossRowsetsFoldsToInsert) {
    BlockReader reader;
    configure_two_rowset_merge(reader,
                               {{/*key=*/1, /*val=*/10, /*before=*/0, /*tso=*/1, /*lsn=*/1,
                                 static_cast<int64_t>(ChangelogOp::APPEND)}},
                               {{/*key=*/1, /*val=*/20, /*before=*/10, /*tso=*/2, /*lsn=*/2,
                                 static_cast<int64_t>(ChangelogOp::UPDATE)}},
                               /*batch_size=*/16);

    auto out = drain(reader, &BlockReader::_min_delta_next_block);
    ASSERT_EQ(out.size(), 1);
    EXPECT_EQ(out[0].op, static_cast<int64_t>(changelog::StreamChangeOp::INSERT));
    EXPECT_EQ(out[0].key, 1);
    EXPECT_EQ(out[0].val, 20); // most recent value, i.e. rowset1's later TSO
}

// Same key across two rowsets where the later rowset deletes it: APPEND (tso=1) then
// DELETE (tso=3) folds to SKIP, so the merged group produces no output. This only holds
// if both rows land in one key group; if the merge dropped one, the surviving lone event
// would fold to a stray INSERT or DELETE instead of cancelling out.
TEST_F(BlockReaderChangelogMergeTest, MinDeltaSameKeyAcrossRowsetsFoldsToSkip) {
    BlockReader reader;
    configure_two_rowset_merge(
            reader, {{5, 100, 0, 1, 1, static_cast<int64_t>(ChangelogOp::APPEND)}},
            {{5, 100, 100, 3, 3, static_cast<int64_t>(ChangelogOp::DELETE)}}, 16);

    auto out = drain(reader, &BlockReader::_min_delta_next_block);
    EXPECT_TRUE(out.empty());
}

// Interleaved keys across two rowsets. Each rowset is independently key-ordered, and each
// key's change chain is spread over both rowsets under different TSOs:
//   key 1: APPEND(tso=1, rowset0) + UPDATE(tso=3, rowset1) -> INSERT(val=15)
//   key 2: UPDATE(tso=2, rowset0) + DELETE(tso=4, rowset1) -> DELETE(before=18)
// The merge must produce the globally (key, TSO)-ordered stream and keep every event so
// MIN_DELTA groups each key across the rowset boundary.
TEST_F(BlockReaderChangelogMergeTest, MinDeltaInterleavedKeysAcrossRowsets) {
    BlockReader reader;
    configure_two_rowset_merge(reader,
                               {{1, 10, 0, 1, 1, static_cast<int64_t>(ChangelogOp::APPEND)},
                                {2, 20, 18, 2, 2, static_cast<int64_t>(ChangelogOp::UPDATE)}},
                               {{1, 15, 10, 3, 3, static_cast<int64_t>(ChangelogOp::UPDATE)},
                                {2, 25, 20, 4, 4, static_cast<int64_t>(ChangelogOp::DELETE)}},
                               16);

    auto out = drain(reader, &BlockReader::_min_delta_next_block);
    ASSERT_EQ(out.size(), 2);
    EXPECT_EQ(out[0].key, 1);
    EXPECT_EQ(out[0].op, static_cast<int64_t>(changelog::StreamChangeOp::INSERT));
    EXPECT_EQ(out[0].val, 15); // most recent value for key 1
    EXPECT_EQ(out[1].key, 2);
    EXPECT_EQ(out[1].op, static_cast<int64_t>(changelog::StreamChangeOp::DELETE));
    EXPECT_EQ(out[1].val, 18); // delete uses the first op's __BEFORE__ value
}

// DETAIL scan over the same two-rowset / same-key / different-TSO input emits every event
// verbatim in ascending-TSO order: the rowset0 APPEND becomes an INSERT and the rowset1
// UPDATE becomes a BEFORE + AFTER pair. A dedup in the merge would drop the APPEND (or
// the UPDATE), leaving history incomplete.
TEST_F(BlockReaderChangelogMergeTest, DetailSameKeyAcrossRowsetsOrderedByTso) {
    BlockReader reader;
    configure_two_rowset_merge(reader,
                               {{1, 10, 0, 1, 1, static_cast<int64_t>(ChangelogOp::APPEND)}},
                               {{1, 20, 10, 2, 2, static_cast<int64_t>(ChangelogOp::UPDATE)}}, 16);

    auto out = drain(reader, &BlockReader::_detail_change_next_block);
    ASSERT_EQ(out.size(), 3);
    EXPECT_EQ(out[0].op, static_cast<int64_t>(changelog::StreamChangeOp::INSERT));
    EXPECT_EQ(out[0].val, 10); // rowset0 APPEND (tso=1) first
    EXPECT_EQ(out[1].op, static_cast<int64_t>(changelog::StreamChangeOp::UPDATE_BEFORE));
    EXPECT_EQ(out[1].val, 10); // rowset1 UPDATE (tso=2) before value
    EXPECT_EQ(out[2].op, static_cast<int64_t>(changelog::StreamChangeOp::UPDATE_AFTER));
    EXPECT_EQ(out[2].val, 20); // rowset1 UPDATE after value
}

} // namespace doris
