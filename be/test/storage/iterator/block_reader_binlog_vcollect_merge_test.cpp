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

// End-to-end coverage of the full BlockReader -> VCollectIterator path for a
// row-binlog scan that reads TWO rowsets holding the SAME user key with DIFFERENT
// TSOs. Unlike block_reader_change_next_block_test.cpp (which injects a single
// LevelIterator) and unlike a hand-built Level1Iterator, this test drives the REAL
// VCollectIterator::add_child + build_heap path:
//   * add_child() wraps each mock rowset reader in a real Level0Iterator, and
//   * build_heap() assembles the merge Level1Iterator that orders same-key rows by
//     ascending TSO (binlog_tso_col_idx -> sequence column, small_seq_first = true).
//
// The read schema is a real row-binlog schema: DUP_KEYS, which is what the FE actually
// generates for the hidden row-binlog table. With DUP_KEYS, build_heap computes
// _skip_same = false, so the merge keeps EVERY change event instead of deduplicating by
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
#include "storage/iterator/vcollect_iterator.h"
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
#include "io/fs/local_file_system.h"
#include "storage/binlog.h"
#include "storage/iterator/binlog_block_reader_utils.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/schema.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"

namespace doris {

using namespace ErrorCode;

namespace {

// Merged binlog block layout. The leading column is the primary key used both for the
// merge-heap key comparison and for MIN_DELTA group boundaries:
//   0: key                  (Int64, the only key column)
//   1: val                  (Int64, "after" value)
//   2: __DORIS_BEFORE__val__ (Int64, "before" value mirror)
//   3: __DORIS_BINLOG_TSO__  (Int64, the merge sequence / order column)
//   4: __DORIS_BINLOG_LSN__  (Int64)
//   5: __DORIS_BINLOG_OP__   (Int64, ROW_BINLOG_APPEND/UPDATE/DELETE)
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

// Row-binlog read schema: single leading key column, a value column plus its __DORIS_BEFORE__
// mirror, and the three binlog meta columns. Marking BINLOG_TSO_COL by name makes
// TabletSchema::binlog_tso_col_idx() return its position, which Level1Iterator::init()
// uses to pick the TSO as the merge sequence column.
std::shared_ptr<TabletSchema> make_binlog_schema() {
    auto schema = std::make_shared<TabletSchema>();
    const std::string col_names[] = {
            "key",          "val",          binlog::build_before_column_name("val"),
            BINLOG_TSO_COL, BINLOG_LSN_COL, BINLOG_OP_COL};
    for (int i = 0; i < NUM_COLS; ++i) {
        TabletColumn col;
        col.set_name(col_names[i]);
        col.set_type(FieldType::OLAP_FIELD_TYPE_BIGINT);
        col.set_unique_id(i);
        col.set_is_key(i == KEY_IDX); // only the leading key column is a key
        schema->append_column(std::move(col));
    }
    // DUP_KEYS matches the schema the FE generates for the hidden row-binlog table; it makes
    // build_heap compute _skip_same = false so the merge keeps every change event.
    schema->_keys_type = KeysType::DUP_KEYS;
    return schema;
}

// Minimal RowsetMeta carrying only num_rows (used by build_heap to pick the base rowset).
class FakeRowsetMeta : public RowsetMeta {
public:
    FakeRowsetMeta() : RowsetMeta() { _fs = io::global_local_filesystem(); }
    io::FileSystemSPtr fs() override { return _fs; }

private:
    io::FileSystemSPtr _fs;
};

// Minimal Rowset shell: no IO, just holds the meta so rowset()->rowset_meta()->num_rows()
// works and type()==BETA_ROWSET satisfies Level0Iterator's DCHECK.
class FakeRowset : public Rowset {
public:
    FakeRowset(TabletSchemaSPtr schema, RowsetMetaSharedPtr meta)
            : Rowset(schema, meta, "/fake/tablet/path") {}

    Status create_reader(std::shared_ptr<RowsetReader>* result) override {
        return Status::NotSupported("");
    }
    Status remove() override { return Status::OK(); }
    Status link_files_to(const std::string&, RowsetId, size_t, std::set<int64_t>*) override {
        return Status::OK();
    }
    Status copy_files_to(const std::string&, const RowsetId&) override { return Status::OK(); }
    Status remove_old_files(std::vector<std::string>*) override { return Status::OK(); }
    Status check_file_exist() override { return Status::OK(); }
    Status upload_to(const StorageResource&, const RowsetId&) override { return Status::OK(); }
    Status get_inverted_index_size(int64_t* index_size) override {
        *index_size = 0;
        return Status::OK();
    }
    void clear_inverted_index_cache() override {}
    Status init() override { return Status::OK(); }
    void do_close() override {}
    Status check_current_rowset_segment() override { return Status::OK(); }
    int64_t num_segments() const override { return 0; }
    Result<std::string> segment_path(int64_t) override {
        return ResultError(Status::InternalError(""));
    }
};

// Fake per-rowset reader. next_batch(Block*) fills the caller-provided block (built by
// Level0Iterator from the read schema) with the preset rows on the first call, then
// reports END_OF_FILE with an empty block. This is the single-rowset stream a real
// BetaRowsetReader would hand a Level0Iterator, so add_child/build_heap run for real.
class FakeRowsetReader : public RowsetReader {
public:
    FakeRowsetReader(std::vector<Row> rows, int64_t version)
            : _rows(std::move(rows)), _version(version) {
        auto meta = std::make_shared<FakeRowsetMeta>();
        meta->set_num_rows(static_cast<int64_t>(_rows.size()));
        meta->set_rowset_type(BETA_ROWSET);
        _rowset = std::make_shared<FakeRowset>(nullptr, meta);
        _read_schema = std::make_shared<ReadSchema>(make_binlog_schema()->columns());
        DORIS_CHECK(_read_schema
                            ->init_row_binlog_column_mappings({{VAL_IDX, BEFORE_VAL_IDX}}, TSO_IDX,
                                                              LSN_IDX, OP_IDX)
                            .ok());
    }

    Status init(RowsetReaderContext*, const RowSetSplits&) override { return Status::OK(); }
    Status get_segment_iterators(RowsetReaderContext*, std::vector<RowwiseIteratorUPtr>*,
                                 bool) override {
        return Status::OK();
    }
    void reset_read_options() override {}

    Status next_batch(Block* block) override {
        if (_emitted) {
            return Status::Error<END_OF_FILE>("");
        }
        _emitted = true;
        // The block is created from the read schema (all BIGINT, non-nullable) and
        // cleared before each refresh, so append straight into its Int64 columns. The
        // scoped guard writes the filled columns back into `block` on destruction, so
        // block->rows() reflects the appended rows (Level0Iterator relies on that).
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
    Status next_batch(BlockView*) override { return Status::NotSupported(""); }
    Status next_batch(BlockWithSameBit*) override { return Status::NotSupported(""); }

    bool delete_flag() override { return false; }
    Version version() override { return Version(_version, _version); }
    RowsetSharedPtr rowset() override { return _rowset; }
    const ReadSchema& read_schema() const override { return *_read_schema; }
    int64_t filtered_rows() override { return 0; }
    uint64_t merged_rows() override { return 0; }
    RowsetTypePB type() const override { return BETA_ROWSET; }
    int64_t newest_write_timestamp() override { return 0; }
    void update_profile(RuntimeProfile*) override {}
    RowsetReaderSharedPtr clone() override {
        return std::make_shared<FakeRowsetReader>(_rows, _version);
    }
    void set_topn_limit(size_t) override {}

private:
    std::vector<Row> _rows;
    int64_t _version;
    bool _emitted = false;
    RowsetSharedPtr _rowset;
    ReadSchemaSPtr _read_schema;
};

// Wire a BlockReader as if init() had completed for a row-binlog MIN_DELTA/DETAIL scan,
// then drive the REAL VCollectIterator init + add_child + build_heap path with one
// child per rowset. Nothing here hardcodes _skip_same; build_heap computes it from the
// (DUP_KEYS) row-binlog read schema, which yields _skip_same = false so every event is
// kept.
void configure_two_rowset_merge(BlockReader& reader, std::vector<Row> rowset0_rows,
                                std::vector<Row> rowset1_rows, size_t batch_size) {
    config::enable_adaptive_batch_size = false;
    reader._reader_context.batch_size = batch_size;
    reader._reader_context.read_row_binlog = true;
    reader._reader_context.read_orderby_key_columns = nullptr;
    // Non-QUERY reader type keeps VCollectIterator::init() from dereferencing the (unset)
    // _tablet while deciding _merge; the row-binlog branch then forces the merge anyway.
    reader._reader_type = ReaderType::READER_BASE_COMPACTION;

    reader._tablet_schema = make_binlog_schema();
    // Identity read schema over the full row-binlog layout. The explicit TSO ordinal is the merge
    // sequence column; _validate_merge_compare_contract() also checks the leading key prefix.
    reader._read_schema = std::make_shared<ReadSchema>(reader._tablet_schema->columns());
    DORIS_CHECK(reader._read_schema
                        ->init_row_binlog_column_mappings({{VAL_IDX, BEFORE_VAL_IDX}}, TSO_IDX,
                                                          LSN_IDX, OP_IDX)
                        .ok());

    // Mirror BlockReader::_init_collect_iter: init the collect iterator (force_merge=true,
    // as a MIN_DELTA stream does), add one child per rowset, then build the heap.
    reader._vcollect_iter.init(&reader, /*ori_data_overlapping=*/true, /*force_merge=*/true,
                               /*is_reverse=*/false);

    std::vector<RowsetReaderSharedPtr> rs_readers;
    // Give rowset0 more rows so build_heap picks a deterministic base rowset; behavior is
    // symmetric, this just fixes the base/cumulative split for the two-child path.
    auto reader0 = std::make_shared<FakeRowsetReader>(std::move(rowset0_rows), /*version=*/2);
    auto reader1 = std::make_shared<FakeRowsetReader>(std::move(rowset1_rows), /*version=*/3);
    for (auto& rs_reader : {reader0, reader1}) {
        RowSetSplits split(rs_reader);
        ASSERT_TRUE(reader._vcollect_iter.add_child(split).ok());
        rs_readers.push_back(rs_reader);
    }
    ASSERT_TRUE(reader._vcollect_iter.build_heap(rs_readers).ok());

    auto st = reader._vcollect_iter.current_row(&reader._next_row);
    reader._eof = st.is<END_OF_FILE>();
}

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

class BlockReaderBinlogVCollectMergeTest : public testing::Test {
protected:
    void SetUp() override { _saved_adaptive = config::enable_adaptive_batch_size; }
    void TearDown() override { config::enable_adaptive_batch_size = _saved_adaptive; }
    bool _saved_adaptive = false;
};

// Two rowsets, same key, different TSO. rowset0 holds the APPEND (tso=1), rowset1 holds
// a later UPDATE (tso=2). With the DUP_KEYS row-binlog schema, build_heap keeps every
// same-key row: the merge orders them by ascending TSO into one key group, and MIN_DELTA
// folds APPEND+UPDATE into a single INSERT carrying the most recent value.
TEST_F(BlockReaderBinlogVCollectMergeTest, MinDeltaSameKeyAcrossRowsetsFoldsToInsert) {
    BlockReader reader;
    configure_two_rowset_merge(
            reader,
            {{/*key=*/1, /*val=*/10, /*before=*/0, /*tso=*/1, /*lsn=*/1, ROW_BINLOG_APPEND}},
            {{/*key=*/1, /*val=*/20, /*before=*/10, /*tso=*/2, /*lsn=*/2, ROW_BINLOG_UPDATE}},
            /*batch_size=*/16);

    auto out = drain(reader, &BlockReader::_min_delta_next_block);
    ASSERT_EQ(out.size(), 1);
    EXPECT_EQ(out[0].op, binlog::STREAM_CHANGE_INSERT);
    EXPECT_EQ(out[0].key, 1);
    EXPECT_EQ(out[0].val, 20); // most recent value, i.e. rowset1's later TSO
}

// Same key across two rowsets where the later rowset deletes it: APPEND (tso=1) then
// DELETE (tso=3) folds to SKIP, so the merged group produces no output. This only holds
// if both rows land in one key group; if the merge dropped one, the surviving lone event
// would fold to a stray INSERT or DELETE instead of cancelling out.
TEST_F(BlockReaderBinlogVCollectMergeTest, MinDeltaSameKeyAcrossRowsetsFoldsToSkip) {
    BlockReader reader;
    configure_two_rowset_merge(reader, {{5, 100, 0, 1, 1, ROW_BINLOG_APPEND}},
                               {{5, 100, 100, 3, 3, ROW_BINLOG_DELETE}}, 16);

    auto out = drain(reader, &BlockReader::_min_delta_next_block);
    EXPECT_TRUE(out.empty());
}

// Interleaved keys across two rowsets. Each rowset is independently key-ordered, and each
// key's change chain is spread over both rowsets under different TSOs:
//   key 1: APPEND(tso=1, rowset0) + UPDATE(tso=3, rowset1) -> INSERT(val=15)
//   key 2: UPDATE(tso=2, rowset0) + DELETE(tso=4, rowset1) -> DELETE(before=18)
// The merge must produce the globally (key, TSO)-ordered stream and keep every event so
// MIN_DELTA groups each key across the rowset boundary.
TEST_F(BlockReaderBinlogVCollectMergeTest, MinDeltaInterleavedKeysAcrossRowsets) {
    BlockReader reader;
    configure_two_rowset_merge(
            reader, {{1, 10, 0, 1, 1, ROW_BINLOG_APPEND}, {2, 20, 18, 2, 2, ROW_BINLOG_UPDATE}},
            {{1, 15, 10, 3, 3, ROW_BINLOG_UPDATE}, {2, 25, 20, 4, 4, ROW_BINLOG_DELETE}}, 16);

    auto out = drain(reader, &BlockReader::_min_delta_next_block);
    ASSERT_EQ(out.size(), 2);
    EXPECT_EQ(out[0].key, 1);
    EXPECT_EQ(out[0].op, binlog::STREAM_CHANGE_INSERT);
    EXPECT_EQ(out[0].val, 15); // most recent value for key 1
    EXPECT_EQ(out[1].key, 2);
    EXPECT_EQ(out[1].op, binlog::STREAM_CHANGE_DELETE);
    EXPECT_EQ(out[1].val, 18); // delete uses the first op's __DORIS_BEFORE__ value
}

// DETAIL scan over the same two-rowset / same-key / different-TSO input emits every event
// verbatim in ascending-TSO order: the rowset0 APPEND becomes an INSERT and the rowset1
// UPDATE becomes a BEFORE + AFTER pair. A dedup in build_heap would drop the APPEND (or
// the UPDATE), leaving history incomplete.
TEST_F(BlockReaderBinlogVCollectMergeTest, DetailSameKeyAcrossRowsetsOrderedByTso) {
    BlockReader reader;
    configure_two_rowset_merge(reader, {{1, 10, 0, 1, 1, ROW_BINLOG_APPEND}},
                               {{1, 20, 10, 2, 2, ROW_BINLOG_UPDATE}}, 16);

    auto out = drain(reader, &BlockReader::_detail_change_next_block);
    ASSERT_EQ(out.size(), 3);
    EXPECT_EQ(out[0].op, binlog::STREAM_CHANGE_INSERT);
    EXPECT_EQ(out[0].val, 10); // rowset0 APPEND (tso=1) first
    EXPECT_EQ(out[1].op, binlog::STREAM_CHANGE_UPDATE_BEFORE);
    EXPECT_EQ(out[1].val, 10); // rowset1 UPDATE (tso=2) before value
    EXPECT_EQ(out[2].op, binlog::STREAM_CHANGE_UPDATE_AFTER);
    EXPECT_EQ(out[2].val, 20); // rowset1 UPDATE after value
}

} // namespace doris
