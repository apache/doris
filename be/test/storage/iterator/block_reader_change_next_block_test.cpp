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
#include "storage/iterator/block_reader.h"
#include "storage/iterator/vcollect_iterator.h"
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_dummy.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nothing.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "storage/binlog.h"
#include "storage/iterator/binlog_block_reader_utils.h"
#include "storage/schema.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"

namespace doris {

using namespace ErrorCode;

namespace {

// Merged binlog block schema used across the tests. Column layout mirrors what a
// row-binlog scan produces after merge:
//   0: key         (Int64, the primary key used to group same-key rows)
//   1: val         (Int64, the "after" value of a data column)
//   2: __DORIS_BEFORE__val__ (Int64, the "before" value mirror of `val`)
//   3: __DORIS_BINLOG_TSO__ (Int64)
//   4: __DORIS_BINLOG_LSN__ (Int64)
//   5: __DORIS_BINLOG_OP__  (Int64, one of ROW_BINLOG_APPEND/UPDATE/DELETE)
constexpr int KEY_IDX = 0;
constexpr int VAL_IDX = 1;
constexpr int OP_IDX = 5;

struct Row {
    int64_t key;
    int64_t val;
    int64_t before_val;
    int64_t tso;
    int64_t lsn;
    int64_t op;
};

class ThrowOnCompareColumn final : public COWHelper<IColumnDummy, ThrowOnCompareColumn> {
private:
    friend class COWHelper<IColumnDummy, ThrowOnCompareColumn>;

    ThrowOnCompareColumn(size_t size, int error_code, std::shared_ptr<size_t> compare_calls)
            : _error_code(error_code), _compare_calls(std::move(compare_calls)) {
        s = size;
    }
    ThrowOnCompareColumn(const ThrowOnCompareColumn&) = default;

public:
    std::string get_name() const override { return "ThrowOnCompare"; }

    MutableColumnPtr clone_dummy(size_t size) const override {
        return ThrowOnCompareColumn::create(size, _error_code, _compare_calls);
    }

    bool structure_equals(const IColumn& rhs) const override {
        return typeid(rhs) == typeid(ThrowOnCompareColumn);
    }

    int compare_at(size_t, size_t, const IColumn&, int) const override {
        ++*_compare_calls;
        throw Exception(_error_code, "injected compare_at failure");
    }

private:
    int _error_code;
    std::shared_ptr<size_t> _compare_calls;
};

TabletColumn make_test_column(std::string name, FieldType type, bool is_key, bool is_nullable,
                              int32_t unique_id) {
    TabletColumn column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, type, is_nullable,
                        unique_id, sizeof(int64_t));
    column.set_name(std::move(name));
    column.set_is_key(is_key);
    column.set_index_length(sizeof(int64_t));
    return column;
}

TabletSchemaSPtr make_test_tablet_schema(
        const std::vector<std::pair<std::string, FieldType>>& value_columns = {
                {"val", FieldType::OLAP_FIELD_TYPE_BIGINT}}) {
    auto schema = std::make_shared<TabletSchema>();
    int32_t unique_id = 0;
    schema->append_column(
            make_test_column("key", FieldType::OLAP_FIELD_TYPE_BIGINT, true, false, unique_id++));
    for (const auto& [name, type] : value_columns) {
        schema->append_column(make_test_column(name, type, false, true, unique_id++));
    }
    for (const auto& [name, type] : value_columns) {
        schema->append_column(make_test_column(binlog::build_before_column_name(name), type, false,
                                               true, unique_id++));
    }
    schema->append_column(make_test_column(BINLOG_TSO_COL, FieldType::OLAP_FIELD_TYPE_BIGINT, false,
                                           true, unique_id++));
    schema->append_column(make_test_column(BINLOG_LSN_COL, FieldType::OLAP_FIELD_TYPE_BIGINT, false,
                                           false, unique_id++));
    schema->append_column(make_test_column(BINLOG_OP_COL, FieldType::OLAP_FIELD_TYPE_BIGINT, false,
                                           false, unique_id));
    return schema;
}

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

std::shared_ptr<Block> make_all_null_source_block() {
    auto block = std::make_shared<Block>();
    auto int_type = std::make_shared<DataTypeInt64>();
    auto nullable_int_type = make_nullable(int_type);
    auto key_col = ColumnInt64::create();
    auto val_col = nullable_int_type->create_column();
    auto before_col = nullable_int_type->create_column();
    auto tso_col = ColumnInt64::create();
    auto lsn_col = ColumnInt64::create();
    auto op_col = ColumnInt64::create();

    key_col->insert_many_vals(1, 2);
    val_col->insert_many_defaults(2);
    before_col->insert_many_defaults(2);
    tso_col->insert_value(1);
    tso_col->insert_value(2);
    lsn_col->insert_value(1);
    lsn_col->insert_value(2);
    op_col->insert_value(ROW_BINLOG_DELETE);
    op_col->insert_value(ROW_BINLOG_APPEND);

    block->insert({std::move(key_col), int_type, "key"});
    block->insert({std::move(val_col), nullable_int_type, "val"});
    block->insert(
            {std::move(before_col), nullable_int_type, binlog::build_before_column_name("val")});
    block->insert({std::move(tso_col), int_type, BINLOG_TSO_COL});
    block->insert({std::move(lsn_col), int_type, BINLOG_LSN_COL});
    block->insert({std::move(op_col), int_type, BINLOG_OP_COL});
    return block;
}

std::shared_ptr<Block> make_colliding_name_source_block(int64_t after_v, int64_t after_collision,
                                                        int64_t before_v,
                                                        int64_t before_collision) {
    auto block = std::make_shared<Block>();
    auto type = std::make_shared<DataTypeInt64>();
    const std::vector<std::pair<std::string, int64_t>> values = {
            {"key", 1},
            {"v", after_v},
            {"__DORIS_BEFORE__v__", after_collision},
            {binlog::build_before_column_name("v"), before_v},
            {binlog::build_before_column_name("__DORIS_BEFORE__v__"), before_collision},
            {BINLOG_TSO_COL, 1},
            {BINLOG_LSN_COL, 1},
            {BINLOG_OP_COL, ROW_BINLOG_UPDATE},
    };
    for (const auto& [name, value] : values) {
        auto column = ColumnInt64::create();
        column->insert_value(value);
        block->insert({std::move(column), type, name});
    }
    return block;
}

std::shared_ptr<Block> make_signed_zero_source_block() {
    auto block = std::make_shared<Block>();
    auto int_type = std::make_shared<DataTypeInt64>();
    auto float_type = std::make_shared<DataTypeFloat32>();
    auto key_col = ColumnInt64::create();
    auto val_col = ColumnFloat32::create();
    auto before_col = ColumnFloat32::create();
    auto tso_col = ColumnInt64::create();
    auto lsn_col = ColumnInt64::create();
    auto op_col = ColumnInt64::create();

    key_col->insert_value(1);
    val_col->insert_value(-0.0F);
    before_col->insert_value(+0.0F);
    tso_col->insert_value(1);
    lsn_col->insert_value(1);
    op_col->insert_value(ROW_BINLOG_UPDATE);

    block->insert({std::move(key_col), int_type, "key"});
    block->insert({std::move(val_col), float_type, "val"});
    block->insert({std::move(before_col), float_type, binlog::build_before_column_name("val")});
    block->insert({std::move(tso_col), int_type, BINLOG_TSO_COL});
    block->insert({std::move(lsn_col), int_type, BINLOG_LSN_COL});
    block->insert({std::move(op_col), int_type, BINLOG_OP_COL});
    return block;
}

std::shared_ptr<Block> make_unsupported_compare_source_block(
        const std::shared_ptr<size_t>& compare_calls) {
    auto block = std::make_shared<Block>();
    auto int_type = std::make_shared<DataTypeInt64>();
    auto nothing_type = std::make_shared<DataTypeNothing>();
    auto key_col = ColumnInt64::create();
    auto val_col = ThrowOnCompareColumn::create(0, ErrorCode::NOT_IMPLEMENTED_ERROR, compare_calls);
    auto before_col =
            ThrowOnCompareColumn::create(0, ErrorCode::NOT_IMPLEMENTED_ERROR, compare_calls);
    auto tso_col = ColumnInt64::create();
    auto lsn_col = ColumnInt64::create();
    auto op_col = ColumnInt64::create();

    for (int64_t key : {1, 2}) {
        key_col->insert_value(key);
        val_col->insert_default();
        before_col->insert_default();
        tso_col->insert_value(key);
        lsn_col->insert_value(key);
        op_col->insert_value(ROW_BINLOG_UPDATE);
    }

    block->insert({std::move(key_col), int_type, "key"});
    block->insert({std::move(val_col), nothing_type, "val"});
    block->insert({std::move(before_col), nothing_type, binlog::build_before_column_name("val")});
    block->insert({std::move(tso_col), int_type, BINLOG_TSO_COL});
    block->insert({std::move(lsn_col), int_type, BINLOG_LSN_COL});
    block->insert({std::move(op_col), int_type, BINLOG_OP_COL});
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

    RowLocation current_row_location() override { return {}; }
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

// Read schema mirroring the merged binlog block layout above.
ReadSchemaSPtr make_read_schema(const std::vector<std::string>& names,
                                ReadSchema::RowBinlogValueColumnPairs value_pairs,
                                int32_t tso_ordinal, int32_t lsn_ordinal, int32_t op_ordinal) {
    std::vector<TabletColumnPtr> cols;
    auto add_bigint = [&](const std::string& name) {
        auto col = std::make_shared<TabletColumn>();
        col->set_name(name);
        col->set_type(FieldType::OLAP_FIELD_TYPE_BIGINT);
        cols.push_back(std::move(col));
    };
    for (const auto& name : names) {
        add_bigint(name);
    }
    auto read_schema = std::make_shared<ReadSchema>(std::move(cols));
    DORIS_CHECK(read_schema
                        ->init_row_binlog_column_mappings(std::move(value_pairs), tso_ordinal,
                                                          lsn_ordinal, op_ordinal)
                        .ok());
    return read_schema;
}

// Wire a BlockReader as if init() had already completed for a row-binlog change
// scan over the fixed 6-column schema, then plug in the fake merge iterator.
void configure_reader(BlockReader& reader, std::shared_ptr<Block> source, size_t batch_size,
                      TabletSchemaSPtr schema = make_test_tablet_schema()) {
    config::enable_adaptive_batch_size = false;
    reader._reader_context.batch_size = batch_size;

    // The fake fixture layout is [key][current values][before values][TSO][LSN][OP]. Supply its
    // relationships explicitly, as OlapScanner does before constructing BlockReader.
    reader._tablet_schema = std::move(schema);
    ASSERT_EQ(reader._tablet_schema->num_columns(), source->columns());
    std::vector<DataTypePtr> read_types;
    read_types.reserve(source->columns());
    for (size_t ordinal = 0; ordinal < source->columns(); ++ordinal) {
        read_types.emplace_back(source->get_by_position(ordinal).type);
    }
    auto read_schema =
            std::make_shared<ReadSchema>(reader._tablet_schema->columns(), std::move(read_types));
    const ColumnId value_count = cast_set<ColumnId>((source->columns() - 4) / 2);
    ReadSchema::RowBinlogValueColumnPairs value_pairs;
    for (ColumnId i = 0; i < value_count; ++i) {
        value_pairs.emplace_back(1 + i, 1 + value_count + i);
    }
    DORIS_CHECK(read_schema
                        ->init_row_binlog_column_mappings(std::move(value_pairs),
                                                          1 + 2 * value_count, 2 + 2 * value_count,
                                                          3 + 2 * value_count)
                        .ok());
    reader._read_schema = std::move(read_schema);

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
std::vector<OutRow> drain(BlockReader& reader, Status (BlockReader::*fn)(Block*, bool*)) {
    std::vector<OutRow> result;
    bool eof = false;
    int guard = 0;
    while (!eof) {
        Block block = reader._read_schema->create_read_block();
        Status st = (reader.*fn)(&block, &eof);
        EXPECT_TRUE(st.ok()) << st;
        const int32_t op_ordinal = reader._read_schema->op_ordinal();
        if (op_ordinal < 0) {
            ADD_FAILURE() << "row-binlog read schema has no op column";
            return result;
        }
        for (size_t r = 0; r < block.rows(); ++r) {
            result.push_back({out_i64(block, KEY_IDX, r), out_i64(block, VAL_IDX, r),
                              out_i64(block, op_ordinal, r)});
        }
        if (++guard >= 1000) {
            ADD_FAILURE() << "drain did not terminate";
            break;
        }
    }
    return result;
}

void expect_out_rows(const std::vector<OutRow>& actual, const std::vector<OutRow>& expected) {
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        SCOPED_TRACE(i);
        EXPECT_EQ(actual[i].key, expected[i].key);
        EXPECT_EQ(actual[i].val, expected[i].val);
        EXPECT_EQ(actual[i].op, expected[i].op);
    }
}

} // namespace

class BlockReaderChangeNextBlockTest : public testing::Test {
protected:
    void SetUp() override { _saved_adaptive = config::enable_adaptive_batch_size; }
    void TearDown() override { config::enable_adaptive_batch_size = _saved_adaptive; }
    bool _saved_adaptive = false;
};

TEST_F(BlockReaderChangeNextBlockTest, BinlogSchemaWithoutBeforeUsesCurrentColumn) {
    auto read_schema =
            make_read_schema({"key", "val", BINLOG_TSO_COL, BINLOG_OP_COL}, {}, 2, -1, 3);

    EXPECT_EQ(VAL_IDX, read_schema->before_column_ordinal(VAL_IDX));
}

TEST_F(BlockReaderChangeNextBlockTest, BinlogSchemaDoesNotRequireLsn) {
    auto read_schema = make_read_schema(
            {"key", "val", binlog::build_before_column_name("val"), BINLOG_TSO_COL, BINLOG_OP_COL},
            {{1, 2}}, 3, -1, 4);

    EXPECT_EQ(-1, read_schema->lsn_ordinal());
    EXPECT_EQ(2, read_schema->before_column_ordinal(VAL_IDX));
}

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
    // delete uses the first op's before value (val's __DORIS_BEFORE__ mirror of row 0).
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

// A physical UPDATE whose complete BEFORE and AFTER row values are equal has no net delta.
TEST_F(BlockReaderChangeNextBlockTest, MinDeltaNoOpUpdateIsSkipped) {
    auto source = make_source_block({
            {1, 20, 20, 1, 1, ROW_BINLOG_UPDATE},
    });
    BlockReader reader;
    configure_reader(reader, source, 16);

    auto out = drain(reader, &BlockReader::_min_delta_next_block);
    EXPECT_TRUE(out.empty());
}

// An unavailable historical row is encoded as an all-NULL BEFORE image. It must not cancel an
// all-NULL row inserted later, because the net state changes from absent to present.
TEST_F(BlockReaderChangeNextBlockTest, MinDeltaAllNullBeforeImageIsRetained) {
    auto source = make_all_null_source_block();
    BlockReader reader;
    configure_reader(reader, source, 16);

    bool eof = false;
    Block output = source->clone_empty();
    ASSERT_TRUE(reader._min_delta_next_block(&output, &eof).ok());
    ASSERT_EQ(output.rows(), 2);
    EXPECT_EQ(out_i64(output, OP_IDX, 0), binlog::STREAM_CHANGE_UPDATE_BEFORE);
    EXPECT_EQ(out_i64(output, OP_IDX, 1), binlog::STREAM_CHANGE_UPDATE_AFTER);
    const auto& value_column =
            assert_cast<const ColumnNullable&>(*output.get_by_position(VAL_IDX).column);
    EXPECT_TRUE(value_column.is_null_at(0));
    EXPECT_TRUE(value_column.is_null_at(1));
    EXPECT_TRUE(eof);
}

// A user column may have the same name as another column's generated BEFORE mirror. Pairing by
// schema ordinals must still recognize an unchanged complete row and suppress the UPDATE.
TEST_F(BlockReaderChangeNextBlockTest, MinDeltaNoOpWithCollidingBeforeNameIsSkipped) {
    auto source = make_colliding_name_source_block(/*after_v=*/10, /*after_collision=*/20,
                                                   /*before_v=*/10, /*before_collision=*/20);
    auto schema =
            make_test_tablet_schema({{"v", FieldType::OLAP_FIELD_TYPE_BIGINT},
                                     {"__DORIS_BEFORE__v__", FieldType::OLAP_FIELD_TYPE_BIGINT}});
    BlockReader reader;
    configure_reader(reader, source, 16, std::move(schema));

    bool eof = false;
    Block output = source->clone_empty();
    ASSERT_TRUE(reader._min_delta_next_block(&output, &eof).ok());
    EXPECT_EQ(output.rows(), 0);
    EXPECT_TRUE(eof);
}

// compare_at considers +0 and -0 equal, so they represent no net MIN_DELTA change.
TEST_F(BlockReaderChangeNextBlockTest, MinDeltaSignedZeroUpdateIsSkipped) {
    auto source = make_signed_zero_source_block();
    BlockReader reader;
    configure_reader(reader, source, 16,
                     make_test_tablet_schema({{"val", FieldType::OLAP_FIELD_TYPE_FLOAT}}));

    bool eof = false;
    Block output = source->clone_empty();
    ASSERT_TRUE(reader._min_delta_next_block(&output, &eof).ok());
    EXPECT_EQ(output.rows(), 0);
    EXPECT_TRUE(eof);
}

TEST_F(BlockReaderChangeNextBlockTest, MinDeltaUnsupportedCompareIsRetainedAndCached) {
    auto compare_calls = std::make_shared<size_t>(0);
    auto source = make_unsupported_compare_source_block(compare_calls);
    BlockReader reader;
    configure_reader(reader, source, 16);

    bool eof = false;
    Block output = source->clone_empty();
    ASSERT_TRUE(reader._min_delta_next_block(&output, &eof).ok());

    // Both equal-value updates must be retained. The second key uses the cached unsupported
    // capability, so the exception-based capability probe runs exactly once per reader.
    ASSERT_EQ(output.rows(), 4);
    EXPECT_EQ(out_i64(output, OP_IDX, 0), binlog::STREAM_CHANGE_UPDATE_BEFORE);
    EXPECT_EQ(out_i64(output, OP_IDX, 1), binlog::STREAM_CHANGE_UPDATE_AFTER);
    EXPECT_EQ(out_i64(output, OP_IDX, 2), binlog::STREAM_CHANGE_UPDATE_BEFORE);
    EXPECT_EQ(out_i64(output, OP_IDX, 3), binlog::STREAM_CHANGE_UPDATE_AFTER);
    EXPECT_EQ(*compare_calls, 1);
    EXPECT_TRUE(reader._min_delta_value_compare_unsupported);
    EXPECT_TRUE(eof);
}

TEST_F(BlockReaderChangeNextBlockTest, MinDeltaUnexpectedCompareExceptionIsNotSuppressed) {
    auto source = make_source_block({{1, 20, 20, 1, 1, ROW_BINLOG_UPDATE}});
    auto throwing_type = std::make_shared<DataTypeNothing>();
    auto compare_calls = std::make_shared<size_t>(0);
    source->get_by_position(VAL_IDX).column =
            ThrowOnCompareColumn::create(1, ErrorCode::INTERNAL_ERROR, compare_calls);
    source->get_by_position(VAL_IDX).type = throwing_type;
    source->get_by_position(VAL_IDX + 1).column =
            ThrowOnCompareColumn::create(1, ErrorCode::INTERNAL_ERROR, compare_calls);
    source->get_by_position(VAL_IDX + 1).type = throwing_type;
    BlockReader reader;
    configure_reader(reader, source, 16);

    reader._stored_data_columns = source->clone_empty_columns();
    for (size_t i = 0; i < source->columns(); ++i) {
        reader._stored_data_columns[i]->insert_from(*source->get_by_position(i).column, 0);
    }

    try {
        static_cast<void>(reader._min_delta_values_equal(0));
        FAIL() << "expected a non-NOT_IMPLEMENTED comparison exception";
    } catch (const Exception& e) {
        EXPECT_EQ(e.code(), ErrorCode::INTERNAL_ERROR);
    }
    EXPECT_EQ(*compare_calls, 1);
    EXPECT_FALSE(reader._min_delta_value_compare_unsupported);
}

// The comparison is between the first BEFORE and last AFTER values, so A -> B -> A also has no
// net delta even though neither individual row-binlog UPDATE is a no-op.
TEST_F(BlockReaderChangeNextBlockTest, MinDeltaUpdatesReturningToOriginalAreSkipped) {
    auto source = make_source_block({
            {1, 20, 10, 1, 1, ROW_BINLOG_UPDATE},
            {1, 10, 20, 2, 2, ROW_BINLOG_UPDATE},
    });
    BlockReader reader;
    configure_reader(reader, source, 16);

    auto out = drain(reader, &BlockReader::_min_delta_next_block);
    EXPECT_TRUE(out.empty());
}

// Exercise long operation chains where intermediate rows repeatedly change existence and value.
// MIN_DELTA must preserve only the net state transition across the whole key window.
TEST_F(BlockReaderChangeNextBlockTest, MinDeltaComplexOperationChains) {
    struct TestCase {
        std::string_view name;
        std::vector<Row> rows;
        std::vector<OutRow> expected;
        size_t batch_size;
    };
    const std::vector<TestCase> test_cases = {
            {
                    "insert_delete_reinsert_update_back_delete",
                    {
                            {1, 1, 0, 1, 1, ROW_BINLOG_APPEND},
                            {1, 1, 1, 2, 2, ROW_BINLOG_DELETE},
                            {1, 1, 0, 3, 3, ROW_BINLOG_APPEND},
                            {1, 2, 1, 4, 4, ROW_BINLOG_UPDATE},
                            {1, 3, 2, 5, 5, ROW_BINLOG_UPDATE},
                            {1, 1, 3, 6, 6, ROW_BINLOG_UPDATE},
                            {1, 1, 1, 7, 7, ROW_BINLOG_DELETE},
                    },
                    {},
                    1,
            },
            {
                    "existing_row_delete_reinsert_and_return_to_original",
                    {
                            {1, 2, 1, 1, 1, ROW_BINLOG_UPDATE},
                            {1, 2, 2, 2, 2, ROW_BINLOG_DELETE},
                            {1, 2, 0, 3, 3, ROW_BINLOG_APPEND},
                            {1, 3, 2, 4, 4, ROW_BINLOG_UPDATE},
                            {1, 1, 3, 5, 5, ROW_BINLOG_UPDATE},
                    },
                    {},
                    2,
            },
            {
                    "existing_row_delete_reinsert_and_finish_changed",
                    {
                            {1, 2, 1, 1, 1, ROW_BINLOG_UPDATE},
                            {1, 2, 2, 2, 2, ROW_BINLOG_DELETE},
                            {1, 2, 0, 3, 3, ROW_BINLOG_APPEND},
                            {1, 3, 2, 4, 4, ROW_BINLOG_UPDATE},
                            {1, 4, 3, 5, 5, ROW_BINLOG_UPDATE},
                    },
                    {
                            {1, 1, binlog::STREAM_CHANGE_UPDATE_BEFORE},
                            {1, 4, binlog::STREAM_CHANGE_UPDATE_AFTER},
                    },
                    1,
            },
            {
                    "new_row_temporarily_deleted_but_finishes_present",
                    {
                            {1, 1, 0, 1, 1, ROW_BINLOG_APPEND},
                            {1, 2, 1, 2, 2, ROW_BINLOG_UPDATE},
                            {1, 2, 2, 3, 3, ROW_BINLOG_DELETE},
                            {1, 5, 0, 4, 4, ROW_BINLOG_APPEND},
                            {1, 6, 5, 5, 5, ROW_BINLOG_UPDATE},
                    },
                    {
                            {1, 6, binlog::STREAM_CHANGE_INSERT},
                    },
                    1,
            },
            {
                    "existing_row_temporarily_reinserted_but_finishes_deleted",
                    {
                            {1, 2, 1, 1, 1, ROW_BINLOG_UPDATE},
                            {1, 2, 2, 2, 2, ROW_BINLOG_DELETE},
                            {1, 3, 0, 3, 3, ROW_BINLOG_APPEND},
                            {1, 4, 3, 4, 4, ROW_BINLOG_UPDATE},
                            {1, 4, 4, 5, 5, ROW_BINLOG_DELETE},
                    },
                    {
                            {1, 1, binlog::STREAM_CHANGE_DELETE},
                    },
                    1,
            },
            {
                    "delete_reinsert_update_and_return_to_original",
                    {
                            {1, 1, 1, 1, 1, ROW_BINLOG_DELETE},
                            {1, 1, 0, 2, 2, ROW_BINLOG_APPEND},
                            {1, 2, 1, 3, 3, ROW_BINLOG_UPDATE},
                            {1, 1, 2, 4, 4, ROW_BINLOG_UPDATE},
                    },
                    {},
                    1,
            },
    };

    for (const auto& test_case : test_cases) {
        SCOPED_TRACE(test_case.name);
        auto source = make_source_block(test_case.rows);
        BlockReader reader;
        configure_reader(reader, source, test_case.batch_size);

        auto out = drain(reader, &BlockReader::_min_delta_next_block);
        expect_out_rows(out, test_case.expected);
    }
}

// Build a source block with a second value column that is present in the physical MIN_DELTA
// projection but absent from the SQL output projection.
struct TwoValueRow {
    int64_t key;
    int64_t val1;
    int64_t val2;
    int64_t before_val1;
    int64_t before_val2;
    int64_t tso;
    int64_t lsn;
    int64_t op;
};

std::shared_ptr<Block> make_two_value_source_block(const std::vector<TwoValueRow>& rows) {
    auto block = std::make_shared<Block>();
    auto type = std::make_shared<DataTypeInt64>();
    auto key_col = ColumnInt64::create();
    auto val1_col = ColumnInt64::create();
    auto val2_col = ColumnInt64::create();
    auto before_val1_col = ColumnInt64::create();
    auto before_val2_col = ColumnInt64::create();
    auto tso_col = ColumnInt64::create();
    auto lsn_col = ColumnInt64::create();
    auto op_col = ColumnInt64::create();
    for (const auto& row : rows) {
        key_col->insert_value(row.key);
        val1_col->insert_value(row.val1);
        val2_col->insert_value(row.val2);
        before_val1_col->insert_value(row.before_val1);
        before_val2_col->insert_value(row.before_val2);
        tso_col->insert_value(row.tso);
        lsn_col->insert_value(row.lsn);
        op_col->insert_value(row.op);
    }
    block->insert({std::move(key_col), type, "key"});
    block->insert({std::move(val1_col), type, "val"});
    block->insert({std::move(val2_col), type, "val2"});
    block->insert({std::move(before_val1_col), type, binlog::build_before_column_name("val")});
    block->insert({std::move(before_val2_col), type, binlog::build_before_column_name("val2")});
    block->insert({std::move(tso_col), type, BINLOG_TSO_COL});
    block->insert({std::move(lsn_col), type, BINLOG_LSN_COL});
    block->insert({std::move(op_col), type, BINLOG_OP_COL});
    return block;
}

void configure_two_value_reader(BlockReader& reader, std::shared_ptr<Block> source,
                                size_t batch_size = 16) {
    configure_reader(reader, source, batch_size,
                     make_test_tablet_schema({{"val", FieldType::OLAP_FIELD_TYPE_BIGINT},
                                              {"val2", FieldType::OLAP_FIELD_TYPE_BIGINT}}));
}

TEST_F(BlockReaderChangeNextBlockTest, MinDeltaNoOpUpdateComparesAllValueColumns) {
    auto source = make_two_value_source_block({
            {/*key=*/1, /*val1=*/20, /*val2=*/30, /*before_val1=*/20, /*before_val2=*/30,
             /*tso=*/1, /*lsn=*/1, ROW_BINLOG_UPDATE},
    });
    BlockReader reader;
    configure_two_value_reader(reader, source);

    auto out = drain(reader, &BlockReader::_min_delta_next_block);
    EXPECT_TRUE(out.empty());
}

TEST_F(BlockReaderChangeNextBlockTest, MinDeltaRetainsChangeInUnprojectedValueColumn) {
    auto source = make_two_value_source_block({
            {/*key=*/1, /*val1=*/20, /*val2=*/31, /*before_val1=*/20, /*before_val2=*/30,
             /*tso=*/1, /*lsn=*/1, ROW_BINLOG_UPDATE},
    });
    BlockReader reader;
    configure_two_value_reader(reader, source);

    auto out = drain(reader, &BlockReader::_min_delta_next_block);
    ASSERT_EQ(out.size(), 2);
    EXPECT_EQ(out[0].op, binlog::STREAM_CHANGE_UPDATE_BEFORE);
    EXPECT_EQ(out[1].op, binlog::STREAM_CHANGE_UPDATE_AFTER);
    EXPECT_EQ(out[0].val, 20);
    EXPECT_EQ(out[1].val, 20);
}

// key 1 changes both columns several times and returns to its complete original row image, so it
// disappears. key 2 returns only the projected value column to its original value while the hidden
// value column remains changed, so its UPDATE pair must survive. batch_size=1 also forces the pair
// through the pending-row path after the skipped key.
TEST_F(BlockReaderChangeNextBlockTest, MinDeltaComplexMultiColumnChains) {
    auto source = make_two_value_source_block({
            {1, 11, 100, 10, 100, 1, 1, ROW_BINLOG_UPDATE},
            {1, 11, 101, 11, 100, 2, 2, ROW_BINLOG_UPDATE},
            {1, 10, 100, 11, 101, 3, 3, ROW_BINLOG_UPDATE},
            {2, 21, 200, 20, 200, 4, 4, ROW_BINLOG_UPDATE},
            {2, 20, 201, 21, 200, 5, 5, ROW_BINLOG_UPDATE},
    });
    BlockReader reader;
    configure_two_value_reader(reader, source, /*batch_size=*/1);

    auto out = drain(reader, &BlockReader::_min_delta_next_block);
    expect_out_rows(out, {
                                 {2, 20, binlog::STREAM_CHANGE_UPDATE_BEFORE},
                                 {2, 20, binlog::STREAM_CHANGE_UPDATE_AFTER},
                         });
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
    EXPECT_EQ(out[0].val, 99); // delete uses __DORIS_BEFORE__ mirror
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

TEST_F(BlockReaderChangeNextBlockTest, DetailUsesOrdinalBeforePairWhenNamesCollide) {
    auto source = make_colliding_name_source_block(/*after_v=*/11, /*after_collision=*/20,
                                                   /*before_v=*/10, /*before_collision=*/20);
    auto schema =
            make_test_tablet_schema({{"v", FieldType::OLAP_FIELD_TYPE_BIGINT},
                                     {"__DORIS_BEFORE__v__", FieldType::OLAP_FIELD_TYPE_BIGINT}});
    BlockReader reader;
    configure_reader(reader, source, 16, std::move(schema));

    bool eof = false;
    Block output = source->clone_empty();
    ASSERT_TRUE(reader._detail_change_next_block(&output, &eof).ok());
    ASSERT_EQ(output.rows(), 2);
    EXPECT_EQ(out_i64(output, /*v=*/1, 0), 10);
    EXPECT_EQ(out_i64(output, /*v=*/1, 1), 11);
    EXPECT_EQ(out_i64(output, /*op=*/7, 0), binlog::STREAM_CHANGE_UPDATE_BEFORE);
    EXPECT_EQ(out_i64(output, /*op=*/7, 1), binlog::STREAM_CHANGE_UPDATE_AFTER);
    EXPECT_TRUE(eof);
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
