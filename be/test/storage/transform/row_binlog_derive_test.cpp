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

// Branch-coverage tests for the row-binlog derive stages
// (PlainRowBinlogDeriveStage + MowRowBinlogDeriveStage).
//
// Blueprint: dev-docs/transform-chain-test-design.md §5 (Row Binlog derive).
// This file pins down the §5 coverage table B1-B22 + N1-N7:
//   - Plain op: APPEND / DELETE off the row's own delete sign (B6/B7/B8).
//   - Mow op: APPEND (new key) / UPDATE (existing key) / DELETE (delete sign),
//     plus the AFTER image and per-row LSN values (B12-B17, B22).
//   - BEFORE image (write_before=true): UPDATE/DELETE rows mirror history,
//     APPEND row is NULL, and the 0-value-column no-op (B19/B20/B21).
//   - Reject / negative paths: flexible-PU reject, fixed-PU narrow/wide reject,
//     missing source schema, segment_id<0 (B3/B4/B9/B10, N2-N6).
//
// Patterns (read_op helper, binlog write config, build_transform_chain + apply,
// reading the derived block) are ported from
// be/test/storage/transform/block_transform_test.cpp.

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "common/config.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "storage/binlog.h"
#include "storage/mow/mow_transform_test_base.h"
#include "storage/partial_update_info.h"
#include "storage/transform/block_transform.h"

namespace doris {

using segment_v2::build_transform_chain;
using segment_v2::TransformExecContext;

class RowBinlogDeriveTest : public MowTransformTestBase {
protected:
    // Reads a (nullable) BIGINT op cell from the binlog block.
    // (ported verbatim from block_transform_test.cpp)
    static int64_t read_op(const Block& block, int col_pos, size_t row) {
        const IColumn* col = block.get_by_position(col_pos).column.get();
        if (col->is_nullable()) {
            col = &assert_cast<const ColumnNullable&>(*col).get_nested_column();
        }
        return assert_cast<const ColumnInt64&>(*col).get_data()[row];
    }

    // Reads a (nullable) BIGINT LSN cell from the binlog block.
    static int64_t read_lsn(const Block& block, int col_pos, size_t row) {
        const IColumn* col = block.get_by_position(col_pos).column.get();
        if (col->is_nullable()) {
            col = &assert_cast<const ColumnNullable&>(*col).get_nested_column();
        }
        return assert_cast<const ColumnInt64&>(*col).get_data()[row];
    }

    // A row-binlog DUP schema WITH __BEFORE__* mirror columns for the binlog
    // source `create_binlog_pu_source_schema()` (k1 key, v1, v2, hidden
    // delete_sign). Layout matches resolve_binlog_context's before_col_start =
    // normal_col_start + normal_col_num: the visible source columns first, then
    // one BEFORE column per visible value column, then TSO/LSN/OP.
    //   [k1(0), v1(1), v2(2), __BEFORE__v1__(3), __BEFORE__v2__(4),
    //    __DORIS_BINLOG_TSO__(5), __DORIS_BINLOG_LSN__(6), __DORIS_BINLOG_OP__(7)]
    // The fixture's create_binlog_tablet builds a binlog schema with no BEFORE
    // columns, so this variant is defined locally for the write_before=true path.
    TabletSchemaSPtr create_binlog_before_schema() {
        TabletSchemaPB pb;
        pb.set_keys_type(DUP_KEYS); // row binlog is written as a DUP block
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_LZ4);
        pb.set_next_column_unique_id(20);

        auto add_col = [&](int uid, const std::string& name, const std::string& type, bool is_key,
                           bool nullable) {
            ColumnPB* c = pb.add_column();
            c->set_unique_id(uid);
            c->set_name(name);
            c->set_type(type);
            c->set_is_key(is_key);
            int len = 4;
            if (type == "TINYINT") {
                len = 1;
            } else if (type == "LARGEINT") {
                len = 16;
            }
            c->set_length(len);
            c->set_index_length(len);
            c->set_is_nullable(nullable);
            c->set_aggregation("NONE");
        };
        add_col(0, "k1", "INT", true, false);
        add_col(1, "v1", "INT", false, true);
        add_col(2, "v2", "INT", false, true);
        // BEFORE mirror columns -- always nullable (NULL when no historical row).
        add_col(3, "__BEFORE__v1__", "INT", false, true);
        add_col(4, "__BEFORE__v2__", "INT", false, true);
        add_col(5, BINLOG_TSO_COL, "BIGINT", false, true);
        add_col(6, BINLOG_LSN_COL, "BIGINT", false, false);
        add_col(7, BINLOG_OP_COL, "BIGINT", false, false);
        // init_from_pb reads this straight from the PB (it is not inferred from
        // the column name). [k1,v1,v2,__BEFORE__v1__,__BEFORE__v2__,TSO,LSN,OP]
        pb.set_binlog_tso_col_idx(5);
        pb.set_binlog_lsn_col_idx(6);
        pb.set_binlog_op_col_idx(7);

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    // A row-binlog DUP schema with __BEFORE__* columns for a key-only source
    // (k1 key, hidden delete_sign, 0 visible value columns) -- the BEFORE no-op
    // case. Layout: [k1(0), TSO(1), LSN(2), OP(3)]; no BEFORE column is emitted
    // because num_visible_value_columns()==0.
    TabletSchemaSPtr create_binlog_keyonly_before_schema() {
        TabletSchemaPB pb;
        pb.set_keys_type(DUP_KEYS);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_LZ4);
        pb.set_next_column_unique_id(20);

        auto add_col = [&](int uid, const std::string& name, const std::string& type, bool is_key,
                           bool nullable) {
            ColumnPB* c = pb.add_column();
            c->set_unique_id(uid);
            c->set_name(name);
            c->set_type(type);
            c->set_is_key(is_key);
            int len = 4;
            if (type == "TINYINT") {
                len = 1;
            } else if (type == "LARGEINT") {
                len = 16;
            }
            c->set_length(len);
            c->set_index_length(len);
            c->set_is_nullable(nullable);
            c->set_aggregation("NONE");
        };
        add_col(0, "k1", "INT", true, false);
        add_col(1, BINLOG_TSO_COL, "BIGINT", false, true);
        add_col(2, BINLOG_LSN_COL, "BIGINT", false, false);
        add_col(3, BINLOG_OP_COL, "BIGINT", false, false);
        pb.set_binlog_tso_col_idx(1);
        pb.set_binlog_lsn_col_idx(2);
        pb.set_binlog_op_col_idx(3);

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    // A key-only (k1 key, hidden delete_sign) MoW source schema for the BEFORE
    // no-op case -- num_visible_value_columns()==0.
    TabletSchemaSPtr create_binlog_keyonly_source_schema() {
        TabletSchemaPB pb;
        pb.set_keys_type(UNIQUE_KEYS);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_LZ4);
        pb.set_next_column_unique_id(10);

        auto add_col = [&](int uid, const std::string& name, const std::string& type, bool is_key,
                           bool nullable, const std::string& def, bool visible) {
            ColumnPB* c = pb.add_column();
            c->set_unique_id(uid);
            c->set_name(name);
            c->set_type(type);
            c->set_is_key(is_key);
            c->set_length(type == "TINYINT" ? 1 : 4);
            c->set_index_length(type == "TINYINT" ? 1 : 4);
            c->set_is_nullable(nullable);
            c->set_aggregation("NONE");
            c->set_visible(visible);
            if (!def.empty()) {
                c->set_default_value(def);
            }
        };
        add_col(0, "k1", "INT", true, false, "", true);
        add_col(1, DELETE_SIGN, "TINYINT", false, false, std::to_string(0), /*visible=*/false);
        pb.set_delete_sign_idx(1);

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    // (k1 key, v1, seq, delete_sign hidden) UNIQUE_KEYS MoW source schema WITH a
    // hidden sequence column, for the binlog MoW seq-source path. The matching
    // binlog schema omits that hidden non-key column.
    TabletSchemaSPtr create_binlog_seq_source_schema() {
        TabletSchemaPB pb;
        pb.set_keys_type(UNIQUE_KEYS);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_LZ4);
        pb.set_next_column_unique_id(10);

        auto add_col = [&](int uid, const std::string& name, const std::string& type, bool is_key,
                           bool nullable, const std::string& def = "", bool visible = true) {
            ColumnPB* c = pb.add_column();
            c->set_unique_id(uid);
            c->set_name(name);
            c->set_type(type);
            c->set_is_key(is_key);
            c->set_length(type == "TINYINT" ? 1 : 4);
            c->set_index_length(type == "TINYINT" ? 1 : 4);
            c->set_is_nullable(nullable);
            c->set_aggregation("NONE");
            c->set_visible(visible);
            if (!def.empty()) {
                c->set_default_value(def);
            }
        };
        add_col(0, "k1", "INT", true, false);
        add_col(1, "v1", "INT", false, true, std::to_string(0));
        add_col(2, SEQUENCE_COL, "INT", false, false, std::to_string(0), /*visible=*/false);
        add_col(3, DELETE_SIGN, "TINYINT", false, false, std::to_string(0), /*visible=*/false);
        pb.set_sequence_col_idx(2);
        pb.set_delete_sign_idx(3);

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    // The row-binlog DUP schema matching create_binlog_seq_source_schema:
    //   [k1(0), v1(1), TSO(2), LSN(3), OP(4)]. No BEFORE columns.
    TabletSchemaSPtr create_binlog_seq_schema() {
        TabletSchemaPB pb;
        pb.set_keys_type(DUP_KEYS);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_LZ4);
        pb.set_next_column_unique_id(20);

        auto add_col = [&](int uid, const std::string& name, const std::string& type, bool is_key,
                           bool nullable) {
            ColumnPB* c = pb.add_column();
            c->set_unique_id(uid);
            c->set_name(name);
            c->set_type(type);
            c->set_is_key(is_key);
            int len = 4;
            if (type == "LARGEINT") {
                len = 16;
            }
            c->set_length(len);
            c->set_index_length(len);
            c->set_is_nullable(nullable);
            c->set_aggregation("NONE");
        };
        add_col(0, "k1", "INT", true, false);
        add_col(1, "v1", "INT", false, true);
        add_col(2, BINLOG_TSO_COL, "BIGINT", false, true);
        add_col(3, BINLOG_LSN_COL, "BIGINT", false, false);
        add_col(4, BINLOG_OP_COL, "BIGINT", false, false);
        pb.set_binlog_tso_col_idx(2);
        pb.set_binlog_lsn_col_idx(3);
        pb.set_binlog_op_col_idx(4);

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    // Toggles off the merge-on-write correctness check (which would otherwise
    // re-probe the segment we never actually wrote) around chain.apply.
    Status apply_no_correctness_check(const segment_v2::BlockTransformChain& chain,
                                      TransformExecContext& ctx, Block* block) {
        auto saved = config::enable_merge_on_write_correctness_check;
        config::enable_merge_on_write_correctness_check = false;
        auto st = chain.apply(ctx, block);
        config::enable_merge_on_write_correctness_check = saved;
        return st;
    }

    TransformExecContext exec_ctx(const TabletSchemaSPtr& binlog_schema, RowsetWriterContext* rwc) {
        TransformExecContext ctx;
        ctx.tablet_schema = binlog_schema;
        ctx.write_type = rwc->write_type;
        ctx.rowset_ctx = rwc;
        ctx.segment_id = 0;
        return ctx;
    }
};

// ===========================================================================
// §5.2 Plain derive (no probe): op comes only from the row's own delete sign.
// ===========================================================================

// B6/B7: Plain APPEND (delete sign 0) vs DELETE (delete sign 1) in one block,
// so APPEND is distinguished from a default-0 op column; pin AFTER + per-row LSN.
TEST_F(RowBinlogDeriveTest, PlainAppendAndDeleteWithAfterAndLsn) {
    auto tablet = create_binlog_tablet(/*tablet_id=*/8001, TKeysType::DUP_KEYS, /*mow=*/false);
    ASSERT_TRUE(tablet != nullptr);
    auto binlog_schema = tablet->row_binlog_tablet_schema(); // k1,v1,v2,TSO,LSN,OP
    ASSERT_TRUE(binlog_schema != nullptr);
    auto source_schema = create_binlog_pu_source_schema(); // k1,v1,v2,delete_sign(3 hidden)

    RowsetWriterContext rwc;
    rwc.tablet = tablet;
    rwc.tablet_schema = binlog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_binlog_opt().enable = true;
    auto& cfg = rwc.write_binlog_opt().write_binlog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.write_before = false; // no PU, no BEFORE -> plain derive
    cfg.insert_seg_lsn(0, make_seg_lsn(2));

    auto chain = build_transform_chain(rwc);
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"PlainRowBinlogDerive"}));

    TransformExecContext ctx = exec_ctx(binlog_schema, &rwc);
    ctx.tablet = tablet;

    // full-width source block: row 0 keeps delete sign 0 (APPEND), row 1 sets it (DELETE)
    Block block = source_schema->create_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t ks[] = {1, 2};
        int32_t v1s[] = {10, 20};
        int32_t v2s[] = {100, 200};
        int8_t ds[] = {0, 1};
        for (int i = 0; i < 2; ++i) {
            cols[0]->insert_data(reinterpret_cast<const char*>(&ks[i]), sizeof(int32_t));
            cols[1]->insert_data(reinterpret_cast<const char*>(&v1s[i]), sizeof(int32_t));
            cols[2]->insert_data(reinterpret_cast<const char*>(&v2s[i]), sizeof(int32_t));
            cols[3]->insert_data(reinterpret_cast<const char*>(&ds[i]), sizeof(int8_t));
        }
    }

    ASSERT_TRUE(chain.apply(ctx, &block).ok());

    ASSERT_EQ(block.columns(), binlog_schema->num_columns());
    ASSERT_EQ(block.rows(), 2);
    // AFTER columns carry the source values unchanged
    EXPECT_EQ(read_int(block, 0, 0), 1);
    EXPECT_EQ(read_int(block, 1, 0), 10);
    EXPECT_EQ(read_int(block, 2, 0), 100);
    EXPECT_EQ(read_int(block, 0, 1), 2);
    EXPECT_EQ(read_int(block, 1, 1), 20);
    EXPECT_EQ(read_int(block, 2, 1), 200);
    // op: delete sign decides; both must differ from each other (not vacuous 0)
    const int lsn_idx = binlog_schema->binlog_lsn_col_idx();
    const int op_idx = binlog_schema->binlog_op_col_idx();
    EXPECT_EQ(read_op(block, op_idx, 0), ROW_BINLOG_APPEND); // delete sign 0
    EXPECT_EQ(read_op(block, op_idx, 1), ROW_BINLOG_DELETE); // delete sign 1
    // per-row LSN == the injected make_seg_lsn ids (1000, 1001)
    EXPECT_EQ(read_lsn(block, lsn_idx, 0), 1000);
    EXPECT_EQ(read_lsn(block, lsn_idx, 1), 1001);
}

// B6: a DUP source with no delete-sign column has read_delete_signs()==nullptr,
// so every row is APPEND regardless.
TEST_F(RowBinlogDeriveTest, PlainNoDeleteSignColumnAllAppend) {
    auto tablet = create_binlog_tablet(/*tablet_id=*/8002, TKeysType::DUP_KEYS, /*mow=*/false);
    ASSERT_TRUE(tablet != nullptr);
    auto source_schema = tablet->tablet_schema();            // k1, v1, v2 (no delete sign)
    auto binlog_schema = tablet->row_binlog_tablet_schema(); // + TSO, LSN, OP
    ASSERT_EQ(source_schema->delete_sign_idx(), -1);

    RowsetWriterContext rwc;
    rwc.tablet = tablet;
    rwc.tablet_schema = binlog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_binlog_opt().enable = true;
    auto& cfg = rwc.write_binlog_opt().write_binlog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.write_before = false;
    cfg.insert_seg_lsn(0, make_seg_lsn(2));

    auto chain = build_transform_chain(rwc);
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"PlainRowBinlogDerive"}));

    TransformExecContext ctx = exec_ctx(binlog_schema, &rwc);
    ctx.tablet = tablet;

    Block block = source_schema->create_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t ks[] = {1, 2};
        int32_t v1s[] = {10, 20};
        int32_t v2s[] = {100, 200};
        for (int i = 0; i < 2; ++i) {
            cols[0]->insert_data(reinterpret_cast<const char*>(&ks[i]), sizeof(int32_t));
            cols[1]->insert_data(reinterpret_cast<const char*>(&v1s[i]), sizeof(int32_t));
            cols[2]->insert_data(reinterpret_cast<const char*>(&v2s[i]), sizeof(int32_t));
        }
    }

    ASSERT_TRUE(chain.apply(ctx, &block).ok());
    const int op_idx = binlog_schema->binlog_op_col_idx();
    EXPECT_EQ(read_op(block, op_idx, 0), ROW_BINLOG_APPEND);
    EXPECT_EQ(read_op(block, op_idx, 1), ROW_BINLOG_APPEND);
}

// Hidden keys are part of the row image, while hidden non-key columns are not.
// Keep the source cids intentionally non-contiguous to pin the ordinal mapping
// that used to live in RowBinlogSourceDataWriter.
TEST_F(RowBinlogDeriveTest, PlainMapsHiddenKeysAndSkipsHiddenNonKeys) {
    auto make_column = [](TabletSchemaPB* pb, int uid, const std::string& name,
                          const std::string& type, bool is_key, bool visible, bool nullable) {
        ColumnPB* column = pb->add_column();
        column->set_unique_id(uid);
        column->set_name(name);
        column->set_type(type);
        column->set_is_key(is_key);
        column->set_length(type == "BIGINT" ? 8 : 4);
        column->set_index_length(type == "BIGINT" ? 8 : 4);
        column->set_is_nullable(nullable);
        column->set_aggregation("NONE");
        column->set_visible(visible);
    };

    TabletSchemaPB source_pb;
    source_pb.set_keys_type(DUP_KEYS);
    source_pb.set_num_short_key_columns(2);
    source_pb.set_num_rows_per_row_block(1024);
    source_pb.set_compress_kind(COMPRESS_LZ4);
    source_pb.set_next_column_unique_id(10);
    make_column(&source_pb, 0, "k1", "INT", true, true, false);
    make_column(&source_pb, 1, "k_hidden", "INT", true, false, false);
    make_column(&source_pb, 2, "hidden_value_1", "INT", false, false, true);
    make_column(&source_pb, 3, "v1", "INT", false, true, true);
    make_column(&source_pb, 4, "hidden_value_2", "INT", false, false, true);
    make_column(&source_pb, 5, "v2", "INT", false, true, true);
    auto source_schema = std::make_shared<TabletSchema>();
    source_schema->init_from_pb(source_pb);

    TabletSchemaPB binlog_pb;
    binlog_pb.set_keys_type(DUP_KEYS);
    binlog_pb.set_num_short_key_columns(2);
    binlog_pb.set_num_rows_per_row_block(1024);
    binlog_pb.set_compress_kind(COMPRESS_LZ4);
    binlog_pb.set_next_column_unique_id(20);
    make_column(&binlog_pb, 0, "k1", "INT", true, true, false);
    make_column(&binlog_pb, 1, "k_hidden", "INT", true, false, false);
    make_column(&binlog_pb, 3, "v1", "INT", false, true, true);
    make_column(&binlog_pb, 5, "v2", "INT", false, true, true);
    auto add_system_column = [&](int uid, const std::string& name, bool nullable) {
        make_column(&binlog_pb, uid, name, "BIGINT", false, true, nullable);
    };
    add_system_column(10, BINLOG_TSO_COL, true);
    add_system_column(11, BINLOG_LSN_COL, false);
    add_system_column(12, BINLOG_OP_COL, false);
    binlog_pb.set_binlog_tso_col_idx(4);
    binlog_pb.set_binlog_lsn_col_idx(5);
    binlog_pb.set_binlog_op_col_idx(6);
    auto binlog_schema = std::make_shared<TabletSchema>();
    binlog_schema->init_from_pb(binlog_pb);

    RowsetWriterContext rwc;
    rwc.tablet_schema = binlog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_binlog_opt().enable = true;
    auto& cfg = rwc.write_binlog_opt().write_binlog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.insert_seg_lsn(0, make_seg_lsn(2));

    Block block = source_schema->create_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& columns = guard.mutable_columns();
        int32_t values[2][6] = {{1, 101, 1001, 11, 10001, 111},
                                {2, 102, 1002, 22, 10002, 222}};
        for (const auto& row : values) {
            for (size_t cid = 0; cid < 6; ++cid) {
                columns[cid]->insert_data(reinterpret_cast<const char*>(&row[cid]),
                                          sizeof(int32_t));
            }
        }
    }

    auto chain = build_transform_chain(rwc);
    TransformExecContext ctx = exec_ctx(binlog_schema, &rwc);
    ASSERT_TRUE(chain.apply(ctx, &block).ok());

    ASSERT_EQ(block.columns(), 7);
    EXPECT_EQ(read_int(block, 0, 0), 1);
    EXPECT_EQ(read_int(block, 1, 0), 101);
    EXPECT_EQ(read_int(block, 2, 0), 11);
    EXPECT_EQ(read_int(block, 3, 0), 111);
    EXPECT_EQ(read_int(block, 0, 1), 2);
    EXPECT_EQ(read_int(block, 1, 1), 102);
    EXPECT_EQ(read_int(block, 2, 1), 22);
    EXPECT_EQ(read_int(block, 3, 1), 222);
    EXPECT_TRUE(read_is_null(block, binlog_schema->binlog_tso_col_idx(), 0));
    EXPECT_TRUE(read_is_null(block, binlog_schema->binlog_tso_col_idx(), 1));
    EXPECT_EQ(read_lsn(block, binlog_schema->binlog_lsn_col_idx(), 0), 1000);
    EXPECT_EQ(read_lsn(block, binlog_schema->binlog_lsn_col_idx(), 1), 1001);
    EXPECT_EQ(read_op(block, binlog_schema->binlog_op_col_idx(), 0), ROW_BINLOG_APPEND);
    EXPECT_EQ(read_op(block, binlog_schema->binlog_op_col_idx(), 1), ROW_BINLOG_APPEND);
}

// ===========================================================================
// §5.3 Mow derive (per-row probe): op = APPEND (new) / UPDATE (existing) /
// DELETE (delete sign), with the AFTER image rebuilt from history.
// ===========================================================================

// B12/B13/B17/B22: committed history holds key 1 with a real non-zero v2; the
// UPDATE row's missing v2 must come back from history (not a default 0), the new
// key is an APPEND, and per-row LSN == injected ids.
TEST_F(RowBinlogDeriveTest, MowUpdateAndAppendTakesHistoryV2) {
    auto binlog_tablet =
            create_binlog_tablet(/*tablet_id=*/8101, TKeysType::DUP_KEYS, /*mow=*/false);
    ASSERT_TRUE(binlog_tablet != nullptr);
    auto binlog_schema = binlog_tablet->row_binlog_tablet_schema(); // k1,v1,v2,TSO,LSN,OP
    auto source_schema = create_binlog_pu_source_schema();          // k1,v1,v2,delete_sign

    // seed history with write_rowset_block so v2 holds a real non-zero value:
    // key 1 -> (v1=100, v2=777). key 99 does not exist.
    TabletSharedPtr probe_tablet;
    auto rowset = write_rowset_block(
            source_schema, 8102, 2,
            [](Block& b) {
                auto guard = b.mutate_columns_scoped();
                auto& cols = guard.mutable_columns();
                int32_t k = 1, v1 = 100, v2 = 777;
                int8_t ds = 0;
                cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
                cols[1]->insert_data(reinterpret_cast<const char*>(&v1), sizeof(int32_t));
                cols[2]->insert_data(reinterpret_cast<const char*>(&v2), sizeof(int32_t));
                cols[3]->insert_data(reinterpret_cast<const char*>(&ds), sizeof(int8_t));
            },
            &probe_tablet);
    auto mow = make_mow_context(100, {rowset});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *source_schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k1", "v1"}, false, 0, 0, "UTC", "")
                        .ok());

    RowsetWriterContext rwc;
    rwc.tablet = probe_tablet;
    rwc.tablet_schema = binlog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_binlog_opt().enable = true;
    auto& cfg = rwc.write_binlog_opt().write_binlog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.partial_update_info = pui;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.source.mow_context = mow;
    cfg.write_before = false;
    cfg.insert_seg_lsn(0, make_seg_lsn(2));

    auto chain = build_transform_chain(rwc);
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"MowRowBinlogDerive"}));

    TransformExecContext ctx = exec_ctx(binlog_schema, &rwc);
    ctx.tablet = probe_tablet;
    ctx.mow_context = mow;
    ctx.partial_update_info = pui;

    Block block = source_schema->create_block_by_cids({0, 1}); // narrow PU: k1, v1
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t ks[] = {1, 99};
        int32_t v1s[] = {10, 20};
        for (int i = 0; i < 2; ++i) {
            cols[0]->insert_data(reinterpret_cast<const char*>(&ks[i]), sizeof(int32_t));
            cols[1]->insert_data(reinterpret_cast<const char*>(&v1s[i]), sizeof(int32_t));
        }
    }

    ASSERT_TRUE(apply_no_correctness_check(chain, ctx, &block).ok());

    ASSERT_EQ(block.columns(), binlog_schema->num_columns());
    ASSERT_EQ(block.rows(), 2);
    const int lsn_idx = binlog_schema->binlog_lsn_col_idx();
    const int op_idx = binlog_schema->binlog_op_col_idx();
    // op: key 1 existed -> UPDATE; key 99 new -> APPEND
    EXPECT_EQ(read_op(block, op_idx, 0), ROW_BINLOG_UPDATE);
    EXPECT_EQ(read_op(block, op_idx, 1), ROW_BINLOG_APPEND);
    // AFTER row 0 (UPDATE): key kept, provided v1=10 kept, missing v2 == history 777
    EXPECT_EQ(read_int(block, 0, 0), 1);
    EXPECT_EQ(read_int(block, 1, 0), 10);
    EXPECT_EQ(read_int(block, 2, 0), 777); // <-- v2 comes from history, not default
    // AFTER row 1 (APPEND, brand-new key): key kept, v1 kept, missing v2 default 0
    EXPECT_EQ(read_int(block, 0, 1), 99);
    EXPECT_EQ(read_int(block, 1, 1), 20);
    EXPECT_EQ(read_int(block, 2, 1), 0);
    // per-row LSN == injected ids
    EXPECT_EQ(read_lsn(block, lsn_idx, 0), 1000);
    EXPECT_EQ(read_lsn(block, lsn_idx, 1), 1001);
}

// B14/B15: a row carrying a delete sign is a DELETE whether the key exists
// (old key) or not (new key); the delete sign rides in the update columns.
TEST_F(RowBinlogDeriveTest, MowDeleteExistingAndNewKey) {
    auto binlog_tablet =
            create_binlog_tablet(/*tablet_id=*/8201, TKeysType::DUP_KEYS, /*mow=*/false);
    ASSERT_TRUE(binlog_tablet != nullptr);
    auto binlog_schema = binlog_tablet->row_binlog_tablet_schema();
    auto source_schema = create_binlog_pu_source_schema(); // k1,v1,v2,delete_sign(3)

    TabletSharedPtr probe_tablet;
    auto rowset = write_rowset_block(
            source_schema, 8202, 2,
            [](Block& b) {
                auto guard = b.mutate_columns_scoped();
                auto& cols = guard.mutable_columns();
                int32_t k = 1, v1 = 100, v2 = 777;
                int8_t ds = 0;
                cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
                cols[1]->insert_data(reinterpret_cast<const char*>(&v1), sizeof(int32_t));
                cols[2]->insert_data(reinterpret_cast<const char*>(&v2), sizeof(int32_t));
                cols[3]->insert_data(reinterpret_cast<const char*>(&ds), sizeof(int8_t));
            },
            &probe_tablet);
    auto mow = make_mow_context(100, {rowset});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *source_schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k1", "v1", DELETE_SIGN}, false, 0,
                          0, "UTC", "")
                        .ok());

    RowsetWriterContext rwc;
    rwc.tablet = probe_tablet;
    rwc.tablet_schema = binlog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_binlog_opt().enable = true;
    auto& cfg = rwc.write_binlog_opt().write_binlog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.partial_update_info = pui;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.source.mow_context = mow;
    cfg.write_before = false;
    cfg.insert_seg_lsn(0, make_seg_lsn(2));

    auto chain = build_transform_chain(rwc);
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"MowRowBinlogDerive"}));

    TransformExecContext ctx = exec_ctx(binlog_schema, &rwc);
    ctx.tablet = probe_tablet;
    ctx.mow_context = mow;
    ctx.partial_update_info = pui;

    // narrow PU block in update_cids order {k1, v1, delete_sign}
    Block block = source_schema->create_block_by_cids({0, 1, 3});
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t ks[] = {1, 99}; // existing key, then new key
        int32_t v1s[] = {10, 20};
        int8_t one8 = 1; // both rows are deletes
        for (int i = 0; i < 2; ++i) {
            cols[0]->insert_data(reinterpret_cast<const char*>(&ks[i]), sizeof(int32_t));
            cols[1]->insert_data(reinterpret_cast<const char*>(&v1s[i]), sizeof(int32_t));
            cols[2]->insert_data(reinterpret_cast<const char*>(&one8), sizeof(int8_t));
        }
    }

    ASSERT_TRUE(apply_no_correctness_check(chain, ctx, &block).ok());

    const int lsn_idx = binlog_schema->binlog_lsn_col_idx();
    const int op_idx = binlog_schema->binlog_op_col_idx();
    EXPECT_EQ(read_op(block, op_idx, 0), ROW_BINLOG_DELETE); // existing key + delete sign
    EXPECT_EQ(read_op(block, op_idx, 1), ROW_BINLOG_DELETE); // new key + delete sign
    EXPECT_EQ(read_lsn(block, lsn_idx, 0), 1000);
    EXPECT_EQ(read_lsn(block, lsn_idx, 1), 1001);
}

// ===========================================================================
// §5.3.3 / §5.3.4 BEFORE image (write_before=true). Previously 0 coverage.
// ===========================================================================

// B19/B21: UPDATE & DELETE rows mirror the historical value columns into the
// __BEFORE__* columns; the APPEND row (no history) has BEFORE == NULL. BEFORE
// covers value columns only (not the key, not delete_sign).
TEST_F(RowBinlogDeriveTest, MowBeforeImageMirrorsHistory) {
    auto source_schema = create_binlog_pu_source_schema(); // k1,v1,v2,delete_sign(3 hidden)
    auto binlog_schema = create_binlog_before_schema(); // + __BEFORE__v1__/v2__ + TSO/LSN/OP
    ASSERT_EQ(binlog_schema->binlog_lsn_col_idx(), 6);
    ASSERT_EQ(binlog_schema->num_columns(), 8U);

    // history: key 1 -> (v1=100, v2=0); key 2 -> (v1=300, v2=400). key 98 new.
    TabletSharedPtr probe_tablet;
    auto rowset = write_rowset_block(
            source_schema, 8302, 2,
            [](Block& b) {
                auto guard = b.mutate_columns_scoped();
                auto& cols = guard.mutable_columns();
                int32_t ks[] = {1, 2};
                int32_t v1s[] = {100, 300};
                int32_t v2s[] = {0, 400};
                int8_t ds = 0;
                for (int i = 0; i < 2; ++i) {
                    cols[0]->insert_data(reinterpret_cast<const char*>(&ks[i]), sizeof(int32_t));
                    cols[1]->insert_data(reinterpret_cast<const char*>(&v1s[i]), sizeof(int32_t));
                    cols[2]->insert_data(reinterpret_cast<const char*>(&v2s[i]), sizeof(int32_t));
                    cols[3]->insert_data(reinterpret_cast<const char*>(&ds), sizeof(int8_t));
                }
            },
            &probe_tablet);
    auto mow = make_mow_context(100, {rowset});

    RowsetWriterContext rwc;
    rwc.tablet = probe_tablet;
    rwc.tablet_schema = binlog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_binlog_opt().enable = true;
    auto& cfg = rwc.write_binlog_opt().write_binlog_config();
    cfg.source.tablet_schema = source_schema;
    // write_before=true alone (no PU) routes to the MoW derive; the source block
    // is a full-width upsert, no partial_update_info.
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.source.mow_context = mow;
    cfg.write_before = true;
    cfg.insert_seg_lsn(0, make_seg_lsn(3));

    // setup_retriever_and_lookup requires partial_update_info on the retriever
    // context (build_after_block / retrieve_historical_row DCHECK it). For a
    // full-width upsert (no PU), supply an empty fixed-PU info covering all
    // columns so it is NOT treated as a partial update by the derive.
    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *source_schema, UniqueKeyUpdateModePB::UPSERT,
                          PartialUpdateNewRowPolicyPB::APPEND, {}, false, 0, 0, "UTC", "")
                        .ok());
    cfg.source.partial_update_info = pui;

    auto chain = build_transform_chain(rwc); // write_before -> MoW derive
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"MowRowBinlogDerive"}));

    TransformExecContext ctx = exec_ctx(binlog_schema, &rwc);
    ctx.tablet = probe_tablet;
    ctx.mow_context = mow;
    ctx.partial_update_info = pui;

    // full-width upsert source block (k1,v1,v2,delete_sign):
    //   r0 key1 (11,1) delete 0 -> UPDATE; r1 key2 (0,0) delete 1 -> DELETE;
    //   r2 key98 (50,5) delete 0 -> APPEND.
    Block block = source_schema->create_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t ks[] = {1, 2, 98};
        int32_t v1s[] = {11, 0, 50};
        int32_t v2s[] = {1, 0, 5};
        int8_t ds[] = {0, 1, 0};
        for (int i = 0; i < 3; ++i) {
            cols[0]->insert_data(reinterpret_cast<const char*>(&ks[i]), sizeof(int32_t));
            cols[1]->insert_data(reinterpret_cast<const char*>(&v1s[i]), sizeof(int32_t));
            cols[2]->insert_data(reinterpret_cast<const char*>(&v2s[i]), sizeof(int32_t));
            cols[3]->insert_data(reinterpret_cast<const char*>(&ds[i]), sizeof(int8_t));
        }
    }

    ASSERT_TRUE(apply_no_correctness_check(chain, ctx, &block).ok());

    ASSERT_EQ(block.columns(), binlog_schema->num_columns());
    ASSERT_EQ(block.rows(), 3);
    const int lsn_idx = binlog_schema->binlog_lsn_col_idx();
    const int op_idx = binlog_schema->binlog_op_col_idx();
    const int before_v1 = 3;
    const int before_v2 = 4;

    // op
    EXPECT_EQ(read_op(block, op_idx, 0), ROW_BINLOG_UPDATE);
    EXPECT_EQ(read_op(block, op_idx, 1), ROW_BINLOG_DELETE);
    EXPECT_EQ(read_op(block, op_idx, 2), ROW_BINLOG_APPEND);

    // AFTER carries the source values
    EXPECT_EQ(read_int(block, 1, 0), 11);
    EXPECT_EQ(read_int(block, 2, 0), 1);
    EXPECT_EQ(read_int(block, 1, 2), 50);
    EXPECT_EQ(read_int(block, 2, 2), 5);

    // BEFORE of UPDATE row 0 == history (v1=100, v2=0), not NULL
    EXPECT_FALSE(read_is_null(block, before_v1, 0));
    EXPECT_FALSE(read_is_null(block, before_v2, 0));
    EXPECT_EQ(read_int(block, before_v1, 0), 100);
    EXPECT_EQ(read_int(block, before_v2, 0), 0);
    // BEFORE of DELETE row 1 == history (v1=300, v2=400) -- delete rows still
    // read history when write_before=true (skip_delete_sign is off)
    EXPECT_FALSE(read_is_null(block, before_v1, 1));
    EXPECT_FALSE(read_is_null(block, before_v2, 1));
    EXPECT_EQ(read_int(block, before_v1, 1), 300);
    EXPECT_EQ(read_int(block, before_v2, 1), 400);
    // BEFORE of APPEND row 2 (no history) == NULL on both value columns
    EXPECT_TRUE(read_is_null(block, before_v1, 2));
    EXPECT_TRUE(read_is_null(block, before_v2, 2));

    // per-row LSN
    EXPECT_EQ(read_lsn(block, lsn_idx, 0), 1000);
    EXPECT_EQ(read_lsn(block, lsn_idx, 1), 1001);
    EXPECT_EQ(read_lsn(block, lsn_idx, 2), 1002);
}

// B20: BEFORE no-op -- a key-only source (0 visible value columns) emits no
// BEFORE column even with write_before=true; fill_before_columns returns early.
TEST_F(RowBinlogDeriveTest, MowBeforeNoopZeroValueColumns) {
    auto source_schema = create_binlog_keyonly_source_schema(); // k1, delete_sign(hidden)
    auto binlog_schema = create_binlog_keyonly_before_schema(); // [k1, TSO, LSN, OP]
    ASSERT_EQ(source_schema->num_visible_value_columns(), 0U);
    ASSERT_EQ(binlog_schema->binlog_lsn_col_idx(), 2);
    ASSERT_EQ(binlog_schema->num_columns(), 4U);

    // history holds key 1
    TabletSharedPtr probe_tablet;
    auto rowset = write_rowset_block(
            source_schema, 8402, 2,
            [](Block& b) {
                auto guard = b.mutate_columns_scoped();
                auto& cols = guard.mutable_columns();
                int32_t k = 1;
                int8_t ds = 0;
                cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
                cols[1]->insert_data(reinterpret_cast<const char*>(&ds), sizeof(int8_t));
            },
            &probe_tablet);
    auto mow = make_mow_context(100, {rowset});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *source_schema, UniqueKeyUpdateModePB::UPSERT,
                          PartialUpdateNewRowPolicyPB::APPEND, {}, false, 0, 0, "UTC", "")
                        .ok());

    RowsetWriterContext rwc;
    rwc.tablet = probe_tablet;
    rwc.tablet_schema = binlog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_binlog_opt().enable = true;
    auto& cfg = rwc.write_binlog_opt().write_binlog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.partial_update_info = pui;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.source.mow_context = mow;
    cfg.write_before = true;
    cfg.insert_seg_lsn(0, make_seg_lsn(2));

    auto chain = build_transform_chain(rwc);
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"MowRowBinlogDerive"}));

    TransformExecContext ctx = exec_ctx(binlog_schema, &rwc);
    ctx.tablet = probe_tablet;
    ctx.mow_context = mow;
    ctx.partial_update_info = pui;

    Block block = source_schema->create_block(); // full width: k1, delete_sign
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t ks[] = {1, 98};
        int8_t ds = 0;
        for (int32_t k : ks) {
            cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
            cols[1]->insert_data(reinterpret_cast<const char*>(&ds), sizeof(int8_t));
        }
    }

    ASSERT_TRUE(apply_no_correctness_check(chain, ctx, &block).ok());

    ASSERT_EQ(block.columns(), binlog_schema->num_columns()); // [k1, TSO, LSN, OP]
    ASSERT_EQ(block.rows(), 2);
    EXPECT_EQ(read_int(block, 0, 0), 1);
    EXPECT_EQ(read_int(block, 0, 1), 98);
    const int op_idx = binlog_schema->binlog_op_col_idx();
    EXPECT_EQ(read_op(block, op_idx, 0), ROW_BINLOG_UPDATE); // key 1 existed
    EXPECT_EQ(read_op(block, op_idx, 1), ROW_BINLOG_APPEND); // key 98 new
}

// Binlog MoW over a SEQUENCE source. The binlog retriever probes with
// skip_seq_loses=false (historical_row_retriever.cpp), so a row whose seq loses
// to history (FOUND_NEWER) is NOT default-filled -- it still reads the old row
// and is op=UPDATE, keeping history v1. This differs from the fixed-PU fill
// stage (skip_seq_loses=true), which would default-fill the seq loser. The
// blueprint's "seq loses -> DELETE via use_default_or_null" (§5.5) does not hold
// for this policy; the FOUND+use_default DELETE arm is covered by
// MowDeleteExistingAndNewKey instead.
TEST_F(RowBinlogDeriveTest, MowSeqSourceSeqLosesStillReadsHistory) {
    auto source_schema = create_binlog_seq_source_schema(); // k1,v1,seq/delete-sign hidden
    auto binlog_schema = create_binlog_seq_schema();        // [k1,v1,TSO,LSN,OP]
    ASSERT_EQ(binlog_schema->binlog_lsn_col_idx(), 3);
    ASSERT_EQ(binlog_schema->num_columns(), 5U);

    // history: key 1 -> (v1=100, seq=10); key 2 -> (v1=200, seq=10).
    TabletSharedPtr probe_tablet;
    auto rowset = write_rowset_block(
            source_schema, 8902, 2,
            [](Block& b) {
                auto guard = b.mutate_columns_scoped();
                auto& cols = guard.mutable_columns();
                int32_t ks[] = {1, 2};
                int32_t v1s[] = {100, 200};
                int32_t seqs[] = {10, 10};
                int8_t ds = 0;
                for (int i = 0; i < 2; ++i) {
                    cols[0]->insert_data(reinterpret_cast<const char*>(&ks[i]), sizeof(int32_t));
                    cols[1]->insert_data(reinterpret_cast<const char*>(&v1s[i]), sizeof(int32_t));
                    cols[2]->insert_data(reinterpret_cast<const char*>(&seqs[i]), sizeof(int32_t));
                    cols[3]->insert_data(reinterpret_cast<const char*>(&ds), sizeof(int8_t));
                }
            },
            &probe_tablet);
    auto mow = make_mow_context(100, {rowset});

    // fixed PU that provides {k1, seq} -- v1 is missing and must come from history.
    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *source_schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k1", SEQUENCE_COL}, false, 0, 0,
                          "UTC", "")
                        .ok());

    RowsetWriterContext rwc;
    rwc.tablet = probe_tablet;
    rwc.tablet_schema = binlog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_binlog_opt().enable = true;
    auto& cfg = rwc.write_binlog_opt().write_binlog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.partial_update_info = pui;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.source.mow_context = mow;
    cfg.write_before = false;
    cfg.insert_seg_lsn(0, make_seg_lsn(2));

    auto chain = build_transform_chain(rwc);
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"MowRowBinlogDerive"}));

    TransformExecContext ctx = exec_ctx(binlog_schema, &rwc);
    ctx.tablet = probe_tablet;
    ctx.mow_context = mow;
    ctx.partial_update_info = pui;

    // narrow PU block {k1, seq}: r0 seq=3 loses to history 10; r1 seq=20 wins.
    Block block = source_schema->create_block_by_cids({0, 2});
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t ks[] = {1, 2};
        int32_t seqs[] = {3, 20};
        for (int i = 0; i < 2; ++i) {
            cols[0]->insert_data(reinterpret_cast<const char*>(&ks[i]), sizeof(int32_t));
            cols[1]->insert_data(reinterpret_cast<const char*>(&seqs[i]), sizeof(int32_t));
        }
    }

    ASSERT_TRUE(apply_no_correctness_check(chain, ctx, &block).ok());

    ASSERT_EQ(block.columns(), binlog_schema->num_columns());
    ASSERT_EQ(block.rows(), 2);
    const int lsn_idx = binlog_schema->binlog_lsn_col_idx();
    const int op_idx = binlog_schema->binlog_op_col_idx();
    // both rows read the old row (use_default_or_null=false): the seq loser is NOT
    // default-filled, so op=UPDATE and v1 is the historical value.
    EXPECT_EQ(read_op(block, op_idx, 0), ROW_BINLOG_UPDATE); // seq loses, still UPDATE
    EXPECT_EQ(read_op(block, op_idx, 1), ROW_BINLOG_UPDATE); // seq wins
    EXPECT_EQ(read_int(block, 0, 0), 1);
    EXPECT_EQ(read_int(block, 1, 0), 100); // <-- history v1, NOT default 0 (the key point)
    EXPECT_EQ(read_int(block, 0, 1), 2);
    EXPECT_EQ(read_int(block, 1, 1), 200);
    EXPECT_EQ(read_lsn(block, lsn_idx, 0), 1000);
    EXPECT_EQ(read_lsn(block, lsn_idx, 1), 1001);
}

// ===========================================================================
// §5.4 Reject / negative paths.
// ===========================================================================

// N4 (B9): binlog<row> does not support flexible partial update.
TEST_F(RowBinlogDeriveTest, MowRejectsFlexiblePartialUpdate) {
    auto binlog_tablet =
            create_binlog_tablet(/*tablet_id=*/8501, TKeysType::DUP_KEYS, /*mow=*/false);
    ASSERT_TRUE(binlog_tablet != nullptr);
    auto binlog_schema = binlog_tablet->row_binlog_tablet_schema();
    auto source_schema = create_binlog_pu_source_schema();
    auto probe_tablet = make_tablet(source_schema, /*tablet_id=*/8502);
    auto mow = make_mow_context(100, {});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *source_schema,
                          UniqueKeyUpdateModePB::UPDATE_FLEXIBLE_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {}, false, 0, 0, "UTC", "")
                        .ok());

    RowsetWriterContext rwc;
    rwc.tablet = probe_tablet;
    rwc.tablet_schema = binlog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_binlog_opt().enable = true;
    auto& cfg = rwc.write_binlog_opt().write_binlog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.partial_update_info = pui;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.source.mow_context = mow;
    cfg.write_before = false;
    cfg.insert_seg_lsn(0, make_seg_lsn(2));

    auto chain = build_transform_chain(rwc);
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"MowRowBinlogDerive"}));

    TransformExecContext ctx = exec_ctx(binlog_schema, &rwc);
    ctx.tablet = probe_tablet;
    ctx.mow_context = mow;
    ctx.partial_update_info = pui;

    Block block = source_schema->create_block_by_cids({0, 1});
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 1, v1 = 10;
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&v1), sizeof(int32_t));
    }

    auto st = chain.apply(ctx, &block);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("does not support flexible partial update"), std::string::npos)
            << st;
}

// N5/N6 (B10): the fixed-PU MoW derive rejects a block that is too narrow
// (<= num_key_columns) or too wide (>= num_columns).
TEST_F(RowBinlogDeriveTest, MowFixedPartialUpdateRejectsBadWidth) {
    auto binlog_tablet =
            create_binlog_tablet(/*tablet_id=*/8601, TKeysType::DUP_KEYS, /*mow=*/false);
    ASSERT_TRUE(binlog_tablet != nullptr);
    auto binlog_schema = binlog_tablet->row_binlog_tablet_schema();
    auto source_schema = create_binlog_pu_source_schema(); // 1 key + 2 visible + hidden ds = 4 cols
    ASSERT_EQ(source_schema->num_key_columns(), 1U);
    ASSERT_EQ(source_schema->num_columns(), 4U);
    auto probe_tablet = make_tablet(source_schema, /*tablet_id=*/8602);
    auto mow = make_mow_context(100, {});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *source_schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k1", "v1"}, false, 0, 0, "UTC", "")
                        .ok());

    RowsetWriterContext rwc;
    rwc.tablet = probe_tablet;
    rwc.tablet_schema = binlog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_binlog_opt().enable = true;
    auto& cfg = rwc.write_binlog_opt().write_binlog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.partial_update_info = pui;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.source.mow_context = mow;
    cfg.write_before = false;

    auto chain = build_transform_chain(rwc);
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"MowRowBinlogDerive"}));

    auto make_ctx = [&] {
        TransformExecContext ctx = exec_ctx(binlog_schema, &rwc);
        ctx.tablet = probe_tablet;
        ctx.mow_context = mow;
        ctx.partial_update_info = pui;
        return ctx;
    };

    // too narrow: only the key column (1 <= num_key_columns 1)
    {
        cfg.insert_seg_lsn(0, make_seg_lsn(1));
        TransformExecContext ctx = make_ctx();
        Block block = source_schema->create_block_by_cids({0});
        block.get_by_position(0).column->assert_mutable()->insert_default();
        auto st = chain.apply(ctx, &block);
        EXPECT_FALSE(st.ok());
        EXPECT_NE(st.to_string().find("illegal partial update block columns"), std::string::npos)
                << st;
    }
    // too wide: full width including hidden delete_sign (4 >= num_columns 4)
    {
        cfg.insert_seg_lsn(0, make_seg_lsn(1));
        TransformExecContext ctx = make_ctx();
        Block block = source_schema->create_block(); // 4 columns
        block.get_by_position(0).column->assert_mutable()->insert_default();
        block.get_by_position(1).column->assert_mutable()->insert_default();
        block.get_by_position(2).column->assert_mutable()->insert_default();
        block.get_by_position(3).column->assert_mutable()->insert_default();
        auto st = chain.apply(ctx, &block);
        EXPECT_FALSE(st.ok());
        EXPECT_NE(st.to_string().find("illegal partial update block columns"), std::string::npos)
                << st;
    }
}

// N2 (B3): a binlog write with no source tablet schema fails with a clear error.
TEST_F(RowBinlogDeriveTest, RejectsMissingSourceSchema) {
    auto tablet = create_binlog_tablet(/*tablet_id=*/8701, TKeysType::DUP_KEYS, /*mow=*/false);
    ASSERT_TRUE(tablet != nullptr);
    auto binlog_schema = tablet->row_binlog_tablet_schema();

    RowsetWriterContext rwc;
    rwc.tablet = tablet;
    rwc.tablet_schema = binlog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_binlog_opt().enable = true;
    auto& cfg = rwc.write_binlog_opt().write_binlog_config();
    cfg.source.tablet_schema = nullptr; // missing source schema
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.write_before = false;
    cfg.insert_seg_lsn(0, make_seg_lsn(1));

    auto chain = build_transform_chain(rwc); // no PU, no BEFORE -> plain derive
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"PlainRowBinlogDerive"}));

    TransformExecContext ctx = exec_ctx(binlog_schema, &rwc);
    ctx.tablet = tablet;

    Block block = create_binlog_pu_source_schema()->create_block();
    block.get_by_position(0).column->assert_mutable()->insert_default();
    block.get_by_position(1).column->assert_mutable()->insert_default();
    block.get_by_position(2).column->assert_mutable()->insert_default();
    block.get_by_position(3).column->assert_mutable()->insert_default();
    auto st = chain.apply(ctx, &block);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("missing source_tablet_schema"), std::string::npos) << st;
}

// N3 (B4): a binlog block flushed without a segment id (segment_id == -1, the
// add_block seam) is rejected before any LSN is consumed.
TEST_F(RowBinlogDeriveTest, RejectsNegativeSegmentId) {
    auto tablet = create_binlog_tablet(/*tablet_id=*/8801, TKeysType::DUP_KEYS, /*mow=*/false);
    ASSERT_TRUE(tablet != nullptr);
    auto source_schema = tablet->tablet_schema();
    auto binlog_schema = tablet->row_binlog_tablet_schema();

    RowsetWriterContext rwc;
    rwc.tablet = tablet;
    rwc.tablet_schema = binlog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_binlog_opt().enable = true;
    auto& cfg = rwc.write_binlog_opt().write_binlog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.write_before = false;
    cfg.insert_seg_lsn(0, make_seg_lsn(1));

    auto chain = build_transform_chain(rwc);
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"PlainRowBinlogDerive"}));

    TransformExecContext ctx = exec_ctx(binlog_schema, &rwc);
    ctx.tablet = tablet;
    ctx.segment_id = -1; // add_block seam: no segment id

    Block block = source_schema->create_block();
    block.get_by_position(0).column->assert_mutable()->insert_default();
    block.get_by_position(1).column->assert_mutable()->insert_default();
    block.get_by_position(2).column->assert_mutable()->insert_default();
    auto st = chain.apply(ctx, &block);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("flush_single_block"), std::string::npos) << st;
}

// N1 (B2) cloud guard -> NotSupported("append binlog"): SKIPPED. The guard is
// `config::is_cloud_mode()`, which depends on cloud_unique_id being set at
// process start (BE-wide), and the MowTransformTestBase fixture boots a local
// StorageEngine that is incompatible with cloud mode. Toggling it under the
// fixture is not unit-testable here.
//
// N7 (B5) write_before=false but schema carries BEFORE columns -> DCHECK:
// SKIPPED. It is a DCHECK that aborts in a debug build (and is a no-op in
// release), so it cannot be asserted with a normal EXPECT.

} // namespace doris
