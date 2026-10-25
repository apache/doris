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

// Branch-coverage tests for the changelog derive stages
// (PlainChangelogDeriveStage + MowChangelogDeriveStage), driven through
// build_transform_chain so the chain picks the stage the way production does.
//
// Test groups:
//   Plain*   op from the row's own delete sign (APPEND / DELETE), the AFTER
//            image, per-row LSN, and the hidden-column mapping (hidden keys are
//            part of the row image, hidden non-keys are not)
//   Mow*     op from the history probe (APPEND on a new key, UPDATE on an
//            existing one, DELETE on a delete sign), AFTER rebuilt for a fixed
//            partial update, the BEFORE image mirroring history, the
//            zero-value-column no-op, and the sequence-column source path
//   Flexible partial update materialization through the MoW derive stage,
//            including aligned LSN and operation output
//   Rejects* a fixed partial update of the wrong width, a missing source
//            schema, and a negative segment id
//
// Oracle values are ported from the deleted ChangelogSegmentWriter.

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "common/config.h"
#include "core/column/column_complex.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "storage/changelog.h"
#include "storage/mow/mow_transform_test_base.h"
#include "storage/partial_update_info.h"
#include "storage/transform/block_transform.h"

namespace doris {

using segment_v2::build_transform_chain;
using segment_v2::TransformExecContext;

class ChangelogDeriveTest : public MowTransformTestBase {
protected:
    // Reads a (nullable) BIGINT op cell from the changelog block.
    static int64_t read_op(const Block& block, int col_pos, size_t row) {
        const IColumn* col = block.get_by_position(col_pos).column.get();
        if (col->is_nullable()) {
            col = &assert_cast<const ColumnNullable&>(*col).get_nested_column();
        }
        return assert_cast<const ColumnInt64&>(*col).get_data()[row];
    }

    // Reads a (nullable) BIGINT LSN cell from the changelog block.
    static int64_t read_lsn(const Block& block, int col_pos, size_t row) {
        const IColumn* col = block.get_by_position(col_pos).column.get();
        if (col->is_nullable()) {
            col = &assert_cast<const ColumnNullable&>(*col).get_nested_column();
        }
        return assert_cast<const ColumnInt64&>(*col).get_data()[row];
    }

    // A changelog DUP schema WITH BEFORE mirror columns for the changelog
    // source `create_changelog_pu_source_schema()` (k1 key, v1, v2, hidden
    // delete_sign). Layout matches resolve_changelog_context's before_col_start =
    // normal_col_start + normal_col_num: the visible source columns first, then
    // one BEFORE column per visible value column, then TSO/LSN/OP.
    //   [k1(0), v1(1), v2(2), __DORIS_CHANGELOG_BEFORE_v1__(3), __DORIS_CHANGELOG_BEFORE_v2__(4),
    //    __DORIS_CHANGELOG_TSO__(5), __DORIS_CHANGELOG_LSN__(6), __DORIS_CHANGELOG_OP__(7)]
    // The fixture's create_changelog_tablet builds a changelog schema with no BEFORE
    // columns, so this variant is defined locally for the write_before=true path.
    TabletSchemaSPtr create_changelog_before_schema() {
        TabletSchemaPB pb;
        pb.set_keys_type(DUP_KEYS); // changelog is written as a DUP block
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
            } else if (type == "BIGINT") {
                len = 8;
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
        add_col(3, changelog::build_before_column_name("v1"), "INT", false, true);
        add_col(4, changelog::build_before_column_name("v2"), "INT", false, true);
        add_col(5, CHANGELOG_TSO_COL, "BIGINT", false, true);
        add_col(6, CHANGELOG_LSN_COL, "BIGINT", false, false);
        add_col(7, CHANGELOG_OP_COL, "BIGINT", false, false);

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    // A changelog DUP schema with BEFORE columns for a key-only source
    // (k1 key, hidden delete_sign, 0 visible value columns) -- the BEFORE no-op
    // case. Layout: [k1(0), TSO(1), LSN(2), OP(3)]; no BEFORE column is emitted
    // because num_visible_value_columns()==0.
    TabletSchemaSPtr create_changelog_keyonly_before_schema() {
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
            } else if (type == "BIGINT") {
                len = 8;
            } else if (type == "LARGEINT") {
                len = 16;
            }
            c->set_length(len);
            c->set_index_length(len);
            c->set_is_nullable(nullable);
            c->set_aggregation("NONE");
        };
        add_col(0, "k1", "INT", true, false);
        add_col(1, CHANGELOG_TSO_COL, "BIGINT", false, true);
        add_col(2, CHANGELOG_LSN_COL, "BIGINT", false, false);
        add_col(3, CHANGELOG_OP_COL, "BIGINT", false, false);

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    // A key-only (k1 key, hidden delete_sign) MoW source schema for the BEFORE
    // no-op case -- num_visible_value_columns()==0.
    TabletSchemaSPtr create_changelog_keyonly_source_schema() {
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
    // hidden sequence column, for the changelog MoW seq-source path. The matching
    // changelog schema omits that hidden non-key column.
    TabletSchemaSPtr create_changelog_seq_source_schema() {
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

    // The changelog DUP schema matching create_changelog_seq_source_schema:
    //   [k1(0), v1(1), TSO(2), LSN(3), OP(4)]. No BEFORE columns.
    TabletSchemaSPtr create_changelog_seq_schema() {
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
            if (type == "BIGINT") {
                len = 8;
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
        add_col(2, CHANGELOG_TSO_COL, "BIGINT", false, true);
        add_col(3, CHANGELOG_LSN_COL, "BIGINT", false, false);
        add_col(4, CHANGELOG_OP_COL, "BIGINT", false, false);

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

    TransformExecContext exec_ctx(const TabletSchemaSPtr& changelog_schema,
                                  RowsetWriterContext* rwc) {
        TransformExecContext ctx;
        ctx.tablet_schema = changelog_schema;
        ctx.write_type = rwc->write_type;
        ctx.rowset_ctx = rwc;
        ctx.segment_id = 0;
        return ctx;
    }

    void register_segment_lsns(RowsetWriterContext& rwc, size_t num_rows) {
        rwc.allocated_lsn_map = std::make_shared<segment_v2::SegmentAllocatedLsnMap>();
        rwc.insert_segment_allocated_lsns(0, make_seg_lsn(num_rows));
    }
};

// ===========================================================================
// §5.2 Plain derive (no probe): op comes only from the row's own delete sign.
// ===========================================================================

// B6/B7: Plain APPEND (delete sign 0) vs DELETE (delete sign 1) in one block,
// so APPEND is distinguished from a default-0 op column; pin AFTER + per-row LSN.
TEST_F(ChangelogDeriveTest, PlainAppendAndDeleteWithAfterAndLsn) {
    auto tablet = create_changelog_tablets(/*tablet_id=*/8001, TKeysType::DUP_KEYS, /*mow=*/false)
                          .changelog;
    ASSERT_TRUE(tablet != nullptr);
    auto changelog_schema = tablet->tablet_schema(); // k1,v1,v2,TSO,LSN,OP
    ASSERT_TRUE(changelog_schema != nullptr);
    auto source_schema = create_changelog_pu_source_schema(); // k1,v1,v2,delete_sign(3 hidden)

    RowsetWriterContext rwc;
    rwc.tablet = tablet;
    rwc.tablet_schema = changelog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_changelog_opt().enable = true;
    auto& cfg = rwc.write_changelog_opt().write_changelog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.write_before = false; // no PU, no BEFORE -> plain derive
    register_segment_lsns(rwc, 2);

    ASSERT_TRUE(testutil::resolve_test_changelog_layout(rwc).ok());
    auto chain = build_transform_chain(rwc);
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"PlainChangelogDerive"}));

    TransformExecContext ctx = exec_ctx(changelog_schema, &rwc);
    ctx.tablet = tablet;

    // full-width source block: row 0 keeps delete sign 0 (APPEND), row 1 sets it (DELETE)
    Block block = source_schema->create_storage_block();
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

    ASSERT_EQ(block.columns(), changelog_schema->num_columns());
    ASSERT_EQ(block.rows(), 2);
    // AFTER columns carry the source values unchanged
    EXPECT_EQ(read_int(block, 0, 0), 1);
    EXPECT_EQ(read_int(block, 1, 0), 10);
    EXPECT_EQ(read_int(block, 2, 0), 100);
    EXPECT_EQ(read_int(block, 0, 1), 2);
    EXPECT_EQ(read_int(block, 1, 1), 20);
    EXPECT_EQ(read_int(block, 2, 1), 200);
    // op: delete sign decides; both must differ from each other (not vacuous 0)
    const int lsn_idx = changelog_schema->changelog_lsn_col_idx();
    const int op_idx = changelog_schema->changelog_op_col_idx();
    EXPECT_EQ(read_op(block, op_idx, 0),
              static_cast<int64_t>(ChangelogOp::APPEND)); // delete sign 0
    EXPECT_EQ(read_op(block, op_idx, 1),
              static_cast<int64_t>(ChangelogOp::DELETE)); // delete sign 1
    // per-row LSN == the injected make_seg_lsn ids (1000, 1001)
    EXPECT_EQ(read_lsn(block, lsn_idx, 0), 1000);
    EXPECT_EQ(read_lsn(block, lsn_idx, 1), 1001);
}

// B6: a DUP source with no delete-sign column has read_delete_signs()==nullptr,
// so every row is APPEND regardless.
TEST_F(ChangelogDeriveTest, PlainNoDeleteSignColumnAllAppend) {
    auto tablets = create_changelog_tablets(/*tablet_id=*/8002, TKeysType::DUP_KEYS, /*mow=*/false);
    auto tablet = tablets.changelog;
    ASSERT_TRUE(tablet != nullptr);
    auto source_schema = tablets.source->tablet_schema(); // k1, v1, v2 (no delete sign)
    auto changelog_schema = tablet->tablet_schema();      // + TSO, LSN, OP
    ASSERT_EQ(source_schema->delete_sign_idx(), -1);

    RowsetWriterContext rwc;
    rwc.tablet = tablet;
    rwc.tablet_schema = changelog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_changelog_opt().enable = true;
    auto& cfg = rwc.write_changelog_opt().write_changelog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.write_before = false;
    register_segment_lsns(rwc, 2);

    ASSERT_TRUE(testutil::resolve_test_changelog_layout(rwc).ok());
    auto chain = build_transform_chain(rwc);
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"PlainChangelogDerive"}));

    TransformExecContext ctx = exec_ctx(changelog_schema, &rwc);
    ctx.tablet = tablet;

    Block block = source_schema->create_storage_block();
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
    const int op_idx = changelog_schema->changelog_op_col_idx();
    EXPECT_EQ(read_op(block, op_idx, 0), static_cast<int64_t>(ChangelogOp::APPEND));
    EXPECT_EQ(read_op(block, op_idx, 1), static_cast<int64_t>(ChangelogOp::APPEND));
}

// Hidden keys are part of the row image, while hidden non-key columns are not.
// Keep the source cids intentionally non-contiguous to pin the ordinal mapping
// that used to live in ChangelogSourceDataWriter.
TEST_F(ChangelogDeriveTest, PlainMapsHiddenKeysAndSkipsHiddenNonKeys) {
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

    TabletSchemaPB changelog_pb;
    changelog_pb.set_keys_type(DUP_KEYS);
    changelog_pb.set_num_short_key_columns(2);
    changelog_pb.set_num_rows_per_row_block(1024);
    changelog_pb.set_compress_kind(COMPRESS_LZ4);
    changelog_pb.set_next_column_unique_id(20);
    make_column(&changelog_pb, 0, "k1", "INT", true, true, false);
    make_column(&changelog_pb, 1, "k_hidden", "INT", true, false, false);
    make_column(&changelog_pb, 3, "v1", "INT", false, true, true);
    make_column(&changelog_pb, 5, "v2", "INT", false, true, true);
    auto add_system_column = [&](int uid, const std::string& name, bool nullable) {
        make_column(&changelog_pb, uid, name, "BIGINT", false, true, nullable);
    };
    add_system_column(10, CHANGELOG_TSO_COL, true);
    add_system_column(11, CHANGELOG_LSN_COL, false);
    add_system_column(12, CHANGELOG_OP_COL, false);
    auto changelog_schema = std::make_shared<TabletSchema>();
    changelog_schema->init_from_pb(changelog_pb);

    RowsetWriterContext rwc;
    rwc.tablet_schema = changelog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_changelog_opt().enable = true;
    auto& cfg = rwc.write_changelog_opt().write_changelog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    register_segment_lsns(rwc, 2);

    Block block = source_schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& columns = guard.mutable_columns();
        int32_t values[2][6] = {{1, 101, 1001, 11, 10001, 111}, {2, 102, 1002, 22, 10002, 222}};
        for (const auto& row : values) {
            for (size_t cid = 0; cid < 6; ++cid) {
                columns[cid]->insert_data(reinterpret_cast<const char*>(&row[cid]),
                                          sizeof(int32_t));
            }
        }
    }

    ASSERT_TRUE(testutil::resolve_test_changelog_layout(rwc).ok());
    auto chain = build_transform_chain(rwc);
    TransformExecContext ctx = exec_ctx(changelog_schema, &rwc);
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
    EXPECT_TRUE(read_is_null(block, changelog_schema->changelog_tso_col_idx(), 0));
    EXPECT_TRUE(read_is_null(block, changelog_schema->changelog_tso_col_idx(), 1));
    EXPECT_EQ(read_lsn(block, changelog_schema->changelog_lsn_col_idx(), 0), 1000);
    EXPECT_EQ(read_lsn(block, changelog_schema->changelog_lsn_col_idx(), 1), 1001);
    EXPECT_EQ(read_op(block, changelog_schema->changelog_op_col_idx(), 0),
              static_cast<int64_t>(ChangelogOp::APPEND));
    EXPECT_EQ(read_op(block, changelog_schema->changelog_op_col_idx(), 1),
              static_cast<int64_t>(ChangelogOp::APPEND));
}

// ===========================================================================
// §5.3 Mow derive (per-row probe): op = APPEND (new) / UPDATE (existing) /
// DELETE (delete sign), with the AFTER image rebuilt from history.
// ===========================================================================

// B12/B13/B17/B22: committed history holds key 1 with a real non-zero v2; the
// UPDATE row's missing v2 must come back from history (not a default 0), the new
// key is an APPEND, and per-row LSN == injected ids.
TEST_F(ChangelogDeriveTest, MowUpdateAndAppendTakesHistoryV2) {
    auto changelog_tablet =
            create_changelog_tablets(/*tablet_id=*/8101, TKeysType::DUP_KEYS, /*mow=*/false)
                    .changelog;
    ASSERT_TRUE(changelog_tablet != nullptr);
    auto changelog_schema = changelog_tablet->tablet_schema(); // k1,v1,v2,TSO,LSN,OP
    auto source_schema = create_changelog_pu_source_schema();  // k1,v1,v2,delete_sign

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
    rwc.tablet_schema = changelog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_changelog_opt().enable = true;
    auto& cfg = rwc.write_changelog_opt().write_changelog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.base_tablet = probe_tablet;
    cfg.source.partial_update_info = pui;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.source.mow_context = mow;
    cfg.write_before = false;
    register_segment_lsns(rwc, 2);

    ASSERT_TRUE(testutil::resolve_test_changelog_layout(rwc).ok());
    auto chain = build_transform_chain(rwc);
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"MowChangelogDerive"}));

    TransformExecContext ctx = exec_ctx(changelog_schema, &rwc);
    ctx.tablet = probe_tablet;
    ctx.mow_context = mow;
    ctx.partial_update_info = pui;

    Block block = source_schema->create_storage_block({0, 1}); // narrow PU: k1, v1
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

    ASSERT_EQ(block.columns(), changelog_schema->num_columns());
    ASSERT_EQ(block.rows(), 2);
    const int lsn_idx = changelog_schema->changelog_lsn_col_idx();
    const int op_idx = changelog_schema->changelog_op_col_idx();
    EXPECT_TRUE(read_is_null(block, changelog_schema->changelog_tso_col_idx(), 0));
    EXPECT_TRUE(read_is_null(block, changelog_schema->changelog_tso_col_idx(), 1));
    // op: key 1 existed -> UPDATE; key 99 new -> APPEND
    EXPECT_EQ(read_op(block, op_idx, 0), static_cast<int64_t>(ChangelogOp::UPDATE));
    EXPECT_EQ(read_op(block, op_idx, 1), static_cast<int64_t>(ChangelogOp::APPEND));
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

// A hidden key column is part of the row image AND part of the probe key: the
// key the stage encodes has to cover both key columns, or the lookup misses and
// every row looks brand new. Replaces the deleted writer UT's
// collectHiddenKeyInSourceDataWriter, which asserted the same thing one layer
// down (its source_key_columns() had to hold both).
TEST_F(ChangelogDeriveTest, MowHiddenKeyColumnIsPartOfTheProbeKey) {
    auto add_col = [](TabletSchemaPB* pb, int uid, const std::string& name, const std::string& type,
                      bool is_key, bool visible, bool nullable) {
        ColumnPB* c = pb->add_column();
        c->set_unique_id(uid);
        c->set_name(name);
        c->set_type(type);
        c->set_is_key(is_key);
        const int len = type == "BIGINT" ? 8 : (type == "TINYINT" ? 1 : 4);
        c->set_length(len);
        c->set_index_length(len);
        c->set_is_nullable(nullable);
        c->set_aggregation("NONE");
        c->set_visible(visible);
    };

    // source: k1(key) + a HIDDEN key + v1 + v2 + hidden delete sign
    TabletSchemaPB source_pb;
    source_pb.set_keys_type(UNIQUE_KEYS);
    source_pb.set_num_short_key_columns(1);
    source_pb.set_num_rows_per_row_block(1024);
    source_pb.set_compress_kind(COMPRESS_LZ4);
    source_pb.set_next_column_unique_id(20);
    add_col(&source_pb, 0, "k1", "INT", true, true, false);
    add_col(&source_pb, 1, "__DORIS_TEST_HIDDEN_KEY__", "BIGINT", true, false, false);
    add_col(&source_pb, 2, "v1", "INT", false, true, true);
    add_col(&source_pb, 3, "v2", "INT", false, true, true);
    add_col(&source_pb, 4, DELETE_SIGN, "TINYINT", false, false, false);
    source_pb.set_delete_sign_idx(4);
    auto source_schema = std::make_shared<TabletSchema>();
    source_schema->init_from_pb(source_pb);
    ASSERT_EQ(source_schema->num_key_columns(), 2U);

    // changelog: both keys (hidden keys stay), the two values, then TSO/LSN/OP
    TabletSchemaPB changelog_pb;
    changelog_pb.set_keys_type(DUP_KEYS);
    changelog_pb.set_num_short_key_columns(1);
    changelog_pb.set_num_rows_per_row_block(1024);
    changelog_pb.set_compress_kind(COMPRESS_LZ4);
    changelog_pb.set_next_column_unique_id(20);
    add_col(&changelog_pb, 0, "k1", "INT", true, true, false);
    add_col(&changelog_pb, 1, "__DORIS_TEST_HIDDEN_KEY__", "BIGINT", true, false, false);
    add_col(&changelog_pb, 2, "v1", "INT", false, true, true);
    add_col(&changelog_pb, 3, "v2", "INT", false, true, true);
    add_col(&changelog_pb, 4, CHANGELOG_TSO_COL, "BIGINT", false, false, true);
    add_col(&changelog_pb, 5, CHANGELOG_LSN_COL, "BIGINT", false, false, false);
    add_col(&changelog_pb, 6, CHANGELOG_OP_COL, "BIGINT", false, false, false);
    auto changelog_schema = std::make_shared<TabletSchema>();
    changelog_schema->init_from_pb(changelog_pb);
    ASSERT_EQ(changelog_schema->changelog_lsn_col_idx(), 5);

    // history: (k1=1, hidden key=1001) -> v1=100, v2=777
    TabletSharedPtr probe_tablet;
    auto rowset = write_rowset_block(
            source_schema, 8342, 2,
            [](Block& b) {
                auto guard = b.mutate_columns_scoped();
                auto& cols = guard.mutable_columns();
                int32_t k1 = 1, v1 = 100, v2 = 777;
                int64_t hidden_key = 1001;
                int8_t ds = 0;
                cols[0]->insert_data(reinterpret_cast<const char*>(&k1), sizeof(int32_t));
                cols[1]->insert_data(reinterpret_cast<const char*>(&hidden_key), sizeof(int64_t));
                cols[2]->insert_data(reinterpret_cast<const char*>(&v1), sizeof(int32_t));
                cols[3]->insert_data(reinterpret_cast<const char*>(&v2), sizeof(int32_t));
                cols[4]->insert_data(reinterpret_cast<const char*>(&ds), sizeof(int8_t));
            },
            &probe_tablet);
    auto mow = make_mow_context(100, {rowset});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *source_schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND,
                          {"k1", "__DORIS_TEST_HIDDEN_KEY__", "v1"}, false, 0, 0, "UTC", "")
                        .ok());

    RowsetWriterContext rwc;
    rwc.tablet = probe_tablet;
    rwc.tablet_schema = changelog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_changelog_opt().enable = true;
    auto& cfg = rwc.write_changelog_opt().write_changelog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.base_tablet = probe_tablet;
    cfg.source.partial_update_info = pui;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.source.mow_context = mow;
    cfg.write_before = false;
    register_segment_lsns(rwc, 1);

    ASSERT_TRUE(testutil::resolve_test_changelog_layout(rwc).ok());
    auto chain = build_transform_chain(rwc);
    TransformExecContext ctx = exec_ctx(changelog_schema, &rwc);
    ctx.tablet = probe_tablet;
    ctx.mow_context = mow;
    ctx.partial_update_info = pui;

    // narrow PU providing both key columns and v1
    Block block = source_schema->create_storage_block({0, 1, 2});
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k1 = 1, v1 = 10;
        int64_t hidden_key = 1001;
        cols[0]->insert_data(reinterpret_cast<const char*>(&k1), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&hidden_key), sizeof(int64_t));
        cols[2]->insert_data(reinterpret_cast<const char*>(&v1), sizeof(int32_t));
    }

    ASSERT_TRUE(apply_no_correctness_check(chain, ctx, &block).ok());

    ASSERT_EQ(block.columns(), changelog_schema->num_columns());
    ASSERT_EQ(block.rows(), 1);
    // the probe encoded both key columns, so it found the history row: UPDATE,
    // and the missing v2 is the historical 777. Dropping the hidden key from the
    // probe key would miss and report APPEND with v2 defaulted to 0.
    EXPECT_EQ(read_op(block, changelog_schema->changelog_op_col_idx(), 0),
              static_cast<int64_t>(ChangelogOp::UPDATE));
    EXPECT_EQ(read_int(block, 2, 0), 10);
    EXPECT_EQ(read_int(block, 3, 0), 777);
    EXPECT_FALSE(read_is_null(block, 3, 0));
    // the hidden key is part of the row image too
    EXPECT_EQ(read_int(block, 0, 0), 1);
}

// A fixed partial update that also asks for the BEFORE image: one probe serves
// both, so the AFTER row is rebuilt from history while BEFORE mirrors the row
// that was there. This is the combination the op revision comment relies on --
// BEFORE is read first, then the op is revised from the old delete signs.
TEST_F(ChangelogDeriveTest, MowPartialUpdateWithBeforeImage) {
    auto source_schema = create_changelog_pu_source_schema(); // k1,v1,v2,delete_sign(hidden)
    auto changelog_schema = create_changelog_before_schema(); // + BEFORE v1/v2 + TSO/LSN/OP

    // history: key 1 -> (v1=100, v2=777); key 99 does not exist
    TabletSharedPtr probe_tablet;
    auto rowset = write_rowset_block(
            source_schema, 8322, 2,
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
    rwc.tablet_schema = changelog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_changelog_opt().enable = true;
    auto& cfg = rwc.write_changelog_opt().write_changelog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.base_tablet = probe_tablet;
    cfg.source.partial_update_info = pui;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.source.mow_context = mow;
    cfg.write_before = true;
    register_segment_lsns(rwc, 2);

    ASSERT_TRUE(testutil::resolve_test_changelog_layout(rwc).ok());
    auto chain = build_transform_chain(rwc);
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"MowChangelogDerive"}));

    TransformExecContext ctx = exec_ctx(changelog_schema, &rwc);
    ctx.tablet = probe_tablet;
    ctx.mow_context = mow;
    ctx.partial_update_info = pui;

    Block block = source_schema->create_storage_block({0, 1}); // narrow PU: k1, v1
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

    ASSERT_EQ(block.columns(), changelog_schema->num_columns());
    ASSERT_EQ(block.rows(), 2);
    const int op_idx = changelog_schema->changelog_op_col_idx();
    EXPECT_EQ(read_op(block, op_idx, 0),
              static_cast<int64_t>(ChangelogOp::UPDATE)); // key 1 existed
    EXPECT_EQ(read_op(block, op_idx, 1),
              static_cast<int64_t>(ChangelogOp::APPEND)); // key 99 is new
    // AFTER: provided v1 kept, missing v2 rebuilt from history for the old key
    EXPECT_EQ(read_int(block, 1, 0), 10);
    EXPECT_EQ(read_int(block, 2, 0), 777);
    EXPECT_EQ(read_int(block, 1, 1), 20);
    EXPECT_EQ(read_int(block, 2, 1), 0); // brand-new key -> column default
    // BEFORE mirrors the row that was there, and is NULL for the new key
    EXPECT_FALSE(read_is_null(block, 3, 0));
    EXPECT_EQ(read_int(block, 3, 0), 100);
    EXPECT_FALSE(read_is_null(block, 4, 0));
    EXPECT_EQ(read_int(block, 4, 0), 777);
    EXPECT_TRUE(read_is_null(block, 3, 1));
    EXPECT_TRUE(read_is_null(block, 4, 1));
}

// An insert whose old row is a tombstone is an APPEND, not an UPDATE: the probe
// still finds the deleted row, so the op has to be revised from the old delete
// sign. Without that revision the changelog would tell a consumer to update a row
// it has already deleted.
TEST_F(ChangelogDeriveTest, MowInsertAfterDeletedHistoryRowIsAppend) {
    auto changelog_tablet =
            create_changelog_tablets(/*tablet_id=*/8161, TKeysType::DUP_KEYS, /*mow=*/false)
                    .changelog;
    ASSERT_TRUE(changelog_tablet != nullptr);
    auto changelog_schema = changelog_tablet->tablet_schema();
    auto source_schema = create_changelog_pu_source_schema();

    // history: key 1 is a tombstone (delete sign 1), key 2 is a live row
    TabletSharedPtr probe_tablet;
    auto rowset = write_rowset_block(
            source_schema, 8162, 2,
            [](Block& b) {
                auto guard = b.mutate_columns_scoped();
                auto& cols = guard.mutable_columns();
                int32_t ks[] = {1, 2};
                int32_t v1s[] = {100, 200};
                int32_t v2s[] = {777, 888};
                int8_t dss[] = {1, 0}; // key 1 was deleted, key 2 was not
                for (int i = 0; i < 2; ++i) {
                    cols[0]->insert_data(reinterpret_cast<const char*>(&ks[i]), sizeof(int32_t));
                    cols[1]->insert_data(reinterpret_cast<const char*>(&v1s[i]), sizeof(int32_t));
                    cols[2]->insert_data(reinterpret_cast<const char*>(&v2s[i]), sizeof(int32_t));
                    cols[3]->insert_data(reinterpret_cast<const char*>(&dss[i]), sizeof(int8_t));
                }
            },
            &probe_tablet);
    auto mow = make_mow_context(100, {rowset});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *source_schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k1", "v1"}, false, 0, 0, "UTC", "")
                        .ok());

    RowsetWriterContext rwc;
    rwc.tablet = probe_tablet;
    rwc.tablet_schema = changelog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_changelog_opt().enable = true;
    auto& cfg = rwc.write_changelog_opt().write_changelog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.base_tablet = probe_tablet;
    cfg.source.partial_update_info = pui;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.source.mow_context = mow;
    cfg.write_before = false;
    register_segment_lsns(rwc, 2);

    ASSERT_TRUE(testutil::resolve_test_changelog_layout(rwc).ok());
    auto chain = build_transform_chain(rwc);
    TransformExecContext ctx = exec_ctx(changelog_schema, &rwc);
    ctx.tablet = probe_tablet;
    ctx.mow_context = mow;
    ctx.partial_update_info = pui;

    // both keys are re-inserted by this load
    Block block = source_schema->create_storage_block({0, 1});
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t ks[] = {1, 2};
        int32_t v1s[] = {10, 20};
        for (int i = 0; i < 2; ++i) {
            cols[0]->insert_data(reinterpret_cast<const char*>(&ks[i]), sizeof(int32_t));
            cols[1]->insert_data(reinterpret_cast<const char*>(&v1s[i]), sizeof(int32_t));
        }
    }

    ASSERT_TRUE(apply_no_correctness_check(chain, ctx, &block).ok());

    ASSERT_EQ(block.rows(), 2);
    const int op_idx = changelog_schema->changelog_op_col_idx();
    // key 1: the probe found the tombstone, so the op is revised UPDATE -> APPEND.
    // Dropping revise_operators_by_old_delete_sign leaves it UPDATE.
    EXPECT_EQ(read_op(block, op_idx, 0), static_cast<int64_t>(ChangelogOp::APPEND));
    // key 2: a live old row, so it stays an UPDATE -- the revision is not blanket
    EXPECT_EQ(read_op(block, op_idx, 1), static_cast<int64_t>(ChangelogOp::UPDATE));
    // key 1's old row is a tombstone, so its missing v2 takes the column default
    // rather than the deleted row's 777; key 2's live old row still supplies 888
    EXPECT_EQ(read_int(block, 2, 0), 0);
    EXPECT_EQ(read_int(block, 2, 1), 888);
}

// Changelog can be written into its own tablet: then ctx.tablet is that changelog
// tablet and the history lives in source.base_tablet. The lookup has to follow
// base_tablet -- against the changelog tablet every key would look brand new.
TEST_F(ChangelogDeriveTest, MowLooksUpHistoryInBaseTabletNotChangelogTablet) {
    auto changelog_tablet =
            create_changelog_tablets(/*tablet_id=*/8151, TKeysType::DUP_KEYS, /*mow=*/false)
                    .changelog;
    ASSERT_TRUE(changelog_tablet != nullptr);
    auto changelog_schema = changelog_tablet->tablet_schema();
    auto source_schema = create_changelog_pu_source_schema();

    // history lives in the source tablet only: key 1 -> (v1=100, v2=777)
    TabletSharedPtr source_tablet;
    auto rowset = write_rowset_block(
            source_schema, 8152, 2,
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
            &source_tablet);
    auto mow = make_mow_context(100, {rowset});
    // Mask that history row in the changelog tablet's own delete bitmap: the rowsets
    // to search come from the MowContext either way, so this is what makes the
    // two tablets answer differently. Looking the key up against the changelog
    // tablet now misses; against the source tablet it still hits.
    changelog_tablet->tablet_meta()->delete_bitmap().add({rowset->rowset_id(), 0, 2}, 0);

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *source_schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k1", "v1"}, false, 0, 0, "UTC", "")
                        .ok());

    RowsetWriterContext rwc;
    rwc.tablet = changelog_tablet;
    rwc.tablet_schema = changelog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_changelog_opt().enable = true;
    auto& cfg = rwc.write_changelog_opt().write_changelog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.base_tablet = source_tablet; // the tablet that holds the history
    cfg.source.partial_update_info = pui;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.source.mow_context = mow;
    cfg.write_before = false;
    register_segment_lsns(rwc, 1);

    ASSERT_TRUE(testutil::resolve_test_changelog_layout(rwc).ok());
    auto chain = build_transform_chain(rwc);
    TransformExecContext ctx = exec_ctx(changelog_schema, &rwc);
    ctx.tablet = changelog_tablet; // NOT the tablet holding the history
    ctx.mow_context = mow;
    ctx.partial_update_info = pui;

    Block block = source_schema->create_storage_block({0, 1}); // narrow PU: k1, v1
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 1, v1 = 10;
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&v1), sizeof(int32_t));
    }

    ASSERT_TRUE(apply_no_correctness_check(chain, ctx, &block).ok());

    ASSERT_EQ(block.rows(), 1);
    // the key was found in base_tablet: UPDATE, and the missing v2 is the
    // historical 777. The same lookup against the changelog tablet hits its delete
    // bitmap instead and would report APPEND with v2 defaulted to 0.
    EXPECT_EQ(read_op(block, changelog_schema->changelog_op_col_idx(), 0),
              static_cast<int64_t>(ChangelogOp::UPDATE));
    EXPECT_EQ(read_int(block, 0, 0), 1);
    EXPECT_EQ(read_int(block, 1, 0), 10);
    EXPECT_EQ(read_int(block, 2, 0), 777);
    EXPECT_FALSE(read_is_null(block, 2, 0));
}

// B14/B15: a row carrying a delete sign is a DELETE whether the key exists
// (old key) or not (new key); the delete sign rides in the update columns.
TEST_F(ChangelogDeriveTest, MowDeleteExistingAndNewKey) {
    auto changelog_tablet =
            create_changelog_tablets(/*tablet_id=*/8201, TKeysType::DUP_KEYS, /*mow=*/false)
                    .changelog;
    ASSERT_TRUE(changelog_tablet != nullptr);
    auto changelog_schema = changelog_tablet->tablet_schema();
    auto source_schema = create_changelog_pu_source_schema(); // k1,v1,v2,delete_sign(3)

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
    rwc.tablet_schema = changelog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_changelog_opt().enable = true;
    auto& cfg = rwc.write_changelog_opt().write_changelog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.base_tablet = probe_tablet;
    cfg.source.partial_update_info = pui;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.source.mow_context = mow;
    cfg.write_before = false;
    register_segment_lsns(rwc, 2);

    ASSERT_TRUE(testutil::resolve_test_changelog_layout(rwc).ok());
    auto chain = build_transform_chain(rwc);
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"MowChangelogDerive"}));

    TransformExecContext ctx = exec_ctx(changelog_schema, &rwc);
    ctx.tablet = probe_tablet;
    ctx.mow_context = mow;
    ctx.partial_update_info = pui;

    // narrow PU block in update_cids order {k1, v1, delete_sign}
    Block block = source_schema->create_storage_block({0, 1, 3});
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

    const int lsn_idx = changelog_schema->changelog_lsn_col_idx();
    const int op_idx = changelog_schema->changelog_op_col_idx();
    EXPECT_EQ(read_op(block, op_idx, 0),
              static_cast<int64_t>(ChangelogOp::DELETE)); // existing key + delete sign
    EXPECT_EQ(read_op(block, op_idx, 1),
              static_cast<int64_t>(ChangelogOp::DELETE)); // new key + delete sign
    EXPECT_EQ(read_lsn(block, lsn_idx, 0), 1000);
    EXPECT_EQ(read_lsn(block, lsn_idx, 1), 1001);
}

// ===========================================================================
// §5.3.3 / §5.3.4 BEFORE image (write_before=true). Previously 0 coverage.
// ===========================================================================

// B19/B21: UPDATE & DELETE rows mirror the historical value columns into the
// BEFORE columns; the APPEND row (no history) has BEFORE == NULL. BEFORE
// covers value columns only (not the key, not delete_sign).
TEST_F(ChangelogDeriveTest, MowBeforeImageMirrorsHistory) {
    auto source_schema = create_changelog_pu_source_schema(); // k1,v1,v2,delete_sign(3 hidden)
    auto changelog_schema = create_changelog_before_schema(); // + BEFORE v1/v2 + TSO/LSN/OP
    ASSERT_EQ(changelog_schema->changelog_lsn_col_idx(), 6);
    ASSERT_EQ(changelog_schema->num_columns(), 8U);

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
    rwc.tablet_schema = changelog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_changelog_opt().enable = true;
    auto& cfg = rwc.write_changelog_opt().write_changelog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.base_tablet = probe_tablet;
    // write_before=true alone (no PU) routes to the MoW derive; the source block
    // is a full-width upsert, no partial_update_info.
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.source.mow_context = mow;
    cfg.write_before = true;
    register_segment_lsns(rwc, 3);

    // setup_retriever_and_lookup requires partial_update_info on the retriever
    // context (build_after_block / retrieve_historical_row DCHECK it). For a
    // full-width upsert (no PU), supply an empty fixed-PU info covering all
    // columns so it is NOT treated as a partial update by the derive.
    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *source_schema, UniqueKeyUpdateModePB::UPSERT,
                          PartialUpdateNewRowPolicyPB::APPEND, {}, false, 0, 0, "UTC", "")
                        .ok());
    cfg.source.partial_update_info = pui;

    ASSERT_TRUE(testutil::resolve_test_changelog_layout(rwc).ok());
    auto chain = build_transform_chain(rwc); // write_before -> MoW derive
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"MowChangelogDerive"}));

    TransformExecContext ctx = exec_ctx(changelog_schema, &rwc);
    ctx.tablet = probe_tablet;
    ctx.mow_context = mow;
    ctx.partial_update_info = pui;

    // full-width upsert source block (k1,v1,v2,delete_sign):
    //   r0 key1 (11,1) delete 0 -> UPDATE; r1 key2 (0,0) delete 1 -> DELETE;
    //   r2 key98 (50,5) delete 0 -> APPEND.
    Block block = source_schema->create_storage_block();
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

    ASSERT_EQ(block.columns(), changelog_schema->num_columns());
    ASSERT_EQ(block.rows(), 3);
    const int lsn_idx = changelog_schema->changelog_lsn_col_idx();
    const int op_idx = changelog_schema->changelog_op_col_idx();
    const int before_v1 = 3;
    const int before_v2 = 4;

    // op
    EXPECT_EQ(read_op(block, op_idx, 0), static_cast<int64_t>(ChangelogOp::UPDATE));
    EXPECT_EQ(read_op(block, op_idx, 1), static_cast<int64_t>(ChangelogOp::DELETE));
    EXPECT_EQ(read_op(block, op_idx, 2), static_cast<int64_t>(ChangelogOp::APPEND));

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
TEST_F(ChangelogDeriveTest, MowBeforeNoopZeroValueColumns) {
    auto source_schema = create_changelog_keyonly_source_schema();    // k1, delete_sign(hidden)
    auto changelog_schema = create_changelog_keyonly_before_schema(); // [k1, TSO, LSN, OP]
    ASSERT_EQ(source_schema->num_visible_value_columns(), 0U);
    ASSERT_EQ(changelog_schema->changelog_lsn_col_idx(), 2);
    ASSERT_EQ(changelog_schema->num_columns(), 4U);

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
    rwc.tablet_schema = changelog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_changelog_opt().enable = true;
    auto& cfg = rwc.write_changelog_opt().write_changelog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.base_tablet = probe_tablet;
    cfg.source.partial_update_info = pui;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.source.mow_context = mow;
    cfg.write_before = true;
    register_segment_lsns(rwc, 2);

    ASSERT_TRUE(testutil::resolve_test_changelog_layout(rwc).ok());
    auto chain = build_transform_chain(rwc);
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"MowChangelogDerive"}));

    TransformExecContext ctx = exec_ctx(changelog_schema, &rwc);
    ctx.tablet = probe_tablet;
    ctx.mow_context = mow;
    ctx.partial_update_info = pui;

    Block block = source_schema->create_storage_block(); // full width: k1, delete_sign
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

    ASSERT_EQ(block.columns(), changelog_schema->num_columns()); // [k1, TSO, LSN, OP]
    ASSERT_EQ(block.rows(), 2);
    EXPECT_EQ(read_int(block, 0, 0), 1);
    EXPECT_EQ(read_int(block, 0, 1), 98);
    const int op_idx = changelog_schema->changelog_op_col_idx();
    EXPECT_EQ(read_op(block, op_idx, 0),
              static_cast<int64_t>(ChangelogOp::UPDATE)); // key 1 existed
    EXPECT_EQ(read_op(block, op_idx, 1), static_cast<int64_t>(ChangelogOp::APPEND)); // key 98 new
}

// Changelog MoW over a SEQUENCE source. The changelog retriever probes with
// skip_seq_loses=false (historical_row_retriever.cpp), so a row whose seq loses
// to history (FOUND_NEWER) is NOT default-filled -- it still reads the old row
// and is op=UPDATE, keeping history v1. This differs from the fixed-PU fill
// stage (skip_seq_loses=true), which would default-fill the seq loser. The
// blueprint's "seq loses -> DELETE via use_default_or_null" (§5.5) does not hold
// for this policy; the FOUND+use_default DELETE arm is covered by
// MowDeleteExistingAndNewKey instead.
TEST_F(ChangelogDeriveTest, MowSeqSourceSeqLosesStillReadsHistory) {
    auto source_schema = create_changelog_seq_source_schema(); // k1,v1,seq/delete-sign hidden
    auto changelog_schema = create_changelog_seq_schema();     // [k1,v1,TSO,LSN,OP]
    ASSERT_EQ(changelog_schema->changelog_lsn_col_idx(), 3);
    ASSERT_EQ(changelog_schema->num_columns(), 5U);

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
    rwc.tablet_schema = changelog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_changelog_opt().enable = true;
    auto& cfg = rwc.write_changelog_opt().write_changelog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.base_tablet = probe_tablet;
    cfg.source.partial_update_info = pui;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.source.mow_context = mow;
    cfg.write_before = false;
    register_segment_lsns(rwc, 2);

    ASSERT_TRUE(testutil::resolve_test_changelog_layout(rwc).ok());
    auto chain = build_transform_chain(rwc);
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"MowChangelogDerive"}));

    TransformExecContext ctx = exec_ctx(changelog_schema, &rwc);
    ctx.tablet = probe_tablet;
    ctx.mow_context = mow;
    ctx.partial_update_info = pui;

    // narrow PU block {k1, seq}: r0 seq=3 loses to history 10; r1 seq=20 wins.
    Block block = source_schema->create_storage_block({0, 2});
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

    ASSERT_EQ(block.columns(), changelog_schema->num_columns());
    ASSERT_EQ(block.rows(), 2);
    const int lsn_idx = changelog_schema->changelog_lsn_col_idx();
    const int op_idx = changelog_schema->changelog_op_col_idx();
    // both rows read the old row (use_default_or_null=false): the seq loser is NOT
    // default-filled, so op=UPDATE and v1 is the historical value.
    EXPECT_EQ(read_op(block, op_idx, 0),
              static_cast<int64_t>(ChangelogOp::UPDATE)); // seq loses, still UPDATE
    EXPECT_EQ(read_op(block, op_idx, 1), static_cast<int64_t>(ChangelogOp::UPDATE)); // seq wins
    EXPECT_EQ(read_int(block, 0, 0), 1);
    EXPECT_EQ(read_int(block, 1, 0), 100); // <-- history v1, NOT default 0 (the key point)
    EXPECT_EQ(read_int(block, 0, 1), 2);
    EXPECT_EQ(read_int(block, 1, 1), 200);
    EXPECT_EQ(read_lsn(block, lsn_idx, 0), 1000);
    EXPECT_EQ(read_lsn(block, lsn_idx, 1), 1001);
}

// ===========================================================================
// §5.4 Flexible support and reject / negative paths.
// ===========================================================================

// A flexible partial update reaches the same MoW derive stage as a fixed
// partial update, but materializes its full AFTER row from the skip bitmap.
TEST_F(ChangelogDeriveTest, MowMaterializesFlexiblePartialUpdate) {
    TabletSchemaPB source_pb;
    create_flexible_mow_schema()->to_schema_pb(&source_pb);
    source_pb.mutable_column(source_pb.delete_sign_idx())->set_visible(false);
    source_pb.mutable_column(source_pb.skip_bitmap_col_idx())->set_visible(false);
    auto source_schema = std::make_shared<TabletSchema>();
    source_schema->init_from_pb(source_pb);

    TabletSchemaPB changelog_pb;
    create_changelog_seq_schema()->to_schema_pb(&changelog_pb);
    changelog_pb.mutable_column(0)->set_name("k");
    changelog_pb.mutable_column(1)->set_name("v");
    auto changelog_schema = std::make_shared<TabletSchema>();
    changelog_schema->init_from_pb(changelog_pb);
    auto probe_tablet = make_tablet(source_schema, /*tablet_id=*/8502);
    auto mow = make_mow_context(100, {});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *source_schema,
                          UniqueKeyUpdateModePB::UPDATE_FLEXIBLE_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {}, false, 0, 0, "UTC", "")
                        .ok());

    RowsetWriterContext rwc;
    rwc.tablet = probe_tablet;
    rwc.tablet_schema = changelog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_changelog_opt().enable = true;
    auto& cfg = rwc.write_changelog_opt().write_changelog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.base_tablet = probe_tablet;
    cfg.source.partial_update_info = pui;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.source.mow_context = mow;
    cfg.write_before = false;
    register_segment_lsns(rwc, 1);

    ASSERT_TRUE(testutil::resolve_test_changelog_layout(rwc).ok());
    auto chain = build_transform_chain(rwc);
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"MowChangelogDerive"}));

    TransformExecContext ctx = exec_ctx(changelog_schema, &rwc);
    ctx.tablet = probe_tablet;
    ctx.mow_context = mow;
    ctx.partial_update_info = pui;

    Block block = source_schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 1, v1 = 10;
        int8_t delete_sign = 0;
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&v1), sizeof(int32_t));
        cols[2]->insert_data(reinterpret_cast<const char*>(&delete_sign), sizeof(int8_t));
        assert_cast<ColumnBitmap*>(cols[3].get())->insert_value(BitmapValue {});
    }

    ASSERT_TRUE(chain.apply(ctx, &block).ok());
    ASSERT_EQ(block.columns(), changelog_schema->num_columns());
    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(read_int(block, 0, 0), 1);
    EXPECT_EQ(read_int(block, 1, 0), 10);
    EXPECT_EQ(read_lsn(block, changelog_schema->changelog_lsn_col_idx(), 0), 1000);
    EXPECT_EQ(read_op(block, changelog_schema->changelog_op_col_idx(), 0),
              static_cast<int64_t>(ChangelogOp::APPEND));
}

// N5/N6 (B10): the fixed-PU MoW derive rejects a block that is too narrow
// (< num_key_columns) or too wide (>= num_columns).
TEST_F(ChangelogDeriveTest, MowFixedPartialUpdateRejectsBadWidth) {
    auto changelog_tablet =
            create_changelog_tablets(/*tablet_id=*/8601, TKeysType::DUP_KEYS, /*mow=*/false)
                    .changelog;
    ASSERT_TRUE(changelog_tablet != nullptr);
    auto changelog_schema = changelog_tablet->tablet_schema();
    auto source_schema =
            create_changelog_pu_source_schema(); // 1 key + 2 visible + hidden ds = 4 cols
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
    rwc.tablet_schema = changelog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_changelog_opt().enable = true;
    auto& cfg = rwc.write_changelog_opt().write_changelog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.base_tablet = probe_tablet;
    cfg.source.partial_update_info = pui;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.source.mow_context = mow;
    cfg.write_before = false;

    ASSERT_TRUE(testutil::resolve_test_changelog_layout(rwc).ok());
    auto chain = build_transform_chain(rwc);
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"MowChangelogDerive"}));

    auto make_ctx = [&] {
        TransformExecContext ctx = exec_ctx(changelog_schema, &rwc);
        ctx.tablet = probe_tablet;
        ctx.mow_context = mow;
        ctx.partial_update_info = pui;
        return ctx;
    };

    // too narrow: no columns (0 < num_key_columns 1)
    {
        register_segment_lsns(rwc, 1);
        TransformExecContext ctx = make_ctx();
        Block block;
        auto st = chain.apply(ctx, &block);
        EXPECT_FALSE(st.ok());
        EXPECT_NE(st.to_string().find("illegal partial update block columns"), std::string::npos)
                << st;
    }
    // too wide: full width including hidden delete_sign (4 >= num_columns 4)
    {
        register_segment_lsns(rwc, 1);
        TransformExecContext ctx = make_ctx();
        Block block = source_schema->create_storage_block(); // 4 columns
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

// N2 (B3): a changelog write with no source tablet schema fails with a clear error.
TEST_F(ChangelogDeriveTest, RejectsMissingSourceSchema) {
    auto tablet = create_changelog_tablets(/*tablet_id=*/8701, TKeysType::DUP_KEYS, /*mow=*/false)
                          .changelog;
    ASSERT_TRUE(tablet != nullptr);
    auto changelog_schema = tablet->tablet_schema();

    RowsetWriterContext rwc;
    rwc.tablet = tablet;
    rwc.tablet_schema = changelog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_changelog_opt().enable = true;
    auto& cfg = rwc.write_changelog_opt().write_changelog_config();
    cfg.source.tablet_schema = nullptr; // missing source schema
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.write_before = false;
    register_segment_lsns(rwc, 1);

    auto chain = build_transform_chain(rwc); // no PU, no BEFORE -> plain derive
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"PlainChangelogDerive"}));

    TransformExecContext ctx = exec_ctx(changelog_schema, &rwc);
    ctx.tablet = tablet;

    Block block = create_changelog_pu_source_schema()->create_storage_block();
    block.get_by_position(0).column->assert_mutable()->insert_default();
    block.get_by_position(1).column->assert_mutable()->insert_default();
    block.get_by_position(2).column->assert_mutable()->insert_default();
    block.get_by_position(3).column->assert_mutable()->insert_default();
    auto st = chain.apply(ctx, &block);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("missing source_tablet_schema"), std::string::npos) << st;
}

// N3 (B4): a changelog block flushed without a segment id (segment_id == -1, the
// add_block seam) is rejected before any LSN is consumed.
TEST_F(ChangelogDeriveTest, RejectsNegativeSegmentId) {
    auto tablets = create_changelog_tablets(/*tablet_id=*/8801, TKeysType::DUP_KEYS, /*mow=*/false);
    auto tablet = tablets.changelog;
    ASSERT_TRUE(tablet != nullptr);
    auto source_schema = tablets.source->tablet_schema();
    auto changelog_schema = tablet->tablet_schema();

    RowsetWriterContext rwc;
    rwc.tablet = tablet;
    rwc.tablet_schema = changelog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_changelog_opt().enable = true;
    auto& cfg = rwc.write_changelog_opt().write_changelog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.write_before = false;
    register_segment_lsns(rwc, 1);

    ASSERT_TRUE(testutil::resolve_test_changelog_layout(rwc).ok());
    auto chain = build_transform_chain(rwc);
    EXPECT_EQ(chain.stage_names(), (std::vector<std::string_view> {"PlainChangelogDerive"}));

    TransformExecContext ctx = exec_ctx(changelog_schema, &rwc);
    ctx.tablet = tablet;
    ctx.segment_id = -1; // add_block seam: no segment id

    Block block = source_schema->create_storage_block();
    block.get_by_position(0).column->assert_mutable()->insert_default();
    block.get_by_position(1).column->assert_mutable()->insert_default();
    block.get_by_position(2).column->assert_mutable()->insert_default();
    auto st = chain.apply(ctx, &block);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("flush_single_block"), std::string::npos) << st;
}

// The schema and write_before have to agree about the BEFORE columns. Both
// directions are rejected: a schema without BEFORE columns while write_before is
// on would fill over the TSO/LSN columns, and one with them while write_before is
// off would leave those columns empty in an otherwise full block.
TEST_F(ChangelogDeriveTest, RejectsSchemaAndWriteBeforeDisagreement) {
    auto tablets = create_changelog_tablets(/*tablet_id=*/8901, TKeysType::DUP_KEYS, /*mow=*/false);
    auto changelog_schema = tablets.changelog->tablet_schema(); // no BEFORE columns
    auto source_schema = create_changelog_pu_source_schema();

    RowsetWriterContext rwc;
    rwc.tablet = tablets.changelog;
    rwc.tablet_schema = changelog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_changelog_opt().enable = true;
    auto& cfg = rwc.write_changelog_opt().write_changelog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.base_tablet = tablets.source;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.source.mow_context = make_mow_context(100, {});
    cfg.write_before = true; // the schema has no BEFORE columns
    register_segment_lsns(rwc, 1);

    EXPECT_FALSE(testutil::resolve_test_changelog_layout(rwc).ok());
}

// The TSO/LSN/OP positions come from the column names. A changelog schema whose
// hidden columns carry other names -- a changelog tablet created by a build that
// named them differently -- fails the load with an error instead of aborting the BE.
TEST_F(ChangelogDeriveTest, RejectsSchemaWithoutHiddenColumns) {
    auto tablets = create_changelog_tablets(/*tablet_id=*/8951, TKeysType::DUP_KEYS, /*mow=*/false);
    TabletSchemaPB pb;
    tablets.changelog->tablet_schema()->to_schema_pb(&pb);
    for (auto& column : *pb.mutable_column()) {
        if (column.name() == CHANGELOG_TSO_COL || column.name() == CHANGELOG_LSN_COL ||
            column.name() == CHANGELOG_OP_COL) {
            column.set_name("__DORIS_RENAMED_" + column.name());
        }
    }
    auto changelog_schema = std::make_shared<TabletSchema>();
    changelog_schema->init_from_pb(pb);
    ASSERT_LT(changelog_schema->changelog_tso_col_idx(), 0);
    ASSERT_LT(changelog_schema->changelog_lsn_col_idx(), 0);
    ASSERT_LT(changelog_schema->changelog_op_col_idx(), 0);
    auto source_schema = create_changelog_pu_source_schema();

    RowsetWriterContext rwc;
    rwc.tablet = tablets.changelog;
    rwc.tablet_schema = changelog_schema;
    rwc.write_type = DataWriteType::TYPE_DIRECT;
    rwc.write_changelog_opt().enable = true;
    auto& cfg = rwc.write_changelog_opt().write_changelog_config();
    cfg.source.tablet_schema = source_schema;
    cfg.source.source_write_type = DataWriteType::TYPE_DIRECT;
    cfg.source.is_transient_rowset_writer = false;
    cfg.write_before = false;
    register_segment_lsns(rwc, 1);

    auto st = testutil::resolve_test_changelog_layout(rwc);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("misses its hidden columns"), std::string::npos) << st;
}

// The wire mapping is independent of both names and target physical order.
TEST_F(ChangelogDeriveTest, ResolveExplicitInterleavedMapping) {
    auto source = create_changelog_pu_source_schema();
    auto target = create_changelog_before_schema();
    PChangelogWriteColumnMappings mapping;
    mapping.set_need_historical_value(true);
    for (int cid = 0; cid < 3; ++cid) {
        auto* entry = mapping.add_entries();
        entry->set_source_column_unique_id(source->column(cid).unique_id());
        entry->set_current_column_unique_id(target->column(cid).unique_id());
        if (cid > 0) {
            entry->set_before_column_unique_id(target->column(cid + 2).unique_id());
        }
    }
    TabletSchemaPB target_pb;
    target->to_schema_pb(&target_pb);
    target_pb.mutable_column()->SwapElements(1, 4);
    auto reordered = std::make_shared<TabletSchema>();
    reordered->init_from_pb(target_pb);
    std::shared_ptr<const changelog::ResolvedChangelogLayout> layout;
    ASSERT_TRUE(changelog::resolve_layout(source, reordered, mapping, &layout).ok());
    EXPECT_EQ(layout->current_target_cids, (std::vector<uint32_t> {0, 4, 2}));
    EXPECT_EQ(layout->before_target_cids, (std::vector<uint32_t> {3, 1}));
    EXPECT_TRUE(layout->need_historical_value);
    auto invalid = mapping;
    invalid.mutable_entries(1)->set_current_column_unique_id(99999);
    EXPECT_FALSE(changelog::resolve_layout(source, reordered, invalid, &layout).ok());
    invalid = mapping;
    invalid.mutable_entries(1)->set_current_column_unique_id(
            invalid.entries(0).current_column_unique_id());
    EXPECT_FALSE(changelog::resolve_layout(source, reordered, invalid, &layout).ok());
    invalid = mapping;
    invalid.mutable_entries(0)->clear_source_column_unique_id();
    EXPECT_FALSE(changelog::resolve_layout(source, reordered, invalid, &layout).ok());
    invalid = mapping;
    invalid.set_need_historical_value(false);
    EXPECT_FALSE(changelog::resolve_layout(source, reordered, invalid, &layout).ok());
    invalid = mapping;
    invalid.clear_need_historical_value();
    EXPECT_FALSE(changelog::resolve_layout(source, reordered, invalid, &layout).ok());
}

TEST_F(ChangelogDeriveTest, ResolveRejectsShapeAndNullableNarrowing) {
    auto source = create_changelog_pu_source_schema();
    auto target = create_changelog_before_schema();
    auto mapping = testutil::create_changelog_mapping(*source, *target, true);
    TabletSchemaPB pb;
    target->to_schema_pb(&pb);
    pb.mutable_column(1)->set_is_nullable(false);
    auto invalid_target = std::make_shared<TabletSchema>();
    invalid_target->init_from_pb(pb);
    std::shared_ptr<const changelog::ResolvedChangelogLayout> layout;
    EXPECT_FALSE(changelog::resolve_layout(source, invalid_target, mapping, &layout).ok());
    target->to_schema_pb(&pb);
    pb.mutable_column(1)->set_type("BIGINT");
    invalid_target->init_from_pb(pb);
    EXPECT_FALSE(changelog::resolve_layout(source, invalid_target, mapping, &layout).ok());
    source->to_schema_pb(&pb);
    pb.mutable_column(1)->set_is_nullable(false);
    auto nonnullable_source = std::make_shared<TabletSchema>();
    nonnullable_source->init_from_pb(pb);
    ASSERT_TRUE(changelog::resolve_layout(nonnullable_source, target, mapping, &layout).ok());
    EXPECT_TRUE(layout->current_nullable_widening[1]);
    EXPECT_FALSE(layout->current_nullable_widening[0]);
    target->to_schema_pb(&pb);
    pb.mutable_column(0)->set_is_nullable(true);
    auto nullable_key_target = std::make_shared<TabletSchema>();
    nullable_key_target->init_from_pb(pb);
    EXPECT_FALSE(changelog::resolve_layout(source, nullable_key_target, mapping, &layout).ok());
}

TEST_F(ChangelogDeriveTest, PlainInterleavedTargetPreservesValues) {
    auto source = create_changelog_pu_source_schema();
    auto tablets = create_changelog_tablets(8991, TKeysType::DUP_KEYS, false);
    TabletSchemaPB pb;
    tablets.changelog->tablet_schema()->to_schema_pb(&pb);
    pb.mutable_column()->SwapElements(1, 4); // v1 crosses a system column
    auto target = std::make_shared<TabletSchema>();
    target->init_from_pb(pb);
    RowsetWriterContext context;
    context.tablet_schema = target;
    context.write_changelog_opt().enable = true;
    auto& cfg = context.write_changelog_opt().write_changelog_config();
    cfg.source.tablet_schema = source;
    ASSERT_TRUE(testutil::resolve_test_changelog_layout(context).ok());
    register_segment_lsns(context, 1);
    Block block = source->create_storage_block();
    block.get_by_position(0).column->assert_mutable()->insert(Field(int64_t(7)));
    block.get_by_position(1).column->assert_mutable()->insert(Field(int64_t(11)));
    block.get_by_position(2).column->assert_mutable()->insert(Field(int64_t(13)));
    block.get_by_position(3).column->assert_mutable()->insert_default();
    auto ctx = exec_ctx(target, &context);
    auto chain = build_transform_chain(context);
    ASSERT_TRUE(chain.apply(ctx, &block).ok());
    EXPECT_EQ(block.get_by_position(4).column->get_int(0), 11);
    EXPECT_EQ(block.get_by_position(2).column->get_int(0), 13);
    EXPECT_EQ(block.get_by_position(0).column->get_int(0), 7);
    EXPECT_EQ(read_op(block, target->changelog_op_col_idx(), 0),
              static_cast<int64_t>(ChangelogOp::APPEND));
}

} // namespace doris
