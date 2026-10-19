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

// Full-branch-coverage tests for the FLEXIBLE partial-update fill stage and the
// in-block BlockAggregator it drives. The fill path under test is
// FlexiblePartialUpdateFillStage::apply -> BlockAggregator (dedup by sequence,
// insert-after-delete) -> probe_and_plan -> MowKeyProbe::probe ->
// FlexibleReadPlan::fill_non_primary_key_columns.
//
// Test groups:
//   Flexible*     happy paths: per-cell fill from history/defaults, delete sign,
//                 insert-after-delete (new and existing key), seq dedup
//   Agg*          BlockAggregator::aggregate_rows branch map, one test per arm:
//                 FOUND baseline {stale-first-row skipped / first-row-with-seq
//                 starts / first-row-without-seq inherits}, NOT_FOUND baseline
//                 {own seq starts / default seq}, main loop {no-seq direct-fed /
//                 accepted / out-of-order discarded}, all-rows-stale empty
//                 output, delete-inside-group clears state, insert-after-delete
//                 seq inheritance
//   *Persists*    end-to-end through the vertical writer and a read-back
//
// Oracle values are ported from the deleted writer implementation
// (VerticalSegmentWriter::_append_block_with_flexible_partial_content), with
// weak assertions upgraded to exact delete-bitmap membership + cardinality.

#include <gtest/gtest.h>

#include <map>
#include <tuple>
#include <utility>
#include <vector>

#include "common/config.h"
#include "core/column/column_complex.h"
#include "core/value/bitmap_value.h"
#include "storage/mow/mow_transform_test_base.h"
#include "storage/partial_update_info.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/transform/block_transform.h"
#include "storage/transform/partial_update_fill.h"

namespace doris {

using segment_v2::FlexiblePartialUpdateFillStage;
using segment_v2::TransformExecContext;

class FlexiblePartialUpdateTest : public MowTransformTestBase {
protected:
    // The per-write RowsetWriterContext the stage reads through
    // ctx.rowset_ctx->make_historical_row_retriever_context().
    void fill_rowset_ctx(RowsetWriterContext* rwc, const TabletSchemaSPtr& schema,
                         const TabletSharedPtr& tablet, std::shared_ptr<PartialUpdateInfo> pui,
                         const RowsetId& new_rsid) {
        rwc->tablet_id = kTabletId;
        rwc->tablet = tablet;
        rwc->tablet_schema = schema;
        rwc->partial_update_info = std::move(pui);
        rwc->is_transient_rowset_writer = false;
        rwc->write_type = DataWriteType::TYPE_DIRECT;
        rwc->rowset_id = new_rsid;
    }

    TransformExecContext make_exec_ctx(const TabletSchemaSPtr& schema,
                                       const TabletSharedPtr& tablet,
                                       const std::shared_ptr<MowContext>& mow,
                                       const std::shared_ptr<PartialUpdateInfo>& pui,
                                       RowsetWriterContext* rwc, const RowsetId& new_rsid) {
        TransformExecContext ctx;
        ctx.tablet_schema = schema;
        ctx.write_type = DataWriteType::TYPE_DIRECT;
        ctx.tablet = tablet;
        ctx.mow_context = mow;
        ctx.partial_update_info = pui;
        ctx.rowset_ctx = rwc;
        ctx.rowset_id = new_rsid;
        ctx.segment_id = 0;
        return ctx;
    }

    std::shared_ptr<PartialUpdateInfo> make_flexible_pui(
            const TabletSchemaSPtr& schema,
            PartialUpdateNewRowPolicyPB policy = PartialUpdateNewRowPolicyPB::APPEND) {
        auto pui = std::make_shared<PartialUpdateInfo>();
        EXPECT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FLEXIBLE_COLUMNS,
                              policy, {}, false, 0, 0, "UTC", "")
                            .ok());
        return pui;
    }

    // (k INT key, v INT NOT NULL no default, delete-sign, skip-bitmap) flexible
    // schema: a new key whose insert skips `v` can neither default nor null it.
    TabletSchemaSPtr create_flexible_required_value_schema() {
        TabletSchemaPB pb;
        pb.set_keys_type(UNIQUE_KEYS);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_LZ4);
        pb.set_next_column_unique_id(10);

        auto add_col = [&](int uid, const std::string& name, const std::string& type, bool is_key,
                           bool nullable, const std::string& def = "") {
            ColumnPB* c = pb.add_column();
            c->set_unique_id(uid);
            c->set_name(name);
            c->set_type(type);
            c->set_is_key(is_key);
            int len = 4;
            if (type == "TINYINT") {
                len = 1;
            } else if (type == "BITMAP") {
                len = 16;
            }
            c->set_length(len);
            c->set_index_length(len);
            c->set_is_nullable(nullable);
            c->set_aggregation("NONE");
            if (!def.empty()) {
                c->set_default_value(def);
            }
        };
        add_col(0, "k", "INT", true, false);
        add_col(1, "v", "INT", false, /*nullable=*/false); // NOT NULL, no default
        add_col(2, DELETE_SIGN, "TINYINT", false, false, std::to_string(0));
        add_col(3, SKIP_BITMAP_COL, "BITMAP", false, false);
        pb.set_delete_sign_idx(2);
        pb.set_skip_bitmap_col_idx(3);

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    // Runs FlexiblePartialUpdateFillStage with the MoW correctness-check sentinel
    // disabled (so the delete bitmap holds only the real marks and exact
    // cardinality / contains assertions are meaningful).
    Status run_flexible_fill(TransformExecContext& ctx, Block* block) {
        auto saved = config::enable_merge_on_write_correctness_check;
        config::enable_merge_on_write_correctness_check = false;
        FlexiblePartialUpdateFillStage stage;
        auto st = stage.apply(ctx, block);
        config::enable_merge_on_write_correctness_check = saved;
        return st;
    }

    // The two value columns / sequence / delete-sign / skip-bitmap flexible-seq
    // Two-value-column flexible-seq schema: k(0) v1(1) v2(2) seq(3)
    // delete_sign(4) skip_bitmap(5). create_flexible_mow_schema(true) (one value
    // col) cannot express "insert provides v1 but skips v2", which the
    // no-resurrect tests need; this local builder adds the second value column.
    TabletSchemaSPtr create_flexible_seq2_schema() {
        TabletSchemaPB pb;
        pb.set_keys_type(UNIQUE_KEYS);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_LZ4);
        pb.set_next_column_unique_id(10);

        auto add_col = [&](int uid, const std::string& name, const std::string& type, bool is_key,
                           bool nullable, const std::string& def = "") {
            ColumnPB* c = pb.add_column();
            c->set_unique_id(uid);
            c->set_name(name);
            c->set_type(type);
            c->set_is_key(is_key);
            int len = 4;
            if (type == "TINYINT") {
                len = 1;
            } else if (type == "BITMAP") {
                len = 16;
            }
            c->set_length(len);
            c->set_index_length(len);
            c->set_is_nullable(nullable);
            c->set_aggregation("NONE");
            if (!def.empty()) {
                c->set_default_value(def);
            }
        };
        add_col(0, "k", "INT", true, false);
        add_col(1, "v1", "INT", false, true, std::to_string(0));
        add_col(2, "v2", "INT", false, true, std::to_string(0));
        add_col(3, SEQUENCE_COL, "INT", false, false, std::to_string(0));
        add_col(4, DELETE_SIGN, "TINYINT", false, false, std::to_string(0));
        add_col(5, SKIP_BITMAP_COL, "BITMAP", false, false);
        pb.set_sequence_col_idx(3);
        pb.set_delete_sign_idx(4);
        pb.set_skip_bitmap_col_idx(5);

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    // Writes a single history row of the create_flexible_seq2_schema layout.
    RowsetSharedPtr write_seq2_history(const TabletSchemaSPtr& schema, int64_t rsid_num, int32_t k,
                                       int32_t v1, int32_t v2, int32_t seq,
                                       TabletSharedPtr* out_tablet) {
        return write_rowset_block(
                schema, rsid_num, /*version=*/2,
                [&](Block& b) {
                    auto guard = b.mutate_columns_scoped();
                    auto& cols = guard.mutable_columns();
                    int8_t ds0 = 0;
                    cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
                    cols[1]->insert_data(reinterpret_cast<const char*>(&v1), sizeof(int32_t));
                    cols[2]->insert_data(reinterpret_cast<const char*>(&v2), sizeof(int32_t));
                    cols[3]->insert_data(reinterpret_cast<const char*>(&seq), sizeof(int32_t));
                    cols[4]->insert_data(reinterpret_cast<const char*>(&ds0), sizeof(int8_t));
                    cols[5]->insert_default(); // empty skip bitmap
                },
                out_tablet);
    }

    // One flexible-seq2 input row builder. Each give_* says the row provides that
    // column; every column with give_*==false is put into the skip bitmap.
    struct Seq2Row {
        int32_t k;
        int32_t v1;
        int32_t v2;
        int32_t seq;
        int8_t delete_sign;
        bool give_v1;
        bool give_v2;
        bool give_seq;
        bool give_ds;
    };

    void append_seq2_row(const TabletSchemaSPtr& schema, MutableColumns& cols, const Seq2Row& r) {
        cols[0]->insert_data(reinterpret_cast<const char*>(&r.k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&r.v1), sizeof(int32_t));
        cols[2]->insert_data(reinterpret_cast<const char*>(&r.v2), sizeof(int32_t));
        cols[3]->insert_data(reinterpret_cast<const char*>(&r.seq), sizeof(int32_t));
        cols[4]->insert_data(reinterpret_cast<const char*>(&r.delete_sign), sizeof(int8_t));
        BitmapValue skip;
        if (!r.give_v1) {
            skip.add(static_cast<uint64_t>(schema->column(1).unique_id()));
        }
        if (!r.give_v2) {
            skip.add(static_cast<uint64_t>(schema->column(2).unique_id()));
        }
        if (!r.give_seq) {
            skip.add(static_cast<uint64_t>(schema->column(3).unique_id()));
        }
        if (!r.give_ds) {
            skip.add(static_cast<uint64_t>(schema->column(4).unique_id()));
        }
        assert_cast<ColumnBitmap*>(cols[5].get())->insert_value(std::move(skip));
    }

    // (k INT key, v INT nullable, delete_sign, skip_bitmap, __DORIS_ROW_STORE_COL__)
    // flexible MoW schema with the hidden full row-store column on, exercising
    // FlexibleReadPlan::fill_non_primary_key_columns_for_row_store: a skipped cell
    // is restored through fetch_value_through_row_column instead of the column store.
    TabletSchemaSPtr create_flexible_row_store_schema() {
        TabletSchemaPB pb;
        pb.set_keys_type(UNIQUE_KEYS);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_LZ4);
        pb.set_next_column_unique_id(10);
        pb.set_store_row_column(true);

        auto add = [&](int uid, const std::string& name, const std::string& type, bool is_key,
                       int len, bool nullable, const std::string& def = "") {
            ColumnPB* c = pb.add_column();
            c->set_unique_id(uid);
            c->set_name(name);
            c->set_type(type);
            c->set_is_key(is_key);
            c->set_length(len);
            c->set_index_length(type == "STRING" ? 4 : len);
            c->set_is_nullable(nullable);
            c->set_is_bf_column(false);
            c->set_aggregation("NONE");
            if (!def.empty()) {
                c->set_default_value(def);
            }
        };
        // The hidden row-store / skip-bitmap / delete-sign columns are non-nullable,
        // matching create_row_store_schema and create_flexible_mow_schema.
        add(0, "k", "INT", true, 4, false);
        add(1, "v", "INT", false, 4, true, std::to_string(0));
        add(2, DELETE_SIGN, "TINYINT", false, 1, false, std::to_string(0));
        add(3, SKIP_BITMAP_COL, "BITMAP", false, 16, false);
        add(4, BeConsts::ROW_STORE_COL, "STRING", false, 2147483643, false);
        pb.set_delete_sign_idx(2);
        pb.set_skip_bitmap_col_idx(3);

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    // Index the result block of create_flexible_seq2_schema by key.
    struct Seq2Out {
        int32_t v1;
        int32_t v2;
        int32_t seq;
        int8_t ds;
    };
    std::map<int32_t, Seq2Out> index_seq2(const Block& block) {
        std::map<int32_t, Seq2Out> by_key;
        for (size_t r = 0; r < block.rows(); ++r) {
            by_key[read_int(block, 0, r)] = Seq2Out {.v1 = read_int(block, 1, r),
                                                     .v2 = read_int(block, 2, r),
                                                     .seq = read_int(block, 3, r),
                                                     .ds = read_tinyint(block, 4, r)};
        }
        return by_key;
    }
};

// ===========================================================================
// Happy-path fill tests.
// ===========================================================================

// Flexible PU: a full-width input whose per-row skip bitmap marks `v` as not
// provided. An existing key takes the old `v` from history, a brand-new key
// takes the column default.
TEST_F(FlexiblePartialUpdateTest, FlexibleFillFromHistoryAndDefault) {
    auto schema = create_flexible_mow_schema(); // k(0) v(1) delete_sign(2) skip_bitmap(3)
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3101, 2, {{1, 11}, {2, 22}}, &tablet);
    auto mow = make_mow_context(100, {rowset});

    auto pui = make_flexible_pui(schema);
    RowsetId new_rsid;
    new_rsid.init(3102);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    const int32_t v_uid = schema->column(1).unique_id();
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t ks[] = {1, 99};
        int32_t v_dummy = 999;
        int8_t zero8 = 0;
        for (int32_t k : ks) {
            cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
            cols[1]->insert_data(reinterpret_cast<const char*>(&v_dummy), sizeof(int32_t));
            cols[2]->insert_data(reinterpret_cast<const char*>(&zero8), sizeof(int8_t));
            BitmapValue skip;
            skip.add(static_cast<uint64_t>(v_uid));
            assert_cast<ColumnBitmap*>(cols[3].get())->insert_value(std::move(skip));
        }
    }

    ASSERT_TRUE(run_flexible_fill(ctx, &block).ok());
    ASSERT_EQ(block.columns(), schema->num_columns());
    ASSERT_EQ(block.rows(), 2);
    std::map<int32_t, int32_t> kv;
    for (size_t r = 0; r < block.rows(); ++r) {
        kv[read_int(block, 0, r)] = read_int(block, 1, r);
    }
    EXPECT_EQ(kv[1], 11); // existing key: old v from history
    EXPECT_EQ(kv[99], 0); // brand-new key: column default
    EXPECT_EQ(ctx.partial_update_stats.num_rows_updated, 1);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_new_added, 1);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_deleted, 0);
}

// Flexible PU per-cell fill: a provided cell is kept, a skipped cell is filled
// from history, on different rows in the same block.
TEST_F(FlexiblePartialUpdateTest, FlexibleProvidedCellKeptSkippedFilled) {
    auto schema = create_flexible_mow_schema();
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3311, 2, {{1, 11}, {2, 22}}, &tablet);
    auto mow = make_mow_context(100, {rowset});

    auto pui = make_flexible_pui(schema);
    RowsetId new_rsid;
    new_rsid.init(3312);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    const auto v_uid = static_cast<uint64_t>(schema->column(1).unique_id());
    const auto ds_uid = static_cast<uint64_t>(schema->column(2).unique_id());
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t ks[] = {1, 2};
        int32_t vs[] = {88, 999};
        int8_t ds0 = 0;
        for (int i = 0; i < 2; ++i) {
            cols[0]->insert_data(reinterpret_cast<const char*>(&ks[i]), sizeof(int32_t));
            cols[1]->insert_data(reinterpret_cast<const char*>(&vs[i]), sizeof(int32_t));
            cols[2]->insert_data(reinterpret_cast<const char*>(&ds0), sizeof(int8_t));
            BitmapValue skip;
            skip.add(ds_uid);
            if (i == 1) {
                skip.add(v_uid);
            }
            assert_cast<ColumnBitmap*>(cols[3].get())->insert_value(std::move(skip));
        }
    }

    ASSERT_TRUE(run_flexible_fill(ctx, &block).ok());
    ASSERT_EQ(block.rows(), 2);
    std::map<int32_t, int32_t> kv;
    for (size_t r = 0; r < block.rows(); ++r) {
        kv[read_int(block, 0, r)] = read_int(block, 1, r);
    }
    EXPECT_EQ(kv[1], 88); // provided cell kept
    EXPECT_EQ(kv[2], 22); // skipped cell filled from history
}

// Flexible PU delete: a row with a delete sign skips reading the old row (default
// fill) and marks the old row.
TEST_F(FlexiblePartialUpdateTest, FlexibleDeleteSign) {
    auto schema = create_flexible_mow_schema();
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3321, 2, {{1, 11}}, &tablet);
    auto mow = make_mow_context(100, {rowset});

    auto pui = make_flexible_pui(schema);
    RowsetId new_rsid;
    new_rsid.init(3322);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    const auto v_uid = static_cast<uint64_t>(schema->column(1).unique_id());
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 1;
        int32_t v_dummy = 999;
        int8_t ds1 = 1;
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&v_dummy), sizeof(int32_t));
        cols[2]->insert_data(reinterpret_cast<const char*>(&ds1), sizeof(int8_t));
        BitmapValue skip;
        skip.add(v_uid);
        assert_cast<ColumnBitmap*>(cols[3].get())->insert_value(std::move(skip));
    }

    ASSERT_TRUE(run_flexible_fill(ctx, &block).ok());
    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(read_int(block, 1, 0), 0);     // v default (old row not read)
    EXPECT_EQ(read_tinyint(block, 2, 0), 1); // delete sign kept
    // exactly the one old row (history rowset seg0 row0) marked
    EXPECT_TRUE(mow->delete_bitmap->contains(
            {rowset->rowset_id(), 0, DeleteBitmap::TEMP_VERSION_COMMON}, 0));
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 1U);
    // a delete on an existing key counts as an update, like the fixed path
    EXPECT_EQ(ctx.partial_update_stats.num_rows_updated, 1);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_deleted, 0);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_new_added, 0);
}

// A lone delete-signed row on an absent key: the tombstone is kept and
// default-filled, nothing is marked, and handle_new_key is not consulted --
// the ERROR policy must NOT fire for a delete.
TEST_F(FlexiblePartialUpdateTest, FlexibleDeleteSignNewKey) {
    auto schema = create_flexible_mow_schema();
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3671, 2, {{7, 70}}, &tablet); // key 99 is absent
    auto mow = make_mow_context(100, {rowset});
    auto pui = make_flexible_pui(schema, PartialUpdateNewRowPolicyPB::ERROR);
    RowsetId new_rsid;
    new_rsid.init(3672);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    const auto v_uid = static_cast<uint64_t>(schema->column(1).unique_id());
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 99, v_dummy = 999;
        int8_t ds1 = 1;
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&v_dummy), sizeof(int32_t));
        cols[2]->insert_data(reinterpret_cast<const char*>(&ds1), sizeof(int8_t));
        BitmapValue skip;
        skip.add(v_uid);
        assert_cast<ColumnBitmap*>(cols[3].get())->insert_value(std::move(skip));
    }

    ASSERT_TRUE(run_flexible_fill(ctx, &block).ok());
    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(read_int(block, 0, 0), 99);
    EXPECT_EQ(read_int(block, 1, 0), 0);     // v default (no old row)
    EXPECT_EQ(read_tinyint(block, 2, 0), 1); // delete sign kept
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 0U);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_new_added, 1);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_updated, 0);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_deleted, 0);
}

// ERROR new-key policy rejects a brand-new (non-delete) flexible row.
TEST_F(FlexiblePartialUpdateTest, FlexibleNewKeyErrorPolicyRejected) {
    auto schema = create_flexible_mow_schema();
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3675, 2, {{7, 70}}, &tablet); // key 99 is absent
    auto mow = make_mow_context(100, {rowset});
    auto pui = make_flexible_pui(schema, PartialUpdateNewRowPolicyPB::ERROR);
    RowsetId new_rsid;
    new_rsid.init(3676);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    const auto ds_uid = static_cast<uint64_t>(schema->column(2).unique_id());
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 99, v = 55;
        int8_t ds0 = 0;
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&v), sizeof(int32_t));
        cols[2]->insert_data(reinterpret_cast<const char*>(&ds0), sizeof(int8_t));
        BitmapValue skip;
        skip.add(ds_uid);
        assert_cast<ColumnBitmap*>(cols[3].get())->insert_value(std::move(skip));
    }

    auto st = run_flexible_fill(ctx, &block);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("Can't append new rows in partial update"), std::string::npos)
            << st;
}

// APPEND with a new key whose insert SKIPS a NOT NULL no-default column: the
// flexible-only handle_new_key arm that reads the skip bitmap rejects the row.
TEST_F(FlexiblePartialUpdateTest, FlexibleNewKeyAppendRequiredColumnMissing) {
    auto schema = create_flexible_required_value_schema();
    TabletSharedPtr tablet;
    auto rowset = write_rowset_block(
            schema, 3678, 2,
            [&](Block& b) {
                auto guard = b.mutate_columns_scoped();
                auto& cols = guard.mutable_columns();
                int32_t k = 7, v = 70;
                int8_t ds0 = 0;
                cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
                cols[1]->insert_data(reinterpret_cast<const char*>(&v), sizeof(int32_t));
                cols[2]->insert_data(reinterpret_cast<const char*>(&ds0), sizeof(int8_t));
                cols[3]->insert_default();
            },
            &tablet);
    auto mow = make_mow_context(100, {rowset});
    auto pui = make_flexible_pui(schema);
    RowsetId new_rsid;
    new_rsid.init(3679);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    const auto v_uid = static_cast<uint64_t>(schema->column(1).unique_id());
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 99, v_dummy = 0;
        int8_t ds0 = 0;
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&v_dummy), sizeof(int32_t));
        cols[2]->insert_data(reinterpret_cast<const char*>(&ds0), sizeof(int8_t));
        BitmapValue skip;
        skip.add(v_uid); // skips the NOT NULL no-default v on a brand-new key
        assert_cast<ColumnBitmap*>(cols[3].get())->insert_value(std::move(skip));
    }

    auto st = run_flexible_fill(ctx, &block);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("should have default value or be nullable"), std::string::npos)
            << st;
}

// Flexible PU insert-after-delete on a brand-new key: a delete row followed by an
// insert row for the same key merges into the single insert.
TEST_F(FlexiblePartialUpdateTest, FlexibleInsertAfterDeleteNewKey) {
    auto schema = create_flexible_mow_schema();
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3331, 2, {{7, 70}}, &tablet); // key 1 is new
    auto mow = make_mow_context(100, {rowset});

    auto pui = make_flexible_pui(schema);
    RowsetId new_rsid;
    new_rsid.init(3332);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    const auto v_uid = static_cast<uint64_t>(schema->column(1).unique_id());
    const auto ds_uid = static_cast<uint64_t>(schema->column(2).unique_id());
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 1;
        int32_t v0 = 0;
        int32_t v1 = 77;
        int8_t ds_del = 1;
        int8_t ds_ins = 0;
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&v0), sizeof(int32_t));
        cols[2]->insert_data(reinterpret_cast<const char*>(&ds_del), sizeof(int8_t));
        BitmapValue skip_del;
        skip_del.add(v_uid);
        assert_cast<ColumnBitmap*>(cols[3].get())->insert_value(std::move(skip_del));
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&v1), sizeof(int32_t));
        cols[2]->insert_data(reinterpret_cast<const char*>(&ds_ins), sizeof(int8_t));
        BitmapValue skip_ins;
        skip_ins.add(ds_uid);
        assert_cast<ColumnBitmap*>(cols[3].get())->insert_value(std::move(skip_ins));
    }

    ASSERT_TRUE(run_flexible_fill(ctx, &block).ok());
    ASSERT_EQ(block.rows(), 1); // the delete row was merged away
    EXPECT_EQ(read_int(block, 0, 0), 1);
    EXPECT_EQ(read_int(block, 1, 0), 77);    // the insert's provided v survives
    EXPECT_EQ(read_tinyint(block, 2, 0), 0); // not a delete
}

// Insert-after-delete on an EXISTING key must not resurrect history: with two
// value columns and an insert that provides only v1 and SKIPS v2, the surviving
// insert is treated as a brand-new row -- v2 takes the DEFAULT (0), NOT the
// history v2 (400). This is the core use_defaults_for_in_load_deleted
// semantics; a single-value-column schema could not prove it.
TEST_F(FlexiblePartialUpdateTest, FlexibleInsertAfterDeleteExistingKeyNoResurrect) {
    auto schema = create_flexible_seq2_schema(); // k v1 v2 seq ds skip
    TabletSharedPtr tablet;
    // history k=2: v1=300, v2=400, seq=2 (distinct from defaults)
    auto rowset =
            write_seq2_history(schema, 3401, /*k=*/2, /*v1=*/300, /*v2=*/400, /*seq=*/2, &tablet);
    auto mow = make_mow_context(100, {rowset});

    auto pui = make_flexible_pui(schema);
    RowsetId new_rsid;
    new_rsid.init(3402);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        // r0: DELETE k=2 (seq=7), provides ds + seq, skips v1/v2
        append_seq2_row(schema, cols,
                        Seq2Row {.k = 2,
                                 .v1 = 0,
                                 .v2 = 0,
                                 .seq = 7,
                                 .delete_sign = 1,
                                 .give_v1 = false,
                                 .give_v2 = false,
                                 .give_seq = true,
                                 .give_ds = true});
        // r1: INSERT k=2 only v1=330 (seq=8), skips v2 and ds
        append_seq2_row(schema, cols,
                        Seq2Row {.k = 2,
                                 .v1 = 330,
                                 .v2 = 999,
                                 .seq = 8,
                                 .delete_sign = 0,
                                 .give_v1 = true,
                                 .give_v2 = false,
                                 .give_seq = true,
                                 .give_ds = false});
    }

    ASSERT_TRUE(run_flexible_fill(ctx, &block).ok());
    ASSERT_EQ(block.rows(), 1); // tombstone merged away
    auto out = index_seq2(block);
    ASSERT_TRUE(out.contains(2));
    EXPECT_EQ(out[2].v1, 330); // provided
    EXPECT_EQ(out[2].v2, 0);   // DEFAULT, not the history 400 (no resurrection)
    EXPECT_EQ(out[2].ds, 0);
    // history k=2 old row (seg0 row0) marked; the surviving insert is not self-marked
    EXPECT_TRUE(mow->delete_bitmap->contains(
            {rowset->rowset_id(), 0, DeleteBitmap::TEMP_VERSION_COMMON}, 0));
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 1U);
}

// Seq dedup with the key present in history (k=1, baseline 5), so the FOUND
// arm of aggregate_rows actually runs. Two same-key rows, both seq >= 5, merge
// into the higher-seq winner.
TEST_F(FlexiblePartialUpdateTest, FlexibleSeqColumnDedupHigherWinsFoundArm) {
    auto schema = create_flexible_mow_schema(/*has_seq=*/true); // k v seq ds skip
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3301, 2, {{1, 11, 5, 0}}, &tablet); // history k=1 seq=5
    auto mow = make_mow_context(100, {rowset});

    auto pui = make_flexible_pui(schema);
    RowsetId new_rsid;
    new_rsid.init(3302);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    const auto ds_uid = static_cast<uint64_t>(schema->column(3).unique_id());
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t ks[] = {1, 1};
        int32_t vs[] = {50, 80};
        int32_t seqs[] = {6, 8}; // both >= history baseline 5
        int8_t ds0 = 0;
        for (int i = 0; i < 2; ++i) {
            cols[0]->insert_data(reinterpret_cast<const char*>(&ks[i]), sizeof(int32_t));
            cols[1]->insert_data(reinterpret_cast<const char*>(&vs[i]), sizeof(int32_t));
            cols[2]->insert_data(reinterpret_cast<const char*>(&seqs[i]), sizeof(int32_t));
            cols[3]->insert_data(reinterpret_cast<const char*>(&ds0), sizeof(int8_t));
            BitmapValue skip; // only delete sign not provided
            skip.add(ds_uid);
            assert_cast<ColumnBitmap*>(cols[4].get())->insert_value(std::move(skip));
        }
    }

    ASSERT_TRUE(run_flexible_fill(ctx, &block).ok());
    ASSERT_EQ(block.rows(), 1); // merged
    EXPECT_EQ(read_int(block, 0, 0), 1);
    EXPECT_EQ(read_int(block, 1, 0), 80); // higher-seq row wins
    EXPECT_EQ(read_int(block, 2, 0), 8);
    // FOUND in history: the old row (seg0 row0) is marked by the probe
    EXPECT_TRUE(mow->delete_bitmap->contains(
            {rowset->rowset_id(), 0, DeleteBitmap::TEMP_VERSION_COMMON}, 0));
}

// Flexible seq-loser self-mark: the one place this move changed position
// semantics (legacy marked segment_start_pos + delta_pos, the stage marks the
// block row index and relies on one-block-one-fresh-segment). A two-row group
// for k=1 shrinks to one row, then a lone stale row for k=2 (below its
// baseline; single-row groups skip aggregation) loses at the probe: the
// self-mark must land at the POST-shrink position of a NON-zero segment id.
TEST_F(FlexiblePartialUpdateTest, FlexibleSeqLoserSelfMarkAfterShrink) {
    auto schema = create_flexible_mow_schema(/*has_seq=*/true);
    TabletSharedPtr tablet;
    // k=1 baseline seq=5 (history row 0), k=2 baseline seq=10 (history row 1)
    auto rowset = write_rowset(schema, 3651, 2, {{1, 11, 5, 0}, {2, 22, 10, 0}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    auto pui = make_flexible_pui(schema);
    RowsetId new_rsid;
    new_rsid.init(3652);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);
    ctx.segment_id = 3; // this block lands in segment 3 of the new rowset

    const auto v_uid = static_cast<uint64_t>(schema->column(1).unique_id());
    const auto ds_uid = static_cast<uint64_t>(schema->column(3).unique_id());
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        // r0/r1: k=1 seq 6 then 8, both >= baseline 5 -> merge into one row
        // r2: k=2 seq 3 < baseline 10, gives seq only -> lone seq loser
        int32_t ks[] = {1, 1, 2};
        int32_t vs[] = {60, 80, 0};
        int32_t seqs[] = {6, 8, 3};
        int8_t ds0 = 0;
        for (int i = 0; i < 3; ++i) {
            cols[0]->insert_data(reinterpret_cast<const char*>(&ks[i]), sizeof(int32_t));
            cols[1]->insert_data(reinterpret_cast<const char*>(&vs[i]), sizeof(int32_t));
            cols[2]->insert_data(reinterpret_cast<const char*>(&seqs[i]), sizeof(int32_t));
            cols[3]->insert_data(reinterpret_cast<const char*>(&ds0), sizeof(int8_t));
            BitmapValue skip;
            skip.add(ds_uid);
            if (i == 2) {
                skip.add(v_uid); // the loser omits v: it must stay default, not history
            }
            assert_cast<ColumnBitmap*>(cols[4].get())->insert_value(std::move(skip));
        }
    }

    ASSERT_TRUE(run_flexible_fill(ctx, &block).ok());
    ASSERT_EQ(block.rows(), 2); // k=1 merged away one row, k=2 kept
    EXPECT_EQ(read_int(block, 0, 1), 2);
    EXPECT_EQ(read_int(block, 1, 1), 0); // loser: default, NOT the history 22
    EXPECT_EQ(read_int(block, 2, 1), 3);

    // the losing row self-marks at exactly {new_rsid, segment 3, post-shrink pos 1}
    EXPECT_TRUE(mow->delete_bitmap->contains({new_rsid, 3, DeleteBitmap::TEMP_VERSION_COMMON}, 1));
    EXPECT_FALSE(mow->delete_bitmap->contains({new_rsid, 0, DeleteBitmap::TEMP_VERSION_COMMON}, 1));
    EXPECT_FALSE(mow->delete_bitmap->contains({new_rsid, 3, DeleteBitmap::TEMP_VERSION_COMMON}, 2));
    // k=2's history row survives the loss; k=1's history row is a normal update mark
    EXPECT_FALSE(mow->delete_bitmap->contains(
            {rowset->rowset_id(), 0, DeleteBitmap::TEMP_VERSION_COMMON}, 1));
    EXPECT_TRUE(mow->delete_bitmap->contains(
            {rowset->rowset_id(), 0, DeleteBitmap::TEMP_VERSION_COMMON}, 0));
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 2U);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_updated, 1);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_deleted, 1);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_new_added, 0);
}

// ===========================================================================
// BlockAggregator::aggregate_rows branch coverage. All use
// create_flexible_mow_schema(true) with the key present in history so the FOUND
// arm (probe_previous_seq_value returning a baseline seq) is exercised.
// ===========================================================================

// FOUND arm, stale first row skipped (seq < history baseline -> `continue`).
// History k=1 seq=10. Two rows: r0 seq=3 (< 10, stale, skipped as start), r1
// seq=12 (>= 10, becomes start and the only survivor).
TEST_F(FlexiblePartialUpdateTest, AggFoundStaleFirstRowSkipped) {
    auto schema = create_flexible_mow_schema(/*has_seq=*/true);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3501, 2, {{1, 11, 10, 0}}, &tablet); // baseline seq=10
    auto mow = make_mow_context(100, {rowset});
    auto pui = make_flexible_pui(schema);
    RowsetId new_rsid;
    new_rsid.init(3502);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    const auto ds_uid = static_cast<uint64_t>(schema->column(3).unique_id());
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 1;
        int32_t vs[] = {30, 120};
        int32_t seqs[] = {3, 12};
        int8_t ds0 = 0;
        for (int i = 0; i < 2; ++i) {
            cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
            cols[1]->insert_data(reinterpret_cast<const char*>(&vs[i]), sizeof(int32_t));
            cols[2]->insert_data(reinterpret_cast<const char*>(&seqs[i]), sizeof(int32_t));
            cols[3]->insert_data(reinterpret_cast<const char*>(&ds0), sizeof(int8_t));
            BitmapValue skip;
            skip.add(ds_uid);
            assert_cast<ColumnBitmap*>(cols[4].get())->insert_value(std::move(skip));
        }
    }

    ASSERT_TRUE(run_flexible_fill(ctx, &block).ok());
    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(read_int(block, 1, 0), 120); // stale seq=3 row dropped, seq=12 kept
    EXPECT_EQ(read_int(block, 2, 0), 12);
}

// FOUND arm, first-row-with-seq becomes start. History k=1 seq=5. First row
// already has seq=9 (>= 5) so it is the start; a following stale seq=4 row is
// out-of-order-discarded in the main loop.
TEST_F(FlexiblePartialUpdateTest, AggFoundFirstRowWithSeqBecomesStart) {
    auto schema = create_flexible_mow_schema(/*has_seq=*/true);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3511, 2, {{1, 11, 5, 0}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    auto pui = make_flexible_pui(schema);
    RowsetId new_rsid;
    new_rsid.init(3512);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    const auto ds_uid = static_cast<uint64_t>(schema->column(3).unique_id());
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 1;
        int32_t vs[] = {90, 40};
        int32_t seqs[] = {9, 4};
        int8_t ds0 = 0;
        for (int i = 0; i < 2; ++i) {
            cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
            cols[1]->insert_data(reinterpret_cast<const char*>(&vs[i]), sizeof(int32_t));
            cols[2]->insert_data(reinterpret_cast<const char*>(&seqs[i]), sizeof(int32_t));
            cols[3]->insert_data(reinterpret_cast<const char*>(&ds0), sizeof(int8_t));
            BitmapValue skip;
            skip.add(ds_uid);
            assert_cast<ColumnBitmap*>(cols[4].get())->insert_value(std::move(skip));
        }
    }

    ASSERT_TRUE(run_flexible_fill(ctx, &block).ok());
    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(read_int(block, 1, 0), 90); // seq=9 start kept, seq=4 discarded
    EXPECT_EQ(read_int(block, 2, 0), 9);
}

// FOUND arm, first-row-without-seq inherits the history baseline. History k=1
// seq=5, v=11. r0 omits seq -> inherits baseline 5 (and is direct-fed by the
// main loop). r1 has seq=8 (>= 5) -> accepted and merged.
TEST_F(FlexiblePartialUpdateTest, AggFoundFirstRowWithoutSeqInheritsBaseline) {
    auto schema = create_flexible_mow_schema(/*has_seq=*/true);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3521, 2, {{1, 11, 5, 0}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    auto pui = make_flexible_pui(schema);
    RowsetId new_rsid;
    new_rsid.init(3522);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    const auto v_uid = static_cast<uint64_t>(schema->column(1).unique_id());
    const auto seq_uid = static_cast<uint64_t>(schema->column(2).unique_id());
    const auto ds_uid = static_cast<uint64_t>(schema->column(3).unique_id());
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 1;
        // r0: v=70, omits seq (and ds) -> inherits history seq 5
        int32_t v0 = 70, seq0 = 0;
        int8_t ds0 = 0;
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&v0), sizeof(int32_t));
        cols[2]->insert_data(reinterpret_cast<const char*>(&seq0), sizeof(int32_t));
        cols[3]->insert_data(reinterpret_cast<const char*>(&ds0), sizeof(int8_t));
        BitmapValue skip0;
        skip0.add(seq_uid);
        skip0.add(ds_uid);
        assert_cast<ColumnBitmap*>(cols[4].get())->insert_value(std::move(skip0));
        // r1: seq=8 (>= inherited baseline 5) -> accepted; gives seq only and
        // SKIPS v, so the merge keeps r0's v=70 (proves the inherited start runs).
        int32_t v1 = 80, seq1 = 8;
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&v1), sizeof(int32_t));
        cols[2]->insert_data(reinterpret_cast<const char*>(&seq1), sizeof(int32_t));
        cols[3]->insert_data(reinterpret_cast<const char*>(&ds0), sizeof(int8_t));
        BitmapValue skip1;
        skip1.add(ds_uid); // gives seq, skips v + ds
        skip1.add(v_uid);
        assert_cast<ColumnBitmap*>(cols[4].get())->insert_value(std::move(skip1));
    }

    ASSERT_TRUE(run_flexible_fill(ctx, &block).ok());
    ASSERT_EQ(block.rows(), 1);
    // r1 provided seq=8 only; v cell skipped -> keeps r0's v=70 after merge
    EXPECT_EQ(read_int(block, 1, 0), 70);
    EXPECT_EQ(read_int(block, 2, 0), 8); // baseline inherited then advanced to 8
}

// NOT_FOUND arm, first-row-with-seq. Brand-new key (history has only k=7):
// the first row's own seq becomes the start; a later stale row is discarded.
TEST_F(FlexiblePartialUpdateTest, AggNotFoundFirstRowWithSeq) {
    auto schema = create_flexible_mow_schema(/*has_seq=*/true);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3531, 2, {{7, 70, 1, 0}}, &tablet); // k=3 absent
    auto mow = make_mow_context(100, {rowset});
    auto pui = make_flexible_pui(schema);
    RowsetId new_rsid;
    new_rsid.init(3532);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    const auto ds_uid = static_cast<uint64_t>(schema->column(3).unique_id());
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 3;
        int32_t vs[] = {500, 490};
        int32_t seqs[] = {9, 6}; // r0 seq=9 -> start; r1 seq=6 < 9 -> discarded
        int8_t ds0 = 0;
        for (int i = 0; i < 2; ++i) {
            cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
            cols[1]->insert_data(reinterpret_cast<const char*>(&vs[i]), sizeof(int32_t));
            cols[2]->insert_data(reinterpret_cast<const char*>(&seqs[i]), sizeof(int32_t));
            cols[3]->insert_data(reinterpret_cast<const char*>(&ds0), sizeof(int8_t));
            BitmapValue skip;
            skip.add(ds_uid);
            assert_cast<ColumnBitmap*>(cols[4].get())->insert_value(std::move(skip));
        }
    }

    ASSERT_TRUE(run_flexible_fill(ctx, &block).ok());
    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(read_int(block, 0, 0), 3);
    EXPECT_EQ(read_int(block, 1, 0), 500); // seq=9 start kept
    EXPECT_EQ(read_int(block, 2, 0), 9);
}

// NOT_FOUND arm, first-row-without-seq -> default seq. Brand-new key whose
// first row omits seq: cur_seq_val is the encoded DEFAULT seq (0 here). The
// second row gives seq=4 (>= default 0) so it is accepted and merges.
TEST_F(FlexiblePartialUpdateTest, AggNotFoundFirstRowWithoutSeqDefaultSeq) {
    auto schema = create_flexible_mow_schema(/*has_seq=*/true);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3541, 2, {{7, 70, 1, 0}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    auto pui = make_flexible_pui(schema);
    RowsetId new_rsid;
    new_rsid.init(3542);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    const auto v_uid = static_cast<uint64_t>(schema->column(1).unique_id());
    const auto seq_uid = static_cast<uint64_t>(schema->column(2).unique_id());
    const auto ds_uid = static_cast<uint64_t>(schema->column(3).unique_id());
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 4;
        // r0: v=600, omits seq -> default seq baseline
        int32_t v0 = 600, seq0 = 0;
        int8_t ds0 = 0;
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&v0), sizeof(int32_t));
        cols[2]->insert_data(reinterpret_cast<const char*>(&seq0), sizeof(int32_t));
        cols[3]->insert_data(reinterpret_cast<const char*>(&ds0), sizeof(int8_t));
        BitmapValue skip0;
        skip0.add(seq_uid);
        skip0.add(ds_uid);
        assert_cast<ColumnBitmap*>(cols[4].get())->insert_value(std::move(skip0));
        // r1: gives seq=4 only (>= default 0) -> accepted, v cell skipped keeps r0
        int32_t v1 = 0, seq1 = 4;
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&v1), sizeof(int32_t));
        cols[2]->insert_data(reinterpret_cast<const char*>(&seq1), sizeof(int32_t));
        cols[3]->insert_data(reinterpret_cast<const char*>(&ds0), sizeof(int8_t));
        BitmapValue skip1;
        skip1.add(v_uid);
        skip1.add(ds_uid);
        assert_cast<ColumnBitmap*>(cols[4].get())->insert_value(std::move(skip1));
    }

    ASSERT_TRUE(run_flexible_fill(ctx, &block).ok());
    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(read_int(block, 0, 0), 4);
    EXPECT_EQ(read_int(block, 1, 0), 600); // r0's v kept (r1 skipped v)
    EXPECT_EQ(read_int(block, 2, 0), 4);   // advanced to r1's seq
}

// Main loop: a no-seq row is direct-fed, a seq>=cur row is accepted, and a
// seq<cur out-of-order row is discarded -- all three arms in one key's group.
// History k=1 seq=5. r0 no-seq (inherits 5, direct-fed, v=10), r1 seq=7 (>=5
// accepted, v=70), r2 seq=6 (< 7, out-of-order discarded, v=60).
TEST_F(FlexiblePartialUpdateTest, AggMainLoopThreeArms) {
    auto schema = create_flexible_mow_schema(/*has_seq=*/true);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3551, 2, {{1, 11, 5, 0}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    auto pui = make_flexible_pui(schema);
    RowsetId new_rsid;
    new_rsid.init(3552);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    const auto seq_uid = static_cast<uint64_t>(schema->column(2).unique_id());
    const auto ds_uid = static_cast<uint64_t>(schema->column(3).unique_id());
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 1;
        int8_t ds0 = 0;
        // r0: v=10, no seq (direct-fed, inherits baseline 5)
        int32_t v0 = 10, seq0 = 0;
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&v0), sizeof(int32_t));
        cols[2]->insert_data(reinterpret_cast<const char*>(&seq0), sizeof(int32_t));
        cols[3]->insert_data(reinterpret_cast<const char*>(&ds0), sizeof(int8_t));
        BitmapValue s0;
        s0.add(seq_uid);
        s0.add(ds_uid);
        assert_cast<ColumnBitmap*>(cols[4].get())->insert_value(std::move(s0));
        // r1: v=70, seq=7 (>= 5) accepted, advances cur to 7
        int32_t v1 = 70, seq1 = 7;
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&v1), sizeof(int32_t));
        cols[2]->insert_data(reinterpret_cast<const char*>(&seq1), sizeof(int32_t));
        cols[3]->insert_data(reinterpret_cast<const char*>(&ds0), sizeof(int8_t));
        BitmapValue s1;
        s1.add(ds_uid);
        assert_cast<ColumnBitmap*>(cols[4].get())->insert_value(std::move(s1));
        // r2: v=60, seq=6 (< cur 7) out-of-order discarded
        int32_t v2 = 60, seq2 = 6;
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&v2), sizeof(int32_t));
        cols[2]->insert_data(reinterpret_cast<const char*>(&seq2), sizeof(int32_t));
        cols[3]->insert_data(reinterpret_cast<const char*>(&ds0), sizeof(int8_t));
        BitmapValue s2;
        s2.add(ds_uid);
        assert_cast<ColumnBitmap*>(cols[4].get())->insert_value(std::move(s2));
    }

    ASSERT_TRUE(run_flexible_fill(ctx, &block).ok());
    ASSERT_EQ(block.rows(), 1);
    // r0 (v=10) then r1 merges over it (gives v=70, seq=7); r2 discarded
    EXPECT_EQ(read_int(block, 1, 0), 70);
    EXPECT_EQ(read_int(block, 2, 0), 7);
}

// aggregate_rows all-stale boundary: every row of a key is below the history
// baseline, so the FOUND start scan reaches `pos == end` and the main loop
// never appends -> the key is absent from the result and its history row keeps
// no delete-bitmap mark. A second key with a valid row survives alongside, so
// the block itself stays non-empty.
TEST_F(FlexiblePartialUpdateTest, AggAllRowsStaleKeyVanishes) {
    auto schema = create_flexible_mow_schema(/*has_seq=*/true);
    TabletSharedPtr tablet;
    // k=1 seq=20 (high baseline), k=9 seq=1
    auto rowset = write_rowset(schema, 3561, 2, {{1, 11, 20, 0}, {9, 99, 1, 0}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    auto pui = make_flexible_pui(schema);
    RowsetId new_rsid;
    new_rsid.init(3562);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    const auto ds_uid = static_cast<uint64_t>(schema->column(3).unique_id());
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        // two same-key rows for k=1, both < baseline 20 -> the whole key is
        // dropped; one valid row for k=9 (seq 5 >= baseline 1) survives
        int32_t ks[] = {1, 1, 9};
        int32_t vs[] = {30, 40, 90};
        int32_t seqs[] = {3, 4, 5};
        int8_t ds0 = 0;
        for (int i = 0; i < 3; ++i) {
            cols[0]->insert_data(reinterpret_cast<const char*>(&ks[i]), sizeof(int32_t));
            cols[1]->insert_data(reinterpret_cast<const char*>(&vs[i]), sizeof(int32_t));
            cols[2]->insert_data(reinterpret_cast<const char*>(&seqs[i]), sizeof(int32_t));
            cols[3]->insert_data(reinterpret_cast<const char*>(&ds0), sizeof(int8_t));
            BitmapValue skip;
            skip.add(ds_uid);
            assert_cast<ColumnBitmap*>(cols[4].get())->insert_value(std::move(skip));
        }
    }

    ASSERT_TRUE(run_flexible_fill(ctx, &block).ok());
    // the whole stale key disappears; only k=9 remains
    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(read_int(block, 0, 0), 9);
    EXPECT_EQ(read_int(block, 1, 0), 90);
    EXPECT_EQ(read_int(block, 2, 0), 5);
    // history k=1 old row (seg0 row0) must NOT be marked (never reached the
    // probe); k=9's old row (seg0 row1) is marked by its surviving update
    EXPECT_FALSE(mow->delete_bitmap->contains(
            {rowset->rowset_id(), 0, DeleteBitmap::TEMP_VERSION_COMMON}, 0));
    EXPECT_TRUE(mow->delete_bitmap->contains(
            {rowset->rowset_id(), 0, DeleteBitmap::TEMP_VERSION_COMMON}, 1));
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 1U);
}

// AggregateState machine: a delete inside a seq group clears the accumulated rows
// (remove_last_n_rows), leaving a pure tombstone. History k=1 seq=5. r0/r1 are
// upserts that accumulate, r2 is a delete (seq=9) that clears them -> only the
// tombstone survives, marking the history row.
TEST_F(FlexiblePartialUpdateTest, AggDeleteInsideSeqGroupClearsState) {
    auto schema = create_flexible_mow_schema(/*has_seq=*/true);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3571, 2, {{1, 11, 5, 0}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    auto pui = make_flexible_pui(schema);
    RowsetId new_rsid;
    new_rsid.init(3572);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    const auto ds_uid = static_cast<uint64_t>(schema->column(3).unique_id());
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 1;
        // r0 v=60 seq=6, r1 v=70 seq=7 (accumulate), r2 delete seq=9 (clears state)
        int32_t vs[] = {60, 70, 0};
        int32_t seqs[] = {6, 7, 9};
        int8_t dss[] = {0, 0, 1};
        for (int i = 0; i < 3; ++i) {
            cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
            cols[1]->insert_data(reinterpret_cast<const char*>(&vs[i]), sizeof(int32_t));
            cols[2]->insert_data(reinterpret_cast<const char*>(&seqs[i]), sizeof(int32_t));
            cols[3]->insert_data(reinterpret_cast<const char*>(&dss[i]), sizeof(int8_t));
            BitmapValue skip; // r0/r1 give v+seq; r2 gives seq+ds, omits v
            if (i == 2) {
                skip.add(static_cast<uint64_t>(schema->column(1).unique_id())); // delete omits v
            } else {
                skip.add(ds_uid);
            }
            assert_cast<ColumnBitmap*>(cols[4].get())->insert_value(std::move(skip));
        }
    }

    ASSERT_TRUE(run_flexible_fill(ctx, &block).ok());
    // accumulated upserts cleared by the delete -> one tombstone row survives
    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(read_int(block, 0, 0), 1);
    EXPECT_EQ(read_tinyint(block, 3, 0), 1); // delete sign kept (pure tombstone)
    // on a seq table a delete does NOT take defaults: the tombstone's skipped v
    // is read back from history (11), pinning the delete_sign_skip=false branch
    EXPECT_EQ(read_int(block, 1, 0), 11);
    // delete marks the history old row (FOUND, delete_sign_skip false on seq table)
    EXPECT_TRUE(mow->delete_bitmap->contains(
            {rowset->rowset_id(), 0, DeleteBitmap::TEMP_VERSION_COMMON}, 0));
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 1U);
}

// Insert-after-delete WITH seq inheritance: the insert row OMITS seq, so
// aggregate_for_insert_after_delete reads the old row's seq and the insert
// inherits it via fill_sequence_column. History k=2 seq=2. Delete (seq=7) then
// insert v1=330 with NO seq -> resulting seq must equal the history seq (2).
TEST_F(FlexiblePartialUpdateTest, AggInsertAfterDeleteSeqInheritance) {
    auto schema = create_flexible_seq2_schema(); // k v1 v2 seq ds skip
    TabletSharedPtr tablet;
    auto rowset =
            write_seq2_history(schema, 3601, /*k=*/2, /*v1=*/300, /*v2=*/400, /*seq=*/2, &tablet);
    auto mow = make_mow_context(100, {rowset});
    auto pui = make_flexible_pui(schema);
    RowsetId new_rsid;
    new_rsid.init(3602);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        // r0: DELETE k=2 with seq=7 (gives seq + ds; skips v1/v2). The tombstone's
        // own seq must be >= history baseline 2 so step1 keeps the [D, I] pair.
        append_seq2_row(schema, cols,
                        Seq2Row {.k = 2,
                                 .v1 = 0,
                                 .v2 = 0,
                                 .seq = 7,
                                 .delete_sign = 1,
                                 .give_v1 = false,
                                 .give_v2 = false,
                                 .give_seq = true,
                                 .give_ds = true});
        // r1: INSERT k=2 v1=330, NO seq, NO ds -> step2 must read old seq (2) and
        // fill_sequence_column makes the insert inherit it.
        append_seq2_row(schema, cols,
                        Seq2Row {.k = 2,
                                 .v1 = 330,
                                 .v2 = 999,
                                 .seq = 0,
                                 .delete_sign = 0,
                                 .give_v1 = true,
                                 .give_v2 = false,
                                 .give_seq = false,
                                 .give_ds = false});
    }

    ASSERT_TRUE(run_flexible_fill(ctx, &block).ok());
    ASSERT_EQ(block.rows(), 1);
    auto out = index_seq2(block);
    ASSERT_TRUE(out.contains(2));
    EXPECT_EQ(out[2].v1, 330); // provided
    EXPECT_EQ(out[2].v2, 0);   // default, not resurrected history 400
    EXPECT_EQ(out[2].seq, 2);  // inherited the history seq via fill_sequence_column
    EXPECT_EQ(out[2].ds, 0);
    // history old row marked
    EXPECT_TRUE(mow->delete_bitmap->contains(
            {rowset->rowset_id(), 0, DeleteBitmap::TEMP_VERSION_COMMON}, 0));
}

// FlexibleReadPlan::fill_non_primary_key_columns_for_row_store (use_row_store
// branch, previously uncovered by the flexible tests): with the hidden full
// row-store column on, a skipped cell of an existing key is restored through
// fetch_value_through_row_column (== history 11, not the default 0), while a
// provided cell is kept. Mirrors FixedRowStoreReadPath on the flexible path.
TEST_F(FlexiblePartialUpdateTest, FlexibleRowStoreReadPath) {
    auto schema = create_flexible_row_store_schema();
    ASSERT_TRUE(schema->has_row_store_for_all_columns());
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3611, 2, {{1, 11}, {2, 22}}, &tablet);
    auto mow = make_mow_context(100, {rowset});

    auto pui = make_flexible_pui(schema);
    RowsetId new_rsid;
    new_rsid.init(3612);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    const auto v_uid = static_cast<uint64_t>(schema->column(1).unique_id());
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t ks[] = {1, 2};
        int32_t vs[] = {999, 222}; // r0 v is a placeholder (skipped); r1 provides 222
        int8_t ds0 = 0;
        for (int i = 0; i < 2; ++i) {
            cols[0]->insert_data(reinterpret_cast<const char*>(&ks[i]), sizeof(int32_t));
            cols[1]->insert_data(reinterpret_cast<const char*>(&vs[i]), sizeof(int32_t));
            cols[2]->insert_data(reinterpret_cast<const char*>(&ds0), sizeof(int8_t));
            BitmapValue skip;
            if (i == 0) {
                skip.add(v_uid); // r0 skips v -> must come back from the row store
            }
            assert_cast<ColumnBitmap*>(cols[3].get())->insert_value(std::move(skip));
            cols[4]->insert_default(); // row-store col placeholder (regenerated downstream)
        }
    }

    ASSERT_TRUE(run_flexible_fill(ctx, &block).ok());
    ASSERT_EQ(block.columns(), schema->num_columns());
    ASSERT_EQ(block.rows(), 2);
    std::map<int32_t, int32_t> kv;
    for (size_t r = 0; r < block.rows(); ++r) {
        kv[read_int(block, 0, r)] = read_int(block, 1, r);
    }
    EXPECT_EQ(kv[1], 11);  // skipped cell restored through the row column (!= default 0)
    EXPECT_EQ(kv[2], 222); // provided cell kept
    EXPECT_FALSE(read_is_null(block, 1, 0));
    // both existing keys mark their old rows (rowid 0 and 1)
    EXPECT_TRUE(mow->delete_bitmap->contains(
            {rowset->rowset_id(), 0, DeleteBitmap::TEMP_VERSION_COMMON}, 0));
    EXPECT_TRUE(mow->delete_bitmap->contains(
            {rowset->rowset_id(), 0, DeleteBitmap::TEMP_VERSION_COMMON}, 1));
}

// A partial shrink (two same-key rows merge into one) flows through the same
// seam: one row lands in the segment, the aggregated-away row is accounted for
// and the flush succeeds.
TEST_F(FlexiblePartialUpdateTest, FlushCountsAggregatedAwayRows) {
    auto schema = create_flexible_mow_schema(/*has_seq=*/true);
    TabletSharedPtr tablet;
    auto history = write_rowset(schema, 3725, 2, {{7, 70, 1, 0}}, &tablet); // k=1 is new
    auto mow = make_mow_context(100, {history});
    auto pui = make_flexible_pui(schema);

    const auto ds_uid = static_cast<uint64_t>(schema->column(3).unique_id());
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 1;
        int32_t vs[] = {50, 80};
        int32_t seqs[] = {6, 8}; // same key, both valid -> merge to the seq=8 row
        int8_t ds0 = 0;
        for (int i = 0; i < 2; ++i) {
            cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
            cols[1]->insert_data(reinterpret_cast<const char*>(&vs[i]), sizeof(int32_t));
            cols[2]->insert_data(reinterpret_cast<const char*>(&seqs[i]), sizeof(int32_t));
            cols[3]->insert_data(reinterpret_cast<const char*>(&ds0), sizeof(int8_t));
            BitmapValue skip;
            skip.add(ds_uid);
            assert_cast<ColumnBitmap*>(cols[4].get())->insert_value(std::move(skip));
        }
    }

    const bool saved_correctness_check = config::enable_merge_on_write_correctness_check;
    config::enable_merge_on_write_correctness_check = false;
    RowsetSharedPtr output;
    int64_t writer_num_rows = 0;
    const auto flush_status = flush_partial_rowset(schema, 3726, 3, tablet, mow, pui, &block,
                                                   &output, nullptr, &writer_num_rows);
    config::enable_merge_on_write_correctness_check = saved_correctness_check;
    ASSERT_TRUE(flush_status.ok()) << flush_status;
    ASSERT_NE(output, nullptr);
    EXPECT_EQ(output->rowset_meta()->num_rows(), 1);
    // the load-close check compares this counter against the received rows: it
    // must count the 2 input rows, not the 1 that survived aggregation
    EXPECT_EQ(writer_num_rows, 2);

    Block persisted;
    ASSERT_TRUE(read_rowset(output, schema, &persisted).ok());
    ASSERT_EQ(persisted.rows(), 1);
    EXPECT_EQ(read_int(persisted, 0, 0), 1);
    EXPECT_EQ(read_int(persisted, 1, 0), 80); // higher-seq row survived the merge
    EXPECT_EQ(read_int(persisted, 2, 0), 8);
}

// End-to-end writer coverage for flexible updates. The segment's physical ordering may differ
// from the legacy writer, but all filled values and sequence semantics must survive a read-back.
TEST_F(FlexiblePartialUpdateTest, VerticalWriterPersistsFilledRows) {
    auto schema = create_flexible_mow_schema(/*has_seq=*/true);
    TabletSharedPtr tablet;
    auto history = write_rowset(schema, 3711, 2, {{1, 11, 5, 0}}, &tablet);
    auto mow = make_mow_context(100, {history});
    auto pui = make_flexible_pui(schema);

    const auto value_uid = static_cast<uint64_t>(schema->column(1).unique_id());
    const auto delete_sign_uid = static_cast<uint64_t>(schema->column(3).unique_id());
    Block block = schema->create_storage_block();
    {
        auto guard = block.mutate_columns_scoped();
        auto& columns = guard.mutable_columns();
        const std::vector<std::tuple<int32_t, int32_t, int32_t, bool>> rows {{1, 999, 10, true},
                                                                             {99, 909, 7, false}};
        for (const auto& [key, value, sequence, skip_value] : rows) {
            const int8_t delete_sign = 0;
            columns[0]->insert_data(reinterpret_cast<const char*>(&key), sizeof(key));
            columns[1]->insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
            columns[2]->insert_data(reinterpret_cast<const char*>(&sequence), sizeof(sequence));
            columns[3]->insert_data(reinterpret_cast<const char*>(&delete_sign),
                                    sizeof(delete_sign));
            BitmapValue skip;
            if (skip_value) {
                skip.add(value_uid);
            }
            skip.add(delete_sign_uid);
            assert_cast<ColumnBitmap*>(columns[4].get())->insert_value(std::move(skip));
        }
    }

    const bool saved_correctness_check = config::enable_merge_on_write_correctness_check;
    config::enable_merge_on_write_correctness_check = false;
    RowsetSharedPtr output;
    const auto flush_status =
            flush_partial_rowset(schema, 3712, 3, tablet, mow, pui, &block, &output);
    config::enable_merge_on_write_correctness_check = saved_correctness_check;
    ASSERT_TRUE(flush_status.ok()) << flush_status;
    ASSERT_NE(output, nullptr);
    ASSERT_EQ(output->rowset_meta()->num_rows(), 2);

    Block persisted;
    ASSERT_TRUE(read_rowset(output, schema, &persisted).ok());
    ASSERT_EQ(persisted.rows(), 2);
    ASSERT_EQ(persisted.columns(), schema->num_columns());
    EXPECT_EQ(read_int(persisted, 0, 0), 1);
    EXPECT_EQ(read_int(persisted, 1, 0), 11); // skipped value restored from history
    EXPECT_EQ(read_int(persisted, 2, 0), 10);
    EXPECT_EQ(read_tinyint(persisted, 3, 0), 0);
    EXPECT_EQ(read_int(persisted, 0, 1), 99);
    EXPECT_EQ(read_int(persisted, 1, 1), 909); // provided value kept
    EXPECT_EQ(read_int(persisted, 2, 1), 7);
    EXPECT_EQ(read_tinyint(persisted, 3, 1), 0);

    const auto& persisted_skip_bitmaps =
            assert_cast<const ColumnBitmap&>(*persisted.get_by_position(4).column).get_data();
    ASSERT_EQ(persisted_skip_bitmaps.size(), 2);
    EXPECT_TRUE(persisted_skip_bitmaps[0].contains(value_uid));
    EXPECT_TRUE(persisted_skip_bitmaps[0].contains(delete_sign_uid));
    EXPECT_FALSE(persisted_skip_bitmaps[1].contains(value_uid));
    EXPECT_TRUE(persisted_skip_bitmaps[1].contains(delete_sign_uid));

    auto beta_rowset = std::dynamic_pointer_cast<BetaRowset>(output);
    ASSERT_NE(beta_rowset, nullptr);
    std::vector<segment_v2::SegmentSharedPtr> segments;
    ASSERT_TRUE(beta_rowset->load_segments(&segments).ok());
    ASSERT_EQ(segments.size(), 1);
    RowKeyEncoder encoder(*schema, true);
    for (const auto& [row_id, key, sequence] :
         std::vector<std::tuple<uint32_t, int32_t, int32_t>> {{0, 1, 10}, {1, 99, 7}}) {
        std::string persisted_key;
        ASSERT_TRUE(segments[0]->read_key_by_rowid(row_id, &persisted_key).ok());
        EXPECT_EQ(persisted_key, encode_key_with_seq(schema, encoder, key, sequence));
    }
}

TEST_F(FlexiblePartialUpdateTest, VerticalWriterPersistsRowsAcrossSegments) {
    auto schema = create_flexible_mow_schema(/*has_seq=*/true);
    TabletSharedPtr tablet;
    auto history = write_rowset(schema, 3741, 2, {{1, 11, 5, 0}}, &tablet);
    auto mow = make_mow_context(100, {history});
    auto pui = make_flexible_pui(schema);

    const auto value_uid = static_cast<uint64_t>(schema->column(1).unique_id());
    const auto delete_sign_uid = static_cast<uint64_t>(schema->column(3).unique_id());
    auto make_block = [&](const std::vector<std::tuple<int32_t, int32_t, int32_t>>& rows,
                          bool skip_value = false) {
        Block block = schema->create_storage_block();
        {
            auto guard = block.mutate_columns_scoped();
            auto& columns = guard.mutable_columns();
            for (const auto& [key, value, sequence] : rows) {
                const int8_t delete_sign = 0;
                columns[0]->insert_data(reinterpret_cast<const char*>(&key), sizeof(key));
                columns[1]->insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
                columns[2]->insert_data(reinterpret_cast<const char*>(&sequence), sizeof(sequence));
                columns[3]->insert_data(reinterpret_cast<const char*>(&delete_sign),
                                        sizeof(delete_sign));
                BitmapValue skip;
                if (skip_value) {
                    skip.add(value_uid);
                }
                skip.add(delete_sign_uid);
                assert_cast<ColumnBitmap*>(columns[4].get())->insert_value(std::move(skip));
            }
        }
        return block;
    };
    Block first_segment = make_block({{1, 999, 10}}, true);
    Block second_segment = make_block({{2, 22, 7}});
    Block third_segment = make_block({{3, 33, 8}});

    const bool saved_correctness_check = config::enable_merge_on_write_correctness_check;
    config::enable_merge_on_write_correctness_check = false;
    RowsetSharedPtr output;
    int64_t writer_num_rows = 0;
    const auto flush_status = flush_partial_rowset_segments(
            schema, 3742, 3, tablet, mow, pui, {&first_segment, &second_segment, &third_segment},
            &output, nullptr, &writer_num_rows);
    config::enable_merge_on_write_correctness_check = saved_correctness_check;
    ASSERT_TRUE(flush_status.ok()) << flush_status;
    ASSERT_NE(output, nullptr);
    EXPECT_EQ(output->rowset_meta()->num_rows(), 3);
    EXPECT_EQ(writer_num_rows, 3);

    auto beta_rowset = std::dynamic_pointer_cast<BetaRowset>(output);
    ASSERT_NE(beta_rowset, nullptr);
    std::vector<segment_v2::SegmentSharedPtr> segments;
    ASSERT_TRUE(beta_rowset->load_segments(&segments).ok());
    ASSERT_EQ(segments.size(), 3);
    size_t persisted_segment_rows = 0;
    for (const auto& segment : segments) {
        persisted_segment_rows += segment->num_rows();
    }
    EXPECT_EQ(persisted_segment_rows, 3);

    Block persisted;
    const auto read_status = read_rowset(output, schema, &persisted);
    ASSERT_TRUE(read_status.ok()) << read_status;
    ASSERT_EQ(persisted.rows(), 3);
    ASSERT_EQ(persisted.columns(), schema->num_columns());
    const auto& persisted_skip_bitmaps =
            assert_cast<const ColumnBitmap&>(*persisted.get_by_position(4).column).get_data();
    const std::map<int32_t, int32_t> expected_sequence {{1, 10}, {2, 7}, {3, 8}};
    for (size_t row = 0; row < persisted.rows(); ++row) {
        const int32_t key = read_int(persisted, 0, row);
        EXPECT_EQ(key, static_cast<int32_t>(row + 1));
        EXPECT_EQ(read_int(persisted, 1, row), key * 11);
        ASSERT_TRUE(expected_sequence.contains(key));
        EXPECT_EQ(read_int(persisted, 2, row), expected_sequence.at(key));
        EXPECT_EQ(read_tinyint(persisted, 3, row), 0);
        EXPECT_EQ(persisted_skip_bitmaps[row].contains(value_uid), row == 0);
        EXPECT_TRUE(persisted_skip_bitmaps[row].contains(delete_sign_uid));
    }

    // Probe the rowset again as partial-update history. SegmentLoader opens every segment PK index
    // before finding key 2 and restoring its skipped value.
    auto next_mow = make_mow_context(101, {output});
    auto next_pui = make_flexible_pui(schema);
    RowsetId next_rsid;
    next_rsid.init(3743);
    RowsetWriterContext next_rwc;
    fill_rowset_ctx(&next_rwc, schema, tablet, next_pui, next_rsid);
    TransformExecContext next_ctx =
            make_exec_ctx(schema, tablet, next_mow, next_pui, &next_rwc, next_rsid);
    Block next_update = make_block({{2, 999, 9}}, true);
    ASSERT_TRUE(run_flexible_fill(next_ctx, &next_update).ok());
    ASSERT_EQ(next_update.rows(), 1);
    EXPECT_EQ(read_int(next_update, 0, 0), 2);
    EXPECT_EQ(read_int(next_update, 1, 0), 22);
    EXPECT_EQ(read_int(next_update, 2, 0), 9);
}

} // namespace doris
