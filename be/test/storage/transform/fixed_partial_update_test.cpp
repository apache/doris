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

// Blueprint-aligned, full-branch-coverage tests for the FIXED partial-update
// fill path and the MowKeyProbe branches it drives.
//
// Design / branch map: dev-docs/transform-chain-test-design.md §3 (固定列更新 +
// MowKeyProbe). Each TEST_F below names the §3 branch ids (B1-B14) it pins. The
// fill path is FixedPartialUpdateFillStage::apply -> probe_and_plan ->
// MowKeyProbe::probe -> FixedReadPlan::{read_columns_by_plan,fill_missing_columns}.
//
// Oracle values are PORTED from partial_update_fill_test.cpp; the weak
// `cardinality() >= N` assertions there are upgraded here to exact delete-bitmap
// membership + exact cardinality + exact PartialUpdateStats.

#include <gtest/gtest.h>

#include <tuple>
#include <utility>
#include <vector>

#include "common/config.h"
#include "core/column/column_string.h"
#include "storage/mow/mow_transform_test_base.h"
#include "storage/partial_update_info.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/transform/block_transform.h"
#include "storage/transform/partial_update_fill.h"

namespace doris {

using segment_v2::FixedPartialUpdateFillStage;
using segment_v2::TransformExecContext;

class FixedPartialUpdateTest : public MowTransformTestBase {
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

    // Runs the fixed-PU fill stage with the correctness sentinel disabled (it
    // requires real visible rowsets), restoring the config afterwards.
    static Status run_stage(TransformExecContext& ctx, Block* block) {
        auto saved = config::enable_merge_on_write_correctness_check;
        config::enable_merge_on_write_correctness_check = false;
        FixedPartialUpdateFillStage stage;
        auto st = stage.apply(ctx, block);
        config::enable_merge_on_write_correctness_check = saved;
        return st;
    }

    // True iff the delete bitmap has `row_id` marked under the TEMP version for
    // {rowset_id, segment_id}.
    static bool marked(const std::shared_ptr<MowContext>& mow, const RowsetId& rsid,
                       uint32_t segment_id, uint32_t row_id) {
        return mow->delete_bitmap->contains({rsid, segment_id, DeleteBitmap::TEMP_VERSION_COMMON},
                                            row_id);
    }
};

// B1/B2/B11/B13: existing keys take old `v` from history (column store), the new
// key takes the column default; old rows of the two existing keys are marked.
TEST_F(FixedPartialUpdateTest, FixedFillFromHistoryAndDefault) {
    auto schema = create_mow_schema(/*has_seq=*/false); // k(0) v(1) delete_sign(2)
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3001, 2, {{1, 11}, {2, 22}, {3, 33}}, &tablet);
    auto mow = make_mow_context(100, {rowset});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k"}, false, 0, 0, "UTC", "")
                        .ok());

    RowsetId new_rsid;
    new_rsid.init(3002);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    // narrow input: only the key column, rows = existing 1, 3 and brand-new 99
    Block block = schema->create_block_by_cids({0});
    IColumn* kc = block.get_by_position(0).column->assert_mutable().get();
    for (int32_t k : {1, 3, 99}) {
        kc->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
    }

    auto st = run_stage(ctx, &block);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.columns(), schema->num_columns()); // expanded to the full schema
    ASSERT_EQ(block.rows(), 3);
    // keys kept in input order
    EXPECT_EQ(read_int(block, 0, 0), 1);
    EXPECT_EQ(read_int(block, 0, 1), 3);
    EXPECT_EQ(read_int(block, 0, 2), 99);
    // v: old value for existing keys, default 0 for the brand-new key
    EXPECT_EQ(read_int(block, 1, 0), 11);
    EXPECT_EQ(read_int(block, 1, 1), 33);
    EXPECT_EQ(read_int(block, 1, 2), 0);
    EXPECT_FALSE(read_is_null(block, 1, 0));
    EXPECT_FALSE(read_is_null(block, 1, 2)); // a default-0 cell is a real 0, not NULL

    // exact marks: old rows for keys 1 (history rid0) and 3 (history rid2) only;
    // key 99 was brand-new so nothing is marked for it.
    const RowsetId hist = rowset->rowset_id();
    EXPECT_TRUE(marked(mow, hist, 0, 0));  // key 1
    EXPECT_TRUE(marked(mow, hist, 0, 2));  // key 3
    EXPECT_FALSE(marked(mow, hist, 0, 1)); // key 2 untouched by this load
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 2U);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_updated, 2);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_new_added, 1);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_deleted, 0);
}

// B2/B11/B13: with a seq column, incoming seq (10) > history seq (5) -> FOUND,
// new wins; missing v read from history, old row marked.
TEST_F(FixedPartialUpdateTest, FixedSeqColumnHigherSeqWins) {
    auto schema = create_mow_schema(/*has_seq=*/true); // k(0) v(1) seq(2) delete_sign(3)
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3201, 2, {{1, 11, 5, 0}}, &tablet); // key 1: v=11, seq=5
    auto mow = make_mow_context(100, {rowset});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k", SEQUENCE_COL}, false, 0, 0,
                          "UTC", "")
                        .ok());
    RowsetId new_rsid;
    new_rsid.init(3202);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    // narrow input {k, seq} in update_cids order: key 1 with seq 10 > old seq 5
    Block block = schema->create_block_by_cids({0, 2});
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 1;
        int32_t seq = 10;
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&seq), sizeof(int32_t));
    }

    auto st = run_stage(ctx, &block);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(read_int(block, 0, 0), 1);  // key
    EXPECT_EQ(read_int(block, 1, 0), 11); // missing v read from history (incoming seq wins)
    EXPECT_EQ(read_int(block, 2, 0), 10); // provided seq kept

    // FOUND -> old row (history rid0) marked, treated as an update.
    EXPECT_TRUE(marked(mow, rowset->rowset_id(), 0, 0));
    EXPECT_FALSE(marked(mow, new_rsid, 0, 0)); // not a self-mark
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 1U);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_updated, 1);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_deleted, 0);
}

// B2 boundary: incoming seq == history seq -> segment lookup returns OK (not
// KEY_ALREADY_EXISTS), so FOUND/new-wins; v read from history, old row marked.
TEST_F(FixedPartialUpdateTest, FixedSeqColumnEqualSeqNewWins) {
    auto schema = create_mow_schema(/*has_seq=*/true);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3271, 2, {{2, 22, 10, 0}}, &tablet); // key 2: v=22, seq=10
    auto mow = make_mow_context(100, {rowset});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k", SEQUENCE_COL}, false, 0, 0,
                          "UTC", "")
                        .ok());
    RowsetId new_rsid;
    new_rsid.init(3272);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    Block block = schema->create_block_by_cids({0, 2});
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 2;
        int32_t seq = 10; // == history seq -> new still wins
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&seq), sizeof(int32_t));
    }

    auto st = run_stage(ctx, &block);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(read_int(block, 1, 0), 22); // equal-seq is FOUND -> read history, not default
    EXPECT_EQ(read_int(block, 2, 0), 10);
    EXPECT_TRUE(marked(mow, rowset->rowset_id(), 0, 0)); // old row marked (update)
    EXPECT_FALSE(marked(mow, new_rsid, 0, 0));           // not a self-mark
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 1U);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_updated, 1);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_deleted, 0);
}

// B3/B4/B8: incoming seq (5) < history seq (10) -> FOUND_NEWER, the losing NEW
// row self-marks {new_rsid, seg_id, segment_pos}, v falls back to default.
TEST_F(FixedPartialUpdateTest, FixedSeqColumnLowerSeqLoses) {
    auto schema = create_mow_schema(/*has_seq=*/true);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3211, 2, {{1, 11, 10, 0}}, &tablet); // key 1: v=11, seq=10
    auto mow = make_mow_context(100, {rowset});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k", SEQUENCE_COL}, false, 0, 0,
                          "UTC", "")
                        .ok());
    RowsetId new_rsid;
    new_rsid.init(3212);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    Block block = schema->create_block_by_cids({0, 2});
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 1;
        int32_t seq = 5; // < old seq 10 -> incoming loses
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&seq), sizeof(int32_t));
    }

    auto st = run_stage(ctx, &block);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(read_int(block, 1, 0), 0); // v = default (incoming loses, no history read)
    EXPECT_EQ(read_int(block, 2, 0), 5); // provided seq kept

    // B8 self-mark semantics: on FOUND_NEWER the NEW row at its own segment_pos
    // (row 0 of the writing segment) is marked -- the history old row is NOT.
    EXPECT_TRUE(marked(mow, new_rsid, 0, 0));             // losing new row self-marked
    EXPECT_FALSE(marked(mow, rowset->rowset_id(), 0, 0)); // history old row untouched
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 1U);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_deleted, 1);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_updated, 0);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_new_added, 0);
}

// B5: delete on an existing key WITHOUT a seq column -> delete_sign_skip=true ->
// the old row is NOT read (v=default), but the old row is still marked.
TEST_F(FixedPartialUpdateTest, FixedDeleteSignExistingKey) {
    auto schema = create_mow_schema(/*has_seq=*/false); // k(0) v(1) delete_sign(2)
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3221, 2, {{1, 11}}, &tablet); // key 1: v=11
    auto mow = make_mow_context(100, {rowset});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k", DELETE_SIGN}, false, 0, 0,
                          "UTC", "")
                        .ok());
    RowsetId new_rsid;
    new_rsid.init(3222);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    // narrow input {k, delete_sign}: delete the existing key 1
    Block block = schema->create_block_by_cids({0, 2});
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 1;
        int8_t ds = 1;
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&ds), sizeof(int8_t));
    }

    auto st = run_stage(ctx, &block);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(read_int(block, 0, 0), 1);     // key
    EXPECT_EQ(read_int(block, 1, 0), 0);     // v default (no-seq table: old row not read)
    EXPECT_EQ(read_tinyint(block, 2, 0), 1); // delete sign kept

    // FOUND-but-default still marks the OLD row (it is an update, not a self-mark).
    EXPECT_TRUE(marked(mow, rowset->rowset_id(), 0, 0));
    EXPECT_FALSE(marked(mow, new_rsid, 0, 0));
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 1U);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_updated, 1);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_deleted, 0);
}

// B7: delete on an existing key WITH a seq column -> the !_has_sequence_col guard
// makes delete_sign_skip=false, so the old row IS read (v=history, not default);
// contrast FixedDeleteSignExistingKey above which fills the default. History v
// must differ from the default 0 for this to be observable.
TEST_F(FixedPartialUpdateTest, FixedDeleteSignSeqTableReadsOldRow) {
    auto schema = create_mow_schema(/*has_seq=*/true); // k(0) v(1) seq(2) delete_sign(3)
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3281, 2, {{1, 11, 7, 0}}, &tablet); // key 1: v=11, seq=7
    auto mow = make_mow_context(100, {rowset});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k", SEQUENCE_COL, DELETE_SIGN},
                          false, 0, 0, "UTC", "")
                        .ok());
    RowsetId new_rsid;
    new_rsid.init(3282);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    // narrow input {k, seq, delete_sign} in cid order: delete key 1 with seq 9 (>= old 7
    // so FOUND, not FOUND_NEWER).
    Block block = schema->create_block_by_cids({0, 2, 3});
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 1;
        int32_t seq = 9;
        int8_t ds = 1;
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&seq), sizeof(int32_t));
        cols[2]->insert_data(reinterpret_cast<const char*>(&ds), sizeof(int8_t));
    }

    auto st = run_stage(ctx, &block);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(read_int(block, 0, 0), 1);     // key 1
    EXPECT_EQ(read_int(block, 1, 0), 11);    // v = HISTORY (seq-table guard reads old row)
    EXPECT_FALSE(read_is_null(block, 1, 0)); // a real read value, not NULL
    EXPECT_EQ(read_int(block, 2, 0), 9);     // provided seq kept
    EXPECT_EQ(read_tinyint(block, 3, 0), 1); // delete sign kept

    EXPECT_TRUE(marked(mow, rowset->rowset_id(), 0, 0)); // old row marked (FOUND update)
    EXPECT_FALSE(marked(mow, new_rsid, 0, 0));
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 1U);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_updated, 1);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_deleted, 0);
}

// B1/B12: delete on a brand-new key -> handle_new_key short-circuited for a
// delete (ERROR policy must NOT fire), tombstone kept with defaults.
TEST_F(FixedPartialUpdateTest, FixedDeleteSignNewKey) {
    auto schema = create_mow_schema(/*has_seq=*/false);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3231, 2, {{1, 11}}, &tablet); // only key 1 exists
    auto mow = make_mow_context(100, {rowset});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::ERROR, {"k", DELETE_SIGN}, false, 0, 0,
                          "UTC", "")
                        .ok());
    RowsetId new_rsid;
    new_rsid.init(3232);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    // delete a key that is not in the table; ERROR policy must NOT fire for a delete
    Block block = schema->create_block_by_cids({0, 2});
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 99;
        int8_t ds = 1;
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&ds), sizeof(int8_t));
    }

    auto st = run_stage(ctx, &block);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(read_int(block, 0, 0), 99);    // key kept
    EXPECT_EQ(read_int(block, 1, 0), 0);     // v default (no old row)
    EXPECT_EQ(read_tinyint(block, 2, 0), 1); // delete sign kept

    // NOT_FOUND -> nothing marked; it is counted as a new add even for a delete.
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 0U);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_new_added, 1);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_updated, 0);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_deleted, 0);
}

// B11: a provided value column is kept as-is; only columns not in the update list
// are filled from history.
TEST_F(FixedPartialUpdateTest, FixedProvidedValueKept) {
    auto schema = create_mow_schema(/*has_seq=*/false);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3241, 2, {{1, 11}}, &tablet); // history v=11
    auto mow = make_mow_context(100, {rowset});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k", "v"}, false, 0, 0, "UTC", "")
                        .ok());
    RowsetId new_rsid;
    new_rsid.init(3242);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    // narrow input {k, v}: provide a new v for the existing key 1
    Block block = schema->create_block_by_cids({0, 1});
    {
        auto guard = block.mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        int32_t k = 1;
        int32_t v = 88;
        cols[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        cols[1]->insert_data(reinterpret_cast<const char*>(&v), sizeof(int32_t));
    }

    auto st = run_stage(ctx, &block);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 1);
    EXPECT_EQ(read_int(block, 1, 0), 88); // provided v kept, not the history value 11

    EXPECT_TRUE(marked(mow, rowset->rowset_id(), 0, 0)); // old row marked
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 1U);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_updated, 1);
}

// B9: ERROR new-key policy rejects a brand-new (non-delete) key.
TEST_F(FixedPartialUpdateTest, FixedNewKeyErrorPolicyRejected) {
    auto schema = create_mow_schema(/*has_seq=*/false);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3251, 2, {{1, 11}}, &tablet);
    auto mow = make_mow_context(100, {rowset});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::ERROR, {"k"}, false, 0, 0, "UTC", "")
                        .ok());
    RowsetId new_rsid;
    new_rsid.init(3252);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    Block block = schema->create_block_by_cids({0});
    int32_t k = 99; // not in the table
    block.get_by_position(0).column->assert_mutable()->insert_data(
            reinterpret_cast<const char*>(&k), sizeof(int32_t));

    auto st = run_stage(ctx, &block);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("Can't append new rows"), std::string::npos) << st;
}

// B10: APPEND policy, a brand-new key whose not-in-update-list column is NOT NULL
// and has no default cannot be inserted -> rejected.
TEST_F(FixedPartialUpdateTest, FixedNewKeyAppendRequiredColumnMissing) {
    auto schema =
            create_mow_schema_required_value(); // k(0) v(1) NOT NULL no-default, delete_sign(2)
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3261, 2, {{1, 11}}, &tablet);
    auto mow = make_mow_context(100, {rowset});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k"}, false, 0, 0, "UTC", "")
                        .ok());
    RowsetId new_rsid;
    new_rsid.init(3262);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    Block block = schema->create_block_by_cids({0});
    int32_t k = 99; // brand-new key; v is not in the update list, NOT NULL, no default
    block.get_by_position(0).column->assert_mutable()->insert_data(
            reinterpret_cast<const char*>(&k), sizeof(int32_t));

    auto st = run_stage(ctx, &block);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("should have default value or be nullable"), std::string::npos)
            << st;
}

// B14: row-store schema -> read_columns_by_plan takes the
// fetch_value_through_row_column path. The existing key's missing `v` must be
// restored from the row column (== history 11, != default 0); the new key takes
// the default. Cross-checked against the column-store result.
TEST_F(FixedPartialUpdateTest, FixedRowStoreReadPath) {
    auto schema = create_row_store_schema(); // k(0) v(1) delete_sign(2) __DORIS_ROW_STORE_COL__(3)
    ASSERT_TRUE(schema->has_row_store_for_all_columns());
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 3291, 2, {{1, 11}, {2, 22}}, &tablet);
    auto mow = make_mow_context(100, {rowset});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k"}, false, 0, 0, "UTC", "")
                        .ok());
    RowsetId new_rsid;
    new_rsid.init(3292);
    RowsetWriterContext rwc;
    fill_rowset_ctx(&rwc, schema, tablet, pui, new_rsid);
    TransformExecContext ctx = make_exec_ctx(schema, tablet, mow, pui, &rwc, new_rsid);

    // narrow input {k}: two existing keys -- both read v back through the row
    // column. (No brand-new key here: the non-nullable hidden row-store column
    // cannot be defaulted for a newly inserted row under non-strict append.)
    Block block = schema->create_block_by_cids({0});
    IColumn* kc = block.get_by_position(0).column->assert_mutable().get();
    for (int32_t k : {1, 2}) {
        kc->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
    }

    auto st = run_stage(ctx, &block);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.columns(), schema->num_columns());
    ASSERT_EQ(block.rows(), 2);
    EXPECT_EQ(read_int(block, 0, 0), 1);
    EXPECT_EQ(read_int(block, 0, 1), 2);
    // v for both keys restored through the row column (history 11 / 22 != default 0).
    EXPECT_EQ(read_int(block, 1, 0), 11);
    EXPECT_EQ(read_int(block, 1, 1), 22);
    EXPECT_FALSE(read_is_null(block, 1, 0));
    EXPECT_FALSE(read_is_null(block, 1, 1));
    // delete_sign is a missing cid here too and must round-trip from the row store.
    EXPECT_EQ(read_tinyint(block, 2, 0), 0);
    EXPECT_EQ(read_tinyint(block, 2, 1), 0);

    EXPECT_TRUE(marked(mow, rowset->rowset_id(), 0, 0)); // key 1 old row (rowid 0)
    EXPECT_TRUE(marked(mow, rowset->rowset_id(), 0, 1)); // key 2 old row (rowid 1)
    EXPECT_EQ(mow->delete_bitmap->cardinality(), 2U);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_updated, 2);
    EXPECT_EQ(ctx.partial_update_stats.num_rows_new_added, 0);

    // Cross-check: the same data over a column-store schema yields the same v.
    {
        auto cs_schema = create_mow_schema(/*has_seq=*/false); // k(0) v(1) delete_sign(2)
        ASSERT_FALSE(cs_schema->has_row_store_for_all_columns());
        TabletSharedPtr cs_tablet;
        auto cs_rowset = write_rowset(cs_schema, 3293, 2, {{1, 11}, {2, 22}}, &cs_tablet);
        auto cs_mow = make_mow_context(100, {cs_rowset});
        auto cs_pui = std::make_shared<PartialUpdateInfo>();
        ASSERT_TRUE(cs_pui->init(kTabletId, 1, *cs_schema,
                                 UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                                 PartialUpdateNewRowPolicyPB::APPEND, {"k"}, false, 0, 0, "UTC", "")
                            .ok());
        RowsetId cs_new_rsid;
        cs_new_rsid.init(3294);
        RowsetWriterContext cs_rwc;
        fill_rowset_ctx(&cs_rwc, cs_schema, cs_tablet, cs_pui, cs_new_rsid);
        TransformExecContext cs_ctx =
                make_exec_ctx(cs_schema, cs_tablet, cs_mow, cs_pui, &cs_rwc, cs_new_rsid);

        Block cs_block = cs_schema->create_block_by_cids({0});
        IColumn* cs_kc = cs_block.get_by_position(0).column->assert_mutable().get();
        for (int32_t k : {1, 2}) {
            cs_kc->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));
        }
        auto cs_st = run_stage(cs_ctx, &cs_block);
        ASSERT_TRUE(cs_st.ok()) << cs_st;
        EXPECT_EQ(read_int(cs_block, 1, 0), read_int(block, 1, 0)); // 11 both paths
        EXPECT_EQ(read_int(cs_block, 1, 1), read_int(block, 1, 1)); // 22 both paths
    }
}

// End-to-end writer coverage: the transform chain expands the narrow fixed-update block, the
// generic VerticalSegmentWriter persists it, and RowsetReader must recover the same logical rows.
// Physical column/page order is intentionally not asserted.
TEST_F(FixedPartialUpdateTest, VerticalWriterPersistsFilledRows) {
    auto schema = create_mow_schema(/*has_seq=*/true);
    TabletSharedPtr tablet;
    auto history = write_rowset(schema, 3701, 2, {{1, 11, 5, 0}}, &tablet);
    auto mow = make_mow_context(100, {history});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k", SEQUENCE_COL}, false, 0, 0,
                          "UTC", "")
                        .ok());

    Block block = schema->create_block_by_cids({0, 2});
    {
        auto guard = block.mutate_columns_scoped();
        auto& columns = guard.mutable_columns();
        for (const auto& [key, sequence] :
             std::vector<std::pair<int32_t, int32_t>> {{1, 10}, {99, 7}}) {
            columns[0]->insert_data(reinterpret_cast<const char*>(&key), sizeof(key));
            columns[1]->insert_data(reinterpret_cast<const char*>(&sequence), sizeof(sequence));
        }
    }

    const bool saved_vertical_writer = config::enable_vertical_segment_writer;
    const bool saved_correctness_check = config::enable_merge_on_write_correctness_check;
    config::enable_vertical_segment_writer = true;
    config::enable_merge_on_write_correctness_check = false;
    RowsetSharedPtr output;
    const auto flush_status =
            flush_partial_rowset(schema, 3702, 3, tablet, mow, pui, &block, &output);
    config::enable_vertical_segment_writer = saved_vertical_writer;
    config::enable_merge_on_write_correctness_check = saved_correctness_check;
    ASSERT_TRUE(flush_status.ok()) << flush_status;
    ASSERT_NE(output, nullptr);
    ASSERT_EQ(output->rowset_meta()->num_rows(), 2);

    Block persisted;
    ASSERT_TRUE(read_rowset(output, schema, &persisted).ok());
    ASSERT_EQ(persisted.rows(), 2);
    ASSERT_EQ(persisted.columns(), schema->num_columns());
    EXPECT_EQ(read_int(persisted, 0, 0), 1);
    EXPECT_EQ(read_int(persisted, 1, 0), 11); // restored from history
    EXPECT_EQ(read_int(persisted, 2, 0), 10); // provided sequence
    EXPECT_EQ(read_tinyint(persisted, 3, 0), 0);
    EXPECT_EQ(read_int(persisted, 0, 1), 99);
    EXPECT_EQ(read_int(persisted, 1, 1), 0); // default for a new key
    EXPECT_EQ(read_int(persisted, 2, 1), 7);
    EXPECT_EQ(read_tinyint(persisted, 3, 1), 0);

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

// The hidden RowStore column is streamed through DerivedColumnGenerator rather than the normal
// schema-column loop. Verify that the generic Vertical writer persists exactly the rebuilt cells.
TEST_F(FixedPartialUpdateTest, VerticalWriterPersistsRebuiltRowStore) {
    auto schema = create_row_store_schema();
    TabletSharedPtr tablet;
    auto history = write_rowset(schema, 3731, 2, {{1, 11}, {2, 22}}, &tablet);
    auto mow = make_mow_context(100, {history});

    Block historical_rows;
    ASSERT_TRUE(read_rowset(history, schema, &historical_rows).ok());
    ASSERT_EQ(historical_rows.rows(), 2);

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k"}, false, 0, 0, "UTC", "")
                        .ok());
    Block block = schema->create_block_by_cids({0});
    auto* keys = block.get_by_position(0).column->assert_mutable().get();
    for (const int32_t key : {1, 2}) {
        keys->insert_data(reinterpret_cast<const char*>(&key), sizeof(key));
    }

    const bool saved_vertical_writer = config::enable_vertical_segment_writer;
    const bool saved_correctness_check = config::enable_merge_on_write_correctness_check;
    config::enable_vertical_segment_writer = true;
    config::enable_merge_on_write_correctness_check = false;
    RowsetSharedPtr output;
    const auto flush_status =
            flush_partial_rowset(schema, 3732, 3, tablet, mow, pui, &block, &output);
    config::enable_vertical_segment_writer = saved_vertical_writer;
    config::enable_merge_on_write_correctness_check = saved_correctness_check;
    ASSERT_TRUE(flush_status.ok()) << flush_status;
    ASSERT_NE(output, nullptr);

    Block persisted;
    ASSERT_TRUE(read_rowset(output, schema, &persisted).ok());
    ASSERT_EQ(persisted.rows(), 2);
    const auto& historical_row_store =
            assert_cast<const ColumnString&>(*historical_rows.get_by_position(3).column);
    const auto& persisted_row_store =
            assert_cast<const ColumnString&>(*persisted.get_by_position(3).column);
    for (size_t row = 0; row < 2; ++row) {
        EXPECT_EQ(read_int(persisted, 0, row), static_cast<int32_t>(row + 1));
        EXPECT_EQ(read_int(persisted, 1, row), static_cast<int32_t>((row + 1) * 11));
        EXPECT_EQ(read_tinyint(persisted, 2, row), 0);
        EXPECT_EQ(persisted_row_store.get_data_at(row).to_string(),
                  historical_row_store.get_data_at(row).to_string());
    }
}

} // namespace doris
