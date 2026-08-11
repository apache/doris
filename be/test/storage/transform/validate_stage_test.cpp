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

// build_transform_chain composition per write path, and every ValidateStage
// branch that is reachable through the built chain.

#include <gtest/gtest.h>

#include <memory>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/mow/mow_transform_test_base.h"
#include "storage/partial_update_info.h"
#include "storage/rowset/beta_rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/segment_creator.h"
#include "storage/transform/block_transform.h"

namespace doris {

using segment_v2::build_transform_chain;
using segment_v2::TransformExecContext;

// Own fixture subclass so the TEST_F names here never clash with the other
// files sharing MowTransformTestBase.
class ValidateStageTest : public MowTransformTestBase {
protected:
    // A minimal direct-write rowset context: enough for build_transform_chain to
    // pick the non-binlog, non-compaction stage set.
    RowsetWriterContext direct_rwc(const TabletSchemaSPtr& schema) {
        RowsetWriterContext c;
        c.tablet_schema = schema;
        c.write_type = DataWriteType::TYPE_DIRECT;
        c.enable_unique_key_merge_on_write = true;
        return c;
    }
    // The per-flush exec context the chain runs against. ValidateStage reads
    // write_type / segment_id straight from here and is_transient_rowset_writer
    // through rowset_ctx, so the two must agree with the rwc used to build.
    TransformExecContext exec_ctx(const TabletSchemaSPtr& schema, RowsetWriterContext* rwc,
                                  int32_t segment_id = 0) {
        TransformExecContext ctx;
        ctx.tablet_schema = schema;
        ctx.write_type = rwc->write_type;
        ctx.rowset_ctx = rwc;
        ctx.segment_id = segment_id;
        return ctx;
    }
};

// =============================================================================
// chain composition per write path -- assert the exact ordered stage list.
// =============================================================================

// TYPE_COMPACTION -> empty chain (rows are already final).
TEST_F(ValidateStageTest, CompositionCompactionEmpty) {
    auto schema = create_mow_schema(/*has_seq=*/false);
    RowsetWriterContext c = direct_rwc(schema);
    c.write_type = DataWriteType::TYPE_COMPACTION;
    EXPECT_TRUE(build_transform_chain(c).empty());
    EXPECT_TRUE(build_transform_chain(c).stage_names().empty());
}

// TYPE_DIRECT, no PU -> Validate, RowStoreFill, VariantParse.
TEST_F(ValidateStageTest, CompositionDirectNonPartialUpdate) {
    using V = std::vector<std::string_view>;
    auto schema = create_mow_schema(/*has_seq=*/false);
    EXPECT_EQ(build_transform_chain(direct_rwc(schema)).stage_names(),
              (V {"Validate", "RowStoreFill", "VariantParse"}));
}

// TYPE_SCHEMA_CHANGE, no PU -> same shape as a direct write.
TEST_F(ValidateStageTest, CompositionSchemaChange) {
    using V = std::vector<std::string_view>;
    auto schema = create_mow_schema(/*has_seq=*/false);
    RowsetWriterContext c = direct_rwc(schema);
    c.write_type = DataWriteType::TYPE_SCHEMA_CHANGE;
    EXPECT_EQ(build_transform_chain(c).stage_names(),
              (V {"Validate", "RowStoreFill", "VariantParse"}));
}

// Non-binlog TYPE_DEFAULT does not rebuild the row store: Validate and parse only.
TEST_F(ValidateStageTest, CompositionDefaultOmitsRowStoreFill) {
    using V = std::vector<std::string_view>;
    auto schema = create_mow_schema(/*has_seq=*/false);
    RowsetWriterContext c = direct_rwc(schema);
    c.write_type = DataWriteType::TYPE_DEFAULT;
    EXPECT_EQ(build_transform_chain(c).stage_names(), (V {"Validate", "VariantParse"}));
}

// A transient-rowset-writer PU degrades to the plain direct chain: the PU
// predicate is false because is_transient_rowset_writer is set, so the block
// is treated as a full-width direct write.
TEST_F(ValidateStageTest, CompositionTransientPartialUpdateDegradesToNoFill) {
    using V = std::vector<std::string_view>;
    auto schema = create_mow_schema(/*has_seq=*/false);
    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k"}, false, 0, 0, "UTC", "")
                        .ok());
    RowsetWriterContext c = direct_rwc(schema);
    c.partial_update_info = pui;
    c.is_transient_rowset_writer = true; // degrade: PU predicate becomes false
    EXPECT_EQ(build_transform_chain(c).stage_names(),
              (V {"Validate", "RowStoreFill", "VariantParse"}));
}

// Partial update loads only get validated by the chain for now: the segment
// writers still own the fill, parse and row-store work. The fill stages take
// this slot when they move into the chain.
TEST_F(ValidateStageTest, CompositionPartialUpdateValidateOnly) {
    using V = std::vector<std::string_view>;
    auto schema = create_mow_schema(/*has_seq=*/false);
    auto fixed = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(fixed->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                            PartialUpdateNewRowPolicyPB::APPEND, {"k"}, false, 0, 0, "UTC", "")
                        .ok());
    RowsetWriterContext c = direct_rwc(schema);
    c.partial_update_info = fixed;
    EXPECT_EQ(build_transform_chain(c).stage_names(), (V {"Validate"}));

    auto fschema = create_flexible_mow_schema();
    auto flexible = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(flexible->init(kTabletId, 1, *fschema,
                               UniqueKeyUpdateModePB::UPDATE_FLEXIBLE_COLUMNS,
                               PartialUpdateNewRowPolicyPB::APPEND, {}, false, 0, 0, "UTC", "")
                        .ok());
    RowsetWriterContext fc = direct_rwc(fschema);
    fc.partial_update_info = flexible;
    EXPECT_EQ(build_transform_chain(fc).stage_names(), (V {"Validate"}));
}

// Binlog sub-writers keep deriving inside RowBinlogSegmentWriter for now, so
// their chain stays empty for every write type. The derive stage takes this
// slot when it moves into the chain.
TEST_F(ValidateStageTest, CompositionBinlogEmpty) {
    auto schema = create_mow_schema(/*has_seq=*/false);
    RowsetWriterContext rwc = direct_rwc(schema);
    rwc.write_binlog_opt().enable = true;

    for (auto write_type : {DataWriteType::TYPE_DIRECT, DataWriteType::TYPE_DEFAULT,
                            DataWriteType::TYPE_SCHEMA_CHANGE}) {
        rwc.write_type = write_type;
        EXPECT_TRUE(build_transform_chain(rwc).empty());
    }
}

// =============================================================================
// ValidateStage branches (V1-V9, without V5's fill which is not in the chain
// yet). ValidateStage is the chain's first stage on every non-compaction,
// non-binlog path, so we build the real chain and drive it with a block that
// already fails / passes validate.
// =============================================================================

// V1: non-PU direct, full width (columns == num_columns) -> accepted.
TEST_F(ValidateStageTest, V1_DirectAcceptsGoodWidth) {
    auto schema = create_mow_schema(/*has_seq=*/false);
    RowsetWriterContext c = direct_rwc(schema);
    auto chain = build_transform_chain(c);
    TransformExecContext ctx = exec_ctx(schema, &c);

    Block block = schema->create_block(); // full width, 0 rows
    EXPECT_TRUE(chain.apply(ctx, &block).ok());
}

// V2: non-PU direct, wrong width (columns != num_columns) -> InvalidArgument.
TEST_F(ValidateStageTest, V2_DirectRejectsBadWidth) {
    auto schema = create_mow_schema(/*has_seq=*/false); // 3 columns
    RowsetWriterContext c = direct_rwc(schema);
    auto chain = build_transform_chain(c);
    TransformExecContext ctx = exec_ctx(schema, &c);

    Block block = schema->create_block_by_cids({0}); // 1 column != num_columns(3)
    auto st = chain.apply(ctx, &block);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(st.code(), ErrorCode::INVALID_ARGUMENT) << st;
    EXPECT_NE(st.to_string().find("illegal block columns"), std::string::npos) << st;
}

// V3: PU on a write path with no tablet context -> NotSupported instead of a
// crash inside the probe (e.g. the streaming BetaRowsetWriterV2).
TEST_F(ValidateStageTest, V3_PartialUpdateRejectsNoTabletContext) {
    auto schema = create_mow_schema(/*has_seq=*/false);
    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k"}, false, 0, 0, "UTC", "")
                        .ok());
    RowsetWriterContext rwc = direct_rwc(schema);
    rwc.partial_update_info = pui;
    auto chain = build_transform_chain(rwc);

    TransformExecContext ctx = exec_ctx(schema, &rwc);
    ctx.partial_update_info = pui;
    ctx.tablet = nullptr; // no tablet context
    ctx.mow_context = nullptr;

    Block block = schema->create_block_by_cids({0});
    block.get_by_position(0).column->assert_mutable()->insert_default();
    auto st = chain.apply(ctx, &block);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(st.code(), ErrorCode::NOT_IMPLEMENTED_ERROR) << st;
    EXPECT_NE(st.to_string().find("no tablet context"), std::string::npos) << st;
}

// V4: PU flushed without a segment id (segment_id == -1, the add_block seam) ->
// InternalError naming flush_single_block.
TEST_F(ValidateStageTest, V4_PartialUpdateRejectsWithoutSegmentId) {
    auto schema = create_mow_schema(/*has_seq=*/false);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 8201, 2, {{1, 11}}, &tablet);
    auto mow = make_mow_context(100, {rowset});

    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k"}, false, 0, 0, "UTC", "")
                        .ok());

    RowsetWriterContext rwc = direct_rwc(schema);
    rwc.tablet_id = kTabletId;
    rwc.tablet = tablet;
    rwc.partial_update_info = pui;

    auto chain = build_transform_chain(rwc);
    TransformExecContext ctx;
    ctx.tablet_schema = schema;
    ctx.write_type = DataWriteType::TYPE_DIRECT;
    ctx.tablet = tablet;
    ctx.mow_context = mow;
    ctx.partial_update_info = pui;
    ctx.rowset_ctx = &rwc;
    ctx.segment_id = -1; // add_block seam: no segment id

    Block block = schema->create_block_by_cids({0});
    IColumn* kc = block.get_by_position(0).column->assert_mutable().get();
    int32_t k = 1;
    kc->insert_data(reinterpret_cast<const char*>(&k), sizeof(int32_t));

    auto st = chain.apply(ctx, &block);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(st.code(), ErrorCode::INTERNAL_ERROR) << st;
    EXPECT_NE(st.to_string().find("flush_single_block"), std::string::npos) << st;
}

// V6 + V7: fixed PU rejects both a too-wide block (columns >= num_columns) and a
// too-narrow one (columns < num_key_columns) with the same InvalidArgument.
TEST_F(ValidateStageTest, V6V7_FixedPartialUpdateRejectsBadWidth) {
    auto schema = create_mow_schema(/*has_seq=*/false); // 1 key, 3 cols
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 8401, 2, {{1, 11}}, &tablet);
    auto mow = make_mow_context(100, {rowset});
    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k"}, false, 0, 0, "UTC", "")
                        .ok());
    RowsetId new_rsid;
    new_rsid.init(8402);
    RowsetWriterContext rwc = direct_rwc(schema);
    rwc.tablet_id = kTabletId;
    rwc.tablet = tablet;
    rwc.partial_update_info = pui;
    rwc.rowset_id = new_rsid;
    auto chain = build_transform_chain(rwc);

    auto make_ctx = [&] {
        TransformExecContext ctx;
        ctx.tablet_schema = schema;
        ctx.write_type = DataWriteType::TYPE_DIRECT;
        ctx.tablet = tablet;
        ctx.mow_context = mow;
        ctx.partial_update_info = pui;
        ctx.rowset_ctx = &rwc;
        ctx.rowset_id = new_rsid;
        ctx.segment_id = 0;
        return ctx;
    };

    // V6 too wide: full width (3 == num_columns) is not a partial update block.
    {
        TransformExecContext ctx = make_ctx();
        Block block = schema->create_block();
        auto st = chain.apply(ctx, &block);
        EXPECT_FALSE(st.ok());
        EXPECT_EQ(st.code(), ErrorCode::INVALID_ARGUMENT) << st;
        EXPECT_NE(st.to_string().find("illegal partial update block columns"), std::string::npos)
                << st;
    }
    // V7 too narrow: fewer columns than the key (0 < 1 key column).
    {
        TransformExecContext ctx = make_ctx();
        Block block = schema->create_block_by_cids({});
        auto st = chain.apply(ctx, &block);
        EXPECT_FALSE(st.ok());
        EXPECT_EQ(st.code(), ErrorCode::INVALID_ARGUMENT) << st;
        EXPECT_NE(st.to_string().find("illegal partial update block columns"), std::string::npos)
                << st;
    }
}

// V8: flexible PU requires a full-width block; any width mismatch is rejected.
TEST_F(ValidateStageTest, V8_FlexiblePartialUpdateRejectsBadWidth) {
    auto schema = create_flexible_mow_schema(); // k v delete_sign skip_bitmap: 4 cols
    auto tablet = make_tablet(schema, 8501);
    auto mow = make_mow_context(100, {});
    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FLEXIBLE_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {}, false, 0, 0, "UTC", "")
                        .ok());
    RowsetWriterContext rwc = direct_rwc(schema);
    rwc.tablet = tablet;
    rwc.partial_update_info = pui;
    auto chain = build_transform_chain(rwc);

    TransformExecContext ctx = exec_ctx(schema, &rwc);
    ctx.tablet = tablet;
    ctx.mow_context = mow;
    ctx.partial_update_info = pui;

    Block block = schema->create_block_by_cids({0}); // 1 col != num_columns(4)
    auto st = chain.apply(ctx, &block);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(st.code(), ErrorCode::INVALID_ARGUMENT) << st;
    EXPECT_NE(st.to_string().find("illegal flexible partial update block columns"),
              std::string::npos)
            << st;
}

// V9: a transient PU is validated as a plain direct write (the PU predicate is
// false because is_transient_rowset_writer is set). The full-width block is
// accepted; a narrow one is rejected with the non-PU "illegal block columns"
// error, proving the transient path does NOT use the partial-update validation.
TEST_F(ValidateStageTest, V9_TransientPartialUpdateValidatedAsDirect) {
    auto schema = create_mow_schema(/*has_seq=*/false); // 3 columns
    auto pui = std::make_shared<PartialUpdateInfo>();
    ASSERT_TRUE(pui->init(kTabletId, 1, *schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                          PartialUpdateNewRowPolicyPB::APPEND, {"k"}, false, 0, 0, "UTC", "")
                        .ok());
    RowsetWriterContext rwc = direct_rwc(schema);
    rwc.partial_update_info = pui;
    rwc.is_transient_rowset_writer = true; // degrade to direct
    auto chain = build_transform_chain(rwc);

    // full-width block is accepted as a direct write
    {
        TransformExecContext ctx = exec_ctx(schema, &rwc);
        ctx.partial_update_info = pui;
        Block block = schema->create_block(); // 3 cols == num_columns
        EXPECT_TRUE(chain.apply(ctx, &block).ok());
    }
    // a narrow block is rejected with the non-PU width error -- not the PU one
    {
        TransformExecContext ctx = exec_ctx(schema, &rwc);
        ctx.partial_update_info = pui;
        Block block = schema->create_block_by_cids({0}); // 1 col != num_columns(3)
        auto st = chain.apply(ctx, &block);
        EXPECT_FALSE(st.ok());
        EXPECT_EQ(st.code(), ErrorCode::INVALID_ARGUMENT) << st;
        EXPECT_NE(st.to_string().find("illegal block columns"), std::string::npos) << st;
    }
}

// The flush seam runs the chain before it creates any segment writer: a block
// ValidateStage rejects must surface the chain's error with no segment file
// touched. The context deliberately has no file_writer_creator, so reaching
// writer creation at all would fail very differently from InvalidArgument.
// Today the writers still duplicate this width check; once later changes remove
// those duplicates, this seam is the only guard, so pin it now.
TEST_F(ValidateStageTest, FlushSeamRejectsBeforeCreatingAWriter) {
    auto schema = create_mow_schema(/*has_seq=*/false); // 3 columns
    RowsetWriterContext rwc = direct_rwc(schema);

    SegmentFileCollection segment_files;
    InvertedIndexFileCollection index_files;
    SegmentFlusher flusher(rwc, segment_files, index_files);

    Block block = schema->create_block_by_cids({0}); // 1 column != num_columns(3)
    block.get_by_position(0).column->assert_mutable()->insert_default();
    auto st = flusher.flush_single_block(&block, /*segment_id=*/0);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(st.code(), ErrorCode::INVALID_ARGUMENT) << st;
    EXPECT_NE(st.to_string().find("illegal block columns"), std::string::npos) << st;
}

// The sort-key invariant (num_key_columns >= num_short_key_columns) is the only
// branch a plain unit test cannot exercise: pure DCHECK, aborts a debug build
// and compiles out in release. The "Can only do partial update on merge-on-write
// unique table" branch does return a deterministic InternalError in release,
// but its DCHECK still aborts debug test runs.

} // namespace doris
