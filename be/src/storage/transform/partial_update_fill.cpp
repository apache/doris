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

#include "storage/transform/partial_update_fill.h"

#include <algorithm>
#include <optional>
#include <span>

#include "common/cast_set.h"
#include "common/config.h"
#include "core/block/block.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/key/row_key_encoder.h"
#include "storage/mow/historical_row_fetcher.h"
#include "storage/mow/key_probe.h"
#include "storage/partial_update_info.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/segment_loader.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/transform/transform_util.h"
#include "util/debug_points.h"

namespace doris::segment_v2 {

namespace {

// Re-adds the merge-on-write sentinel mark to the delete bitmap, when the
// correctness check is on.
void maybe_add_sentinel_mark(TransformExecContext& ctx) {
    if (config::enable_merge_on_write_correctness_check) {
        ctx.tablet->add_sentinel_mark_to_delete_bitmap(ctx.mow_context->delete_bitmap.get(),
                                                       *ctx.mow_context->rowset_ids);
    }
}

// The probe both fill stages use, with the writer mark policy (mark the old row
// on FOUND, self-mark the new row on FOUND_NEWER). They differ only in
// `skip_in_load_deleted`: flexible treats the incoming row as new when
// the old row was already TEMP-deleted earlier in this load (insert-after-delete).
MowKeyProbe make_partial_update_key_probe(TransformExecContext& ctx,
                                          const TabletSchemaSPtr& tablet_schema,
                                          bool skip_in_load_deleted) {
    return MowKeyProbe(ctx.tablet.get(), tablet_schema.get(), tablet_schema->has_sequence_col(),
                       ctx.mow_context, ctx.rowset_id, cast_set<uint32_t>(ctx.segment_id),
                       MowKeyProbe::Policy {
                               .delete_bitmap_mode = MowKeyProbe::DeleteBitmapMode::OLD_AND_NEW_ROW,
                               .skip_delete_sign = true,
                               .skip_seq_loses = true,
                               .skip_in_load_deleted = skip_in_load_deleted,
                       });
}

// Shared probe + read-plan loop for both fill stages. For each row: encode the
// key (with seq suffix if present), probe the load's rowset snapshot, register a
// brand-new key on a miss, then either flag it for default fill or plan a
// historical read.
//
// `skip_bitmaps != nullptr` chooses flexible mode: the seq/delete-sign for each
// row come from the skip bitmap and the read plan is per cell. `nullptr` is
// fixed mode: block-level `seq_column` and whole-row reads.
Status probe_and_plan(TransformExecContext& ctx, RowKeyEncoder& key_encoder, MowKeyProbe& probe,
                      HistoricalRowFetcher& fetcher,
                      const std::vector<RowsetSharedPtr>& specified_rowsets,
                      std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches,
                      const std::vector<IOlapColumnDataAccessor*>& key_columns,
                      IOlapColumnDataAccessor* seq_column,
                      std::optional<std::span<const Int8>> delete_signs, size_t num_rows,
                      Block* block, std::vector<BitmapValue>* skip_bitmaps,
                      std::vector<bool>& use_default_or_null_flag, bool& has_default_or_nullable) {
    const TabletSchema& schema = *ctx.tablet_schema;
    PartialUpdateInfo& info = *ctx.partial_update_info;
    const bool flexible = (skip_bitmaps != nullptr);
    const bool schema_has_seq = schema.has_sequence_col();
    const bool fixed_has_input_seq = (seq_column != nullptr);
    const int32_t seq_col_unique_id = (flexible && schema_has_seq)
                                              ? schema.column(schema.sequence_col_idx()).unique_id()
                                              : -1;
    const int32_t delete_sign_col_unique_id =
            flexible ? schema.column(schema.delete_sign_idx()).unique_id() : -1;

    use_default_or_null_flag.reserve(num_rows);
    for (size_t pos = 0; pos < num_rows; ++pos) {
        // one block == one fresh segment: segment_pos == block row index
        const bool row_has_seq =
                flexible ? (schema_has_seq && !skip_bitmaps->at(pos).contains(seq_col_unique_id))
                         : fixed_has_input_seq;
        std::string key = encode_mow_key_invalidate_cache(key_encoder, key_columns, seq_column, pos,
                                                          row_has_seq, ctx.rowset_ctx->tablet_id,
                                                          schema, ctx.write_type);
        const bool have_delete_sign =
                flexible ? (!skip_bitmaps->at(pos).contains(delete_sign_col_unique_id) &&
                            (*delete_signs)[pos] != 0)
                         : (delete_signs && (*delete_signs)[pos] != 0);
        ProbeOutcome out =
                DORIS_TRY(probe.probe(key, /*segment_pos=*/pos, row_has_seq, have_delete_sign,
                                      specified_rowsets, segment_caches, ctx.partial_update_stats));
        if (out.result == KeyProbeResult::NOT_FOUND && !have_delete_sign) {
            RETURN_IF_ERROR(info.handle_new_key(
                    schema,
                    [&]() -> std::string {
                        return block->dump_one_line(pos, cast_set<int>(schema.num_key_columns()));
                    },
                    flexible ? &skip_bitmaps->at(pos) : nullptr));
        }
        has_default_or_nullable |= out.use_default_or_null;
        use_default_or_null_flag.emplace_back(out.use_default_or_null);
        if (!out.use_default_or_null) {
            fetcher.pin_rowset(out.rowset);
            if (flexible) {
                fetcher.plan_flexible_read(out.loc, pos, skip_bitmaps->at(pos));
            } else {
                fetcher.plan_fixed_read(out.loc, pos);
            }
        }
    }
    CHECK_EQ(use_default_or_null_flag.size(), num_rows);
    return Status::OK();
}

} // namespace

Status FixedPartialUpdateFillStage::apply(TransformExecContext& ctx, Block* block) const {
    DBUG_EXECUTE_IF("_append_block_with_partial_content.block", DBUG_BLOCK);

    const TabletSchemaSPtr& tablet_schema = ctx.tablet_schema;
    const TabletSchema& schema = *tablet_schema;
    auto& info = *ctx.partial_update_info;
    const size_t num_rows = block->rows();

    // 1. widen the narrow input to the full schema. The input also keeps any
    //    generated auto-inc column at the tail, which fill_missing_columns()
    //    reads from `block` by name.
    const auto& update_cids = info.update_cids;
    Block full_block = widen_partial_update_block(schema, update_cids, *block);

    // 2. key-only conversion with stage-local encoder + convertor
    RowKeyEncoder key_encoder(schema, /*mow=*/true);
    // FE forbids partial update on mow tables with cluster keys; everything
    // below assumes sort keys == schema keys
    DCHECK_EQ(key_encoder.num_sort_key_columns(), schema.num_key_columns());
    OlapBlockDataConvertor convertor;
    convertor.resize(schema.num_columns());
    std::vector<IOlapColumnDataAccessor*> key_columns;
    RETURN_IF_ERROR(convert_key_columns(convertor, schema, full_block, num_rows, key_columns));
    IOlapColumnDataAccessor* seq_column = nullptr;
    if (schema.has_sequence_col()) {
        const auto seq_cid = cast_set<uint32_t>(schema.sequence_col_idx());
        const bool have_input_seq_column =
                std::find(update_cids.begin(), update_cids.end(), seq_cid) != update_cids.end();
        if (have_input_seq_column) {
            RETURN_IF_ERROR(convert_seq_column(convertor, schema, full_block, seq_cid, num_rows,
                                               seq_column));
        }
    }

    // 3. probe every key against the load's rowset snapshot
    DBUG_EXECUTE_IF("VerticalSegmentWriter._append_block_with_partial_content.sleep",
                    { sleep(60); })
    const std::vector<RowsetSharedPtr>& specified_rowsets = ctx.mow_context->rowset_ptrs;
    std::vector<std::unique_ptr<SegmentCacheHandle>> segment_caches(specified_rowsets.size());

    MowKeyProbe probe = make_partial_update_key_probe(ctx, tablet_schema,
                                                      /*skip_in_load_deleted=*/false);
    HistoricalRowFetcher fetcher(ctx.rowset_ctx->make_historical_row_retriever_context());

    // probe_and_plan is shared with the flexible stage and always sets this, but
    // the fixed fill below ignores it -- only the flexible fill reads it.
    bool has_default_or_nullable = false;
    std::vector<bool> use_default_or_null_flag;
    const auto* delete_signs = BaseTablet::get_delete_sign_column_data(full_block, num_rows);
    std::optional<std::span<const Int8>> delete_signs_view;
    if (delete_signs != nullptr) {
        delete_signs_view = std::span<const Int8>(delete_signs, num_rows);
    }
    RETURN_IF_ERROR(probe_and_plan(ctx, key_encoder, probe, fetcher, specified_rowsets,
                                   segment_caches, key_columns, seq_column, delete_signs_view,
                                   num_rows, block, /*skip_bitmaps=*/nullptr,
                                   use_default_or_null_flag, has_default_or_nullable));

    maybe_add_sentinel_mark(ctx);

    // 4. read history / defaults into the missing columns
    RETURN_IF_ERROR(fetcher.fill_missing_columns(schema, full_block, use_default_or_null_flag,
                                                 has_default_or_nullable,
                                                 /*segment_start_pos=*/0, block));

    // 5. swap in the full-width block; downstream it looks like a plain upsert
    block->swap(full_block);
    return Status::OK();
}

Status FlexiblePartialUpdateFillStage::apply(TransformExecContext& ctx, Block* block) const {
    const TabletSchemaSPtr& tablet_schema = ctx.tablet_schema;
    TabletSchema& schema = *tablet_schema;
    auto& info = *ctx.partial_update_info;

    DCHECK(block->columns() == schema.num_columns());
    DCHECK(schema.has_skip_bitmap_col());
    auto skip_bitmap_col_idx = schema.skip_bitmap_col_idx();

    Block full_block = schema.create_block();

    DBUG_EXECUTE_IF("VerticalSegmentWriter._append_block_with_flexible_partial_content.sleep",
                    { sleep(60); })
    const std::vector<RowsetSharedPtr>& specified_rowsets = ctx.mow_context->rowset_ptrs;
    std::vector<std::unique_ptr<SegmentCacheHandle>> segment_caches(specified_rowsets.size());

    // encoder shared with the aggregator, which owns the conversion code
    RowKeyEncoder key_encoder(schema, /*mow=*/true);
    DCHECK_EQ(key_encoder.num_sort_key_columns(), schema.num_key_columns());

    MowKeyProbe probe = make_partial_update_key_probe(ctx, tablet_schema,
                                                      /*skip_in_load_deleted=*/true);
    HistoricalRowFetcher fetcher(ctx.rowset_ctx->make_historical_row_retriever_context());
    BlockAggregator aggregator(schema, ctx.tablet, ctx.mow_context, info, key_encoder, probe,
                               fetcher);

    // 1. aggregate duplicate keys inside the block; the row set may shrink
    size_t num_rows = block->rows();
    RETURN_IF_ERROR(aggregator.aggregate_for_flexible_partial_update(
            block, num_rows, specified_rowsets, segment_caches));
    num_rows = block->rows();

    // 2. encode primary key columns + sequence column
    std::vector<IOlapColumnDataAccessor*> key_columns;
    RETURN_IF_ERROR(aggregator.convert_pk_columns(block, 0, num_rows, key_columns));
    IOlapColumnDataAccessor* seq_column = nullptr;
    RETURN_IF_ERROR(aggregator.convert_seq_column(block, 0, num_rows, seq_column));

    std::vector<BitmapValue>* skip_bitmaps =
            &get_mutable_skip_bitmap_column(block, skip_bitmap_col_idx)->get_data();
    const auto* delete_signs = BaseTablet::get_delete_sign_column_data(*block, num_rows);
    DCHECK(delete_signs != nullptr);
    std::optional<std::span<const Int8>> delete_signs_view;
    if (delete_signs != nullptr) {
        delete_signs_view = std::span<const Int8>(delete_signs, num_rows);
    }

    for (size_t cid = 0; cid < schema.num_key_columns(); ++cid) {
        full_block.replace_by_position(cid, block->get_by_position(cid).column);
    }

    // 3. probe + plan
    bool has_default_or_nullable = false;
    std::vector<bool> use_default_or_null_flag;
    RETURN_IF_ERROR(probe_and_plan(ctx, key_encoder, probe, fetcher, specified_rowsets,
                                   segment_caches, key_columns, seq_column, delete_signs_view,
                                   num_rows, block, skip_bitmaps, use_default_or_null_flag,
                                   has_default_or_nullable));

    maybe_add_sentinel_mark(ctx);

    // 4. fill all non-primary-key columns one cell at a time, as marked by the
    //    skip bitmap
    RETURN_IF_ERROR(fetcher.fill_non_primary_key_columns(
            schema, full_block, use_default_or_null_flag, has_default_or_nullable,
            /*segment_start_pos=*/0, /*block_start_pos=*/0, block, skip_bitmaps));
    // TODO(bobhan1): should we replace the skip bitmap column with empty bitmaps to reduce
    // storage occupation? this column is not needed in read path for merge-on-write table

    // 5. output: final full-width block replaces the input
    block->swap(full_block);
    return Status::OK();
}

} // namespace doris::segment_v2
