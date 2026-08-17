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

#include "common/cast_set.h"
#include "common/config.h"
#include "core/block/block.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/key/row_key_encoder.h"
#include "storage/mow/historical_row_fetcher.h"
#include "storage/mow/key_probe.h"
#include "storage/partial_update_info.h"
#include "storage/row_ttl.h"
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

// The probe + read-plan loop of the fixed fill. For each row: encode the key
// (with seq suffix when the load provides one), probe the load's rowset
// snapshot, register a brand-new key on a miss, then either flag the row for
// default fill or plan a whole-row historical read.
Status probe_and_plan(TransformExecContext& ctx, RowKeyEncoder& key_encoder, MowKeyProbe& probe,
                      HistoricalRowFetcher& fetcher,
                      const std::vector<RowsetSharedPtr>& specified_rowsets,
                      std::vector<std::unique_ptr<SegmentCacheHandle>>& segment_caches,
                      const std::vector<IOlapColumnDataAccessor*>& key_columns,
                      IOlapColumnDataAccessor* seq_column, const signed char* delete_signs,
                      size_t num_rows, Block* block, std::vector<bool>& use_default_or_null_flag,
                      bool& has_default_or_nullable) {
    const TabletSchema& schema = *ctx.tablet_schema;
    PartialUpdateInfo& info = *ctx.partial_update_info;
    const bool have_input_seq_column = (seq_column != nullptr);

    use_default_or_null_flag.reserve(num_rows);
    for (size_t pos = 0; pos < num_rows; ++pos) {
        // Encode without touching the row cache: the writer's key index build
        // invalidates every row's cache entry under the same conditions, so the
        // erase runs once there, not twice.
        // one block == one fresh segment: segment_pos == block row index
        std::string key = key_encoder.full_encode_primary_keys(key_columns, pos);
        if (have_input_seq_column) {
            key_encoder.append_seq_suffix(&key, seq_column, pos);
        }
        const bool have_delete_sign = (delete_signs != nullptr && delete_signs[pos] != 0);
        ProbeOutcome out = DORIS_TRY(probe.probe(key, /*segment_pos=*/pos, have_input_seq_column,
                                                 have_delete_sign, specified_rowsets,
                                                 segment_caches, ctx.partial_update_stats));
        if (out.result == KeyProbeResult::NOT_FOUND && !have_delete_sign) {
            RETURN_IF_ERROR(info.handle_new_key(schema, [&]() -> std::string {
                return block->dump_one_line(pos, cast_set<int>(schema.num_key_columns()));
            }));
        }
        has_default_or_nullable |= out.use_default_or_null;
        use_default_or_null_flag.emplace_back(out.use_default_or_null);
        if (!out.use_default_or_null) {
            fetcher.pin_rowset(out.rowset);
            fetcher.plan_fixed_read(out.loc, pos);
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

    MowKeyProbe probe = MowKeyProbe::for_partial_update(
            ctx.tablet.get(), tablet_schema.get(), schema.has_sequence_col(), ctx.mow_context,
            ctx.rowset_id, cast_set<uint32_t>(ctx.segment_id), /*flexible=*/false);
    HistoricalRowFetcher fetcher(ctx.rowset_ctx->make_historical_row_retriever_context());

    bool has_default_or_nullable = false;
    std::vector<bool> use_default_or_null_flag;
    const auto* delete_signs = BaseTablet::get_delete_sign_column_data(full_block, num_rows);
    RETURN_IF_ERROR(probe_and_plan(ctx, key_encoder, probe, fetcher, specified_rowsets,
                                   segment_caches, key_columns, seq_column, delete_signs, num_rows,
                                   block, use_default_or_null_flag, has_default_or_nullable));

    maybe_add_sentinel_mark(ctx);

    // 4. read history / defaults into the missing columns
    RETURN_IF_ERROR(fetcher.fill_missing_columns(schema, full_block, use_default_or_null_flag,
                                                 has_default_or_nullable,
                                                 /*segment_start_pos=*/0, block));
    if (schema.has_ttl_col() && info.row_ttl_source_cid() >= 0) {
        RETURN_IF_ERROR(copy_row_ttl_source(&full_block, schema, info.row_ttl_source_cid(),
                                            use_default_or_null_flag, /*row_pos=*/0));
    }

    // 5. swap in the full-width block; downstream it looks like a plain upsert
    block->swap(full_block);
    return Status::OK();
}

} // namespace doris::segment_v2
