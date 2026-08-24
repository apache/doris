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

#include "storage/transform/row_binlog_derive.h"

#include <algorithm>
#include <optional>
#include <span>

#include "common/cast_set.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "storage/binlog.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/key/row_key_encoder.h"
#include "storage/mow/historical_row_fetcher.h"
#include "storage/partial_update_info.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/historical_row_retriever.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/transform/transform_util.h"
#include "util/time.h"

namespace doris::segment_v2 {

namespace {

// Number of row-binlog system columns: TSO, LSN, op.
constexpr uint32_t BINLOG_COLNUM = 3;

// Runs the primary-key historical lookup over `block`'s source key (+seq)
// columns, leaving the planned reads and the op for each row in `retriever` for
// building AFTER/BEFORE. `seq_pos` is the seq column's position in the input
// block (-1 if absent). `convertor` must live longer than `retriever`: its
// accessors back the lookup plan.
Status setup_retriever_and_lookup(TransformExecContext& ctx, const SegmentWriteBinlogOptions& cfg,
                                  const TabletSchemaSPtr& source_schema, const Block* block,
                                  int32_t seq_pos, const Int8* delete_signs, size_t num_rows,
                                  OlapBlockDataConvertor& convertor,
                                  std::unique_ptr<PrimaryKeyModelRowRetriever>& retriever) {
    retriever = std::make_unique<PrimaryKeyModelRowRetriever>();
    // Row binlog lives in its own tablet, so ctx.tablet is that binlog tablet
    // while the history to look up sits in the source tablet. Falling back to
    // ctx.tablet would silently search a tablet that holds no source rows, so
    // the caller has to name the source tablet -- every site that fills
    // source.tablet_schema fills this too.
    RETURN_IF_ERROR(retriever->init(HistoricalRowRetrieverContext {
            .tablet = cfg.source.base_tablet,
            .tablet_schema = source_schema,
            .rowset_writer_ctx = ctx.rowset_ctx,
            .partial_update_info = cfg.source.partial_update_info,
            .is_transient_rowset_writer = cfg.source.is_transient_rowset_writer,
            .write_type = cfg.source.source_write_type}));

    // key (+seq) only conversion from the input block for the lookup
    convertor.resize(source_schema->num_columns());
    std::vector<IOlapColumnDataAccessor*> key_columns;
    RETURN_IF_ERROR(convert_key_columns(convertor, *source_schema, *block, num_rows, key_columns));
    IOlapColumnDataAccessor* seq_column = nullptr;
    if (seq_pos != -1) {
        RETURN_IF_ERROR(convert_seq_column(convertor, *source_schema, *block,
                                           static_cast<size_t>(seq_pos), num_rows, seq_column));
    }
    RETURN_IF_ERROR(retriever->prepare_lookup_plan_from_source_columns(key_columns, seq_column,
                                                                       cfg.source.mow_context));
    RETURN_IF_ERROR(retriever->retrieve_historical_row(delete_signs, 0, num_rows));
    return Status::OK();
}

// Fills the BEFORE value columns into `out` from the retriever's historical
// reads. No-op when the source schema has no value columns. The retriever is
// not const: reading BEFORE also loads the old delete signs it keeps.
Status fill_before_columns(Block& out, const TabletSchema& binlog_schema,
                           PrimaryKeyModelRowRetriever* retriever,
                           const std::vector<uint32_t>& before_binlog_cids,
                           const std::vector<uint32_t>& value_source_cids, size_t num_rows) {
    if (value_source_cids.empty()) {
        return Status::OK();
    }
    DORIS_CHECK_EQ(before_binlog_cids.size(), value_source_cids.size());
    Block before_block = binlog_schema.create_storage_block(before_binlog_cids);
    DCHECK(retriever != nullptr);
    RETURN_IF_ERROR(retriever->build_before_block(&before_block, value_source_cids, 0, num_rows));
    size_t col_pos_in_block = 0;
    for (auto cid : before_binlog_cids) {
        out.replace_by_position(cid, before_block.get_by_position(col_pos_in_block++).column);
    }
    return Status::OK();
}

// Fills the TSO / LSN / op columns into `out`. TSO is unknown until publish,
// LSN is the per-row auto-inc value, and op describes the row change.
void fill_binlog_system_columns(Block& out, const TabletSchema& binlog_schema,
                                uint32_t binlog_tso_cid, uint32_t binlog_lsn_cid,
                                uint32_t binlog_op_cid, const std::vector<int64_t>& lsn_ids,
                                const std::vector<int64_t>& operators, size_t num_rows) {
    std::vector<uint32_t> binlog_cids = {binlog_tso_cid, binlog_lsn_cid, binlog_op_cid};
    Block binlog_system_block = binlog_schema.create_storage_block(binlog_cids);
    {
        auto binlog_system_columns_guard = binlog_system_block.mutate_columns_scoped();
        auto& binlog_system_columns = binlog_system_columns_guard.mutable_columns();

        // The reader replaces this placeholder with the real commit_tso.
        IColumn* tso_col_ptr = binlog_system_columns[0].get();
        auto* tso_nullable_column = check_and_get_column<ColumnNullable>(tso_col_ptr);
        DCHECK(tso_nullable_column != nullptr);
        tso_nullable_column->insert_many_defaults(num_rows);

        // Auto-inc LSN keeps row order until the rowset is published.
        IColumn* lsn_col_ptr = binlog_system_columns[1].get();
        for (size_t i = 0; i < num_rows; i++) {
            assert_cast<ColumnInt64*>(lsn_col_ptr)->insert_value(lsn_ids.at(i));
        }

        // wrong op only happens under partial update, it is fixed by the
        // delete bitmap at publish
        const FieldType op_col_type = binlog_schema.column(binlog_op_cid).type();
        IColumn* op_col_ptr = binlog_system_columns[2].get();
        auto* op_nullable_column = check_and_get_column<ColumnNullable>(op_col_ptr);
        IColumn* op_nested_column = op_nullable_column != nullptr
                                            ? &op_nullable_column->get_nested_column()
                                            : op_col_ptr;
        CHECK(operators.size() >= num_rows) << operators.size() << " vs " << num_rows;
        CHECK(op_col_type == FieldType::OLAP_FIELD_TYPE_BIGINT)
                << "row binlog op column type must be BIGINT, actual="
                << static_cast<int>(op_col_type);
        auto* op_int64_column = assert_cast<ColumnInt64*>(op_nested_column);
        for (size_t i = 0; i < num_rows; i++) {
            op_int64_column->insert_value(operators[i]);
        }

        if (op_nullable_column != nullptr) {
            auto& op_null_map = op_nullable_column->get_null_map_data();
            for (size_t i = 0; i < num_rows; i++) {
                op_null_map.emplace_back(0);
            }
        }
    }
    size_t col_pos_in_block = 0;
    for (auto cid : binlog_cids) {
        out.replace_by_position(cid,
                                binlog_system_block.get_by_position(col_pos_in_block++).column);
    }
}

// Shared setup step: work out the binlog schema layout and consume this
// segment's LSN range (exactly once).
Status resolve_binlog_context(TransformExecContext& ctx, const Block* block,
                              BinlogDeriveContext* c) {
    c->binlog_schema = ctx.tablet_schema;
    auto& cfg = ctx.rowset_ctx->write_binlog_opt().write_binlog_config();
    c->source_schema = cfg.source.tablet_schema;
    if (UNLIKELY(c->source_schema == nullptr)) {
        return Status::InternalError(
                "binlog<row> derive missing source_tablet_schema, tablet_id={}",
                ctx.rowset_ctx->tablet_id);
    }
    if (UNLIKELY(cfg.source.base_tablet == nullptr &&
                 binlog_needs_historical_lookup(*ctx.rowset_ctx))) {
        return Status::InternalError(
                "binlog<row> derive needs the source tablet to look history up, tablet_id={}",
                ctx.rowset_ctx->tablet_id);
    }
    c->num_rows = block->rows();

    // the LSN range for each row was registered per segment id before this
    // flush; consume it exactly once.
    if (ctx.segment_id < 0) {
        return Status::InternalError<false>(
                "binlog<row> blocks must be flushed through flush_single_block");
    }
    c->lsn_ids = cfg.get_seg_lsn(ctx.segment_id);
    cfg.remove_seg(ctx.segment_id);
    CHECK(c->lsn_ids->size() >= c->num_rows) << c->lsn_ids->size() << " vs " << c->num_rows;

    // Source hidden non-key columns are omitted while hidden keys remain part
    // of the normal row image.
    int tso_col_id = c->binlog_schema->binlog_tso_col_idx();
    int lsn_col_id = c->binlog_schema->binlog_lsn_col_idx();
    int op_col_id = c->binlog_schema->binlog_op_col_idx();
    CHECK(tso_col_id >= 0) << "binlog<row> schema missing " << BINLOG_TSO_COL;
    CHECK(lsn_col_id >= 0) << "binlog<row> schema missing " << BINLOG_LSN_COL;
    CHECK(op_col_id >= 0) << "binlog<row> schema missing " << BINLOG_OP_COL;
    c->binlog_tso_cid = cast_set<uint32_t>(tso_col_id);
    c->binlog_lsn_cid = cast_set<uint32_t>(lsn_col_id);
    c->binlog_op_cid = cast_set<uint32_t>(op_col_id);
    for (uint32_t source_cid = 0; source_cid < c->source_schema->num_columns();
         ++source_cid) {
        const auto& source_column = c->source_schema->column(source_cid);
        if (!source_column.visible() && !source_column.is_key()) {
            continue;
        }

        const auto binlog_cid = c->binlog_schema->field_index(source_column.name());
        DORIS_CHECK_GE(binlog_cid, 0)
                << "row-binlog after column " << source_column.name() << " does not exist";
        DORIS_CHECK_NE(binlog_cid, tso_col_id);
        DORIS_CHECK_NE(binlog_cid, lsn_col_id);
        DORIS_CHECK_NE(binlog_cid, op_col_id);
        const auto& binlog_column = c->binlog_schema->column(binlog_cid);
        DORIS_CHECK_EQ(binlog_column.is_key(), source_column.is_key())
                << "row-binlog column " << source_column.name() << " has inconsistent key metadata";

        c->normal_source_cids.emplace_back(source_cid);
        c->normal_binlog_cids.emplace_back(cast_set<uint32_t>(binlog_cid));
        if (source_column.visible() && !source_column.is_key()) {
            c->value_source_cids.emplace_back(source_cid);
        }
        if (!binlog_column.is_key() && binlog_column.before_column_unique_id() >= 0) {
            const auto before_cid =
                    c->binlog_schema->field_index(binlog_column.before_column_unique_id());
            DORIS_CHECK_GE(before_cid, 0)
                    << "before column unique id " << binlog_column.before_column_unique_id()
                    << " referenced by row-binlog after column " << binlog_column.name()
                    << " does not exist";
            c->before_binlog_cids.emplace_back(cast_set<uint32_t>(before_cid));
        }
    }
    c->write_before = cfg.write_before;
    // The schema must carry exactly one normal destination per source row-image
    // column and exactly one UID-resolved before destination per source value
    // when write_before is enabled.
    const size_t expected_before_columns = c->write_before ? c->value_source_cids.size() : 0;
    const size_t expected_columns = c->normal_source_cids.size() + expected_before_columns +
                                    BINLOG_COLNUM;
    if (c->normal_binlog_cids.size() != c->normal_source_cids.size() ||
        c->before_binlog_cids.size() != expected_before_columns ||
        c->binlog_schema->num_columns() != expected_columns) {
        return Status::InternalError<false>(
                "binlog<row> schema does not match the derived block: num_columns={}, expected={} "
                "(source_normal={}, binlog_normal={}, before={}, system={}), write_before={}, "
                "tablet_id={}",
                c->binlog_schema->num_columns(), expected_columns, c->normal_source_cids.size(),
                c->normal_binlog_cids.size(), c->before_binlog_cids.size(), BINLOG_COLNUM,
                c->write_before, ctx.rowset_ctx->tablet_id);
    }
    return Status::OK();
}

// A view over the delete-sign column, or nullopt when the schema has no
// delete-sign column or the column does not cover the requested rows.
std::optional<std::span<const Int8>> read_delete_signs(const Block* block, int32_t delete_sign_pos,
                                                       size_t num_rows) {
    if (delete_sign_pos == -1) {
        return std::nullopt;
    }
    const ColumnWithTypeAndName& delete_sign_column = block->get_by_position(delete_sign_pos);
    const auto& delete_sign_col = assert_cast<const ColumnInt8&>(*(delete_sign_column.column));
    if (delete_sign_col.size() >= num_rows) {
        return std::span<const Int8>(delete_sign_col.get_data().data(), num_rows);
    }
    return std::nullopt;
}

// Shared final step: build the final binlog block -- AFTER columns (COW from
// after_src), optional BEFORE columns, the TSO/LSN/op columns -- and swap
// it into `block`.
Status emit_binlog_block(const BinlogDeriveContext& c, const Block* after_src,
                         PrimaryKeyModelRowRetriever* retriever,
                         const std::vector<int64_t>* plain_operators, Block* block) {
    Block out = c.binlog_schema->create_storage_block();
    // every index below is a cid, so the block has to carry every schema column
    if (out.columns() != c.binlog_schema->num_columns()) {
        return Status::InternalError<false>(
                "binlog<row> block width {} does not match its schema's {} columns", out.columns(),
                c.binlog_schema->num_columns());
    }
    // key + AFTER columns: COW pointers of the source-layout visible columns
    DORIS_CHECK_EQ(c.normal_source_cids.size(), c.normal_binlog_cids.size());
    for (size_t ordinal = 0; ordinal < c.normal_source_cids.size(); ++ordinal) {
        const uint32_t source_cid = c.normal_source_cids[ordinal];
        ColumnPtr after_column = after_src->get_by_position(source_cid).column;
        // The binlog schema declares every AFTER value column nullable, so a NOT
        // NULL source column has to be wrapped: the segment writer converts by
        // the binlog schema. Keys keep their own nullability.
        if (!c.source_schema->column(source_cid).is_key()) {
            after_column = make_nullable(after_column);
        }
        out.replace_by_position(c.normal_binlog_cids[ordinal], std::move(after_column));
    }
    if (c.write_before) {
        RETURN_IF_ERROR(fill_before_columns(out, *c.binlog_schema, retriever,
                                            c.before_binlog_cids, c.value_source_cids, c.num_rows));
    }
    // An insert whose old row is a tombstone is an append, not an update. Run
    // this after the BEFORE read, which already loaded the old delete signs.
    if (retriever != nullptr) {
        RETURN_IF_ERROR(retriever->revise_operators_by_old_delete_sign(c.num_rows));
    }
    const std::vector<int64_t>& operators =
            retriever != nullptr ? retriever->get_operators() : *plain_operators;
    fill_binlog_system_columns(out, *c.binlog_schema, c.binlog_tso_cid, c.binlog_lsn_cid,
                               c.binlog_op_cid, *c.lsn_ids, operators, c.num_rows);
    block->swap(out);
    return Status::OK();
}

} // namespace

bool binlog_needs_historical_lookup(const RowsetWriterContext& context) {
    const auto& cfg = context.write_binlog_opt().write_binlog_config();
    const bool is_source_direct_write =
            cfg.source.source_write_type == DataWriteType::TYPE_DIRECT &&
            !cfg.source.is_transient_rowset_writer;
    // A direct partial update (fixed needs AFTER rebuilt; flexible is rejected
    // inside the MoW stage) or a requested BEFORE image needs the probe.
    const bool is_partial_update = cfg.source.partial_update_info != nullptr &&
                                   cfg.source.partial_update_info->is_partial_update() &&
                                   is_source_direct_write;
    return is_partial_update || cfg.write_before;
}

Status RowBinlogDeriveStage::apply(TransformExecContext& ctx, Block* block) const {
    BinlogDeriveContext c;
    RETURN_IF_ERROR(resolve_binlog_context(ctx, block, &c));
    return derive(ctx, block, c);
}

Status PlainRowBinlogDeriveStage::derive(TransformExecContext& /*ctx*/, Block* block,
                                         const BinlogDeriveContext& c) const {
    DCHECK(!c.write_before);

    // No probe: op is APPEND, or DELETE when the row carries a delete sign. DUP
    // tables have no delete-sign column, so every row is APPEND.
    std::optional<std::span<const Int8>> delete_signs =
            read_delete_signs(block, c.source_schema->delete_sign_idx(), c.num_rows);
    std::vector<int64_t> operators;
    operators.reserve(c.num_rows);
    for (size_t pos = 0; pos < c.num_rows; ++pos) {
        bool have_delete_sign = (delete_signs && (*delete_signs)[pos] != 0);
        operators.emplace_back(have_delete_sign ? ROW_BINLOG_DELETE : ROW_BINLOG_APPEND);
    }
    return emit_binlog_block(c, /*after_src=*/block, /*retriever=*/nullptr, &operators, block);
}

Status MowRowBinlogDeriveStage::derive(TransformExecContext& ctx, Block* block,
                                       const BinlogDeriveContext& c) const {
    const auto& cfg = ctx.rowset_ctx->write_binlog_opt().write_binlog_config();

    bool is_source_direct_write = cfg.source.source_write_type == DataWriteType::TYPE_DIRECT &&
                                  !cfg.source.is_transient_rowset_writer;
    if (cfg.source.partial_update_info && is_source_direct_write &&
        cfg.source.partial_update_info->is_flexible_partial_update()) {
        return Status::NotSupported("binlog<row> does not support flexible partial update");
    }
    bool is_partial_update = cfg.source.partial_update_info &&
                             cfg.source.partial_update_info->is_fixed_partial_update() &&
                             is_source_direct_write;
    if (is_partial_update) {
        if (block->columns() < c.source_schema->num_key_columns() ||
            block->columns() >= c.source_schema->num_columns()) {
            return Status::InvalidArgument(fmt::format(
                    "illegal partial update block columns: {}, num key columns: {}, total "
                    "schema columns: {}",
                    block->columns(), c.source_schema->num_key_columns(),
                    c.source_schema->num_columns()));
        }
    }

    // find the delete sign and sequence columns in the input block (positions
    // move in the narrow partial-update block)
    int32_t delete_sign_pos = c.source_schema->delete_sign_idx();
    int32_t seq_pos = c.source_schema->sequence_col_idx();
    if (is_partial_update) {
        delete_sign_pos = -1;
        seq_pos = -1;
        int32_t pos = 0;
        for (auto& cid : cfg.source.partial_update_info->update_cids) {
            if (cid == c.source_schema->delete_sign_idx()) {
                delete_sign_pos = pos;
            } else if (cid == c.source_schema->sequence_col_idx()) {
                seq_pos = pos;
            }
            pos++;
        }
    }
    std::optional<std::span<const Int8>> delete_signs =
            read_delete_signs(block, delete_sign_pos, c.num_rows);

    // probe history once: produces the AFTER-fill plan, the BEFORE-read plan,
    // and the exact op for each row (APPEND / UPDATE / DELETE). retrieve_historical_row
    // takes the raw delete-sign pointer (the codebase convention), so bridge here.
    // declared first so it outlives the retriever: the lookup plan points into it
    OlapBlockDataConvertor key_convertor;
    std::unique_ptr<PrimaryKeyModelRowRetriever> retriever;
    RETURN_IF_ERROR(setup_retriever_and_lookup(ctx, cfg, c.source_schema, block, seq_pos,
                                               delete_signs ? delete_signs->data() : nullptr,
                                               c.num_rows, key_convertor, retriever));

    // Building AFTER: fixed partial update widens the narrow input and rebuilds
    // missing columns from history; upserts are already source-schema shaped.
    Block full_block;
    const Block* after_src = block;
    if (is_partial_update) {
        full_block = widen_partial_update_block(
                *c.source_schema, cfg.source.partial_update_info->update_cids, *block);
        RETURN_IF_ERROR(retriever->build_after_block(&full_block, 0, c.num_rows));
        after_src = &full_block;
    }

    return emit_binlog_block(c, after_src, retriever.get(), /*plain_operators=*/nullptr, block);
}

} // namespace doris::segment_v2
