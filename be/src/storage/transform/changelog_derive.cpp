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

#include "storage/transform/changelog_derive.h"

#include <algorithm>
#include <optional>
#include <span>
#include <unordered_map>
#include <unordered_set>

#include "common/cast_set.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "gen_cpp/Descriptors_types.h"
#include "storage/changelog.h"
#include "storage/key/row_key_encoder.h"
#include "storage/mow/historical_row_fetcher.h"
#include "storage/partial_update_info.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/historical_row_retriever.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/transform/transform_util.h"
#include "util/time.h"

namespace doris::changelog {

Status validate_mapping(const PChangelogWriteColumnMappings& mapping) {
    if (!mapping.has_need_historical_value()) {
        return Status::InvalidArgument("changelog mapping misses need_historical_value");
    }
    std::unordered_set<int32_t> source_uids;
    std::unordered_set<int32_t> target_uids;
    for (const auto& entry : mapping.entries()) {
        if (!entry.has_source_column_unique_id() || !entry.has_current_column_unique_id()) {
            return Status::InvalidArgument("changelog mapping misses source/current UID");
        }
        if (!source_uids.insert(entry.source_column_unique_id()).second ||
            !target_uids.insert(entry.current_column_unique_id()).second) {
            return Status::InvalidArgument("duplicate changelog source/current UID");
        }
        if (entry.has_before_column_unique_id() &&
            (!mapping.need_historical_value() ||
             !target_uids.insert(entry.before_column_unique_id()).second)) {
            return Status::InvalidArgument("invalid or duplicate changelog before UID");
        }
    }
    return Status::OK();
}

Status mapping_from_thrift(const TChangelogWriteColumnMappings& input,
                           PChangelogWriteColumnMappings* output) {
    if (!input.__isset.entries || !input.__isset.need_historical_value) {
        return Status::InvalidArgument("changelog mapping misses entries/historical flag");
    }
    output->Clear();
    output->set_need_historical_value(input.need_historical_value);
    for (const auto& entry : input.entries) {
        if (!entry.__isset.source_column_unique_id || !entry.__isset.current_column_unique_id) {
            return Status::InvalidArgument("changelog mapping misses source/current UID");
        }
        auto* encoded = output->add_entries();
        encoded->set_source_column_unique_id(entry.source_column_unique_id);
        encoded->set_current_column_unique_id(entry.current_column_unique_id);
        if (entry.__isset.before_column_unique_id) {
            encoded->set_before_column_unique_id(entry.before_column_unique_id);
        }
    }
    return validate_mapping(*output);
}

namespace {
bool same_type_shape(const TabletColumn& source, const TabletColumn& target, bool top_level) {
    if (source.type() != target.type() || source.length() != target.length() ||
        source.precision() != target.precision() || source.frac() != target.frac() ||
        source.get_subtype_count() != target.get_subtype_count() ||
        (source.is_nullable() && !target.is_nullable()) ||
        (!top_level && source.is_nullable() != target.is_nullable())) {
        return false;
    }
    for (uint32_t i = 0; i < source.get_subtype_count(); ++i) {
        if (!same_type_shape(source.get_sub_column(i), target.get_sub_column(i), false)) {
            return false;
        }
    }
    return true;
}
} // namespace

Status resolve_layout(const TabletSchemaSPtr& source, const TabletSchemaSPtr& target,
                      const PChangelogWriteColumnMappings& mapping,
                      std::shared_ptr<const ResolvedChangelogLayout>* result) {
    RETURN_IF_ERROR(validate_mapping(mapping));
    if (source == nullptr || target == nullptr) {
        return Status::InvalidArgument("changelog layout requires source and target schemas");
    }
    auto layout = std::make_shared<ResolvedChangelogLayout>();
    layout->source_schema = source;
    layout->target_schema = target;
    layout->need_historical_value = mapping.need_historical_value();
    const int tso = target->changelog_tso_col_idx();
    const int lsn = target->changelog_lsn_col_idx();
    const int op = target->changelog_op_col_idx();
    if (tso < 0 || lsn < 0 || op < 0) {
        return Status::InvalidArgument("changelog schema misses its hidden columns");
    }
    layout->tso_cid = tso;
    layout->lsn_cid = lsn;
    layout->op_cid = op;
    if (target->column(tso).type() != FieldType::OLAP_FIELD_TYPE_BIGINT ||
        !target->column(tso).is_nullable() ||
        target->column(lsn).type() != FieldType::OLAP_FIELD_TYPE_BIGINT ||
        target->column(lsn).is_nullable() ||
        target->column(op).type() != FieldType::OLAP_FIELD_TYPE_BIGINT) {
        return Status::InvalidArgument("invalid changelog system column types");
    }
    std::unordered_map<int32_t, uint32_t> source_cids;
    std::unordered_map<int32_t, uint32_t> target_cids;
    for (uint32_t cid = 0; cid < source->num_columns(); ++cid) {
        const auto& column = source->column(cid);
        // Hidden non-key execution columns (delete sign, sequence, TSO) have
        // no changelog image and are outside the mapping's source namespace.
        if (!column.visible() && !column.is_key()) {
            continue;
        }
        if (!source_cids.emplace(column.unique_id(), cid).second) {
            return Status::InvalidArgument("duplicate source schema UID");
        }
    }
    for (uint32_t cid = 0; cid < target->num_columns(); ++cid) {
        if (!target_cids.emplace(target->column(cid).unique_id(), cid).second) {
            return Status::InvalidArgument("duplicate changelog schema UID");
        }
    }
    std::unordered_set<uint32_t> mapped_source;
    std::unordered_set<uint32_t> mapped_target = {layout->tso_cid, layout->lsn_cid, layout->op_cid};
    for (const auto& entry : mapping.entries()) {
        auto src = source_cids.find(entry.source_column_unique_id());
        auto dst = target_cids.find(entry.current_column_unique_id());
        if (src == source_cids.end() || dst == target_cids.end()) {
            return Status::InvalidArgument("unknown changelog source/current UID: {}/{}",
                                           entry.source_column_unique_id(),
                                           entry.current_column_unique_id());
        }
        const auto& source_col = source->column(src->second);
        const auto& target_col = target->column(dst->second);
        if ((!source_col.visible() && !source_col.is_key()) ||
            source_col.is_key() != target_col.is_key() ||
            !same_type_shape(source_col, target_col, !source_col.is_key()) ||
            !mapped_target.insert(dst->second).second) {
            return Status::InvalidArgument("invalid changelog current mapping for UID {}",
                                           entry.source_column_unique_id());
        }
        mapped_source.insert(src->second);
        layout->current_source_cids.push_back(src->second);
        layout->current_target_cids.push_back(dst->second);
        layout->current_nullable_widening.push_back(!source_col.is_nullable() &&
                                                    target_col.is_nullable());
        const bool needs_before = mapping.need_historical_value() && !source_col.is_key();
        if (entry.has_before_column_unique_id() != needs_before) {
            return Status::InvalidArgument("incomplete changelog before mapping for UID {}",
                                           entry.source_column_unique_id());
        }
        if (needs_before) {
            auto before = target_cids.find(entry.before_column_unique_id());
            if (before == target_cids.end() || !mapped_target.insert(before->second).second) {
                return Status::InvalidArgument("unknown or reused changelog before UID {}",
                                               entry.before_column_unique_id());
            }
            const auto& before_col = target->column(before->second);
            if (before_col.is_key() || !before_col.is_nullable() ||
                !same_type_shape(source_col, before_col, true)) {
                return Status::InvalidArgument("invalid changelog before type for UID {}",
                                               entry.before_column_unique_id());
            }
            layout->before_source_cids.push_back(src->second);
            layout->before_target_cids.push_back(before->second);
        }
    }
    for (uint32_t cid = 0; cid < source->num_columns(); ++cid) {
        const auto& col = source->column(cid);
        if ((col.visible() || col.is_key()) && !mapped_source.contains(cid)) {
            return Status::InvalidArgument("unmapped changelog source UID {}", col.unique_id());
        }
    }
    if (mapped_target.size() != target->num_columns()) {
        return Status::InvalidArgument("unmapped changelog target columns");
    }
    *result = std::move(layout);
    return Status::OK();
}
} // namespace doris::changelog

namespace doris::segment_v2 {

namespace {

// Runs the primary-key historical lookup over `block`'s source key (+seq)
// columns, leaving the planned reads and the op for each row in `retriever` for
// building AFTER/BEFORE. `seq_pos` is the seq column's position in the input
// block (-1 if absent).
Status setup_retriever_and_lookup(TransformExecContext& ctx,
                                  const SegmentWriteChangelogOptions& cfg,
                                  const TabletSchemaSPtr& source_schema, const Block* block,
                                  int32_t seq_pos, const Int8* delete_signs, size_t num_rows,
                                  std::unique_ptr<PrimaryKeyModelRowRetriever>& retriever) {
    retriever = std::make_unique<PrimaryKeyModelRowRetriever>();
    // Changelog lives in its own tablet, so ctx.tablet is that changelog tablet
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

    // key (+seq) columns of the input block for the lookup
    std::vector<const IColumn*> key_columns;
    collect_key_columns(*source_schema, *block, key_columns);
    const IColumn* seq_column = nullptr;
    if (seq_pos != -1) {
        seq_column = block->get_by_position(static_cast<size_t>(seq_pos)).column.get();
    }
    RETURN_IF_ERROR(retriever->prepare_lookup_plan_from_source_columns(key_columns, seq_column,
                                                                       cfg.source.mow_context));
    RETURN_IF_ERROR(retriever->retrieve_historical_row(delete_signs, 0, num_rows));
    return Status::OK();
}

// Fills the BEFORE value columns into `out` from the retriever's historical
// reads. No-op when the source schema has no value columns. The retriever is
// not const: reading BEFORE also loads the old delete signs it keeps.
Status fill_before_columns(Block& out, const TabletSchema& changelog_schema,
                           PrimaryKeyModelRowRetriever* retriever,
                           const std::vector<uint32_t>& before_cids,
                           const std::vector<uint32_t>& value_source_cids, size_t num_rows) {
    size_t value_column_num = value_source_cids.size();
    if (value_column_num == 0) {
        return Status::OK();
    }
    Block before_block = changelog_schema.create_storage_block(before_cids);
    DCHECK(retriever != nullptr);
    DCHECK_EQ(before_cids.size(), value_source_cids.size());
    RETURN_IF_ERROR(retriever->build_before_block(&before_block, value_source_cids, 0, num_rows));
    size_t col_pos_in_block = 0;
    for (auto cid : before_cids) {
        out.replace_by_position(cid, before_block.get_by_position(col_pos_in_block++).column);
    }
    return Status::OK();
}

// Fills the TSO / LSN / op columns into `out`. TSO is unknown until publish,
// LSN is the per-row auto-inc value, and op describes the row change.
void fill_changelog_system_columns(Block& out, const TabletSchema& changelog_schema,
                                   uint32_t changelog_tso_cid, uint32_t changelog_lsn_cid,
                                   uint32_t changelog_op_cid, const std::vector<int64_t>& lsn_ids,
                                   const std::vector<ChangelogOp>& operators, size_t num_rows) {
    std::vector<uint32_t> changelog_cids = {changelog_tso_cid, changelog_lsn_cid, changelog_op_cid};
    Block changelog_system_block = changelog_schema.create_storage_block(changelog_cids);
    {
        auto changelog_system_columns_guard = changelog_system_block.mutate_columns_scoped();
        auto& changelog_system_columns = changelog_system_columns_guard.mutable_columns();

        // The reader replaces this placeholder with the real commit_tso.
        IColumn* tso_col_ptr = changelog_system_columns[0].get();
        auto* tso_nullable_column = check_and_get_column<ColumnNullable>(tso_col_ptr);
        DCHECK(tso_nullable_column != nullptr);
        tso_nullable_column->insert_many_defaults(num_rows);

        // Auto-inc LSN keeps row order until the rowset is published.
        IColumn* lsn_col_ptr = changelog_system_columns[1].get();
        for (size_t i = 0; i < num_rows; i++) {
            assert_cast<ColumnInt64*>(lsn_col_ptr)->insert_value(lsn_ids.at(i));
        }

        // wrong op only happens under partial update, it is fixed by the
        // delete bitmap at publish
        const FieldType op_col_type = changelog_schema.column(changelog_op_cid).type();
        IColumn* op_col_ptr = changelog_system_columns[2].get();
        auto* op_nullable_column = check_and_get_column<ColumnNullable>(op_col_ptr);
        IColumn* op_nested_column = op_nullable_column != nullptr
                                            ? &op_nullable_column->get_nested_column()
                                            : op_col_ptr;
        CHECK(operators.size() >= num_rows) << operators.size() << " vs " << num_rows;
        CHECK(op_col_type == FieldType::OLAP_FIELD_TYPE_BIGINT)
                << "changelog op column type must be BIGINT, actual="
                << static_cast<int>(op_col_type);
        auto* op_int64_column = assert_cast<ColumnInt64*>(op_nested_column);
        for (size_t i = 0; i < num_rows; i++) {
            op_int64_column->insert_value(static_cast<int64_t>(operators[i]));
        }

        if (op_nullable_column != nullptr) {
            auto& op_null_map = op_nullable_column->get_null_map_data();
            for (size_t i = 0; i < num_rows; i++) {
                op_null_map.emplace_back(0);
            }
        }
    }
    size_t col_pos_in_block = 0;
    for (auto cid : changelog_cids) {
        out.replace_by_position(cid,
                                changelog_system_block.get_by_position(col_pos_in_block++).column);
    }
}

// Borrow the prepared layout and consume this segment's LSN range exactly once.
Status resolve_changelog_context(TransformExecContext& ctx, const Block* block,
                                 ChangelogDeriveContext* c) {
    c->changelog_schema = ctx.tablet_schema;
    auto& cfg = ctx.rowset_ctx->write_changelog_opt().write_changelog_config();
    c->source_schema = cfg.source.tablet_schema;
    if (UNLIKELY(c->source_schema == nullptr)) {
        return Status::InternalError("changelog derive missing source_tablet_schema, tablet_id={}",
                                     ctx.rowset_ctx->tablet_id);
    }
    if (UNLIKELY(cfg.source.base_tablet == nullptr &&
                 changelog_needs_historical_lookup(*ctx.rowset_ctx))) {
        return Status::InternalError(
                "changelog derive needs the source tablet to look history up, tablet_id={}",
                ctx.rowset_ctx->tablet_id);
    }
    if (cfg.layout == nullptr) {
        return Status::InvalidArgument("changelog derive requires a resolved column mapping");
    }
    c->num_rows = block->rows();

    // the LSN range for each row was registered per segment id before this
    // flush; consume it exactly once.
    if (ctx.segment_id < 0) {
        return Status::InternalError<false>(
                "changelog blocks must be flushed through flush_single_block");
    }
    c->lsn_ids = ctx.rowset_ctx->get_segment_allocated_lsns(ctx.segment_id);
    ctx.rowset_ctx->remove_segment_allocated_lsns(ctx.segment_id);
    CHECK(c->lsn_ids->size() >= c->num_rows) << c->lsn_ids->size() << " vs " << c->num_rows;

    c->layout = cfg.layout.get();
    c->changelog_schema = c->layout->target_schema;
    c->source_schema = c->layout->source_schema;
    c->changelog_tso_cid = c->layout->tso_cid;
    c->changelog_lsn_cid = c->layout->lsn_cid;
    c->changelog_op_cid = c->layout->op_cid;
    c->write_before = c->layout->need_historical_value;
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

// Shared final step: build the final changelog block -- AFTER columns (COW from
// after_src), optional BEFORE columns, the TSO/LSN/op columns -- and swap
// it into `block`.
Status emit_changelog_block(const ChangelogDeriveContext& c, const Block* after_src,
                            PrimaryKeyModelRowRetriever* retriever,
                            const std::vector<ChangelogOp>* plain_operators, Block* block) {
    Block out = c.changelog_schema->create_storage_block();
    // every index below is a cid, so the block has to carry every schema column
    if (out.columns() != c.changelog_schema->num_columns()) {
        return Status::InternalError<false>(
                "changelog block width {} does not match its schema's {} columns", out.columns(),
                c.changelog_schema->num_columns());
    }
    // key + AFTER columns: COW pointers of the source-layout visible columns
    for (size_t ordinal = 0; ordinal < c.layout->current_source_cids.size(); ++ordinal) {
        const uint32_t source_cid = c.layout->current_source_cids[ordinal];
        ColumnPtr after_column = after_src->get_by_position(source_cid).column;
        // The changelog schema declares every AFTER value column nullable and `out` keeps that
        // type when a column is replaced, so a NOT NULL source column is wrapped to match it; a
        // nullable ARRAY, MAP or STRUCT writer would refuse it otherwise. Keys keep their own
        // nullability.
        if (c.layout->current_nullable_widening[ordinal]) {
            after_column = make_nullable(after_column);
        }
        out.replace_by_position(c.layout->current_target_cids[ordinal], std::move(after_column));
    }
    if (c.write_before) {
        RETURN_IF_ERROR(fill_before_columns(out, *c.changelog_schema, retriever,
                                            c.layout->before_target_cids,
                                            c.layout->before_source_cids, c.num_rows));
    }
    // An insert whose old row is a tombstone is an append, not an update. Run
    // this after the BEFORE read, which already loaded the old delete signs.
    if (retriever != nullptr) {
        RETURN_IF_ERROR(retriever->revise_operators_by_old_delete_sign(c.num_rows));
    }
    const std::vector<ChangelogOp>& operators =
            retriever != nullptr ? retriever->get_operators() : *plain_operators;
    fill_changelog_system_columns(out, *c.changelog_schema, c.changelog_tso_cid,
                                  c.changelog_lsn_cid, c.changelog_op_cid, *c.lsn_ids, operators,
                                  c.num_rows);
    block->swap(out);
    return Status::OK();
}

} // namespace

bool changelog_needs_historical_lookup(const RowsetWriterContext& context) {
    const auto& cfg = context.write_changelog_opt().write_changelog_config();
    const bool is_source_direct_write =
            cfg.source.source_write_type == DataWriteType::TYPE_DIRECT &&
            !cfg.source.is_transient_rowset_writer;
    // A direct partial update (fixed and flexible both need AFTER rebuilt) or a
    // requested BEFORE image needs the probe.
    const bool is_partial_update = cfg.source.partial_update_info != nullptr &&
                                   cfg.source.partial_update_info->is_partial_update() &&
                                   is_source_direct_write;
    return is_partial_update || (cfg.layout != nullptr && cfg.layout->need_historical_value);
}

Status ChangelogDeriveStage::apply(TransformExecContext& ctx, Block* block) const {
    ChangelogDeriveContext c;
    RETURN_IF_ERROR(resolve_changelog_context(ctx, block, &c));
    return derive(ctx, block, c);
}

Status PlainChangelogDeriveStage::derive(TransformExecContext& /*ctx*/, Block* block,
                                         const ChangelogDeriveContext& c) const {
    DCHECK(!c.write_before);

    // No probe: op is APPEND, or DELETE when the row carries a delete sign. DUP
    // tables have no delete-sign column, so every row is APPEND.
    std::optional<std::span<const Int8>> delete_signs =
            read_delete_signs(block, c.source_schema->delete_sign_idx(), c.num_rows);
    std::vector<ChangelogOp> operators;
    operators.reserve(c.num_rows);
    for (size_t pos = 0; pos < c.num_rows; ++pos) {
        bool have_delete_sign = (delete_signs && (*delete_signs)[pos] != 0);
        operators.emplace_back(have_delete_sign ? ChangelogOp::DELETE : ChangelogOp::APPEND);
    }
    return emit_changelog_block(c, /*after_src=*/block, /*retriever=*/nullptr, &operators, block);
}

Status MowChangelogDeriveStage::derive(TransformExecContext& ctx, Block* block,
                                       const ChangelogDeriveContext& c) const {
    const auto& cfg = ctx.rowset_ctx->write_changelog_opt().write_changelog_config();

    const bool is_source_direct_write =
            cfg.source.source_write_type == DataWriteType::TYPE_DIRECT &&
            !cfg.source.is_transient_rowset_writer;
    const bool is_flexible_partial_update =
            cfg.source.partial_update_info && is_source_direct_write &&
            cfg.source.partial_update_info->is_flexible_partial_update();
    const bool is_fixed_partial_update = cfg.source.partial_update_info && is_source_direct_write &&
                                         cfg.source.partial_update_info->is_fixed_partial_update();

    if (is_flexible_partial_update) {
        auto retriever = std::make_unique<PrimaryKeyModelRowRetriever>();
        RETURN_IF_ERROR(retriever->init(HistoricalRowRetrieverContext {
                .tablet = cfg.source.base_tablet,
                .tablet_schema = c.source_schema,
                .rowset_writer_ctx = ctx.rowset_ctx,
                .partial_update_info = cfg.source.partial_update_info,
                .is_transient_rowset_writer = cfg.source.is_transient_rowset_writer,
                .write_type = cfg.source.source_write_type}));

        std::vector<int64_t> effective_lsn_ids(c.lsn_ids->begin(), c.lsn_ids->begin() + c.num_rows);
        RETURN_IF_ERROR(retriever->materialize_flexible_partial_update(
                block, cfg.source.mow_context, &effective_lsn_ids));

        ChangelogDeriveContext effective_context = c;
        effective_context.num_rows = block->rows();
        effective_context.lsn_ids =
                std::make_shared<const std::vector<int64_t>>(std::move(effective_lsn_ids));
        return emit_changelog_block(effective_context, block, retriever.get(),
                                    /*plain_operators=*/nullptr, block);
    }

    if (is_fixed_partial_update) {
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
    if (is_fixed_partial_update) {
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
    std::unique_ptr<PrimaryKeyModelRowRetriever> retriever;
    RETURN_IF_ERROR(setup_retriever_and_lookup(ctx, cfg, c.source_schema, block, seq_pos,
                                               delete_signs ? delete_signs->data() : nullptr,
                                               c.num_rows, retriever));

    // Building AFTER: fixed partial update widens the narrow input and rebuilds
    // missing columns from history; upserts are already source-schema shaped.
    Block full_block;
    const Block* after_src = block;
    if (is_fixed_partial_update) {
        full_block = widen_partial_update_block(
                *c.source_schema, cfg.source.partial_update_info->update_cids, *block);
        RETURN_IF_ERROR(retriever->build_after_block(&full_block, 0, c.num_rows));
        after_src = &full_block;
    }

    return emit_changelog_block(c, after_src, retriever.get(), /*plain_operators=*/nullptr, block);
}

} // namespace doris::segment_v2
