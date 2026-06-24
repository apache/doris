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

#include "storage/transform/block_transform.h"

#include <limits>
#include <numeric>
#include <unordered_set>

#include "common/cast_set.h"
#include "common/logging.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "exec/common/variant_util.h"
#include "storage/partial_update_info.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/transform/partial_update_fill.h"
#include "storage/transform/row_binlog_derive.h"
#include "util/jsonb/serialize.h"

namespace doris::segment_v2 {

namespace {

// Parses raw variant columns into subcolumn form, in place. The heavy variant
// work stays in the variant ColumnWriter; this stage only reshapes the block
// before conversion.
class VariantParseStage : public BlockTransform {
public:
    Status apply(TransformExecContext& ctx, Block* block) const override {
        const auto& schema = *ctx.tablet_schema;
        if (schema.num_variant_columns() == 0) {
            return Status::OK();
        }
        std::vector<uint32_t> column_ids(block->columns());
        std::iota(column_ids.begin(), column_ids.end(), 0);
        return variant_util::parse_and_materialize_variant_columns(*block, schema, column_ids);
    }
    std::string_view name() const override { return "VariantParse"; }
};

// The one place write-path input is checked: schema rules and block width.
// (The binlog chain checks its own source-shaped input in RowBinlogDeriveStage;
// the writers keep only DCHECK guards.)
class ValidateStage : public BlockTransform {
public:
    Status apply(TransformExecContext& ctx, Block* block) const override {
        const TabletSchema& schema = *ctx.tablet_schema;
        if (schema.cluster_key_uids().empty()) {
            DCHECK(schema.num_key_columns() >= schema.num_short_key_columns())
                    << ", table_id=" << schema.table_id()
                    << ", num_key_columns=" << schema.num_key_columns()
                    << ", num_short_key_columns=" << schema.num_short_key_columns();
        }
        const auto* info = ctx.partial_update_info.get();
        const bool is_partial_update_load = info != nullptr && info->is_partial_update() &&
                                            ctx.write_type == DataWriteType::TYPE_DIRECT &&
                                            !ctx.rowset_ctx->is_transient_rowset_writer;
        if (!is_partial_update_load) {
            if (block->columns() != schema.num_columns()) {
                return Status::InvalidArgument(
                        "illegal block columns, block columns = {}, tablet_schema columns = {}",
                        block->dump_structure(), schema.dump_structure());
            }
            return Status::OK();
        }

        // No tablet context (e.g. the streaming BetaRowsetWriterV2) means this
        // path can't do partial update: return a clear error instead of
        // crashing in the probe.
        if (ctx.tablet == nullptr || ctx.mow_context == nullptr) {
            return Status::NotSupported(
                    "partial update is not supported on this write path (no tablet context)");
        }
        if (!(schema.keys_type() == UNIQUE_KEYS &&
              ctx.rowset_ctx->enable_unique_key_merge_on_write)) {
            auto msg = fmt::format(
                    "Can only do partial update on merge-on-write unique table, but found: "
                    "keys_type={}, enable_unique_key_merge_on_write={}, tablet_id={}",
                    schema.keys_type(), ctx.rowset_ctx->enable_unique_key_merge_on_write,
                    ctx.tablet->tablet_id());
            DCHECK(false) << msg;
            return Status::InternalError<false>(msg);
        }
        // partial update needs the segment id, which only flush_single_block sets
        if (ctx.segment_id < 0) {
            return Status::InternalError<false>(
                    "partial update blocks must be flushed through flush_single_block, "
                    "tablet_id={}",
                    ctx.tablet->tablet_id());
        }
        if (info->is_flexible_partial_update()) {
            if (block->columns() != schema.num_columns()) {
                return Status::InvalidArgument(
                        "illegal flexible partial update block columns, block columns = {}, "
                        "tablet_schema columns = {}",
                        block->dump_structure(), schema.dump_structure());
            }
        } else {
            DCHECK(info->is_fixed_partial_update());
            if (block->columns() < schema.num_key_columns() ||
                block->columns() >= schema.num_columns()) {
                return Status::InvalidArgument(fmt::format(
                        "illegal partial update block columns: {}, num key columns: {}, total "
                        "schema columns: {}",
                        block->columns(), schema.num_key_columns(), schema.num_columns()));
            }
        }
        return Status::OK();
    }
    std::string_view name() const override { return "Validate"; }
};

// Generates the hidden row-store column (each row as JSONB). A
// DerivedColumnGenerator so the vertical writer can stream it in batches.
class RowStoreColumnGenerator : public DerivedColumnGenerator {
public:
    RowStoreColumnGenerator(TabletSchemaSPtr schema, DataTypeSerDeSPtrs serdes)
            : _schema(std::move(schema)),
              _serdes(std::move(serdes)),
              _row_store_cids(_schema->row_columns_uids().begin(),
                              _schema->row_columns_uids().end()) {}

    size_t generate(const Block& block, size_t row_pos, size_t max_rows, size_t max_bytes,
                    IColumn* dst) const override {
        auto* dst_str = static_cast<ColumnString*>(dst);
        return JsonbSerializeUtil::block_to_jsonb(*_schema, block, *dst_str,
                                                  cast_set<int>(_schema->num_columns()), _serdes,
                                                  _row_store_cids, row_pos, max_rows, max_bytes);
    }

private:
    TabletSchemaSPtr _schema;
    DataTypeSerDeSPtrs _serdes;
    std::unordered_set<int32_t> _row_store_cids;
};

// Registers the row-store generator. Safe to run after VariantParse: the
// variant serde produces the same bytes whether the variant is parsed or not.
class RowStoreFillStage : public BlockTransform {
public:
    Status apply(TransformExecContext& ctx, Block* block) const override {
        if (block->rows() == 0) {
            return Status::OK();
        }
        const auto& schema = *ctx.tablet_schema;
        for (size_t i = 0; i < schema.num_columns(); ++i) {
            if (!schema.column(i).is_row_store_column()) {
                continue;
            }
            std::shared_ptr<const DerivedColumnGenerator> generator =
                    std::make_shared<RowStoreColumnGenerator>(
                            ctx.tablet_schema, create_data_type_serdes(block->get_data_types()));
            ctx.derived_column = std::make_pair(cast_set<uint32_t>(i), std::move(generator));
            break;
        }
        return Status::OK();
    }
    std::string_view name() const override { return "RowStoreFill"; }
};

} // namespace

BlockTransformChain build_transform_chain(const RowsetWriterContext& context) {
    if (context.write_type == DataWriteType::TYPE_COMPACTION) {
        return BlockTransformChain {};
    }
    if (context.write_binlog_opt().enable) {
        // binlog<row> sub-writer: only the source -> binlog-schema derivation;
        // binlog schemas have no variant or row-store columns. Plain (no probe)
        // for DUP and no-BEFORE upserts; MoW (with probe) for partial update or
        // the BEFORE image.
        if (binlog_needs_historical_lookup(context)) {
            return BlockTransformChain {{std::make_shared<MowRowBinlogDeriveStage>()}};
        }
        return BlockTransformChain {{std::make_shared<PlainRowBinlogDeriveStage>()}};
    }
    std::vector<std::shared_ptr<const BlockTransform>> stages;
    stages.push_back(std::make_shared<ValidateStage>());
    const bool is_partial_update_load = context.partial_update_info != nullptr &&
                                        context.partial_update_info->is_partial_update() &&
                                        context.write_type == DataWriteType::TYPE_DIRECT &&
                                        !context.is_transient_rowset_writer;
    if (is_partial_update_load) {
        // Parse runs after the fill, once on the full-width block: the fill's
        // probe only touches key columns, which are never variant.
        if (context.partial_update_info->is_fixed_partial_update()) {
            stages.push_back(std::make_shared<FixedPartialUpdateFillStage>());
        } else {
            stages.push_back(std::make_shared<FlexiblePartialUpdateFillStage>());
        }
    }
    stages.push_back(std::make_shared<VariantParseStage>());
    // Fill the row-store column for direct and schema-change writes (types may
    // have changed, so it must be rebuilt); for partial update it runs after
    // the fill.
    if (context.write_type == DataWriteType::TYPE_DIRECT ||
        context.write_type == DataWriteType::TYPE_SCHEMA_CHANGE) {
        stages.push_back(std::make_shared<RowStoreFillStage>());
    }
    return BlockTransformChain {std::move(stages)};
}

Status materialize_derived_columns(const DerivedColumn& derived_column, Block* block) {
    if (!derived_column.second) {
        return Status::OK();
    }
    const auto& [cid, generator] = derived_column;
    auto column_ptr = block->get_by_position(cid).column->clone_empty();
    size_t num_rows = block->rows();
    size_t pos = 0;
    while (pos < num_rows) {
        size_t rows = generator->generate(*block, pos, num_rows - pos,
                                          std::numeric_limits<size_t>::max(), column_ptr.get());
        DCHECK_GT(rows, 0);
        pos += rows;
    }
    block->replace_by_position(cid, std::move(column_ptr));
    return Status::OK();
}

} // namespace doris::segment_v2
