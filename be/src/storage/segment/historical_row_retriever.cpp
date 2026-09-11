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

#include "storage/segment/historical_row_retriever.h"

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/consts.h"
#include "common/logging.h" // LOG
#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/string_ref.h"
#include "runtime/exec_env.h"
#include "storage/binlog.h"
#include "storage/data_dir.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/key/row_key_encoder.h"
#include "storage/mow/historical_row_fetcher.h"
#include "storage/mow/key_probe.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/segment.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

using namespace ErrorCode;

namespace {

void insert_value_to_nullable_column(IColumn* dst_column, const IColumn& src_column, size_t pos) {
    auto* nullable_column = assert_cast<ColumnNullable*>(dst_column);
    if (is_column_nullable(src_column)) {
        nullable_column->insert_from(src_column, pos);
        return;
    }

    nullable_column->get_nested_column().insert_from(src_column, pos);
    nullable_column->get_null_map_data().push_back(0);
}

} // namespace

Status PrimaryKeyModelRowRetriever::init(const HistoricalRowRetrieverContext& context) {
    _context = context;
    _key_columns.resize(_context.tablet_schema->num_key_columns());
    _key_encoder = std::make_unique<RowKeyEncoder>(*_context.tablet_schema, /*mow=*/true);
    _row_fetcher = std::make_unique<HistoricalRowFetcher>(_context);
    return Status::OK();
}

PrimaryKeyModelRowRetriever::PrimaryKeyModelRowRetriever() = default;

PrimaryKeyModelRowRetriever::~PrimaryKeyModelRowRetriever() = default;

Status PrimaryKeyModelRowRetriever::retrieve_historical_row(const Int8* delete_sign_column_data,
                                                            size_t row_pos, size_t num_rows) {
    auto& tablet_schema = _context.tablet_schema;

    std::vector<RowsetSharedPtr> specified_rowsets;
    {
        std::shared_lock rlock(_context.tablet->get_header_lock());
        specified_rowsets = _mow_context->rowset_ptrs;
    }
    std::vector<std::unique_ptr<SegmentCacheHandle>> segment_caches(specified_rowsets.size());

    CHECK(_context.rowset_writer_ctx != nullptr);
    bool need_historical_value = _context.rowset_writer_ctx->write_binlog_opt()
                                         .write_binlog_config()
                                         .need_historical_value;
    // The lookup uses the tablet's latest schema, the delete-sign rule the source schema. Hold the
    // shared pointer: tablet_schema() hands out a copy and the probe only borrows it.
    TabletSchemaSPtr lookup_schema = _context.tablet->tablet_schema();
    MowKeyProbe probe = MowKeyProbe::for_row_binlog(_context.tablet.get(), lookup_schema.get(),
                                                    tablet_schema->has_sequence_col(), _mow_context,
                                                    need_historical_value);
    // the binlog retriever reports no partial update counters
    PartialUpdateStats discarded_stats;

    for (size_t block_pos = row_pos; block_pos < row_pos + num_rows; block_pos++) {
        // After converting to olap column, [0, num_rows) in the result column is corresponding to
        // [row_pos, row_pos + num_rows) in the original block
        size_t delta_pos = block_pos - row_pos;
        std::string key = encode_mow_key_invalidate_cache(
                *_key_encoder, _key_columns, _seq_column, delta_pos, _seq_column != nullptr,
                _context.tablet->tablet_id(), *tablet_schema, _context.write_type);

        // mark key with delete sign as deleted.
        bool have_delete_sign =
                (delete_sign_column_data != nullptr && delete_sign_column_data[block_pos] != 0);

        ProbeOutcome outcome = DORIS_TRY(probe.probe(key, /*segment_pos=*/0, _seq_column != nullptr,
                                                     have_delete_sign, specified_rowsets,
                                                     segment_caches, discarded_stats));
        if (outcome.result == KeyProbeResult::NOT_FOUND) {
            // it's an insert row
            _has_default_or_nullable = true;
            _use_default_or_null_flag.emplace_back(true);
            _operators.emplace_back(have_delete_sign ? ROW_BINLOG_DELETE : ROW_BINLOG_APPEND);
            continue;
        }
        if (outcome.use_default_or_null) {
            _has_default_or_nullable = true;
            _use_default_or_null_flag.emplace_back(true);
            _operators.emplace_back(ROW_BINLOG_DELETE);
        } else {
            // partial update should not contain invisible columns
            _use_default_or_null_flag.emplace_back(false);
            _row_fetcher->pin_rowset(outcome.rowset);
            // currently we think row_pos must be zero, so we won't consider row_pos > 0
            DCHECK(row_pos == 0);
            _row_fetcher->plan_fixed_read(outcome.loc, delta_pos);
            _operators.emplace_back(have_delete_sign ? ROW_BINLOG_DELETE : ROW_BINLOG_UPDATE);
        }
    }

    CHECK_EQ(_use_default_or_null_flag.size(), num_rows);

    return Status::OK();
}

// NOLINTNEXTLINE(readability-function-size,readability-function-cognitive-complexity): Keep the
// probe, aligned sidecar aggregation, and history-read plan in one snapshot-consistent operation.
Status PrimaryKeyModelRowRetriever::materialize_flexible_partial_update(
        Block* block, std::shared_ptr<MowContext> mow_context,
        std::vector<int64_t>* row_lsns) { // NOLINT(readability-non-const-parameter): in/out sidecar
    auto& tablet_schema = _context.tablet_schema;
    if (_context.partial_update_info == nullptr ||
        !_context.partial_update_info->is_flexible_partial_update()) {
        return Status::InvalidArgument(
                "flexible partial update materialization requires flexible partial update info");
    }
    if (block->columns() != tablet_schema->num_columns()) {
        return Status::InvalidArgument(
                "illegal flexible partial update block columns: {}, schema columns: {}",
                block->columns(), tablet_schema->num_columns());
    }

    _mow_context = std::move(mow_context);
    if (_mow_context == nullptr) {
        return Status::InvalidArgument(
                "flexible partial update materialization requires MOW context");
    }

    std::vector<RowsetSharedPtr> specified_rowsets;
    {
        std::shared_lock rlock(_context.tablet->get_header_lock());
        specified_rowsets = _mow_context->rowset_ptrs;
    }
    std::vector<std::unique_ptr<SegmentCacheHandle>> segment_caches(specified_rowsets.size());

    CHECK(_context.rowset_writer_ctx != nullptr);
    const bool need_historical_value = _context.rowset_writer_ctx->write_binlog_opt()
                                                   .write_binlog_config()
                                                   .need_historical_value;
    TabletSchemaSPtr lookup_schema = _context.tablet->tablet_schema();
    MowKeyProbe probe = MowKeyProbe::for_row_binlog(_context.tablet.get(), lookup_schema.get(),
                                                    tablet_schema->has_sequence_col(), _mow_context,
                                                    need_historical_value);
    BlockAggregator aggregator(*tablet_schema, _context.tablet, _mow_context,
                               *_context.partial_update_info, *_key_encoder, probe, *_row_fetcher);

    size_t num_rows = block->rows();
    std::vector<uint8_t> insert_after_delete_flags;
    RETURN_IF_ERROR(aggregator.aggregate_for_flexible_partial_update(
            block, num_rows, specified_rowsets, segment_caches, row_lsns,
            &insert_after_delete_flags));
    num_rows = block->rows();
    if (num_rows == 0) {
        return Status::OK();
    }
    DCHECK_EQ(insert_after_delete_flags.size(), num_rows);

    std::vector<IOlapColumnDataAccessor*> key_columns;
    RETURN_IF_ERROR(aggregator.convert_pk_columns(block, 0, num_rows, key_columns));
    IOlapColumnDataAccessor* seq_column = nullptr;
    RETURN_IF_ERROR(aggregator.convert_seq_column(block, 0, num_rows, seq_column));

    auto* skip_bitmaps =
            &get_mutable_skip_bitmap_column(block, tablet_schema->skip_bitmap_col_idx())
                     ->get_data();
    const auto* delete_signs = BaseTablet::get_delete_sign_column_data(*block, num_rows);
    DCHECK(delete_signs != nullptr);

    const bool schema_has_seq = tablet_schema->has_sequence_col();
    const int32_t seq_col_unique_id =
            schema_has_seq ? tablet_schema->column(tablet_schema->sequence_col_idx()).unique_id()
                           : -1;
    const int32_t delete_sign_col_unique_id =
            tablet_schema->column(tablet_schema->delete_sign_idx()).unique_id();
    Block full_block = tablet_schema->create_storage_block();
    for (size_t cid = 0; cid < tablet_schema->num_key_columns(); ++cid) {
        const auto& input_column = block->get_by_position(cid);
        auto& full_column = full_block.get_by_position(cid);
        full_column.column = input_column.column;
        full_column.type = input_column.type;
    }

    bool has_default_or_nullable = false;
    std::vector<bool> use_default_or_null_flag;
    use_default_or_null_flag.reserve(num_rows);
    PartialUpdateStats discarded_stats;
    for (size_t pos = 0; pos < num_rows; ++pos) {
        const bool row_has_seq =
                schema_has_seq && !skip_bitmaps->at(pos).contains(seq_col_unique_id);
        std::string key = _key_encoder->full_encode_primary_keys(key_columns, pos);
        if (row_has_seq) {
            _key_encoder->append_seq_suffix(&key, seq_column, pos);
        }
        const bool have_delete_sign = !skip_bitmaps->at(pos).contains(delete_sign_col_unique_id) &&
                                      delete_signs[pos] != 0;
        ProbeOutcome outcome =
                DORIS_TRY(probe.probe(key, /*segment_pos=*/pos, row_has_seq, have_delete_sign,
                                      specified_rowsets, segment_caches, discarded_stats));
        const bool insert_after_delete = insert_after_delete_flags[pos] != 0;
        DCHECK(!insert_after_delete || !have_delete_sign);
        if (outcome.result == KeyProbeResult::NOT_FOUND && !have_delete_sign) {
            RETURN_IF_ERROR(_context.partial_update_info->handle_new_key(
                    *tablet_schema,
                    [&]() -> std::string {
                        return block->dump_one_line(
                                pos, cast_set<int>(tablet_schema->num_key_columns()));
                    },
                    &skip_bitmaps->at(pos)));
        }

        const bool use_default_or_null = outcome.use_default_or_null || insert_after_delete;
        has_default_or_nullable |= use_default_or_null;
        use_default_or_null_flag.emplace_back(use_default_or_null);
        if (!use_default_or_null) {
            _row_fetcher->pin_rowset(outcome.rowset);
            _row_fetcher->plan_flexible_read(outcome.loc, pos, skip_bitmaps->at(pos));
            // The same location supplies the optional BEFORE image and the old delete sign used
            // to distinguish an append after a tombstone from an update.
            _row_fetcher->plan_fixed_read(outcome.loc, pos);
        } else if (insert_after_delete && outcome.result != KeyProbeResult::NOT_FOUND) {
            // The base writer treats this row as a new row for missing-column fill, but the net
            // CDC change still replaces the live row that existed before this load. Keep its
            // BEFORE image while preventing old values from leaking into AFTER.
            _row_fetcher->pin_rowset(outcome.rowset);
            _row_fetcher->plan_fixed_read(outcome.loc, pos);
        }
        _operators.emplace_back(
                outcome.result == KeyProbeResult::NOT_FOUND
                        ? (have_delete_sign ? ROW_BINLOG_DELETE : ROW_BINLOG_APPEND)
                        : (have_delete_sign ? ROW_BINLOG_DELETE : ROW_BINLOG_UPDATE));
    }

    RETURN_IF_ERROR(_row_fetcher->fill_non_primary_key_columns(
            *tablet_schema, full_block, use_default_or_null_flag, has_default_or_nullable,
            /*segment_start_pos=*/0, /*block_start_pos=*/0, block, skip_bitmaps));
    block->swap(full_block);
    DCHECK_EQ(_operators.size(), block->rows());
    DCHECK(row_lsns == nullptr || row_lsns->size() == block->rows());
    return Status::OK();
}

Status PrimaryKeyModelRowRetriever::build_after_block(Block* block, size_t row_pos,
                                                      size_t num_rows) {
    DCHECK_EQ(_use_default_or_null_flag.size(), num_rows);
    if (_context.partial_update_info == nullptr) {
        return Status::InternalError("partial update info is null");
    }
    return _row_fetcher->fill_missing_columns(
            *_context.tablet_schema, *block, _use_default_or_null_flag, _has_default_or_nullable,
            cast_set<uint32_t>(row_pos), block, &_old_delete_signs);
}

Status PrimaryKeyModelRowRetriever::build_before_block(Block* before_block,
                                                       const std::vector<uint32_t>& value_cids,
                                                       size_t /*row_pos*/, size_t num_rows) {
    auto& tablet_schema = _context.tablet_schema;

    if (num_rows == 0 || value_cids.empty()) {
        return Status::OK();
    }

    // Create block to hold historical values for value columns.
    Block old_value_block = tablet_schema->create_storage_block(value_cids);
    CHECK_EQ(value_cids.size(), old_value_block.columns());

    // key: logical row index in current batch; value: index in old_value_block
    std::map<uint32_t, uint32_t> read_index;
    RETURN_IF_ERROR(_row_fetcher->read_columns(*tablet_schema, value_cids, old_value_block,
                                               &read_index, /*force_read_old_delete_signs=*/true));
    RETURN_IF_ERROR(_fill_old_delete_signs(old_value_block, read_index, num_rows));

    {
        auto mutable_before_columns_guard = before_block->mutate_columns_scoped();
        auto& mutable_before_columns = mutable_before_columns_guard.mutable_columns();
        // Fill each row in before_block.
        for (uint32_t idx = 0; idx < num_rows; ++idx) {
            auto it = read_index.find(idx);
            if (it == read_index.end() || _old_delete_signs[idx] != 0) {
                // No live historical row, fill BEFORE with NULL.
                for (size_t i = 0; i < value_cids.size(); ++i) {
                    auto* nullable_column =
                            assert_cast<ColumnNullable*>(mutable_before_columns[i].get());
                    nullable_column->insert_many_defaults(1);
                }
                continue;
            }

            uint32_t pos_in_old_block = it->second;
            for (size_t i = 0; i < value_cids.size(); ++i) {
                insert_value_to_nullable_column(mutable_before_columns[i].get(),
                                                *old_value_block.get_by_position(i).column,
                                                pos_in_old_block);
            }
        }
    }
    return Status::OK();
}

Status PrimaryKeyModelRowRetriever::revise_operators_by_old_delete_sign(size_t num_rows) {
    if (_operators.empty() || _row_fetcher->empty()) {
        return Status::OK();
    }
    DCHECK_EQ(_operators.size(), num_rows);

    if (_old_delete_signs.empty()) {
        // If no BEFORE/missing column was read, read only old delete signs here.
        Block old_delete_sign_block;
        std::map<uint32_t, uint32_t> read_index;
        RETURN_IF_ERROR(_row_fetcher->read_columns(*_context.tablet_schema,
                                                   std::vector<uint32_t> {}, old_delete_sign_block,
                                                   &read_index,
                                                   /*force_read_old_delete_signs=*/true));
        RETURN_IF_ERROR(_fill_old_delete_signs(old_delete_sign_block, read_index, num_rows));
    }
    DCHECK_EQ(_old_delete_signs.size(), num_rows);

    for (size_t idx = 0; idx < num_rows; ++idx) {
        if (_operators[idx] == ROW_BINLOG_UPDATE && _old_delete_signs[idx] != 0) {
            _operators[idx] = ROW_BINLOG_APPEND;
        }
    }
    return Status::OK();
}

Status PrimaryKeyModelRowRetriever::_fill_old_delete_signs(
        const Block& old_value_block, const std::map<uint32_t, uint32_t>& read_index,
        size_t num_rows) {
    return _row_fetcher->fill_old_delete_signs(old_value_block, read_index, num_rows,
                                               &_old_delete_signs);
}

} // namespace doris::segment_v2
