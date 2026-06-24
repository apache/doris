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
#include "service/point_query_executor.h"
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

PrimaryKeyModelRowRetriever::~PrimaryKeyModelRowRetriever() = default;

Status PrimaryKeyModelRowRetriever::retrieve_historical_row(const Int8* delete_sign_column_data,
                                                            size_t row_pos, size_t num_rows) {
    auto* tablet = static_cast<Tablet*>(_context.tablet.get());
    auto& tablet_schema = _context.tablet_schema;

    DCHECK(_context.partial_update_info);

    std::vector<RowsetSharedPtr> specified_rowsets;
    {
        std::shared_lock rlock(_context.tablet->get_header_lock());
        specified_rowsets = _mow_context->rowset_ptrs;
    }
    std::vector<std::unique_ptr<SegmentCacheHandle>> segment_caches(specified_rowsets.size());

    CHECK(_context.rowset_writer_ctx != nullptr);
    bool write_before =
            _context.rowset_writer_ctx->write_binlog_opt().write_binlog_config().write_before;
    // The lookup uses the tablet's latest schema while the delete-sign decision
    // uses the source schema; if row binlog needs the BEFORE image, delete rows
    // must still read historical values so __BEFORE__* columns can be filled in,
    // and rows whose sequence value loses still read the old row that is kept.
    TabletSchemaSPtr lookup_schema = tablet->tablet_schema();
    MowKeyProbe probe(_context.tablet.get(), lookup_schema.get(), tablet_schema->has_sequence_col(),
                      _mow_context, RowsetId {}, 0,
                      MowKeyProbe::Policy {
                              .delete_bitmap_mode = MowKeyProbe::DeleteBitmapMode::READ_ONLY,
                              .skip_delete_sign = !write_before,
                              .skip_seq_loses = false,
                      });

    // The binlog retriever doesn't report partial-update counts; discard them.
    PartialUpdateStats discarded;
    for (size_t block_pos = row_pos; block_pos < row_pos + num_rows; block_pos++) {
        // After converting to olap column, [0, num_rows) in the result column is corresponding to
        // [row_pos, row_pos + num_rows) in the original block
        size_t delta_pos = block_pos - row_pos;
        std::string key = _key_encoder->full_encode_primary_keys(_key_columns, delta_pos);

        MowKeyProbe::maybe_invalidate_row_cache(_context.tablet->tablet_id(),
                                                *_context.tablet_schema, _context.write_type, key);
        if (_seq_column != nullptr) {
            _key_encoder->append_seq_suffix(&key, _seq_column, delta_pos);
        }

        // mark key with delete sign as deleted.
        bool have_delete_sign =
                (delete_sign_column_data != nullptr && delete_sign_column_data[block_pos] != 0);

        ProbeOutcome out = DORIS_TRY(probe.probe(key, /*segment_pos=*/0, _seq_column != nullptr,
                                                 have_delete_sign, specified_rowsets,
                                                 segment_caches, discarded));
        if (out.result == KeyProbeResult::NOT_FOUND) {
            // it's an insert row
            _has_default_or_nullable = true;
            _use_default_or_null_flag.emplace_back(true);
            _operators.emplace_back(have_delete_sign ? ROW_BINLOG_DELETE : ROW_BINLOG_APPEND);
            continue;
        }
        if (out.use_default_or_null) {
            _has_default_or_nullable = true;
            _use_default_or_null_flag.emplace_back(true);
            _operators.emplace_back(ROW_BINLOG_DELETE);
        } else {
            _use_default_or_null_flag.emplace_back(false);
            _row_fetcher->pin_rowset(out.rowset);
            // currently we think row_pos must be zero, so we won't consider row_pos > 0
            DCHECK(row_pos == 0);
            _row_fetcher->plan_fixed_read(out.loc, delta_pos);
            _operators.emplace_back(have_delete_sign ? ROW_BINLOG_DELETE : ROW_BINLOG_UPDATE);
        }
    }

    CHECK_EQ(_use_default_or_null_flag.size(), num_rows);

    return Status::OK();
}

Status PrimaryKeyModelRowRetriever::build_after_block(Block* block, size_t row_pos,
                                                      size_t num_rows) {
    DCHECK_EQ(_use_default_or_null_flag.size(), num_rows);
    if (config::is_cloud_mode()) {
        return Status::NotSupported("fill_missing_columns");
    }
    if (_context.partial_update_info == nullptr) {
        return Status::InternalError("partial update info is null");
    }
    return _row_fetcher->fill_missing_columns(*_context.tablet_schema, *block,
                                              _use_default_or_null_flag, _has_default_or_nullable,
                                              cast_set<uint32_t>(row_pos), block);
}

Status PrimaryKeyModelRowRetriever::build_before_block(Block* before_block,
                                                       const std::vector<uint32_t>& value_cids,
                                                       size_t /*row_pos*/, size_t num_rows) const {
    if (config::is_cloud_mode()) {
        // TODO(plat1ko): cloud mode
        return Status::NotSupported("fill_before_columns");
    }

    auto& tablet_schema = _context.tablet_schema;

    if (num_rows == 0 || value_cids.empty()) {
        return Status::OK();
    }

    // Create block to hold historical values for value columns.
    Block old_value_block = tablet_schema->create_block_by_cids(value_cids);
    CHECK_EQ(value_cids.size(), old_value_block.columns());

    // key: logical row index in current batch; value: index in old_value_block
    std::map<uint32_t, uint32_t> read_index;
    RETURN_IF_ERROR(_row_fetcher->read_columns(*tablet_schema, value_cids, old_value_block,
                                               &read_index, false, nullptr));

    {
        auto mutable_before_columns_guard = before_block->mutate_columns_scoped();
        auto& mutable_before_columns = mutable_before_columns_guard.mutable_columns();
        // Fill each row in before_block.
        for (uint32_t idx = 0; idx < num_rows; ++idx) {
            auto it = read_index.find(idx);
            if (it == read_index.end()) {
                // No historical row, fill BEFORE with NULL.
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

} // namespace doris::segment_v2
