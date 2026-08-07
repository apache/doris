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

#include "storage/iterator/block_reader.h"

#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <stdint.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <ostream>
#include <string>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "cloud/config.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/status.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "exprs/aggregate/aggregate_function_reader.h"
#include "exprs/function_filter.h"
#include "runtime/runtime_state.h"
#include "storage/binlog.h"
#include "storage/iterator/binlog_block_reader_utils.h"
#include "storage/iterator/vcollect_iterator.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/predicate/like_column_predicate.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
class ColumnPredicate;
} // namespace doris

namespace doris {
using namespace ErrorCode;

static constexpr int32_t BLOCK_SIZE_CHECK_INTERVAL_ROWS = 64;

BlockReader::~BlockReader() {
    for (int i = 0; i < _agg_functions.size(); ++i) {
        _agg_functions[i]->destroy(_agg_places[i]);
        delete[] _agg_places[i];
    }
}

Status BlockReader::next_block_with_aggregation(Block* block, bool* eof) {
    auto res = (this->*_next_block_func)(block, eof);
    if (!config::is_cloud_mode()) {
        if (!res.ok()) [[unlikely]] {
            static_cast<Tablet*>(_tablet.get())->report_error(res);
        }
    }
    return res;
}

int64_t BlockReader::_read_binlog_op(const IColumn& col, size_t row) const {
    const IColumn* cur = &col;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*cur)) {
        if (nullable->is_null_at(row)) {
            return binlog::ROW_BINLOG_UNKNOWN;
        }
        cur = &nullable->get_nested_column();
    }

    if (const auto* int64_col = check_and_get_column<ColumnInt64>(*cur)) {
        return int64_col->get_element(row);
    }

    return binlog::ROW_BINLOG_UNKNOWN;
}

Status BlockReader::_write_binlog_op(IColumn& col, int64_t op) const {
    IColumn* cur = &col;
    ColumnNullable* nullable = nullptr;
    if (auto* n = typeid_cast<ColumnNullable*>(cur)) {
        nullable = n;
        cur = &nullable->get_nested_column();
    }

    if (auto* int64_col = typeid_cast<ColumnInt64*>(cur)) {
        int64_col->insert_value(op);
    } else {
        return Status::InternalError("invalid column type");
    }

    if (nullable != nullptr) {
        nullable->get_null_map_data().push_back(0);
    }
    return Status::OK();
}

// Resolves which source-block column to read from for a given binlog row position.
// When use_before is true, return the ordinal of its __BEFORE__ mirror.
// Binlog meta columns map to themselves.
uint32_t BlockReader::_resolve_source_column_ordinal(uint32_t ordinal, bool use_before) const {
    return use_before ? _read_schema->before_column_ordinal(ordinal) : ordinal;
}

void BlockReader::_init_pending_row_columns(const Block& block) {
    if (!_pending_row_columns.empty()) {
        return;
    }
    _pending_row_columns = block.clone_empty_columns();
}

// Drains the carry-over row produced on the previous batch boundary into the current
// output block. Returns true if a row was emitted, false if no pending row exists.
bool BlockReader::_emit_pending_row(MutableColumns& target_columns, size_t& output_row_count) {
    if (!_has_pending_row) {
        return false;
    }
    for (size_t i = 0; i < _pending_row_columns.size(); ++i) {
        target_columns[i]->insert_from(*_pending_row_columns[i], 0);
        _pending_row_columns[i]->clear();
    }
    _has_pending_row = false;
    output_row_count++;
    return true;
}

// Copies one source row into target_columns with the given output op code, picking BEFORE
// or AFTER values per column according to use_before. Used by _detail_change_next_block to
// materialize the BEFORE / AFTER halves of an UPDATE pair (and INSERT / DELETE singletons).
Status BlockReader::_append_change_row(MutableColumns& target_columns, const Block& src_block,
                                       size_t row_pos, int64_t output_op, bool use_before) {
    const int32_t op_ordinal = _read_schema->op_ordinal();
    for (size_t ordinal = 0; ordinal < target_columns.size(); ++ordinal) {
        if (static_cast<int32_t>(ordinal) == op_ordinal) {
            RETURN_IF_ERROR(_write_binlog_op(*target_columns[ordinal], output_op));
            continue;
        }
        auto source_ordinal =
                _resolve_source_column_ordinal(static_cast<uint32_t>(ordinal), use_before);
        target_columns[ordinal]->insert_from(*src_block.get_by_position(source_ordinal).column,
                                             row_pos);
    }
    return Status::OK();
}

// MIN_DELTA reader: groups consecutive rows sharing the same primary key in
// _stored_data_columns, then collapses the group into the minimum equivalent change
// (SKIP / INSERT / DELETE / UPDATE_BEFORE+AFTER) via AggregateFunctionMinDelta.
Status BlockReader::_min_delta_next_block(Block* block, bool* eof) {
    if (UNLIKELY(_eof && !_has_pending_row)) {
        *eof = true;
        return Status::OK();
    }

    if (_stored_data_columns.empty()) {
        _stored_data_columns = _next_row.block->clone_empty_columns();
    }

    auto target_columns_guard = block->mutate_columns_scoped();
    auto& target_columns = target_columns_guard.mutable_columns();
    size_t output_row_count = 0;
    _init_pending_row_columns(*block);
    const int32_t tso_ordinal = _read_schema->tso_ordinal();
    const int32_t lsn_ordinal = _read_schema->lsn_ordinal();
    const int32_t op_ordinal = _read_schema->op_ordinal();
    while (output_row_count < batch_max_rows()) {
        if (_emit_pending_row(target_columns, output_row_count)) {
            continue;
        }
        if (_eof) {
            break;
        }
        bool need_pop = _stored_data_columns[0]->size() > 1;
        for (size_t i = 0; i < _stored_data_columns.size(); ++i) {
            if (need_pop) {
                _stored_data_columns[i]->pop_back(1);
            }
            _stored_data_columns[i]->insert_from(*_next_row.block->get_by_position(i).column,
                                                 _next_row.row_pos);
        }
        auto res = _vcollect_iter.next(&_next_row);
        if (UNLIKELY(res.is<END_OF_FILE>())) {
            _eof = true;
        } else if (UNLIKELY(!res.ok())) {
            return res;
        }

        if (!_eof && _next_row.is_same) {
            continue;
        }
        size_t group_size = _stored_data_columns[0]->size();
        auto first_op = _read_binlog_op(*_stored_data_columns[op_ordinal], 0);
        auto last_op = _read_binlog_op(*_stored_data_columns[op_ordinal], group_size - 1);
        auto result = binlog::AggregateFunctionMinDelta::calculate_result(first_op, last_op);
        switch (result) {
        case binlog::AggregateFunctionMinDelta::ResultType::SKIP:
            break;
        case binlog::AggregateFunctionMinDelta::ResultType::INSERT:
            for (size_t ordinal = 0; ordinal < target_columns.size(); ++ordinal) {
                if (static_cast<int32_t>(ordinal) == op_ordinal) {
                    RETURN_IF_ERROR(_write_binlog_op(*target_columns[ordinal],
                                                     binlog::STREAM_CHANGE_INSERT));
                } else {
                    // insert should use most recent value
                    target_columns[ordinal]->insert_from(*_stored_data_columns[ordinal],
                                                         group_size - 1);
                }
            }
            output_row_count++;
            break;
        case binlog::AggregateFunctionMinDelta::ResultType::DELETE:
            for (size_t ordinal = 0; ordinal < target_columns.size(); ++ordinal) {
                const auto read_ordinal = static_cast<int32_t>(ordinal);
                if (read_ordinal == op_ordinal) {
                    RETURN_IF_ERROR(_write_binlog_op(*target_columns[ordinal],
                                                     binlog::STREAM_CHANGE_DELETE));
                } else if (read_ordinal == lsn_ordinal || read_ordinal == tso_ordinal) {
                    target_columns[ordinal]->insert_from(*_stored_data_columns[ordinal],
                                                         group_size - 1);
                } else {
                    // delete should use first op value
                    auto source_ordinal =
                            _resolve_source_column_ordinal(static_cast<uint32_t>(ordinal), true);
                    target_columns[ordinal]->insert_from(*_stored_data_columns[source_ordinal], 0);
                }
            }
            output_row_count++;
            break;
        case binlog::AggregateFunctionMinDelta::ResultType::UPDATE_BEFORE_AFTER:
            for (size_t ordinal = 0; ordinal < target_columns.size(); ++ordinal) {
                if (static_cast<int32_t>(ordinal) == op_ordinal) {
                    RETURN_IF_ERROR(_write_binlog_op(*target_columns[ordinal],
                                                     binlog::STREAM_CHANGE_UPDATE_BEFORE));
                } else {
                    auto source_ordinal =
                            _resolve_source_column_ordinal(static_cast<uint32_t>(ordinal), true);
                    target_columns[ordinal]->insert_from(*_stored_data_columns[source_ordinal], 0);
                }
            }
            output_row_count++;
            if (output_row_count >= batch_max_rows()) {
                for (size_t ordinal = 0; ordinal < target_columns.size(); ++ordinal) {
                    if (static_cast<int32_t>(ordinal) == op_ordinal) {
                        RETURN_IF_ERROR(_write_binlog_op(*_pending_row_columns[ordinal],
                                                         binlog::STREAM_CHANGE_UPDATE_AFTER));
                    } else {
                        _pending_row_columns[ordinal]->insert_from(*_stored_data_columns[ordinal],
                                                                   group_size - 1);
                    }
                }
                _has_pending_row = true;
            } else {
                for (size_t ordinal = 0; ordinal < target_columns.size(); ++ordinal) {
                    if (static_cast<int32_t>(ordinal) == op_ordinal) {
                        RETURN_IF_ERROR(_write_binlog_op(*target_columns[ordinal],
                                                         binlog::STREAM_CHANGE_UPDATE_AFTER));
                    } else {
                        target_columns[ordinal]->insert_from(*_stored_data_columns[ordinal],
                                                             group_size - 1);
                    }
                }
                output_row_count++;
            }
            break;
        }

        for (auto& col : _stored_data_columns) {
            col->clear();
        }
    }
    *eof = _eof && !_has_pending_row;
    return Status::OK();
}

// DETAIL reader: emits every recorded binlog change verbatim. APPEND -> single INSERT row,
// DELETE -> single DELETE row, UPDATE -> a BEFORE+AFTER pair. When the AFTER row would
// overflow batch_max_rows(), it is parked in _pending_row_columns and flushed next call.
Status BlockReader::_detail_change_next_block(Block* block, bool* eof) {
    if (UNLIKELY(_eof && !_has_pending_row)) {
        *eof = true;
        return Status::OK();
    }
    auto target_columns_guard = block->mutate_columns_scoped();
    auto& target_columns = target_columns_guard.mutable_columns();
    size_t output_row_count = 0;
    _init_pending_row_columns(*block);
    const int32_t op_ordinal = _read_schema->op_ordinal();
    while (output_row_count < batch_max_rows()) {
        if (_emit_pending_row(target_columns, output_row_count)) {
            continue;
        }
        if (_eof) {
            break;
        }
        if (UNLIKELY(_next_row.block == nullptr)) {
            return Status::InternalError("invalid row reference in detail change reader");
        }
        const Block& source_block = *_next_row.block;
        const size_t row = _next_row.row_pos;
        int64_t op = _read_binlog_op(*source_block.get_by_position(op_ordinal).column, row);
        if (op == ROW_BINLOG_UPDATE) {
            RETURN_IF_ERROR(_append_change_row(target_columns, source_block, row,
                                               binlog::STREAM_CHANGE_UPDATE_BEFORE, true));
            output_row_count++;
            if (output_row_count >= batch_max_rows()) {
                RETURN_IF_ERROR(_append_change_row(_pending_row_columns, source_block, row,
                                                   binlog::STREAM_CHANGE_UPDATE_AFTER, false));
                _has_pending_row = true;
            } else {
                RETURN_IF_ERROR(_append_change_row(target_columns, source_block, row,
                                                   binlog::STREAM_CHANGE_UPDATE_AFTER, false));
                output_row_count++;
            }
        } else if (op == ROW_BINLOG_APPEND) {
            RETURN_IF_ERROR(_append_change_row(target_columns, source_block, row,
                                               binlog::STREAM_CHANGE_INSERT, false));
            output_row_count++;
        } else if (op == ROW_BINLOG_DELETE) {
            RETURN_IF_ERROR(_append_change_row(target_columns, source_block, row,
                                               binlog::STREAM_CHANGE_DELETE, true));
            output_row_count++;
        }

        auto res = _vcollect_iter.next(&_next_row);
        if (UNLIKELY(res.is<END_OF_FILE>())) {
            _eof = true;
        } else if (UNLIKELY(!res.ok())) {
            return res;
        }
    }
    *eof = _eof && !_has_pending_row;
    return Status::OK();
}

bool BlockReader::_rowsets_not_mono_asc_disjoint(const ReaderParams& read_params) {
    std::string pre_rs_last_key;
    bool pre_rs_key_bounds_truncated {false};
    const std::vector<RowSetSplits>& rs_splits = read_params.rs_splits;
    for (const auto& rs_split : rs_splits) {
        if (rs_split.rs_reader->rowset()->num_rows() == 0) {
            continue;
        }
        if (rs_split.rs_reader->rowset()->is_segments_overlapping()) {
            return true;
        }
        std::string rs_first_key;
        bool has_first_key = rs_split.rs_reader->rowset()->first_key(&rs_first_key);
        if (!has_first_key) {
            return true;
        }
        bool cur_rs_key_bounds_truncated {
                rs_split.rs_reader->rowset()->is_segments_key_bounds_truncated()};
        if (!Slice::lhs_is_strictly_less_than_rhs(Slice {pre_rs_last_key},
                                                  pre_rs_key_bounds_truncated, Slice {rs_first_key},
                                                  cur_rs_key_bounds_truncated)) {
            return true;
        }
        bool has_last_key = rs_split.rs_reader->rowset()->last_key(&pre_rs_last_key);
        pre_rs_key_bounds_truncated = cur_rs_key_bounds_truncated;
        CHECK(has_last_key);
    }
    return false;
}

Status BlockReader::_init_collect_iter(const ReaderParams& read_params) {
    auto res = _capture_rs_readers(read_params);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init reader when _capture_rs_readers. res:" << res
                     << ", tablet_id:" << read_params.tablet->tablet_id()
                     << ", schema_hash:" << read_params.tablet->schema_hash()
                     << ", reader_type:" << int(read_params.reader_type)
                     << ", version:" << read_params.version;
        return res;
    }
    // check if rowsets are noneoverlapping
    {
        SCOPED_RAW_TIMER(&_stats.block_reader_vcollect_iter_init_timer_ns);
        _is_rowsets_overlapping = _rowsets_not_mono_asc_disjoint(read_params);
        const bool is_min_delta_stream = read_params.binlog_scan_type == TBinlogScanType::MIN_DELTA;
        const bool force_merge = read_params.read_orderby_key || is_min_delta_stream;
        const bool is_reverse = !is_min_delta_stream && read_params.read_orderby_key_reverse;
        _vcollect_iter.init(this, _is_rowsets_overlapping, force_merge, is_reverse);
    }

    std::vector<RowsetReaderSharedPtr> valid_rs_readers;
    RuntimeState* runtime_state = read_params.runtime_state;

    {
        SCOPED_RAW_TIMER(&_stats.block_reader_rs_readers_init_timer_ns);
        for (int i = 0; i < read_params.rs_splits.size(); ++i) {
            if (runtime_state != nullptr && runtime_state->is_cancelled()) {
                return runtime_state->cancel_reason();
            }

            auto& rs_split = read_params.rs_splits[i];

            // _vcollect_iter.topn_next() will init rs_reader by itself
            if (!_vcollect_iter.use_topn_next()) {
                RETURN_IF_ERROR(rs_split.rs_reader->init(&_reader_context, rs_split));
            }

            Status res1 = _vcollect_iter.add_child(rs_split);
            if (!res1.ok() && !res1.is<END_OF_FILE>()) {
                LOG(WARNING) << "failed to add child to iterator, err=" << res1;
                return res1;
            }
            if (res1.ok()) {
                valid_rs_readers.push_back(rs_split.rs_reader);
            }
        }
    }
    {
        SCOPED_RAW_TIMER(&_stats.block_reader_build_heap_init_timer_ns);
        RETURN_IF_ERROR(_vcollect_iter.build_heap(valid_rs_readers));
        // _vcollect_iter.topn_next() can not use current_row
        if (!_vcollect_iter.use_topn_next()) {
            auto status = _vcollect_iter.current_row(&_next_row);
            _eof = status.is<END_OF_FILE>();
        }
    }

    return Status::OK();
}

Status BlockReader::_init_agg_state(const ReaderParams& read_params) {
    if (_eof) {
        return Status::OK();
    }

    auto stored_block = _next_row.block->create_same_struct_block(batch_max_rows());
    _stored_data_columns = std::move(*stored_block).mutate_columns();

    _stored_has_null_tag.resize(_stored_data_columns.size());
    _stored_has_variable_length_tag.resize(_stored_data_columns.size());

    for (auto idx : _agg_columns_idx) {
        const auto& column = *_read_schema->column(idx);
        AggregateFunctionPtr function =
                column.get_aggregate_function(AGG_READER_SUFFIX, read_params.get_be_exec_version());

        // Every aggregate value column must resolve its reader aggregate function.
        if (!function) {
            return Status::InternalError(
                    "Failed to init reader when init agg state: "
                    "tablet_id: {}, schema_hash: {}, reader_type: {}, version: {}",
                    read_params.tablet->tablet_id(), read_params.tablet->schema_hash(),
                    int(read_params.reader_type), read_params.version.to_string());
        }
        const auto* column_ptr = _stored_data_columns[idx].get();
        const IColumn* columns[] = {column_ptr};
        function->check_input_columns_type(columns);
        function->check_result_column_type(*column_ptr);
        _agg_functions.push_back(function);
        // create aggregate data
        AggregateDataPtr place = new char[function->size_of_data()];
        SAFE_CREATE(function->create(place), {
            _agg_functions.pop_back();
            delete[] place;
        });
        _agg_places.push_back(place);

        // calculate `_has_variable_length_tag` tag. like string, array, map
        _stored_has_variable_length_tag[idx] = _stored_data_columns[idx]->is_variable_length();
    }

    return Status::OK();
}

Status BlockReader::init(const ReaderParams& read_params) {
    SCOPED_RAW_TIMER(&_stats.tablet_reader_init_timer_ns);
    RETURN_IF_ERROR(TabletReader::init(read_params));

    const bool use_sequence_map = _tablet_schema->has_seq_map() &&
                                  _tablet_schema->keys_type() == UNIQUE_KEYS && !_direct_mode &&
                                  read_params.binlog_scan_type != TBinlogScanType::MIN_DELTA &&
                                  read_params.binlog_scan_type != TBinlogScanType::DETAIL &&
                                  !(read_params.reader_type == ReaderType::READER_QUERY &&
                                    _tablet->enable_unique_key_merge_on_write());
    if (use_sequence_map) {
        auto read_schema = std::make_shared<ReadSchema>(*_read_schema);
        RETURN_IF_ERROR(read_schema->init_sequence_map(*_tablet_schema));
        _read_schema = std::move(read_schema);
    }

    // Every merge and caller Block has the exact read-schema layout. Cache only
    // the non-key columns of AGG tables that go through the aggregate machinery.
    const auto num_block_columns = static_cast<uint32_t>(_read_schema->num_block_columns());
    const bool is_agg_keys = _tablet->keys_type() == AGG_KEYS;
    for (uint32_t ordinal = 0; ordinal < num_block_columns; ++ordinal) {
        if (is_agg_keys && !_read_schema->column(ordinal)->is_key()) {
            _agg_columns_idx.emplace_back(ordinal);
        }
    }

    auto status = _init_collect_iter(read_params);
    if (!status.ok()) [[unlikely]] {
        if (!config::is_cloud_mode()) {
            static_cast<Tablet*>(_tablet.get())->report_error(status);
        }
        return status;
    }

    // MIN_DELTA: collapse consecutive same-key changes to the minimum equivalent change set
    // (e.g. INSERT+DELETE -> SKIP, INSERT+UPDATE -> INSERT). Reduces downstream traffic.
    if (read_params.binlog_scan_type == TBinlogScanType::MIN_DELTA) {
        _next_block_func = &BlockReader::_min_delta_next_block;
        return Status::OK();
    }
    // DETAIL: emit every recorded change as-is, with BEFORE+AFTER rows for UPDATE.
    // Used when the consumer needs full change history rather than the net delta.
    if (read_params.binlog_scan_type == TBinlogScanType::DETAIL) {
        _next_block_func = &BlockReader::_detail_change_next_block;
        return Status::OK();
    }

    if (_direct_mode) {
        _next_block_func = &BlockReader::_direct_next_block;
        return Status::OK();
    }
    switch (_tablet_schema->keys_type()) {
    case KeysType::DUP_KEYS:
        _next_block_func = &BlockReader::_direct_next_block;
        break;
    case KeysType::UNIQUE_KEYS:
        if (read_params.reader_type == ReaderType::READER_QUERY &&
            _reader_context.enable_unique_key_merge_on_write) {
            _next_block_func = &BlockReader::_direct_next_block;
        } else if (use_sequence_map) {
            _next_block_func = &BlockReader::_replace_key_next_block;
        } else {
            _next_block_func = &BlockReader::_unique_key_next_block;
            if (_filter_delete) {
                _delete_filter_column = ColumnUInt8::create();
            }
        }
        break;
    case KeysType::AGG_KEYS:
        _next_block_func = &BlockReader::_agg_key_next_block;
        RETURN_IF_ERROR(_init_agg_state(read_params));
        break;
    default:
        DCHECK(false) << "No next row function for type:" << _tablet_schema->keys_type();
        break;
    }

    return Status::OK();
}

Status BlockReader::_direct_next_block(Block* block, bool* eof) {
    auto res = _vcollect_iter.next(block);
    if (UNLIKELY(!res.ok() && !res.is<END_OF_FILE>())) {
        return res;
    }
    *eof = res.is<END_OF_FILE>();
    _eof = *eof;
    if (UNLIKELY(_reader_context.record_rowids)) {
        res = _vcollect_iter.current_block_row_locations(&_block_row_locations);
        if (UNLIKELY(!res.ok() && res != Status::Error<END_OF_FILE>(""))) {
            return res;
        }
        DCHECK_EQ(_block_row_locations.size(), block->rows());
    }
    return Status::OK();
}

Status BlockReader::_replace_key_next_block(Block* block, bool* eof) {
    if (UNLIKELY(_eof)) {
        *eof = true;
        return Status::OK();
    }

    auto target_block_row = 0;
    auto target_columns_guard = block->mutate_columns_scoped();
    auto& target_columns = target_columns_guard.mutable_columns();
    // currently seq mapping only support mor table
    // so this will not be executed for the time being
    if (UNLIKELY(_reader_context.record_rowids)) {
        _block_row_locations.resize(batch_max_rows());
    }
    auto merged_row = 0;
    while (target_block_row < batch_max_rows() && !_eof) {
        RETURN_IF_ERROR(_insert_data_normal(target_columns));
        if (UNLIKELY(_reader_context.record_rowids)) {
            _block_row_locations[target_block_row] = _vcollect_iter.current_row_location();
        }
        target_block_row++;

        while (!_eof) {
            // the version is in reverse order, the first row is the highest version,
            // in UNIQUE_KEY highest version is the final result
            auto res = _vcollect_iter.next(&_next_row);
            if (UNLIKELY(res.is<END_OF_FILE>())) {
                _eof = true;
                *eof = true;
                if (UNLIKELY(_reader_context.record_rowids)) {
                    _block_row_locations.resize(target_block_row);
                }
                break;
            }

            if (UNLIKELY(!res.ok())) {
                LOG(WARNING) << "next failed: " << res;
                return res;
            }

            if (_next_row.is_same) {
                merged_row++;
                _compare_sequence_map_and_replace(target_columns);
            } else {
                break;
            }
        }
        // Byte-budget check: after the inner loop _next_row is either EOF or the next different
        // key, so it is safe to stop accumulating here without repeating any row.
        if (target_block_row % BLOCK_SIZE_CHECK_INTERVAL_ROWS == 0 &&
            _reached_byte_budget(target_columns)) {
            if (UNLIKELY(_reader_context.record_rowids)) {
                _block_row_locations.resize(target_block_row);
            }
            break;
        }
    }
    _merged_rows += merged_row;
    return Status::OK();
}

bool BlockReader::_reached_byte_budget(const MutableColumns& columns) const {
    return config::enable_adaptive_batch_size && _reader_context.preferred_block_size_bytes > 0 &&
           Block::columns_byte_size(columns) >= _reader_context.preferred_block_size_bytes;
}

void BlockReader::_compare_sequence_map_and_replace(MutableColumns& columns) {
    auto src_block = _next_row.block.get();
    auto src_pos = _next_row.row_pos;

    for (const auto& [seq_ordinal, value_ordinals] : _read_schema->sequence_map()) {
        auto* dst_seq_column = columns[seq_ordinal].get();
        auto dst_pos = dst_seq_column->size() - 1;
        auto src_seq_column = src_block->get_by_position(seq_ordinal).column;
        // the rowset version of dst is higher .
        auto res = dst_seq_column->compare_at(dst_pos, src_pos, *src_seq_column, -1);
        if (res >= 0) {
            continue;
        }

        // update value and seq column
        for (auto value_ordinal : value_ordinals) {
            auto src_column = src_block->get_by_position(value_ordinal).column;
            auto* dst_column = columns[value_ordinal].get();
            dst_column->pop_back(1);
            dst_column->insert_from(*src_column, src_pos);
        }

        dst_seq_column->pop_back(1);
        dst_seq_column->insert_from(*src_seq_column, src_pos);
    }
}

Status BlockReader::_agg_key_next_block(Block* block, bool* eof) {
    if (UNLIKELY(_eof)) {
        *eof = true;
        return Status::OK();
    }

    auto target_block_row = 0;
    auto merged_row = 0;
    auto target_columns_guard = block->mutate_columns_scoped();
    auto& target_columns = target_columns_guard.mutable_columns();
    RETURN_IF_ERROR(_insert_data_normal(target_columns));
    target_block_row++;
    _append_agg_data(target_columns);

    while (true) {
        auto res = _vcollect_iter.next(&_next_row);
        if (UNLIKELY(res.is<END_OF_FILE>())) {
            _eof = true;
            *eof = true;
            break;
        }
        if (UNLIKELY(!res.ok())) {
            LOG(WARNING) << "next failed: " << res;
            return res;
        }

        if (!_next_row.is_same) {
            if (target_block_row == batch_max_rows()) {
                break;
            }
            // Byte-budget check at group boundary: _next_row is the first row of the new group
            // and is still pending (not yet inserted), so stopping here is safe.
            if (target_block_row % BLOCK_SIZE_CHECK_INTERVAL_ROWS == 0 &&
                _reached_byte_budget(target_columns)) {
                break;
            }

            _agg_data_counters.push_back(_last_agg_data_counter);
            _last_agg_data_counter = 0;

            RETURN_IF_ERROR(_insert_data_normal(target_columns));
            target_block_row++;
        } else {
            merged_row++;
        }

        _append_agg_data(target_columns);
    }

    _agg_data_counters.push_back(_last_agg_data_counter);
    _last_agg_data_counter = 0;
    _update_agg_data(target_columns);

    _merged_rows += merged_row;
    return Status::OK();
}

Status BlockReader::_unique_key_next_block(Block* block, bool* eof) {
    if (UNLIKELY(_eof)) {
        *eof = true;
        return Status::OK();
    }

    auto target_block_row = 0;
    auto target_columns_guard = block->mutate_columns_scoped();
    auto& target_columns = target_columns_guard.mutable_columns();
    if (UNLIKELY(_reader_context.record_rowids)) {
        _block_row_locations.resize(batch_max_rows());
    }

    do {
        RETURN_IF_ERROR(_insert_data_normal(target_columns));

        if (UNLIKELY(_reader_context.record_rowids)) {
            _block_row_locations[target_block_row] = _vcollect_iter.current_row_location();
        }
        target_block_row++;

        // the version is in reverse order, the first row is the highest version,
        // in UNIQUE_KEY highest version is the final result, there is no need to
        // merge the lower versions
        auto res = _vcollect_iter.next(&_next_row);
        if (UNLIKELY(res.is<END_OF_FILE>())) {
            _eof = true;
            *eof = true;
            if (UNLIKELY(_reader_context.record_rowids)) {
                _block_row_locations.resize(target_block_row);
            }
            break;
        }

        if (UNLIKELY(!res.ok())) {
            LOG(WARNING) << "next failed: " << res;
            return res;
        }
        // Byte-budget check: _next_row is already saved so stopping here is safe.
        if (target_block_row % BLOCK_SIZE_CHECK_INTERVAL_ROWS == 0 &&
            _reached_byte_budget(target_columns)) {
            if (UNLIKELY(_reader_context.record_rowids)) {
                _block_row_locations.resize(target_block_row);
            }
            break;
        }
    } while (target_block_row < batch_max_rows());

    if (_delete_sign_available) {
        int delete_sign_idx = _read_schema->delete_sign_ordinal();
        DORIS_CHECK_GT(delete_sign_idx, 0);
        DORIS_CHECK_LT(delete_sign_idx, static_cast<int>(target_columns.size()));
        auto delete_filter_column = IColumn::mutate(std::move(_delete_filter_column));
        reinterpret_cast<ColumnUInt8*>(delete_filter_column.get())->resize(target_block_row);

        auto* __restrict filter_data =
                reinterpret_cast<ColumnUInt8*>(delete_filter_column.get())->get_data().data();
        auto* __restrict delete_data =
                reinterpret_cast<ColumnInt8*>(target_columns[delete_sign_idx].get())
                        ->get_data()
                        .data();
        int delete_count = 0;
        for (int i = 0; i < target_block_row; ++i) {
            bool sign = (delete_data[i] == 0);
            filter_data[i] = sign;
            if (UNLIKELY(!sign)) {
                if (UNLIKELY(_reader_context.record_rowids)) {
                    _block_row_locations[i].row_id = -1;
                    delete_count++;
                }
            }
        }
        auto target_columns_size = target_columns.size();
        _delete_filter_column = std::move(delete_filter_column);
        ColumnWithTypeAndName column_with_type_and_name {_delete_filter_column,
                                                         std::make_shared<DataTypeUInt8>(),
                                                         "__DORIS_COMPACTION_FILTER__"};
        target_columns_guard.restore();
        block->insert(column_with_type_and_name);
        RETURN_IF_ERROR(Block::filter_block(block, target_columns_size, target_columns_size));
        _stats.rows_del_filtered += target_block_row - block->rows();
        if (UNLIKELY(_reader_context.record_rowids)) {
            DCHECK_EQ(_block_row_locations.size(), block->rows() + delete_count);
        }
    }
    return Status::OK();
}

Status BlockReader::_insert_data_normal(MutableColumns& columns) {
    auto block = _next_row.block.get();

    RETURN_IF_CATCH_EXCEPTION({
        size_t aggregate_index = 0;
        for (size_t ordinal = 0; ordinal < columns.size(); ++ordinal) {
            if (aggregate_index < _agg_columns_idx.size() &&
                ordinal == _agg_columns_idx[aggregate_index]) {
                ++aggregate_index;
                continue;
            }
            columns[ordinal]->insert_from(*block->get_by_position(ordinal).column,
                                          _next_row.row_pos);
        }
    });
    return Status::OK();
}

void BlockReader::_append_agg_data(MutableColumns& columns) {
    _stored_row_ref.push_back(_next_row);
    _last_agg_data_counter++;

    // execute aggregate when accumulated `batch_max_rows()` rows or some ref invalid soon
    // `_stored_data_columns` is sized to `batch_max_rows()`,
    // this flush keeps the number of rows in `_stored_row_ref` within `batch_max_rows()`.
    bool is_last = (_next_row.block->rows() == _next_row.row_pos + 1);
    if (is_last || _stored_row_ref.size() == batch_max_rows()) {
        _update_agg_data(columns);
    }
}

void BlockReader::_update_agg_data(MutableColumns& columns) {
    // copy data to stored block
    size_t copy_size = _copy_agg_data();

    // calculate has_null_tag
    for (auto idx : _agg_columns_idx) {
        _stored_has_null_tag[idx] = _stored_data_columns[idx]->has_null(0, copy_size);
    }

    // calculate aggregate and insert
    int counter_sum = 0;
    for (int counter : _agg_data_counters) {
        _update_agg_value(columns, counter_sum, counter_sum + counter - 1);
        counter_sum += counter;
    }

    // some key still has value at next block, so do not insert
    if (_last_agg_data_counter) {
        _update_agg_value(columns, counter_sum, counter_sum + _last_agg_data_counter - 1, false);
        _last_agg_data_counter = 0;
    }

    _agg_data_counters.clear();
}

size_t BlockReader::_copy_agg_data() {
    size_t copy_size = _stored_row_ref.size();

    for (size_t i = 0; i < copy_size; i++) {
        auto& ref = _stored_row_ref[i];
        _temp_ref_map[ref.block.get()].emplace_back(ref.row_pos, i);
    }

    for (auto idx : _agg_columns_idx) {
        auto& dst_column = _stored_data_columns[idx];
        if (_stored_has_variable_length_tag[idx]) {
            //variable length type should replace ordered
            dst_column->clear();
            for (size_t i = 0; i < copy_size; i++) {
                auto& ref = _stored_row_ref[i];
                dst_column->insert_from(*ref.block->get_by_position(idx).column, ref.row_pos);
            }
        } else {
            for (auto& it : _temp_ref_map) {
                if (!it.second.empty()) {
                    auto& src_column = *it.first->get_by_position(idx).column;
                    for (auto& pos : it.second) {
                        dst_column->replace_column_data(src_column, pos.first, pos.second);
                    }
                }
            }
        }
    }

    for (auto& it : _temp_ref_map) {
        it.second.clear();
    }
    _stored_row_ref.clear();

    return copy_size;
}

void BlockReader::_update_agg_value(MutableColumns& columns, int begin, int end, bool is_close) {
    for (int i = 0; i < _agg_columns_idx.size(); i++) {
        auto idx = _agg_columns_idx[i];

        AggregateFunctionPtr function = _agg_functions[i];
        AggregateDataPtr place = _agg_places[i];
        auto* column_ptr = _stored_data_columns[idx].get();

        if (begin <= end) {
            function->add_batch_range(begin, end, place, const_cast<const IColumn**>(&column_ptr),
                                      _arena, _stored_has_null_tag[idx]);
        }

        if (is_close) {
            function->insert_result_into(place, *columns[idx]);
            // reset aggregate data
            function->reset(place);
        }
    }
    if (is_close) {
        _arena.clear();
    }
}

} // namespace doris
