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

#include "exec/sink/viceberg_merge_sink.h"

#include <fmt/format.h>

#include "agent/be_exec_version_manager.h"
#include "common/consts.h"
#include "common/exception.h"
#include "common/logging.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "exec/sink/sink_common.h"
#include "exec/sink/viceberg_delete_sink.h"
#include "exec/sink/writer/iceberg/viceberg_table_writer.h"
#include "exprs/vexpr_context.h"
#include "format/table/iceberg/schema.h"
#include "format/table/iceberg/schema_parser.h"
#include "runtime/runtime_state.h"
#include "util/string_util.h"

namespace doris {

namespace {} // namespace

VIcebergMergeSink::VIcebergMergeSink(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs,
                                     std::shared_ptr<Dependency> dep,
                                     std::shared_ptr<Dependency> fin_dep)
        : AsyncResultWriter(output_exprs, dep, fin_dep), _t_sink(t_sink) {
    DCHECK(_t_sink.__isset.iceberg_merge_sink);
}

VIcebergMergeSink::~VIcebergMergeSink() = default;

Status VIcebergMergeSink::init_properties(ObjectPool* pool, const RowDescriptor& row_desc) {
    RETURN_IF_ERROR(_build_inner_sinks());

    if (_writes_data_files) {
        _table_writer = std::make_unique<VIcebergTableWriter>(_table_sink, _table_output_expr_ctxs,
                                                              nullptr, nullptr);
        _table_writer->defer_file_cleanup_until_outer_close();
        RETURN_IF_ERROR(_table_writer->init_properties(pool, row_desc));
    }
    _delete_writer = std::make_unique<VIcebergDeleteSink>(_delete_sink, _delete_output_expr_ctxs,
                                                          nullptr, nullptr);
    _delete_writer->defer_file_cleanup_until_outer_close();
    RETURN_IF_ERROR(_delete_writer->init_properties(pool));
    return Status::OK();
}

Status VIcebergMergeSink::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;

    _written_rows_counter = ADD_COUNTER(profile, "RowsWritten", TUnit::UNIT);
    _insert_rows_counter = ADD_COUNTER(profile, "InsertRows", TUnit::UNIT);
    _delete_rows_counter = ADD_COUNTER(profile, "DeleteRows", TUnit::UNIT);
    // The query-wide version keeps validation all-or-nothing during a rolling BE upgrade.
    _require_merge_cardinality_check =
            _require_merge_cardinality_check &&
            state->be_exec_version() >= SUPPORT_ICEBERG_MERGE_CARDINALITY_VERSION;
    if (_require_merge_cardinality_check) {
        _matched_row_id_state_bytes_counter =
                ADD_COUNTER(profile, "MatchedRowIdStateBytes", TUnit::BYTES);
    }
    _send_data_timer = ADD_TIMER(profile, "SendDataTime");
    _open_timer = ADD_TIMER(profile, "OpenTime");
    _close_timer = ADD_TIMER(profile, "CloseTime");

    SCOPED_TIMER(_open_timer);

    RETURN_IF_ERROR(_prepare_output_layout());

    RuntimeProfile* delete_profile = profile->create_child("IcebergMergeDeleteWriter", true, true);

    if (_table_writer) {
        RuntimeProfile* table_profile =
                profile->create_child("IcebergMergeTableWriter", true, true);
        RETURN_IF_ERROR(_table_writer->open(state, table_profile));
    }
    RETURN_IF_ERROR(_delete_writer->open(state, delete_profile));

    return Status::OK();
}

Status VIcebergMergeSink::write(RuntimeState* state, Block& block) {
    SCOPED_TIMER(_send_data_timer);
    if (block.rows() == 0) {
        return Status::OK();
    }

    Block output_block;
    RETURN_IF_ERROR(_projection_block(block, &output_block));
    if (output_block.rows() == 0) {
        return Status::OK();
    }

    if (_operation_idx < 0 || _row_id_idx < 0) {
        return Status::InternalError("Iceberg merge sink missing operation/row_id columns");
    }

    const auto& op_column = output_block.get_by_position(_operation_idx).column;
    const auto* op_data = remove_nullable(op_column).get();

    IColumn::Filter delete_filter(output_block.rows(), 0);
    IColumn::Filter insert_filter(output_block.rows(), 0);
    bool has_delete = false;
    bool has_insert = false;
    size_t delete_rows = 0;
    size_t insert_rows = 0;

    for (size_t i = 0; i < output_block.rows(); ++i) {
        int8_t op = static_cast<int8_t>(op_data->get_int(i));
        bool delete_op = is_delete_op(op);
        bool insert_op = is_insert_op(op);
        if (!delete_op && !insert_op) {
            return Status::InternalError("Unknown Iceberg merge operation {}", op);
        }
        if (delete_op) {
            delete_filter[i] = 1;
            has_delete = true;
            ++delete_rows;
        }
        if (insert_op) {
            insert_filter[i] = 1;
            has_insert = true;
            ++insert_rows;
        }
    }

    if (_require_merge_cardinality_check) {
        // The physical sink hashes matched rows by row_id, so exact state retained across blocks
        // enforces SQL MERGE cardinality for the whole query without changing UPDATE semantics.
        RETURN_IF_ERROR(_validate_matched_row_ids(output_block, delete_filter.data()));
        COUNTER_SET(_matched_row_id_state_bytes_counter,
                    static_cast<int64_t>(_matched_row_id_state_size));
    }
    _row_count += output_block.rows();
    _delete_row_count += delete_rows;
    _insert_row_count += insert_rows;

    // A delete-only plan deliberately omits the data writer so Variant target schemas never enter
    // the unsupported Iceberg data-write path. Reject a mismatched FE plan before dereferencing it.
    if (has_insert && !_writes_data_files) {
        return Status::InternalError(
                "Iceberg delete-only merge sink received a data insert operation");
    }

    bool skip_io = false;
#ifdef BE_TEST
    skip_io = _skip_io;
#endif

    if (has_delete && !skip_io) {
        Block delete_block = output_block;
        std::vector<int> delete_indices {_row_id_idx};
        delete_block.erase_not_in(delete_indices);
        Block::filter_block_internal(&delete_block, delete_filter);
        RETURN_IF_ERROR(_delete_writer->write(state, delete_block));
    }

    if (has_insert && !skip_io) {
        if (_data_column_indices.empty()) {
            return Status::InternalError("Iceberg merge sink has no data columns for insert");
        }
        Block insert_block = output_block;
        insert_block.erase_not_in(_data_column_indices);
        Block::filter_block_internal(&insert_block, insert_filter);
        RETURN_IF_ERROR(_table_writer->write_prepared_block(insert_block));
    }

    if (_written_rows_counter != nullptr) {
        COUNTER_UPDATE(_written_rows_counter, output_block.rows());
    }
    if (_insert_rows_counter != nullptr) {
        COUNTER_UPDATE(_insert_rows_counter, insert_rows);
    }
    if (_delete_rows_counter != nullptr) {
        COUNTER_UPDATE(_delete_rows_counter, delete_rows);
    }

    return Status::OK();
}

Status VIcebergMergeSink::_validate_matched_row_ids(const Block& block,
                                                    const uint8_t* delete_filter) {
    const auto& row_id = block.get_by_position(_row_id_idx);
    const IColumn* row_id_data = row_id.column.get();
    const IDataType* row_id_type = row_id.type.get();
    const auto* nullable_row_id = check_and_get_column<ColumnNullable>(row_id_data);
    if (nullable_row_id != nullptr) {
        row_id_data = nullable_row_id->get_nested_column_ptr().get();
    }
    if (const auto* nullable_type = check_and_get_data_type<DataTypeNullable>(row_id_type)) {
        row_id_type = nullable_type->get_nested_type().get();
    }

    const auto* struct_column = check_and_get_column<ColumnStruct>(row_id_data);
    const auto* struct_type = check_and_get_data_type<DataTypeStruct>(row_id_type);
    if (struct_column == nullptr || struct_type == nullptr) {
        return Status::InternalError("Iceberg merge row_id column is not a struct");
    }

    int file_path_idx = -1;
    int row_position_idx = -1;
    const auto& field_names = struct_type->get_element_names();
    for (size_t i = 0; i < field_names.size(); ++i) {
        std::string field_name = doris::to_lower(field_names[i]);
        if (field_name == "file_path") {
            file_path_idx = static_cast<int>(i);
        } else if (field_name == "row_position") {
            row_position_idx = static_cast<int>(i);
        }
    }
    if (file_path_idx < 0 || row_position_idx < 0) {
        return Status::InternalError(
                "Iceberg merge row_id must contain file_path and row_position fields");
    }

    const auto& file_path_column = struct_column->get_column_ptr(file_path_idx);
    const auto& row_position_column = struct_column->get_column_ptr(row_position_idx);
    const auto* nullable_file_path = check_and_get_column<ColumnNullable>(file_path_column.get());
    const auto* nullable_row_position =
            check_and_get_column<ColumnNullable>(row_position_column.get());
    const auto* file_paths =
            check_and_get_column<ColumnString>(remove_nullable(file_path_column).get());
    const auto* row_positions = check_and_get_column<ColumnVector<TYPE_BIGINT>>(
            remove_nullable(row_position_column).get());
    if (file_paths == nullptr || row_positions == nullptr) {
        return Status::InternalError("Iceberg merge row_id fields have incorrect types");
    }

    std::map<roaring::Roaring64Map*, size_t> touched_bitmap_sizes;
    for (size_t i = 0; i < block.rows(); ++i) {
        if (delete_filter[i] == 0) {
            continue;
        }
        if ((nullable_row_id != nullptr && nullable_row_id->is_null_at(i)) ||
            (nullable_file_path != nullptr && nullable_file_path->is_null_at(i)) ||
            (nullable_row_position != nullptr && nullable_row_position->is_null_at(i))) {
            return Status::InternalError("Iceberg merge matched row_id cannot be null");
        }

        int64_t row_position = row_positions->get_element(i);
        if (row_position < 0) {
            return Status::InternalError("Invalid row_position {} in Iceberg merge row_id",
                                         row_position);
        }
        // Intern each file path once and keep exact positions in a compressed bitmap; retaining a
        // full path string per matched row makes MERGE memory grow with path_length * row_count.
        auto [file_it, inserted] =
                _matched_row_positions.try_emplace(file_paths->get_data_at(i).to_string());
        auto* positions = &file_it->second;
        auto touched_it = touched_bitmap_sizes.find(positions);
        if (touched_it == touched_bitmap_sizes.end()) {
            touched_it = touched_bitmap_sizes.emplace(positions, positions->getSizeInBytes()).first;
        }
        if (inserted) {
            _matched_row_id_state_size +=
                    sizeof(std::pair<const std::string, roaring::Roaring64Map>);
            _matched_row_id_state_size += file_it->first.capacity();
            _matched_row_id_state_size += touched_it->second;
        }
        if (!positions->addChecked(static_cast<uint64_t>(row_position))) {
            return Status::InvalidArgument(
                    "Iceberg MERGE failed because multiple source rows matched the same target "
                    "row");
        }
    }

    // Measure only bitmaps touched by this block; rescanning all retained files on every write
    // makes a many-file MERGE quadratic in the number of input blocks.
    for (const auto& [positions, previous_size] : touched_bitmap_sizes) {
        size_t current_size = positions->getSizeInBytes();
        if (current_size >= previous_size) {
            _matched_row_id_state_size += current_size - previous_size;
        } else {
            _matched_row_id_state_size -= previous_size - current_size;
        }
    }
    return Status::OK();
}

Status VIcebergMergeSink::close(Status close_status) {
    SCOPED_TIMER(_close_timer);

    if (!close_status.ok()) {
        LOG(WARNING) << fmt::format("VIcebergMergeSink close with error: {}",
                                    close_status.to_string());
        if (_table_writer) {
            static_cast<void>(_table_writer->close(close_status));
        }
        if (_delete_writer) {
            static_cast<void>(_delete_writer->close(close_status));
        }
        return close_status;
    }

    Status table_status = Status::OK();
    Status delete_status = Status::OK();
    if (_table_writer) {
        table_status = _table_writer->close(close_status);
    }
    if (_delete_writer) {
        delete_status = _delete_writer->close(close_status);
    }

    if (_written_rows_counter != nullptr) {
        COUNTER_SET(_written_rows_counter, static_cast<int64_t>(_row_count));
    }
    if (_insert_rows_counter != nullptr) {
        COUNTER_SET(_insert_rows_counter, static_cast<int64_t>(_insert_row_count));
    }
    if (_delete_rows_counter != nullptr) {
        COUNTER_SET(_delete_rows_counter, static_cast<int64_t>(_delete_row_count));
    }

    Status result_status = table_status.ok() ? delete_status : table_status;
    if (_table_writer) {
        _table_writer->finish_deferred_file_cleanup(result_status);
    }
    if (_delete_writer) {
        _delete_writer->finish_deferred_file_cleanup(result_status);
    }
    return result_status;
}

Status VIcebergMergeSink::_build_inner_sinks() {
    if (!_t_sink.__isset.iceberg_merge_sink) {
        return Status::InternalError("Missing iceberg merge sink config");
    }

    const auto& merge_sink = _t_sink.iceberg_merge_sink;
    // An old FE cannot produce delete-only plans, so an unset flag retains its data-writer path.
    _writes_data_files = !merge_sink.__isset.writes_data_files || merge_sink.writes_data_files;
    // Missing means an old FE plan, which predates SQL MERGE cardinality validation.
    _require_merge_cardinality_check = merge_sink.__isset.require_merge_cardinality_check &&
                                       merge_sink.require_merge_cardinality_check;

    TIcebergTableSink table_sink;
    if (merge_sink.__isset.db_name) {
        table_sink.__set_db_name(merge_sink.db_name);
    }
    if (merge_sink.__isset.tb_name) {
        table_sink.__set_tb_name(merge_sink.tb_name);
    }
    if (merge_sink.__isset.schema_json) {
        table_sink.__set_schema_json(merge_sink.schema_json);
    }
    if (merge_sink.__isset.partition_specs_json) {
        table_sink.__set_partition_specs_json(merge_sink.partition_specs_json);
    }
    if (merge_sink.__isset.partition_spec_id) {
        table_sink.__set_partition_spec_id(merge_sink.partition_spec_id);
    }
    if (merge_sink.__isset.sort_fields) {
        table_sink.__set_sort_fields(merge_sink.sort_fields);
    }
    if (merge_sink.__isset.file_format) {
        table_sink.__set_file_format(merge_sink.file_format);
    }
    if (merge_sink.__isset.compression_type) {
        table_sink.__set_compression_type(merge_sink.compression_type);
    }
    if (merge_sink.__isset.output_path) {
        table_sink.__set_output_path(merge_sink.output_path);
    }
    if (merge_sink.__isset.original_output_path) {
        table_sink.__set_original_output_path(merge_sink.original_output_path);
    }
    if (merge_sink.__isset.hadoop_config) {
        table_sink.__set_hadoop_config(merge_sink.hadoop_config);
    }
    if (merge_sink.__isset.file_type) {
        table_sink.__set_file_type(merge_sink.file_type);
    }
    if (merge_sink.__isset.broker_addresses) {
        table_sink.__set_broker_addresses(merge_sink.broker_addresses);
    }
    if (merge_sink.__isset.collect_column_stats) {
        table_sink.__set_collect_column_stats(merge_sink.collect_column_stats);
    }
    _table_sink.__set_type(TDataSinkType::ICEBERG_TABLE_SINK);
    _table_sink.__set_iceberg_table_sink(table_sink);

    TIcebergDeleteSink delete_sink;
    if (merge_sink.__isset.db_name) {
        delete_sink.__set_db_name(merge_sink.db_name);
    }
    if (merge_sink.__isset.tb_name) {
        delete_sink.__set_tb_name(merge_sink.tb_name);
    }
    if (merge_sink.__isset.delete_type) {
        delete_sink.__set_delete_type(merge_sink.delete_type);
    }
    if (merge_sink.__isset.file_format) {
        delete_sink.__set_file_format(merge_sink.file_format);
    }
    if (merge_sink.__isset.compression_type) {
        delete_sink.__set_compress_type(merge_sink.compression_type);
    }
    if (merge_sink.__isset.output_path) {
        delete_sink.__set_output_path(merge_sink.output_path);
    }
    if (merge_sink.__isset.table_location) {
        delete_sink.__set_table_location(merge_sink.table_location);
    }
    if (merge_sink.__isset.hadoop_config) {
        delete_sink.__set_hadoop_config(merge_sink.hadoop_config);
    }
    if (merge_sink.__isset.file_type) {
        delete_sink.__set_file_type(merge_sink.file_type);
    }
    if (merge_sink.__isset.partition_spec_id_for_delete) {
        delete_sink.__set_partition_spec_id(merge_sink.partition_spec_id_for_delete);
    }
    if (merge_sink.__isset.partition_data_json_for_delete) {
        delete_sink.__set_partition_data_json(merge_sink.partition_data_json_for_delete);
    }
    if (merge_sink.__isset.broker_addresses) {
        delete_sink.__set_broker_addresses(merge_sink.broker_addresses);
    }
    if (merge_sink.__isset.format_version) {
        delete_sink.__set_format_version(merge_sink.format_version);
    }
    if (merge_sink.__isset.rewritable_delete_file_sets) {
        delete_sink.__set_rewritable_delete_file_sets(merge_sink.rewritable_delete_file_sets);
    }
    _delete_sink.__set_type(TDataSinkType::ICEBERG_DELETE_SINK);
    _delete_sink.__set_iceberg_delete_sink(delete_sink);

    return Status::OK();
}

Status VIcebergMergeSink::_prepare_output_layout() {
    if (_vec_output_expr_ctxs.empty()) {
        return Status::InternalError("Iceberg merge sink has empty output expressions");
    }

    std::string row_id_name = doris::to_lower(BeConsts::ICEBERG_ROWID_COL);
    std::string op_name = doris::to_lower(kOperationColumnName);

    _operation_idx = -1;
    _row_id_idx = -1;
    for (size_t i = 0; i < _vec_output_expr_ctxs.size(); ++i) {
        std::string expr_name = doris::to_lower(_vec_output_expr_ctxs[i]->expr_name());
        if (_operation_idx < 0 && expr_name == op_name) {
            _operation_idx = static_cast<int>(i);
        } else if (_row_id_idx < 0 && expr_name == row_id_name) {
            _row_id_idx = static_cast<int>(i);
        }
    }

    if (_operation_idx < 0) {
        return Status::InternalError("Iceberg merge sink missing operation column");
    }
    if (_row_id_idx < 0) {
        return Status::InternalError("Iceberg merge sink missing row_id column");
    }

    _data_column_indices.clear();
    _table_output_expr_ctxs.clear();
    for (size_t i = 0; i < _vec_output_expr_ctxs.size(); ++i) {
        if (static_cast<int>(i) == _operation_idx || static_cast<int>(i) == _row_id_idx) {
            continue;
        }
        _data_column_indices.push_back(static_cast<int>(i));
        _table_output_expr_ctxs.emplace_back(_vec_output_expr_ctxs[i]);
    }

    return Status::OK();
}

} // namespace doris
