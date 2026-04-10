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

#include "common/consts.h"
#include "common/exception.h"
#include "common/logging.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
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

    _table_writer = std::make_unique<VIcebergTableWriter>(_table_sink, _table_output_expr_ctxs,
                                                          nullptr, nullptr);
    _delete_writer = std::make_unique<VIcebergDeleteSink>(_delete_sink, _delete_output_expr_ctxs,
                                                          nullptr, nullptr);
    RETURN_IF_ERROR(_table_writer->init_properties(pool, row_desc));
    RETURN_IF_ERROR(_delete_writer->init_properties(pool));
    return Status::OK();
}

Status VIcebergMergeSink::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;

    _written_rows_counter = ADD_COUNTER(profile, "RowsWritten", TUnit::UNIT);
    _insert_rows_counter = ADD_COUNTER(profile, "InsertRows", TUnit::UNIT);
    _delete_rows_counter = ADD_COUNTER(profile, "DeleteRows", TUnit::UNIT);
    _send_data_timer = ADD_TIMER(profile, "SendDataTime");
    _open_timer = ADD_TIMER(profile, "OpenTime");
    _close_timer = ADD_TIMER(profile, "CloseTime");

    SCOPED_TIMER(_open_timer);

    RETURN_IF_ERROR(_prepare_output_layout());

    RuntimeProfile* table_profile = profile->create_child("IcebergMergeTableWriter", true, true);
    RuntimeProfile* delete_profile = profile->create_child("IcebergMergeDeleteWriter", true, true);

    RETURN_IF_ERROR(_table_writer->open(state, table_profile));
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

    _row_count += output_block.rows();

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
            ++_delete_row_count;
            ++delete_rows;
        }
        if (insert_op) {
            insert_filter[i] = 1;
            has_insert = true;
            ++_insert_row_count;
            ++insert_rows;
        }
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

    if (!table_status.ok()) {
        return table_status;
    }
    return delete_status;
}

Status VIcebergMergeSink::_build_inner_sinks() {
    if (!_t_sink.__isset.iceberg_merge_sink) {
        return Status::InternalError("Missing iceberg merge sink config");
    }

    const auto& merge_sink = _t_sink.iceberg_merge_sink;

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
