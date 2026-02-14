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

#include "vmc_table_writer.h"

#include "runtime/runtime_state.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/core/materialize_block.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/sink/writer/maxcompute/vmc_partition_writer.h"

#include "vec/runtime/vjni_format_transformer.h"

namespace doris {
namespace vectorized {
#include "common/compile_check_begin.h"

VMCTableWriter::VMCTableWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_expr_ctxs,
                                std::shared_ptr<pipeline::Dependency> dep,
                                std::shared_ptr<pipeline::Dependency> fin_dep)
        : AsyncResultWriter(output_expr_ctxs, dep, fin_dep),
          _t_sink(t_sink),
          _mc_sink(_t_sink.max_compute_table_sink) {
    DCHECK(_t_sink.__isset.max_compute_table_sink);
}

Status VMCTableWriter::init_properties(ObjectPool* pool) {
    return Status::OK();
}

Status VMCTableWriter::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;

    _written_rows_counter = ADD_COUNTER(_operator_profile, "WrittenRows", TUnit::UNIT);
    _send_data_timer = ADD_TIMER(_operator_profile, "SendDataTime");
    _close_timer = ADD_TIMER(_operator_profile, "CloseTime");
    _partition_writers_count = ADD_COUNTER(_operator_profile, "PartitionsWriteCount", TUnit::UNIT);

    // Determine partition columns
    if (_mc_sink.__isset.partition_columns && !_mc_sink.partition_columns.empty()) {
        _partition_column_names = _mc_sink.partition_columns;
    }

    // Check for static partition
    if (_mc_sink.__isset.static_partition_spec && !_mc_sink.static_partition_spec.empty()) {
        _has_static_partition = true;
        // Build "key1=val1/key2=val2" format
        std::stringstream ss;
        bool first = true;
        for (const auto& [key, val] : _mc_sink.static_partition_spec) {
            if (!first) ss << "/";
            first = false;
            ss << key << "=" << val;
        }
        _static_partition_spec = ss.str();
    }

    // Build write output expr contexts (all columns for now)
    // For dynamic partition, partition columns are at the end of the output block
    // and need to be excluded from the data written to MC
    for (int i = 0; i < _vec_output_expr_ctxs.size(); i++) {
        _write_output_vexpr_ctxs.emplace_back(_vec_output_expr_ctxs[i]);
    }

    // If we have dynamic partition columns, find their indices and mark them for exclusion
    if (!_partition_column_names.empty() && !_has_static_partition) {
        // Partition columns are the last N columns in the output block
        size_t total_cols = _vec_output_expr_ctxs.size();
        size_t num_partition_cols = _partition_column_names.size();
        size_t data_cols = total_cols - num_partition_cols;
        for (size_t i = data_cols; i < total_cols; i++) {
            _partition_column_indices.push_back(static_cast<int>(i));
            _non_write_columns_indices.insert(i);
        }
        // Rebuild write output expr contexts without partition columns
        _write_output_vexpr_ctxs.clear();
        for (size_t i = 0; i < data_cols; i++) {
            _write_output_vexpr_ctxs.emplace_back(_vec_output_expr_ctxs[i]);
        }
    }

    return Status::OK();
}

std::map<std::string, std::string> VMCTableWriter::_build_base_writer_params() {
    std::map<std::string, std::string> params;
    if (_mc_sink.__isset.access_key) params["access_key"] = _mc_sink.access_key;
    if (_mc_sink.__isset.secret_key) params["secret_key"] = _mc_sink.secret_key;
    if (_mc_sink.__isset.endpoint) params["endpoint"] = _mc_sink.endpoint;
    if (_mc_sink.__isset.project) params["project"] = _mc_sink.project;
    if (_mc_sink.__isset.table_name) params["table"] = _mc_sink.table_name;
    if (_mc_sink.__isset.quota) params["quota"] = _mc_sink.quota;
    if (_mc_sink.__isset.session_id) params["session_id"] = _mc_sink.session_id;
    if (_mc_sink.__isset.block_id_start) {
        params["block_id_start"] = std::to_string(_mc_sink.block_id_start);
    }
    if (_mc_sink.__isset.block_id_count) {
        params["block_id_count"] = std::to_string(_mc_sink.block_id_count);
    }
    if (_mc_sink.__isset.connect_timeout) {
        params["connect_timeout"] = std::to_string(_mc_sink.connect_timeout);
    }
    if (_mc_sink.__isset.read_timeout) {
        params["read_timeout"] = std::to_string(_mc_sink.read_timeout);
    }
    if (_mc_sink.__isset.retry_count) {
        params["retry_count"] = std::to_string(_mc_sink.retry_count);
    }
    return params;
}

std::shared_ptr<VMCPartitionWriter> VMCTableWriter::_create_partition_writer(
        const std::string& partition_spec) {
    auto params = _build_base_writer_params();
    params["partition_spec"] = partition_spec;
    // For dynamic partition, clear session_id so Java side creates a new session
    if (!_has_static_partition && !_partition_column_names.empty()) {
        params["session_id"] = "";
        // Dynamic partition: each partition has its own session, block_id starts from 0
        params["block_id_start"] = "0";
        params["block_id_count"] = "20000";
    }
    return std::make_shared<VMCPartitionWriter>(_state, _write_output_vexpr_ctxs, partition_spec,
                                                std::move(params));
}

std::string VMCTableWriter::_get_partition_spec(const Block& block, int row_idx) {
    std::stringstream ss;
    for (int i = 0; i < _partition_column_indices.size(); i++) {
        int col_idx = _partition_column_indices[i];
        const auto& column = block.get_by_position(col_idx);
        auto col_ptr = column.column->convert_to_full_column_if_const();

        if (i > 0) ss << "/";
        ss << _partition_column_names[i] << "=";

        if (col_ptr->is_nullable()) {
            const auto* nullable_col =
                    static_cast<const ColumnNullable*>(col_ptr.get());
            if (nullable_col->is_null_at(row_idx)) {
                ss << "__HIVE_DEFAULT_PARTITION__";
                continue;
            }
            col_ptr = nullable_col->get_nested_column_ptr();
        }

        // Get string representation of the partition value
        std::string val;
        if (auto* str_col = check_and_get_column<ColumnString>(col_ptr.get())) {
            auto sv = str_col->get_data_at(row_idx);
            val = std::string(sv.data, sv.size);
        } else {
            // For non-string types, use the column's string representation
            vectorized::DataTypeSerDe::FormatOptions fmt_opts;
            val = column.type->to_string(*col_ptr, row_idx, fmt_opts);
        }
        ss << val;
    }
    return ss.str();
}

Status VMCTableWriter::_filter_block(doris::vectorized::Block& block,
                                     const vectorized::IColumn::Filter* filter,
                                     doris::vectorized::Block* output_block) {
    const ColumnsWithTypeAndName& columns_with_type_and_name =
            block.get_columns_with_type_and_name();
    vectorized::ColumnsWithTypeAndName result_columns;
    for (const auto& col : columns_with_type_and_name) {
        result_columns.emplace_back(col.column->clone_resized(col.column->size()), col.type,
                                    col.name);
    }
    *output_block = {std::move(result_columns)};

    std::vector<uint32_t> columns_to_filter;
    int column_to_keep = output_block->columns();
    columns_to_filter.resize(column_to_keep);
    for (uint32_t i = 0; i < column_to_keep; ++i) {
        columns_to_filter[i] = i;
    }

    Block::filter_block_internal(output_block, columns_to_filter, *filter);
    return Status::OK();
}

Status VMCTableWriter::write(RuntimeState* state, vectorized::Block& block) {
    SCOPED_RAW_TIMER(&_send_data_ns);
    if (block.rows() == 0) {
        return Status::OK();
    }

    Block output_block;
    RETURN_IF_ERROR(vectorized::VExprContext::get_output_block_after_execute_exprs(
            _vec_output_expr_ctxs, block, &output_block, false));
    materialize_block_inplace(output_block);

    _row_count += output_block.rows();

    // Case 1: Non-partitioned table or static partition
    if (_partition_column_indices.empty()) {
        std::string partition_key = _has_static_partition ? _static_partition_spec : "";
        auto it = _partitions_to_writers.find(partition_key);
        if (it == _partitions_to_writers.end()) {
            auto writer = _create_partition_writer(
                    _has_static_partition ? _static_partition_spec : "");
            RETURN_IF_ERROR(writer->open());
            _partitions_to_writers.insert({partition_key, writer});
            it = _partitions_to_writers.find(partition_key);
        }
        // Erase partition columns if any (for static partition case)
        output_block.erase(_non_write_columns_indices);
        return it->second->write(output_block);
    }

    // Case 2: Dynamic partition - dispatch rows to partition writers
    std::unordered_map<std::shared_ptr<VMCPartitionWriter>, IColumn::Filter> writer_positions;

    for (int i = 0; i < output_block.rows(); ++i) {
        std::string partition_spec = _get_partition_spec(output_block, i);
        auto writer_iter = _partitions_to_writers.find(partition_spec);
        if (writer_iter == _partitions_to_writers.end()) {
            if (_partitions_to_writers.size() + 1 >
                config::table_sink_partition_write_max_partition_nums_per_writer) {
                return Status::InternalError("Too many open partitions {}",
                                             config::table_sink_partition_write_max_partition_nums_per_writer);
            }
            auto writer = _create_partition_writer(partition_spec);
            RETURN_IF_ERROR(writer->open());
            _partitions_to_writers.insert({partition_spec, writer});
            writer_iter = _partitions_to_writers.find(partition_spec);
        }
        auto& writer = writer_iter->second;
        auto pos_iter = writer_positions.find(writer);
        if (pos_iter == writer_positions.end()) {
            IColumn::Filter filter(output_block.rows(), 0);
            filter[i] = 1;
            writer_positions.insert({writer, std::move(filter)});
        } else {
            pos_iter->second[i] = 1;
        }
    }

    // Erase partition columns, then write filtered blocks to each partition writer
    output_block.erase(_non_write_columns_indices);
    for (auto& [writer, filter] : writer_positions) {
        Block filtered_block;
        RETURN_IF_ERROR(_filter_block(output_block, &filter, &filtered_block));
        RETURN_IF_ERROR(writer->write(filtered_block));
    }
    return Status::OK();
}

Status VMCTableWriter::close(Status status) {
    Status result_status;
    int64_t partitions_count = _partitions_to_writers.size();
    {
        SCOPED_RAW_TIMER(&_close_ns);
        for (const auto& [partition_spec, writer] : _partitions_to_writers) {
            Status st = writer->close(status);
            if (!st.ok()) {
                LOG(WARNING) << "VMCPartitionWriter close failed for partition "
                             << partition_spec << ": " << st.to_string();
                if (result_status.ok()) {
                    result_status = st;
                }
            }
        }
        _partitions_to_writers.clear();
    }
    if (status.ok()) {
        COUNTER_SET(_written_rows_counter, static_cast<int64_t>(_row_count));
        COUNTER_SET(_send_data_timer, _send_data_ns);
        COUNTER_SET(_close_timer, _close_ns);
        COUNTER_SET(_partition_writers_count, partitions_count);
    }
    return result_status;
}

} // namespace vectorized
} // namespace doris
