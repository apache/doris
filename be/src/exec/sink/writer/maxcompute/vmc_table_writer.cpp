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

#include "exec/sink/writer/maxcompute/vmc_table_writer.h"

#include "core/block/materialize_block.h"
#include "exec/sink/writer/maxcompute/vmc_partition_writer.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "format/transformer/vjni_format_transformer.h"
#include "runtime/runtime_state.h"
#include "util/uid_util.h"

namespace doris {
#include "common/compile_check_begin.h"

VMCTableWriter::VMCTableWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_expr_ctxs,
                               std::shared_ptr<Dependency> dep, std::shared_ptr<Dependency> fin_dep)
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
    _next_block_id.store(state->per_fragment_instance_idx() * BLOCK_ID_STRIDE);

    LOG(INFO) << "VMCTableWriter::open"
              << ", fragment_instance_id=" << print_id(state->fragment_instance_id())
              << ", per_fragment_instance_idx=" << state->per_fragment_instance_idx()
              << ", write_session_id=" << _mc_sink.write_session_id
              << ", next_block_id_start=" << _next_block_id.load();

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

    // Build write output expr contexts
    for (int i = 0; i < _vec_output_expr_ctxs.size(); i++) {
        _write_output_vexpr_ctxs.emplace_back(_vec_output_expr_ctxs[i]);
    }

    // For static partition, partition columns need to be excluded from the data written to MC.
    // For dynamic partition, MaxCompute Storage API (with DynamicPartitionOptions) expects
    // partition column values in the Arrow data, so we keep them.
    if (!_partition_column_names.empty() && _has_static_partition) {
        size_t total_cols = _vec_output_expr_ctxs.size();
        size_t num_partition_cols = _partition_column_names.size();
        size_t data_cols = total_cols - num_partition_cols;
        for (size_t i = data_cols; i < total_cols; i++) {
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
    auto params = _mc_sink.properties;
    if (_mc_sink.__isset.endpoint) params["endpoint"] = _mc_sink.endpoint;
    if (_mc_sink.__isset.project) params["project"] = _mc_sink.project;
    if (_mc_sink.__isset.table_name) params["table"] = _mc_sink.table_name;
    if (_mc_sink.__isset.quota) params["quota"] = _mc_sink.quota;
    if (_mc_sink.__isset.write_session_id) {
        params["write_session_id"] = _mc_sink.write_session_id;
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
    if (_mc_sink.__isset.max_write_batch_rows) {
        params["max_write_batch_rows"] = std::to_string(_mc_sink.max_write_batch_rows);
    }
    return params;
}

std::shared_ptr<VMCPartitionWriter> VMCTableWriter::_create_partition_writer(
        const std::string& partition_spec) {
    auto params = _build_base_writer_params();
    params["partition_spec"] = partition_spec;
    // Each partition writer gets a unique block_id from the atomic counter
    params["block_id"] = std::to_string(_next_block_id.fetch_add(1));
    LOG(INFO) << "VMCTableWriter::_create_partition_writer"
              << ", fragment_instance_id=" << print_id(_state->fragment_instance_id())
              << ", partition_spec=" << partition_spec << ", block_id=" << params["block_id"];
    return std::make_shared<VMCPartitionWriter>(_state, _write_output_vexpr_ctxs, partition_spec,
                                                std::move(params));
}

Status VMCTableWriter::write(RuntimeState* state, Block& block) {
    SCOPED_RAW_TIMER(&_send_data_ns);
    if (block.rows() == 0) {
        return Status::OK();
    }

    Block output_block;
    RETURN_IF_ERROR(VExprContext::get_output_block_after_execute_exprs(_vec_output_expr_ctxs, block,
                                                                       &output_block, false));
    materialize_block_inplace(output_block);

    _row_count += output_block.rows();

    // Case 1: Static partition - strip partition columns and write to specific partition writer
    if (_has_static_partition) {
        auto it = _partitions_to_writers.find(_static_partition_spec);
        if (it == _partitions_to_writers.end()) {
            auto writer = _create_partition_writer(_static_partition_spec);
            RETURN_IF_ERROR(writer->open());
            _partitions_to_writers.insert({_static_partition_spec, writer});
            it = _partitions_to_writers.find(_static_partition_spec);
        }
        output_block.erase(_non_write_columns_indices);
        return _write_block_in_chunks(it->second, output_block);
    }

    // Case 2: Dynamic partition or non-partitioned table
    std::string partition_key = "";
    auto it = _partitions_to_writers.find(partition_key);
    if (it == _partitions_to_writers.end()) {
        auto writer = _create_partition_writer("");
        RETURN_IF_ERROR(writer->open());
        _partitions_to_writers.insert({partition_key, writer});
        it = _partitions_to_writers.find(partition_key);
    }
    return _write_block_in_chunks(it->second, output_block);
}

Status VMCTableWriter::_write_block_in_chunks(const std::shared_ptr<VMCPartitionWriter>& writer,
                                              Block& output_block) {
    // Limit per-JNI data to MAX_WRITE_BLOCK_BYTES. When data source is not MC scanner
    // (e.g. Doris internal table, Hive, JDBC), the upstream batch_size controls Block
    // row count but not byte size. With large rows (585KB/row), a 4096-row Block is
    // ~2.4GB. Splitting ensures each JNI call processes bounded data, limiting Arrow
    // and SDK native memory per call.
    static constexpr size_t MAX_WRITE_BLOCK_BYTES = 256 * 1024 * 1024; // 256MB

    const size_t block_bytes = output_block.allocated_bytes();
    const size_t rows = output_block.rows();

    if (block_bytes <= MAX_WRITE_BLOCK_BYTES || rows <= 1) {
        return writer->write(output_block);
    }

    const size_t bytes_per_row = block_bytes / rows;
    const size_t max_rows = std::max(size_t(1), MAX_WRITE_BLOCK_BYTES / bytes_per_row);

    for (size_t offset = 0; offset < rows; offset += max_rows) {
        const size_t num_rows = std::min(max_rows, rows - offset);
        Block sub_block = output_block.clone_empty();
        auto columns = sub_block.mutate_columns();
        for (size_t i = 0; i < columns.size(); i++) {
            columns[i]->insert_range_from(*output_block.get_by_position(i).column, offset,
                                          num_rows);
        }
        sub_block.set_columns(std::move(columns));
        RETURN_IF_ERROR(writer->write(sub_block));
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
                LOG(WARNING) << "VMCPartitionWriter close failed for partition " << partition_spec
                             << ": " << st.to_string();
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

} // namespace doris
