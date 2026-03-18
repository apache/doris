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

#include "vpaimon_table_writer.h"

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "runtime/runtime_state.h"
#include "util/uid_util.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/materialize_block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vdatetime_value.h"
#include "vec/sink/writer/paimon/vpaimon_partition_writer.h"
#include "vec/sink/writer/vhive_utils.h"

namespace doris {
namespace vectorized {
#include "common/compile_check_begin.h"

VPaimonTableWriter::VPaimonTableWriter(const TDataSink& t_sink,
                                       const VExprContextSPtrs& output_expr_ctxs,
                                       std::shared_ptr<pipeline::Dependency> dep,
                                       std::shared_ptr<pipeline::Dependency> fin_dep)
        : AsyncResultWriter(output_expr_ctxs, dep, fin_dep), _t_sink(t_sink) {
    DCHECK(_t_sink.__isset.paimon_table_sink);
}

Status VPaimonTableWriter::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;

    _written_rows_counter = ADD_COUNTER(_operator_profile, "WrittenRows", TUnit::UNIT);
    _send_data_timer = ADD_TIMER(_operator_profile, "SendDataTime");
    _partition_writers_dispatch_timer =
            ADD_CHILD_TIMER(_operator_profile, "PartitionsDispatchTime", "SendDataTime");
    _partition_writers_write_timer =
            ADD_CHILD_TIMER(_operator_profile, "PartitionsWriteTime", "SendDataTime");
    _partition_writers_count = ADD_COUNTER(_operator_profile, "PartitionsWriteCount", TUnit::UNIT);
    _open_timer = ADD_TIMER(_operator_profile, "OpenTime");
    _close_timer = ADD_TIMER(_operator_profile, "CloseTime");
    _write_file_counter = ADD_COUNTER(_operator_profile, "WriteFileCount", TUnit::UNIT);

    SCOPED_TIMER(_open_timer);

    auto& paimon_sink = _t_sink.paimon_table_sink;
    // Identify partition vs. regular columns using the columns descriptor
    for (int i = 0; i < (int)paimon_sink.columns.size(); ++i) {
        switch (paimon_sink.columns[i].column_type) {
        case THiveColumnType::PARTITION_KEY:
            _partition_columns_input_index.emplace_back(i);
            _non_write_columns_indices.insert(i);
            break;
        case THiveColumnType::REGULAR:
            _write_output_vexpr_ctxs.push_back(_vec_output_expr_ctxs[i]);
            break;
        default:
            _non_write_columns_indices.insert(i);
            break;
        }
    }
    return Status::OK();
}

Status VPaimonTableWriter::write(RuntimeState* state, vectorized::Block& block) {
    SCOPED_RAW_TIMER(&_send_data_ns);
    if (block.rows() == 0) {
        return Status::OK();
    }

    Block output_block;
    RETURN_IF_ERROR(vectorized::VExprContext::get_output_block_after_execute_exprs(
            _vec_output_expr_ctxs, block, &output_block, false));
    materialize_block_inplace(output_block);

    std::unordered_map<std::shared_ptr<VPaimonPartitionWriter>, IColumn::Filter> writer_positions;
    _row_count += output_block.rows();

    auto& paimon_sink = _t_sink.paimon_table_sink;

    // Non-partitioned table
    if (_partition_columns_input_index.empty()) {
        std::shared_ptr<VPaimonPartitionWriter> writer;
        {
            SCOPED_RAW_TIMER(&_partition_writers_dispatch_ns);
            auto it = _partitions_to_writers.find("");
            if (it == _partitions_to_writers.end()) {
                try {
                    writer = _create_partition_writer(output_block, -1);
                } catch (doris::Exception& e) {
                    return e.to_status();
                }
                _partitions_to_writers.insert({"", writer});
                RETURN_IF_ERROR(writer->open(_state, _operator_profile));
            } else {
                if (it->second->written_len() > config::hive_sink_max_file_size) {
                    std::string file_name(it->second->file_name());
                    int file_name_index = it->second->file_name_index();
                    {
                        SCOPED_RAW_TIMER(&_close_ns);
                        static_cast<void>(it->second->close(Status::OK()));
                    }
                    _partitions_to_writers.erase(it);
                    try {
                        writer = _create_partition_writer(output_block, -1, &file_name,
                                                          file_name_index + 1);
                    } catch (doris::Exception& e) {
                        return e.to_status();
                    }
                    _partitions_to_writers.insert({"", writer});
                    RETURN_IF_ERROR(writer->open(_state, _operator_profile));
                } else {
                    writer = it->second;
                }
            }
        }
        SCOPED_RAW_TIMER(&_partition_writers_write_ns);
        output_block.erase(_non_write_columns_indices);
        RETURN_IF_ERROR(writer->write(output_block));
        return Status::OK();
    }

    // Partitioned table - dispatch each row to the right partition writer
    {
        SCOPED_RAW_TIMER(&_partition_writers_dispatch_ns);
        for (int i = 0; i < (int)output_block.rows(); ++i) {
            std::vector<std::string> partition_values;
            try {
                partition_values = _create_partition_values(output_block, i);
            } catch (doris::Exception& e) {
                return e.to_status();
            }

            // Build partition key string: "col1=val1/col2=val2"
            std::string partition_name;
            for (int j = 0; j < (int)_partition_columns_input_index.size(); ++j) {
                if (j > 0) {
                    partition_name += "/";
                }
                const std::string& col_name =
                        paimon_sink.columns[_partition_columns_input_index[j]].name;
                partition_name +=
                        VHiveUtils::escape_path_name(col_name) + "=" +
                        VHiveUtils::escape_path_name(partition_values[j]);
            }

            auto create_and_open_writer =
                    [&](int position, const std::string* file_name, int file_name_index,
                        std::shared_ptr<VPaimonPartitionWriter>& writer_out) -> Status {
                try {
                    auto w = _create_partition_writer(output_block, position, file_name,
                                                      file_name_index);
                    RETURN_IF_ERROR(w->open(_state, _operator_profile));
                    IColumn::Filter filter(output_block.rows(), 0);
                    filter[position] = 1;
                    writer_positions.insert({w, std::move(filter)});
                    _partitions_to_writers.insert({partition_name, w});
                    writer_out = w;
                } catch (doris::Exception& e) {
                    return e.to_status();
                }
                return Status::OK();
            };

            auto writer_it = _partitions_to_writers.find(partition_name);
            if (writer_it == _partitions_to_writers.end()) {
                if (_partitions_to_writers.size() + 1 >
                    config::table_sink_partition_write_max_partition_nums_per_writer) {
                    return Status::InternalError(
                            "Too many open partitions {}",
                            config::table_sink_partition_write_max_partition_nums_per_writer);
                }
                std::shared_ptr<VPaimonPartitionWriter> w;
                RETURN_IF_ERROR(create_and_open_writer(i, nullptr, 0, w));
            } else {
                std::shared_ptr<VPaimonPartitionWriter> w;
                if (writer_it->second->written_len() > config::hive_sink_max_file_size) {
                    std::string file_name(writer_it->second->file_name());
                    int file_name_index = writer_it->second->file_name_index();
                    {
                        SCOPED_RAW_TIMER(&_close_ns);
                        static_cast<void>(writer_it->second->close(Status::OK()));
                    }
                    writer_positions.erase(writer_it->second);
                    _partitions_to_writers.erase(writer_it);
                    RETURN_IF_ERROR(
                            create_and_open_writer(i, &file_name, file_name_index + 1, w));
                } else {
                    w = writer_it->second;
                    auto pos_it = writer_positions.find(w);
                    if (pos_it == writer_positions.end()) {
                        IColumn::Filter filter(output_block.rows(), 0);
                        filter[i] = 1;
                        writer_positions.insert({w, std::move(filter)});
                    } else {
                        pos_it->second[i] = 1;
                    }
                }
            }
        }
    }

    SCOPED_RAW_TIMER(&_partition_writers_write_ns);
    output_block.erase(_non_write_columns_indices);
    for (auto& [writer, filter] : writer_positions) {
        Block filtered_block;
        RETURN_IF_ERROR(_filter_block(output_block, &filter, &filtered_block));
        RETURN_IF_ERROR(writer->write(filtered_block));
    }
    return Status::OK();
}

Status VPaimonTableWriter::close(Status status) {
    Status result_status;
    int64_t partitions_count = _partitions_to_writers.size();
    LOG(INFO) << "VPaimonTableWriter::close - called with status.ok()=" << status.ok()
              << ", partitions_count=" << partitions_count << ", row_count=" << _row_count;
    {
        SCOPED_RAW_TIMER(&_close_ns);
        for (const auto& [name, writer] : _partitions_to_writers) {
            LOG(INFO) << "Closing partition writer for: " << name;
            Status st = writer->close(status);
            if (!st.ok()) {
                LOG(WARNING) << "paimon partition writer close failed for " << name << ": "
                             << st.to_string();
                if (result_status.ok()) {
                    result_status = st;
                }
            }
        }
        _partitions_to_writers.clear();
    }
    if (status.ok()) {
        SCOPED_TIMER(_operator_profile->total_time_counter());
        COUNTER_SET(_written_rows_counter, static_cast<int64_t>(_row_count));
        COUNTER_SET(_send_data_timer, _send_data_ns);
        COUNTER_SET(_partition_writers_dispatch_timer, _partition_writers_dispatch_ns);
        COUNTER_SET(_partition_writers_write_timer, _partition_writers_write_ns);
        COUNTER_SET(_partition_writers_count, partitions_count);
        COUNTER_SET(_close_timer, _close_ns);
        COUNTER_SET(_write_file_counter, _write_file_count);
    }
    return result_status;
}

Status VPaimonTableWriter::_filter_block(doris::vectorized::Block& block,
                                         const vectorized::IColumn::Filter* filter,
                                         doris::vectorized::Block* output_block) {
    const ColumnsWithTypeAndName& cols = block.get_columns_with_type_and_name();
    vectorized::ColumnsWithTypeAndName result;
    for (const auto& col : cols) {
        result.emplace_back(col.column->clone_resized(col.column->size()), col.type, col.name);
    }
    *output_block = {std::move(result)};

    std::vector<uint32_t> columns_to_filter(output_block->columns());
    for (uint32_t i = 0; i < output_block->columns(); ++i) {
        columns_to_filter[i] = i;
    }
    Block::filter_block_internal(output_block, columns_to_filter, *filter);
    return Status::OK();
}

std::shared_ptr<VPaimonPartitionWriter> VPaimonTableWriter::_create_partition_writer(
        vectorized::Block& block, int position, const std::string* file_name, int file_name_index) {
    auto& paimon_sink = _t_sink.paimon_table_sink;

    std::string partition_path;
    std::vector<std::string> partition_values;

    if (!_partition_columns_input_index.empty() && position >= 0) {
        partition_values = _create_partition_values(block, position);
        for (int j = 0; j < (int)_partition_columns_input_index.size(); ++j) {
            if (!partition_path.empty()) {
                partition_path += "/";
            }
            const std::string& col_name =
                    paimon_sink.columns[_partition_columns_input_index[j]].name;
            partition_path += VHiveUtils::escape_path_name(col_name) + "=" +
                              VHiveUtils::escape_path_name(partition_values[j]);
        }
    }

    std::string write_path = partition_path.empty()
                                     ? paimon_sink.output_path
                                     : fmt::format("{}/{}", paimon_sink.output_path, partition_path);
    std::string original_write_path = write_path;

    VPaimonPartitionWriter::WriteInfo write_info = {
            .write_path = write_path,
            .original_write_path = original_write_path,
            .file_type = paimon_sink.file_type,
            .broker_addresses = {}};
    if (paimon_sink.__isset.broker_addresses) {
        write_info.broker_addresses.assign(paimon_sink.broker_addresses.begin(),
                                           paimon_sink.broker_addresses.end());
    }

    std::vector<std::string> column_names;
    for (int i = 0; i < (int)paimon_sink.columns.size(); ++i) {
        if (!_non_write_columns_indices.count(i)) {
            column_names.emplace_back(paimon_sink.columns[i].name);
        }
    }

    _write_file_count++;
    return std::make_shared<VPaimonPartitionWriter>(
            partition_values, _write_output_vexpr_ctxs, column_names,
            std::move(write_info), file_name ? *file_name : _compute_file_name(),
            file_name_index, paimon_sink.file_format, paimon_sink.compression_type,
            paimon_sink.hadoop_config);
}

std::vector<std::string> VPaimonTableWriter::_create_partition_values(vectorized::Block& block,
                                                                      int position) {
    std::vector<std::string> partition_values;
    for (int idx : _partition_columns_input_index) {
        vectorized::ColumnWithTypeAndName col = block.get_by_position(idx);
        std::string value = _to_partition_value(
                _vec_output_expr_ctxs[idx]->root()->data_type(), col, position);
        partition_values.emplace_back(value);
    }
    return partition_values;
}

std::string VPaimonTableWriter::_to_partition_value(const DataTypePtr& type,
                                                    const ColumnWithTypeAndName& col,
                                                    int position) {
    ColumnPtr column;
    if (auto* nullable = check_and_get_column<ColumnNullable>(*col.column)) {
        if (nullable->get_null_map_data()[position]) {
            return "__PAIMON_DEFAULT_PARTITION__";
        }
        column = nullable->get_nested_column_ptr();
    } else {
        column = col.column;
    }

    auto [item, size] = column->get_data_at(position);
    switch (type->get_primitive_type()) {
    case TYPE_BOOLEAN: {
        auto val = check_and_get_column<const ColumnUInt8>(*column)->get_data()[position];
        return val ? "true" : "false";
    }
    case TYPE_TINYINT:
        return std::to_string(*reinterpret_cast<const Int8*>(item));
    case TYPE_SMALLINT:
        return std::to_string(*reinterpret_cast<const Int16*>(item));
    case TYPE_INT:
        return std::to_string(*reinterpret_cast<const Int32*>(item));
    case TYPE_BIGINT:
        return std::to_string(*reinterpret_cast<const Int64*>(item));
    case TYPE_FLOAT:
        return std::to_string(*reinterpret_cast<const Float32*>(item));
    case TYPE_DOUBLE:
        return std::to_string(*reinterpret_cast<const Float64*>(item));
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_STRING:
        return std::string(item, size);
    case TYPE_DATE: {
        VecDateTimeValue value = binary_cast<int64_t, VecDateTimeValue>(*(int64_t*)item);
        char buf[64];
        char* pos = value.to_string(buf);
        return std::string(buf, pos - buf - 1);
    }
    case TYPE_DATETIME: {
        VecDateTimeValue value = binary_cast<int64_t, VecDateTimeValue>(*(int64_t*)item);
        char buf[64];
        char* pos = value.to_string(buf);
        return std::string(buf, pos - buf - 1);
    }
    case TYPE_DATEV2: {
        DateV2Value<DateV2ValueType> value =
                binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(*(int32_t*)item);
        char buf[64];
        char* pos = value.to_string(buf);
        return std::string(buf, pos - buf - 1);
    }
    case TYPE_DATETIMEV2: {
        DateV2Value<DateTimeV2ValueType> value =
                binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(*(int64_t*)item);
        char buf[64];
        char* pos = value.to_string(buf);
        return std::string(buf, pos - buf - 1);
    }
    default:
        return std::string(item, size);
    }
}

std::string VPaimonTableWriter::_compute_file_name() {
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    return fmt::format("{}_{}", print_id(_state->query_id()), boost::uuids::to_string(uuid));
}

#include "common/compile_check_end.h"
} // namespace vectorized
} // namespace doris
