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

#include "viceberg_table_writer.h"

#include "runtime/runtime_state.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/exec/format/table/iceberg/partition_spec_parser.h"
#include "vec/exec/format/table/iceberg/schema_parser.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/sink/writer/iceberg/partition_transformers.h"
#include "vec/sink/writer/iceberg/viceberg_partition_writer.h"

namespace doris {
namespace vectorized {

VIcebergTableWriter::VIcebergTableWriter(const TDataSink& t_sink,
                                         const VExprContextSPtrs& output_expr_ctxs)
        : AsyncResultWriter(output_expr_ctxs), _t_sink(t_sink) {
    DCHECK(_t_sink.__isset.iceberg_table_sink);
}

Status VIcebergTableWriter::init_properties(ObjectPool* pool) {
    return Status::OK();
}

Status VIcebergTableWriter::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;
    _profile = profile;

    // add all counter
    _written_rows_counter = ADD_COUNTER(_profile, "WrittenRows", TUnit::UNIT);
    _send_data_timer = ADD_TIMER(_profile, "SendDataTime");
    _partition_writers_dispatch_timer =
            ADD_CHILD_TIMER(_profile, "PartitionsDispatchTime", "SendDataTime");
    _partition_writers_write_timer =
            ADD_CHILD_TIMER(_profile, "PartitionsWriteTime", "SendDataTime");
    _partition_writers_count = ADD_COUNTER(_profile, "PartitionsWriteCount", TUnit::UNIT);
    _open_timer = ADD_TIMER(_profile, "OpenTime");
    _close_timer = ADD_TIMER(_profile, "CloseTime");
    _write_file_counter = ADD_COUNTER(_profile, "WriteFileCount", TUnit::UNIT);

    SCOPED_TIMER(_open_timer);
    try {
        _schema = iceberg::SchemaParser::from_json(_t_sink.iceberg_table_sink.schema_json);
        std::string partition_spec_json =
                _t_sink.iceberg_table_sink
                        .partition_specs_json[_t_sink.iceberg_table_sink.partition_spec_id];
        if (!partition_spec_json.empty()) {
            _partition_spec = iceberg::PartitionSpecParser::from_json(_schema, partition_spec_json);
            _iceberg_partition_columns = _to_iceberg_partition_columns(*_partition_spec);
        }
    } catch (doris::Exception& e) {
        return e.to_status();
    }

    std::set<int> partition_idx_set;
    for (const auto& iceberg_partition_column : _iceberg_partition_columns) {
        partition_idx_set.insert(iceberg_partition_column.source_idx());
    }

    for (int i = 0; i < _schema->columns().size(); ++i) {
        _write_output_vexpr_ctxs.emplace_back(_vec_output_expr_ctxs[i]);
    }

    return Status::OK();
}

std::vector<VIcebergTableWriter::IcebergPartitionColumn>
VIcebergTableWriter::_to_iceberg_partition_columns(
        const doris::iceberg::PartitionSpec& partition_spec) {
    std::vector<IcebergPartitionColumn> partition_columns;

    std::unordered_map<int, int> id_to_column_idx;
    id_to_column_idx.reserve(_schema->columns().size());
    for (int i = 0; i < _schema->columns().size(); i++) {
        id_to_column_idx[_schema->columns()[i].field_id()] = i;
    }
    for (const auto& partition_field : partition_spec.fields()) {
        int column_idx = id_to_column_idx[partition_field.source_id()];
        const iceberg::NestedField* field = _schema->find_field(partition_field.source_id());

        iceberg::Type* input_type = field->field_type();
        PartitionColumnTransform transform =
                PartitionColumnTransform::create(partition_field, *input_type);
        partition_columns.emplace_back(partition_field, *input_type, column_idx, transform.type(),
                                       transform.block_transform());
    }

    return partition_columns;
}

Status VIcebergTableWriter::write(vectorized::Block& block) {
    SCOPED_RAW_TIMER(&_send_data_ns);
    std::unordered_map<std::shared_ptr<VIcebergPartitionWriter>, IColumn::Filter> writer_positions;
    _row_count += block.rows();

    if (_iceberg_partition_columns.empty()) {
        std::shared_ptr<VIcebergPartitionWriter> writer;
        {
            SCOPED_RAW_TIMER(&_partition_writers_dispatch_ns);
            auto writer_iter = _partitions_to_writers.find("");
            if (writer_iter == _partitions_to_writers.end()) {
                try {
                    writer = _create_partition_writer(block, -1);
                } catch (doris::Exception& e) {
                    return e.to_status();
                }
                _partitions_to_writers.insert({"", writer});
                RETURN_IF_ERROR(writer->open(_state, _profile));
            } else {
                if (writer_iter->second->written_len() > config::iceberg_sink_max_file_size) {
                    std::string file_name(writer_iter->second->file_name());
                    int file_name_index = writer_iter->second->file_name_index();
                    {
                        SCOPED_RAW_TIMER(&_close_ns);
                        static_cast<void>(writer_iter->second->close(Status::OK()));
                    }
                    _partitions_to_writers.erase(writer_iter);
                    try {
                        writer = _create_partition_writer(block, -1, &file_name,
                                                          file_name_index + 1);
                    } catch (doris::Exception& e) {
                        return e.to_status();
                    }
                    _partitions_to_writers.insert({"", writer});
                    RETURN_IF_ERROR(writer->open(_state, _profile));
                } else {
                    writer = writer_iter->second;
                }
            }
        }
        SCOPED_RAW_TIMER(&_partition_writers_write_ns);
        RETURN_IF_ERROR(writer->write(block));
        return Status::OK();
    }

    {
        SCOPED_RAW_TIMER(&_partition_writers_dispatch_ns);
        for (int i = 0; i < block.rows(); ++i) {
            std::optional<PartitionData> partition_data;
            try {
                partition_data = _get_partition_data(block, i);
            } catch (doris::Exception& e) {
                return e.to_status();
            }
            std::string partition_name;
            DCHECK(partition_data.has_value());

            partition_name = _partition_spec->partition_to_path(partition_data.value());
            auto create_and_open_writer =
                    [&](const std::string& partition_name, int position,
                        const std::string* file_name, int file_name_index,
                        std::shared_ptr<VIcebergPartitionWriter>& writer_ptr) -> Status {
                try {
                    auto writer =
                            _create_partition_writer(block, position, file_name, file_name_index);
                    RETURN_IF_ERROR(writer->open(_state, _profile));
                    IColumn::Filter filter(block.rows(), 0);
                    filter[position] = 1;
                    writer_positions.insert({writer, std::move(filter)});
                    _partitions_to_writers.insert({partition_name, writer});
                    writer_ptr = writer;
                } catch (doris::Exception& e) {
                    return e.to_status();
                }
                return Status::OK();
            };

            auto writer_iter = _partitions_to_writers.find(partition_name);
            if (writer_iter == _partitions_to_writers.end()) {
                std::shared_ptr<VIcebergPartitionWriter> writer;
                if (_partitions_to_writers.size() + 1 >
                    config::table_sink_partition_write_max_partition_nums_per_writer) {
                    return Status::InternalError(
                            "Too many open partitions {}",
                            config::table_sink_partition_write_max_partition_nums_per_writer);
                }
                RETURN_IF_ERROR(create_and_open_writer(partition_name, i, nullptr, 0, writer));
            } else {
                std::shared_ptr<VIcebergPartitionWriter> writer;
                if (writer_iter->second->written_len() > config::iceberg_sink_max_file_size) {
                    std::string file_name(writer_iter->second->file_name());
                    int file_name_index = writer_iter->second->file_name_index();
                    {
                        SCOPED_RAW_TIMER(&_close_ns);
                        static_cast<void>(writer_iter->second->close(Status::OK()));
                    }
                    writer_positions.erase(writer_iter->second);
                    _partitions_to_writers.erase(writer_iter);
                    RETURN_IF_ERROR(create_and_open_writer(partition_name, i, &file_name,
                                                           file_name_index + 1, writer));
                } else {
                    writer = writer_iter->second;
                }
                auto writer_pos_iter = writer_positions.find(writer);
                if (writer_pos_iter == writer_positions.end()) {
                    IColumn::Filter filter(block.rows(), 0);
                    filter[i] = 1;
                    writer_positions.insert({writer, std::move(filter)});
                } else {
                    writer_pos_iter->second[i] = 1;
                }
            }
        }
    }
    SCOPED_RAW_TIMER(&_partition_writers_write_ns);
    for (auto it = writer_positions.begin(); it != writer_positions.end(); ++it) {
        RETURN_IF_ERROR(it->first->write(block, &it->second));
    }
    return Status::OK();
}

Status VIcebergTableWriter::close(Status status) {
    int64_t partitions_to_writers_size = _partitions_to_writers.size();
    {
        SCOPED_RAW_TIMER(&_close_ns);
        for (const auto& pair : _partitions_to_writers) {
            Status st = pair.second->close(status);
            if (st != Status::OK()) {
                LOG(WARNING) << fmt::format("Unsupported type for partition {}", st.to_string());
                continue;
            }
        }
        _partitions_to_writers.clear();
    }
    if (status.ok()) {
        SCOPED_TIMER(_profile->total_time_counter());

        COUNTER_SET(_written_rows_counter, static_cast<int64_t>(_row_count));
        COUNTER_SET(_send_data_timer, _send_data_ns);
        COUNTER_SET(_partition_writers_dispatch_timer, _partition_writers_dispatch_ns);
        COUNTER_SET(_partition_writers_write_timer, _partition_writers_write_ns);
        COUNTER_SET(_partition_writers_count, partitions_to_writers_size);
        COUNTER_SET(_close_timer, _close_ns);
        COUNTER_SET(_write_file_counter, _write_file_count);
    }
    return Status::OK();
}

std::shared_ptr<VIcebergPartitionWriter> VIcebergTableWriter::_create_partition_writer(
        vectorized::Block& block, int position, const std::string* file_name, int file_name_index) {
    auto& iceberg_table_sink = _t_sink.iceberg_table_sink;
    std::vector<std::string> partition_values;
    std::optional<PartitionData> partition_data;
    partition_data = _get_partition_data(block, position);
    std::string partition_path;
    std::vector<std::string> partitionValues;
    if (partition_data.has_value()) {
        partition_path = _partition_spec->partition_to_path(partition_data.value());
        partitionValues = _partition_spec->partition_values(partition_data.value());
    } else {
    }
    const std::string& output_path = iceberg_table_sink.output_path;

    auto write_path = fmt::format("{}/{}", output_path, partition_path);
    auto original_write_path = fmt::format("{}/{}", output_path, partition_path);
    auto target_path = fmt::format("{}/{}", output_path, partition_path);

    VIcebergPartitionWriter::WriteInfo write_info = {std::move(write_path),
                                                     std::move(original_write_path),
                                                     std::move(target_path), TFileType::FILE_HDFS};

    _write_file_count++;
    return std::make_shared<VIcebergPartitionWriter>(
            _t_sink, std::move(partitionValues), _vec_output_expr_ctxs, _write_output_vexpr_ctxs,
            _non_write_columns_indices, *_schema, std::move(write_info),
            (file_name == nullptr) ? _compute_file_name() : *file_name, file_name_index,
            iceberg_table_sink.file_format, iceberg_table_sink.compression_type,
            iceberg_table_sink.hadoop_config);
}

std::optional<PartitionData> VIcebergTableWriter::_get_partition_data(vectorized::Block& block,
                                                                      int position) {
    if (_iceberg_partition_columns.empty()) {
        return std::nullopt;
    }

    std::vector<std::any> values(_iceberg_partition_columns.size());
    std::transform(_iceberg_partition_columns.begin(), _iceberg_partition_columns.end(),
                   values.begin(), [&](const IcebergPartitionColumn& column) {
                       vectorized::ColumnWithTypeAndName partition_column =
                               block.get_by_position(column.source_idx());
                       return _get_iceberg_partition_value(
                               _vec_output_expr_ctxs[column.source_idx()]->root()->type(),
                               partition_column, position);
                   });

    return PartitionData(std::move(values));
}

std::any VIcebergTableWriter::_get_iceberg_partition_value(
        const TypeDescriptor& type_desc, const ColumnWithTypeAndName& partition_column,
        int position) {
    ColumnPtr column;
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*partition_column.column)) {
        auto* __restrict null_map_data = nullable_column->get_null_map_data().data();
        if (null_map_data[position]) {
            return StringRef();
        }
        column = nullable_column->get_nested_column_ptr();
    } else {
        column = partition_column.column;
    }
    auto [item, size] = column->get_data_at(position);
    switch (type_desc.type) {
    case TYPE_BOOLEAN: {
        vectorized::Field field =
                vectorized::check_and_get_column<const ColumnUInt8>(*column)->operator[](position);
        return field.get<bool>();
    }
    case TYPE_TINYINT: {
        return *reinterpret_cast<const Int8*>(item);
    }
    case TYPE_SMALLINT: {
        return *reinterpret_cast<const Int16*>(item);
    }
    case TYPE_INT: {
        return *reinterpret_cast<const Int32*>(item);
    }
    case TYPE_BIGINT: {
        return *reinterpret_cast<const Int64*>(item);
    }
    case TYPE_FLOAT: {
        return *reinterpret_cast<const Float32*>(item);
    }
    case TYPE_DOUBLE: {
        return *reinterpret_cast<const Float64*>(item);
    }
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_STRING: {
        return std::string(item, size);
    }
    case TYPE_DATE: {
        return binary_cast<int64_t, doris::VecDateTimeValue>(*(int64_t*)item);
    }
    case TYPE_DATETIME: {
        return binary_cast<int64_t, doris::VecDateTimeValue>(*(int64_t*)item);
    }
    case TYPE_DATEV2: {
        return binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(*(int32_t*)item);
    }
    case TYPE_DATETIMEV2: {
        return binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(*(int64_t*)item);
    }
    case TYPE_DECIMALV2: {
        return *(Decimal128V2*)(item);
    }
    case TYPE_DECIMAL32: {
        return *(Decimal32*)(item);
    }
    case TYPE_DECIMAL64: {
        return *(Decimal64*)(item);
    }
    case TYPE_DECIMAL128I: {
        return *(Decimal128V3*)(item);
    }
    case TYPE_DECIMAL256: {
        return *(Decimal256*)(item);
    }
    default: {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Unsupported type for partition {}", type_desc.debug_string());
    }
    }
}

std::string VIcebergTableWriter::_compute_file_name() {
    boost::uuids::uuid uuid = boost::uuids::random_generator()();

    std::string uuid_str = boost::uuids::to_string(uuid);

    return fmt::format("{}_{}", print_id(_state->query_id()), uuid_str);
}

} // namespace vectorized
} // namespace doris
