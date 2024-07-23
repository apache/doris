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
#include "vec/core/materialize_block.h"
#include "vec/exec/format/table/iceberg/partition_spec_parser.h"
#include "vec/exec/format/table/iceberg/schema_parser.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/sink/writer/iceberg/partition_transformers.h"
#include "vec/sink/writer/iceberg/viceberg_partition_writer.h"
#include "vec/sink/writer/vhive_utils.h"

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
            _iceberg_partition_columns = _to_iceberg_partition_columns();
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
VIcebergTableWriter::_to_iceberg_partition_columns() {
    std::vector<IcebergPartitionColumn> partition_columns;

    std::unordered_map<int, int> id_to_column_idx;
    id_to_column_idx.reserve(_schema->columns().size());
    for (int i = 0; i < _schema->columns().size(); i++) {
        id_to_column_idx[_schema->columns()[i].field_id()] = i;
    }
    for (const auto& partition_field : _partition_spec->fields()) {
        int column_idx = id_to_column_idx[partition_field.source_id()];
        std::unique_ptr<PartitionColumnTransform> partition_column_transform =
                PartitionColumnTransforms::create(
                        partition_field, _vec_output_expr_ctxs[column_idx]->root()->type());
        partition_columns.emplace_back(partition_field,
                                       _vec_output_expr_ctxs[column_idx]->root()->type(),
                                       column_idx, std::move(partition_column_transform));
    }
    return partition_columns;
}

Status VIcebergTableWriter::write(RuntimeState* state, vectorized::Block& block) {
    SCOPED_RAW_TIMER(&_send_data_ns);
    if (block.rows() == 0) {
        return Status::OK();
    }
    Block output_block;
    RETURN_IF_ERROR(vectorized::VExprContext::get_output_block_after_execute_exprs(
            _vec_output_expr_ctxs, block, &output_block, false));
    materialize_block_inplace(output_block);

    std::unordered_map<std::shared_ptr<VIcebergPartitionWriter>, IColumn::Filter> writer_positions;
    _row_count += output_block.rows();

    if (_iceberg_partition_columns.empty()) {
        std::shared_ptr<VIcebergPartitionWriter> writer;
        {
            SCOPED_RAW_TIMER(&_partition_writers_dispatch_ns);
            auto writer_iter = _partitions_to_writers.find("");
            if (writer_iter == _partitions_to_writers.end()) {
                try {
                    writer = _create_partition_writer(output_block, -1);
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
                        writer = _create_partition_writer(output_block, -1, &file_name,
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
        output_block.erase(_non_write_columns_indices);
        RETURN_IF_ERROR(writer->write(output_block));
        return Status::OK();
    }

    {
        SCOPED_RAW_TIMER(&_partition_writers_dispatch_ns);
        _transformed_block.reserve(_iceberg_partition_columns.size());
        for (auto& iceberg_partition_columns : _iceberg_partition_columns) {
            _transformed_block.insert(iceberg_partition_columns.partition_column_transform().apply(
                    output_block, iceberg_partition_columns.source_idx()));
        }
        for (int i = 0; i < output_block.rows(); ++i) {
            std::optional<PartitionData> partition_data;
            try {
                partition_data = _get_partition_data(_transformed_block, i);
            } catch (doris::Exception& e) {
                return e.to_status();
            }
            std::string partition_name;
            DCHECK(partition_data.has_value());
            try {
                partition_name = _partition_to_path(partition_data.value());
            } catch (doris::Exception& e) {
                return e.to_status();
            }
            auto create_and_open_writer =
                    [&](const std::string& partition_name, int position,
                        const std::string* file_name, int file_name_index,
                        std::shared_ptr<VIcebergPartitionWriter>& writer_ptr) -> Status {
                try {
                    auto writer = _create_partition_writer(output_block, position, file_name,
                                                           file_name_index);
                    RETURN_IF_ERROR(writer->open(_state, _profile));
                    IColumn::Filter filter(output_block.rows(), 0);
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
                    IColumn::Filter filter(output_block.rows(), 0);
                    filter[i] = 1;
                    writer_positions.insert({writer, std::move(filter)});
                } else {
                    writer_pos_iter->second[i] = 1;
                }
            }
        }
    }
    SCOPED_RAW_TIMER(&_partition_writers_write_ns);
    output_block.erase(_non_write_columns_indices);
    for (auto it = writer_positions.begin(); it != writer_positions.end(); ++it) {
        Block filtered_block;
        RETURN_IF_ERROR(_filter_block(output_block, &it->second, &filtered_block));
        RETURN_IF_ERROR(it->first->write(filtered_block));
    }
    return Status::OK();
}

Status VIcebergTableWriter::_filter_block(doris::vectorized::Block& block,
                                          const vectorized::IColumn::Filter* filter,
                                          doris::vectorized::Block* output_block) {
    const ColumnsWithTypeAndName& columns_with_type_and_name =
            block.get_columns_with_type_and_name();
    vectorized::ColumnsWithTypeAndName result_columns;
    for (int i = 0; i < columns_with_type_and_name.size(); ++i) {
        const auto& col = columns_with_type_and_name[i];
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

Status VIcebergTableWriter::close(Status status) {
    int64_t partitions_to_writers_size = _partitions_to_writers.size();
    {
        SCOPED_RAW_TIMER(&_close_ns);
        for (const auto& pair : _partitions_to_writers) {
            Status st = pair.second->close(status);
            if (st != Status::OK()) {
                LOG(WARNING) << fmt::format("partition writer close failed for partition {}",
                                            st.to_string());
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

std::string VIcebergTableWriter::_partition_to_path(const doris::iceberg::StructLike& data) {
    std::stringstream ss;
    for (size_t i = 0; i < _iceberg_partition_columns.size(); i++) {
        auto& iceberg_partition_column = _iceberg_partition_columns[i];
        TypeDescriptor result_type =
                iceberg_partition_column.partition_column_transform().get_result_type();
        std::string value_string =
                iceberg_partition_column.partition_column_transform().to_human_string(result_type,
                                                                                      data.get(i));
        if (i > 0) {
            ss << "/";
        }
        ss << _escape(iceberg_partition_column.field().name()) << '=' << _escape(value_string);
    }

    return ss.str();
}

std::string VIcebergTableWriter::_escape(const std::string& path) {
    return VHiveUtils::escape_path_name(path);
}

std::vector<std::string> VIcebergTableWriter::_partition_values(
        const doris::iceberg::StructLike& data) {
    std::vector<std::string> partition_values;
    partition_values.reserve(_iceberg_partition_columns.size());
    for (size_t i = 0; i < _iceberg_partition_columns.size(); i++) {
        auto& iceberg_partition_column = _iceberg_partition_columns[i];
        TypeDescriptor result_type =
                iceberg_partition_column.partition_column_transform().get_result_type();
        partition_values.emplace_back(
                iceberg_partition_column.partition_column_transform().get_partition_value(
                        result_type, data.get(i)));
    }

    return partition_values;
}

std::shared_ptr<VIcebergPartitionWriter> VIcebergTableWriter::_create_partition_writer(
        vectorized::Block& block, int position, const std::string* file_name, int file_name_index) {
    auto& iceberg_table_sink = _t_sink.iceberg_table_sink;
    std::optional<PartitionData> partition_data;
    partition_data = _get_partition_data(_transformed_block, position);
    std::string partition_path;
    std::vector<std::string> partition_values;
    if (partition_data.has_value()) {
        partition_path = _partition_to_path(partition_data.value());
        partition_values = _partition_values(partition_data.value());
    }
    const std::string& output_path = iceberg_table_sink.output_path;

    std::string write_path;
    std::string original_write_path;
    std::string target_path;
    if (partition_path.empty()) {
        original_write_path = iceberg_table_sink.original_output_path;
        target_path = output_path;
        write_path = output_path;
    } else {
        original_write_path =
                fmt::format("{}/{}", iceberg_table_sink.original_output_path, partition_path);
        target_path = fmt::format("{}/{}", output_path, partition_path);
        write_path = fmt::format("{}/{}", output_path, partition_path);
    }

    VIcebergPartitionWriter::WriteInfo write_info = {
            std::move(write_path), std::move(original_write_path), std::move(target_path),
            iceberg_table_sink.file_type};

    _write_file_count++;
    std::vector<std::string> column_names;
    column_names.reserve(_write_output_vexpr_ctxs.size());
    for (int i = 0; i < _schema->columns().size(); i++) {
        if (_non_write_columns_indices.find(i) == _non_write_columns_indices.end()) {
            column_names.emplace_back(_schema->columns()[i].field_name());
        }
    }
    return std::make_shared<VIcebergPartitionWriter>(
            _t_sink, std::move(partition_values), _write_output_vexpr_ctxs, *_schema,
            &_t_sink.iceberg_table_sink.schema_json, std::move(column_names), std::move(write_info),
            (file_name == nullptr) ? _compute_file_name() : *file_name, file_name_index,
            iceberg_table_sink.file_format, iceberg_table_sink.compression_type,
            iceberg_table_sink.hadoop_config);
}

std::optional<PartitionData> VIcebergTableWriter::_get_partition_data(
        vectorized::Block& transformed_block, int position) {
    if (_iceberg_partition_columns.empty()) {
        return std::nullopt;
    }

    std::vector<std::any> values;
    values.reserve(_iceberg_partition_columns.size());
    int column_idx = 0;
    for (auto& iceberg_partition_column : _iceberg_partition_columns) {
        const vectorized::ColumnWithTypeAndName& partition_column =
                transformed_block.get_by_position(column_idx);
        const TypeDescriptor& result_type =
                iceberg_partition_column.partition_column_transform().get_result_type();
        auto value = _get_iceberg_partition_value(result_type, partition_column, position);
        values.emplace_back(value);
        ++column_idx;
    }
    return PartitionData(std::move(values));
}

std::any VIcebergTableWriter::_get_iceberg_partition_value(
        const TypeDescriptor& type_desc, const ColumnWithTypeAndName& partition_column,
        int position) {
    //1) get the partition column ptr
    ColumnPtr col_ptr = partition_column.column->convert_to_full_column_if_const();
    CHECK(col_ptr != nullptr);
    if (col_ptr->is_nullable()) {
        const ColumnNullable* nullable_column =
                reinterpret_cast<const vectorized::ColumnNullable*>(col_ptr.get());
        auto* __restrict null_map_data = nullable_column->get_null_map_data().data();
        if (null_map_data[position]) {
            return std::any();
        }
        col_ptr = nullable_column->get_nested_column_ptr();
    }

    //2) get parition field data from paritionblock
    auto [item, size] = col_ptr->get_data_at(position);
    switch (type_desc.type) {
    case TYPE_BOOLEAN: {
        vectorized::Field field =
                vectorized::check_and_get_column<const ColumnUInt8>(*col_ptr)->operator[](position);
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
