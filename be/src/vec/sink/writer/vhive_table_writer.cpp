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

#include "vhive_table_writer.h"

#include "runtime/runtime_state.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/sink/writer/vhive_partition_writer.h"
#include "vec/sink/writer/vhive_utils.h"

namespace doris {
namespace vectorized {

VHiveTableWriter::VHiveTableWriter(const TDataSink& t_sink,
                                   const VExprContextSPtrs& output_expr_ctxs)
        : AsyncResultWriter(output_expr_ctxs), _t_sink(t_sink) {
    DCHECK(_t_sink.__isset.hive_table_sink);
}

Status VHiveTableWriter::init_properties(ObjectPool* pool) {
    return Status::OK();
}

Status VHiveTableWriter::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;
    _profile = profile;

    for (int i = 0; i < _t_sink.hive_table_sink.columns.size(); ++i) {
        if (_t_sink.hive_table_sink.columns[i].column_type == THiveColumnType::PARTITION_KEY) {
            _partition_columns_input_index.emplace_back(i);
        }
    }
    return Status::OK();
}

Status VHiveTableWriter::write(vectorized::Block& block) {
    std::unordered_map<std::shared_ptr<VHivePartitionWriter>, IColumn::Filter> writer_positions;

    auto& hive_table_sink = _t_sink.hive_table_sink;

    if (_partition_columns_input_index.empty()) {
        auto writer_iter = _partitions_to_writers.find("");
        if (writer_iter == _partitions_to_writers.end()) {
            try {
                std::shared_ptr<VHivePartitionWriter> writer = _create_partition_writer(block, -1);
                _partitions_to_writers.insert({"", writer});
                RETURN_IF_ERROR(writer->open(_state, _profile));
                RETURN_IF_ERROR(writer->write(block));
            } catch (doris::Exception& e) {
                return e.to_status();
            }
            return Status::OK();
        } else {
            std::shared_ptr<VHivePartitionWriter> writer;
            if (writer_iter->second->written_len() > config::hive_sink_max_file_size) {
                static_cast<void>(writer_iter->second->close(Status::OK()));
                _partitions_to_writers.erase(writer_iter);
                try {
                    writer = _create_partition_writer(block, -1);
                    _partitions_to_writers.insert({"", writer});
                    RETURN_IF_ERROR(writer->open(_state, _profile));
                    RETURN_IF_ERROR(writer->write(block));
                } catch (doris::Exception& e) {
                    return e.to_status();
                }
            } else {
                writer = writer_iter->second;
            }
            RETURN_IF_ERROR(writer->write(block));
            return Status::OK();
        }
    }

    for (int i = 0; i < block.rows(); ++i) {
        std::vector<std::string> partition_values;
        try {
            partition_values = _create_partition_values(block, i);
        } catch (doris::Exception& e) {
            return e.to_status();
        }
        std::string partition_name = VHiveUtils::make_partition_name(
                hive_table_sink.columns, _partition_columns_input_index, partition_values);

        auto create_and_open_writer =
                [&](const std::string& partition_name, int position,
                    std::shared_ptr<VHivePartitionWriter>& writer_ptr) -> Status {
            try {
                auto writer = _create_partition_writer(block, position);
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
            std::shared_ptr<VHivePartitionWriter> writer;
            RETURN_IF_ERROR(create_and_open_writer(partition_name, i, writer));
        } else {
            std::shared_ptr<VHivePartitionWriter> writer;
            if (writer_iter->second->written_len() > config::hive_sink_max_file_size) {
                static_cast<void>(writer_iter->second->close(Status::OK()));
                writer_positions.erase(writer_iter->second);
                _partitions_to_writers.erase(writer_iter);
                RETURN_IF_ERROR(create_and_open_writer(partition_name, i, writer));
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

    for (auto it = writer_positions.begin(); it != writer_positions.end(); ++it) {
        RETURN_IF_ERROR(it->first->write(block, &it->second));
    }
    return Status::OK();
}

Status VHiveTableWriter::close(Status status) {
    for (const auto& pair : _partitions_to_writers) {
        Status st = pair.second->close(status);
        if (st != Status::OK()) {
            LOG(WARNING) << fmt::format("Unsupported type for partition {}", st.to_string());
            continue;
        }
    }
    _partitions_to_writers.clear();
    return Status::OK();
}

std::shared_ptr<VHivePartitionWriter> VHiveTableWriter::_create_partition_writer(
        vectorized::Block& block, int position) {
    auto& hive_table_sink = _t_sink.hive_table_sink;
    std::vector<std::string> partition_values;
    std::string partition_name;
    if (!_partition_columns_input_index.empty()) {
        partition_values = _create_partition_values(block, position);
        partition_name = VHiveUtils::make_partition_name(
                hive_table_sink.columns, _partition_columns_input_index, partition_values);
    }
    const std::vector<THivePartition>& partitions = hive_table_sink.partitions;
    const THiveLocationParams& write_location = hive_table_sink.location;
    const THivePartition* existing_partition = nullptr;
    bool existing_table = true;
    for (const auto& partition : partitions) {
        if (partition_values == partition.values) {
            existing_partition = &partition;
            break;
        }
    }
    TUpdateMode::type update_mode;
    VHivePartitionWriter::WriteInfo write_info;
    TFileFormatType::type file_format_type;
    TFileCompressType::type write_compress_type;
    if (existing_partition == nullptr) { // new partition
        if (existing_table == false) {   // new table
            update_mode = TUpdateMode::NEW;
            if (_partition_columns_input_index.empty()) { // new unpartitioned table
                write_info = {write_location.write_path, write_location.target_path,
                              write_location.file_type};
            } else { // a new partition in a new partitioned table
                auto write_path = fmt::format("{}/{}", write_location.write_path, partition_name);
                auto target_path = fmt::format("{}/{}", write_location.target_path, partition_name);
                write_info = {std::move(write_path), std::move(target_path),
                              write_location.file_type};
            }
        } else { // a new partition in an existing partitioned table, or an existing unpartitioned table
            if (_partition_columns_input_index.empty()) { // an existing unpartitioned table
                update_mode =
                        !hive_table_sink.overwrite ? TUpdateMode::APPEND : TUpdateMode::OVERWRITE;
                write_info = {write_location.write_path, write_location.target_path,
                              write_location.file_type};
            } else { // a new partition in an existing partitioned table
                update_mode = TUpdateMode::NEW;
                auto write_path = fmt::format("{}/{}", write_location.write_path, partition_name);
                auto target_path = fmt::format("{}/{}", write_location.target_path, partition_name);
                write_info = {std::move(write_path), std::move(target_path),
                              write_location.file_type};
            }
            // need to get schema from existing table ?
        }
        file_format_type = hive_table_sink.file_format;
        write_compress_type = hive_table_sink.compression_type;
    } else { // existing partition
        if (!hive_table_sink.overwrite) {
            update_mode = TUpdateMode::APPEND;
            auto write_path = fmt::format("{}/{}", write_location.write_path, partition_name);
            auto target_path = fmt::format("{}", existing_partition->location.target_path);
            write_info = {std::move(write_path), std::move(target_path),
                          existing_partition->location.file_type};
            file_format_type = existing_partition->file_format;
            write_compress_type = hive_table_sink.compression_type;
        } else {
            update_mode = TUpdateMode::OVERWRITE;
            auto write_path = fmt::format("{}/{}", write_location.write_path, partition_name);
            auto target_path = fmt::format("{}/{}", write_location.target_path, partition_name);
            write_info = {std::move(write_path), std::move(target_path), write_location.file_type};
            file_format_type = hive_table_sink.file_format;
            write_compress_type = hive_table_sink.compression_type;
            // need to get schema from existing table ?
        }
    }

    return std::make_shared<VHivePartitionWriter>(
            _t_sink, std::move(partition_name), update_mode, _vec_output_expr_ctxs,
            hive_table_sink.columns, std::move(write_info),
            fmt::format("{}{}", _compute_file_name(),
                        _get_file_extension(file_format_type, write_compress_type)),
            file_format_type, write_compress_type, hive_table_sink.hadoop_config);
}

std::vector<std::string> VHiveTableWriter::_create_partition_values(vectorized::Block& block,
                                                                    int position) {
    std::vector<std::string> partition_values;
    for (int i = 0; i < _partition_columns_input_index.size(); ++i) {
        int partition_column_idx = _partition_columns_input_index[i];
        vectorized::ColumnWithTypeAndName partition_column =
                block.get_by_position(partition_column_idx);
        std::string value =
                _to_partition_value(_vec_output_expr_ctxs[partition_column_idx]->root()->type(),
                                    partition_column, position);

        // Check if value contains only printable ASCII characters
        bool isValid = true;
        for (char c : value) {
            if (c < 0x20 || c > 0x7E) {
                isValid = false;
                break;
            }
        }

        if (!isValid) {
            // Encode value using Base16 encoding with space separator
            std::stringstream encoded;
            for (unsigned char c : value) {
                encoded << std::hex << std::setw(2) << std::setfill('0') << (int)c;
                encoded << " ";
            }
            throw doris::Exception(
                    doris::ErrorCode::INTERNAL_ERROR,
                    "Hive partition values can only contain printable ASCII characters (0x20 - "
                    "0x7E). Invalid value: {}",
                    encoded.str());
        }

        partition_values.emplace_back(value);
    }

    return partition_values;
}

std::string VHiveTableWriter::_to_partition_value(const TypeDescriptor& type_desc,
                                                  const ColumnWithTypeAndName& partition_column,
                                                  int position) {
    ColumnPtr column;
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*partition_column.column)) {
        auto* __restrict null_map_data = nullable_column->get_null_map_data().data();
        if (null_map_data[position]) {
            return "__HIVE_DEFAULT_PARTITION__";
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
        return std::to_string(field.get<bool>());
    }
    case TYPE_TINYINT: {
        return std::to_string(*reinterpret_cast<const Int8*>(item));
    }
    case TYPE_SMALLINT: {
        return std::to_string(*reinterpret_cast<const Int16*>(item));
    }
    case TYPE_INT: {
        return std::to_string(*reinterpret_cast<const Int32*>(item));
    }
    case TYPE_BIGINT: {
        return std::to_string(*reinterpret_cast<const Int64*>(item));
    }
    case TYPE_FLOAT: {
        return std::to_string(*reinterpret_cast<const Float32*>(item));
    }
    case TYPE_DOUBLE: {
        return std::to_string(*reinterpret_cast<const Float64*>(item));
    }
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_STRING: {
        return std::string(item, size);
    }
    case TYPE_DATE: {
        VecDateTimeValue value = binary_cast<int64_t, doris::VecDateTimeValue>(*(int64_t*)item);

        char buf[64];
        char* pos = value.to_string(buf);
        return std::string(buf, pos - buf - 1);
    }
    case TYPE_DATETIME: {
        VecDateTimeValue value = binary_cast<int64_t, doris::VecDateTimeValue>(*(int64_t*)item);

        char buf[64];
        char* pos = value.to_string(buf);
        return std::string(buf, pos - buf - 1);
        break;
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
        char* pos = value.to_string(buf, type_desc.scale);
        return std::string(buf, pos - buf - 1);
    }
    case TYPE_DECIMALV2: {
        Decimal128V2 value = *(Decimal128V2*)(item);
        return value.to_string(type_desc.scale);
    }
    case TYPE_DECIMAL32: {
        Decimal32 value = *(Decimal32*)(item);
        return value.to_string(type_desc.scale);
    }
    case TYPE_DECIMAL64: {
        Decimal64 value = *(Decimal64*)(item);
        return value.to_string(type_desc.scale);
    }
    case TYPE_DECIMAL128I: {
        Decimal128V3 value = *(Decimal128V3*)(item);
        return value.to_string(type_desc.scale);
    }
    case TYPE_DECIMAL256: {
        Decimal256 value = *(Decimal256*)(item);
        return value.to_string(type_desc.scale);
    }
    default: {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Unsupported type for partition {}", type_desc.debug_string());
    }
    }
}

std::string VHiveTableWriter::_get_file_extension(TFileFormatType::type file_format_type,
                                                  TFileCompressType::type write_compress_type) {
    std::string compress_name;
    switch (write_compress_type) {
    case TFileCompressType::SNAPPYBLOCK: {
        compress_name = ".snappy";
        break;
    }
    case TFileCompressType::ZLIB: {
        compress_name = ".zlib";
        break;
    }
    case TFileCompressType::ZSTD: {
        compress_name = ".zstd";
        break;
    }
    default: {
        compress_name = "";
        break;
    }
    }

    std::string file_format_name;
    switch (file_format_type) {
    case TFileFormatType::FORMAT_PARQUET: {
        file_format_name = ".parquet";
        break;
    }
    case TFileFormatType::FORMAT_ORC: {
        file_format_name = ".orc";
        break;
    }
    default: {
        file_format_name = "";
        break;
    }
    }
    return fmt::format("{}{}", compress_name, file_format_name);
}

std::string VHiveTableWriter::_compute_file_name() {
    boost::uuids::uuid uuid = boost::uuids::random_generator()();

    std::string uuid_str = boost::uuids::to_string(uuid);

    return fmt::format("{}_{}", print_id(_state->query_id()), uuid_str);
}

} // namespace vectorized
} // namespace doris
