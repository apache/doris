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

#include "vhive_partition_writer.h"

#include "io/file_factory.h"
#include "runtime/runtime_state.h"
#include "vec/core/materialize_block.h"
#include "vec/runtime/vorc_transformer.h"
#include "vec/runtime/vparquet_transformer.h"

namespace doris {
namespace vectorized {

VHivePartitionWriter::VHivePartitionWriter(const TDataSink& t_sink, std::string partition_name,
                                           TUpdateMode::type update_mode,
                                           const VExprContextSPtrs& output_expr_ctxs,
                                           const std::vector<THiveColumn>& columns,
                                           WriteInfo write_info, std::string file_name,
                                           TFileFormatType::type file_format_type,
                                           TFileCompressType::type hive_compress_type,
                                           const std::map<std::string, std::string>& hadoop_conf)
        : _partition_name(std::move(partition_name)),
          _update_mode(update_mode),
          _vec_output_expr_ctxs(output_expr_ctxs),
          _columns(columns),
          _write_info(std::move(write_info)),
          _file_name(std::move(file_name)),
          _file_format_type(file_format_type),
          _hive_compress_type(hive_compress_type),
          _hadoop_conf(hadoop_conf) {}

Status VHivePartitionWriter::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;

    io::FSPropertiesRef fs_properties(_write_info.file_type);
    fs_properties.properties = &_hadoop_conf;
    io::FileDescription file_description = {
            .path = fmt::format("{}/{}", _write_info.write_path, _file_name)};
    _fs = DORIS_TRY(FileFactory::create_fs(fs_properties, file_description));
    RETURN_IF_ERROR(_fs->create_file(file_description.path, &_file_writer));

    switch (_file_format_type) {
    case TFileFormatType::FORMAT_PARQUET: {
        bool parquet_disable_dictionary = false;
        TParquetCompressionType::type parquet_compression_type;
        switch (_hive_compress_type) {
        case TFileCompressType::PLAIN: {
            parquet_compression_type = TParquetCompressionType::UNCOMPRESSED;
            break;
        }
        case TFileCompressType::SNAPPYBLOCK: {
            parquet_compression_type = TParquetCompressionType::SNAPPY;
            break;
        }
        case TFileCompressType::ZSTD: {
            parquet_compression_type = TParquetCompressionType::ZSTD;
            break;
        }
        default: {
            return Status::InternalError("Unsupported hive compress type {} with parquet",
                                         to_string(_hive_compress_type));
        }
        }
        std::vector<TParquetSchema> parquet_schemas;
        parquet_schemas.reserve(_columns.size());
        for (int i = 0; i < _columns.size(); i++) {
            VExprSPtr column_expr = _vec_output_expr_ctxs[i]->root();
            TParquetSchema parquet_schema;
            parquet_schema.schema_column_name = _columns[i].name;
            parquet_schemas.emplace_back(std::move(parquet_schema));
        }
        _file_format_transformer.reset(new VParquetTransformer(
                state, _file_writer.get(), _vec_output_expr_ctxs, parquet_schemas,
                parquet_compression_type, parquet_disable_dictionary, TParquetVersion::PARQUET_1_0,
                false));
        return _file_format_transformer->open();
    }
    case TFileFormatType::FORMAT_ORC: {
        orc::CompressionKind orc_compression_type;
        switch (_hive_compress_type) {
        case TFileCompressType::PLAIN: {
            orc_compression_type = orc::CompressionKind::CompressionKind_NONE;
            break;
        }
        case TFileCompressType::SNAPPYBLOCK: {
            orc_compression_type = orc::CompressionKind::CompressionKind_SNAPPY;
            break;
        }
        case TFileCompressType::ZLIB: {
            orc_compression_type = orc::CompressionKind::CompressionKind_ZLIB;
            break;
        }
        case TFileCompressType::ZSTD: {
            orc_compression_type = orc::CompressionKind::CompressionKind_ZSTD;
            break;
        }
        default: {
            return Status::InternalError("Unsupported type {} with orc", _hive_compress_type);
        }
        }
        orc_compression_type = orc::CompressionKind::CompressionKind_ZLIB;

        std::unique_ptr<orc::Type> root_schema = orc::createStructType();
        for (int i = 0; i < _columns.size(); i++) {
            VExprSPtr column_expr = _vec_output_expr_ctxs[i]->root();
            try {
                root_schema->addStructField(_columns[i].name, _build_orc_type(column_expr->type()));
            } catch (doris::Exception& e) {
                return e.to_status();
            }
        }

        _file_format_transformer.reset(
                new VOrcTransformer(state, _file_writer.get(), _vec_output_expr_ctxs,
                                    std::move(root_schema), false, orc_compression_type));
        return _file_format_transformer->open();
    }
    default: {
        return Status::InternalError("Unsupported file format type {}",
                                     to_string(_file_format_type));
    }
    }
}

Status VHivePartitionWriter::close(const Status& status) {
    if (_file_format_transformer != nullptr) {
        Status st = _file_format_transformer->close();
        if (!st.ok()) {
            LOG(WARNING) << fmt::format("_file_format_transformer close failed, reason: {}",
                                        st.to_string());
        }
    }
    if (!status.ok() && _fs != nullptr) {
        auto path = fmt::format("{}/{}", _write_info.write_path, _file_name);
        Status st = _fs->delete_file(path);
        if (!st.ok()) {
            LOG(WARNING) << fmt::format("Delete file {} failed, reason: {}", path, st.to_string());
        }
    }
    _state->hive_partition_updates().emplace_back(_build_partition_update());
    return Status::OK();
}

Status VHivePartitionWriter::write(vectorized::Block& block, vectorized::IColumn::Filter* filter) {
    Block output_block;
    RETURN_IF_ERROR(_projection_and_filter_block(block, filter, &output_block));
    RETURN_IF_ERROR(_file_format_transformer->write(output_block));
    _row_count += output_block.rows();
    _input_size_in_bytes += output_block.bytes();
    return Status::OK();
}

std::unique_ptr<orc::Type> VHivePartitionWriter::_build_orc_type(
        const TypeDescriptor& type_descriptor) {
    std::pair<Status, std::unique_ptr<orc::Type>> result;
    switch (type_descriptor.type) {
    case TYPE_BOOLEAN: {
        return orc::createPrimitiveType(orc::BOOLEAN);
    }
    case TYPE_TINYINT: {
        return orc::createPrimitiveType(orc::BYTE);
    }
    case TYPE_SMALLINT: {
        return orc::createPrimitiveType(orc::SHORT);
    }
    case TYPE_INT: {
        return orc::createPrimitiveType(orc::INT);
    }
    case TYPE_BIGINT: {
        return orc::createPrimitiveType(orc::LONG);
    }
    case TYPE_FLOAT: {
        return orc::createPrimitiveType(orc::FLOAT);
    }
    case TYPE_DOUBLE: {
        return orc::createPrimitiveType(orc::DOUBLE);
    }
    case TYPE_CHAR: {
        return orc::createCharType(orc::CHAR, type_descriptor.len);
    }
    case TYPE_VARCHAR: {
        return orc::createCharType(orc::VARCHAR, type_descriptor.len);
    }
    case TYPE_STRING: {
        return orc::createPrimitiveType(orc::STRING);
    }
    case TYPE_BINARY: {
        return orc::createPrimitiveType(orc::STRING);
    }
    case TYPE_DATEV2: {
        return orc::createPrimitiveType(orc::DATE);
    }
    case TYPE_DATETIMEV2: {
        return orc::createPrimitiveType(orc::TIMESTAMP);
    }
    case TYPE_DECIMAL32: {
        return orc::createDecimalType(type_descriptor.precision, type_descriptor.scale);
    }
    case TYPE_DECIMAL64: {
        return orc::createDecimalType(type_descriptor.precision, type_descriptor.scale);
    }
    case TYPE_DECIMAL128I: {
        return orc::createDecimalType(type_descriptor.precision, type_descriptor.scale);
    }
    case TYPE_STRUCT: {
        std::unique_ptr<orc::Type> struct_type = orc::createStructType();
        for (int j = 0; j < type_descriptor.children.size(); ++j) {
            struct_type->addStructField(type_descriptor.field_names[j],
                                        _build_orc_type(type_descriptor.children[j]));
        }
        return struct_type;
    }
    case TYPE_ARRAY: {
        return orc::createListType(_build_orc_type(type_descriptor.children[0]));
    }
    case TYPE_MAP: {
        return orc::createMapType(_build_orc_type(type_descriptor.children[0]),
                                  _build_orc_type(type_descriptor.children[1]));
    }
    default: {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Unsupported type {} to build orc type",
                               type_descriptor.debug_string());
    }
    }
}

Status VHivePartitionWriter::_projection_and_filter_block(doris::vectorized::Block& input_block,
                                                          const vectorized::IColumn::Filter* filter,
                                                          doris::vectorized::Block* output_block) {
    Status status = Status::OK();
    if (input_block.rows() == 0) {
        return status;
    }
    RETURN_IF_ERROR(vectorized::VExprContext::get_output_block_after_execute_exprs(
            _vec_output_expr_ctxs, input_block, output_block));
    materialize_block_inplace(*output_block);

    if (filter == nullptr) {
        return status;
    }

    std::vector<uint32_t> columns_to_filter;
    int column_to_keep = input_block.columns();
    columns_to_filter.resize(column_to_keep);
    for (uint32_t i = 0; i < column_to_keep; ++i) {
        columns_to_filter[i] = i;
    }

    Block::filter_block_internal(output_block, columns_to_filter, *filter);

    return status;
}

THivePartitionUpdate VHivePartitionWriter::_build_partition_update() {
    THivePartitionUpdate hive_partition_update;
    hive_partition_update.__set_name(_partition_name);
    hive_partition_update.__set_update_mode(_update_mode);
    THiveLocationParams location;
    location.__set_write_path(_write_info.write_path);
    location.__set_target_path(_write_info.target_path);
    hive_partition_update.__set_location(location);
    hive_partition_update.__set_file_names({_file_name});
    hive_partition_update.__set_row_count(_row_count);
    hive_partition_update.__set_file_size(_input_size_in_bytes);
    return hive_partition_update;
}

} // namespace vectorized
} // namespace doris