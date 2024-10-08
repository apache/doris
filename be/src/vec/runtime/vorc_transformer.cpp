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

#include "vec/runtime/vorc_transformer.h"

#include <glog/logging.h>
#include <stdlib.h>
#include <string.h>

#include <exception>
#include <ostream>

#include "common/status.h"
#include "io/fs/file_writer.h"
#include "orc/Int128.hh"
#include "orc/MemoryPool.hh"
#include "orc/OrcFile.hh"
#include "orc/Vector.hh"
#include "runtime/define_primitive_type.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "util/binary_cast.hpp"
#include "util/debug_util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array.h"
#include "vec/common/string_ref.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {
VOrcOutputStream::VOrcOutputStream(doris::io::FileWriter* file_writer)
        : _file_writer(file_writer), _cur_pos(0), _written_len(0), _name("VOrcOutputStream") {}

VOrcOutputStream::~VOrcOutputStream() {
    if (!_is_closed) {
        try {
            close();
        } catch (...) {
            /*
         * Under normal circumstances, close() will be called first, and then the destructor will be called.
         * If the task is canceled, close() will not be executed, but the destructor will be called directly,
         * which will cause the be core.When the task is canceled, since the log file has been written during
         * close(), no operation is performed here.
         */
        }
    }
}

void VOrcOutputStream::close() {
    if (!_is_closed) {
        Defer defer {[this] { _is_closed = true; }};
        Status st = _file_writer->close();
        if (!st.ok()) {
            LOG(WARNING) << "close orc output stream failed: " << st;
            throw std::runtime_error(st.to_string());
        }
    }
}

void VOrcOutputStream::write(const void* data, size_t length) {
    if (!_is_closed) {
        Status st = _file_writer->append({static_cast<const uint8_t*>(data), length});
        if (!st.ok()) {
            LOG(WARNING) << "Write to ORC file failed: " << st;
            // When a write error occurs,
            // the error needs to be thrown to the upper layer.
            // so that fe can get the exception.
            throw std::runtime_error(st.to_string());
        }
        _cur_pos += length;
        _written_len += length;
    }
}

void VOrcOutputStream::set_written_len(int64_t written_len) {
    _written_len = written_len;
}

VOrcTransformer::VOrcTransformer(RuntimeState* state, doris::io::FileWriter* file_writer,
                                 const VExprContextSPtrs& output_vexpr_ctxs, std::string schema,
                                 std::vector<std::string> column_names, bool output_object_data,
                                 TFileCompressType::type compress_type,
                                 const iceberg::Schema* iceberg_schema)
        : VFileFormatTransformer(state, output_vexpr_ctxs, output_object_data),
          _file_writer(file_writer),
          _column_names(std::move(column_names)),
          _write_options(new orc::WriterOptions()),
          _schema_str(std::move(schema)),
          _iceberg_schema(iceberg_schema) {
    _write_options->setTimezoneName(_state->timezone());
    _write_options->setUseTightNumericVector(true);
    set_compression_type(compress_type);
}

Status VOrcTransformer::open() {
    if (!_schema_str.empty()) {
        try {
            _schema = orc::Type::buildTypeFromString(_schema_str);
        } catch (const std::exception& e) {
            return Status::InternalError("Orc build schema from \"{}\" failed: {}", _schema_str,
                                         e.what());
        }
    } else {
        _schema = orc::createStructType();
        const std::vector<iceberg::NestedField>* nested_fields = nullptr;
        if (_iceberg_schema != nullptr) {
            const iceberg::StructType& iceberg_root_struct_type = _iceberg_schema->root_struct();
            nested_fields = &iceberg_root_struct_type.fields();
        }
        for (int i = 0; i < _output_vexpr_ctxs.size(); i++) {
            VExprSPtr column_expr = _output_vexpr_ctxs[i]->root();
            try {
                std::unique_ptr<orc::Type> orc_type = _build_orc_type(
                        column_expr->type(), nested_fields ? &(*nested_fields)[i] : nullptr);
                _schema->addStructField(_column_names[i], std::move(orc_type));
            } catch (doris::Exception& e) {
                return e.to_status();
            }
        }
    }

    _output_stream = std::make_unique<VOrcOutputStream>(_file_writer);
    try {
        _write_options->setMemoryPool(ExecEnv::GetInstance()->orc_memory_pool());
        _writer = orc::createWriter(*_schema, _output_stream.get(), *_write_options);
    } catch (const std::exception& e) {
        return Status::InternalError("failed to create writer: {}", e.what());
    }
    _writer->addUserMetadata("CreatedBy", doris::get_short_version());
    return Status::OK();
}

void VOrcTransformer::set_compression_type(const TFileCompressType::type& compress_type) {
    switch (compress_type) {
    case TFileCompressType::PLAIN: {
        _write_options->setCompression(orc::CompressionKind::CompressionKind_NONE);
        break;
    }
    case TFileCompressType::SNAPPYBLOCK: {
        _write_options->setCompression(orc::CompressionKind::CompressionKind_SNAPPY);
        break;
    }
    case TFileCompressType::ZLIB: {
        _write_options->setCompression(orc::CompressionKind::CompressionKind_ZLIB);
        break;
    }
    case TFileCompressType::ZSTD: {
        _write_options->setCompression(orc::CompressionKind::CompressionKind_ZSTD);
        break;
    }
    default: {
        _write_options->setCompression(orc::CompressionKind::CompressionKind_ZLIB);
    }
    }
}

std::unique_ptr<orc::Type> VOrcTransformer::_build_orc_type(
        const TypeDescriptor& type_descriptor, const iceberg::NestedField* nested_field) {
    std::unique_ptr<orc::Type> type;
    switch (type_descriptor.type) {
    case TYPE_BOOLEAN: {
        type = orc::createPrimitiveType(orc::BOOLEAN);
        break;
    }
    case TYPE_TINYINT: {
        type = orc::createPrimitiveType(orc::BYTE);
        break;
    }
    case TYPE_SMALLINT: {
        type = orc::createPrimitiveType(orc::SHORT);
        break;
    }
    case TYPE_IPV4:
    case TYPE_INT: {
        type = orc::createPrimitiveType(orc::INT);
        break;
    }
    case TYPE_BIGINT: {
        type = orc::createPrimitiveType(orc::LONG);
        type->setAttribute(ICEBERG_LONG_TYPE, "LONG");
        break;
    }
    case TYPE_FLOAT: {
        type = orc::createPrimitiveType(orc::FLOAT);
        break;
    }
    case TYPE_DOUBLE: {
        type = orc::createPrimitiveType(orc::DOUBLE);
        break;
    }
    case TYPE_CHAR: {
        type = orc::createCharType(orc::CHAR, type_descriptor.len);
        break;
    }
    case TYPE_VARCHAR: {
        type = orc::createCharType(orc::VARCHAR, type_descriptor.len);
        break;
    }
    case TYPE_STRING: {
        type = orc::createPrimitiveType(orc::STRING);
        break;
    }
    case TYPE_IPV6:
    case TYPE_BINARY: {
        type = orc::createPrimitiveType(orc::STRING);
        break;
    }
    case TYPE_DATEV2: {
        type = orc::createPrimitiveType(orc::DATE);
        break;
    }
    case TYPE_DATETIMEV2: {
        type = orc::createPrimitiveType(orc::TIMESTAMP);
        break;
    }
    case TYPE_DECIMAL32: {
        type = orc::createDecimalType(type_descriptor.precision, type_descriptor.scale);
        break;
    }
    case TYPE_DECIMAL64: {
        type = orc::createDecimalType(type_descriptor.precision, type_descriptor.scale);
        break;
    }
    case TYPE_DECIMAL128I: {
        type = orc::createDecimalType(type_descriptor.precision, type_descriptor.scale);
        break;
    }
    case TYPE_STRUCT: {
        type = orc::createStructType();
        for (int j = 0; j < type_descriptor.children.size(); ++j) {
            type->addStructField(
                    type_descriptor.field_names[j],
                    _build_orc_type(
                            type_descriptor.children[j],
                            nested_field
                                    ? &nested_field->field_type()->as_struct_type()->fields()[j]
                                    : nullptr));
        }
        break;
    }
    case TYPE_ARRAY: {
        type = orc::createListType(_build_orc_type(
                type_descriptor.children[0],
                nested_field ? &nested_field->field_type()->as_list_type()->element_field()
                             : nullptr));
        break;
    }
    case TYPE_MAP: {
        type = orc::createMapType(
                _build_orc_type(type_descriptor.children[0],
                                nested_field
                                        ? &nested_field->field_type()->as_map_type()->key_field()
                                        : nullptr),
                _build_orc_type(type_descriptor.children[1],
                                nested_field
                                        ? &nested_field->field_type()->as_map_type()->value_field()
                                        : nullptr));
        break;
    }
    default: {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Unsupported type {} to build orc type",
                               type_descriptor.debug_string());
    }
    }
    if (nested_field != nullptr) {
        type->setAttribute(ORC_ICEBERG_ID_KEY, std::to_string(nested_field->field_id()));
        type->setAttribute(ORC_ICEBERG_REQUIRED_KEY, std::to_string(nested_field->is_required()));
    }
    return type;
}

std::unique_ptr<orc::ColumnVectorBatch> VOrcTransformer::_create_row_batch(size_t sz) {
    return _writer->createRowBatch(sz);
}

int64_t VOrcTransformer::written_len() {
    // written_len() will be called in VFileResultWriter::_close_file_writer
    // but _output_stream may be nullptr
    // because the failure built by _schema in open()
    if (_output_stream) {
        return _output_stream->getLength();
    }
    return 0;
}

Status VOrcTransformer::close() {
    try {
        if (_writer != nullptr) {
            _writer->close();
        }
        if (_output_stream) {
            _output_stream->close();
        }
    } catch (const std::exception& e) {
        return Status::IOError(e.what());
    }
    return Status::OK();
}

Status VOrcTransformer::write(const Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    // Buffer used by date/datetime/datev2/datetimev2/largeint type
    std::vector<StringRef> buffer_list;
    Defer defer {[&]() {
        for (auto& bufferRef : buffer_list) {
            if (bufferRef.data) {
                free(const_cast<char*>(bufferRef.data));
            }
        }
    }};

    size_t sz = block.rows();
    auto row_batch = _create_row_batch(sz);
    auto* root = dynamic_cast<orc::StructVectorBatch*>(row_batch.get());
    try {
        for (size_t i = 0; i < block.columns(); i++) {
            const auto& col = block.get_by_position(i);
            const auto& raw_column = col.column;
            RETURN_IF_ERROR(_resize_row_batch(col.type, *raw_column, root->fields[i]));
            RETURN_IF_ERROR(_serdes[i]->write_column_to_orc(
                    _state->timezone(), *raw_column, nullptr, root->fields[i], 0, sz, buffer_list));
        }
        root->numElements = sz;
        _writer->add(*row_batch);
        _cur_written_rows += sz;
    } catch (const std::exception& e) {
        LOG(WARNING) << "Orc write error: " << e.what();
        return Status::InternalError(e.what());
    }

    return Status::OK();
}

Status VOrcTransformer::_resize_row_batch(const DataTypePtr& type, const IColumn& column,
                                          orc::ColumnVectorBatch* orc_col_batch) {
    auto real_type = remove_nullable(type);
    WhichDataType which(real_type);

    if (which.is_struct()) {
        auto* struct_batch = dynamic_cast<orc::StructVectorBatch*>(orc_col_batch);
        const auto& struct_col =
                column.is_nullable()
                        ? assert_cast<const ColumnStruct&>(
                                  assert_cast<const ColumnNullable&>(column).get_nested_column())
                        : assert_cast<const ColumnStruct&>(column);
        int idx = 0;
        for (auto* child : struct_batch->fields) {
            const IColumn& child_column = struct_col.get_column(idx);
            child->resize(child_column.size());
            auto child_type = assert_cast<const vectorized::DataTypeStruct*>(real_type.get())
                                      ->get_element(idx);
            ++idx;
            RETURN_IF_ERROR(_resize_row_batch(child_type, child_column, child));
        }
    } else if (which.is_map()) {
        auto* map_batch = dynamic_cast<orc::MapVectorBatch*>(orc_col_batch);
        const auto& map_column =
                column.is_nullable()
                        ? assert_cast<const ColumnMap&>(
                                  assert_cast<const ColumnNullable&>(column).get_nested_column())
                        : assert_cast<const ColumnMap&>(column);

        // key of map
        const IColumn& nested_keys_column = map_column.get_keys();
        map_batch->keys->resize(nested_keys_column.size());
        auto key_type =
                assert_cast<const vectorized::DataTypeMap*>(real_type.get())->get_key_type();
        RETURN_IF_ERROR(_resize_row_batch(key_type, nested_keys_column, map_batch->keys.get()));

        // value of map
        const IColumn& nested_values_column = map_column.get_values();
        map_batch->elements->resize(nested_values_column.size());
        auto value_type =
                assert_cast<const vectorized::DataTypeMap*>(real_type.get())->get_value_type();
        RETURN_IF_ERROR(
                _resize_row_batch(value_type, nested_values_column, map_batch->elements.get()));
    } else if (which.is_array()) {
        auto* list_batch = dynamic_cast<orc::ListVectorBatch*>(orc_col_batch);
        const auto& array_col =
                column.is_nullable()
                        ? assert_cast<const ColumnArray&>(
                                  assert_cast<const ColumnNullable&>(column).get_nested_column())
                        : assert_cast<const ColumnArray&>(column);
        const IColumn& nested_column = array_col.get_data();
        list_batch->elements->resize(nested_column.size());
        auto child_type =
                assert_cast<const vectorized::DataTypeArray*>(real_type.get())->get_nested_type();
        RETURN_IF_ERROR(_resize_row_batch(child_type, nested_column, list_batch->elements.get()));
    }
    return Status::OK();
}

} // namespace doris::vectorized
