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

#ifdef BUILD_RUST_READERS

#include "format/lance/lance_rust_reader.h"

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/type.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/primitive_type.h"
#include "format/lance/lance_ffi.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/timezone_utils.h"

namespace doris {
#include "common/compile_check_avoid_begin.h"

const std::vector<SlotDescriptor*> LanceRustReader::_empty_slot_descs;

LanceRustReader::LanceRustReader(const std::vector<SlotDescriptor*>& file_slot_descs,
                                 RuntimeState* state, RuntimeProfile* /*profile*/,
                                 const TFileRangeDesc& range,
                                 const TFileScanRangeParams* range_params)
        : _file_slot_descs(file_slot_descs), _state(state), _range(range), _params(range_params) {
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, _ctzz);
}

LanceRustReader::LanceRustReader(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                                 io::IOContext* /*io_ctx*/)
        : _file_slot_descs(_empty_slot_descs),
          _state(nullptr),
          _range(range),
          _params(&params),
          _schema_only(true) {
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, _ctzz);
}

LanceRustReader::~LanceRustReader() {
    static_cast<void>(close());
}

Status LanceRustReader::init_reader() {
    return _open_with_json(false);
}

Status LanceRustReader::init_schema_reader() {
    return _open_with_json(true);
}

Status LanceRustReader::_open_with_json(bool schema_only) {
    std::string uri = _range.path;
    if (uri.empty()) {
        return Status::InvalidArgument("Lance reader: dataset URI is empty");
    }

    // Lance datasets are directories (e.g., data.lance/).
    // The TVF path may point to a file inside (e.g., data.lance/data/xxx.lance).
    // Strip back to the .lance dataset root and extract the fragment file name
    // so we only read the specific fragment assigned to this scan range.
    std::string fragment_file;
    auto lance_pos = uri.find(".lance");
    if (lance_pos != std::string::npos) {
        auto end = lance_pos + 6; // ".lance" is 6 chars
        if (end < uri.size() && uri[end] == '/') {
            // Extract the relative path after the dataset root (e.g., "data/xxx.lance")
            fragment_file = uri.substr(end + 1);
            uri = uri.substr(0, end);
        }
    }

    // Build JSON config for the Rust FFI
    rapidjson::Document doc(rapidjson::kObjectType);
    auto& alloc = doc.GetAllocator();

    doc.AddMember("uri", rapidjson::Value(uri.c_str(), alloc), alloc);

    // Pass fragment file name so Rust reads only this fragment, not all
    if (!fragment_file.empty() && !schema_only) {
        doc.AddMember("fragment_file", rapidjson::Value(fragment_file.c_str(), alloc), alloc);
    }

    // Columns (only for data reads, not schema-only)
    rapidjson::Value cols(rapidjson::kArrayType);
    if (!schema_only) {
        for (const auto* slot : _file_slot_descs) {
            cols.PushBack(rapidjson::Value(slot->col_name().c_str(), alloc), alloc);
        }
    }
    doc.AddMember("columns", cols, alloc);

    // Batch size
    size_t batch_size = schema_only ? 1 : 4096;
    if (!schema_only && _state) {
        size_t bs = static_cast<size_t>(_state->query_options().batch_size);
        if (bs > 0) batch_size = bs;
    }
    doc.AddMember("batch_size", static_cast<uint64_t>(batch_size), alloc);

    // Version (time travel) from TLanceFileDesc
    uint64_t version = 0;
    if (_range.__isset.table_format_params && _range.table_format_params.__isset.lance_params &&
        _range.table_format_params.lance_params.__isset.version) {
        version = static_cast<uint64_t>(_range.table_format_params.lance_params.version);
    }
    doc.AddMember("version", version, alloc);

    // Storage options (S3 credentials from scan range params properties)
    rapidjson::Value storage_opts(rapidjson::kObjectType);
    if (_range.__isset.table_format_params && _range.table_format_params.__isset.lance_params &&
        _range.table_format_params.lance_params.__isset.dataset_uri) {
        // If a specific dataset_uri is set in lance_params, use it instead
        const auto& lance_uri = _range.table_format_params.lance_params.dataset_uri;
        if (!lance_uri.empty()) {
            doc.RemoveMember("uri");
            doc.AddMember("uri", rapidjson::Value(lance_uri.c_str(), alloc), alloc);
        }
    }
    // Map Doris S3 property keys to lance/object_store standard keys
    static const std::vector<std::pair<std::string, std::string>> s3_key_mapping = {
            {"AWS_ACCESS_KEY", "aws_access_key_id"},
            {"AWS_SECRET_KEY", "aws_secret_access_key"},
            {"AWS_TOKEN", "aws_session_token"},
            {"AWS_ENDPOINT", "aws_endpoint"},
            {"AWS_REGION", "aws_region"},
    };
    // Read S3 properties from TFileScanRangeParams.properties
    if (_params && _params->__isset.properties) {
        for (const auto& [k, v] : _params->properties) {
            // Map Doris key names to object_store key names
            bool mapped = false;
            for (const auto& [doris_key, lance_key] : s3_key_mapping) {
                if (k == doris_key) {
                    storage_opts.AddMember(rapidjson::Value(lance_key.c_str(), alloc),
                                           rapidjson::Value(v.c_str(), alloc), alloc);
                    mapped = true;
                    break;
                }
            }
            // Pass through any unrecognized keys as-is
            if (!mapped && !v.empty()) {
                storage_opts.AddMember(rapidjson::Value(k.c_str(), alloc),
                                       rapidjson::Value(v.c_str(), alloc), alloc);
            }
        }
    }
    doc.AddMember("storage_options", storage_opts, alloc);

    // Serialize to JSON string
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    std::string config_json = buffer.GetString();

    LanceReaderHandle handle = nullptr;
    int32_t rc = lance_reader_open_json(reinterpret_cast<const uint8_t*>(config_json.data()),
                                        config_json.size(), &handle);
    if (rc != lance_ffi::LANCE_FFI_OK) {
        return _get_ffi_error(rc);
    }

    _reader_handle = handle;
    return Status::OK();
}

Status LanceRustReader::_do_get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (!_reader_handle) {
        return Status::InternalError("Lance reader is not initialized");
    }

    if (_col_name_to_block_idx.empty()) {
        _col_name_to_block_idx = block->get_name_to_pos_map();
    }

    ArrowSchema c_schema {};
    ArrowArray c_array {};
    bool is_eof = false;
    int64_t batch_bytes = 0;

    int32_t rc =
            lance_reader_next_batch(_reader_handle, &c_schema, &c_array, &is_eof, &batch_bytes);

    if (rc == lance_ffi::LANCE_FFI_EOF || is_eof) {
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }
    if (rc != lance_ffi::LANCE_FFI_OK) {
        return _get_ffi_error(rc);
    }

    // Import Arrow C Data Interface into C++ RecordBatch
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> import_result =
            arrow::ImportRecordBatch(&c_array, &c_schema);
    if (!import_result.ok()) {
        return Status::InternalError("Failed to import Lance arrow batch: {}",
                                     import_result.status().message());
    }

    auto record_batch = std::move(import_result).ValueUnsafe();
    const auto num_rows = static_cast<size_t>(record_batch->num_rows());
    const auto num_columns = record_batch->num_columns();

    // Convert Arrow columns to Doris Block columns (same pattern as PaimonCppReader)
    for (int c = 0; c < num_columns; ++c) {
        const auto& field = record_batch->schema()->field(c);

        auto it = _col_name_to_block_idx.find(field->name());
        if (it == _col_name_to_block_idx.end()) {
            continue;
        }

        const ColumnWithTypeAndName& column_with_name = block->get_by_position(it->second);
        try {
            RETURN_IF_ERROR(column_with_name.type->get_serde()->read_column_from_arrow(
                    column_with_name.column->assume_mutable_ref(), record_batch->column(c).get(), 0,
                    num_rows, _ctzz));
        } catch (Exception& e) {
            return Status::InternalError("Failed to convert Lance arrow to block: {}", e.what());
        }
    }

    *read_rows = num_rows;
    *eof = false;
    return Status::OK();
}

Status LanceRustReader::_get_columns_impl(
        std::unordered_map<std::string, DataTypePtr>* name_to_type) {
    if (_schema_only && _reader_handle) {
        // In schema-only mode, get columns from the Lance schema
        ArrowSchema c_schema {};
        int32_t rc = lance_reader_get_schema(_reader_handle, &c_schema);
        if (rc != lance_ffi::LANCE_FFI_OK) {
            return _get_ffi_error(rc);
        }
        auto import_result = arrow::ImportSchema(&c_schema);
        if (!import_result.ok()) {
            return Status::InternalError("Failed to import Lance schema: {}",
                                         import_result.status().message());
        }
        auto schema = std::move(import_result).ValueUnsafe();
        for (const auto& field : schema->fields()) {
            auto doris_type = _arrow_type_to_doris_type(field->type());
            if (doris_type) {
                name_to_type->emplace(field->name(), make_nullable(doris_type));
            }
        }
        return Status::OK();
    }

    for (const auto* slot : _file_slot_descs) {
        name_to_type->emplace(slot->col_name(), slot->type());
    }
    return Status::OK();
}

Status LanceRustReader::get_parsed_schema(std::vector<std::string>* col_names,
                                          std::vector<DataTypePtr>* col_types) {
    if (!_reader_handle) {
        return Status::InternalError("Lance reader is not initialized");
    }

    ArrowSchema c_schema {};
    int32_t rc = lance_reader_get_schema(_reader_handle, &c_schema);
    if (rc != lance_ffi::LANCE_FFI_OK) {
        return _get_ffi_error(rc);
    }

    auto import_result = arrow::ImportSchema(&c_schema);
    if (!import_result.ok()) {
        return Status::InternalError("Failed to import Lance schema: {}",
                                     import_result.status().message());
    }

    auto schema = std::move(import_result).ValueUnsafe();
    for (const auto& field : schema->fields()) {
        auto doris_type = _arrow_type_to_doris_type(field->type());
        if (doris_type) {
            col_names->push_back(field->name());
            col_types->push_back(make_nullable(doris_type));
        }
    }
    return Status::OK();
}

Status LanceRustReader::close() {
    if (_reader_handle) {
        lance_reader_close(_reader_handle);
        _reader_handle = nullptr;
    }
    return Status::OK();
}

Status LanceRustReader::_get_ffi_error(int32_t status_code) const {
    constexpr size_t kBufSize = 1024;
    uint8_t buf[kBufSize];
    size_t len = lance_reader_last_error(buf, kBufSize);

    std::string msg;
    if (len > 0) {
        msg.assign(reinterpret_cast<const char*>(buf), len);
    } else {
        msg = fmt::format("Lance FFI error code: {}", status_code);
    }
    return Status::InternalError("Rust Lance reader: {}", msg);
}

DataTypePtr LanceRustReader::_arrow_type_to_doris_type(
        const std::shared_ptr<arrow::DataType>& arrow_type) {
    // Arrow type IDs: STRING and UTF8 are the same value in arrow-cpp.
    // LARGE_STRING and LARGE_UTF8 are the same. Doris has no unsigned int types
    // except UInt8 (used for BOOLEAN), so unsigned ints are widened to signed.
    switch (arrow_type->id()) {
    case arrow::Type::BOOL:
        return std::make_shared<DataTypeUInt8>();
    case arrow::Type::INT8:
    case arrow::Type::UINT8:
        return std::make_shared<DataTypeInt8>();
    case arrow::Type::INT16:
    case arrow::Type::UINT16:
        return std::make_shared<DataTypeInt16>();
    case arrow::Type::INT32:
    case arrow::Type::UINT32:
        return std::make_shared<DataTypeInt32>();
    case arrow::Type::INT64:
    case arrow::Type::UINT64:
        return std::make_shared<DataTypeInt64>();
    case arrow::Type::HALF_FLOAT:
    case arrow::Type::FLOAT:
        return std::make_shared<DataTypeFloat32>();
    case arrow::Type::DOUBLE:
        return std::make_shared<DataTypeFloat64>();
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:
        return std::make_shared<DataTypeString>();
    case arrow::Type::BINARY:
    case arrow::Type::LARGE_BINARY:
        return std::make_shared<DataTypeString>();
    case arrow::Type::DATE32:
    case arrow::Type::DATE64:
        return std::make_shared<DataTypeDateV2>();
    case arrow::Type::TIMESTAMP:
        return std::make_shared<DataTypeDateTimeV2>();
    case arrow::Type::DECIMAL128: {
        auto decimal_type = std::static_pointer_cast<arrow::Decimal128Type>(arrow_type);
        return create_decimal(decimal_type->precision(), decimal_type->scale(), false);
    }
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST: {
        auto list_type = std::static_pointer_cast<arrow::BaseListType>(arrow_type);
        auto inner = _arrow_type_to_doris_type(list_type->value_type());
        if (inner) {
            return std::make_shared<DataTypeArray>(make_nullable(inner));
        }
        return nullptr;
    }
    case arrow::Type::FIXED_SIZE_LIST: {
        auto fsl_type = std::static_pointer_cast<arrow::FixedSizeListType>(arrow_type);
        auto inner = _arrow_type_to_doris_type(fsl_type->value_type());
        if (inner) {
            return std::make_shared<DataTypeArray>(make_nullable(inner));
        }
        return nullptr;
    }
    default:
        // Unsupported types fall back to string
        return std::make_shared<DataTypeString>();
    }
}

#include "common/compile_check_avoid_end.h"
} // namespace doris

#endif // BUILD_RUST_READERS
