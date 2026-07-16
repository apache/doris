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

#include "format/lance/lance_reader.h"

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <lance/lance.h>

#include <algorithm>
#include <array>
#include <memory>
#include <utility>

#include "common/exception.h"
#include "core/block/block.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_struct.h"
#include "fmt/format.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/timezone_utils.h"

namespace doris {
#include "common/compile_check_begin.h"

namespace {

struct LanceScannerDeleter {
    void operator()(LanceScanner* scanner) const { lance_scanner_close(scanner); }
};

struct LanceBatchDeleter {
    void operator()(LanceBatch* batch) const { lance_batch_free(batch); }
};

int arrow_time_precision(arrow::TimeUnit::type unit) {
    switch (unit) {
    case arrow::TimeUnit::SECOND:
        return 0;
    case arrow::TimeUnit::MILLI:
        return 3;
    case arrow::TimeUnit::MICRO:
    case arrow::TimeUnit::NANO:
        return 6;
    }
    return 6;
}

Status arrow_type_to_doris_type(const std::shared_ptr<arrow::DataType>& arrow_type,
                                DataTypePtr* doris_type) {
    const auto nullable_primitive = [&](PrimitiveType type, int precision = 0, int scale = 0) {
        *doris_type = DataTypeFactory::instance().create_data_type(type, true, precision, scale);
        return Status::OK();
    };

    switch (arrow_type->id()) {
    case arrow::Type::BOOL:
        return nullable_primitive(TYPE_BOOLEAN);
    case arrow::Type::INT8:
    case arrow::Type::UINT8:
        return nullable_primitive(TYPE_TINYINT);
    case arrow::Type::INT16:
    case arrow::Type::UINT16:
        return nullable_primitive(TYPE_SMALLINT);
    case arrow::Type::INT32:
    case arrow::Type::UINT32:
        return nullable_primitive(TYPE_INT);
    case arrow::Type::INT64:
    case arrow::Type::UINT64:
        return nullable_primitive(TYPE_BIGINT);
    case arrow::Type::FLOAT:
        return nullable_primitive(TYPE_FLOAT);
    case arrow::Type::DOUBLE:
        return nullable_primitive(TYPE_DOUBLE);
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:
        return nullable_primitive(TYPE_STRING);
    case arrow::Type::BINARY:
    case arrow::Type::LARGE_BINARY:
    case arrow::Type::FIXED_SIZE_BINARY:
        return nullable_primitive(TYPE_VARBINARY);
    case arrow::Type::DATE32:
        return nullable_primitive(TYPE_DATEV2);
    case arrow::Type::TIMESTAMP: {
        const auto timestamp = std::static_pointer_cast<arrow::TimestampType>(arrow_type);
        return nullable_primitive(TYPE_DATETIMEV2, 0, arrow_time_precision(timestamp->unit()));
    }
    case arrow::Type::DECIMAL128:
    case arrow::Type::DECIMAL256: {
        const auto decimal = std::static_pointer_cast<arrow::DecimalType>(arrow_type);
        const int precision = decimal->precision();
        const PrimitiveType doris_decimal_type = precision <= 9    ? TYPE_DECIMAL32
                                                 : precision <= 18 ? TYPE_DECIMAL64
                                                 : precision <= 38 ? TYPE_DECIMAL128I
                                                                   : TYPE_DECIMAL256;
        return nullable_primitive(doris_decimal_type, precision, decimal->scale());
    }
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST:
    case arrow::Type::FIXED_SIZE_LIST: {
        const auto list = std::static_pointer_cast<arrow::BaseListType>(arrow_type);
        DataTypePtr value_type;
        RETURN_IF_ERROR(arrow_type_to_doris_type(list->value_type(), &value_type));
        *doris_type = make_nullable(std::make_shared<DataTypeArray>(value_type));
        return Status::OK();
    }
    case arrow::Type::MAP: {
        const auto map = std::static_pointer_cast<arrow::MapType>(arrow_type);
        DataTypePtr key_type;
        DataTypePtr item_type;
        RETURN_IF_ERROR(arrow_type_to_doris_type(map->key_type(), &key_type));
        RETURN_IF_ERROR(arrow_type_to_doris_type(map->item_type(), &item_type));
        *doris_type = make_nullable(std::make_shared<DataTypeMap>(key_type, item_type));
        return Status::OK();
    }
    case arrow::Type::STRUCT: {
        const auto struct_type = std::static_pointer_cast<arrow::StructType>(arrow_type);
        DataTypes field_types;
        Strings field_names;
        field_types.reserve(struct_type->num_fields());
        field_names.reserve(struct_type->num_fields());
        for (const auto& field : struct_type->fields()) {
            DataTypePtr field_type;
            RETURN_IF_ERROR(arrow_type_to_doris_type(field->type(), &field_type));
            field_types.emplace_back(std::move(field_type));
            field_names.emplace_back(field->name());
        }
        *doris_type = make_nullable(std::make_shared<DataTypeStruct>(field_types, field_names));
        return Status::OK();
    }
    default:
        return Status::NotSupported("unsupported Lance Arrow type: {}", arrow_type->ToString());
    }
}

} // namespace

LanceReader::LanceReader(const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
                         RuntimeProfile* profile, const TFileRangeDesc& range,
                         const TFileScanRangeParams* scan_params)
        : _file_slot_descs(file_slot_descs),
          _state(state),
          _profile(profile),
          _range(range),
          _scan_params(scan_params) {
    if (_state != nullptr) {
        _ctzz = _state->timezone_obj();
    } else {
        TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, _ctzz);
    }
}

LanceReader::~LanceReader() {
    static_cast<void>(close());
}

Status LanceReader::init_reader() {
    RETURN_IF_ERROR(_validate_range());

    _projected_columns.clear();
    _projected_columns.reserve(_file_slot_descs.size());
    std::unordered_set<std::string> unique_columns;
    for (const auto* slot : _file_slot_descs) {
        if (!unique_columns.emplace(slot->col_name()).second) {
            return Status::InvalidArgument("duplicate Lance projected column: {}",
                                           slot->col_name());
        }
        _projected_columns.emplace_back(slot->col_name());
    }

    RETURN_IF_ERROR(_open_dataset());
    return _open_scanner();
}

Status LanceReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (_scanner == nullptr) {
        return Status::InternalError("Lance reader is not initialized");
    }
    *read_rows = 0;
    *eof = false;
    if (_eof) {
        *eof = true;
        return Status::OK();
    }

    const auto target_rows = _state->batch_size();
    while (*read_rows < target_rows) {
        LanceBatch* raw_batch = nullptr;
        const int32_t status = lance_scanner_next(_scanner, &raw_batch);
        if (status == 1) {
            _eof = true;
            // Preserve GenericReader's convention: return a non-empty final block first, then
            // report EOF on the next call.
            if (*read_rows == 0) {
                *eof = true;
            }
            break;
        }
        if (status != 0 || raw_batch == nullptr) {
            return _lance_error("read next Lance batch");
        }

        std::unique_ptr<LanceBatch, LanceBatchDeleter> batch(raw_batch);
        size_t batch_rows = 0;
        RETURN_IF_ERROR(_fill_block_from_arrow(batch.get(), block, &batch_rows));
        *read_rows += batch_rows;
    }
    return Status::OK();
}

Status LanceReader::get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                                std::unordered_set<std::string>* missing_cols) {
    missing_cols->clear();
    for (const auto* slot : _file_slot_descs) {
        name_to_type->emplace(slot->col_name(), slot->type());
    }
    return Status::OK();
}

Status LanceReader::init_schema_reader() {
    RETURN_IF_ERROR(_validate_range(false));
    RETURN_IF_ERROR(_open_dataset());

    ArrowSchema schema {};
    if (lance_dataset_schema(_dataset, &schema) != 0) {
        return _lance_error("get Lance dataset schema");
    }
    auto result = arrow::ImportSchema(&schema);
    if (!result.ok()) {
        if (schema.release != nullptr) {
            schema.release(&schema);
        }
        return Status::InternalError("import Lance Arrow schema failed: {}",
                                     result.status().message());
    }
    _schema = std::move(result).ValueUnsafe();
    return Status::OK();
}

Status LanceReader::get_parsed_schema(std::vector<std::string>* col_names,
                                      std::vector<DataTypePtr>* col_types) {
    if (_schema == nullptr) {
        return Status::InternalError("Lance schema reader is not initialized");
    }
    std::unordered_set<std::string> column_names;
    for (const auto& field : _schema->fields()) {
        if (!column_names.emplace(field->name()).second) {
            return Status::InvalidArgument("duplicate Lance schema column: {}", field->name());
        }
        DataTypePtr doris_type;
        RETURN_IF_ERROR(arrow_type_to_doris_type(field->type(), &doris_type));
        col_names->emplace_back(field->name());
        col_types->emplace_back(std::move(doris_type));
    }
    return Status::OK();
}

Status LanceReader::close() {
    if (_scanner != nullptr) {
        lance_scanner_close(_scanner);
        _scanner = nullptr;
    }
    if (_dataset != nullptr) {
        lance_dataset_close(_dataset);
        _dataset = nullptr;
    }
    _schema.reset();
    return Status::OK();
}

Status LanceReader::_validate_range(bool require_fragment_ids) const {
    if (!_range.__isset.table_format_params || !_range.table_format_params.__isset.lance_params) {
        return Status::InvalidArgument("Lance reader requires lance_params in table format params");
    }
    const auto& params = _range.table_format_params.lance_params;
    if (!params.__isset.dataset_uri || params.dataset_uri.empty()) {
        return Status::InvalidArgument("Lance reader requires a non-empty dataset_uri");
    }
    // Version zero is Lance-C's latest-snapshot sentinel. FE must pin a positive version so
    // retries and different BE instances read the same snapshot.
    if (!params.__isset.version || params.version <= 0) {
        return Status::InvalidArgument("Lance reader requires a positive, fixed dataset version");
    }
    if (!params.__isset.fragment_ids || params.fragment_ids.empty()) {
        if (require_fragment_ids) {
            return Status::InvalidArgument("Lance reader requires non-empty fragment_ids");
        }
        return Status::OK();
    }

    std::unordered_set<int64_t> unique_ids;
    for (const int64_t id : params.fragment_ids) {
        if (id < 0) {
            return Status::InvalidArgument("Lance fragment id must be non-negative: {}", id);
        }
        if (!unique_ids.emplace(id).second) {
            return Status::InvalidArgument("Lance scan range contains duplicate fragment id: {}",
                                           id);
        }
    }
    return Status::OK();
}

Status LanceReader::_open_dataset() {
    const auto& lance_params = _range.table_format_params.lance_params;
    auto storage_option_values = _storage_options();
    std::vector<const char*> storage_option_ptrs;
    storage_option_ptrs.reserve(storage_option_values.size() + 1);
    for (const auto& option : storage_option_values) {
        storage_option_ptrs.emplace_back(option.c_str());
    }
    storage_option_ptrs.emplace_back(nullptr);

    LanceDataset* dataset =
            lance_dataset_open(lance_params.dataset_uri.c_str(),
                               storage_option_values.empty() ? nullptr : storage_option_ptrs.data(),
                               static_cast<uint64_t>(lance_params.version));
    if (dataset == nullptr) {
        return _lance_error("open Lance dataset");
    }
    _dataset = dataset;
    return Status::OK();
}

Status LanceReader::_open_scanner() {
    std::vector<const char*> column_ptrs;
    column_ptrs.reserve(_projected_columns.size() + 1);
    for (const auto& column : _projected_columns) {
        column_ptrs.emplace_back(column.c_str());
    }
    column_ptrs.emplace_back(nullptr);

    LanceScanner* scanner = lance_scanner_new(
            _dataset, _projected_columns.empty() ? nullptr : column_ptrs.data(), nullptr);
    if (scanner == nullptr) {
        return _lance_error("create Lance scanner");
    }
    std::unique_ptr<LanceScanner, LanceScannerDeleter> scanner_guard(scanner);

    const int64_t batch_size =
            _state == nullptr ? 4096 : static_cast<int64_t>(_state->batch_size());
    if (lance_scanner_set_batch_size(scanner, std::max<int64_t>(1, batch_size)) != 0) {
        return _lance_error("set Lance scanner batch size");
    }

    const auto& thrift_fragment_ids = _range.table_format_params.lance_params.fragment_ids;
    std::vector<uint64_t> fragment_ids;
    fragment_ids.reserve(thrift_fragment_ids.size());
    for (const int64_t id : thrift_fragment_ids) {
        fragment_ids.emplace_back(static_cast<uint64_t>(id));
    }
    if (lance_scanner_set_fragment_ids(scanner, fragment_ids.data(), fragment_ids.size()) != 0) {
        return _lance_error("set Lance scanner fragment ids");
    }

    _scanner = scanner_guard.release();
    return Status::OK();
}

Status LanceReader::_fill_block_from_arrow(LanceBatch* batch, Block* block, size_t* read_rows) {
    ArrowArray array {};
    ArrowSchema schema {};
    if (lance_batch_to_arrow(batch, &array, &schema) != 0) {
        return _lance_error("export Lance batch to Arrow");
    }

    auto result = arrow::ImportRecordBatch(&array, &schema);
    if (!result.ok()) {
        if (array.release != nullptr) {
            array.release(&array);
        }
        if (schema.release != nullptr) {
            schema.release(&schema);
        }
        return Status::InternalError("import Lance Arrow batch failed: {}",
                                     result.status().message());
    }

    auto record_batch = std::move(result).ValueUnsafe();
    if (_block_name_to_idx.empty()) {
        _block_name_to_idx = block->get_name_to_pos_map();
    }
    auto columns_guard = block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    const auto row_count = static_cast<size_t>(record_batch->num_rows());
    std::unordered_set<std::string> materialized_columns;
    materialized_columns.reserve(record_batch->num_columns());
    for (int arrow_idx = 0; arrow_idx < record_batch->num_columns(); ++arrow_idx) {
        const auto& field = record_batch->schema()->field(arrow_idx);
        const auto block_it = _block_name_to_idx.find(field->name());
        if (block_it == _block_name_to_idx.end()) {
            continue;
        }
        const size_t block_idx = block_it->second;
        try {
            RETURN_IF_ERROR(columns_guard.get_datatype_by_position(block_idx)
                                    ->get_serde()
                                    ->read_column_from_arrow(*columns[block_idx],
                                                             record_batch->column(arrow_idx).get(),
                                                             0, row_count, _ctzz));
        } catch (const Exception& e) {
            return Status::InternalError("convert Lance Arrow column '{}' failed: {}",
                                         field->name(), e.what());
        }
        materialized_columns.emplace(field->name());
    }
    for (const auto& projected_column : _projected_columns) {
        if (!materialized_columns.contains(projected_column)) {
            return Status::InternalError("Lance did not return requested column '{}'",
                                         projected_column);
        }
    }
    *read_rows = row_count;
    return Status::OK();
}

std::vector<std::string> LanceReader::_storage_options() const {
    if (_scan_params == nullptr || !_scan_params->__isset.properties) {
        return {};
    }

    static constexpr std::array<std::pair<std::string_view, std::string_view>, 5> kStorageKeys = {
            {{"AWS_ACCESS_KEY", "aws_access_key_id"},
             {"AWS_SECRET_KEY", "aws_secret_access_key"},
             {"AWS_TOKEN", "aws_session_token"},
             {"AWS_ENDPOINT", "aws_endpoint"},
             {"AWS_REGION", "aws_region"}}};
    std::vector<std::string> options;
    options.reserve(kStorageKeys.size() * 2);
    for (const auto& [doris_key, lance_key] : kStorageKeys) {
        const auto it = _scan_params->properties.find(std::string(doris_key));
        if (it != _scan_params->properties.end() && !it->second.empty()) {
            options.emplace_back(lance_key);
            options.emplace_back(it->second);
        }
    }
    return options;
}

Status LanceReader::_lance_error(std::string_view operation) {
    const char* raw_message = lance_last_error_message();
    std::string message = raw_message == nullptr ? "" : raw_message;
    if (raw_message != nullptr) {
        lance_free_string(raw_message);
    }
    if (message.empty()) {
        message = fmt::format("error_code={}", static_cast<int>(lance_last_error_code()));
    }
    return Status::InternalError("{} failed: {}", operation, message);
}

#include "common/compile_check_end.h"
} // namespace doris
