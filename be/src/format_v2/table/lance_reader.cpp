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

#include "format_v2/table/lance_reader.h"

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <lance/lance.h>

#include <limits>
#include <memory>

#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_struct.h"

namespace doris::format::lance {
namespace {

struct LanceDatasetDeleter {
    void operator()(LanceDataset* dataset) const { lance_dataset_close(dataset); }
};

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
    const auto nullable_primitive = [&](PrimitiveType type, int precision = 0, int scale = 0,
                                        int len = -1) {
        *doris_type =
                DataTypeFactory::instance().create_data_type(type, true, precision, scale, len);
        return Status::OK();
    };

    switch (arrow_type->id()) {
    case arrow::Type::BOOL:
        return nullable_primitive(TYPE_BOOLEAN);
    case arrow::Type::INT8:
        return nullable_primitive(TYPE_TINYINT);
    case arrow::Type::UINT8:
    case arrow::Type::INT16:
        return nullable_primitive(TYPE_SMALLINT);
    case arrow::Type::UINT16:
    case arrow::Type::INT32:
        return nullable_primitive(TYPE_INT);
    case arrow::Type::UINT32:
    case arrow::Type::INT64:
        return nullable_primitive(TYPE_BIGINT);
    case arrow::Type::UINT64:
        return nullable_primitive(TYPE_LARGEINT);
    case arrow::Type::HALF_FLOAT:
    case arrow::Type::FLOAT:
        return nullable_primitive(TYPE_FLOAT);
    case arrow::Type::DOUBLE:
        return nullable_primitive(TYPE_DOUBLE);
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:
        return nullable_primitive(TYPE_STRING);
    case arrow::Type::BINARY:
    case arrow::Type::LARGE_BINARY:
        return nullable_primitive(TYPE_VARBINARY, 0, 0,
                                  std::numeric_limits<int32_t>::max());
    case arrow::Type::FIXED_SIZE_BINARY: {
        const auto binary = std::static_pointer_cast<arrow::FixedSizeBinaryType>(arrow_type);
        return nullable_primitive(TYPE_VARBINARY, 0, 0, binary->byte_width());
    }
    case arrow::Type::DATE32:
    case arrow::Type::DATE64:
        return nullable_primitive(TYPE_DATEV2);
    case arrow::Type::TIME32:
    case arrow::Type::TIME64: {
        const auto time = std::static_pointer_cast<arrow::TimeType>(arrow_type);
        return nullable_primitive(TYPE_TIMEV2, 0, arrow_time_precision(time->unit()));
    }
    case arrow::Type::TIMESTAMP: {
        const auto timestamp = std::static_pointer_cast<arrow::TimestampType>(arrow_type);
        const auto doris_type = timestamp->timezone().empty() ? TYPE_DATETIMEV2 : TYPE_TIMESTAMPTZ;
        return nullable_primitive(doris_type, 0, arrow_time_precision(timestamp->unit()));
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

LanceTableReader::~LanceTableReader() {
    static_cast<void>(close());
}

Status LanceTableReader::fetch_schema(const TFileRangeDesc& range,
                                      const TFileScanRangeParams& scan_params,
                                      std::vector<std::string>* column_names,
                                      std::vector<DataTypePtr>* column_types) const {
    if (column_names == nullptr || column_types == nullptr) {
        return Status::InvalidArgument("Lance schema output must not be null");
    }
    RETURN_IF_ERROR(_validate_range(range));

    const auto& params = range.table_format_params.lance_params;
    const auto storage_options = _storage_options(&scan_params);
    std::vector<const char*> storage_option_ptrs;
    storage_option_ptrs.reserve(storage_options.size() + 1);
    for (const auto& option : storage_options) {
        storage_option_ptrs.emplace_back(option.c_str());
    }
    storage_option_ptrs.emplace_back(nullptr);

    std::unique_ptr<LanceDataset, LanceDatasetDeleter> dataset(
            lance_dataset_open(params.dataset_uri.c_str(),
                               storage_options.empty() ? nullptr : storage_option_ptrs.data(),
                               static_cast<uint64_t>(params.version)));
    if (dataset == nullptr) {
        return _lance_error("open Lance dataset for schema");
    }

    ArrowSchema arrow_schema {};
    if (lance_dataset_schema(dataset.get(), &arrow_schema) != 0) {
        return _lance_error("get Lance dataset schema");
    }
    auto imported_schema = arrow::ImportSchema(&arrow_schema);
    if (!imported_schema.ok()) {
        if (arrow_schema.release != nullptr) {
            arrow_schema.release(&arrow_schema);
        }
        return Status::InternalError("import Lance Arrow schema failed: {}",
                                     imported_schema.status().message());
    }

    const auto schema = std::move(imported_schema).ValueUnsafe();
    std::vector<std::string> parsed_names;
    std::vector<DataTypePtr> parsed_types;
    parsed_names.reserve(schema->num_fields());
    parsed_types.reserve(schema->num_fields());
    std::unordered_set<std::string> unique_names;
    unique_names.reserve(schema->num_fields());
    for (const auto& field : schema->fields()) {
        if (!unique_names.emplace(field->name()).second) {
            return Status::InvalidArgument("duplicate Lance schema column: {}", field->name());
        }
        DataTypePtr doris_type;
        RETURN_IF_ERROR(arrow_type_to_doris_type(field->type(), &doris_type));
        parsed_names.emplace_back(field->name());
        parsed_types.emplace_back(std::move(doris_type));
    }
    *column_names = std::move(parsed_names);
    *column_types = std::move(parsed_types);
    return Status::OK();
}

Status LanceTableReader::init(TableReadOptions&& options) {
    RETURN_IF_ERROR(TableReader::init(std::move(options)));
    DORIS_CHECK(_runtime_state != nullptr);
    DORIS_CHECK(_scanner_profile != nullptr);
    DORIS_CHECK(_scan_params != nullptr);

    _ctz = _runtime_state->timezone_obj();
    if (_scan_params->__isset.lance_substrait_filter) {
        _scanner_profile->add_info_string("LancePushdownFormat", "SUBSTRAIT");
        _scanner_profile->add_info_string(
                "LanceSubstraitFilterBytes",
                std::to_string(_scan_params->lance_substrait_filter.size()));
    }

    _output_name_to_idx.clear();
    _output_name_to_idx.reserve(_projected_columns.size());
    for (size_t idx = 0; idx < _projected_columns.size(); ++idx) {
        const auto& column = _projected_columns[idx];
        if (column.type == nullptr) {
            return Status::InvalidArgument("Lance projected column '{}' has no type", column.name);
        }
        if (!_output_name_to_idx.emplace(column.name, idx).second) {
            return Status::InvalidArgument("duplicate Lance projected column: {}", column.name);
        }
    }
    return Status::OK();
}

Status LanceTableReader::prepare_split(const SplitReadOptions& options) {
    RETURN_IF_ERROR(_validate_range(options.current_range));
    _close_scanner();
    _eof = false;

    RETURN_IF_ERROR(TableReader::prepare_split(options));
    // Lance does not currently provide metadata aggregate pushdown. Do not let a generic
    // table-level count supplied by a future planner bypass fragment reads.
    _remaining_table_level_count = -1;
    if (current_split_pruned()) {
        return Status::OK();
    }

    const auto key = _dataset_key(options.current_range);
    if (_dataset == nullptr) {
        RETURN_IF_ERROR(_open_dataset(key));
        _opened_dataset_key = key;
    } else if (!_opened_dataset_key.has_value() || *_opened_dataset_key != key) {
        return Status::InvalidArgument(
                "Lance reader cannot mix dataset snapshots or storage options in one scan");
    }

    RETURN_IF_ERROR(_prepare_conjuncts());
    return _open_scanner(options.current_range);
}

Status LanceTableReader::get_block(Block* block, bool* eos) {
    DORIS_CHECK(block != nullptr);
    DORIS_CHECK(eos != nullptr);
    DORIS_CHECK(block->columns() == _projected_columns.size());
    *eos = false;

    if (_eof) {
        *eos = true;
        return Status::OK();
    }
    if (_scanner == nullptr) {
        return Status::InternalError("Lance scanner is not initialized for the current split");
    }

    const auto target_rows = std::max<size_t>(1, _scanner_batch_size);
    while (true) {
        block->clear_column_data(_projected_columns.size());
        size_t raw_rows = 0;
        while (raw_rows < target_rows) {
            if (_io_ctx != nullptr && _io_ctx->should_stop) {
                _eof = true;
                _close_scanner();
                *eos = true;
                return Status::OK();
            }

            LanceBatch* raw_batch = nullptr;
            const int32_t scan_status = lance_scanner_next(_scanner, &raw_batch);
            if (scan_status == 1) {
                _eof = true;
                _close_scanner();
                break;
            }
            if (scan_status != 0 || raw_batch == nullptr) {
                return _lance_error("read next Lance batch");
            }

            std::unique_ptr<LanceBatch, LanceBatchDeleter> batch(raw_batch);
            size_t rows = 0;
            RETURN_IF_ERROR(_fill_block_from_arrow(batch.get(), block, &rows));
            _record_scan_rows(rows);
            raw_rows += rows;
        }

        if (raw_rows == 0) {
            DORIS_CHECK(_eof);
            *eos = true;
            return Status::OK();
        }

        if (!_conjuncts.empty()) {
            RETURN_IF_ERROR(VExprContext::filter_block(_conjuncts, block, block->columns()));
        }
        if (block->rows() > 0) {
            // Preserve a non-empty final block. The next get_block() observes `_eof` and reports
            // split EOF, matching the generic reader contract.
            return Status::OK();
        }
        if (_eof) {
            *eos = true;
            return Status::OK();
        }
    }
}

Status LanceTableReader::abort_split() {
    _close_scanner();
    _eof = true;
    return TableReader::abort_split();
}

Status LanceTableReader::close() {
    _close_scanner();
    _close_dataset();
    _opened_dataset_key.reset();
    _eof = true;
    return TableReader::close();
}

Status LanceTableReader::_validate_range(const TFileRangeDesc& range) const {
    if (!range.__isset.table_format_params || !range.table_format_params.__isset.lance_params) {
        return Status::InvalidArgument("Lance split requires lance_params in table format params");
    }
    const auto& params = range.table_format_params.lance_params;
    if (!params.__isset.dataset_uri || params.dataset_uri.empty()) {
        return Status::InvalidArgument("Lance split requires a non-empty dataset_uri");
    }
    if (!params.__isset.version || params.version < 0) {
        return Status::InvalidArgument("Lance split requires a non-negative dataset version");
    }
    std::unordered_set<int64_t> unique_ids;
    for (const auto fragment_id : params.fragment_ids) {
        if (fragment_id < 0) {
            return Status::InvalidArgument("Lance fragment id must be non-negative: {}",
                                           fragment_id);
        }
        if (!unique_ids.emplace(fragment_id).second) {
            return Status::InvalidArgument("Lance split contains duplicate fragment id: {}",
                                           fragment_id);
        }
    }
    return Status::OK();
}

Status LanceTableReader::_open_dataset(const DatasetKey& key) {
    std::vector<const char*> storage_option_ptrs;
    storage_option_ptrs.reserve(key.storage_options.size() + 1);
    for (const auto& option : key.storage_options) {
        storage_option_ptrs.emplace_back(option.c_str());
    }
    storage_option_ptrs.emplace_back(nullptr);

    _dataset = lance_dataset_open(
            key.uri.c_str(), key.storage_options.empty() ? nullptr : storage_option_ptrs.data(),
            static_cast<uint64_t>(key.version));
    if (_dataset == nullptr) {
        return _lance_error("open Lance dataset");
    }
    return Status::OK();
}

Status LanceTableReader::_open_scanner(const TFileRangeDesc& range) {
    std::vector<const char*> columns;
    columns.reserve(_projected_columns.size() + 1);
    for (const auto& column : _projected_columns) {
        columns.emplace_back(column.name.c_str());
    }
    columns.emplace_back(nullptr);

    LanceScanner* scanner = lance_scanner_new(
            _dataset, _projected_columns.empty() ? nullptr : columns.data(), nullptr);
    if (scanner == nullptr) {
        return _lance_error("create Lance scanner");
    }
    std::unique_ptr<LanceScanner, LanceScannerDeleter> scanner_guard(scanner);

    if (_scan_params->__isset.lance_substrait_filter &&
        !_scan_params->lance_substrait_filter.empty()) {
        const auto& filter = _scan_params->lance_substrait_filter;
        if (lance_scanner_set_substrait_filter(
                    scanner, reinterpret_cast<const uint8_t*>(filter.data()), filter.size()) != 0) {
            return _lance_error("set Lance Substrait filter");
        }
    }

    const auto batch_size = _batch_size > 0 ? _batch_size : _runtime_state->batch_size();
    if (lance_scanner_set_batch_size(scanner, static_cast<int64_t>(batch_size)) != 0) {
        return _lance_error("set Lance scanner batch size");
    }

    const auto& lance_params = range.table_format_params.lance_params;
    if (lance_params.__isset.fragment_ids && !lance_params.fragment_ids.empty()) {
        const auto& thrift_ids = lance_params.fragment_ids;
        std::vector<uint64_t> fragment_ids;
        fragment_ids.reserve(thrift_ids.size());
        for (const auto fragment_id : thrift_ids) {
            fragment_ids.emplace_back(static_cast<uint64_t>(fragment_id));
        }
        if (lance_scanner_set_fragment_ids(scanner, fragment_ids.data(), fragment_ids.size()) != 0) {
            return _lance_error("set Lance scanner fragment ids");
        }
    }
    _scanner = scanner_guard.release();
    _scanner_batch_size = batch_size;
    return Status::OK();
}

void LanceTableReader::_close_scanner() {
    if (_scanner != nullptr) {
        lance_scanner_close(_scanner);
        _scanner = nullptr;
    }
    _scanner_batch_size = 0;
}

void LanceTableReader::_close_dataset() {
    if (_dataset != nullptr) {
        lance_dataset_close(_dataset);
        _dataset = nullptr;
    }
}

Status LanceTableReader::_fill_block_from_arrow(LanceBatch* batch, Block* block, size_t* rows) {
    DORIS_CHECK(batch != nullptr);
    DORIS_CHECK(block != nullptr);
    DORIS_CHECK(rows != nullptr);
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

    const auto record_batch = std::move(result).ValueUnsafe();
    const auto row_count = static_cast<size_t>(record_batch->num_rows());
    std::unordered_set<std::string> materialized_columns;
    materialized_columns.reserve(record_batch->num_columns());
    auto columns_guard = block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    for (int arrow_idx = 0; arrow_idx < record_batch->num_columns(); ++arrow_idx) {
        const auto& field = record_batch->schema()->field(arrow_idx);
        const auto output_it = _output_name_to_idx.find(field->name());
        if (output_it == _output_name_to_idx.end()) {
            return Status::InternalError("Lance returned unknown column '{}'", field->name());
        }
        if (!materialized_columns.emplace(field->name()).second) {
            return Status::InternalError("Lance returned duplicate column '{}'", field->name());
        }
        const auto output_idx = output_it->second;
        try {
            RETURN_IF_ERROR(columns_guard.get_datatype_by_position(output_idx)
                                    ->get_serde()
                                    ->read_column_from_arrow(*columns[output_idx],
                                                             record_batch->column(arrow_idx).get(),
                                                             0, row_count, _ctz));
        } catch (const Exception& e) {
            return Status::InternalError("convert Lance Arrow column '{}' failed: {}",
                                         field->name(), e.what());
        }
    }
    for (const auto& column : _projected_columns) {
        if (!materialized_columns.contains(column.name)) {
            return Status::InternalError("Lance did not return requested column '{}'", column.name);
        }
    }
    *rows = row_count;
    return Status::OK();
}

Status LanceTableReader::_prepare_conjuncts() {
    RowDescriptor row_desc;
    for (const auto& conjunct : _conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(_runtime_state, row_desc));
        RETURN_IF_ERROR(conjunct->open(_runtime_state));
    }
    return Status::OK();
}

std::vector<std::string> LanceTableReader::_storage_options(
        const TFileScanRangeParams* scan_params) {
    if (scan_params == nullptr || !scan_params->__isset.properties) {
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
        const auto it = scan_params->properties.find(std::string(doris_key));
        if (it != scan_params->properties.end() && !it->second.empty()) {
            options.emplace_back(lance_key);
            options.emplace_back(it->second);
        }
    }
    const auto endpoint = scan_params->properties.find("AWS_ENDPOINT");
    if (endpoint != scan_params->properties.end() && endpoint->second.rfind("http://", 0) == 0) {
        options.emplace_back("allow_http");
        options.emplace_back("true");
    }
    const auto path_style = scan_params->properties.find("use_path_style");
    if (path_style != scan_params->properties.end() && !path_style->second.empty()) {
        const bool use_path_style = path_style->second == "true" || path_style->second == "1";
        options.emplace_back("aws_virtual_hosted_style_request");
        options.emplace_back(use_path_style ? "false" : "true");
    }
    return options;
}

LanceTableReader::DatasetKey LanceTableReader::_dataset_key(const TFileRangeDesc& range) const {
    const auto& params = range.table_format_params.lance_params;
    return {
            .uri = params.dataset_uri,
            .version = params.version,
            .storage_options = _storage_options(_scan_params),
    };
}

Status LanceTableReader::_lance_error(std::string_view operation) {
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

} // namespace doris::format::lance
