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
#include <arrow/util/key_value_metadata.h>
#include <lance/lance.h>

#include <bit>
#include <cstring>
#include <limits>
#include <memory>

#include "common/logging.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nothing.h"
#include "core/data_type/data_type_struct.h"
#include "exec/common/endian.h"

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

constexpr std::string_view DISTANCE_COLUMN = "_distance";
constexpr std::string_view ARROW_EXTENSION_NAME = "ARROW:extension:name";

size_t vector_element_width(TVectorElementType::type type) {
    switch (type) {
    case TVectorElementType::FLOAT16:
        return sizeof(uint16_t);
    case TVectorElementType::FLOAT32:
        return sizeof(float);
    case TVectorElementType::FLOAT64:
        return sizeof(double);
    case TVectorElementType::UINT8:
    case TVectorElementType::INT8:
        return sizeof(uint8_t);
    }
    return 0;
}

std::string vector_metric_name(TVectorMetric::type metric) {
    switch (metric) {
    case TVectorMetric::DEFAULT:
        return "default";
    case TVectorMetric::L2:
        return "l2";
    case TVectorMetric::COSINE:
        return "cosine";
    case TVectorMetric::DOT_PRODUCT:
        return "dot";
    case TVectorMetric::HAMMING:
        return "hamming";
    }
    return "unknown";
}

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

Status check_arrow_field_semantics(const std::shared_ptr<arrow::Field>& field) {
    if (field->HasMetadata()) {
        const auto extension_name = field->metadata()->Get(ARROW_EXTENSION_NAME);
        if (extension_name.ok() && !extension_name.ValueUnsafe().empty()) {
            return Status::NotSupported(
                    "unsupported Lance Arrow extension type '{}' for field '{}'",
                    extension_name.ValueUnsafe(), field->name());
        }
    }
    if (field->type()->id() == arrow::Type::DICTIONARY) {
        return Status::NotSupported("unsupported Lance Arrow dictionary type for field '{}': {}",
                                    field->name(), field->type()->ToString());
    }
    return Status::OK();
}

Status arrow_field_to_doris_type(const std::shared_ptr<arrow::Field>& field,
                                 DataTypePtr* doris_type) {
    RETURN_IF_ERROR(check_arrow_field_semantics(field));
    const auto& arrow_type = field->type();
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
        return nullable_primitive(TYPE_VARBINARY, 0, 0, std::numeric_limits<int32_t>::max());
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
        const int scale = decimal->scale();
        if (precision <= 0 || precision > arrow::Decimal256Type::kMaxPrecision || scale < 0 ||
            scale > precision) {
            return Status::NotSupported(
                    "unsupported Lance Arrow decimal type for field '{}': precision={}, scale={}",
                    field->name(), precision, scale);
        }
        const PrimitiveType doris_decimal_type = precision <= 9    ? TYPE_DECIMAL32
                                                 : precision <= 18 ? TYPE_DECIMAL64
                                                 : precision <= 38 ? TYPE_DECIMAL128I
                                                                   : TYPE_DECIMAL256;
        return nullable_primitive(doris_decimal_type, precision, scale);
    }
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST:
    case arrow::Type::FIXED_SIZE_LIST: {
        const auto list = std::static_pointer_cast<arrow::BaseListType>(arrow_type);
        DataTypePtr value_type;
        RETURN_IF_ERROR(arrow_field_to_doris_type(list->value_field(), &value_type));
        *doris_type = make_nullable(std::make_shared<DataTypeArray>(value_type));
        return Status::OK();
    }
    case arrow::Type::MAP: {
        const auto map = std::static_pointer_cast<arrow::MapType>(arrow_type);
        RETURN_IF_ERROR(check_arrow_field_semantics(map->value_field()));
        DataTypePtr key_type;
        DataTypePtr item_type;
        RETURN_IF_ERROR(arrow_field_to_doris_type(map->key_field(), &key_type));
        RETURN_IF_ERROR(arrow_field_to_doris_type(map->item_field(), &item_type));
        *doris_type = make_nullable(std::make_shared<DataTypeMap>(key_type, item_type));
        return Status::OK();
    }
    case arrow::Type::STRUCT: {
        const auto struct_type = std::static_pointer_cast<arrow::StructType>(arrow_type);
        DataTypes field_types;
        Strings field_names;
        field_types.reserve(struct_type->num_fields());
        field_names.reserve(struct_type->num_fields());
        for (const auto& child : struct_type->fields()) {
            DataTypePtr field_type;
            RETURN_IF_ERROR(arrow_field_to_doris_type(child, &field_type));
            field_types.emplace_back(std::move(field_type));
            field_names.emplace_back(child->name());
        }
        *doris_type = make_nullable(std::make_shared<DataTypeStruct>(field_types, field_names));
        return Status::OK();
    }
    default:
        return Status::NotSupported("unsupported Lance Arrow type: {}", arrow_type->ToString());
    }
}

} // namespace

Status convert_arrow_schema_to_doris(const std::shared_ptr<arrow::Schema>& arrow_schema,
                                     std::vector<std::string>* column_names,
                                     std::vector<DataTypePtr>* column_types) {
    DORIS_CHECK(arrow_schema != nullptr);
    DORIS_CHECK(column_names != nullptr);
    DORIS_CHECK(column_types != nullptr);

    std::vector<std::string> parsed_names;
    std::vector<DataTypePtr> parsed_types;
    parsed_names.reserve(arrow_schema->num_fields());
    parsed_types.reserve(arrow_schema->num_fields());
    std::unordered_set<std::string> unique_names;
    unique_names.reserve(arrow_schema->num_fields());
    for (const auto& field : arrow_schema->fields()) {
        if (!unique_names.emplace(field->name()).second) {
            return Status::InvalidArgument("duplicate Lance schema column: {}", field->name());
        }
        DataTypePtr doris_type;
        const auto type_status = arrow_field_to_doris_type(field, &doris_type);
        if (type_status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) {
            parsed_types.emplace_back(std::make_shared<DataTypeNothing>());
        } else {
            RETURN_IF_ERROR(type_status);
            DORIS_CHECK(doris_type != nullptr);
            parsed_types.emplace_back(std::move(doris_type));
        }
        parsed_names.emplace_back(field->name());
    }
    *column_names = std::move(parsed_names);
    *column_types = std::move(parsed_types);
    return Status::OK();
}

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

    return convert_arrow_schema_to_doris(std::move(imported_schema).ValueUnsafe(), column_names,
                                         column_types);
}

Status LanceTableReader::init(TableReadOptions&& options) {
    RETURN_IF_ERROR(TableReader::init(std::move(options)));
    DORIS_CHECK(_runtime_state != nullptr);
    DORIS_CHECK(_scanner_profile != nullptr);
    DORIS_CHECK(_scan_params != nullptr);

    _ctz = _runtime_state->timezone_obj();
    _vector_search = _scan_params->__isset.external_search_request;
    _search_split_prepared = false;
    if (_vector_search) {
        RETURN_IF_ERROR(_validate_external_search_request());
        const auto& vector = _scan_params->external_search_request.query.vector;
        _scanner_profile->add_info_string("ExternalSearchType", "VECTOR");
        _scanner_profile->add_info_string("LanceVectorColumn", vector.column);
        _scanner_profile->add_info_string("LanceTopK", std::to_string(vector.top_k));
        _scanner_profile->add_info_string("LanceOffset", std::to_string(vector.offset));
        _scanner_profile->add_info_string("LanceMetric", vector.__isset.metric
                                                                 ? vector_metric_name(vector.metric)
                                                                 : "default");
        _scanner_profile->add_info_string("LanceVectorDimension",
                                          std::to_string(vector.query_vector.dimension));
    }
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
        if (_vector_search && column.name == DISTANCE_COLUMN) {
            const auto distance_type = remove_nullable(column.type);
            if (distance_type->get_primitive_type() != TYPE_FLOAT) {
                return Status::InvalidArgument(
                        "Lance vector search column '{}' must have Doris FLOAT type, but was {}",
                        DISTANCE_COLUMN, column.type->get_name());
            }
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

    if (_vector_search) {
        const auto& lance_params = options.current_range.table_format_params.lance_params;
        if (lance_params.version <= 0) {
            return Status::InvalidArgument(
                    "Lance vector search requires a fixed positive dataset version");
        }
        if (lance_params.__isset.fragment_ids) {
            return Status::InvalidArgument("Lance vector search split must not set fragment ids");
        }
        if (_search_split_prepared) {
            return Status::InvalidArgument(
                    "Lance vector search supports exactly one whole-dataset split");
        }
    }

    const auto key = _dataset_key(options.current_range);
    if (_dataset == nullptr) {
        RETURN_IF_ERROR(_open_dataset(key));
        _opened_dataset_key = key;
    } else if (!_opened_dataset_key.has_value() || *_opened_dataset_key != key) {
        return Status::InvalidArgument(
                "Lance reader cannot mix dataset snapshots or storage options in one scan");
    }

    RETURN_IF_ERROR(_open_scanner(options.current_range));
    _search_split_prepared = _vector_search;
    return Status::OK();
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

Status LanceTableReader::_validate_external_search_request() const {
    DORIS_CHECK(_scan_params != nullptr);
    DORIS_CHECK(_scan_params->__isset.external_search_request);
    if (_scan_params->__isset.lance_substrait_filter) {
        return Status::InvalidArgument(
                "Lance vector search cannot combine its pre-search filter with "
                "lance_substrait_filter");
    }
    const auto& request = _scan_params->external_search_request;
    if (request.__isset.schema_version && request.schema_version != 1) {
        return Status::NotSupported("unsupported external search schema version: {}",
                                    request.schema_version);
    }
    if (!request.__isset.query) {
        return Status::InvalidArgument("external search request requires query");
    }

    const bool has_vector = request.query.__isset.vector;
    const bool has_full_text = request.query.__isset.full_text;
    if (has_vector == has_full_text) {
        return Status::InvalidArgument("external search query must set exactly one search kind");
    }
    if (has_full_text) {
        return Status::NotSupported("Lance Format V2 reader does not yet support full-text search");
    }

    const auto& vector = request.query.vector;
    if (!vector.__isset.column || vector.column.empty() ||
        vector.column.find('\0') != std::string::npos) {
        return Status::InvalidArgument("Lance vector search requires a non-empty column");
    }
    if (!vector.__isset.query_vector) {
        return Status::InvalidArgument("Lance vector search requires a query vector");
    }
    const auto& query_vector = vector.query_vector;
    if (!query_vector.__isset.element_type || !query_vector.__isset.dimension ||
        !query_vector.__isset.values) {
        return Status::InvalidArgument(
                "Lance query vector requires element_type, dimension, and values");
    }
    if (query_vector.dimension <= 0) {
        return Status::InvalidArgument("Lance query vector dimension must be positive: {}",
                                       query_vector.dimension);
    }
    const auto element_width = vector_element_width(query_vector.element_type);
    if (element_width == 0) {
        return Status::NotSupported("unsupported Lance query vector element type: {}",
                                    static_cast<int>(query_vector.element_type));
    }
    const auto dimension = static_cast<size_t>(query_vector.dimension);
    if (dimension > std::numeric_limits<size_t>::max() / element_width ||
        query_vector.values.size() != dimension * element_width) {
        return Status::InvalidArgument(
                "Lance query vector byte size {} does not match dimension {} and element width {}",
                query_vector.values.size(), dimension, element_width);
    }
    if (!vector.__isset.top_k || vector.top_k <= 0) {
        return Status::InvalidArgument("Lance vector search top_k must be positive");
    }
    if (!vector.__isset.offset || vector.offset < 0) {
        return Status::InvalidArgument("Lance vector search offset must be non-negative");
    }
    constexpr auto UINT32_MAX_VALUE = static_cast<int64_t>(std::numeric_limits<uint32_t>::max());
    if (vector.offset > UINT32_MAX_VALUE || vector.top_k > UINT32_MAX_VALUE - vector.offset) {
        return Status::InvalidArgument("Lance vector search top_k + offset exceeds uint32 range");
    }

    if (request.__isset.filter) {
        const auto& filter = request.filter;
        if (!filter.__isset.format || !filter.__isset.payload || filter.payload.empty()) {
            return Status::InvalidArgument(
                    "external search filter requires format and non-empty payload");
        }
        switch (filter.format) {
        case TSearchFilterFormat::SQL:
            if (filter.payload.find('\0') != std::string::npos) {
                return Status::InvalidArgument(
                        "Lance SQL search filter contains an embedded NUL byte");
            }
            break;
        case TSearchFilterFormat::SUBSTRAIT:
            break;
        default:
            return Status::NotSupported("unsupported external search filter format: {}",
                                        static_cast<int>(filter.format));
        }
    }

    if (request.__isset.lance_options) {
        const auto& options = request.lance_options;
        if (options.__isset.nprobes && options.nprobes <= 0) {
            return Status::InvalidArgument("Lance nprobes must be positive");
        }
        if (options.__isset.refine_factor && options.refine_factor <= 0) {
            return Status::InvalidArgument("Lance refine_factor must be positive");
        }
        if (options.__isset.ef && options.ef <= 0) {
            return Status::InvalidArgument("Lance ef must be positive");
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
    if (_vector_search && _projected_columns.empty()) {
        columns.emplace_back(DISTANCE_COLUMN.data());
    }
    columns.emplace_back(nullptr);

    const char* sql_filter = nullptr;
    if (_vector_search) {
        const auto& request = _scan_params->external_search_request;
        if (request.__isset.filter && request.filter.format == TSearchFilterFormat::SQL) {
            sql_filter = request.filter.payload.c_str();
        }
    }
    LanceScanner* scanner =
            lance_scanner_new(_dataset, columns.size() == 1 ? nullptr : columns.data(), sql_filter);
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
    if (_vector_search) {
        const auto& request = _scan_params->external_search_request;
        if (request.__isset.filter && request.filter.format == TSearchFilterFormat::SUBSTRAIT) {
            const auto& filter = request.filter.payload;
            if (lance_scanner_set_substrait_filter(scanner,
                                                   reinterpret_cast<const uint8_t*>(filter.data()),
                                                   filter.size()) != 0) {
                return _lance_error("set Lance vector search Substrait filter");
            }
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
        if (lance_scanner_set_fragment_ids(scanner, fragment_ids.data(), fragment_ids.size()) !=
            0) {
            return _lance_error("set Lance scanner fragment ids");
        }
    }
    // Ordinary scans may carry a pushed-down LIMIT. The FE only sets it when all predicates are
    // pushed into Lance, so the scanner can safely stop after `limit` rows. Vector search manages
    // its own top_k limit in _configure_vector_search, so skip it here.
    if (!_vector_search && lance_params.__isset.limit && lance_params.limit > 0) {
        if (lance_scanner_set_limit(scanner, lance_params.limit) != 0) {
            return _lance_error("set Lance scanner limit");
        }
    }
    if (_vector_search) {
        RETURN_IF_ERROR(_configure_vector_search(scanner));
    }
    _scanner = scanner_guard.release();
    _scanner_batch_size = batch_size;
    return Status::OK();
}

Status LanceTableReader::_configure_vector_search(LanceScanner* scanner) const {
    DORIS_CHECK(scanner != nullptr);
    DORIS_CHECK(_scan_params != nullptr);
    const auto& request = _scan_params->external_search_request;
    const auto& vector = request.query.vector;
    const auto& query = vector.query_vector;
    const auto* bytes = query.values.data();
    const auto dimension = static_cast<size_t>(query.dimension);
    const auto candidate_k = static_cast<uint32_t>(vector.top_k + vector.offset);

    const auto set_nearest = [&](const void* values, LanceDataType type) -> Status {
        if (lance_scanner_nearest(scanner, vector.column.c_str(), values, dimension, type,
                                  candidate_k) != 0) {
            return _lance_error("set Lance nearest query");
        }
        return Status::OK();
    };

    switch (query.element_type) {
    case TVectorElementType::FLOAT16: {
        std::vector<uint16_t> values(dimension);
        for (size_t i = 0; i < dimension; ++i) {
            values[i] = LittleEndian::Load16(bytes + i * sizeof(uint16_t));
        }
        RETURN_IF_ERROR(set_nearest(values.data(), LANCE_DTYPE_FLOAT16));
        break;
    }
    case TVectorElementType::FLOAT32: {
        std::vector<float> values(dimension);
        for (size_t i = 0; i < dimension; ++i) {
            const auto bits = LittleEndian::Load32(bytes + i * sizeof(uint32_t));
            values[i] = std::bit_cast<float>(bits);
        }
        RETURN_IF_ERROR(set_nearest(values.data(), LANCE_DTYPE_FLOAT32));
        break;
    }
    case TVectorElementType::FLOAT64: {
        std::vector<double> values(dimension);
        for (size_t i = 0; i < dimension; ++i) {
            const auto bits = LittleEndian::Load64(bytes + i * sizeof(uint64_t));
            values[i] = std::bit_cast<double>(bits);
        }
        RETURN_IF_ERROR(set_nearest(values.data(), LANCE_DTYPE_FLOAT64));
        break;
    }
    case TVectorElementType::UINT8: {
        std::vector<uint8_t> values(dimension);
        std::memcpy(values.data(), bytes, dimension);
        RETURN_IF_ERROR(set_nearest(values.data(), LANCE_DTYPE_UINT8));
        break;
    }
    case TVectorElementType::INT8: {
        std::vector<int8_t> values(dimension);
        std::memcpy(values.data(), bytes, dimension);
        RETURN_IF_ERROR(set_nearest(values.data(), LANCE_DTYPE_INT8));
        break;
    }
    default:
        return Status::NotSupported("unsupported Lance query vector element type: {}",
                                    static_cast<int>(query.element_type));
    }

    if (vector.__isset.metric && vector.metric != TVectorMetric::DEFAULT) {
        LanceMetricType metric;
        switch (vector.metric) {
        case TVectorMetric::L2:
            metric = LANCE_METRIC_L2;
            break;
        case TVectorMetric::COSINE:
            metric = LANCE_METRIC_COSINE;
            break;
        case TVectorMetric::DOT_PRODUCT:
            metric = LANCE_METRIC_DOT;
            break;
        case TVectorMetric::HAMMING:
            metric = LANCE_METRIC_HAMMING;
            break;
        default:
            return Status::NotSupported("unsupported Lance vector metric: {}",
                                        static_cast<int>(vector.metric));
        }
        if (lance_scanner_set_metric(scanner, metric) != 0) {
            return _lance_error("set Lance vector metric");
        }
    }

    if (request.__isset.lance_options) {
        const auto& options = request.lance_options;
        if (options.__isset.nprobes &&
            lance_scanner_set_nprobes(scanner, static_cast<uint32_t>(options.nprobes)) != 0) {
            return _lance_error("set Lance vector nprobes");
        }
        if (options.__isset.refine_factor &&
            lance_scanner_set_refine_factor(scanner,
                                            static_cast<uint32_t>(options.refine_factor)) != 0) {
            return _lance_error("set Lance vector refine factor");
        }
        if (options.__isset.ef &&
            lance_scanner_set_ef(scanner, static_cast<uint32_t>(options.ef)) != 0) {
            return _lance_error("set Lance vector ef");
        }
        if (options.__isset.use_index &&
            lance_scanner_set_use_index(scanner, options.use_index) != 0) {
            return _lance_error("set Lance vector use_index");
        }
    }
    if (request.__isset.filter && lance_scanner_set_prefilter(scanner, true) != 0) {
        return _lance_error("enable Lance vector prefilter");
    }
    if (lance_scanner_set_offset(scanner, vector.offset) != 0) {
        return _lance_error("set Lance vector offset");
    }
    if (lance_scanner_set_limit(scanner, vector.top_k) != 0) {
        return _lance_error("set Lance vector result limit");
    }
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
            if (_vector_search && field->name() == DISTANCE_COLUMN) {
                // Lance currently auto-projects _distance for nearest queries. It is valid for
                // Doris slot pruning to omit that optional result column.
                continue;
            }
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

// The FE sends these already in Lance's own vocabulary, merged from the catalog properties and
// from whatever the namespace vended. Re-encoding them here would drop every option this list did
// not anticipate, so they are handed to lance-c as they arrive.
std::vector<std::string> LanceTableReader::_storage_options(
        const TFileScanRangeParams* scan_params) {
    if (scan_params == nullptr || !scan_params->__isset.lance_storage_options) {
        return {};
    }
    std::vector<std::string> options;
    options.reserve(scan_params->lance_storage_options.size() * 2);
    for (const auto& [key, value] : scan_params->lance_storage_options) {
        if (value.empty()) {
            continue;
        }
        // These become C strings below, so an embedded NUL would silently truncate the option and
        // leave lance-c reading a different key than the FE resolved. The FE drops those already;
        // this guards against one that predates the check.
        if (key.find('\0') != std::string::npos || value.find('\0') != std::string::npos) {
            LOG(WARNING) << "Ignoring Lance storage option whose key or value contains a NUL";
            continue;
        }
        options.emplace_back(key);
        options.emplace_back(value);
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
