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

#include "format_v2/lance/lance_reader_helper.h"

#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <fmt/format.h>
#include <lance/lance.h>

#include <limits>
#include <unordered_set>

#include "common/logging.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nothing.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"

namespace doris::format::lance {
namespace {

constexpr std::string_view ARROW_EXTENSION_NAME = "ARROW:extension:name";

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

void LanceDatasetDeleter::operator()(LanceDataset* dataset) const {
    lance_dataset_close(dataset);
}

void LanceScannerDeleter::operator()(LanceScanner* scanner) const {
    lance_scanner_close(scanner);
}

void LanceBatchDeleter::operator()(LanceBatch* batch) const {
    lance_batch_free(batch);
}

size_t lance_vector_element_width(TVectorElementType::type type) {
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

Status build_lance_storage_options(const TFileScanRangeParams* scan_params,
                                   std::vector<std::string>* options) {
    DORIS_CHECK(options != nullptr);
    options->clear();
    if (scan_params == nullptr || !scan_params->__isset.lance_scan_params ||
        !scan_params->lance_scan_params.__isset.lance_storage_options) {
        return Status::OK();
    }
    const auto& storage_options = scan_params->lance_scan_params.lance_storage_options;
    options->reserve(storage_options.size() * 2);
    for (const auto& [key, value] : storage_options) {
        // Both values cross a C-string boundary. Reject embedded NULs instead of silently opening
        // a different dataset configuration from the one validated and used by the FE.
        if (key.find('\0') != std::string::npos || value.find('\0') != std::string::npos) {
            return Status::InvalidArgument(
                    "Lance storage option '{}' contains a NUL and cannot reach lance-c",
                    key.substr(0, key.find('\0')));
        }
        options->emplace_back(key);
        options->emplace_back(value);
    }
    return Status::OK();
}

Status lance_error(std::string_view operation) {
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
