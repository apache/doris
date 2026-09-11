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

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/extension_type.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <fmt/format.h>
#include <lance/lance.h>

#include <bit>
#include <limits>
#include <optional>
#include <unordered_set>

#include "common/config.h"
#include "common/logging.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nothing.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type_serde/arrow_validation.h"
#include "exec/common/endian.h"

namespace doris::format::lance {
namespace {

constexpr std::string_view ARROW_EXTENSION_NAME = "ARROW:extension:name";
constexpr std::string_view ARROW_JSON_EXTENSION = "arrow.json";
constexpr std::string_view LANCE_JSON_EXTENSION = "lance.json";
constexpr std::string_view LANCE_BFLOAT16_EXTENSION = "lance.bfloat16";

enum class LanceExtensionKind {
    NONE,
    JSON,
    BFLOAT16,
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

// Extract and validate extension names from metadata and registered Arrow types.
Status get_lance_extension(const std::shared_ptr<arrow::Field>& field,
                           LanceExtensionKind* extension_kind,
                           std::shared_ptr<arrow::DataType>* storage_type) {
    DORIS_CHECK(field != nullptr);
    DORIS_CHECK(extension_kind != nullptr);
    DORIS_CHECK(storage_type != nullptr);
    *extension_kind = LanceExtensionKind::NONE;
    *storage_type = field->type();

    std::optional<std::string> extension_name;
    if (field->HasMetadata()) {
        const auto metadata_name = field->metadata()->Get(ARROW_EXTENSION_NAME);
        if (metadata_name.ok() && !metadata_name.ValueUnsafe().empty()) {
            extension_name = metadata_name.ValueUnsafe();
        }
    }
    if (field->type()->id() == arrow::Type::EXTENSION) {
        const auto extension_type = std::dynamic_pointer_cast<arrow::ExtensionType>(field->type());
        if (extension_type == nullptr) {
            return Status::InvalidArgument("invalid Arrow extension type for Lance field '{}'",
                                           field->name());
        }
        if (extension_name.has_value() && *extension_name != extension_type->extension_name()) {
            return Status::InvalidArgument(
                    "conflicting Arrow extension names for Lance field '{}': '{}' and '{}'",
                    field->name(), *extension_name, extension_type->extension_name());
        }
        extension_name = extension_type->extension_name();
        *storage_type = extension_type->storage_type();
    }
    if (!extension_name.has_value()) {
        return Status::OK();
    }

    if (*extension_name == ARROW_JSON_EXTENSION) {
        if ((*storage_type)->id() != arrow::Type::STRING &&
            (*storage_type)->id() != arrow::Type::LARGE_STRING) {
            return Status::NotSupported(
                    "Arrow JSON extension for Lance field '{}' requires UTF8 storage, got {}",
                    field->name(), (*storage_type)->ToString());
        }
        *extension_kind = LanceExtensionKind::JSON;
        return Status::OK();
    }
    if (*extension_name == LANCE_JSON_EXTENSION) {
        if ((*storage_type)->id() != arrow::Type::LARGE_BINARY) {
            return Status::NotSupported(
                    "Lance JSON extension for field '{}' requires LARGE_BINARY storage, got {}",
                    field->name(), (*storage_type)->ToString());
        }
        *extension_kind = LanceExtensionKind::JSON;
        return Status::OK();
    }
    if (*extension_name == LANCE_BFLOAT16_EXTENSION) {
        if ((*storage_type)->id() != arrow::Type::FIXED_SIZE_BINARY ||
            std::static_pointer_cast<arrow::FixedSizeBinaryType>(*storage_type)->byte_width() !=
                    2) {
            return Status::NotSupported(
                    "Lance BFloat16 extension for field '{}' requires FIXED_SIZE_BINARY(2) "
                    "storage, got {}",
                    field->name(), (*storage_type)->ToString());
        }
        *extension_kind = LanceExtensionKind::BFLOAT16;
        return Status::OK();
    }
    return Status::NotSupported("unsupported Lance Arrow extension type '{}' for field '{}'",
                                *extension_name, field->name());
}

// Map an Arrow field to a Doris type, allowing Doris NULL only at the top level.
Status arrow_field_to_doris_type(const std::shared_ptr<arrow::Field>& field,
                                 DataTypePtr* doris_type, bool allow_null) {
    const auto nullable_primitive = [&](PrimitiveType type, int precision = 0, int scale = 0,
                                        int len = -1) {
        *doris_type =
                DataTypeFactory::instance().create_data_type(type, true, precision, scale, len);
        return Status::OK();
    };

    LanceExtensionKind extension_kind;
    std::shared_ptr<arrow::DataType> arrow_type;
    RETURN_IF_ERROR(get_lance_extension(field, &extension_kind, &arrow_type));
    switch (extension_kind) {
    case LanceExtensionKind::JSON:
        return nullable_primitive(TYPE_JSONB);
    case LanceExtensionKind::BFLOAT16:
        return nullable_primitive(TYPE_FLOAT);
    case LanceExtensionKind::NONE:
        break;
    }
    if (arrow_type->id() == arrow::Type::DICTIONARY) {
        return Status::NotSupported("unsupported Lance Arrow dictionary type for field '{}': {}",
                                    field->name(), arrow_type->ToString());
    }

    switch (arrow_type->id()) {
    case arrow::Type::NA:
        return allow_null ? nullable_primitive(TYPE_NULL)
                          : Status::NotSupported(
                                    "nested Arrow null type is unsupported for Lance field '{}'",
                                    field->name());
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
    case arrow::Type::DURATION:
        return nullable_primitive(TYPE_BIGINT);
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
        RETURN_IF_ERROR(arrow_field_to_doris_type(list->value_field(), &value_type, false));
        *doris_type = make_nullable(std::make_shared<DataTypeArray>(value_type));
        return Status::OK();
    }
    case arrow::Type::MAP: {
        const auto map = std::static_pointer_cast<arrow::MapType>(arrow_type);
        DataTypePtr key_type;
        DataTypePtr item_type;
        RETURN_IF_ERROR(arrow_field_to_doris_type(map->key_field(), &key_type, false));
        RETURN_IF_ERROR(arrow_field_to_doris_type(map->item_field(), &item_type, false));
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
            RETURN_IF_ERROR(arrow_field_to_doris_type(child, &field_type, false));
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

// Determine whether a field subtree contains values that require Lance normalization.
Status field_requires_lance_normalization(const std::shared_ptr<arrow::Field>& field,
                                          bool* requires_normalization) {
    DORIS_CHECK(field != nullptr);
    DORIS_CHECK(requires_normalization != nullptr);

    LanceExtensionKind extension_kind;
    std::shared_ptr<arrow::DataType> storage_type;
    RETURN_IF_ERROR(get_lance_extension(field, &extension_kind, &storage_type));
    bool required = extension_kind == LanceExtensionKind::BFLOAT16 ||
                    field->type()->id() == arrow::Type::EXTENSION;
    for (const auto& child : storage_type->fields()) {
        bool child_required = false;
        RETURN_IF_ERROR(field_requires_lance_normalization(child, &child_required));
        required |= child_required;
    }
    *requires_normalization = required;
    return Status::OK();
}

// Widen little-endian Lance BFloat16 values to Arrow Float32 without precision loss.
Status convert_bfloat16_array(const std::shared_ptr<arrow::Array>& array,
                              std::shared_ptr<arrow::Array>* normalized) {
    DORIS_CHECK(array != nullptr);
    DORIS_CHECK(normalized != nullptr);
    const auto fixed_binary = std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(array);
    if (fixed_binary == nullptr || fixed_binary->byte_width() != 2) {
        return Status::InvalidArgument("invalid Lance BFloat16 array storage: {}",
                                       array->type()->ToString());
    }
    if (config::enable_arrow_input_validation) {
        check_arrow_fixed_width_buffer(*fixed_binary, sizeof(uint16_t));
    }

    arrow::FloatBuilder builder;
    auto arrow_status = builder.Reserve(fixed_binary->length());
    if (!arrow_status.ok()) {
        return Status::InternalError("reserve Lance BFloat16 output failed: {}",
                                     arrow_status.message());
    }
    for (int64_t row = 0; row < fixed_binary->length(); ++row) {
        if (fixed_binary->IsNull(row)) {
            arrow_status = builder.AppendNull();
        } else {
            const auto bits = LittleEndian::Load16(fixed_binary->GetValue(row));
            arrow_status = builder.Append(std::bit_cast<float>(static_cast<uint32_t>(bits) << 16));
        }
        if (!arrow_status.ok()) {
            return Status::InternalError("append Lance BFloat16 value failed: {}",
                                         arrow_status.message());
        }
    }
    std::shared_ptr<arrow::FloatArray> result;
    arrow_status = builder.Finish(&result);
    if (!arrow_status.ok()) {
        return Status::InternalError("finish Lance BFloat16 conversion failed: {}",
                                     arrow_status.message());
    }
    *normalized = std::move(result);
    return Status::OK();
}

// Rebuild a nested Arrow type after one or more child arrays changed physical type.
Status set_lance_nested_type(std::string_view field_name,
                             const std::shared_ptr<arrow::DataType>& source_type,
                             const arrow::FieldVector& child_fields,
                             std::shared_ptr<arrow::ArrayData>* data) {
    switch (source_type->id()) {
    case arrow::Type::LIST:
        (*data)->type = arrow::list(child_fields[0]);
        break;
    case arrow::Type::LARGE_LIST:
        (*data)->type = arrow::large_list(child_fields[0]);
        break;
    case arrow::Type::FIXED_SIZE_LIST:
        (*data)->type = arrow::fixed_size_list(
                child_fields[0],
                std::static_pointer_cast<arrow::FixedSizeListType>(source_type)->list_size());
        break;
    case arrow::Type::STRUCT:
        (*data)->type = arrow::struct_(child_fields);
        break;
    case arrow::Type::MAP: {
        const auto map_type = std::static_pointer_cast<arrow::MapType>(source_type);
        auto normalized_type = arrow::MapType::Make(child_fields[0], map_type->keys_sorted());
        if (!normalized_type.ok()) {
            return Status::InvalidArgument("normalize Lance map field '{}' failed: {}", field_name,
                                           normalized_type.status().message());
        }
        (*data)->type = std::move(normalized_type).ValueUnsafe();
        break;
    }
    default:
        return Status::InvalidArgument("Lance field '{}' has unexpected child-bearing type {}",
                                       field_name, source_type->ToString());
    }
    return Status::OK();
}

// Check whether an Arrow type tree contains a registered extension wrapper.
bool type_contains_registered_extension(const std::shared_ptr<arrow::DataType>& type) {
    if (type->id() == arrow::Type::EXTENSION) {
        return true;
    }
    for (const auto& field : type->fields()) {
        if (type_contains_registered_extension(field->type())) {
            return true;
        }
    }
    return false;
}

// Remove registered ExtensionArray wrappers only along extension-bearing branches.
Status unwrap_lance_extension_arrays(const std::shared_ptr<arrow::DataType>& expected_type,
                                     const std::shared_ptr<arrow::Array>& array,
                                     std::shared_ptr<arrow::Array>* unwrapped) {
    DORIS_CHECK(expected_type != nullptr);
    DORIS_CHECK(array != nullptr);
    DORIS_CHECK(unwrapped != nullptr);

    auto storage_array = array;
    auto expected_storage_type = expected_type;
    if (expected_type->id() == arrow::Type::EXTENSION) {
        const auto extension_type = std::dynamic_pointer_cast<arrow::ExtensionType>(expected_type);
        if (extension_type == nullptr) {
            return Status::InvalidArgument("invalid expected Arrow extension type {}",
                                           expected_type->ToString());
        }
        expected_storage_type = extension_type->storage_type();
    }
    if (array->type_id() == arrow::Type::EXTENSION) {
        const auto extension_array = std::dynamic_pointer_cast<arrow::ExtensionArray>(array);
        if (extension_array == nullptr) {
            return Status::InvalidArgument("invalid Arrow extension array: {}",
                                           array->type()->ToString());
        }
        storage_array = extension_array->storage();
    }

    const auto& child_data = storage_array->data()->child_data;
    const auto& child_fields = expected_storage_type->fields();
    if (child_data.empty()) {
        *unwrapped = std::move(storage_array);
        return Status::OK();
    }
    if (child_fields.size() != child_data.size()) {
        return Status::InvalidArgument(
                "Arrow array type {} has {} child fields but its data has {} children",
                storage_array->type()->ToString(), child_fields.size(), child_data.size());
    }

    std::shared_ptr<arrow::ArrayData> unwrapped_data;
    arrow::FieldVector unwrapped_fields;
    for (size_t child_idx = 0; child_idx < child_data.size(); ++child_idx) {
        if (!type_contains_registered_extension(child_fields[child_idx]->type())) {
            continue;
        }
        auto child_array = arrow::MakeArray(child_data[child_idx]);
        std::shared_ptr<arrow::Array> unwrapped_child;
        RETURN_IF_ERROR(unwrap_lance_extension_arrays(child_fields[child_idx]->type(), child_array,
                                                      &unwrapped_child));
        if (unwrapped_child.get() == child_array.get()) {
            continue;
        }
        if (unwrapped_data == nullptr) {
            unwrapped_data = storage_array->data()->Copy();
            unwrapped_fields = storage_array->type()->fields();
        }
        unwrapped_data->child_data[child_idx] = unwrapped_child->data();
        unwrapped_fields[child_idx] =
                unwrapped_fields[child_idx]->WithType(unwrapped_child->type());
    }
    if (unwrapped_data == nullptr) {
        *unwrapped = std::move(storage_array);
        return Status::OK();
    }
    RETURN_IF_ERROR(
            set_lance_nested_type("", storage_array->type(), unwrapped_fields, &unwrapped_data));
    *unwrapped = arrow::MakeArray(std::move(unwrapped_data));
    return Status::OK();
}

// Materialize the visible range into an offset-zero Arrow array for Doris SerDes.
Status compact_lance_array(const std::shared_ptr<arrow::Array>& array,
                           std::shared_ptr<arrow::Array>* compacted) {
    const auto validation = array->ValidateFull();
    if (!validation.ok()) {
        return Status::InvalidArgument("validate sliced Lance array failed: {}",
                                       validation.message());
    }
    auto builder_result = arrow::MakeBuilder(array->type(), arrow::default_memory_pool());
    if (!builder_result.ok()) {
        return Status::InternalError("create sliced Lance array builder failed: {}",
                                     builder_result.status().message());
    }
    auto builder = std::move(builder_result).ValueUnsafe();
    auto arrow_status = builder->Reserve(array->length());
    if (!arrow_status.ok()) {
        return Status::InternalError("reserve sliced Lance array builder failed: {}",
                                     arrow_status.message());
    }
    arrow_status = builder->AppendArraySlice(*array->data(), 0, array->length());
    if (!arrow_status.ok()) {
        return Status::InternalError("copy sliced Lance array failed: {}", arrow_status.message());
    }
    arrow_status = builder->Finish(compacted);
    if (!arrow_status.ok()) {
        return Status::InternalError("finish sliced Lance array copy failed: {}",
                                     arrow_status.message());
    }
    if ((*compacted)->offset() != 0) {
        return Status::InternalError("compacted Lance array retained offset {}",
                                     (*compacted)->offset());
    }
    return Status::OK();
}

// Compact a sliced variable-offset parent and all of its visible descendants.
template <typename ArrayType>
Status compact_lance_offset_array(const std::shared_ptr<arrow::Array>& array,
                                  std::shared_ptr<arrow::Array>* compacted) {
    const auto offset_array = std::dynamic_pointer_cast<ArrayType>(array);
    if (offset_array == nullptr) {
        return Status::InvalidArgument("invalid sliced Lance offset array: {}",
                                       array->type()->ToString());
    }
    const auto child_begin = static_cast<int64_t>(offset_array->value_offset(0));
    const auto child_end = static_cast<int64_t>(offset_array->value_offset(offset_array->length()));
    const auto& values = offset_array->values();
    if (child_begin < 0 || child_end < child_begin || child_end > values->length()) {
        return Status::InvalidArgument("invalid sliced Lance offsets [{}, {}) for child length {}",
                                       child_begin, child_end, values->length());
    }
    if (array->offset() == 0 && child_begin == 0 && child_end == values->length()) {
        *compacted = array;
        return Status::OK();
    }
    return compact_lance_array(array, compacted);
}

// Compact sliced arrays and nested children only when Doris cannot consume their current layout.
Status compact_lance_array_if_needed(const std::shared_ptr<arrow::Array>& array,
                                     std::shared_ptr<arrow::Array>* compacted) {
    switch (array->type_id()) {
    case arrow::Type::LIST:
        return compact_lance_offset_array<arrow::ListArray>(array, compacted);
    case arrow::Type::LARGE_LIST:
        return compact_lance_offset_array<arrow::LargeListArray>(array, compacted);
    case arrow::Type::MAP:
        return compact_lance_offset_array<arrow::MapArray>(array, compacted);
    case arrow::Type::FIXED_SIZE_LIST: {
        const auto list = std::dynamic_pointer_cast<arrow::FixedSizeListArray>(array);
        if (list == nullptr) {
            return Status::InvalidArgument("invalid sliced Lance fixed-size list array: {}",
                                           array->type()->ToString());
        }
        const auto child_begin = list->value_offset(0);
        const auto child_length = list->length() * list->value_length();
        const auto& values = list->values();
        if (child_begin < 0 || child_length < 0 || child_begin > values->length() - child_length) {
            return Status::InvalidArgument(
                    "invalid sliced Lance fixed-size list range [{}, {}) for child length {}",
                    child_begin, child_begin + child_length, values->length());
        }
        if (array->offset() == 0 && child_begin == 0 && child_length == values->length()) {
            *compacted = array;
            return Status::OK();
        }
        return compact_lance_array(array, compacted);
    }
    case arrow::Type::STRUCT: {
        const auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
        if (struct_array == nullptr) {
            return Status::InvalidArgument("invalid sliced Lance struct array: {}",
                                           array->type()->ToString());
        }
        bool requires_compaction = array->offset() != 0;
        for (const auto& child : array->data()->child_data) {
            requires_compaction |= child->length != array->length();
        }
        if (!requires_compaction) {
            *compacted = array;
            return Status::OK();
        }
        return compact_lance_array(array, compacted);
    }
    default:
        if (array->offset() == 0) {
            *compacted = array;
            return Status::OK();
        }
        return compact_lance_array(array, compacted);
    }
}

} // namespace

// Normalize Lance extensions and materialize sliced arrays for Doris Arrow SerDes.
Status normalize_lance_arrow_array(const std::shared_ptr<arrow::Field>& field,
                                   const std::shared_ptr<arrow::Array>& array,
                                   std::shared_ptr<arrow::Array>* normalized) {
    DORIS_CHECK(field != nullptr);
    DORIS_CHECK(array != nullptr);
    DORIS_CHECK(normalized != nullptr);

    LanceExtensionKind extension_kind;
    std::shared_ptr<arrow::DataType> storage_type;
    RETURN_IF_ERROR(get_lance_extension(field, &extension_kind, &storage_type));

    auto storage_array = array;
    if (type_contains_registered_extension(field->type())) {
        std::shared_ptr<arrow::Array> unwrapped_array;
        RETURN_IF_ERROR(unwrap_lance_extension_arrays(field->type(), array, &unwrapped_array));
        storage_array = std::move(unwrapped_array);
    }
    if (storage_array->type_id() != storage_type->id()) {
        return Status::InvalidArgument(
                "Lance field '{}' storage type {} does not match array type {}", field->name(),
                storage_type->ToString(), storage_array->type()->ToString());
    }
    if (extension_kind == LanceExtensionKind::BFLOAT16) {
        return convert_bfloat16_array(storage_array, normalized);
    }

    std::shared_ptr<arrow::Array> compacted_array;
    RETURN_IF_ERROR(compact_lance_array_if_needed(storage_array, &compacted_array));
    storage_array = std::move(compacted_array);

    const auto& child_fields = storage_type->fields();
    const auto& child_data = storage_array->data()->child_data;
    if (child_fields.empty()) {
        *normalized = std::move(storage_array);
        return Status::OK();
    }
    if (child_fields.size() != child_data.size()) {
        return Status::InvalidArgument(
                "Lance field '{}' has {} child fields but its Arrow array has {} children",
                field->name(), child_fields.size(), child_data.size());
    }

    bool requires_normalization = false;
    for (const auto& child_field : child_fields) {
        bool child_required = false;
        RETURN_IF_ERROR(field_requires_lance_normalization(child_field, &child_required));
        if (child_required) {
            requires_normalization = true;
            break;
        }
    }
    if (!requires_normalization) {
        *normalized = std::move(storage_array);
        return Status::OK();
    }

    arrow::FieldVector normalized_fields;
    std::shared_ptr<arrow::ArrayData> normalized_data;
    for (size_t child_idx = 0; child_idx < child_fields.size(); ++child_idx) {
        bool child_required = false;
        RETURN_IF_ERROR(
                field_requires_lance_normalization(child_fields[child_idx], &child_required));
        if (!child_required) {
            continue;
        }
        auto child_array = arrow::MakeArray(storage_array->data()->child_data[child_idx]);
        std::shared_ptr<arrow::Array> normalized_child;
        RETURN_IF_ERROR(normalize_lance_arrow_array(child_fields[child_idx], child_array,
                                                    &normalized_child));
        if (normalized_child.get() == child_array.get()) {
            continue;
        }
        if (normalized_data == nullptr) {
            normalized_data = storage_array->data()->Copy();
            normalized_fields = storage_array->type()->fields();
        }
        normalized_data->child_data[child_idx] = normalized_child->data();
        normalized_fields[child_idx] =
                normalized_fields[child_idx]->WithType(normalized_child->type());
    }
    if (normalized_data == nullptr) {
        *normalized = std::move(storage_array);
        return Status::OK();
    }

    RETURN_IF_ERROR(set_lance_nested_type(field->name(), storage_array->type(), normalized_fields,
                                          &normalized_data));
    *normalized = arrow::MakeArray(std::move(normalized_data));
    return Status::OK();
}

#ifdef BE_TEST
// Expose Lance Arrow normalization for allocation-sensitive unit tests.
Status normalize_lance_arrow_array_for_test(const std::shared_ptr<arrow::Field>& field,
                                            const std::shared_ptr<arrow::Array>& array,
                                            std::shared_ptr<arrow::Array>* normalized) {
    return normalize_lance_arrow_array(field, array, normalized);
}
#endif

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

Status parse_fragment_ids(const TLanceFileDesc& lance_params, std::vector<uint64_t>* fragment_ids) {
    DORIS_CHECK(fragment_ids != nullptr);
    fragment_ids->clear();
    if (!lance_params.__isset.fragment_ids || lance_params.fragment_ids.empty()) {
        return Status::OK();
    }
    fragment_ids->reserve(lance_params.fragment_ids.size());
    for (const auto fragment_id : lance_params.fragment_ids) {
        if (fragment_id < 0) {
            return Status::InvalidArgument("Lance fragment id must be non-negative: {}",
                                           fragment_id);
        }
        fragment_ids->emplace_back(static_cast<uint64_t>(fragment_id));
    }
    return Status::OK();
}

Status parse_index_segment_uuids(const TLanceFileDesc& lance_params,
                                 std::vector<uint8_t>* segment_uuids, size_t* segment_count) {
    DORIS_CHECK(segment_uuids != nullptr);
    DORIS_CHECK(segment_count != nullptr);
    segment_uuids->clear();
    *segment_count = 0;
    if (!lance_params.__isset.index_segment_uuids || lance_params.index_segment_uuids.empty()) {
        return Status::OK();
    }
    constexpr size_t UUID_SIZE = 16;
    if (lance_params.index_segment_uuids.size() > std::numeric_limits<size_t>::max() / UUID_SIZE) {
        return Status::InvalidArgument("too many Lance index segment UUIDs");
    }
    segment_uuids->reserve(lance_params.index_segment_uuids.size() * UUID_SIZE);
    for (const auto& uuid : lance_params.index_segment_uuids) {
        if (uuid.size() != UUID_SIZE) {
            return Status::InvalidArgument("Lance index segment UUID must contain 16 bytes, got {}",
                                           uuid.size());
        }
        segment_uuids->insert(segment_uuids->end(), uuid.begin(), uuid.end());
    }
    *segment_count = lance_params.index_segment_uuids.size();
    return Status::OK();
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
        const auto type_status = arrow_field_to_doris_type(field, &doris_type, true);
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
