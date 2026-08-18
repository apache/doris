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

#include "format/table/iceberg/iceberg_arrow_write_converter.h"

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/extension_type.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <array>
#include <cstring>
#include <limits>
#include <span>

#include "common/cast_set.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type_serde/arrow_validation.h"

namespace doris::iceberg {
#include "common/compile_check_begin.h"
namespace {

constexpr const char* ORIGINAL_TYPE_KEY = "originalType";
constexpr const char* UUID_TYPE_VALUE = "uuid";

bool is_uuid_field(const std::shared_ptr<arrow::Field>& field) {
    if (!field->HasMetadata()) {
        return false;
    }
    const auto value = field->metadata()->Get(ORIGINAL_TYPE_KEY);
    return value.ok() && value.ValueUnsafe() == UUID_TYPE_VALUE;
}

int hex_value(char value) {
    if (value >= '0' && value <= '9') {
        return value - '0';
    }
    if (value >= 'a' && value <= 'f') {
        return value - 'a' + 10;
    }
    if (value >= 'A' && value <= 'F') {
        return value - 'A' + 10;
    }
    return -1;
}

Status parse_uuid(StringRef uuid, std::array<uint8_t, 16>* bytes) {
    if (uuid.size == bytes->size()) {
        std::memcpy(bytes->data(), uuid.data, bytes->size());
        return Status::OK();
    }
    if (uuid.size != 32 && uuid.size != 36) {
        return Status::InvalidArgument("Invalid Iceberg UUID length: {}", uuid.size);
    }

    int digits = 0;
    int high_nibble = -1;
    int byte_index = 0;
    for (size_t index = 0; index < uuid.size; ++index) {
        const char value = uuid.data[index];
        if (uuid.size == 36 && (index == 8 || index == 13 || index == 18 || index == 23)) {
            if (value != '-') {
                return Status::InvalidArgument("Invalid Iceberg UUID format");
            }
            continue;
        }
        const int hex = hex_value(value);
        if (hex < 0) {
            return Status::InvalidArgument("Invalid Iceberg UUID format");
        }
        if (digits % 2 == 0) {
            high_nibble = hex;
        } else {
            (*bytes)[byte_index++] = static_cast<uint8_t>((high_nibble << 4) | hex);
        }
        ++digits;
    }
    if (digits != 32 || byte_index != 16) {
        return Status::InvalidArgument("Invalid Iceberg UUID format");
    }
    return Status::OK();
}

Status write_uuid(const IColumn& column, const NullMap* null_map,
                  arrow::ArrayBuilder* array_builder, int64_t start, int64_t end) {
    auto& builder = assert_cast<arrow::FixedSizeBinaryBuilder&>(*array_builder);
    const int byte_width =
            assert_cast<const arrow::FixedSizeBinaryType&>(*builder.type()).byte_width();
    if (byte_width != 16) {
        return Status::InvalidArgument("Iceberg UUID expects 16 bytes, got {}", byte_width);
    }
    const auto& strings = assert_cast<const ColumnString&>(column);
    for (int64_t row = start; row < end; ++row) {
        if (null_map != nullptr && (*null_map)[row]) {
            RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column, builder));
            continue;
        }
        std::array<uint8_t, 16> bytes;
        RETURN_IF_ERROR(parse_uuid(strings.get_data_at(row), &bytes));
        RETURN_IF_ERROR(checkArrowStatus(builder.Append(bytes.data()), column, builder));
    }
    return Status::OK();
}

std::span<const NullMap::value_type> nulls(const NullMap* null_map) {
    return null_map == nullptr
                   ? std::span<const NullMap::value_type> {}
                   : std::span<const NullMap::value_type> {null_map->data(), null_map->size()};
}

Status write_variant(const IColumn& column, const NullMap* null_map,
                     arrow::ArrayBuilder* array_builder, int64_t start, int64_t end) {
    if (start < 0 || end < start) {
        return Status::InvalidArgument("Invalid Iceberg Variant row range [{}, {})", start, end);
    }
    if (array_builder->type()->id() != arrow::Type::STRUCT) {
        return Status::InvalidArgument("Iceberg Variant writer requires a struct builder, got {}",
                                       array_builder->type()->ToString());
    }
    auto& builder = assert_cast<arrow::StructBuilder&>(*array_builder);
    const auto builder_type = builder.type();
    const auto& type = assert_cast<const arrow::StructType&>(*builder_type);
    if (type.num_fields() != 2 || type.field(0)->name() != "metadata" ||
        type.field(1)->name() != "value" || type.field(0)->type()->id() != arrow::Type::BINARY ||
        type.field(1)->type()->id() != arrow::Type::BINARY) {
        return Status::InvalidArgument(
                "Iceberg Variant writer requires struct<metadata: binary, value: binary>, got {}",
                type.ToString());
    }
    auto& metadata_builder = assert_cast<arrow::BinaryBuilder&>(*builder.field_builder(0));
    auto& value_builder = assert_cast<arrow::BinaryBuilder&>(*builder.field_builder(1));
    Status status = Status::OK();
    visit_variant_v2_values(
            column, start, end, nulls(null_map),
            [&](size_t) {
                if (status.ok()) {
                    status = checkArrowStatus(builder.AppendNull(), column, builder);
                }
            },
            [&](size_t, VariantRef value) {
                if (!status.ok()) {
                    return;
                }
                if (value.metadata.size > std::numeric_limits<int32_t>::max() ||
                    value.value.size > std::numeric_limits<int32_t>::max()) {
                    status = Status::InvalidArgument(
                            "Iceberg Variant metadata/value exceeds Arrow binary size limit");
                    return;
                }
                status = checkArrowStatus(builder.Append(), column, builder);
                if (status.ok()) {
                    status = checkArrowStatus(
                            metadata_builder.Append(value.metadata.data,
                                                    cast_set<int32_t>(value.metadata.size)),
                            column, metadata_builder);
                }
                if (status.ok()) {
                    status = checkArrowStatus(
                            value_builder.Append(value.value.data,
                                                 cast_set<int32_t>(value.value.size)),
                            column, value_builder);
                }
            });
    return status;
}

arrow::Type::type storage_type_id(const std::shared_ptr<arrow::Field>& field) {
    if (field->type()->id() != arrow::Type::EXTENSION) {
        return field->type()->id();
    }
    return assert_cast<const arrow::ExtensionType&>(*field->type()).storage_type()->id();
}

Status validate_nested_binding(const DataTypePtr& type,
                               const std::shared_ptr<arrow::Field>& field) {
    const PrimitiveType primitive = type->get_primitive_type();
    arrow::Type::type expected;
    switch (primitive) {
    case TYPE_ARRAY:
        expected = arrow::Type::LIST;
        break;
    case TYPE_MAP:
        expected = arrow::Type::MAP;
        break;
    case TYPE_STRUCT:
        expected = arrow::Type::STRUCT;
        break;
    default:
        return Status::InvalidArgument("Doris type {} is not a nested Arrow type",
                                       static_cast<int>(primitive));
    }
    if (storage_type_id(field) != expected) {
        return Status::InvalidArgument(
                "Iceberg Arrow writer has no binding for Doris type {} and {}",
                static_cast<int>(primitive), field->ToString());
    }
    if (primitive == TYPE_STRUCT) {
        const auto& doris_struct = assert_cast<const DataTypeStruct&>(*type);
        const auto storage_type =
                field->type()->id() == arrow::Type::EXTENSION
                        ? assert_cast<const arrow::ExtensionType&>(*field->type()).storage_type()
                        : field->type();
        if (storage_type->num_fields() != static_cast<int>(doris_struct.get_elements().size())) {
            return Status::InvalidArgument(
                    "Iceberg Arrow struct field count does not match Doris type {}",
                    type->get_name());
        }
    }
    return Status::OK();
}

} // namespace

Status IcebergArrowWriteConverter::write_column(const std::shared_ptr<const IDataType>& type,
                                                const DataTypeSerDe& serde, const IColumn& column,
                                                const NullMap* null_map,
                                                const std::shared_ptr<arrow::Field>& field,
                                                arrow::ArrayBuilder* array_builder, int64_t start,
                                                int64_t end, const cctz::time_zone& ctz) const {
    if (type->is_nullable()) {
        return write_type_serde_column(type, serde, column, null_map, field, array_builder, start,
                                       end, ctz);
    }
    const PrimitiveType primitive = type->get_primitive_type();
    if (primitive == TYPE_VARIANT) {
        return write_variant(column, null_map, array_builder, start, end);
    }
    if (is_uuid_field(field)) {
        if (!is_string_type(primitive) ||
            storage_type_id(field) != arrow::Type::FIXED_SIZE_BINARY) {
            return Status::InvalidArgument(
                    "Iceberg UUID writer is not bound for Doris type {} and Arrow field {}",
                    type->get_name(), field->ToString());
        }
        return write_uuid(column, null_map, array_builder, start, end);
    }
    if (primitive == TYPE_ARRAY || primitive == TYPE_MAP || primitive == TYPE_STRUCT) {
        RETURN_IF_ERROR(validate_nested_binding(type, field));
        return write_type_serde_column(type, serde, column, null_map, field, array_builder, start,
                                       end, ctz);
    }
    // Iceberg explicitly declares the remaining primitive mappings as canonical after UUID and
    // Variant have been selected above. A canonical mismatch is an error, never a fallback.
    return write_canonical_column(type, serde, column, null_map, field, array_builder, start, end,
                                  ctz);
}

const IcebergArrowWriteConverter& iceberg_arrow_write_converter() {
    static const IcebergArrowWriteConverter converter;
    return converter;
}

#include "common/compile_check_end.h"
} // namespace doris::iceberg
