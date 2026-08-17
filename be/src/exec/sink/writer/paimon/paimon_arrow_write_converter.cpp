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

#include "exec/sink/writer/paimon/paimon_arrow_write_converter.h"

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/type.h>

#include <limits>
#include <span>

#include "common/cast_set.h"
#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_agg_state.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "format/arrow/arrow_row_batch.h"
#include "format/arrow/arrow_utils.h"

namespace doris {
#include "common/compile_check_begin.h"
namespace {

constexpr size_t PAIMON_VARIANT_SIZE_LIMIT = 128 * 1024 * 1024;

std::span<const NullMap::value_type> nulls(const NullMap* null_map) {
    return null_map == nullptr
                   ? std::span<const NullMap::value_type> {}
                   : std::span<const NullMap::value_type> {null_map->data(), null_map->size()};
}

void validate_variant_primitive(VariantPrimitiveId primitive_id) {
    switch (primitive_id) {
    case VariantPrimitiveId::NULL_VALUE:
    case VariantPrimitiveId::TRUE_VALUE:
    case VariantPrimitiveId::FALSE_VALUE:
    case VariantPrimitiveId::INT8:
    case VariantPrimitiveId::INT16:
    case VariantPrimitiveId::INT32:
    case VariantPrimitiveId::INT64:
    case VariantPrimitiveId::DOUBLE:
    case VariantPrimitiveId::DECIMAL4:
    case VariantPrimitiveId::DECIMAL8:
    case VariantPrimitiveId::DECIMAL16:
    case VariantPrimitiveId::DATE:
    case VariantPrimitiveId::TIMESTAMP_MICROS:
    case VariantPrimitiveId::TIMESTAMP_NTZ_MICROS:
    case VariantPrimitiveId::FLOAT:
    case VariantPrimitiveId::BINARY:
    case VariantPrimitiveId::STRING:
    case VariantPrimitiveId::UUID:
        return;
    case VariantPrimitiveId::TIME_NTZ_MICROS:
    case VariantPrimitiveId::TIMESTAMP_NANOS:
    case VariantPrimitiveId::TIMESTAMP_NTZ_NANOS:
        throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                        "Paimon does not support Variant primitive id {}",
                        static_cast<uint8_t>(primitive_id));
    }
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                    "Paimon does not support unknown Variant primitive id {}",
                    static_cast<uint8_t>(primitive_id));
}

void validate_variant_value(VariantRef value, uint32_t depth = 0) {
    if (depth > VARIANT_MAX_NESTING_DEPTH) {
        throw Exception(ErrorCode::CORRUPTION, "Variant value exceeds maximum nesting depth {}",
                        VARIANT_MAX_NESTING_DEPTH);
    }
    const size_t encoded_size = value.value_size();
    if (encoded_size != value.value.size) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant value has {} trailing bytes after the encoded value",
                        value.value.size - encoded_size);
    }

    switch (value.basic_type()) {
    case VariantBasicType::PRIMITIVE:
        validate_variant_primitive(value.primitive_id());
        return;
    case VariantBasicType::SHORT_STRING:
        return;
    case VariantBasicType::OBJECT:
        for (uint32_t index = 0; index < value.num_elements(); ++index) {
            uint32_t field_id = 0;
            const VariantRef child = value.object_value_at(index, &field_id);
            value.metadata.key_at(field_id);
            validate_variant_value(child, depth + 1);
        }
        return;
    case VariantBasicType::ARRAY:
        for (uint32_t index = 0; index < value.num_elements(); ++index) {
            validate_variant_value(value.array_at(index), depth + 1);
        }
        return;
    }
}

Status write_variant(const IColumn& column, const NullMap* null_map,
                     arrow::ArrayBuilder* array_builder, int64_t start, int64_t end) {
    if (start < 0 || end < start) {
        return Status::InvalidArgument("Invalid Paimon Variant row range [{}, {})", start, end);
    }
    if (array_builder->type()->id() != arrow::Type::STRUCT) {
        return Status::InvalidArgument("Paimon Variant writer requires a struct builder, got {}",
                                       array_builder->type()->ToString());
    }
    auto& builder = assert_cast<arrow::StructBuilder&>(*array_builder);
    const auto builder_type = builder.type();
    const auto& type = assert_cast<const arrow::StructType&>(*builder_type);
    if (type.num_fields() != 2 || type.field(0)->name() != "value" ||
        type.field(1)->name() != "metadata" || type.field(0)->type()->id() != arrow::Type::BINARY ||
        type.field(1)->type()->id() != arrow::Type::BINARY) {
        return Status::InvalidArgument(
                "Paimon Variant writer requires struct<value: binary, metadata: binary>, got {}",
                type.ToString());
    }
    auto& value_builder = assert_cast<arrow::BinaryBuilder&>(*builder.field_builder(0));
    auto& metadata_builder = assert_cast<arrow::BinaryBuilder&>(*builder.field_builder(1));
    Status status = Status::OK();
    visit_variant_v2_values(
            column, start, end, nulls(null_map),
            [&](size_t) {
                if (status.ok()) {
                    status = to_doris_status(builder.AppendNull());
                }
            },
            [&](size_t row, VariantRef value) {
                if (!status.ok()) {
                    return;
                }
                try {
                    if (value.value.size > PAIMON_VARIANT_SIZE_LIMIT ||
                        value.metadata.size > PAIMON_VARIANT_SIZE_LIMIT) {
                        throw Exception(ErrorCode::INVALID_ARGUMENT,
                                        "exceeds the 128 MiB value/metadata limit");
                    }
                    value.metadata.validate();
                    validate_variant_value(value);
                } catch (const Exception& e) {
                    status = Status::Error<false>(e.code(),
                                                  "Paimon Variant V2 row {} is incompatible: {}",
                                                  row, e.what());
                    return;
                }
                status = to_doris_status(builder.Append());
                if (status.ok()) {
                    status = to_doris_status(value_builder.Append(
                            reinterpret_cast<const uint8_t*>(value.value.data),
                            cast_set<int32_t, size_t, false>(value.value.size)));
                }
                if (status.ok()) {
                    status = to_doris_status(metadata_builder.Append(
                            reinterpret_cast<const uint8_t*>(value.metadata.data),
                            cast_set<int32_t, size_t, false>(value.metadata.size)));
                }
            });
    return status;
}

Status convert_to_paimon_arrow_type(const DataTypePtr& origin_type,
                                    std::shared_ptr<arrow::DataType>* result,
                                    const std::string& timezone) {
    const DataTypePtr type = get_serialized_type(origin_type);
    switch (type->get_primitive_type()) {
    case TYPE_VARIANT:
        *result = arrow::struct_({arrow::field("value", arrow::binary(), false),
                                  arrow::field("metadata", arrow::binary(), false)});
        return Status::OK();
    case TYPE_ARRAY: {
        const auto& array_type = assert_cast<const DataTypeArray&>(*remove_nullable(type));
        std::shared_ptr<arrow::DataType> element_type;
        RETURN_IF_ERROR(convert_to_paimon_arrow_type(array_type.get_nested_type(), &element_type,
                                                     timezone));
        *result = std::make_shared<arrow::ListType>(element_type);
        return Status::OK();
    }
    case TYPE_MAP: {
        const auto& map_type = assert_cast<const DataTypeMap&>(*remove_nullable(type));
        std::shared_ptr<arrow::DataType> key_type;
        std::shared_ptr<arrow::DataType> value_type;
        RETURN_IF_ERROR(convert_to_paimon_arrow_type(map_type.get_key_type(), &key_type, timezone));
        RETURN_IF_ERROR(
                convert_to_paimon_arrow_type(map_type.get_value_type(), &value_type, timezone));
        *result = std::make_shared<arrow::MapType>(key_type, value_type);
        return Status::OK();
    }
    case TYPE_STRUCT: {
        const auto& struct_type = assert_cast<const DataTypeStruct&>(*remove_nullable(type));
        std::vector<std::shared_ptr<arrow::Field>> fields;
        fields.reserve(struct_type.get_elements().size());
        for (size_t index = 0; index < struct_type.get_elements().size(); ++index) {
            const DataTypePtr& element = struct_type.get_element(index);
            std::shared_ptr<arrow::DataType> field_type;
            RETURN_IF_ERROR(convert_to_paimon_arrow_type(element, &field_type, timezone));
            fields.push_back(arrow::field(struct_type.get_element_name(index), field_type,
                                          element->is_nullable()));
        }
        *result = arrow::struct_(std::move(fields));
        return Status::OK();
    }
    default:
        return convert_to_arrow_type(origin_type, result, timezone);
    }
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
        return Status::InvalidArgument("Doris type {} is not a nested Paimon Arrow type",
                                       static_cast<int>(primitive));
    }
    if (field->type()->id() != expected) {
        return Status::InvalidArgument(
                "Paimon Arrow writer has no binding for Doris type {} and Arrow field {}",
                static_cast<int>(primitive), field->ToString());
    }
    if (primitive == TYPE_STRUCT) {
        const auto& doris_struct = assert_cast<const DataTypeStruct&>(*type);
        if (field->type()->num_fields() != static_cast<int>(doris_struct.get_elements().size())) {
            return Status::InvalidArgument(
                    "Paimon Arrow struct field count does not match Doris type {}",
                    type->get_name());
        }
    }
    return Status::OK();
}

Status validate_timestamp_binding(const DataTypePtr& type,
                                  const std::shared_ptr<arrow::Field>& field) {
    if (field->type()->id() != arrow::Type::TIMESTAMP) {
        return Status::InvalidArgument(
                "Paimon timestamp writer has no binding for Doris type {} and Arrow field {}",
                type->get_name(), field->ToString());
    }
    const auto& timestamp = assert_cast<const arrow::TimestampType&>(*field->type());
    const arrow::TimeUnit::type expected_unit = type->get_scale() > 3   ? arrow::TimeUnit::MICRO
                                                : type->get_scale() > 0 ? arrow::TimeUnit::MILLI
                                                                        : arrow::TimeUnit::SECOND;
    if (timestamp.unit() != expected_unit || !timestamp.timezone().empty()) {
        return Status::InvalidArgument(
                "Paimon timestamp writer has no binding for Doris type {} and Arrow field {}",
                type->get_name(), field->ToString());
    }
    return Status::OK();
}

} // namespace

Status PaimonArrowWriteConverter::write_column(const std::shared_ptr<const IDataType>& type,
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
    if (primitive == TYPE_ARRAY || primitive == TYPE_MAP || primitive == TYPE_STRUCT) {
        RETURN_IF_ERROR(validate_nested_binding(type, field));
        return write_type_serde_column(type, serde, column, null_map, field, array_builder, start,
                                       end, ctz);
    }
    if (primitive == TYPE_DATETIMEV2 || primitive == TYPE_TIMESTAMPTZ) {
        RETURN_IF_ERROR(validate_timestamp_binding(type, field));
        return write_type_serde_column(type, serde, column, null_map, field, array_builder, start,
                                       end, ctz);
    }
    // Paimon Arrow protocol v1 explicitly declares every remaining primitive as the canonical
    // Doris Arrow representation. write_canonical_column validates that declaration first.
    return write_canonical_column(type, serde, column, null_map, field, array_builder, start, end,
                                  ctz);
}

const PaimonArrowWriteConverter& paimon_arrow_write_converter() {
    static const PaimonArrowWriteConverter converter;
    return converter;
}

Status get_paimon_arrow_schema_from_block(const Block& block,
                                          std::shared_ptr<arrow::Schema>* result) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(block.columns());
    for (const auto& type_and_name : block) {
        std::shared_ptr<arrow::DataType> arrow_type;
        RETURN_IF_ERROR(convert_to_paimon_arrow_type(type_and_name.type, &arrow_type, ""));
        fields.push_back(create_arrow_field_with_metadata(
                type_and_name.name, arrow_type, type_and_name.type->is_nullable(),
                type_and_name.type->get_primitive_type()));
    }
    *result = arrow::schema(std::move(fields));
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
