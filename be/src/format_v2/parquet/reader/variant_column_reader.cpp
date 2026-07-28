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

#include "format_v2/parquet/reader/variant_column_reader.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string_view>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_decimal.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/column/variant_v2/column_variant_v2_typed_column.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_metadata.h"
#include "format_v2/parquet/parquet_column_schema.h"

namespace doris::format::parquet {
namespace {

struct Cell {
    const IColumn* column = nullptr;
    bool is_null = false;
};

Cell cell_at(const IColumn& column, size_t row) {
    if (row >= column.size()) {
        throw Exception(ErrorCode::CORRUPTION, "Parquet Variant row {} exceeds column size {}", row,
                        column.size());
    }
    if (const auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        return {.column = &nullable->get_nested_column(),
                .is_null = nullable->get_null_map_data()[row] != 0};
    }
    return {.column = &column, .is_null = false};
}

const ParquetColumnSchema* find_child(const ParquetColumnSchema& schema, std::string_view name,
                                      size_t* index) {
    for (size_t i = 0; i < schema.children.size(); ++i) {
        if (schema.children[i]->name == name) {
            if (index != nullptr) {
                *index = i;
            }
            return schema.children[i].get();
        }
    }
    return nullptr;
}

Cell struct_child_at(const ParquetColumnSchema& schema, const IColumn& physical, size_t row,
                     std::string_view name, const ParquetColumnSchema** child_schema) {
    const auto& structure = assert_cast<const ColumnStruct&>(physical);
    size_t index = 0;
    const auto* child = find_child(schema, name, &index);
    if (child == nullptr || index >= structure.tuple_size()) {
        throw Exception(ErrorCode::CORRUPTION, "Parquet Variant {} has no physical child {}",
                        schema.name, name);
    }
    if (child_schema != nullptr) {
        *child_schema = child;
    }
    return cell_at(structure.get_column(index), row);
}

uint8_t decimal_width(int precision) {
    if (precision <= 0 || precision > 38) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Parquet Variant decimal precision {} is outside [1, 38]", precision);
    }
    return precision <= 9 ? 4 : (precision <= 18 ? 8 : 16);
}

uint8_t integer_width(const ParquetColumnSchema& schema, PrimitiveType type) {
    if (schema.type_descriptor.is_unsigned_integer) {
        throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                        "Unsigned integers are not valid Parquet Variant typed values");
    }
    if (schema.type_descriptor.integer_bit_width > 0) {
        switch (schema.type_descriptor.integer_bit_width) {
        case 8:
            return 1;
        case 16:
            return 2;
        case 32:
            return 4;
        case 64:
            return 8;
        default:
            throw Exception(ErrorCode::CORRUPTION, "Invalid Parquet Variant integer width {}",
                            schema.type_descriptor.integer_bit_width);
        }
    }
    switch (type) {
    case TYPE_TINYINT:
        return 1;
    case TYPE_SMALLINT:
        return 2;
    case TYPE_INT:
        return 4;
    case TYPE_BIGINT:
        return 8;
    default:
        throw Exception(ErrorCode::CORRUPTION, "Invalid Parquet Variant integer type {}", type);
    }
}

void append_typed_scalar(const ParquetColumnSchema& schema, const IColumn& column, size_t row,
                         VariantBatchBuilder::Row& builder) {
    const PrimitiveType type = remove_nullable(schema.type)->get_primitive_type();
    switch (type) {
    case TYPE_BOOLEAN:
        builder.add_bool(assert_cast<const ColumnUInt8&>(column).get_data()[row] != 0);
        return;
    case TYPE_TINYINT:
        builder.add_scalar(
                VariantScalarRef::integer(assert_cast<const ColumnInt8&>(column).get_data()[row],
                                          integer_width(schema, type)));
        return;
    case TYPE_SMALLINT:
        builder.add_scalar(
                VariantScalarRef::integer(assert_cast<const ColumnInt16&>(column).get_data()[row],
                                          integer_width(schema, type)));
        return;
    case TYPE_INT:
        builder.add_scalar(
                VariantScalarRef::integer(assert_cast<const ColumnInt32&>(column).get_data()[row],
                                          integer_width(schema, type)));
        return;
    case TYPE_BIGINT:
        builder.add_scalar(
                VariantScalarRef::integer(assert_cast<const ColumnInt64&>(column).get_data()[row],
                                          integer_width(schema, type)));
        return;
    case TYPE_FLOAT:
        builder.add_float(assert_cast<const ColumnFloat32&>(column).get_data()[row]);
        return;
    case TYPE_DOUBLE:
        builder.add_double(assert_cast<const ColumnFloat64&>(column).get_data()[row]);
        return;
    case TYPE_DECIMAL128I: {
        const auto value = assert_cast<const ColumnDecimal128V3&>(column).get_data()[row].value;
        builder.add_decimal(value, static_cast<uint8_t>(schema.type_descriptor.decimal_scale),
                            decimal_width(schema.type_descriptor.decimal_precision));
        return;
    }
    case TYPE_TIMEV2: {
        const double seconds = assert_cast<const ColumnTimeV2&>(column).get_data()[row];
        if (!std::isfinite(seconds) ||
            std::abs(seconds) > static_cast<double>(std::numeric_limits<int64_t>::max()) / 1e6) {
            throw Exception(ErrorCode::CORRUPTION, "Invalid Parquet Variant TIME value");
        }
        builder.add_time_ntz_micros(static_cast<int64_t>(std::llround(seconds * 1e6)));
        return;
    }
    case TYPE_DATETIMEV2: {
        if (schema.type_descriptor.time_unit == ParquetTimeUnit::NANOS) {
            // Native DATETIMEV2 is microsecond based. Reject before returning a silently truncated
            // value; a raw INT64 nanos decoder can be added independently.
            throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                            "Parquet Variant TIMESTAMP(NANOS) is not supported");
        }
        const auto& value = assert_cast<const ColumnDateTimeV2&>(column).get_data()[row];
        builder.add_timestamp_micros(
                variant_timestamp_micros(value, row, "Parquet Variant TIMESTAMP"),
                schema.type_descriptor.timestamp_is_adjusted_to_utc);
        return;
    }
    case TYPE_TIMESTAMPTZ: {
        if (schema.type_descriptor.time_unit == ParquetTimeUnit::NANOS) {
            throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                            "Parquet Variant TIMESTAMP(NANOS) is not supported");
        }
        const auto& value = assert_cast<const ColumnTimeStampTz&>(column).get_data()[row];
        builder.add_timestamp_micros(
                variant_timestamp_micros(value, row, "Parquet Variant TIMESTAMP"), true);
        return;
    }
    case TYPE_VARBINARY: {
        const StringRef value = column.get_data_at(row);
        if (!schema.type_descriptor.is_uuid) {
            builder.add_binary(value);
            return;
        }
        if (value.size != 16) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant UUID has {} bytes instead of 16", value.size);
        }
        std::array<uint8_t, 16> uuid {};
        std::memcpy(uuid.data(), value.data, uuid.size());
        builder.add_uuid(uuid);
        return;
    }
    case TYPE_STRING: {
        const StringRef value = column.get_data_at(row);
        if (schema.type_descriptor.is_uuid) {
            if (value.size != 16) {
                throw Exception(ErrorCode::CORRUPTION,
                                "Parquet Variant UUID has {} bytes instead of 16", value.size);
            }
            std::array<uint8_t, 16> uuid {};
            std::memcpy(uuid.data(), value.data, uuid.size());
            builder.add_uuid(uuid);
        } else if (schema.type_descriptor.is_string_annotation) {
            builder.add_string(value);
        } else {
            builder.add_binary(value);
        }
        return;
    }
    default:
        if (!is_supported_variant_typed_identity(type)) {
            throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                            "Parquet Variant typed value {} is not supported",
                            remove_nullable(schema.type)->get_name());
        }
        dispatch_variant_typed_column(
                column, type, [&]<PrimitiveType Type>(const auto& typed_column) {
                    with_variant_typed_scalar<Type>(
                            typed_column, row,
                            static_cast<uint8_t>(remove_nullable(schema.type)->get_scale()),
                            [&](const VariantScalarRef& scalar) { builder.add_scalar(scalar); });
                });
    }
}

enum class WrapperContext { ROOT, ARRAY_ELEMENT, OBJECT_FIELD };

bool append_wrapper(const ParquetColumnSchema& schema, const IColumn& wrapper, size_t row,
                    VariantMetadataRef metadata, VariantBatchBuilder::Row& builder,
                    WrapperContext context);

void append_typed_value(const ParquetColumnSchema& schema, const IColumn& column, size_t row,
                        VariantMetadataRef metadata, const VariantRef* residual,
                        VariantBatchBuilder::Row& builder) {
    switch (schema.kind) {
    case ParquetColumnSchemaKind::PRIMITIVE:
        if (residual != nullptr) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant scalar typed_value cannot have residual value bytes");
        }
        append_typed_scalar(schema, column, row, builder);
        return;
    case ParquetColumnSchemaKind::STRUCT: {
        if (residual != nullptr && residual->basic_type() != VariantBasicType::OBJECT) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant object typed_value has non-object residual value");
        }
        const auto& structure = assert_cast<const ColumnStruct&>(column);
        if (structure.tuple_size() != schema.children.size()) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant object {} physical field count mismatch", schema.name);
        }
        auto object = builder.start_object();
        if (residual != nullptr) {
            for (uint32_t i = 0; i < residual->num_elements(); ++i) {
                uint32_t field_id = 0;
                const VariantRef child = residual->object_value_at(i, &field_id);
                object.add_key(residual->metadata.key_at(field_id));
                builder.add_value(child);
            }
        }
        for (size_t i = 0; i < schema.children.size(); ++i) {
            const auto& child_schema = *schema.children[i];
            const Cell child = cell_at(structure.get_column(i), row);
            if (child.is_null) {
                // Shredded object fields are optional wrapper groups. A missing group means the
                // key is absent, which differs from a present wrapper encoding a Variant null.
                continue;
            }
            // A null/null wrapper means this object field is absent. Delay add_key until its
            // presence is known so absent shredded fields do not turn into Variant nulls.
            size_t value_index = 0;
            const auto* value_schema = find_child(child_schema, "value", &value_index);
            const auto& child_struct = assert_cast<const ColumnStruct&>(*child.column);
            const bool value_present = value_schema != nullptr &&
                                       !cell_at(child_struct.get_column(value_index), row).is_null;
            size_t typed_index = 0;
            const auto* typed_schema = find_child(child_schema, "typed_value", &typed_index);
            const bool typed_present = typed_schema != nullptr &&
                                       !cell_at(child_struct.get_column(typed_index), row).is_null;
            if (!value_present && !typed_present) {
                continue;
            }
            object.add_key(StringRef(child_schema.name));
            (void)append_wrapper(child_schema, *child.column, row, metadata, builder,
                                 WrapperContext::OBJECT_FIELD);
        }
        object.finish();
        return;
    }
    case ParquetColumnSchemaKind::LIST: {
        if (residual != nullptr) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant array typed_value cannot have residual value bytes");
        }
        if (schema.children.size() != 1) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant array {} has invalid element schema", schema.name);
        }
        const auto& array = assert_cast<const ColumnArray&>(column);
        const size_t begin = array.offset_at(static_cast<ssize_t>(row));
        const size_t end = array.get_offsets()[row];
        auto scope = builder.start_array();
        for (size_t element = begin; element < end; ++element) {
            const Cell cell = cell_at(array.get_data(), element);
            if (cell.is_null) {
                throw Exception(ErrorCode::CORRUPTION,
                                "Parquet Variant shredded array element wrapper is null");
            }
            (void)append_wrapper(*schema.children[0], *cell.column, element, metadata, builder,
                                 WrapperContext::ARRAY_ELEMENT);
        }
        scope.finish();
        return;
    }
    case ParquetColumnSchemaKind::MAP:
    case ParquetColumnSchemaKind::VARIANT:
        throw Exception(ErrorCode::CORRUPTION, "Invalid Parquet Variant typed_value schema {}",
                        schema.name);
    }
}

bool append_wrapper(const ParquetColumnSchema& schema, const IColumn& wrapper, size_t row,
                    VariantMetadataRef metadata, VariantBatchBuilder::Row& builder,
                    WrapperContext context) {
    Cell value;
    if (find_child(schema, "value", nullptr) != nullptr) {
        value = struct_child_at(schema, wrapper, row, "value", nullptr);
    } else {
        value.is_null = true;
    }
    const ParquetColumnSchema* typed_schema = nullptr;
    Cell typed;
    if (find_child(schema, "typed_value", nullptr) != nullptr) {
        typed = struct_child_at(schema, wrapper, row, "typed_value", &typed_schema);
    } else {
        typed.is_null = true;
    }

    if (find_child(schema, "value", nullptr) == nullptr && typed_schema == nullptr) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Parquet Variant wrapper {} has neither value nor typed_value",
                        schema.name);
    }
    if (value.is_null && typed.is_null) {
        if (context == WrapperContext::OBJECT_FIELD) {
            return false;
        }
        if (context == WrapperContext::ARRAY_ELEMENT) {
            throw Exception(ErrorCode::CORRUPTION, "Parquet Variant array element is missing");
        }
        builder.add_null();
        return true;
    }

    VariantRef residual {.metadata = metadata, .value = {}};
    if (!value.is_null) {
        residual.value = value.column->get_data_at(row);
    }
    if (typed.is_null) {
        builder.add_value(residual);
        return true;
    }
    append_typed_value(*typed_schema, *typed.column, row, metadata,
                       value.is_null ? nullptr : &residual, builder);
    return true;
}

void encode_variant_range(const ParquetColumnSchema& schema, const IColumn& wrapper,
                          const ColumnNullable* outer_nullable, size_t begin, size_t end,
                          ColumnVariantV2& variants) {
    try {
        VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = end - begin});
        for (size_t row = begin; row < end; ++row) {
            auto output_row = builder.begin_row();
            if (outer_nullable != nullptr && outer_nullable->get_null_map_data()[row] != 0) {
                output_row.add_null();
                output_row.finish();
                continue;
            }
            const Cell metadata_cell = struct_child_at(schema, wrapper, row, "metadata", nullptr);
            if (metadata_cell.is_null) {
                throw Exception(ErrorCode::CORRUPTION,
                                "Parquet Variant {} has null metadata at row {}", schema.name, row);
            }
            const StringRef metadata_bytes = metadata_cell.column->get_data_at(row);
            const VariantMetadataRef metadata {metadata_bytes.data, metadata_bytes.size};
            metadata.validate();
            (void)append_wrapper(schema, wrapper, row, metadata, output_row, WrapperContext::ROOT);
            output_row.finish();
        }
        VariantBatchBuilder batch = builder.finish_batch();
        variants.insert_encoded_batch(batch);
    } catch (...) {
        if (end - begin <= 1) {
            throw;
        }
        // A single builder has one metadata dictionary. If heterogeneous file rows cannot fit in
        // that dictionary, split without changing the destination column's already-valid batches.
        // Corrupt input still reaches a one-row range and propagates its original exception.
        const size_t middle = begin + (end - begin) / 2;
        encode_variant_range(schema, wrapper, outer_nullable, begin, middle, variants);
        encode_variant_range(schema, wrapper, outer_nullable, middle, end, variants);
    }
}

MutableColumnPtr build_variant_column(const ParquetColumnSchema& schema, const IColumn& physical) {
    if (schema.kind != ParquetColumnSchemaKind::VARIANT) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Parquet column {} is not Variant",
                        schema.name);
    }
    const auto* outer_nullable = check_and_get_column<ColumnNullable>(physical);
    const IColumn& wrapper =
            outer_nullable == nullptr ? physical : outer_nullable->get_nested_column();
    const auto& structure = assert_cast<const ColumnStruct&>(wrapper);
    if (structure.tuple_size() != schema.children.size()) {
        throw Exception(ErrorCode::CORRUPTION, "Parquet Variant {} physical field count mismatch",
                        schema.name);
    }

    auto variants = ColumnVariantV2::create();
    constexpr size_t MAX_RECONSTRUCTION_BATCH_ROWS = 4096;
    for (size_t begin = 0; begin < physical.size(); begin += MAX_RECONSTRUCTION_BATCH_ROWS) {
        encode_variant_range(schema, wrapper, outer_nullable, begin,
                             std::min(physical.size(), begin + MAX_RECONSTRUCTION_BATCH_ROWS),
                             *variants);
    }
    if (outer_nullable == nullptr) {
        return variants;
    }
    auto nulls = outer_nullable->get_null_map_column().clone_resized(physical.size());
    return ColumnNullable::create(std::move(variants), std::move(nulls));
}

MutableColumnPtr transform_node(const VariantMaterializationNode& plan, const IColumn& physical);

MutableColumnPtr transform_non_nullable(const VariantMaterializationNode& plan,
                                        const IColumn& physical) {
    const auto& schema = *plan.schema;
    switch (schema.kind) {
    case ParquetColumnSchemaKind::PRIMITIVE:
        return physical.clone_resized(physical.size());
    case ParquetColumnSchemaKind::VARIANT:
        return build_variant_column(schema, physical);
    case ParquetColumnSchemaKind::STRUCT: {
        const auto& structure = assert_cast<const ColumnStruct&>(physical);
        if (structure.tuple_size() != plan.children.size()) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Projected Parquet STRUCT {} field count mismatch", schema.name);
        }
        MutableColumns fields;
        fields.reserve(plan.children.size());
        for (size_t i = 0; i < plan.children.size(); ++i) {
            fields.push_back(transform_node(*plan.children[i], structure.get_column(i)));
        }
        return ColumnStruct::create(std::move(fields));
    }
    case ParquetColumnSchemaKind::LIST: {
        const auto& array = assert_cast<const ColumnArray&>(physical);
        if (plan.children.size() != 1) {
            throw Exception(ErrorCode::CORRUPTION, "Projected Parquet ARRAY plan is invalid");
        }
        auto values = transform_node(*plan.children[0], array.get_data());
        auto offsets = array.get_offsets_column().clone_resized(array.size());
        return ColumnArray::create(std::move(values), std::move(offsets));
    }
    case ParquetColumnSchemaKind::MAP: {
        const auto& map = assert_cast<const ColumnMap&>(physical);
        if (plan.children.size() != 2) {
            throw Exception(ErrorCode::CORRUPTION, "Projected Parquet MAP plan is invalid");
        }
        auto keys = transform_node(*plan.children[0], map.get_keys());
        auto values = transform_node(*plan.children[1], map.get_values());
        auto offsets = map.get_offsets_ptr()->clone_resized(map.size());
        return ColumnMap::create(std::move(keys), std::move(values), std::move(offsets));
    }
    }
    throw Exception(ErrorCode::INTERNAL_ERROR, "Unknown Parquet schema kind");
}

MutableColumnPtr transform_node(const VariantMaterializationNode& plan, const IColumn& physical) {
    if (plan.schema == nullptr) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "Parquet Variant materialization plan is null");
    }
    if (plan.schema->kind == ParquetColumnSchemaKind::VARIANT) {
        return build_variant_column(*plan.schema, physical);
    }
    if (const auto* nullable = check_and_get_column<ColumnNullable>(physical)) {
        auto nested = transform_non_nullable(plan, nullable->get_nested_column());
        auto nulls = nullable->get_null_map_column().clone_resized(physical.size());
        return ColumnNullable::create(std::move(nested), std::move(nulls));
    }
    return transform_non_nullable(plan, physical);
}

void append_compatible_column(IColumn& output, const IColumn& converted) {
    if (auto* output_nullable = check_and_get_column<ColumnNullable>(output)) {
        if (const auto* converted_nullable = check_and_get_column<ColumnNullable>(converted)) {
            append_compatible_column(output_nullable->get_nested_column(),
                                     converted_nullable->get_nested_column());
            output_nullable->get_null_map_column().insert_range_from(
                    converted_nullable->get_null_map_column(), 0, converted.size());
        } else {
            append_compatible_column(output_nullable->get_nested_column(), converted);
            // External slots and nested Iceberg fields may remain nullable even when one file's
            // physical node is required. Preserve that destination invariant with non-null bits.
            output_nullable->push_false_to_nullmap(converted.size());
        }
        return;
    }

    if (const auto* converted_nullable = check_and_get_column<ColumnNullable>(converted)) {
        // Parquet writers may encode an Iceberg required field as optional. It can populate a
        // non-nullable destination only when this batch proves that every value is present.
        if (converted_nullable->has_null()) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced null data for a "
                            "non-nullable destination");
        }
        append_compatible_column(output, converted_nullable->get_nested_column());
        return;
    }

    if (auto* output_struct = check_and_get_column<ColumnStruct>(output)) {
        const auto* converted_struct = check_and_get_column<ColumnStruct>(converted);
        if (converted_struct == nullptr ||
            output_struct->tuple_size() != converted_struct->tuple_size()) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced an incompatible STRUCT");
        }
        for (size_t i = 0; i < output_struct->tuple_size(); ++i) {
            append_compatible_column(output_struct->get_column(i), converted_struct->get_column(i));
        }
        return;
    }

    if (auto* output_array = check_and_get_column<ColumnArray>(output)) {
        const auto* converted_array = check_and_get_column<ColumnArray>(converted);
        if (converted_array == nullptr) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced an incompatible ARRAY");
        }
        const size_t element_base = output_array->get_data().size();
        append_compatible_column(output_array->get_data(), converted_array->get_data());
        auto& output_offsets = output_array->get_offsets();
        output_offsets.reserve(output_offsets.size() + converted_array->size());
        for (const auto offset : converted_array->get_offsets()) {
            output_offsets.push_back(element_base + offset);
        }
        return;
    }

    if (auto* output_map = check_and_get_column<ColumnMap>(output)) {
        const auto* converted_map = check_and_get_column<ColumnMap>(converted);
        if (converted_map == nullptr) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced an incompatible MAP");
        }
        const size_t element_base = output_map->get_keys().size();
        append_compatible_column(output_map->get_keys(), converted_map->get_keys());
        append_compatible_column(output_map->get_values(), converted_map->get_values());
        auto& output_offsets = output_map->get_offsets();
        output_offsets.reserve(output_offsets.size() + converted_map->size());
        for (const auto offset : converted_map->get_offsets()) {
            output_offsets.push_back(element_base + offset);
        }
        return;
    }

    if (output.get_name() != converted.get_name()) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Parquet Variant materialization produced {} for {} destination",
                        converted.get_name(), output.get_name());
    }
    output.insert_range_from(converted, 0, converted.size());
}

void append_materialized_column(MutableColumnPtr& output, MutableColumnPtr converted) {
    // First align into an empty destination-shaped column. A recursive mismatch can then fail
    // without leaving a partially appended external scan block behind.
    auto compatible = output->clone_empty();
    append_compatible_column(*compatible, *converted);
    output->insert_range_from(*compatible, 0, compatible->size());
}

} // namespace

Status materialize_variant_rows(const ParquetColumnSchema& schema, const IColumn& physical,
                                MutableColumnPtr& output) {
    if (!output) {
        return Status::InvalidArgument("Parquet Variant output column is null");
    }
    RETURN_IF_CATCH_EXCEPTION({
        auto converted = build_variant_column(schema, physical);
        append_materialized_column(output, std::move(converted));
    });
    return Status::OK();
}

Status materialize_variant_columns(const VariantMaterializationNode& plan, const IColumn& physical,
                                   MutableColumnPtr& output) {
    if (!output) {
        return Status::InvalidArgument("Parquet Variant output column is null");
    }
    RETURN_IF_CATCH_EXCEPTION({
        auto converted = transform_node(plan, physical);
        append_materialized_column(output, std::move(converted));
    });
    return Status::OK();
}

} // namespace doris::format::parquet
