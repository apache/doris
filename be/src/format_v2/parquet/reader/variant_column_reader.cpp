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
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_decimal.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/column/variant_v2/column_variant_v2_typed_column.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_metadata.h"
#include "core/value/variant/variant_scalar.h"
#include "format_v2/parquet/parquet_column_schema.h"

namespace doris::format::parquet {
namespace {

struct Cell {
    const IColumn* column = nullptr;
    bool is_null = false;
};

constexpr std::array<char, 1> VARIANT_NULL_VALUE {static_cast<char>(
        static_cast<uint8_t>(VariantPrimitiveId::NULL_VALUE) << VARIANT_VALUE_HEADER_SHIFT)};

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
        if (static_cast<bool>(residual)) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant scalar typed_value cannot have residual value bytes");
        }
        append_typed_scalar(schema, column, row, builder);
        return;
    case ParquetColumnSchemaKind::STRUCT: {
        if (static_cast<bool>(residual) && residual->basic_type() != VariantBasicType::OBJECT) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant object typed_value has non-object residual value");
        }
        const auto& structure = assert_cast<const ColumnStruct&>(column);
        if (structure.tuple_size() != schema.children.size()) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant object {} physical field count mismatch", schema.name);
        }
        auto object = builder.start_object();
        if (static_cast<bool>(residual)) {
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
        if (static_cast<bool>(residual)) {
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
                          bool require_metadata, ColumnVariantV2& variants) {
    try {
        VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = end - begin});
        for (size_t row = begin; row < end; ++row) {
            auto output_row = builder.begin_row();
            if (outer_nullable != nullptr && outer_nullable->get_null_map_data()[row] != 0) {
                output_row.add_null();
                output_row.finish();
                continue;
            }
            VariantMetadataRef metadata;
            if (find_child(schema, "metadata", nullptr) != nullptr) {
                const Cell metadata_cell =
                        struct_child_at(schema, wrapper, row, "metadata", nullptr);
                if (metadata_cell.is_null) {
                    throw Exception(ErrorCode::CORRUPTION,
                                    "Parquet Variant {} has null metadata at row {}", schema.name,
                                    row);
                }
                const StringRef metadata_bytes = metadata_cell.column->get_data_at(row);
                metadata = {metadata_bytes.data, metadata_bytes.size};
                metadata.validate();
            } else if (require_metadata) {
                throw Exception(ErrorCode::CORRUPTION, "Parquet Variant {} has no root metadata",
                                schema.name);
            }
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
        encode_variant_range(schema, wrapper, outer_nullable, begin, middle, require_metadata,
                             variants);
        encode_variant_range(schema, wrapper, outer_nullable, middle, end, require_metadata,
                             variants);
    }
}

std::optional<std::pair<size_t, size_t>> unshredded_child_indices(
        const ParquetColumnSchema& schema) {
    if (schema.children.size() != 2 || find_child(schema, "typed_value", nullptr) != nullptr) {
        return std::nullopt;
    }
    size_t metadata_index = 0;
    size_t value_index = 0;
    if (find_child(schema, "metadata", &metadata_index) == nullptr ||
        find_child(schema, "value", &value_index) == nullptr) {
        return std::nullopt;
    }
    return std::pair {metadata_index, value_index};
}

void import_unshredded_variant_range(const ParquetColumnSchema& schema, const ColumnStruct& wrapper,
                                     const ColumnNullable* outer_nullable, size_t metadata_index,
                                     size_t value_index, size_t begin, size_t end,
                                     DorisVector<VariantRef>& encoded_rows,
                                     ColumnVariantV2::EncodedRowsAppender& appender,
                                     int64_t* imported_bytes) {
    encoded_rows.clear();
    if (encoded_rows.capacity() < end - begin) {
        encoded_rows.reserve(end - begin);
    }
    if (imported_bytes != nullptr) {
        *imported_bytes = 0;
    }
    auto count_imported_bytes = [&](size_t bytes) {
        if (imported_bytes == nullptr) {
            return;
        }
        DORIS_CHECK_LE(bytes,
                       static_cast<size_t>(std::numeric_limits<int64_t>::max() - *imported_bytes));
        *imported_bytes += static_cast<int64_t>(bytes);
    };
    for (size_t row = begin; row < end; ++row) {
        if (outer_nullable != nullptr && outer_nullable->get_null_map_data()[row] != 0) {
            encoded_rows.push_back(
                    {.metadata = {.data = VARIANT_EMPTY_METADATA.data(),
                                  .size = VARIANT_EMPTY_METADATA.size()},
                     .value = {VARIANT_NULL_VALUE.data(), VARIANT_NULL_VALUE.size()}});
            count_imported_bytes(VARIANT_EMPTY_METADATA.size());
            count_imported_bytes(VARIANT_NULL_VALUE.size());
            continue;
        }

        const Cell metadata = cell_at(wrapper.get_column(metadata_index), row);
        if (metadata.is_null) {
            throw Exception(ErrorCode::CORRUPTION, "Parquet Variant {} has null metadata at row {}",
                            schema.name, row);
        }
        const StringRef metadata_bytes = metadata.column->get_data_at(row);
        const Cell value = cell_at(wrapper.get_column(value_index), row);
        const StringRef value_bytes =
                value.is_null ? StringRef(VARIANT_NULL_VALUE.data(), VARIANT_NULL_VALUE.size())
                              : value.column->get_data_at(row);
        encoded_rows.push_back(
                {.metadata = {.data = metadata_bytes.data, .size = metadata_bytes.size},
                 .value = value_bytes});
        count_imported_bytes(metadata_bytes.size);
        count_imported_bytes(value_bytes.size);
    }
    appender.append(std::span<const VariantRef>(encoded_rows));
}

ColumnVariantV2::MutablePtr encode_variant_column(
        const ParquetColumnSchema& schema, const IColumn& physical, bool require_metadata = true,
        const ParquetColumnReaderProfile* profile = nullptr) {
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
    // A complete unshredded root already contains exact Variant Encoding V1 bytes. Keep the
    // existing lazy materialization boundary, but validate and copy those bytes without rebuilding
    // the value tree through VariantBatchBuilder.
    const auto unshredded_indices =
            require_metadata ? unshredded_child_indices(schema) : std::nullopt;
    constexpr size_t MAX_MATERIALIZATION_BATCH_ROWS = 4096;
    DorisVector<VariantRef> encoded_rows;
    std::optional<ColumnVariantV2::EncodedRowsAppender> encoded_rows_appender;
    if (unshredded_indices.has_value()) {
        encoded_rows.reserve(std::min(physical.size(), MAX_MATERIALIZATION_BATCH_ROWS));
        encoded_rows_appender.emplace(variants->create_encoded_rows_appender());
    }
    for (size_t begin = 0; begin < physical.size(); begin += MAX_MATERIALIZATION_BATCH_ROWS) {
        const size_t end = std::min(physical.size(), begin + MAX_MATERIALIZATION_BATCH_ROWS);
        if (unshredded_indices.has_value()) {
            DORIS_CHECK(encoded_rows_appender.has_value());
            int64_t imported_bytes = 0;
            RuntimeProfile::Counter* direct_import_time =
                    profile == nullptr ? nullptr
                                       : profile->variant_unshredded_direct_import_time.get();
            {
                // Update the shared timer on every exit, including validation failures. Rows and
                // bytes are published only after the complete chunk has been appended.
                SCOPED_TIMER(direct_import_time);
                import_unshredded_variant_range(
                        schema, structure, outer_nullable, unshredded_indices->first,
                        unshredded_indices->second, begin, end, encoded_rows,
                        *encoded_rows_appender, profile == nullptr ? nullptr : &imported_bytes);
            }
            if (profile != nullptr) {
                const auto imported_rows = static_cast<int64_t>(end - begin);
                if (profile->variant_unshredded_direct_import_rows != nullptr) {
                    COUNTER_UPDATE(profile->variant_unshredded_direct_import_rows.get(),
                                   imported_rows);
                }
                if (profile->variant_unshredded_direct_import_bytes != nullptr) {
                    COUNTER_UPDATE(profile->variant_unshredded_direct_import_bytes.get(),
                                   imported_bytes);
                }
            }
        } else {
            encode_variant_range(schema, wrapper, outer_nullable, begin, end, require_metadata,
                                 *variants);
        }
    }
    return variants;
}

std::unique_ptr<ParquetColumnSchema> clone_schema(
        const ParquetColumnSchema& source, const format::LocalColumnIndex* projection = nullptr) {
    auto result = std::make_unique<ParquetColumnSchema>();
    result->local_id = source.local_id;
    result->parquet_field_id = source.parquet_field_id;
    result->name = source.name;
    result->type = source.type;
    result->variant_physical_type = source.variant_physical_type;
    result->leaf_column_id = source.leaf_column_id;
    result->type_descriptor = source.type_descriptor;
    result->kind = source.kind;
    result->contains_variant = source.contains_variant;
    result->max_definition_level = source.max_definition_level;
    result->max_repetition_level = source.max_repetition_level;
    result->nullable_definition_level = source.nullable_definition_level;
    result->definition_level = source.definition_level;
    result->repetition_level = source.repetition_level;
    result->repeated_ancestor_definition_level = source.repeated_ancestor_definition_level;
    result->repeated_repetition_level = source.repeated_repetition_level;
    const bool partial = format::is_partial_projection(projection);
    result->children.reserve(partial ? projection->children.size() : source.children.size());
    if (partial) {
        // NativeColumnReader emits a partial STRUCT in projection order, so the retained schema
        // must use that same order or field names will address the wrong physical tuple element.
        for (const auto& child_projection : projection->children) {
            const auto child = std::ranges::find_if(source.children, [&](const auto& candidate) {
                return candidate->local_id == child_projection.local_id();
            });
            DORIS_CHECK(child != source.children.end());
            result->children.push_back(clone_schema(**child, &child_projection));
        }
    } else {
        for (const auto& child : source.children) {
            result->children.push_back(clone_schema(*child));
        }
    }
    return result;
}

ColumnPtr unwrap_nullable(ColumnPtr column) {
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
        return nullable->get_nested_column_ptr();
    }
    return column;
}

ColumnPtr struct_child(const ParquetColumnSchema& schema, ColumnPtr column, std::string_view name,
                       const ParquetColumnSchema** child_schema) {
    column = unwrap_nullable(std::move(column));
    const auto* structure = check_and_get_column<ColumnStruct>(*column);
    if (structure == nullptr) {
        return nullptr;
    }
    size_t index = 0;
    const auto* child = find_child(schema, name, &index);
    if (child == nullptr || index >= structure->tuple_size()) {
        return nullptr;
    }
    if (child_schema != nullptr) {
        *child_schema = child;
    }
    return structure->get_column_ptr(index);
}

bool has_present_value(const ColumnPtr& column) {
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
        return std::ranges::any_of(nullable->get_null_map_data(),
                                   [](uint8_t is_null) { return is_null == 0; });
    }
    return !column->empty();
}

bool supports_direct_typed_variant_state(const ParquetColumnSchema& schema) {
    if (schema.type == nullptr || schema.kind != ParquetColumnSchemaKind::PRIMITIVE) {
        return false;
    }
    // ColumnVariantV2 typed state carries only a Doris type. Binary/UUID annotations, temporal
    // units, and other Parquet-only identity must therefore reconstruct canonical Variant bytes.
    switch (remove_nullable(schema.type)->get_primitive_type()) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_DECIMAL128I:
    case TYPE_DATEV2:
        return true;
    default:
        return false;
    }
}

ColumnPtr normalize_projected_primitive_leaf(const ParquetColumnSchema& schema,
                                             const ColumnPtr& typed) {
    const auto& nullable = assert_cast<const ColumnNullable&>(*typed);
    VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = nullable.size()});
    for (size_t row = 0; row < nullable.size(); ++row) {
        auto output_row = builder.begin_row();
        if (nullable.get_null_map_data()[row] != 0) {
            output_row.add_null();
        } else {
            append_typed_scalar(schema, nullable.get_nested_column(), row, output_row);
        }
        output_row.finish();
    }
    auto values = ColumnVariantV2::create();
    values->insert_encoded_batch(builder.finish_batch());
    auto nulls = nullable.get_null_map_column().clone_resized(nullable.size());
    return ColumnNullable::create(std::move(values), std::move(nulls));
}

struct UnshreddedPathCacheEntry {
    static constexpr uint32_t MISSING = std::numeric_limits<uint32_t>::max();

    uint32_t value_offset = MISSING;
    uint32_t value_size = 0;

    bool present() const noexcept { return value_offset != MISSING; }
};

struct UnshreddedPathCache {
    DorisVector<UnshreddedPathCacheEntry> entries;

    size_t byte_size() const noexcept { return entries.size() * sizeof(UnshreddedPathCacheEntry); }
    size_t allocated_bytes() const noexcept {
        return entries.capacity() * sizeof(UnshreddedPathCacheEntry);
    }
};

struct UnshreddedMetadataIndex {
    static constexpr uint32_t NULL_ROW = std::numeric_limits<uint32_t>::max();

    const IColumn* physical_identity = nullptr;
    DorisVector<VariantMetadataRef> dictionaries;
    DorisVector<uint32_t> row_dictionary_ids;

    size_t byte_size() const noexcept {
        return dictionaries.size() * sizeof(VariantMetadataRef) +
               row_dictionary_ids.size() * sizeof(uint32_t);
    }
    size_t allocated_bytes() const noexcept {
        return dictionaries.capacity() * sizeof(VariantMetadataRef) +
               row_dictionary_ids.capacity() * sizeof(uint32_t);
    }
};

std::shared_ptr<const UnshreddedMetadataIndex> build_unshredded_metadata_index(
        const ParquetColumnSchema& schema, const IColumn& physical,
        std::pair<size_t, size_t> child_indices) {
    const auto* outer_nullable = check_and_get_column<ColumnNullable>(physical);
    const IColumn& wrapper =
            outer_nullable == nullptr ? physical : outer_nullable->get_nested_column();
    const auto& structure = assert_cast<const ColumnStruct&>(wrapper);
    DORIS_CHECK_EQ(structure.tuple_size(), schema.children.size());

    auto index = std::make_shared<UnshreddedMetadataIndex>();
    index->physical_identity = &physical;
    index->row_dictionary_ids.resize(physical.size(), UnshreddedMetadataIndex::NULL_ROW);
    using MetadataIdMap =
            std::unordered_map<std::string_view, uint32_t, std::hash<std::string_view>,
                               std::equal_to<std::string_view>,
                               CustomStdAllocator<std::pair<const std::string_view, uint32_t>>>;
    MetadataIdMap dictionary_ids;
    for (size_t row = 0; row < physical.size(); ++row) {
        if (outer_nullable != nullptr && outer_nullable->get_null_map_data()[row] != 0) {
            continue;
        }
        const Cell metadata = cell_at(structure.get_column(child_indices.first), row);
        if (metadata.is_null) {
            throw Exception(ErrorCode::CORRUPTION, "Parquet Variant {} has null metadata at row {}",
                            schema.name, row);
        }
        const StringRef bytes = metadata.column->get_data_at(row);
        uint32_t dictionary_id = 0;
        if (index->dictionaries.empty()) {
            index->dictionaries.push_back({.data = bytes.data, .size = bytes.size});
        } else if (index->dictionaries.size() == 1 && dictionary_ids.empty() &&
                   StringRef(index->dictionaries.front().data, index->dictionaries.front().size) ==
                           bytes) {
            // Iceberg normally repeats one metadata dictionary throughout a decoded block. Delay
            // the hash table until a second dictionary is actually observed.
        } else {
            if (dictionary_ids.empty()) {
                const VariantMetadataRef first = index->dictionaries.front();
                dictionary_ids.emplace(std::string_view(first.data, first.size), 0);
            }
            const std::string_view key(bytes.data, bytes.size);
            if (const auto found = dictionary_ids.find(key); found != dictionary_ids.end()) {
                dictionary_id = found->second;
            } else {
                dictionary_id = static_cast<uint32_t>(index->dictionaries.size());
                index->dictionaries.push_back({.data = bytes.data, .size = bytes.size});
                dictionary_ids.emplace(key, dictionary_id);
            }
        }
        index->row_dictionary_ids[row] = dictionary_id;
    }
    return index;
}

template <typename ObjectFinder>
bool find_materialized_path_impl(VariantRef current,
                                 std::span<const VariantShreddedPathSegment> path,
                                 const ObjectFinder& find_object, VariantRef* output) {
    DCHECK(output != nullptr);
    for (size_t position = 0; position < path.size(); ++position) {
        const auto& segment = path[position];
        if (segment.kind == VariantShreddedPathSegment::Kind::OBJECT_KEY) {
            if (current.basic_type() != VariantBasicType::OBJECT ||
                !find_object(current, segment.key, position, &current)) {
                return false;
            }
            continue;
        }
        if (current.basic_type() != VariantBasicType::ARRAY) {
            return false;
        }
        const int64_t count = current.num_elements();
        const int64_t index = segment.index < 0 ? count + segment.index : segment.index;
        if (index < 0 || index >= count) {
            return false;
        }
        current = current.array_at(static_cast<uint32_t>(index));
    }
    *output = current;
    return true;
}

bool find_materialized_path(VariantRef current, std::span<const VariantShreddedPathSegment> path,
                            VariantRef* output) {
    return find_materialized_path_impl(
            current, path,
            [](VariantRef object, StringRef key, size_t, VariantRef* found) {
                return object.object_find(key, found);
            },
            output);
}

bool find_materialized_path_with_index(VariantRef current, uint32_t dictionary_id,
                                       const UnshreddedMetadataIndex& metadata_index,
                                       std::span<const VariantShreddedPathSegment> path,
                                       DorisVector<int64_t>& resolved_field_ids,
                                       VariantRef* output) {
    DCHECK_LT(dictionary_id, metadata_index.dictionaries.size());
    DCHECK_EQ(resolved_field_ids.size(), metadata_index.dictionaries.size() * path.size());
    constexpr int64_t UNRESOLVED_FIELD_ID = -2;
    return find_materialized_path_impl(
            current, path,
            [&](VariantRef object, StringRef key, size_t position, VariantRef* found) {
                int64_t& field_id = resolved_field_ids[dictionary_id * path.size() + position];
                bool layout_validated = false;
                if (field_id == UNRESOLVED_FIELD_ID) {
                    // object_find() validates the object layout before consulting metadata.
                    static_cast<void>(object.num_elements());
                    layout_validated = true;
                    field_id = metadata_index.dictionaries[dictionary_id].find_key(key);
                }
                if (field_id < 0) {
                    // A cached metadata miss must not hide a corrupt object in a later row.
                    if (!layout_validated) {
                        static_cast<void>(object.num_elements());
                    }
                    return false;
                }
                return object.object_find_by_id(static_cast<uint32_t>(field_id), found);
            },
            output);
}

struct UnshreddedPathScan {
    MutableColumnPtr outer_nulls;
    MutableColumnPtr typed_values;
    DataTypePtr typed_type;
    std::shared_ptr<const UnshreddedPathCache> path_cache;
    int64_t copied_bytes = 0;
};

enum class UnshreddedTypedKind : uint8_t { UNKNOWN, STRING, INTEGER, UNSUPPORTED };

class UnshreddedTypedValueBuilder {
public:
    UnshreddedTypedValueBuilder(size_t rows, const UnshreddedMetadataIndex& metadata_index)
            : _rows(rows),
              _metadata_index(metadata_index),
              _inner_nulls(ColumnUInt8::create()),
              _result_nulls(ColumnUInt8::create()),
              _validated_metadata(metadata_index.dictionaries.size(), 0) {
        _inner_nulls->reserve(rows);
        _result_nulls->reserve(rows);
    }

    void append_outer_null() { append_null(1); }

    void append_json_null(uint32_t dictionary_id) {
        if (_typed_kind == UnshreddedTypedKind::UNKNOWN) {
            _pending_json_null_dictionaries.push_back(dictionary_id);
        } else if (_typed_kind == UnshreddedTypedKind::INTEGER &&
                   !validate_integer_metadata(dictionary_id)) {
            mark_unsupported();
        }
        append_null(0);
    }

    void append_scalar(const VariantRef& found, uint32_t dictionary_id, size_t row) {
        if (_typed_kind == UnshreddedTypedKind::UNSUPPORTED) {
            append_null(0);
            return;
        }

        const VariantBasicType basic_type = found.basic_type();
        const bool is_string = basic_type == VariantBasicType::SHORT_STRING ||
                               (basic_type == VariantBasicType::PRIMITIVE &&
                                found.primitive_id() == VariantPrimitiveId::STRING);
        if (is_string) {
            if (!prepare(UnshreddedTypedKind::STRING, row)) {
                append_null(0);
                return;
            }
            const StringRef string = found.get_string();
            assert_cast<ColumnString&>(*_typed_values).insert_data(string.data, string.size);
            _inner_nulls->insert_value(0);
            _result_nulls->insert_value(0);
            DCHECK_LE(string.size,
                      static_cast<size_t>(std::numeric_limits<int64_t>::max() - _copied_bytes));
            _copied_bytes += static_cast<int64_t>(string.size);
            return;
        }

        const auto primitive_id = basic_type == VariantBasicType::PRIMITIVE
                                          ? found.primitive_id()
                                          : VariantPrimitiveId::NULL_VALUE;
        const bool is_integer = primitive_id == VariantPrimitiveId::INT8 ||
                                primitive_id == VariantPrimitiveId::INT16 ||
                                primitive_id == VariantPrimitiveId::INT32 ||
                                primitive_id == VariantPrimitiveId::INT64;
        const int64_t integer = is_integer ? found.get_int() : 0;
        // Typed Variant integers are re-encoded using the narrowest width. Keep explicitly widened
        // source integers on the encoded path so observable physical types remain unchanged.
        const bool has_canonical_width =
                is_integer && VariantScalarRef::integer(integer).encoded_size() == found.value.size;
        if (!has_canonical_width || !validate_integer_metadata(dictionary_id) ||
            !prepare(UnshreddedTypedKind::INTEGER, row)) {
            mark_unsupported();
            append_null(0);
            return;
        }
        assert_cast<ColumnInt64&>(*_typed_values).insert_value(integer);
        _inner_nulls->insert_value(0);
        _result_nulls->insert_value(0);
        DCHECK_LE(static_cast<int64_t>(sizeof(int64_t)),
                  std::numeric_limits<int64_t>::max() - _copied_bytes);
        _copied_bytes += sizeof(int64_t);
    }

    UnshreddedPathScan finish(std::shared_ptr<const UnshreddedPathCache> path_cache) && {
        DataTypePtr typed_type;
        if (_typed_values) {
            typed_type = _typed_kind == UnshreddedTypedKind::STRING
                                 ? DataTypePtr(std::make_shared<DataTypeString>())
                                 : DataTypePtr(std::make_shared<DataTypeInt64>());
            _typed_values =
                    ColumnNullable::create(std::move(_typed_values), std::move(_inner_nulls));
        }
        return {.outer_nulls = std::move(_result_nulls),
                .typed_values = std::move(_typed_values),
                .typed_type = std::move(typed_type),
                .path_cache = std::move(path_cache),
                .copied_bytes = _copied_bytes};
    }

private:
    bool validate_integer_metadata(uint32_t dictionary_id) {
        DCHECK_LT(dictionary_id, _metadata_index.dictionaries.size());
        try {
            if (_validated_metadata[dictionary_id] == 0) {
                validate_variant_metadata(_metadata_index.dictionaries[dictionary_id]);
                _validated_metadata[dictionary_id] = 1;
            }
            return true;
        } catch (const Exception&) {
            return false;
        }
    }

    bool prepare(UnshreddedTypedKind kind, size_t row) {
        if (_typed_kind == UnshreddedTypedKind::UNKNOWN) {
            if (kind == UnshreddedTypedKind::INTEGER) {
                for (const uint32_t dictionary_id : _pending_json_null_dictionaries) {
                    if (!validate_integer_metadata(dictionary_id)) {
                        mark_unsupported();
                        return false;
                    }
                }
                _typed_values = ColumnInt64::create();
            } else {
                DCHECK(kind == UnshreddedTypedKind::STRING);
                _typed_values = ColumnString::create();
            }
            _pending_json_null_dictionaries.clear();
            _typed_kind = kind;
            _typed_values->reserve(_rows);
            _typed_values->insert_many_defaults(row);
            return true;
        }
        if (_typed_kind == kind) {
            return true;
        }
        mark_unsupported();
        return false;
    }

    void mark_unsupported() {
        _typed_kind = UnshreddedTypedKind::UNSUPPORTED;
        _typed_values.reset();
        _pending_json_null_dictionaries.clear();
    }

    void append_null(uint8_t outer_null) {
        if (_typed_values) {
            _typed_values->insert_default();
        }
        _inner_nulls->insert_value(1);
        _result_nulls->insert_value(outer_null);
    }

    size_t _rows;
    const UnshreddedMetadataIndex& _metadata_index;
    MutableColumnPtr _typed_values;
    ColumnUInt8::MutablePtr _inner_nulls;
    ColumnUInt8::MutablePtr _result_nulls;
    UnshreddedTypedKind _typed_kind = UnshreddedTypedKind::UNKNOWN;
    int64_t _copied_bytes = 0;
    DorisVector<uint8_t> _validated_metadata;
    DorisVector<uint32_t> _pending_json_null_dictionaries;
};

std::optional<VariantRef> unshredded_root_at(const ColumnNullable* outer_nullable,
                                             const ColumnStruct& structure,
                                             std::pair<size_t, size_t> child_indices,
                                             const UnshreddedMetadataIndex& metadata_index,
                                             size_t row) {
    if (outer_nullable != nullptr && outer_nullable->get_null_map_data()[row] != 0) {
        return std::nullopt;
    }

    const uint32_t dictionary_id = metadata_index.row_dictionary_ids[row];
    DCHECK_LT(dictionary_id, metadata_index.dictionaries.size());
    const Cell value = cell_at(structure.get_column(child_indices.second), row);
    const StringRef value_bytes =
            value.is_null ? StringRef(VARIANT_NULL_VALUE.data(), VARIANT_NULL_VALUE.size())
                          : value.column->get_data_at(row);
    return VariantRef {.metadata = metadata_index.dictionaries[dictionary_id],
                       .value = value_bytes};
}

UnshreddedPathCacheEntry make_unshredded_path_cache_entry(const VariantRef& root,
                                                          const VariantRef& found) {
    const uintptr_t root_begin = reinterpret_cast<uintptr_t>(root.value.data);
    const uintptr_t root_end = root_begin + root.value.size;
    const uintptr_t found_begin = reinterpret_cast<uintptr_t>(found.value.data);
    const uintptr_t found_end = found_begin + found.value.size;
    DCHECK_GE(found_begin, root_begin);
    DCHECK_LE(found_end, root_end);
    const size_t offset = found_begin - root_begin;
    DCHECK_LT(offset, static_cast<size_t>(UnshreddedPathCacheEntry::MISSING));
    DCHECK_LE(found.value.size, static_cast<size_t>(std::numeric_limits<uint32_t>::max()));
    return {.value_offset = static_cast<uint32_t>(offset),
            .value_size = static_cast<uint32_t>(found.value.size)};
}

UnshreddedPathScan scan_unshredded_path(const ParquetColumnSchema& schema, const IColumn& physical,
                                        std::pair<size_t, size_t> child_indices,
                                        const UnshreddedMetadataIndex& metadata_index,
                                        std::span<const VariantShreddedPathSegment> path,
                                        const UnshreddedPathCache* prefix_cache = nullptr) {
    const auto* outer_nullable = check_and_get_column<ColumnNullable>(physical);
    const IColumn& wrapper =
            outer_nullable == nullptr ? physical : outer_nullable->get_nested_column();
    const auto& structure = assert_cast<const ColumnStruct&>(wrapper);
    DORIS_CHECK_EQ(structure.tuple_size(), schema.children.size());

    auto path_cache = std::make_shared<UnshreddedPathCache>();
    path_cache->entries.reserve(physical.size());
    DORIS_CHECK(prefix_cache == nullptr || prefix_cache->entries.size() == physical.size());
    DORIS_CHECK_EQ(metadata_index.physical_identity, &physical);
    DORIS_CHECK_EQ(metadata_index.row_dictionary_ids.size(), physical.size());
    constexpr int64_t UNRESOLVED_FIELD_ID = -2;
    DorisVector<int64_t> resolved_field_ids(metadata_index.dictionaries.size() * path.size(),
                                            UNRESOLVED_FIELD_ID);
    UnshreddedTypedValueBuilder typed_values(physical.size(), metadata_index);

    for (size_t row = 0; row < physical.size(); ++row) {
        const auto root =
                unshredded_root_at(outer_nullable, structure, child_indices, metadata_index, row);
        if (!root.has_value()) {
            DCHECK(prefix_cache == nullptr || !prefix_cache->entries[row].present());
            path_cache->entries.emplace_back();
            typed_values.append_outer_null();
            continue;
        }

        const uint32_t dictionary_id = metadata_index.row_dictionary_ids[row];
        VariantRef current = *root;
        if (prefix_cache != nullptr) {
            const auto& cached = prefix_cache->entries[row];
            if (!cached.present()) {
                path_cache->entries.emplace_back();
                typed_values.append_outer_null();
                continue;
            }
            DCHECK_LE(static_cast<size_t>(cached.value_offset) + cached.value_size,
                      root->value.size);
            current.value = {root->value.data + cached.value_offset, cached.value_size};
        }
        VariantRef found;
        if (!find_materialized_path_with_index(current, dictionary_id, metadata_index, path,
                                               resolved_field_ids, &found)) {
            path_cache->entries.emplace_back();
            typed_values.append_outer_null();
            continue;
        }
        path_cache->entries.push_back(make_unshredded_path_cache_entry(*root, found));
        if (found.is_null()) {
            typed_values.append_json_null(dictionary_id);
        } else {
            typed_values.append_scalar(found, dictionary_id, row);
        }
    }
    return std::move(typed_values).finish(std::move(path_cache));
}

ColumnPtr normalize_unshredded_path(const ParquetColumnSchema& schema, const IColumn& physical,
                                    std::pair<size_t, size_t> child_indices,
                                    std::span<const VariantShreddedPathSegment> path,
                                    int64_t* copied_bytes) {
    DORIS_CHECK(copied_bytes != nullptr);
    const auto* outer_nullable = check_and_get_column<ColumnNullable>(physical);
    const IColumn& wrapper =
            outer_nullable == nullptr ? physical : outer_nullable->get_nested_column();
    const auto& structure = assert_cast<const ColumnStruct&>(wrapper);
    DORIS_CHECK_EQ(structure.tuple_size(), schema.children.size());

    auto values = ColumnVariantV2::create();
    auto appender = values->create_encoded_rows_appender();
    auto nulls = ColumnUInt8::create();
    nulls->reserve(physical.size());
    constexpr size_t MAX_DIRECT_SEEK_BATCH_ROWS = 4096;
    DorisVector<VariantRef> encoded_rows;
    encoded_rows.reserve(std::min(physical.size(), MAX_DIRECT_SEEK_BATCH_ROWS));
    const VariantRef null_value {.metadata = {.data = VARIANT_EMPTY_METADATA.data(),
                                              .size = VARIANT_EMPTY_METADATA.size()},
                                 .value = {VARIANT_NULL_VALUE.data(), VARIANT_NULL_VALUE.size()}};
    int64_t total_copied_bytes = 0;
    auto count_copied_bytes = [&](const VariantRef value) {
        for (const size_t bytes : {value.metadata.size, value.value.size}) {
            DORIS_CHECK_LE(bytes, static_cast<size_t>(std::numeric_limits<int64_t>::max() -
                                                      total_copied_bytes));
            total_copied_bytes += static_cast<int64_t>(bytes);
        }
    };

    for (size_t begin = 0; begin < physical.size(); begin += MAX_DIRECT_SEEK_BATCH_ROWS) {
        const size_t end = std::min(physical.size(), begin + MAX_DIRECT_SEEK_BATCH_ROWS);
        encoded_rows.clear();
        for (size_t row = begin; row < end; ++row) {
            if (outer_nullable != nullptr && outer_nullable->get_null_map_data()[row] != 0) {
                encoded_rows.push_back(null_value);
                nulls->insert_value(1);
                count_copied_bytes(null_value);
                continue;
            }

            const Cell metadata = cell_at(structure.get_column(child_indices.first), row);
            if (metadata.is_null) {
                throw Exception(ErrorCode::CORRUPTION,
                                "Parquet Variant {} has null metadata at row {}", schema.name, row);
            }
            const StringRef metadata_bytes = metadata.column->get_data_at(row);
            const Cell value = cell_at(structure.get_column(child_indices.second), row);
            const StringRef value_bytes =
                    value.is_null ? StringRef(VARIANT_NULL_VALUE.data(), VARIANT_NULL_VALUE.size())
                                  : value.column->get_data_at(row);
            const VariantRef root {
                    .metadata = {.data = metadata_bytes.data, .size = metadata_bytes.size},
                    .value = value_bytes};
            VariantRef found;
            if (find_materialized_path(root, path, &found)) {
                encoded_rows.push_back(found);
                nulls->insert_value(0);
                count_copied_bytes(found);
            } else {
                encoded_rows.push_back(null_value);
                nulls->insert_value(1);
                count_copied_bytes(null_value);
            }
        }
        appender.append(std::span<const VariantRef>(encoded_rows));
    }
    *copied_bytes = total_copied_bytes;
    return ColumnNullable::create(std::move(values), std::move(nulls));
}

ColumnPtr normalize_materialized_path(const ColumnVariantV2& materialized,
                                      std::span<const VariantShreddedPathSegment> path) {
    VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = materialized.size()});
    auto nulls = ColumnUInt8::create();
    nulls->reserve(materialized.size());
    for (size_t row = 0; row < materialized.size(); ++row) {
        auto output_row = builder.begin_row();
        VariantRef value;
        if (find_materialized_path(materialized.get_value_ref(row), path, &value)) {
            output_row.add_value(value);
            nulls->insert_value(0);
        } else {
            output_row.add_null();
            nulls->insert_value(1);
        }
        output_row.finish();
    }
    auto values = ColumnVariantV2::create();
    values->insert_encoded_batch(builder.finish_batch());
    return ColumnNullable::create(std::move(values), std::move(nulls));
}

bool same_data_type(const DataTypePtr& left, const DataTypePtr& right) {
    return (!left && !right) || (left && right && left->equals(*right));
}

bool same_type_descriptor(const ParquetTypeDescriptor& left, const ParquetTypeDescriptor& right) {
    return same_data_type(left.doris_type, right.doris_type) &&
           same_data_type(left.physical_doris_type, right.physical_doris_type) &&
           left.extra_type_info == right.extra_type_info && left.time_unit == right.time_unit &&
           left.physical_type == right.physical_type &&
           left.integer_bit_width == right.integer_bit_width &&
           left.decimal_precision == right.decimal_precision &&
           left.decimal_scale == right.decimal_scale && left.fixed_length == right.fixed_length &&
           left.is_unsigned_integer == right.is_unsigned_integer &&
           left.is_decimal == right.is_decimal && left.is_timestamp == right.is_timestamp &&
           left.timestamp_is_adjusted_to_utc == right.timestamp_is_adjusted_to_utc &&
           left.is_string_like == right.is_string_like &&
           left.is_string_annotation == right.is_string_annotation &&
           left.is_uuid == right.is_uuid && left.unsupported_reason == right.unsupported_reason;
}

bool same_shredded_schema(const ParquetColumnSchema& left, const ParquetColumnSchema& right) {
    if (left.name != right.name || left.kind != right.kind ||
        !same_data_type(left.type, right.type) ||
        !same_type_descriptor(left.type_descriptor, right.type_descriptor) ||
        left.children.size() != right.children.size()) {
        return false;
    }
    for (size_t i = 0; i < left.children.size(); ++i) {
        if (!same_shredded_schema(*left.children[i], *right.children[i])) {
            return false;
        }
    }
    return true;
}

void append_compatible_column(IColumn& output, const IColumn& converted);
void validate_compatible_column(const IColumn& output, const IColumn& converted);

struct OwnedShreddedPathSegment {
    VariantShreddedPathSegment::Kind kind = VariantShreddedPathSegment::Kind::OBJECT_KEY;
    std::string key;
    int64_t index = 0;

    bool operator==(const OwnedShreddedPathSegment&) const = default;
};

class ParquetVariantShreddedState;

struct UnshreddedResultCacheEntry {
    std::vector<OwnedShreddedPathSegment> path;
    ColumnPtr result;
    std::shared_ptr<ParquetVariantShreddedState> subtree_state;

    size_t path_byte_size() const noexcept {
        size_t bytes = path.size() * sizeof(OwnedShreddedPathSegment);
        for (const auto& segment : path) {
            bytes += segment.key.size();
        }
        return bytes;
    }

    size_t path_allocated_bytes() const noexcept {
        size_t bytes = path.capacity() * sizeof(OwnedShreddedPathSegment);
        for (const auto& segment : path) {
            bytes += segment.key.capacity();
        }
        return bytes;
    }
};

class ParquetVariantShreddedState final : public VariantShreddedState {
public:
    ParquetVariantShreddedState(
            std::shared_ptr<const ParquetColumnSchema> schema, ColumnPtr physical, bool complete,
            ParquetColumnReaderProfile profile = {},
            std::vector<OwnedShreddedPathSegment> unshredded_prefix = {},
            std::shared_ptr<const UnshreddedPathCache> unshredded_path_cache = nullptr)
            : _schema(std::move(schema)),
              _physical(std::move(physical)),
              _complete(complete),
              _profile(profile),
              _unshredded_prefix(std::move(unshredded_prefix)),
              _unshredded_path_cache(std::move(unshredded_path_cache)) {
        DORIS_CHECK(_schema != nullptr && static_cast<bool>(_physical));
        const ColumnPtr wrapper = unwrap_nullable(_physical);
        const auto* structure = check_and_get_column<ColumnStruct>(*wrapper);
        if (structure == nullptr || structure->tuple_size() != _schema->children.size()) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant {} physical field count mismatch", _schema->name);
        }
        DORIS_CHECK(_unshredded_prefix.empty() || unshredded_child_indices(*_schema).has_value());
        DORIS_CHECK(!_unshredded_path_cache ||
                    (!_unshredded_prefix.empty() &&
                     _unshredded_path_cache->entries.size() == _physical->size()));
    }

    size_t size() const override { return _physical->size(); }
    size_t byte_size() const override {
        std::lock_guard lock(_materialization_lock);
        return _physical->byte_size() +
               (_unshredded_path_cache ? _unshredded_path_cache->byte_size() : 0) +
               (_unshredded_metadata_index ? _unshredded_metadata_index->byte_size() : 0) +
               unshredded_result_cache_byte_size() +
               (_normalized_prefix ? _normalized_prefix->byte_size() : 0) +
               (_materialized ? _materialized->byte_size() : 0) +
               (_serialized ? _serialized->byte_size() : 0);
    }
    size_t allocated_bytes() const override {
        std::lock_guard lock(_materialization_lock);
        return _physical->allocated_bytes() +
               (_unshredded_path_cache ? _unshredded_path_cache->allocated_bytes() : 0) +
               (_unshredded_metadata_index ? _unshredded_metadata_index->allocated_bytes() : 0) +
               unshredded_result_cache_allocated_bytes() +
               (_normalized_prefix ? _normalized_prefix->allocated_bytes() : 0) +
               (_materialized ? _materialized->allocated_bytes() : 0) +
               (_serialized ? _serialized->allocated_bytes() : 0);
    }
    void sanity_check() const override {
        _physical->sanity_check();
        DORIS_CHECK(!_unshredded_path_cache ||
                    _unshredded_path_cache->entries.size() == _physical->size());
        std::lock_guard lock(_materialization_lock);
        if (_unshredded_metadata_index) {
            DORIS_CHECK_EQ(_unshredded_metadata_index->physical_identity, _physical.get());
            DORIS_CHECK_EQ(_unshredded_metadata_index->row_dictionary_ids.size(),
                           _physical->size());
            for (const uint32_t dictionary_id : _unshredded_metadata_index->row_dictionary_ids) {
                DORIS_CHECK(dictionary_id == UnshreddedMetadataIndex::NULL_ROW ||
                            dictionary_id < _unshredded_metadata_index->dictionaries.size());
            }
        }
        for (const auto& cached : _unshredded_result_cache) {
            DORIS_CHECK(static_cast<bool>(cached.result));
            DORIS_CHECK_EQ(cached.result->size(), _physical->size());
            cached.result->sanity_check();
        }
        if (_normalized_prefix) {
            _normalized_prefix->sanity_check();
        }
    }

    void for_each_subcolumn(const IColumn::ImutableColumnCallback& callback) const override {
        callback(*_physical);
    }

    std::shared_ptr<VariantShreddedState> filter(const IColumn::Filter& filter,
                                                 ssize_t result_size_hint) const override {
        // Compact the decoded physical tree directly. In particular, a leaf-only projection has
        // no metadata/value columns from which a canonical Variant could be reconstructed.
        // The projection schema is immutable and reader-scoped, so derived selections share it
        // instead of cloning the whole shredded tree for every filter operation.
        auto select_column = [&](const ColumnPtr& column) {
            return column->filter(filter, result_size_hint);
        };
        auto select_path_cache = [&](const UnshreddedPathCache& source) {
            DORIS_CHECK_EQ(filter.size(), source.entries.size());
            auto selected = std::make_shared<UnshreddedPathCache>();
            if (result_size_hint > 0) {
                selected->entries.reserve(cast_set<size_t>(result_size_hint));
            }
            for (size_t row = 0; row < source.entries.size(); ++row) {
                if (filter[row] != 0) {
                    selected->entries.push_back(source.entries[row]);
                }
            }
            return selected;
        };
        return select_state(_physical->filter(filter, result_size_hint), select_column,
                            select_path_cache);
    }

    std::shared_ptr<VariantShreddedState> select_range(size_t start, size_t length) const override {
        auto select_column = [&](const ColumnPtr& column) { return column->cut(start, length); };
        auto select_path_cache = [&](const UnshreddedPathCache& source) {
            DORIS_CHECK_LE(start, source.entries.size());
            DORIS_CHECK_LE(length, source.entries.size() - start);
            auto selected = std::make_shared<UnshreddedPathCache>();
            selected->entries.insert(selected->entries.end(),
                                     source.entries.begin() + cast_set<ssize_t>(start),
                                     source.entries.begin() + cast_set<ssize_t>(start + length));
            return selected;
        };
        return select_state(_physical->cut(start, length), select_column, select_path_cache);
    }

    std::shared_ptr<VariantShreddedState> select_indices(
            const uint32_t* indices_begin, const uint32_t* indices_end) const override {
        auto select_column = [&](const ColumnPtr& column) {
            MutableColumnPtr selected = column->clone_empty();
            selected->insert_indices_from(*column, indices_begin, indices_end);
            return ColumnPtr(std::move(selected));
        };
        auto select_path_cache = [&](const UnshreddedPathCache& source) {
            auto selected = std::make_shared<UnshreddedPathCache>();
            selected->entries.reserve(indices_end - indices_begin);
            for (const uint32_t* index = indices_begin; index != indices_end; ++index) {
                DORIS_CHECK_LT(*index, source.entries.size());
                selected->entries.push_back(source.entries[*index]);
            }
            return selected;
        };
        return select_state(select_column(_physical), select_column, select_path_cache);
    }

    bool can_materialize() const override { return _complete; }

    bool try_append(const VariantShreddedState& source) override {
        const auto* parquet_source = dynamic_cast<const ParquetVariantShreddedState*>(&source);
        if (parquet_source == nullptr || _complete != parquet_source->_complete ||
            _unshredded_prefix != parquet_source->_unshredded_prefix ||
            !same_shredded_schema(*_schema, *parquet_source->_schema)) {
            return false;
        }
        validate_compatible_column(*_physical, *parquet_source->_physical);
        auto mutable_physical = IColumn::mutate(std::move(_physical));
        append_compatible_column(*mutable_physical, *parquet_source->_physical);
        _physical = std::move(mutable_physical);
        std::lock_guard lock(_materialization_lock);
        _unshredded_path_cache.reset();
        _unshredded_metadata_index.reset();
        _unshredded_result_cache.clear();
        _normalized_prefix.reset();
        _materialized.reset();
        _serialized.reset();
        return true;
    }

    std::optional<VariantShreddedTypedValue> find_typed_value(
            std::span<const VariantShreddedPathSegment> path) const override {
        auto path_miss = [&]() -> std::optional<VariantShreddedTypedValue> {
            update_counter(_profile.variant_direct_leaf_path_misses, 1);
            return std::nullopt;
        };
        if (path.empty()) {
            return path_miss();
        }

        if (unshredded_child_indices(*_schema).has_value()) {
            return VariantShreddedTypedValue {
                    .column = nullptr, .type = nullptr, .normalized = direct_unshredded_path(path)};
        }

        const ParquetColumnSchema* typed_schema = nullptr;
        ColumnPtr typed = struct_child(*_schema, _physical, "typed_value", &typed_schema);
        if (!typed || typed_schema->kind != ParquetColumnSchemaKind::STRUCT) {
            return path_miss();
        }

        for (size_t position = 0; position < path.size(); ++position) {
            if (path[position].kind != VariantShreddedPathSegment::Kind::OBJECT_KEY) {
                return path_miss();
            }

            const std::string_view key(path[position].key.data, path[position].key.size);
            const ParquetColumnSchema* wrapper_schema = nullptr;
            ColumnPtr wrapper = struct_child(*typed_schema, typed, key, &wrapper_schema);
            if (!wrapper) {
                return path_miss();
            }

            if (ColumnPtr residual = struct_child(*wrapper_schema, wrapper, "value", nullptr);
                static_cast<bool>(residual) && has_present_value(residual)) {
                // A residual value can contribute data to the same logical object. Reconstructing
                // is required in that case; returning only the typed leaf would drop information.
                update_counter(_profile.variant_direct_leaf_residual_fallbacks, 1);
                return std::nullopt;
            }

            typed = struct_child(*wrapper_schema, wrapper, "typed_value", &typed_schema);
            if (!typed) {
                return path_miss();
            }
            if (position + 1 == path.size()) {
                if (typed_schema->kind != ParquetColumnSchemaKind::PRIMITIVE ||
                    check_and_get_column<ColumnNullable>(*typed) == nullptr) {
                    update_counter(_profile.variant_direct_leaf_unsupported_fallbacks, 1);
                    if (!_complete) {
                        // Binary element_at evaluates complex prefixes before the validated leaf.
                        // Serialize only retained descendants so projected-out fields stay hidden.
                        if (auto normalized = find_normalized_value(path); normalized.has_value()) {
                            return VariantShreddedTypedValue {
                                    .column = nullptr, .type = nullptr, .normalized = *normalized};
                        }
                    }
                    return std::nullopt;
                }
                if (!supports_direct_typed_variant_state(*typed_schema)) {
                    if (_complete) {
                        update_counter(_profile.variant_direct_leaf_unsupported_fallbacks, 1);
                        return std::nullopt;
                    }
                    // A partial projection cannot reconstruct its root. Normalize only the exact
                    // requested leaf so Parquet annotations survive heterogeneous file schemas.
                    update_counter(_profile.variant_direct_leaf_rows,
                                   static_cast<int64_t>(typed->size()));
                    return VariantShreddedTypedValue {
                            .column = nullptr,
                            .type = nullptr,
                            .normalized = normalize_projected_primitive_leaf(*typed_schema, typed)};
                }
                update_counter(_profile.variant_direct_leaf_rows,
                               static_cast<int64_t>(typed->size()));
                return VariantShreddedTypedValue {.column = std::move(typed),
                                                  .type = remove_nullable(typed_schema->type),
                                                  .normalized = nullptr};
            }
            if (typed_schema->kind != ParquetColumnSchemaKind::STRUCT) {
                return path_miss();
            }
        }
        return std::nullopt;
    }

    std::optional<ColumnPtr> find_normalized_value(
            std::span<const VariantShreddedPathSegment> path) const override {
        if (path.empty()) {
            return std::nullopt;
        }

        if (unshredded_child_indices(*_schema).has_value()) {
            return direct_unshredded_path(path);
        }

        const ParquetColumnSchema* typed_schema = nullptr;
        ColumnPtr typed = struct_child(*_schema, _physical, "typed_value", &typed_schema);
        if (typed && typed_schema->kind == ParquetColumnSchemaKind::STRUCT) {
            bool direct = true;
            for (size_t position = 0; position < path.size(); ++position) {
                if (path[position].kind != VariantShreddedPathSegment::Kind::OBJECT_KEY) {
                    direct = false;
                    break;
                }
                const std::string_view key(path[position].key.data, path[position].key.size);
                const ParquetColumnSchema* wrapper_schema = nullptr;
                ColumnPtr wrapper = struct_child(*typed_schema, typed, key, &wrapper_schema);
                if (!wrapper) {
                    direct = false;
                    break;
                }
                ColumnPtr residual = struct_child(*wrapper_schema, wrapper, "value", nullptr);
                if (residual && has_present_value(residual)) {
                    direct = false;
                    break;
                }
                typed = struct_child(*wrapper_schema, wrapper, "typed_value", &typed_schema);
                if (!typed) {
                    direct = false;
                    break;
                }
                if (position + 1 == path.size()) {
                    direct = typed_schema->kind == ParquetColumnSchemaKind::PRIMITIVE &&
                             check_and_get_column<ColumnNullable>(*typed) != nullptr;
                } else if (typed_schema->kind != ParquetColumnSchemaKind::STRUCT) {
                    direct = false;
                    break;
                }
            }
            if (direct) {
                return normalize_projected_primitive_leaf(*typed_schema, typed);
            }
        }
        if (_complete) {
            return normalize_materialized_path(materialized_column(), path);
        }
        // Binary element_at chains request complex prefixes before their validated primitive leaf.
        // Reconstruct only retained descendants so an omitted field can never become observable.
        return normalize_materialized_path(serialized_column(), path);
    }

    const ColumnVariantV2& materialized_column() const override {
        std::lock_guard lock(_materialization_lock);
        if (!_complete) {
            throw Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "A projected Parquet Variant can only serve its validated shredded leaves");
        }
        if (!_unshredded_prefix.empty()) {
            if (!_normalized_prefix) {
                const auto child_indices = unshredded_child_indices(*_schema);
                DORIS_CHECK(child_indices.has_value());
                const auto borrowed = borrow_unshredded_path(_unshredded_prefix);
                int64_t copied_bytes = 0;
                {
                    SCOPED_TIMER(_profile.variant_unshredded_direct_seek_time.get());
                    _normalized_prefix = normalize_unshredded_path(
                            *_schema, *_physical, *child_indices, borrowed, &copied_bytes);
                }
                const auto rows = static_cast<int64_t>(_physical->size());
                update_counter(_profile.variant_unshredded_direct_seek_rows, rows);
                update_counter(_profile.variant_unshredded_direct_seek_bytes, copied_bytes);
            }
            const auto& nullable = assert_cast<const ColumnNullable&>(*_normalized_prefix);
            return assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
        }
        if (!_materialized) {
            SCOPED_TIMER(_profile.variant_reconstruction_time.get());
            _materialized = encode_variant_column(*_schema, *_physical, true, &_profile);
            update_counter(_profile.variant_reconstructed_rows,
                           static_cast<int64_t>(_physical->size()));
        }
        return *_materialized;
    }

    const ColumnVariantV2& serialized_column() const override {
        if (_complete) {
            return materialized_column();
        }
        std::lock_guard lock(_materialization_lock);
        if (!_serialized) {
            // Projected states intentionally omit root metadata, but an exchange buffer still
            // needs self-contained bytes. Rebuild only retained paths; access-path planning is the
            // invariant that prevents a downstream consumer from observing an omitted field.
            _serialized = encode_variant_column(*_schema, *_physical, false);
        }
        return *_serialized;
    }

private:
    template <typename ColumnSelector, typename PathCacheSelector>
    std::shared_ptr<ParquetVariantShreddedState> select_state(
            ColumnPtr selected_physical, const ColumnSelector& select_column,
            const PathCacheSelector& select_path_cache) const {
        std::shared_ptr<const UnshreddedPathCache> path_cache;
        std::vector<UnshreddedResultCacheEntry> result_cache;
        {
            std::lock_guard lock(_materialization_lock);
            path_cache = _unshredded_path_cache;
            result_cache = _unshredded_result_cache;
        }

        std::shared_ptr<const UnshreddedPathCache> selected_path_cache;
        if (path_cache) {
            selected_path_cache = select_path_cache(*path_cache);
        }
        auto selected = std::make_shared<ParquetVariantShreddedState>(
                _schema, std::move(selected_physical), _complete, _profile, _unshredded_prefix,
                std::move(selected_path_cache));
        selected->_unshredded_result_cache.reserve(result_cache.size());
        for (const auto& cached : result_cache) {
            ColumnPtr selected_result;
            std::shared_ptr<ParquetVariantShreddedState> selected_subtree;
            if (cached.subtree_state) {
                const auto& nullable = assert_cast<const ColumnNullable&>(*cached.result);
                ColumnPtr selected_nulls = select_column(nullable.get_null_map_column_ptr());
                selected_subtree = cached.subtree_state->select_state(
                        selected->_physical, select_column, select_path_cache);
                selected_result = ColumnNullable::create(
                        ColumnVariantV2::create_shredded(selected_subtree), selected_nulls);
            } else {
                selected_result = select_column(cached.result);
            }
            selected->_unshredded_result_cache.push_back(
                    {.path = cached.path,
                     .result = std::move(selected_result),
                     .subtree_state = std::move(selected_subtree)});
        }
        return selected;
    }

    std::vector<OwnedShreddedPathSegment> combined_unshredded_path(
            std::span<const VariantShreddedPathSegment> suffix) const {
        std::vector<OwnedShreddedPathSegment> combined = _unshredded_prefix;
        combined.reserve(combined.size() + suffix.size());
        for (const auto& segment : suffix) {
            combined.push_back(
                    {.kind = segment.kind,
                     .key = segment.kind == VariantShreddedPathSegment::Kind::OBJECT_KEY
                                    ? (segment.key.size == 0
                                               ? std::string()
                                               : std::string(segment.key.data, segment.key.size))
                                    : std::string(),
                     .index = segment.index});
        }
        return combined;
    }

    static std::vector<VariantShreddedPathSegment> borrow_unshredded_path(
            const std::vector<OwnedShreddedPathSegment>& owned) {
        std::vector<VariantShreddedPathSegment> borrowed;
        borrowed.reserve(owned.size());
        for (const auto& segment : owned) {
            borrowed.push_back({.kind = segment.kind,
                                .key = {segment.key.data(), segment.key.size()},
                                .index = segment.index});
        }
        return borrowed;
    }

    std::shared_ptr<const UnshreddedMetadataIndex> get_unshredded_metadata_index(
            std::pair<size_t, size_t> child_indices) const {
        std::lock_guard lock(_materialization_lock);
        if (!_unshredded_metadata_index) {
            _unshredded_metadata_index =
                    build_unshredded_metadata_index(*_schema, *_physical, child_indices);
        }
        return _unshredded_metadata_index;
    }

    ColumnPtr direct_unshredded_path(std::span<const VariantShreddedPathSegment> suffix) const {
        const auto child_indices = unshredded_child_indices(*_schema);
        DORIS_CHECK(child_indices.has_value());
        std::vector<OwnedShreddedPathSegment> combined = combined_unshredded_path(suffix);
        {
            std::lock_guard lock(_materialization_lock);
            for (const auto& cached : _unshredded_result_cache) {
                if (cached.path == combined) {
                    update_counter(_profile.variant_unshredded_result_cache_hit_rows,
                                   static_cast<int64_t>(_physical->size()));
                    return cached.result;
                }
            }
        }
        const auto borrowed_combined = borrow_unshredded_path(combined);
        std::shared_ptr<const UnshreddedMetadataIndex> metadata_index;
        UnshreddedPathScan scan;
        {
            SCOPED_TIMER(_profile.variant_unshredded_direct_seek_time.get());
            metadata_index = get_unshredded_metadata_index(*child_indices);
            scan = scan_unshredded_path(
                    *_schema, *_physical, *child_indices, *metadata_index,
                    _unshredded_path_cache
                            ? suffix
                            : std::span<const VariantShreddedPathSegment>(borrowed_combined),
                    _unshredded_path_cache.get());
        }

        const auto rows = static_cast<int64_t>(_physical->size());
        update_counter(_profile.variant_unshredded_direct_seek_rows, rows);
        if (_unshredded_path_cache) {
            update_counter(_profile.variant_unshredded_prefix_reuse_rows, rows);
        }
        MutableColumnPtr values;
        std::shared_ptr<ParquetVariantShreddedState> subtree_state;
        if (scan.typed_values) {
            values = ColumnVariantV2::create_typed(std::move(scan.typed_values),
                                                   std::move(scan.typed_type));
            update_counter(_profile.variant_unshredded_direct_seek_bytes, scan.copied_bytes);
            update_counter(_profile.variant_direct_leaf_rows, rows);
        } else {
            subtree_state = std::make_shared<ParquetVariantShreddedState>(
                    _schema, _physical, _complete, _profile, combined, std::move(scan.path_cache));
            values = ColumnVariantV2::create_shredded(subtree_state);
            update_counter(_profile.variant_direct_subtree_rows, rows);
        }
        ColumnPtr result = ColumnNullable::create(std::move(values), std::move(scan.outer_nulls));
        {
            std::lock_guard lock(_materialization_lock);
            for (const auto& cached : _unshredded_result_cache) {
                if (cached.path == combined) {
                    return cached.result;
                }
            }
            _unshredded_result_cache.push_back({.path = std::move(combined),
                                                .result = result,
                                                .subtree_state = std::move(subtree_state)});
        }
        return result;
    }

    size_t cached_result_byte_size(const UnshreddedResultCacheEntry& cached) const {
        const size_t bytes = cached.result->byte_size();
        if (!cached.subtree_state) {
            return bytes;
        }
        const size_t shared_physical_bytes = cached.subtree_state->_physical->byte_size();
        DORIS_CHECK_GE(bytes, shared_physical_bytes);
        return bytes - shared_physical_bytes;
    }

    size_t cached_result_allocated_bytes(const UnshreddedResultCacheEntry& cached) const {
        const size_t bytes = cached.result->allocated_bytes();
        if (!cached.subtree_state) {
            return bytes;
        }
        const size_t shared_physical_bytes = cached.subtree_state->_physical->allocated_bytes();
        DORIS_CHECK_GE(bytes, shared_physical_bytes);
        return bytes - shared_physical_bytes;
    }

    size_t unshredded_result_cache_byte_size() const {
        size_t bytes = _unshredded_result_cache.size() * sizeof(UnshreddedResultCacheEntry);
        for (const auto& cached : _unshredded_result_cache) {
            bytes += cached.path_byte_size() + cached_result_byte_size(cached);
        }
        return bytes;
    }

    size_t unshredded_result_cache_allocated_bytes() const {
        size_t bytes = _unshredded_result_cache.capacity() * sizeof(UnshreddedResultCacheEntry);
        for (const auto& cached : _unshredded_result_cache) {
            bytes += cached.path_allocated_bytes() + cached_result_allocated_bytes(cached);
        }
        return bytes;
    }

    static void update_counter(const std::shared_ptr<RuntimeProfile::Counter>& counter,
                               int64_t value) {
        if (counter != nullptr) {
            COUNTER_UPDATE(counter.get(), value);
        }
    }

    std::shared_ptr<const ParquetColumnSchema> _schema;
    ColumnPtr _physical;
    bool _complete = true;
    ParquetColumnReaderProfile _profile;
    std::vector<OwnedShreddedPathSegment> _unshredded_prefix;
    std::shared_ptr<const UnshreddedPathCache> _unshredded_path_cache;
    mutable std::mutex _materialization_lock;
    mutable std::shared_ptr<const UnshreddedMetadataIndex> _unshredded_metadata_index;
    mutable std::vector<UnshreddedResultCacheEntry> _unshredded_result_cache;
    mutable ColumnPtr _normalized_prefix;
    mutable ColumnVariantV2::MutablePtr _materialized;
    mutable ColumnVariantV2::MutablePtr _serialized;
};

MutableColumnPtr build_variant_column(std::shared_ptr<const ParquetColumnSchema> schema,
                                      ColumnPtr physical, bool complete,
                                      const ParquetColumnReaderProfile& profile) {
    DORIS_CHECK(schema != nullptr);
    if (schema->kind != ParquetColumnSchemaKind::VARIANT) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Parquet column {} is not Variant",
                        schema->name);
    }

    const auto* outer_nullable = check_and_get_column<ColumnNullable>(*physical);
    MutableColumnPtr variants =
            ColumnVariantV2::create_shredded(std::make_shared<ParquetVariantShreddedState>(
                    std::move(schema), physical, complete, profile));
    if (outer_nullable == nullptr) {
        return variants;
    }
    auto nulls = outer_nullable->get_null_map_column().clone_resized(physical->size());
    return ColumnNullable::create(std::move(variants), std::move(nulls));
}

ColumnPtr transform_node(const VariantMaterializationNode& plan, ColumnPtr physical,
                         const ParquetColumnReaderProfile& profile);

ColumnPtr transform_non_nullable(const VariantMaterializationNode& plan, ColumnPtr physical,
                                 const ParquetColumnReaderProfile& profile) {
    const auto& schema = *plan.schema;
    switch (schema.kind) {
    case ParquetColumnSchemaKind::PRIMITIVE:
        return physical;
    case ParquetColumnSchemaKind::VARIANT:
        return build_variant_column(
                plan.variant_state_schema
                        ? plan.variant_state_schema
                        : create_variant_state_schema(schema, plan.variant_projection
                                                                      ? &*plan.variant_projection
                                                                      : nullptr),
                std::move(physical),
                !format::is_partial_projection(plan.variant_projection ? &*plan.variant_projection
                                                                       : nullptr),
                profile);
    case ParquetColumnSchemaKind::STRUCT: {
        const auto& structure = assert_cast<const ColumnStruct&>(*physical);
        if (structure.tuple_size() != plan.children.size()) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Projected Parquet STRUCT {} field count mismatch", schema.name);
        }
        Columns fields;
        fields.reserve(plan.children.size());
        for (size_t i = 0; i < plan.children.size(); ++i) {
            fields.push_back(
                    transform_node(*plan.children[i], structure.get_column_ptr(i), profile));
        }
        return ColumnStruct::create(std::move(fields));
    }
    case ParquetColumnSchemaKind::LIST: {
        const auto& array = assert_cast<const ColumnArray&>(*physical);
        if (plan.children.size() != 1) {
            throw Exception(ErrorCode::CORRUPTION, "Projected Parquet ARRAY plan is invalid");
        }
        auto values = transform_node(*plan.children[0], array.get_data_ptr(), profile);
        return ColumnArray::create(std::move(values), array.get_offsets_ptr());
    }
    case ParquetColumnSchemaKind::MAP: {
        const auto& map = assert_cast<const ColumnMap&>(*physical);
        if (plan.children.size() != 2) {
            throw Exception(ErrorCode::CORRUPTION, "Projected Parquet MAP plan is invalid");
        }
        auto keys = transform_node(*plan.children[0], map.get_keys_ptr(), profile);
        auto values = transform_node(*plan.children[1], map.get_values_ptr(), profile);
        return ColumnMap::create(std::move(keys), std::move(values), map.get_offsets_ptr());
    }
    }
    throw Exception(ErrorCode::INTERNAL_ERROR, "Unknown Parquet schema kind");
}

ColumnPtr transform_node(const VariantMaterializationNode& plan, ColumnPtr physical,
                         const ParquetColumnReaderProfile& profile) {
    if (plan.schema == nullptr) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "Parquet Variant materialization plan is null");
    }
    if (plan.schema->kind == ParquetColumnSchemaKind::VARIANT) {
        return build_variant_column(
                plan.variant_state_schema
                        ? plan.variant_state_schema
                        : create_variant_state_schema(
                                  *plan.schema,
                                  plan.variant_projection ? &*plan.variant_projection : nullptr),
                std::move(physical),
                !format::is_partial_projection(plan.variant_projection ? &*plan.variant_projection
                                                                       : nullptr),
                profile);
    }
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*physical)) {
        auto nested = transform_non_nullable(plan, nullable->get_nested_column_ptr(), profile);
        return ColumnNullable::create(std::move(nested), nullable->get_null_map_column_ptr());
    }
    return transform_non_nullable(plan, std::move(physical), profile);
}

void append_compatible_column(IColumn& output, const IColumn& converted) {
    if (auto* output_nullable = check_and_get_column<ColumnNullable>(output)) {
        auto& nested = output_nullable->get_nested_column();
        auto& null_map = output_nullable->get_null_map_column();
        const size_t nested_size = nested.size();
        const size_t null_map_size = null_map.size();
        try {
            if (const auto* converted_nullable = check_and_get_column<ColumnNullable>(converted)) {
                append_compatible_column(nested, converted_nullable->get_nested_column());
                null_map.insert_range_from(converted_nullable->get_null_map_column(), 0,
                                           converted.size());
            } else {
                append_compatible_column(nested, converted);
                // External slots and nested Iceberg fields may remain nullable even when one
                // file's physical node is required. Preserve that destination invariant with
                // non-null bits.
                output_nullable->push_false_to_nullmap(converted.size());
            }
        } catch (...) {
            if (nested.size() > nested_size) {
                nested.pop_back(nested.size() - nested_size);
            }
            if (null_map.size() > null_map_size) {
                null_map.pop_back(null_map.size() - null_map_size);
            }
            throw;
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
        std::vector<size_t> original_sizes(output_struct->tuple_size());
        for (size_t i = 0; i < output_struct->tuple_size(); ++i) {
            original_sizes[i] = output_struct->get_column(i).size();
        }
        try {
            for (size_t i = 0; i < output_struct->tuple_size(); ++i) {
                append_compatible_column(output_struct->get_column(i),
                                         converted_struct->get_column(i));
            }
        } catch (...) {
            // Variant corruption can surface only during lazy fallback after earlier siblings
            // were appended. Roll every child back to preserve the failed-append invariant.
            for (size_t i = 0; i < output_struct->tuple_size(); ++i) {
                auto& child = output_struct->get_column(i);
                if (child.size() > original_sizes[i]) {
                    child.pop_back(child.size() - original_sizes[i]);
                }
            }
            throw;
        }
        return;
    }

    if (auto* output_array = check_and_get_column<ColumnArray>(output)) {
        const auto* converted_array = check_and_get_column<ColumnArray>(converted);
        if (converted_array == nullptr) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced an incompatible ARRAY");
        }
        auto& output_data = output_array->get_data();
        auto& output_offsets = output_array->get_offsets();
        const size_t element_base = output_data.size();
        const size_t offsets_size = output_offsets.size();
        try {
            append_compatible_column(output_data, converted_array->get_data());
            output_offsets.reserve(output_offsets.size() + converted_array->size());
            for (const auto offset : converted_array->get_offsets()) {
                output_offsets.push_back(element_base + offset);
            }
        } catch (...) {
            if (output_data.size() > element_base) {
                output_data.pop_back(output_data.size() - element_base);
            }
            output_offsets.resize(offsets_size);
            throw;
        }
        return;
    }

    if (auto* output_map = check_and_get_column<ColumnMap>(output)) {
        const auto* converted_map = check_and_get_column<ColumnMap>(converted);
        if (converted_map == nullptr) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced an incompatible MAP");
        }
        auto& output_keys = output_map->get_keys();
        auto& output_values = output_map->get_values();
        auto& output_offsets = output_map->get_offsets();
        const size_t element_base = output_keys.size();
        const size_t values_size = output_values.size();
        const size_t offsets_size = output_offsets.size();
        try {
            append_compatible_column(output_keys, converted_map->get_keys());
            append_compatible_column(output_values, converted_map->get_values());
            output_offsets.reserve(output_offsets.size() + converted_map->size());
            for (const auto offset : converted_map->get_offsets()) {
                output_offsets.push_back(element_base + offset);
            }
        } catch (...) {
            if (output_keys.size() > element_base) {
                output_keys.pop_back(output_keys.size() - element_base);
            }
            if (output_values.size() > values_size) {
                output_values.pop_back(output_values.size() - values_size);
            }
            output_offsets.resize(offsets_size);
            throw;
        }
        return;
    }

    if (auto* output_variant = check_and_get_column<ColumnVariantV2>(output)) {
        const auto* converted_variant = check_and_get_column<ColumnVariantV2>(converted);
        if (converted_variant == nullptr) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced an incompatible column");
        }
        output_variant->insert_range_from(*converted_variant, 0, converted_variant->size());
        return;
    }

    if (output.get_name() != converted.get_name()) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Parquet Variant materialization produced {} for {} destination",
                        converted.get_name(), output.get_name());
    }
    output.insert_range_from(converted, 0, converted.size());
}

void validate_compatible_column(const IColumn& output, const IColumn& converted) {
    if (const auto* output_nullable = check_and_get_column<ColumnNullable>(output)) {
        if (const auto* converted_nullable = check_and_get_column<ColumnNullable>(converted)) {
            validate_compatible_column(output_nullable->get_nested_column(),
                                       converted_nullable->get_nested_column());
        } else {
            validate_compatible_column(output_nullable->get_nested_column(), converted);
        }
        return;
    }
    if (const auto* converted_nullable = check_and_get_column<ColumnNullable>(converted)) {
        if (converted_nullable->has_null()) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced null data for a "
                            "non-nullable destination");
        }
        validate_compatible_column(output, converted_nullable->get_nested_column());
        return;
    }
    if (const auto* output_struct = check_and_get_column<ColumnStruct>(output)) {
        const auto* converted_struct = check_and_get_column<ColumnStruct>(converted);
        if (converted_struct == nullptr ||
            output_struct->tuple_size() != converted_struct->tuple_size()) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced an incompatible STRUCT");
        }
        for (size_t i = 0; i < output_struct->tuple_size(); ++i) {
            validate_compatible_column(output_struct->get_column(i),
                                       converted_struct->get_column(i));
        }
        return;
    }
    if (const auto* output_array = check_and_get_column<ColumnArray>(output)) {
        const auto* converted_array = check_and_get_column<ColumnArray>(converted);
        if (converted_array == nullptr) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced an incompatible ARRAY");
        }
        validate_compatible_column(output_array->get_data(), converted_array->get_data());
        return;
    }
    if (const auto* output_map = check_and_get_column<ColumnMap>(output)) {
        const auto* converted_map = check_and_get_column<ColumnMap>(converted);
        if (converted_map == nullptr) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced an incompatible MAP");
        }
        validate_compatible_column(output_map->get_keys(), converted_map->get_keys());
        validate_compatible_column(output_map->get_values(), converted_map->get_values());
        return;
    }
    if (check_and_get_column<ColumnVariantV2>(output) != nullptr) {
        if (check_and_get_column<ColumnVariantV2>(converted) == nullptr) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Parquet Variant materialization produced an incompatible column");
        }
        return;
    }
    if (output.get_name() != converted.get_name()) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Parquet Variant materialization produced {} for {} destination",
                        converted.get_name(), output.get_name());
    }
}

bool has_exact_column_shape(const IColumn& output, const IColumn& converted) {
    const auto* output_nullable = check_and_get_column<ColumnNullable>(output);
    const auto* converted_nullable = check_and_get_column<ColumnNullable>(converted);
    if (output_nullable != nullptr || converted_nullable != nullptr) {
        return output_nullable != nullptr && converted_nullable != nullptr &&
               has_exact_column_shape(output_nullable->get_nested_column(),
                                      converted_nullable->get_nested_column());
    }
    if (const auto* output_struct = check_and_get_column<ColumnStruct>(output)) {
        const auto* converted_struct = check_and_get_column<ColumnStruct>(converted);
        if (converted_struct == nullptr ||
            output_struct->tuple_size() != converted_struct->tuple_size()) {
            return false;
        }
        for (size_t i = 0; i < output_struct->tuple_size(); ++i) {
            if (!has_exact_column_shape(output_struct->get_column(i),
                                        converted_struct->get_column(i))) {
                return false;
            }
        }
        return true;
    }
    if (const auto* output_array = check_and_get_column<ColumnArray>(output)) {
        const auto* converted_array = check_and_get_column<ColumnArray>(converted);
        return converted_array != nullptr &&
               has_exact_column_shape(output_array->get_data(), converted_array->get_data());
    }
    if (const auto* output_map = check_and_get_column<ColumnMap>(output)) {
        const auto* converted_map = check_and_get_column<ColumnMap>(converted);
        return converted_map != nullptr &&
               has_exact_column_shape(output_map->get_keys(), converted_map->get_keys()) &&
               has_exact_column_shape(output_map->get_values(), converted_map->get_values());
    }
    if (check_and_get_column<ColumnVariantV2>(output) != nullptr) {
        return check_and_get_column<ColumnVariantV2>(converted) != nullptr;
    }
    return output.get_name() == converted.get_name();
}

void append_materialized_column(MutableColumnPtr& output, ColumnPtr converted) {
    // Validate the complete destination shape before mutation. This preserves atomic failures
    // without copying a full scratch batch, while an empty exact-shape output can adopt the tree.
    validate_compatible_column(*output, *converted);
    if (output->empty() && has_exact_column_shape(*output, *converted)) {
        if (converted->is_exclusive()) {
            // The transformed tree consumed the decoder tree and is recursively exclusive. Keep
            // primitive siblings and their buffers intact instead of recursively COW-cloning them.
            output = converted->assert_mutable();
            return;
        }
        output = IColumn::mutate(std::move(converted));
        return;
    }
    append_compatible_column(*output, *converted);
}

} // namespace

std::shared_ptr<const ParquetColumnSchema> create_variant_state_schema(
        const ParquetColumnSchema& schema, const format::LocalColumnIndex* projection) {
    return std::shared_ptr<const ParquetColumnSchema>(clone_schema(schema, projection));
}

Status materialize_variant_rows(const ParquetColumnSchema& schema, const IColumn& physical,
                                MutableColumnPtr& output,
                                const ParquetColumnReaderProfile& profile) {
    return materialize_variant_rows(schema, physical.get_ptr(), output, profile);
}

Status materialize_variant_rows(const ParquetColumnSchema& schema, ColumnPtr physical,
                                MutableColumnPtr& output,
                                const ParquetColumnReaderProfile& profile) {
    if (!output) {
        return Status::InvalidArgument("Parquet Variant output column is null");
    }
    RETURN_IF_CATCH_EXCEPTION({
        auto converted = build_variant_column(create_variant_state_schema(schema),
                                              std::move(physical), true, profile);
        append_materialized_column(output, std::move(converted));
    });
    return Status::OK();
}

Status materialize_variant_columns(const VariantMaterializationNode& plan, const IColumn& physical,
                                   MutableColumnPtr& output,
                                   const ParquetColumnReaderProfile& profile) {
    return materialize_variant_columns(plan, physical.get_ptr(), output, profile);
}

Status materialize_variant_columns(const VariantMaterializationNode& plan, ColumnPtr physical,
                                   MutableColumnPtr& output,
                                   const ParquetColumnReaderProfile& profile) {
    if (!output) {
        return Status::InvalidArgument("Parquet Variant output column is null");
    }
    RETURN_IF_CATCH_EXCEPTION({
        auto converted = transform_node(plan, std::move(physical), profile);
        append_materialized_column(output, std::move(converted));
    });
    return Status::OK();
}

} // namespace doris::format::parquet
