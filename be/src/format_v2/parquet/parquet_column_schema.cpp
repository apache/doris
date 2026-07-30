// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "format_v2/parquet/parquet_column_schema.h"

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "core/data_type/data_type_nullable.h"
#include "format_v2/parquet/native_schema_desc.h"
#include "format_v2/parquet/parquet_type.h"

namespace doris::format::parquet {
namespace {

ParquetTimeUnit native_time_unit(const tparquet::TimeUnit& unit) {
    if (unit.__isset.MILLIS) {
        return ParquetTimeUnit::MILLIS;
    }
    if (unit.__isset.MICROS) {
        return ParquetTimeUnit::MICROS;
    }
    if (unit.__isset.NANOS) {
        return ParquetTimeUnit::NANOS;
    }
    return ParquetTimeUnit::UNKNOWN;
}

ParquetExtraTypeInfo native_time_extra(ParquetTimeUnit unit) {
    switch (unit) {
    case ParquetTimeUnit::MILLIS:
        return ParquetExtraTypeInfo::UNIT_MS;
    case ParquetTimeUnit::MICROS:
        return ParquetExtraTypeInfo::UNIT_MICROS;
    case ParquetTimeUnit::NANOS:
        return ParquetExtraTypeInfo::UNIT_NS;
    case ParquetTimeUnit::UNKNOWN:
    default:
        return ParquetExtraTypeInfo::NONE;
    }
}

void fill_native_type_descriptor(const NativeFieldSchema& field, ParquetTypeDescriptor* result) {
    DORIS_CHECK(result != nullptr);
    const auto& schema = field.parquet_schema;
    result->doris_type = field.data_type;
    result->unsupported_reason = field.unsupported_reason;
    result->physical_type = static_cast<tparquet::Type::type>(field.physical_type);
    result->fixed_length = schema.__isset.type_length ? schema.type_length : -1;
    if (schema.__isset.logicalType) {
        const auto& logical = schema.logicalType;
        if (logical.__isset.DECIMAL) {
            result->is_decimal = true;
            result->decimal_precision = logical.DECIMAL.precision;
            result->decimal_scale = logical.DECIMAL.scale;
        } else if (logical.__isset.INTEGER) {
            result->integer_bit_width = logical.INTEGER.bitWidth;
            result->is_unsigned_integer = !logical.INTEGER.isSigned;
        } else if (logical.__isset.TIME) {
            result->time_unit = native_time_unit(logical.TIME.unit);
            result->extra_type_info = native_time_extra(result->time_unit);
            if (logical.TIME.isAdjustedToUTC) {
                result->unsupported_reason =
                        "Parquet TIME with isAdjustedToUTC=true is not supported";
            }
        } else if (logical.__isset.TIMESTAMP) {
            result->is_timestamp = true;
            result->timestamp_is_adjusted_to_utc = logical.TIMESTAMP.isAdjustedToUTC;
            result->time_unit = native_time_unit(logical.TIMESTAMP.unit);
            result->extra_type_info = native_time_extra(result->time_unit);
        } else if (logical.__isset.FLOAT16) {
            result->extra_type_info = ParquetExtraTypeInfo::FLOAT16;
        }
    } else if (schema.__isset.converted_type) {
        switch (schema.converted_type) {
        case tparquet::ConvertedType::DECIMAL:
            result->is_decimal = true;
            result->decimal_precision = schema.__isset.precision ? schema.precision : -1;
            result->decimal_scale = schema.__isset.scale ? schema.scale : -1;
            break;
        case tparquet::ConvertedType::INT_8:
        case tparquet::ConvertedType::UINT_8:
            result->integer_bit_width = 8;
            result->is_unsigned_integer = schema.converted_type == tparquet::ConvertedType::UINT_8;
            break;
        case tparquet::ConvertedType::INT_16:
        case tparquet::ConvertedType::UINT_16:
            result->integer_bit_width = 16;
            result->is_unsigned_integer = schema.converted_type == tparquet::ConvertedType::UINT_16;
            break;
        case tparquet::ConvertedType::INT_32:
        case tparquet::ConvertedType::UINT_32:
            result->integer_bit_width = 32;
            result->is_unsigned_integer = schema.converted_type == tparquet::ConvertedType::UINT_32;
            break;
        case tparquet::ConvertedType::INT_64:
        case tparquet::ConvertedType::UINT_64:
            result->integer_bit_width = 64;
            result->is_unsigned_integer = schema.converted_type == tparquet::ConvertedType::UINT_64;
            break;
        case tparquet::ConvertedType::TIMESTAMP_MILLIS:
        case tparquet::ConvertedType::TIMESTAMP_MICROS:
            result->is_timestamp = true;
            result->timestamp_is_adjusted_to_utc = true;
            result->time_unit = schema.converted_type == tparquet::ConvertedType::TIMESTAMP_MILLIS
                                        ? ParquetTimeUnit::MILLIS
                                        : ParquetTimeUnit::MICROS;
            result->extra_type_info = native_time_extra(result->time_unit);
            break;
        case tparquet::ConvertedType::TIME_MILLIS:
        case tparquet::ConvertedType::TIME_MICROS:
            result->unsupported_reason = "Parquet TIME with isAdjustedToUTC=true is not supported";
            break;
        default:
            break;
        }
    }

    if (result->is_decimal) {
        switch (result->physical_type) {
        case tparquet::Type::INT32:
            result->extra_type_info = ParquetExtraTypeInfo::DECIMAL_INT32;
            break;
        case tparquet::Type::INT64:
            result->extra_type_info = ParquetExtraTypeInfo::DECIMAL_INT64;
            break;
        case tparquet::Type::BYTE_ARRAY:
        case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
            result->extra_type_info = ParquetExtraTypeInfo::DECIMAL_BYTE_ARRAY;
            break;
        default:
            break;
        }
    } else if (result->physical_type == tparquet::Type::INT96) {
        result->is_timestamp = true;
        result->extra_type_info = ParquetExtraTypeInfo::IMPALA_TIMESTAMP;
    }
    result->is_string_like = !result->is_decimal &&
                             result->extra_type_info != ParquetExtraTypeInfo::FLOAT16 &&
                             (result->physical_type == tparquet::Type::BYTE_ARRAY ||
                              result->physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY);
}

void propagate_native_max_levels(ParquetColumnSchema* schema) {
    DORIS_CHECK(schema != nullptr);
    for (const auto& child : schema->children) {
        DORIS_CHECK(child != nullptr);
        propagate_native_max_levels(child.get());
        schema->max_definition_level =
                std::max(schema->max_definition_level, child->max_definition_level);
        schema->max_repetition_level =
                std::max(schema->max_repetition_level, child->max_repetition_level);
    }
}

std::unique_ptr<ParquetColumnSchema> build_native_node_schema(const NativeFieldSchema& field,
                                                              int32_t local_id) {
    auto result = std::make_unique<ParquetColumnSchema>();
    result->local_id = local_id;
    result->parquet_field_id = field.field_id;
    result->name = field.name;
    result->type = field.data_type;
    result->definition_level = field.definition_level;
    result->repetition_level = field.repetition_level;
    result->max_definition_level = field.definition_level;
    result->max_repetition_level = field.repetition_level;
    result->nullable_definition_level = field.data_type != nullptr && field.data_type->is_nullable()
                                                ? field.definition_level - field.repetition_level
                                                : 0;
    result->repeated_ancestor_definition_level = field.repeated_parent_def_level;
    result->repeated_repetition_level = field.repetition_level;

    const auto primitive_type = remove_nullable(field.data_type)->get_primitive_type();
    if (field.children.empty()) {
        result->kind = ParquetColumnSchemaKind::PRIMITIVE;
        result->leaf_column_id = field.physical_column_index;
        fill_native_type_descriptor(field, &result->type_descriptor);
        return result;
    }
    if (primitive_type == TYPE_ARRAY) {
        result->kind = ParquetColumnSchemaKind::LIST;
    } else if (primitive_type == TYPE_MAP) {
        result->kind = ParquetColumnSchemaKind::MAP;
    } else {
        result->kind = ParquetColumnSchemaKind::STRUCT;
    }
    result->children.reserve(field.children.size());
    for (size_t child_idx = 0; child_idx < field.children.size(); ++child_idx) {
        result->children.push_back(
                build_native_node_schema(field.children[child_idx], cast_set<int32_t>(child_idx)));
    }
    propagate_native_max_levels(result.get());
    return result;
}

} // namespace

Status build_parquet_column_schema(const NativeFieldDescriptor& schema,
                                   std::vector<std::unique_ptr<ParquetColumnSchema>>* fields) {
    if (fields == nullptr) {
        return Status::InvalidArgument("fields is null");
    }
    fields->clear();
    const auto& native_fields = schema.get_fields_schema();
    fields->reserve(native_fields.size());
    for (size_t field_idx = 0; field_idx < native_fields.size(); ++field_idx) {
        // Unsupported logical leaves stay in the file schema so request-level validation can
        // ignore unprojected fields and COUNT(*) placeholders without weakening real projections.
        // The scan projection and native readers must share one tree; rebuilding wrappers through
        // Arrow changes legacy LIST/STRUCT boundaries and makes valid nested values look absent.
        fields->push_back(
                build_native_node_schema(native_fields[field_idx], cast_set<int32_t>(field_idx)));
    }
    return Status::OK();
}

} // namespace doris::format::parquet
