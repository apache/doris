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

#include "format_v2/parquet/reader/native/column_reader.h"

#include <gen_cpp/parquet_types.h>
#include <limits.h>
#include <sys/types.h>

#include <algorithm>
#include <string_view>
#include <utility>

#include "common/cast_set.h"
#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/define_primitive_type.h"
#include "exprs/vexpr.h"
#include "format_v2/parquet/native_schema_desc.h"
#include "format_v2/parquet/reader/native/column_chunk_reader.h"
#include "format_v2/parquet/reader/native/level_decoder.h"
#include "io/fs/tracing_file_reader.h"
#include "runtime/runtime_profile.h"

namespace doris::format::parquet::native {
namespace {

ParquetTimeUnit parquet_time_unit(const tparquet::TimeUnit& unit) {
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

template <typename T>
bool release_vector_if_oversized(std::vector<T>* values, size_t max_retained_bytes) {
    DORIS_CHECK(values != nullptr);
    if (values->capacity() * sizeof(T) <= max_retained_bytes) {
        return false;
    }
    std::vector<T>().swap(*values);
    return true;
}

size_t retained_set_bytes(const std::unordered_set<size_t>& values) {
    return values.bucket_count() * sizeof(void*) + values.size() * sizeof(size_t);
}

Status validate_decimal_physical_type(const NativeFieldSchema& field, int precision, int scale) {
    if (precision <= 0 || scale < 0 || scale > precision) {
        return Status::Corruption("Parquet decimal field {} has invalid precision {} and scale {}",
                                  field.name, precision, scale);
    }
    switch (field.physical_type) {
    case tparquet::Type::INT32:
        if (precision <= 9) {
            return Status::OK();
        }
        break;
    case tparquet::Type::INT64:
        if (precision <= 18) {
            return Status::OK();
        }
        break;
    case tparquet::Type::BYTE_ARRAY:
        return Status::OK();
    case tparquet::Type::FIXED_LEN_BYTE_ARRAY: {
        const int length =
                field.parquet_schema.__isset.type_length ? field.parquet_schema.type_length : -1;
        if (length > 0) {
            const int64_t max_precision = (static_cast<int64_t>(length) * 8 - 1) * 30103 / 100000;
            if (precision <= max_precision) {
                return Status::OK();
            }
        }
        break;
    }
    default:
        break;
    }
    return Status::Corruption("Parquet decimal field {} has incompatible physical type {}",
                              field.name, tparquet::to_string(field.physical_type));
}

Status validate_physical_annotation(const NativeFieldSchema& field) {
    const auto& schema = field.parquet_schema;
    if (field.physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY &&
        (!schema.__isset.type_length || schema.type_length <= 0)) {
        return Status::Corruption("Parquet fixed-length field {} has invalid width {}", field.name,
                                  schema.__isset.type_length ? schema.type_length : -1);
    }
    auto require = [&](bool valid, std::string_view annotation) -> Status {
        if (valid) {
            return Status::OK();
        }
        return Status::Corruption("Parquet {} field {} is incompatible with physical type {}",
                                  annotation, field.name, tparquet::to_string(field.physical_type));
    };

    if (schema.__isset.logicalType) {
        const auto& logical = schema.logicalType;
        if (logical.__isset.STRING || logical.__isset.ENUM || logical.__isset.JSON ||
            logical.__isset.BSON) {
            return require(field.physical_type == tparquet::Type::BYTE_ARRAY, "string");
        }
        if (logical.__isset.DECIMAL) {
            return validate_decimal_physical_type(field, logical.DECIMAL.precision,
                                                  logical.DECIMAL.scale);
        }
        if (logical.__isset.DATE) {
            return require(field.physical_type == tparquet::Type::INT32, "date");
        }
        if (logical.__isset.TIME) {
            const auto unit = parquet_time_unit(logical.TIME.unit);
            return require(
                    (unit == ParquetTimeUnit::MILLIS &&
                     field.physical_type == tparquet::Type::INT32) ||
                            ((unit == ParquetTimeUnit::MICROS || unit == ParquetTimeUnit::NANOS) &&
                             field.physical_type == tparquet::Type::INT64),
                    "time");
        }
        if (logical.__isset.TIMESTAMP) {
            return require(
                    field.physical_type == tparquet::Type::INT64 &&
                            parquet_time_unit(logical.TIMESTAMP.unit) != ParquetTimeUnit::UNKNOWN,
                    "timestamp");
        }
        if (logical.__isset.INTEGER) {
            const int width = logical.INTEGER.bitWidth;
            return require(((width == 8 || width == 16 || width == 32) &&
                            field.physical_type == tparquet::Type::INT32) ||
                                   (width == 64 && field.physical_type == tparquet::Type::INT64),
                           "integer");
        }
        if (logical.__isset.UUID) {
            return require(field.physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY &&
                                   schema.type_length == 16,
                           "UUID");
        }
        if (logical.__isset.FLOAT16) {
            return require(field.physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY &&
                                   schema.type_length == 2,
                           "FLOAT16");
        }
        if (logical.__isset.GEOMETRY || logical.__isset.GEOGRAPHY) {
            // Geospatial WKB has no Doris logical mapping here; keep its BYTE_ARRAY payload raw so
            // reader validation preserves the physical fallback chosen by native schema inference.
            return require(field.physical_type == tparquet::Type::BYTE_ARRAY, "geospatial");
        }
        if (logical.__isset.UNKNOWN) {
            return Status::OK();
        }
        return Status::Corruption("Unsupported Parquet logical annotation on field {}", field.name);
    }

    if (!schema.__isset.converted_type) {
        return Status::OK();
    }
    switch (schema.converted_type) {
    case tparquet::ConvertedType::UTF8:
    case tparquet::ConvertedType::ENUM:
    case tparquet::ConvertedType::JSON:
    case tparquet::ConvertedType::BSON:
        return require(field.physical_type == tparquet::Type::BYTE_ARRAY, "converted string");
    case tparquet::ConvertedType::DECIMAL:
        return validate_decimal_physical_type(field,
                                              schema.__isset.precision ? schema.precision : -1,
                                              schema.__isset.scale ? schema.scale : -1);
    case tparquet::ConvertedType::DATE:
    case tparquet::ConvertedType::TIME_MILLIS:
    case tparquet::ConvertedType::UINT_8:
    case tparquet::ConvertedType::UINT_16:
    case tparquet::ConvertedType::UINT_32:
    case tparquet::ConvertedType::INT_8:
    case tparquet::ConvertedType::INT_16:
    case tparquet::ConvertedType::INT_32:
        return require(field.physical_type == tparquet::Type::INT32, "converted INT32");
    case tparquet::ConvertedType::TIME_MICROS:
    case tparquet::ConvertedType::TIMESTAMP_MILLIS:
    case tparquet::ConvertedType::TIMESTAMP_MICROS:
    case tparquet::ConvertedType::UINT_64:
    case tparquet::ConvertedType::INT_64:
        return require(field.physical_type == tparquet::Type::INT64, "converted INT64");
    case tparquet::ConvertedType::INTERVAL:
        return require(field.physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY &&
                               schema.type_length == 12,
                       "interval");
    default:
        return Status::Corruption("Unsupported Parquet converted annotation {} on field {}",
                                  tparquet::to_string(schema.converted_type), field.name);
    }
}

IColumn::Filter* conversion_failure_map(const NativeFieldSchema& field,
                                        const DataTypePtr& target_type, bool strict_mode,
                                        IColumn::Filter* output_null_map,
                                        IColumn::Filter* compatibility_scratch) {
    const auto& schema = field.parquet_schema;
    const bool is_utc_timestamp =
            field.physical_type == tparquet::Type::INT96 ||
            (field.physical_type == tparquet::Type::INT64 &&
             ((schema.__isset.logicalType && schema.logicalType.__isset.TIMESTAMP &&
               schema.logicalType.TIMESTAMP.isAdjustedToUTC) ||
              (schema.__isset.converted_type &&
               (schema.converted_type == tparquet::ConvertedType::TIMESTAMP_MILLIS ||
                schema.converted_type == tparquet::ConvertedType::TIMESTAMP_MICROS))));
    if (!strict_mode && output_null_map != nullptr && is_utc_timestamp &&
        remove_nullable(target_type)->get_primitive_type() == TYPE_DATETIMEV2) {
        // Legacy UTC timestamp conversion kept out-of-range values as DATETIME defaults. Local
        // timestamps intentionally keep non-strict NULL-on-overflow behavior because timezone
        // conversion is not part of their representation.
        compatibility_scratch->resize_fill(output_null_map->size(), 0);
        return compatibility_scratch;
    }
    return output_null_map;
}

void mark_local_timestamp_defaults(const NativeFieldSchema& field, const DataTypePtr& target_type,
                                   bool strict_mode, IColumn& data_column,
                                   IColumn::Filter* output_null_map, size_t start_row) {
    const auto& schema = field.parquet_schema;
    const bool is_local_timestamp =
            field.physical_type == tparquet::Type::INT64 && schema.__isset.logicalType &&
            schema.logicalType.__isset.TIMESTAMP && !schema.logicalType.TIMESTAMP.isAdjustedToUTC;
    if (strict_mode || output_null_map == nullptr || !is_local_timestamp ||
        remove_nullable(target_type)->get_primitive_type() != TYPE_DATETIMEV2) {
        return;
    }
    auto& values = assert_cast<ColumnDateTimeV2&>(data_column).get_data();
    DORIS_CHECK_EQ(values.size(), output_null_map->size());
    for (size_t row = start_row; row < values.size(); ++row) {
        if ((*output_null_map)[row] == 0 && !values[row].is_valid_date()) {
            // Local timestamps before Doris' representable calendar can materialize as a zero
            // date without a SerDe error. Preserve non-strict scan semantics by nulling only that
            // sentinel; physical NULLs and valid local timestamps keep their original map bits.
            (*output_null_map)[row] = 1;
        }
    }
}

Status init_decode_context(const NativeFieldSchema& field, const cctz::time_zone* ctz,
                           ParquetDecodeContext* context) {
    DORIS_CHECK(context != nullptr);
    // Annotation validation belongs before decoder construction: accepting an impossible pair
    // lets the logical SerDe reinterpret a differently sized physical value stream.
    RETURN_IF_ERROR(validate_physical_annotation(field));
    switch (field.physical_type) {
    case tparquet::Type::BOOLEAN:
        context->physical_type = ParquetPhysicalType::BOOLEAN;
        break;
    case tparquet::Type::INT32:
        context->physical_type = ParquetPhysicalType::INT32;
        break;
    case tparquet::Type::INT64:
        context->physical_type = ParquetPhysicalType::INT64;
        break;
    case tparquet::Type::INT96:
        context->physical_type = ParquetPhysicalType::INT96;
        break;
    case tparquet::Type::FLOAT:
        context->physical_type = ParquetPhysicalType::FLOAT;
        break;
    case tparquet::Type::DOUBLE:
        context->physical_type = ParquetPhysicalType::DOUBLE;
        break;
    case tparquet::Type::BYTE_ARRAY:
        context->physical_type = ParquetPhysicalType::BYTE_ARRAY;
        break;
    case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
        context->physical_type = ParquetPhysicalType::FIXED_LEN_BYTE_ARRAY;
        break;
    default:
        return Status::NotSupported("Unsupported Parquet physical type {}",
                                    tparquet::to_string(field.physical_type));
    }

    const auto& schema = field.parquet_schema;
    context->type_length = schema.__isset.type_length ? schema.type_length : -1;
    context->decimal_precision = schema.__isset.precision ? schema.precision : -1;
    context->decimal_scale = schema.__isset.scale ? schema.scale : -1;
    context->timezone = ctz;
    if (schema.__isset.logicalType) {
        const auto& logical = schema.logicalType;
        if (logical.__isset.STRING || logical.__isset.ENUM || logical.__isset.JSON ||
            logical.__isset.BSON) {
            context->logical_type = ParquetLogicalType::STRING;
        } else if (logical.__isset.DECIMAL) {
            context->logical_type = ParquetLogicalType::DECIMAL;
            context->decimal_precision = logical.DECIMAL.precision;
            context->decimal_scale = logical.DECIMAL.scale;
        } else if (logical.__isset.DATE) {
            context->logical_type = ParquetLogicalType::DATE;
        } else if (logical.__isset.TIME) {
            context->logical_type = ParquetLogicalType::TIME;
            context->time_unit = parquet_time_unit(logical.TIME.unit);
        } else if (logical.__isset.TIMESTAMP) {
            context->logical_type = ParquetLogicalType::TIMESTAMP;
            context->time_unit = parquet_time_unit(logical.TIMESTAMP.unit);
            context->timestamp_is_adjusted_to_utc = logical.TIMESTAMP.isAdjustedToUTC;
        } else if (logical.__isset.INTEGER) {
            context->logical_type = ParquetLogicalType::INTEGER;
            context->logical_integer_bit_width = logical.INTEGER.bitWidth;
            context->logical_integer_is_signed = logical.INTEGER.isSigned;
        } else if (logical.__isset.UUID) {
            context->logical_type = ParquetLogicalType::UUID;
            context->logical_uuid = true;
        } else if (logical.__isset.FLOAT16) {
            context->logical_type = ParquetLogicalType::FLOAT16;
            context->logical_float16 = true;
        }
        if (context->logical_uuid &&
            (context->physical_type != ParquetPhysicalType::FIXED_LEN_BYTE_ARRAY ||
             context->type_length != 16)) {
            return Status::Corruption("Parquet UUID field {} must be FIXED_LEN_BYTE_ARRAY(16)",
                                      field.name);
        }
        if (context->logical_float16 &&
            (context->physical_type != ParquetPhysicalType::FIXED_LEN_BYTE_ARRAY ||
             context->type_length != 2)) {
            return Status::Corruption("Parquet FLOAT16 field {} must be FIXED_LEN_BYTE_ARRAY(2)",
                                      field.name);
        }
        return Status::OK();
    }

    if (!schema.__isset.converted_type) {
        return Status::OK();
    }
    switch (schema.converted_type) {
    case tparquet::ConvertedType::UTF8:
    case tparquet::ConvertedType::ENUM:
    case tparquet::ConvertedType::JSON:
    case tparquet::ConvertedType::BSON:
        context->logical_type = ParquetLogicalType::STRING;
        break;
    case tparquet::ConvertedType::DECIMAL:
        context->logical_type = ParquetLogicalType::DECIMAL;
        break;
    case tparquet::ConvertedType::DATE:
        context->logical_type = ParquetLogicalType::DATE;
        break;
    case tparquet::ConvertedType::TIME_MILLIS:
        context->logical_type = ParquetLogicalType::TIME;
        context->time_unit = ParquetTimeUnit::MILLIS;
        break;
    case tparquet::ConvertedType::TIME_MICROS:
        context->logical_type = ParquetLogicalType::TIME;
        context->time_unit = ParquetTimeUnit::MICROS;
        break;
    case tparquet::ConvertedType::TIMESTAMP_MILLIS:
        context->logical_type = ParquetLogicalType::TIMESTAMP;
        context->time_unit = ParquetTimeUnit::MILLIS;
        // Legacy converted timestamps are defined as UTC-adjusted, unlike an unannotated INT64.
        context->timestamp_is_adjusted_to_utc = true;
        break;
    case tparquet::ConvertedType::TIMESTAMP_MICROS:
        context->logical_type = ParquetLogicalType::TIMESTAMP;
        context->time_unit = ParquetTimeUnit::MICROS;
        context->timestamp_is_adjusted_to_utc = true;
        break;
    case tparquet::ConvertedType::UINT_8:
    case tparquet::ConvertedType::UINT_16:
    case tparquet::ConvertedType::UINT_32:
    case tparquet::ConvertedType::UINT_64:
    case tparquet::ConvertedType::INT_8:
    case tparquet::ConvertedType::INT_16:
    case tparquet::ConvertedType::INT_32:
    case tparquet::ConvertedType::INT_64:
        context->logical_type = ParquetLogicalType::INTEGER;
        context->logical_integer_is_signed =
                schema.converted_type >= tparquet::ConvertedType::INT_8;
        context->logical_integer_bit_width =
                schema.converted_type == tparquet::ConvertedType::UINT_8 ||
                                schema.converted_type == tparquet::ConvertedType::INT_8
                        ? 8
                : schema.converted_type == tparquet::ConvertedType::UINT_16 ||
                                schema.converted_type == tparquet::ConvertedType::INT_16
                        ? 16
                : schema.converted_type == tparquet::ConvertedType::UINT_32 ||
                                schema.converted_type == tparquet::ConvertedType::INT_32
                        ? 32
                        : 64;
        break;
    default:
        break;
    }
    return Status::OK();
}

} // namespace

#ifdef BE_TEST
Status init_decode_context_for_test(const NativeFieldSchema& field, const cctz::time_zone* ctz,
                                    ParquetDecodeContext* context) {
    return init_decode_context(field, ctz, context);
}

bool preserves_timestamp_conversion_default_for_test(const NativeFieldSchema& field,
                                                     const DataTypePtr& target_type,
                                                     bool strict_mode) {
    IColumn::Filter output_null_map;
    output_null_map.resize_fill(1, 0);
    IColumn::Filter compatibility_scratch;
    return conversion_failure_map(field, target_type, strict_mode, &output_null_map,
                                  &compatibility_scratch) == &compatibility_scratch;
}

void mark_local_timestamp_defaults_for_test(const NativeFieldSchema& field,
                                            const DataTypePtr& target_type, bool strict_mode,
                                            IColumn& data_column, IColumn::Filter* output_null_map,
                                            size_t start_row) {
    mark_local_timestamp_defaults(field, target_type, strict_mode, data_column, output_null_map,
                                  start_row);
}
#endif

static void fill_struct_null_map(NativeFieldSchema* field, NullMap& null_map,
                                 const std::vector<level_t>& rep_levels,
                                 const std::vector<level_t>& def_levels) {
    size_t num_levels = def_levels.size();
    DCHECK_EQ(num_levels, rep_levels.size());
    size_t origin_size = null_map.size();
    null_map.resize(origin_size + num_levels);
    size_t pos = origin_size;
    for (size_t i = 0; i < num_levels; ++i) {
        // skip the levels affect its ancestor or its descendants
        if (def_levels[i] < field->repeated_parent_def_level ||
            rep_levels[i] > field->repetition_level) {
            continue;
        }
        if (def_levels[i] >= field->definition_level) {
            null_map[pos++] = 0;
        } else {
            null_map[pos++] = 1;
        }
    }
    null_map.resize(pos);
}

static Status fill_array_offset(NativeFieldSchema* field, ColumnArray::Offsets64& offsets_data,
                                NullMap* null_map_ptr, const std::vector<level_t>& rep_levels,
                                const std::vector<level_t>& def_levels) {
    size_t num_levels = rep_levels.size();
    if (UNLIKELY(num_levels != def_levels.size())) {
        return Status::Corruption("Parquet repetition and definition level counts differ");
    }
    size_t origin_size = offsets_data.size();
    offsets_data.resize(origin_size + num_levels);
    if (null_map_ptr != nullptr) {
        null_map_ptr->resize(origin_size + num_levels);
    }
    size_t offset_pos = origin_size - 1;
    bool parent_opened = false;
    for (size_t i = 0; i < num_levels; ++i) {
        // skip the levels affect its ancestor or its descendants
        if (def_levels[i] < field->repeated_parent_def_level ||
            rep_levels[i] > field->repetition_level) {
            continue;
        }
        if (rep_levels[i] == field->repetition_level) {
            // A continuation can extend only a parent opened by this aligned logical batch.
            if (UNLIKELY(!parent_opened)) {
                return Status::Corruption(
                        "Parquet collection starts with an orphan repetition continuation");
            }
            offsets_data[offset_pos]++;
            continue;
        }
        parent_opened = true;
        offset_pos++;
        offsets_data[offset_pos] = offsets_data[offset_pos - 1];
        if (def_levels[i] >= field->definition_level) {
            offsets_data[offset_pos]++;
        }
        if (null_map_ptr != nullptr) {
            if (def_levels[i] >= field->definition_level - 1) {
                (*null_map_ptr)[offset_pos] = 0;
            } else {
                (*null_map_ptr)[offset_pos] = 1;
            }
        }
    }
    offsets_data.resize(offset_pos + 1);
    if (null_map_ptr != nullptr) {
        null_map_ptr->resize(offset_pos + 1);
    }
    return Status::OK();
}

Status ColumnReader::create(io::FileReaderSPtr file, NativeFieldSchema* field,
                            const tparquet::RowGroup& row_group, const RowRanges& row_ranges,
                            const cctz::time_zone* ctz, io::IOContext* io_ctx,
                            std::unique_ptr<ColumnReader>& reader, size_t max_buf_size,
                            const std::unordered_map<int, tparquet::OffsetIndex>& col_offsets,
                            RuntimeState* state, bool in_collection,
                            const std::set<uint64_t>& column_ids,
                            const std::set<uint64_t>& filter_column_ids,
                            const std::string& page_cache_file_key,
                            const ParquetReaderCompat& compat, bool enable_strict_mode) {
    size_t total_rows = row_group.num_rows;
    if (field->data_type->get_primitive_type() == TYPE_ARRAY) {
        std::unique_ptr<ColumnReader> element_reader;
        RETURN_IF_ERROR(create(file, &field->children[0], row_group, row_ranges, ctz, io_ctx,
                               element_reader, max_buf_size, col_offsets, state, true, column_ids,
                               filter_column_ids, page_cache_file_key, compat, enable_strict_mode));
        auto array_reader = ArrayColumnReader::create_unique(row_ranges, total_rows, ctz, io_ctx);
        element_reader->set_column_in_nested();
        RETURN_IF_ERROR(array_reader->init(std::move(element_reader), field));
        array_reader->_filter_column_ids = filter_column_ids;
        reader.reset(array_reader.release());
    } else if (field->data_type->get_primitive_type() == TYPE_MAP) {
        std::unique_ptr<ColumnReader> key_reader;
        std::unique_ptr<ColumnReader> value_reader;

        if (column_ids.empty() ||
            column_ids.find(field->children[0].get_column_id()) != column_ids.end()) {
            // Create key reader
            RETURN_IF_ERROR(create(file, &field->children[0], row_group, row_ranges, ctz, io_ctx,
                                   key_reader, max_buf_size, col_offsets, state, true, column_ids,
                                   filter_column_ids, page_cache_file_key, compat,
                                   enable_strict_mode));
        } else {
            auto skip_reader = std::make_unique<SkipReadingReader>(row_ranges, total_rows, ctz,
                                                                   io_ctx, &field->children[0]);
            key_reader = std::move(skip_reader);
        }

        if (column_ids.empty() ||
            column_ids.find(field->children[1].get_column_id()) != column_ids.end()) {
            // Create value reader
            RETURN_IF_ERROR(create(file, &field->children[1], row_group, row_ranges, ctz, io_ctx,
                                   value_reader, max_buf_size, col_offsets, state, true, column_ids,
                                   filter_column_ids, page_cache_file_key, compat,
                                   enable_strict_mode));
        } else {
            auto skip_reader = std::make_unique<SkipReadingReader>(row_ranges, total_rows, ctz,
                                                                   io_ctx, &field->children[1]);
            value_reader = std::move(skip_reader);
        }

        auto map_reader = MapColumnReader::create_unique(row_ranges, total_rows, ctz, io_ctx);
        key_reader->set_column_in_nested();
        value_reader->set_column_in_nested();
        RETURN_IF_ERROR(map_reader->init(std::move(key_reader), std::move(value_reader), field));
        map_reader->_filter_column_ids = filter_column_ids;
        reader.reset(map_reader.release());
    } else if (field->data_type->get_primitive_type() == TYPE_STRUCT) {
        std::unordered_map<std::string, std::unique_ptr<ColumnReader>> child_readers;
        child_readers.reserve(field->children.size());
        int non_skip_reader_idx = -1;
        for (int i = 0; i < field->children.size(); ++i) {
            auto& child = field->children[i];
            std::unique_ptr<ColumnReader> child_reader;
            if (column_ids.empty() || column_ids.find(child.get_column_id()) != column_ids.end()) {
                RETURN_IF_ERROR(create(file, &child, row_group, row_ranges, ctz, io_ctx,
                                       child_reader, max_buf_size, col_offsets, state,
                                       in_collection, column_ids, filter_column_ids,
                                       page_cache_file_key, compat, enable_strict_mode));
                child_readers[child.name] = std::move(child_reader);
                // Record the first non-SkippingReader
                if (non_skip_reader_idx == -1) {
                    non_skip_reader_idx = i;
                }
            } else {
                auto skip_reader = std::make_unique<SkipReadingReader>(row_ranges, total_rows, ctz,
                                                                       io_ctx, &child);
                skip_reader->_filter_column_ids = filter_column_ids;
                child_readers[child.name] = std::move(skip_reader);
            }
            child_readers[child.name]->set_column_in_nested();
        }
        // If all children are SkipReadingReader, force the first child to call create
        if (non_skip_reader_idx == -1) {
            std::unique_ptr<ColumnReader> child_reader;
            RETURN_IF_ERROR(create(file, &field->children[0], row_group, row_ranges, ctz, io_ctx,
                                   child_reader, max_buf_size, col_offsets, state, in_collection,
                                   column_ids, filter_column_ids, page_cache_file_key, compat,
                                   enable_strict_mode));
            child_reader->set_column_in_nested();
            child_readers[field->children[0].name] = std::move(child_reader);
        }
        auto struct_reader = StructColumnReader::create_unique(row_ranges, total_rows, ctz, io_ctx);
        RETURN_IF_ERROR(struct_reader->init(std::move(child_readers), field));
        struct_reader->_filter_column_ids = filter_column_ids;
        reader.reset(struct_reader.release());
    } else {
        auto physical_index = field->physical_column_index;
        if (physical_index < 0 || static_cast<size_t>(physical_index) >= row_group.columns.size()) {
            // Keep the leaf-to-chunk invariant checked at this consumer too because unit callers
            // can construct a reader without going through NativeParquetMetadata::init_schema().
            return Status::Corruption("Parquet physical column index {} is out of range {}",
                                      physical_index, row_group.columns.size());
        }
        const auto offset_it = col_offsets.find(physical_index);
        const tparquet::OffsetIndex* offset_index =
                offset_it != col_offsets.end() ? &offset_it->second : nullptr;

        const tparquet::ColumnChunk& chunk = row_group.columns[physical_index];
        if (!chunk.__isset.meta_data) {
            return Status::Corruption("Parquet physical column {} has no chunk metadata",
                                      physical_index);
        }
        if (in_collection) {
            if (offset_index == nullptr) {
                auto scalar_reader = ScalarColumnReader<true, false>::create_unique(
                        row_ranges, total_rows, chunk, offset_index, ctz, io_ctx);

                RETURN_IF_ERROR(scalar_reader->init(file, field, max_buf_size, state,
                                                    page_cache_file_key, compat,
                                                    enable_strict_mode));
                scalar_reader->_filter_column_ids = filter_column_ids;
                reader.reset(scalar_reader.release());
            } else {
                auto scalar_reader = ScalarColumnReader<true, true>::create_unique(
                        row_ranges, total_rows, chunk, offset_index, ctz, io_ctx);

                RETURN_IF_ERROR(scalar_reader->init(file, field, max_buf_size, state,
                                                    page_cache_file_key, compat,
                                                    enable_strict_mode));
                scalar_reader->_filter_column_ids = filter_column_ids;
                reader.reset(scalar_reader.release());
            }
        } else {
            if (offset_index == nullptr) {
                auto scalar_reader = ScalarColumnReader<false, false>::create_unique(
                        row_ranges, total_rows, chunk, offset_index, ctz, io_ctx);

                RETURN_IF_ERROR(scalar_reader->init(file, field, max_buf_size, state,
                                                    page_cache_file_key, compat,
                                                    enable_strict_mode));
                scalar_reader->_filter_column_ids = filter_column_ids;
                reader.reset(scalar_reader.release());
            } else {
                auto scalar_reader = ScalarColumnReader<false, true>::create_unique(
                        row_ranges, total_rows, chunk, offset_index, ctz, io_ctx);

                RETURN_IF_ERROR(scalar_reader->init(file, field, max_buf_size, state,
                                                    page_cache_file_key, compat,
                                                    enable_strict_mode));
                scalar_reader->_filter_column_ids = filter_column_ids;
                reader.reset(scalar_reader.release());
            }
        }
    }
    return Status::OK();
}

void ColumnReader::_generate_read_ranges(RowRange page_row_range, RowRanges* result_ranges) const {
    result_ranges->add(page_row_range);
    RowRanges::ranges_intersection(*result_ranges, _row_ranges, result_ranges);
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::init(
        io::FileReaderSPtr file, NativeFieldSchema* field, size_t max_buf_size, RuntimeState* state,
        const std::string& page_cache_file_key, const ParquetReaderCompat& compat,
        bool enable_strict_mode) {
    _field_schema = field;
    auto& chunk_meta = _chunk_meta.meta_data;
    ColumnChunkRange chunk_range;
    RETURN_IF_ERROR(compute_column_chunk_range(chunk_meta, file->size(), compat.parquet_816_padding,
                                               &chunk_range));
    const size_t chunk_start = chunk_range.offset;
    const size_t chunk_len = chunk_range.length;
    size_t prefetch_buffer_size = std::min(chunk_len, max_buf_size);
    if ((typeid_cast<doris::io::TracingFileReader*>(file.get()) &&
         typeid_cast<io::MergeRangeFileReader*>(
                 ((doris::io::TracingFileReader*)(file.get()))->inner_reader().get())) ||
        typeid_cast<io::MergeRangeFileReader*>(file.get())) {
        // turn off prefetch data when using MergeRangeFileReader
        prefetch_buffer_size = 0;
    }
    _stream_reader = std::make_unique<io::BufferedFileStreamReader>(file, chunk_start, chunk_len,
                                                                    prefetch_buffer_size);
    ParquetPageReadContext ctx(
            (state == nullptr) ? true : state->query_options().enable_parquet_file_page_cache,
            page_cache_file_key, compat.data_page_v2_always_compressed);

    _chunk_reader = std::make_unique<ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>>(
            _stream_reader.get(), &_chunk_meta, field, _offset_index, _total_rows, _io_ctx, ctx,
            &chunk_range);
    _materialization_state.enable_strict_mode = enable_strict_mode;
    RETURN_IF_ERROR(_chunk_reader->init());
    RETURN_IF_ERROR(init_decode_context(*field, _ctz, &_decode_context));
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
void ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::release_batch_scratch(
        size_t max_retained_bytes) {
    const size_t retained_bytes = retained_batch_scratch_bytes();
    const size_t active_bytes = active_batch_scratch_bytes();
    if (retained_bytes <= max_retained_bytes || active_bytes > max_retained_bytes) {
        _oversized_scratch_idle_batches = 0;
        return;
    }
    // An adaptive probe or one repeated outlier must not pin memory forever, but immediately
    // dropping a large steady-state buffer makes every following batch allocate it again. Require
    // three ordinary batches before treating an oversized capacity as idle.
    constexpr uint8_t OVERSIZED_SCRATCH_IDLE_BATCHES = 3;
    if (++_oversized_scratch_idle_batches < OVERSIZED_SCRATCH_IDLE_BATCHES) {
        return;
    }
    _oversized_scratch_idle_batches = 0;
    if (_chunk_reader != nullptr) {
        // Persistent decoders also own batch-sized value/slice buffers, not only the reader.
        _chunk_reader->release_decoder_scratch(max_retained_bytes);
    }
    bool release_selection = false;
    release_selection |= release_vector_if_oversized(&_rep_levels, max_retained_bytes);
    release_selection |= release_vector_if_oversized(&_def_levels, max_retained_bytes);
    release_selection |= release_vector_if_oversized(&_null_run_lengths, max_retained_bytes);
    release_selection |= release_vector_if_oversized(&_nested_filter_map_data, max_retained_bytes);
    release_selection |= release_vector_if_oversized(&_materialization_state.dictionary_indices,
                                                     max_retained_bytes);
    release_selection |= release_vector_if_oversized(&_materialization_state.selection.ranges,
                                                     max_retained_bytes);
    if (retained_set_bytes(_ancestor_null_indices) > max_retained_bytes) {
        std::unordered_set<size_t>().swap(_ancestor_null_indices);
        release_selection = true;
    }
    if (release_selection) {
        _select_vector = ColumnSelectVector();
    }
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
size_t ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::retained_batch_scratch_bytes() const {
    const size_t decoder_bytes =
            _chunk_reader == nullptr ? 0 : _chunk_reader->retained_decoder_scratch_bytes();
    return decoder_bytes + _rep_levels.capacity() * sizeof(level_t) +
           _def_levels.capacity() * sizeof(level_t) +
           _null_run_lengths.capacity() * sizeof(uint16_t) +
           _nested_filter_map_data.capacity() * sizeof(uint8_t) +
           _materialization_state.dictionary_indices.capacity() * sizeof(uint32_t) +
           _materialization_state.selection.ranges.capacity() * sizeof(ParquetSelectionRange) +
           retained_set_bytes(_ancestor_null_indices);
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
size_t ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::active_batch_scratch_bytes() const {
    const size_t decoder_bytes =
            _chunk_reader == nullptr ? 0 : _chunk_reader->active_decoder_scratch_bytes();
    return decoder_bytes + _rep_levels.size() * sizeof(level_t) +
           _def_levels.size() * sizeof(level_t) + _null_run_lengths.size() * sizeof(uint16_t) +
           _nested_filter_map_data.size() * sizeof(uint8_t) +
           _materialization_state.dictionary_indices.size() * sizeof(uint32_t) +
           _materialization_state.selection.ranges.size() * sizeof(ParquetSelectionRange) +
           _ancestor_null_indices.size() * sizeof(size_t);
}

#ifdef BE_TEST
template <bool IN_COLLECTION, bool OFFSET_INDEX>
void ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::reserve_batch_scratch_for_test(
        size_t elements) {
    _rep_levels.reserve(elements);
    _def_levels.reserve(elements);
    _null_run_lengths.reserve(elements);
    _nested_filter_map_data.reserve(elements);
    _materialization_state.dictionary_indices.reserve(elements);
    _materialization_state.selection.ranges.reserve(elements);
    _ancestor_null_indices.reserve(elements);
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
size_t ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::retained_batch_scratch_bytes_for_test()
        const {
    return retained_batch_scratch_bytes();
}
#endif

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::_skip_values(size_t num_values) {
    if (num_values == 0) {
        return Status::OK();
    }
    if (_chunk_reader->max_def_level() > 0) {
        LevelDecoder& def_decoder = _chunk_reader->def_level_decoder();
        size_t skipped = 0;
        size_t null_size = 0;
        size_t nonnull_size = 0;
        while (skipped < num_values) {
            level_t def_level = -1;
            size_t loop_skip = def_decoder.get_next_run(&def_level, num_values - skipped);
            if (loop_skip == 0) {
                return Status::Corruption("Parquet definition level stream ended while skipping");
            }
            if (def_level < _field_schema->definition_level) {
                null_size += loop_skip;
            } else {
                nonnull_size += loop_skip;
            }
            skipped += loop_skip;
        }
        if (null_size > 0) {
            RETURN_IF_ERROR(_chunk_reader->skip_values(null_size, false));
        }
        if (nonnull_size > 0) {
            RETURN_IF_ERROR(_chunk_reader->skip_values(nonnull_size, true));
        }
    } else {
        RETURN_IF_ERROR(_chunk_reader->skip_values(num_values));
    }
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::_read_values(size_t num_values,
                                                                     ColumnPtr& doris_column,
                                                                     const DataTypePtr& type,
                                                                     FilterMap& filter_map,
                                                                     bool is_dict_filter) {
    if (num_values == 0) {
        return Status::OK();
    }
    MutableColumnPtr data_column;
    _null_run_lengths.clear();
    NullMap* map_data_column = nullptr;
    doris_column = IColumn::mutate(std::move(doris_column));
    if (is_column_nullable(*doris_column)) {
        SCOPED_RAW_TIMER(&_decode_null_map_time);
        auto mutable_column = doris_column->assert_mutable();
        auto* nullable_column = assert_cast<ColumnNullable*>(mutable_column.get());

        data_column = nullable_column->get_nested_column_ptr();
        map_data_column = &(nullable_column->get_null_map_data());
        if (_chunk_reader->max_def_level() > 0) {
            LevelDecoder& def_decoder = _chunk_reader->def_level_decoder();
            size_t has_read = 0;
            bool prev_is_null = true;
            while (has_read < num_values) {
                level_t def_level;
                size_t loop_read = def_decoder.get_next_run(&def_level, num_values - has_read);
                if (loop_read == 0) {
                    return Status::Corruption(
                            "Parquet definition level stream ended while materializing");
                }

                bool is_null = def_level < _field_schema->definition_level;
                if (!(prev_is_null ^ is_null)) {
                    _null_run_lengths.emplace_back(0);
                }
                size_t remaining = loop_read;
                while (remaining > USHRT_MAX) {
                    _null_run_lengths.emplace_back(USHRT_MAX);
                    _null_run_lengths.emplace_back(0);
                    remaining -= USHRT_MAX;
                }
                _null_run_lengths.emplace_back((u_short)remaining);
                prev_is_null = is_null;
                has_read += loop_read;
            }
        }
    } else {
        if (_chunk_reader->max_def_level() > 0) {
            return Status::Corruption("Not nullable column has null values in parquet file");
        }
        data_column = doris_column->assert_mutable();
    }
    if (_null_run_lengths.empty()) {
        size_t remaining = num_values;
        while (remaining > USHRT_MAX) {
            _null_run_lengths.emplace_back(USHRT_MAX);
            _null_run_lengths.emplace_back(0);
            remaining -= USHRT_MAX;
        }
        _null_run_lengths.emplace_back((u_short)remaining);
    }
    {
        SCOPED_RAW_TIMER(&_decode_null_map_time);
        RETURN_IF_ERROR(_select_vector.init(_null_run_lengths, num_values, map_data_column,
                                            &filter_map, _filter_map_index));
        _filter_map_index += num_values;
    }
    DORIS_CHECK(_serde != nullptr);
    // Keep selected-row cardinality stable: non-strict conversion failures append a nested
    // default and mark this matching nullable output row instead of shortening the column.
    IColumn::Filter compatibility_scratch;
    _materialization_state.conversion_failure_null_map =
            conversion_failure_map(*_field_schema, type, _materialization_state.enable_strict_mode,
                                   map_data_column, &compatibility_scratch);
    const size_t materialization_start_row = data_column->size();
    const auto status = _chunk_reader->materialize_values(data_column, *_serde, _decode_context,
                                                          _materialization_state, _select_vector);
    _materialization_state.conversion_failure_null_map = nullptr;
    if (status.ok()) {
        mark_local_timestamp_defaults(*_field_schema, type,
                                      _materialization_state.enable_strict_mode, *data_column,
                                      map_data_column, materialization_start_row);
    }
    return status;
}

/**
 * Load the nested column data of complex type.
 * A row of complex type may be stored across two(or more) pages, and the parameter `align_rows` indicates that
 * whether the reader should read the remaining value of the last row in previous page.
 */
template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::_read_nested_column(
        ColumnPtr& doris_column, const DataTypePtr& type, FilterMap& filter_map, size_t batch_size,
        size_t* read_rows, bool* eof, bool is_dict_filter) {
    _rep_levels.clear();
    _def_levels.clear();

    // Handle nullable columns
    MutableColumnPtr data_column;
    NullMap* map_data_column = nullptr;
    doris_column = IColumn::mutate(std::move(doris_column));
    if (is_column_nullable(*doris_column)) {
        SCOPED_RAW_TIMER(&_decode_null_map_time);
        auto mutable_column = doris_column->assert_mutable();
        auto* nullable_column = assert_cast<ColumnNullable*>(mutable_column.get());
        data_column = nullable_column->get_nested_column_ptr();
        map_data_column = &(nullable_column->get_null_map_data());
    } else {
        if (_field_schema->data_type->is_nullable()) {
            return Status::Corruption("Not nullable column has null values in parquet file");
        }
        data_column = doris_column->assert_mutable();
    }

    _null_run_lengths.clear();
    _ancestor_null_indices.clear();
    _nested_filter_map_data.clear();

    auto read_and_fill_data = [&](size_t before_rep_level_sz, size_t filter_map_index) {
        RETURN_IF_ERROR(_chunk_reader->fill_def(_def_levels));
        if (filter_map.has_filter()) {
            RETURN_IF_ERROR(gen_filter_map(filter_map, filter_map_index, before_rep_level_sz,
                                           _rep_levels.size(), _nested_filter_map_data,
                                           &_nested_filter_map));
        } else {
            RETURN_IF_ERROR(_nested_filter_map.init(
                    nullptr, _rep_levels.size() - before_rep_level_sz, false));
        }

        _null_run_lengths.clear();
        _ancestor_null_indices.clear();
        RETURN_IF_ERROR(gen_nested_null_map(before_rep_level_sz, _rep_levels.size(),
                                            _null_run_lengths, _ancestor_null_indices));

        {
            SCOPED_RAW_TIMER(&_decode_null_map_time);
            RETURN_IF_ERROR(_select_vector.init(
                    _null_run_lengths,
                    _rep_levels.size() - before_rep_level_sz - _ancestor_null_indices.size(),
                    map_data_column, &_nested_filter_map, 0, &_ancestor_null_indices));
        }

        DORIS_CHECK(_serde != nullptr);
        // Nested materialization must preserve the same value/null-map row alignment invariant.
        IColumn::Filter compatibility_scratch;
        _materialization_state.conversion_failure_null_map = conversion_failure_map(
                *_field_schema, type, _materialization_state.enable_strict_mode, map_data_column,
                &compatibility_scratch);
        const size_t materialization_start_row = data_column->size();
        const auto status = _chunk_reader->materialize_values(
                data_column, *_serde, _decode_context, _materialization_state, _select_vector);
        _materialization_state.conversion_failure_null_map = nullptr;
        if (status.ok()) {
            mark_local_timestamp_defaults(*_field_schema, type,
                                          _materialization_state.enable_strict_mode, *data_column,
                                          map_data_column, materialization_start_row);
        }
        RETURN_IF_ERROR(status);
        if (!_ancestor_null_indices.empty()) {
            RETURN_IF_ERROR(_chunk_reader->skip_values(_ancestor_null_indices.size(), false));
        }
        if (filter_map.has_filter()) {
            auto new_rep_sz = before_rep_level_sz;
            for (size_t idx = before_rep_level_sz; idx < _rep_levels.size(); idx++) {
                if (_nested_filter_map_data[idx - before_rep_level_sz]) {
                    _rep_levels[new_rep_sz] = _rep_levels[idx];
                    _def_levels[new_rep_sz] = _def_levels[idx];
                    new_rep_sz++;
                }
            }
            _rep_levels.resize(new_rep_sz);
            _def_levels.resize(new_rep_sz);
        }
        return Status::OK();
    };

    while (_current_range_idx < _row_ranges.range_size()) {
        size_t left_row =
                std::max(_current_row_index, _row_ranges.get_range_from(_current_range_idx));
        size_t right_row = std::min(left_row + batch_size - *read_rows,
                                    (size_t)_row_ranges.get_range_to(_current_range_idx));
        _current_row_index = left_row;
        RETURN_IF_ERROR(_chunk_reader->seek_to_nested_row(left_row));
        size_t load_rows = 0;
        bool cross_page = false;
        size_t before_rep_level_sz = _rep_levels.size();
        RETURN_IF_ERROR(_chunk_reader->load_page_nested_rows(_rep_levels, right_row - left_row,
                                                             &load_rows, &cross_page));
        if (UNLIKELY(right_row > left_row && load_rows == 0 && !cross_page)) {
            // A bounded V2/indexed page must advance at least one logical row; zero progress would
            // leave both range cursors unchanged and spin forever on corrupt repetition levels.
            return Status::Corruption("Parquet nested reader made no row progress");
        }
        RETURN_IF_ERROR(read_and_fill_data(before_rep_level_sz, _filter_map_index));
        _filter_map_index += load_rows;
        while (cross_page) {
            before_rep_level_sz = _rep_levels.size();
            RETURN_IF_ERROR(_chunk_reader->load_cross_page_nested_row(_rep_levels, &cross_page));
            RETURN_IF_ERROR(read_and_fill_data(before_rep_level_sz, _filter_map_index - 1));
        }
        *read_rows += load_rows;
        _current_row_index += load_rows;
        _current_range_idx += (_current_row_index == _row_ranges.get_range_to(_current_range_idx));
        if (*read_rows == batch_size) {
            break;
        }
    }
    *eof = _current_range_idx == _row_ranges.range_size();
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::_read_fixed_width_filter_values(
        size_t num_values, const VExprSPtrs& conjuncts, int column_id, FilterMap& filter_map,
        IColumn* projected_column, IColumn::Filter* row_filter) {
    DORIS_CHECK(row_filter != nullptr);
    _null_run_lengths.clear();
    if (_chunk_reader->max_def_level() > 0) {
        LevelDecoder& def_decoder = _chunk_reader->def_level_decoder();
        size_t has_read = 0;
        bool prev_is_null = true;
        while (has_read < num_values) {
            level_t def_level = -1;
            const size_t loop_read = def_decoder.get_next_run(&def_level, num_values - has_read);
            if (loop_read == 0) {
                return Status::Corruption(
                        "Parquet definition level stream ended while filtering fixed-width values");
            }
            const bool is_null = def_level < _field_schema->definition_level;
            if (!(prev_is_null ^ is_null)) {
                _null_run_lengths.emplace_back(0);
            }
            size_t remaining = loop_read;
            while (remaining > USHRT_MAX) {
                _null_run_lengths.emplace_back(USHRT_MAX);
                _null_run_lengths.emplace_back(0);
                remaining -= USHRT_MAX;
            }
            _null_run_lengths.emplace_back(cast_set<uint16_t>(remaining));
            prev_is_null = is_null;
            has_read += loop_read;
        }
    } else {
        size_t remaining = num_values;
        while (remaining > USHRT_MAX) {
            _null_run_lengths.emplace_back(USHRT_MAX);
            _null_run_lengths.emplace_back(0);
            remaining -= USHRT_MAX;
        }
        _null_run_lengths.emplace_back(cast_set<uint16_t>(remaining));
    }
    RETURN_IF_ERROR(_select_vector.init(_null_run_lengths, num_values, nullptr, &filter_map,
                                        _filter_map_index));
    _filter_map_index += num_values;
    bool used_filter = false;
    RETURN_IF_ERROR(_chunk_reader->filter_fixed_width_values(
            conjuncts, column_id, _select_vector, &_fixed_width_predicate_nulls,
            &_fixed_width_predicate_matches, projected_column, row_filter, &used_filter));
    // Chunk encodings are prevalidated before any definition level is consumed, so a false result
    // here would make a materializing fallback observe an advanced level cursor.
    DORIS_CHECK(used_filter);
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::read_fixed_width_filter(
        const VExprSPtrs& conjuncts, int column_id, FilterMap& filter_map, size_t batch_size,
        IColumn* projected_column, IColumn::Filter* row_filter, size_t* read_rows, bool* eof,
        bool* used_filter) {
    DORIS_CHECK(row_filter != nullptr);
    DORIS_CHECK(read_rows != nullptr);
    DORIS_CHECK(eof != nullptr);
    DORIS_CHECK(used_filter != nullptr);
    row_filter->clear();
    *read_rows = 0;
    *used_filter = false;
    if (_in_nested || conjuncts.empty()) {
        return Status::OK();
    }
    if (!std::ranges::all_of(conjuncts, [&](const auto& conjunct) {
            return conjunct != nullptr &&
                   conjunct->can_execute_on_raw_fixed_values(_field_schema->data_type, column_id);
        })) {
        return Status::OK();
    }
    // Validate every advertised value encoding before touching either cursor. A late fallback
    // cannot rewind definition levels or a previously decoded fixed-width page.
    const bool supported_encodings = std::ranges::all_of(
            _chunk_meta.meta_data.encodings, [&](const tparquet::Encoding::type encoding) {
                return ColumnChunkReader<IN_COLLECTION, OFFSET_INDEX>::
                               supports_raw_fixed_filter_encoding(encoding,
                                                                  _chunk_meta.meta_data.type) ||
                       encoding == tparquet::Encoding::RLE ||
                       encoding == tparquet::Encoding::BIT_PACKED;
            });
    if (!supported_encodings) {
        return Status::OK();
    }

    int64_t right_row = 0;
    if constexpr (OFFSET_INDEX == false) {
        RETURN_IF_ERROR(_chunk_reader->parse_page_header());
        right_row = _chunk_reader->page_end_row();
    } else {
        right_row = _chunk_reader->page_end_row();
    }
    RowRanges read_ranges;
    _generate_read_ranges(RowRange {_current_row_index, right_row}, &read_ranges);
    if (read_ranges.count() == 0) {
        _current_row_index = right_row;
    } else {
        RETURN_IF_ERROR(_chunk_reader->parse_page_header());
        RETURN_IF_ERROR(_chunk_reader->load_page_data_idempotent());
        if (!_chunk_reader->can_filter_fixed_width_values(conjuncts, column_id)) {
            return Status::OK();
        }
        size_t has_read = 0;
        for (size_t idx = 0; idx < read_ranges.range_size(); ++idx) {
            const auto range = read_ranges.get_range(idx);
            const size_t skip_values = range.from() - _current_row_index;
            RETURN_IF_ERROR(_skip_values(skip_values));
            _current_row_index += skip_values;
            const size_t values =
                    std::min(static_cast<size_t>(range.to() - range.from()), batch_size - has_read);
            IColumn::Filter fragment_filter;
            RETURN_IF_ERROR(_read_fixed_width_filter_values(
                    values, conjuncts, column_id, filter_map, projected_column, &fragment_filter));
            row_filter->insert(row_filter->end(), fragment_filter.begin(), fragment_filter.end());
            has_read += values;
            *read_rows += values;
            _current_row_index += values;
            if (has_read == batch_size) {
                break;
            }
        }
    }

    if (right_row == _current_row_index) {
        if (!_chunk_reader->has_next_page()) {
            *eof = true;
        } else {
            RETURN_IF_ERROR(_chunk_reader->next_page());
        }
    }
    *used_filter = true;
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::read_column_levels(FilterMap& filter_map,
                                                                           size_t batch_size,
                                                                           size_t* read_rows,
                                                                           bool* eof) {
    DORIS_CHECK(_in_nested);
    DORIS_CHECK(read_rows != nullptr);
    DORIS_CHECK(eof != nullptr);
    _rep_levels.clear();
    _def_levels.clear();
    *read_rows = 0;

    auto consume_level_segment = [&](size_t level_start, size_t filter_map_index) -> Status {
        RETURN_IF_ERROR(_chunk_reader->fill_def(_def_levels));
        // Advance the encoded value stream without constructing a temporary Doris column. The
        // definition levels identify the physical values that actually exist; dictionary skips
        // still validate every index.
        RETURN_IF_ERROR(_chunk_reader->skip_nested_values(_def_levels, level_start));
        if (!filter_map.has_filter()) {
            return Status::OK();
        }

        RETURN_IF_ERROR(gen_filter_map(filter_map, filter_map_index, level_start,
                                       _rep_levels.size(), _nested_filter_map_data,
                                       &_nested_filter_map));
        size_t write_index = level_start;
        for (size_t read_index = level_start; read_index < _rep_levels.size(); ++read_index) {
            if (_nested_filter_map_data[read_index - level_start] != 0) {
                _rep_levels[write_index] = _rep_levels[read_index];
                _def_levels[write_index] = _def_levels[read_index];
                ++write_index;
            }
        }
        _rep_levels.resize(write_index);
        _def_levels.resize(write_index);
        return Status::OK();
    };

    while (_current_range_idx < _row_ranges.range_size()) {
        const size_t left_row =
                std::max(_current_row_index, _row_ranges.get_range_from(_current_range_idx));
        const size_t right_row =
                std::min(left_row + batch_size - *read_rows,
                         static_cast<size_t>(_row_ranges.get_range_to(_current_range_idx)));
        _current_row_index = left_row;
        RETURN_IF_ERROR(_chunk_reader->seek_to_nested_row(left_row));

        size_t loaded_rows = 0;
        bool cross_page = false;
        size_t level_start = _rep_levels.size();
        RETURN_IF_ERROR(_chunk_reader->load_page_nested_rows(_rep_levels, right_row - left_row,
                                                             &loaded_rows, &cross_page));
        if (UNLIKELY(right_row > left_row && loaded_rows == 0 && !cross_page)) {
            // Keep the levels-only path under the same forward-progress invariant as value reads.
            return Status::Corruption("Parquet nested level reader made no row progress");
        }
        RETURN_IF_ERROR(consume_level_segment(level_start, _filter_map_index));
        _filter_map_index += loaded_rows;
        while (cross_page) {
            level_start = _rep_levels.size();
            RETURN_IF_ERROR(_chunk_reader->load_cross_page_nested_row(_rep_levels, &cross_page));
            RETURN_IF_ERROR(consume_level_segment(level_start, _filter_map_index - 1));
        }

        *read_rows += loaded_rows;
        _current_row_index += loaded_rows;
        _current_range_idx += (_current_row_index == _row_ranges.get_range_to(_current_range_idx));
        if (*read_rows == batch_size) {
            break;
        }
    }
    *eof = _current_range_idx == _row_ranges.range_size();
    return Status::OK();
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Result<MutableColumnPtr>
ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::materialize_dictionary_values(
        const ColumnInt32* dict_column, const DataTypePtr& target_type) {
    DORIS_CHECK(dict_column != nullptr);
    DORIS_CHECK(target_type != nullptr);
    Decoder* dictionary_decoder = _chunk_reader->dictionary_decoder();
    DORIS_CHECK(dictionary_decoder != nullptr);
    // Materialize the dictionary in the file-local logical type once. Both predicate evaluation
    // and matched-ID gathering must observe the same decimal scale, timestamp unit, and binary
    // interpretation as ordinary data-page decoding.
    const DataTypePtr dictionary_type = remove_nullable(target_type);
    const DataTypeSerDeSPtr dictionary_serde = dictionary_type->get_serde();
    // The probe is the first typed read for this reader. Publish its SerDe identity now so the
    // first dictionary-ID batch does not mistake initialization for a type change and drop cache.
    _serde_type = dictionary_type.get();
    _serde = dictionary_serde;
    if (_materialization_state.dictionary_generation !=
        dictionary_decoder->dictionary_generation()) {
        _materialization_state.typed_dictionary = dictionary_type->create_column();
        ParquetDecodeContext dictionary_context = _decode_context;
        dictionary_context.encoding = ParquetValueEncoding::DICTIONARY;
        dictionary_context.dictionary_index_only = false;
        auto status = dictionary_serde->read_parquet_dictionary(
                *_materialization_state.typed_dictionary, *dictionary_decoder, dictionary_context);
        if (!status.ok()) {
            return ResultError(std::move(status));
        }
        DORIS_CHECK_EQ(_materialization_state.typed_dictionary->size(),
                       dictionary_decoder->dictionary_size());
        _materialization_state.dictionary_generation = dictionary_decoder->dictionary_generation();
#ifdef BE_TEST
        ++_dictionary_materialization_count;
#endif
    }

    auto result = _materialization_state.typed_dictionary->clone_empty();
    const auto& source_indices = dict_column->get_data();
    auto& indices = _materialization_state.dictionary_indices;
    indices.resize(source_indices.size());
    for (size_t row = 0; row < source_indices.size(); ++row) {
        if (UNLIKELY(source_indices[row] < 0 ||
                     static_cast<size_t>(source_indices[row]) >=
                             _materialization_state.typed_dictionary->size())) {
            return ResultError(Status::Corruption(
                    "Parquet dictionary index {} at row {} exceeds dictionary size {}",
                    source_indices[row], row, _materialization_state.typed_dictionary->size()));
        }
        indices[row] = static_cast<uint32_t>(source_indices[row]);
    }
    result->insert_indices_from(*_materialization_state.typed_dictionary, indices.data(),
                                indices.data() + indices.size());
    return result;
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Result<MutableColumnPtr> ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::dictionary_values(
        const DataTypePtr& target_type) {
    bool has_dict = false;
    auto status = _chunk_reader->load_dictionary_page(&has_dict);
    if (!status.ok()) {
        return ResultError(std::move(status));
    }
    if (!has_dict) {
        return ResultError(Status::NotSupported("Parquet column has no reusable dictionary"));
    }
    Decoder* dictionary_decoder = _chunk_reader->dictionary_decoder();
    if (dictionary_decoder == nullptr || dictionary_decoder->dictionary_size() == 0) {
        return ResultError(Status::NotSupported("Parquet column has no reusable dictionary"));
    }
    auto ids = ColumnInt32::create();
    auto& data = ids->get_data();
    data.resize(dictionary_decoder->dictionary_size());
    for (size_t dictionary_id = 0; dictionary_id < data.size(); ++dictionary_id) {
        data[dictionary_id] = cast_set<int32_t>(dictionary_id);
    }
    // Materialize the typed dictionary once and keep it in _materialization_state. Later row-level
    // filtering decodes only ids and flattens surviving values from this same dictionary.
    return materialize_dictionary_values(ids.get(), target_type);
}

template <bool IN_COLLECTION, bool OFFSET_INDEX>
Status ScalarColumnReader<IN_COLLECTION, OFFSET_INDEX>::read_column_data(
        ColumnPtr& doris_column, const DataTypePtr& type,
        const std::shared_ptr<NativeSchemaNode>& root_node, FilterMap& filter_map,
        size_t batch_size, size_t* read_rows, bool* eof, bool is_dict_filter,
        int64_t real_column_size) {
    const DataTypePtr materialization_type = remove_nullable(type);
    const DataTypePtr file_type = remove_nullable(_field_schema->data_type);
    if (!is_dict_filter && !file_type->equals(*materialization_type)) {
        // File-to-table casts belong to ColumnMapper. Reaching this reader with a table type would
        // also evaluate file-local predicates against values produced under the wrong type.
        return Status::InternalError(
                "Native Parquet reader type mismatch for file column '{}': file={}, requested={}",
                _field_schema->name, file_type->get_name(), materialization_type->get_name());
    }

    const DataTypePtr serde_type = is_dict_filter ? file_type : materialization_type;
    const bool serde_type_changed = _serde_type != serde_type.get();
    if (serde_type_changed || _dictionary_index_only != is_dict_filter) {
        _serde_type = serde_type.get();
        _serde = serde_type->get_serde();
        _dictionary_index_only = is_dict_filter;
        // Switching between typed values and dictionary IDs does not invalidate the dictionary
        // materialized by the pruning probe. Preserve it unless the logical SerDe type changed.
        if (serde_type_changed) {
            _materialization_state.reset_dictionary();
        }
    }
    _decode_context.dictionary_index_only = is_dict_filter;

    _def_levels.clear();
    _rep_levels.clear();
    *read_rows = 0;

    if (_in_nested) {
        return _read_nested_column(doris_column, materialization_type, filter_map, batch_size,
                                   read_rows, eof, is_dict_filter);
    }

    int64_t right_row = 0;
    if constexpr (OFFSET_INDEX == false) {
        RETURN_IF_ERROR(_chunk_reader->parse_page_header());
        right_row = _chunk_reader->page_end_row();
    } else {
        right_row = _chunk_reader->page_end_row();
    }

    do {
        // generate the row ranges that should be read
        RowRanges read_ranges;
        _generate_read_ranges(RowRange {_current_row_index, right_row}, &read_ranges);
        if (read_ranges.count() == 0) {
            // skip the whole page
            _current_row_index = right_row;
        } else {
            bool skip_whole_batch = false;
            // Determining whether to skip page or batch will increase the calculation time.
            // When the filtering effect is greater than 60%, it is possible to skip the page or batch.
            if (filter_map.has_filter() && filter_map.filter_ratio() > 0.6) {
                // lazy read
                size_t remaining_num_values = read_ranges.count();
                if (batch_size >= remaining_num_values &&
                    filter_map.can_filter_all(remaining_num_values, _filter_map_index)) {
                    // We can skip the whole page if the remaining values are filtered by predicate columns
                    _filter_map_index += remaining_num_values;
                    _current_row_index = right_row;
                    *read_rows = remaining_num_values;
                    break;
                }
                skip_whole_batch = batch_size <= remaining_num_values &&
                                   filter_map.can_filter_all(batch_size, _filter_map_index);
                if (skip_whole_batch) {
                    _filter_map_index += batch_size;
                }
            }
            // load page data to decode or skip values
            RETURN_IF_ERROR(_chunk_reader->parse_page_header());
            RETURN_IF_ERROR(_chunk_reader->load_page_data_idempotent());
            size_t has_read = 0;
            for (size_t idx = 0; idx < read_ranges.range_size(); idx++) {
                auto range = read_ranges.get_range(idx);
                // generate the skipped values
                size_t skip_values = range.from() - _current_row_index;
                RETURN_IF_ERROR(_skip_values(skip_values));
                _current_row_index += skip_values;
                // generate the read values
                size_t read_values =
                        std::min((size_t)(range.to() - range.from()), batch_size - has_read);
                if (skip_whole_batch) {
                    RETURN_IF_ERROR(_skip_values(read_values));
                } else {
                    RETURN_IF_ERROR(_read_values(read_values, doris_column, materialization_type,
                                                 filter_map, is_dict_filter));
                }
                has_read += read_values;
                *read_rows += read_values;
                _current_row_index += read_values;
                if (has_read == batch_size) {
                    break;
                }
            }
        }
    } while (false);

    if (right_row == _current_row_index) {
        if (!_chunk_reader->has_next_page()) {
            *eof = true;
        } else {
            RETURN_IF_ERROR(_chunk_reader->next_page());
        }
    }

    return Status::OK();
}

Status ArrayColumnReader::init(std::unique_ptr<ColumnReader> element_reader,
                               NativeFieldSchema* field) {
    _field_schema = field;
    _element_reader = std::move(element_reader);
    return Status::OK();
}

Status ArrayColumnReader::read_column_data(ColumnPtr& doris_column, const DataTypePtr& type,
                                           const std::shared_ptr<NativeSchemaNode>& root_node,
                                           FilterMap& filter_map, size_t batch_size,
                                           size_t* read_rows, bool* eof, bool is_dict_filter,
                                           int64_t real_column_size) {
    MutableColumnPtr data_column;
    NullMap* null_map_ptr = nullptr;
    doris_column = IColumn::mutate(std::move(doris_column));
    if (is_column_nullable(*doris_column)) {
        auto mutable_column = doris_column->assert_mutable();
        auto* nullable_column = assert_cast<ColumnNullable*>(mutable_column.get());
        null_map_ptr = &nullable_column->get_null_map_data();
        data_column = nullable_column->get_nested_column_ptr();
    } else {
        if (_field_schema->data_type->is_nullable()) {
            return Status::Corruption("Not nullable column has null values in parquet file");
        }
        data_column = doris_column->assert_mutable();
    }
    if (type->get_primitive_type() != PrimitiveType::TYPE_ARRAY) {
        return Status::Corruption(
                "Wrong data type for column '{}', expected Array type, actual type: {}.",
                _field_schema->name, type->get_name());
    }

    ColumnPtr& element_column = assert_cast<ColumnArray&>(*data_column).get_data_ptr();
    const DataTypePtr& element_type =
            (assert_cast<const DataTypeArray*>(remove_nullable(type).get()))->get_nested_type();
    // read nested column
    RETURN_IF_ERROR(_element_reader->read_column_data(element_column, element_type,
                                                      root_node->element(), filter_map, batch_size,
                                                      read_rows, eof, is_dict_filter));
    if (*read_rows == 0) {
        return Status::OK();
    }

    ColumnArray::Offsets64& offsets_data = assert_cast<ColumnArray&>(*data_column).get_offsets();
    // fill offset and null map
    RETURN_IF_ERROR(fill_array_offset(_field_schema, offsets_data, null_map_ptr,
                                      _element_reader->get_rep_level(),
                                      _element_reader->get_def_level()));
    if (UNLIKELY(element_column->size() != offsets_data.back())) {
        return Status::Corruption("Parquet array element count does not match repetition levels");
    }
#ifndef NDEBUG
    doris_column->sanity_check();
#endif
    return Status::OK();
}

Status MapColumnReader::init(std::unique_ptr<ColumnReader> key_reader,
                             std::unique_ptr<ColumnReader> value_reader, NativeFieldSchema* field) {
    _field_schema = field;
    _key_reader = std::move(key_reader);
    _value_reader = std::move(value_reader);
    return Status::OK();
}

Status MapColumnReader::read_column_data(ColumnPtr& doris_column, const DataTypePtr& type,
                                         const std::shared_ptr<NativeSchemaNode>& root_node,
                                         FilterMap& filter_map, size_t batch_size,
                                         size_t* read_rows, bool* eof, bool is_dict_filter,
                                         int64_t real_column_size) {
    MutableColumnPtr data_column;
    NullMap* null_map_ptr = nullptr;
    doris_column = IColumn::mutate(std::move(doris_column));
    if (is_column_nullable(*doris_column)) {
        auto mutable_column = doris_column->assert_mutable();
        auto* nullable_column = assert_cast<ColumnNullable*>(mutable_column.get());
        null_map_ptr = &nullable_column->get_null_map_data();
        data_column = nullable_column->get_nested_column_ptr();
    } else {
        if (_field_schema->data_type->is_nullable()) {
            return Status::Corruption("Not nullable column has null values in parquet file");
        }
        data_column = doris_column->assert_mutable();
    }
    if (remove_nullable(type)->get_primitive_type() != PrimitiveType::TYPE_MAP) {
        return Status::Corruption(
                "Wrong data type for column '{}', expected Map type, actual type id {}.",
                _field_schema->name, type->get_name());
    }

    auto& map = assert_cast<ColumnMap&>(*data_column);
    const DataTypePtr& key_type =
            assert_cast<const DataTypeMap*>(remove_nullable(type).get())->get_key_type();
    const DataTypePtr& value_type =
            assert_cast<const DataTypeMap*>(remove_nullable(type).get())->get_value_type();
    ColumnPtr& key_column = map.get_keys_ptr();
    ColumnPtr& value_column = map.get_values_ptr();

    size_t key_rows = 0;
    size_t value_rows = 0;
    bool key_eof = false;
    bool value_eof = false;
    int64_t orig_col_column_size = key_column->size();

    RETURN_IF_ERROR(_key_reader->read_column_data(key_column, key_type, root_node->key(),
                                                  filter_map, batch_size, &key_rows, &key_eof,
                                                  is_dict_filter));

    while (value_rows < key_rows && !value_eof) {
        size_t loop_rows = 0;
        RETURN_IF_ERROR(_value_reader->read_column_data(
                value_column, value_type, root_node->value(), filter_map, key_rows - value_rows,
                &loop_rows, &value_eof, is_dict_filter, key_column->size() - orig_col_column_size));
        value_rows += loop_rows;
    }
    if (UNLIKELY(key_rows != value_rows)) {
        // MAP children share one logical-row boundary; EOF in one sibling is file corruption.
        return Status::Corruption("Parquet map value reader returned {} rows for {} key rows",
                                  value_rows, key_rows);
    }
    *read_rows = key_rows;
    *eof = key_eof;

    if (*read_rows == 0) {
        return Status::OK();
    }

    const auto parent_shape = [this](const ColumnReader& reader) {
        std::vector<std::pair<level_t, bool>> shape;
        const auto& rep_levels = reader.get_rep_level();
        const auto& def_levels = reader.get_def_level();
        if (rep_levels.size() != def_levels.size()) {
            return shape;
        }
        shape.reserve(rep_levels.size());
        for (size_t slot = 0; slot < rep_levels.size(); ++slot) {
            if (rep_levels[slot] <= _field_schema->repetition_level &&
                def_levels[slot] >= _field_schema->repeated_parent_def_level) {
                // MAP siblings may have child-local collections and nullness. At this boundary,
                // only the outer entry distribution and whether that entry exists must agree.
                shape.emplace_back(rep_levels[slot],
                                   def_levels[slot] >= _field_schema->definition_level);
            }
        }
        return shape;
    };
    if (UNLIKELY(_key_reader->get_rep_level().size() != _key_reader->get_def_level().size() ||
                 _value_reader->get_rep_level().size() != _value_reader->get_def_level().size())) {
        return Status::Corruption("Parquet map sibling level counts differ");
    }
    if (UNLIKELY(parent_shape(*_key_reader) != parent_shape(*_value_reader))) {
        return Status::Corruption("Parquet map key/value outer entry shapes differ");
    }

    if (UNLIKELY(key_column->size() != value_column->size())) {
        return Status::Corruption("Parquet map key/value entry counts differ: {} vs {}",
                                  key_column->size(), value_column->size());
    }
    // Non-strict conversion can persist nullable Doris MAP keys, so the native reader must
    // preserve writer output instead of reclassifying an otherwise valid file as corrupt.
    const auto& key_rep_levels = _key_reader->get_rep_level();
    // fill offset and null map
    // The key leaf is the canonical outer MAP shape. A nested value has additional repetition
    // levels for its own collections, so comparing the two leaves would reject valid MAP values.
    RETURN_IF_ERROR(fill_array_offset(_field_schema, map.get_offsets(), null_map_ptr,
                                      key_rep_levels, _key_reader->get_def_level()));
    if (UNLIKELY(key_column->size() != map.get_offsets().back())) {
        return Status::Corruption("Parquet map entry count does not match repetition levels");
    }
#ifndef NDEBUG
    doris_column->sanity_check();
#endif
    return Status::OK();
}

Status MapColumnReader::read_column_levels(FilterMap& filter_map, size_t batch_size,
                                           size_t* read_rows, bool* eof) {
    DORIS_CHECK(dynamic_cast<SkipReadingReader*>(_key_reader.get()) == nullptr);
    return _key_reader->read_column_levels(filter_map, batch_size, read_rows, eof);
}

Status StructColumnReader::init(
        std::unordered_map<std::string, std::unique_ptr<ColumnReader>>&& child_readers,
        NativeFieldSchema* field) {
    _field_schema = field;
    _child_readers = std::move(child_readers);
    return Status::OK();
}

Status StructColumnReader::read_column_levels(FilterMap& filter_map, size_t batch_size,
                                              size_t* read_rows, bool* eof) {
    _read_column_names.clear();
    for (const auto& child : _field_schema->children) {
        auto reader = _child_readers.find(child.name);
        DORIS_CHECK(reader != _child_readers.end());
        if (dynamic_cast<SkipReadingReader*>(reader->second.get()) != nullptr) {
            continue;
        }
        _read_column_names.emplace_back(child.name);
        return reader->second->read_column_levels(filter_map, batch_size, read_rows, eof);
    }
    return Status::InternalError("Struct {} has no physical reader for levels",
                                 _field_schema->name);
}
Status StructColumnReader::read_column_data(ColumnPtr& doris_column, const DataTypePtr& type,
                                            const std::shared_ptr<NativeSchemaNode>& root_node,
                                            FilterMap& filter_map, size_t batch_size,
                                            size_t* read_rows, bool* eof, bool is_dict_filter,
                                            int64_t real_column_size) {
    MutableColumnPtr data_column;
    NullMap* null_map_ptr = nullptr;
    doris_column = IColumn::mutate(std::move(doris_column));
    if (is_column_nullable(*doris_column)) {
        auto mutable_column = doris_column->assert_mutable();
        auto* nullable_column = assert_cast<ColumnNullable*>(mutable_column.get());
        null_map_ptr = &nullable_column->get_null_map_data();
        data_column = nullable_column->get_nested_column_ptr();
    } else {
        if (_field_schema->data_type->is_nullable()) {
            return Status::Corruption("Not nullable column has null values in parquet file");
        }
        data_column = doris_column->assert_mutable();
    }
    if (type->get_primitive_type() != PrimitiveType::TYPE_STRUCT) {
        return Status::Corruption(
                "Wrong data type for column '{}', expected Struct type, actual type id {}.",
                _field_schema->name, type->get_name());
    }

    auto& doris_struct = assert_cast<ColumnStruct&>(*data_column);
    const auto* doris_struct_type = assert_cast<const DataTypeStruct*>(remove_nullable(type).get());

    int64_t not_missing_column_id = -1;
    size_t not_missing_orig_column_size = 0;
    std::vector<size_t> missing_column_idxs {};
    std::vector<size_t> skip_reading_column_idxs {};
    std::vector<std::pair<level_t, bool>> reference_parent_shape;

    auto parent_shape = [this](const ColumnReader& reader) {
        std::vector<std::pair<level_t, bool>> shape;
        const auto& rep_levels = reader.get_rep_level();
        const auto& def_levels = reader.get_def_level();
        if (rep_levels.size() != def_levels.size()) {
            return shape;
        }
        shape.reserve(rep_levels.size());
        for (size_t slot = 0; slot < rep_levels.size(); ++slot) {
            // Deeper repetitions belong to a nested child; only starts visible at this STRUCT's
            // boundary and the optional parent's presence determine how siblings are paired.
            if (rep_levels[slot] <= _field_schema->repetition_level &&
                def_levels[slot] >= _field_schema->repeated_parent_def_level) {
                shape.emplace_back(rep_levels[slot],
                                   def_levels[slot] >= _field_schema->definition_level);
            }
        }
        return shape;
    };

    _read_column_names.clear();

    for (size_t i = 0; i < doris_struct.tuple_size(); ++i) {
        ColumnPtr& doris_field = doris_struct.get_column_ptr(i);
        auto& doris_type = doris_struct_type->get_element(i);
        auto& doris_name = doris_struct_type->get_element_name(i);
        if (!root_node->has_child(doris_name)) {
            missing_column_idxs.push_back(i);
            VLOG_DEBUG << "[ParquetReader] Missing column in schema: column_idx[" << i
                       << "], doris_name: " << doris_name << " (column not exists in root node)";
            continue;
        }
        auto file_name = root_node->file_child_name(doris_name);

        // Check if this is a SkipReadingReader - we should skip it when choosing reference column
        // because SkipReadingReader doesn't know the actual data size in nested context
        bool is_skip_reader =
                dynamic_cast<SkipReadingReader*>(_child_readers[file_name].get()) != nullptr;

        if (is_skip_reader) {
            // Store SkipReadingReader columns to fill them later based on reference column size
            skip_reading_column_idxs.push_back(i);
            continue;
        }

        // Only add non-SkipReadingReader columns to _read_column_names
        // This ensures get_rep_level() and get_def_level() return valid levels
        _read_column_names.emplace_back(file_name);

        size_t field_rows = 0;
        bool field_eof = false;
        if (not_missing_column_id == -1) {
            not_missing_column_id = i;
            not_missing_orig_column_size = doris_field->size();
            RETURN_IF_ERROR(_child_readers[file_name]->read_column_data(
                    doris_field, doris_type, root_node->child(doris_name), filter_map, batch_size,
                    &field_rows, &field_eof, is_dict_filter));
            *read_rows = field_rows;
            *eof = field_eof;
            if (UNLIKELY(_child_readers[file_name]->get_rep_level().size() !=
                         _child_readers[file_name]->get_def_level().size())) {
                return Status::Corruption(
                        "Parquet struct child '{}' has mismatched repetition/definition levels",
                        file_name);
            }
            reference_parent_shape = parent_shape(*_child_readers[file_name]);
            /*
             * Considering the issue in the `_read_nested_column` function where data may span across pages, leading
             * to missing definition and repetition levels, when filling the null_map of the struct later, it is
             * crucial to use the definition and repetition levels from the first read column
             * (since `_read_nested_column` is not called repeatedly).
             *
             *  It is worth mentioning that, theoretically, any sub-column can be chosen to fill the null_map,
             *  and selecting the shortest one will offer better performance
             */
        } else {
            while (field_rows < *read_rows && !field_eof) {
                size_t loop_rows = 0;
                RETURN_IF_ERROR(_child_readers[file_name]->read_column_data(
                        doris_field, doris_type, root_node->child(doris_name), filter_map,
                        *read_rows - field_rows, &loop_rows, &field_eof, is_dict_filter));
                field_rows += loop_rows;
            }
            if (UNLIKELY(*read_rows != field_rows)) {
                // STRUCT siblings must advance the same logical rows before any result is exposed.
                return Status::Corruption("Parquet struct child '{}' returned {} rows, expected {}",
                                          file_name, field_rows, *read_rows);
            }
            if (UNLIKELY(_child_readers[file_name]->get_rep_level().size() !=
                         _child_readers[file_name]->get_def_level().size())) {
                return Status::Corruption(
                        "Parquet struct child '{}' has mismatched repetition/definition levels",
                        file_name);
            }
            if (UNLIKELY(parent_shape(*_child_readers[file_name]) != reference_parent_shape)) {
                return Status::Corruption(
                        "Parquet struct child '{}' has a different repeated-parent shape",
                        file_name);
            }
            //            DCHECK_EQ(*eof, field_eof);
        }
    }

    int64_t missing_column_sz = -1;

    if (not_missing_column_id == -1) {
        // All queried columns are missing in the file (e.g., all added after schema change)
        // We need to pick a column from _field_schema children that exists in the file for RL/DL reference
        std::string reference_file_column_name;
        std::unique_ptr<ColumnReader>* reference_reader = nullptr;

        for (const auto& child : _field_schema->children) {
            auto it = _child_readers.find(child.name);
            if (it != _child_readers.end()) {
                // Skip SkipReadingReader as they don't have valid RL/DL
                bool is_skip_reader = dynamic_cast<SkipReadingReader*>(it->second.get()) != nullptr;
                if (!is_skip_reader) {
                    reference_file_column_name = child.name;
                    reference_reader = &(it->second);
                    break;
                }
            }
        }

        if (reference_reader != nullptr) {
            size_t field_rows = 0;
            bool field_eof = false;
            RETURN_IF_ERROR(
                    (*reference_reader)
                            ->read_column_levels(filter_map, batch_size, &field_rows, &field_eof));

            *read_rows = field_rows;
            *eof = field_eof;
            _read_column_names.emplace_back(reference_file_column_name);
            missing_column_sz = 0;
            const auto& rep_levels = (*reference_reader)->get_rep_level();
            const auto& def_levels = (*reference_reader)->get_def_level();
            DORIS_CHECK_EQ(rep_levels.size(), def_levels.size());
            for (size_t level_index = 0; level_index < def_levels.size(); ++level_index) {
                if (def_levels[level_index] >= _field_schema->repeated_parent_def_level &&
                    rep_levels[level_index] <= _field_schema->repetition_level) {
                    ++missing_column_sz;
                }
            }
        } else {
            return Status::Corruption(
                    "Cannot read struct '{}': all queried columns are missing and no reference "
                    "column found in file",
                    _field_schema->name);
        }
    }

    //  This missing_column_sz is not *read_rows. Because read_rows returns the number of rows.
    //  For example: suppose we have a column array<struct<a:int,b:string>>,
    //  where b is a newly added column, that is, a missing column.
    //  There are two rows of data in this column,
    //      [{1,null},{2,null},{3,null}]
    //      [{4,null},{5,null}]
    //  When you first read subcolumn a, you read 5 data items and the value of *read_rows is 2.
    //  You should insert 5 records into subcolumn b instead of 2.
    if (missing_column_sz == -1) {
        missing_column_sz = doris_struct.get_column(not_missing_column_id).size() -
                            not_missing_orig_column_size;
    }

    // Fill SkipReadingReader columns with the correct amount of data based on reference column
    // Let SkipReadingReader handle the data filling through its read_column_data method
    for (auto idx : skip_reading_column_idxs) {
        auto& doris_field = doris_struct.get_column_ptr(idx);
        auto& doris_type = const_cast<DataTypePtr&>(doris_struct_type->get_element(idx));
        auto& doris_name = const_cast<String&>(doris_struct_type->get_element_name(idx));
        auto file_name = root_node->file_child_name(doris_name);

        size_t field_rows = 0;
        bool field_eof = false;
        RETURN_IF_ERROR(_child_readers[file_name]->read_column_data(
                doris_field, doris_type, root_node->child(doris_name), filter_map,
                missing_column_sz, &field_rows, &field_eof, is_dict_filter, missing_column_sz));
    }

    // Fill truly missing columns (not in root_node) with null or default value
    for (auto idx : missing_column_idxs) {
        auto& doris_field = doris_struct.get_column_ptr(idx);
        auto& doris_type = doris_struct_type->get_element(idx);
        DCHECK(doris_type->is_nullable());
        doris_field = IColumn::mutate(std::move(doris_field));
        auto mutable_column = doris_field->assert_mutable();
        auto* nullable_column = static_cast<ColumnNullable*>(mutable_column.get());
        nullable_column->insert_many_defaults(missing_column_sz);
    }

    if (null_map_ptr != nullptr) {
        fill_struct_null_map(_field_schema, *null_map_ptr, this->get_rep_level(),
                             this->get_def_level());
    }
#ifndef NDEBUG
    doris_column->sanity_check();
#endif
    return Status::OK();
}

template class ScalarColumnReader<true, true>;
template class ScalarColumnReader<true, false>;
template class ScalarColumnReader<false, true>;
template class ScalarColumnReader<false, false>;

} // namespace doris::format::parquet::native
