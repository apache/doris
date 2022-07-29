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

#include "vec/utils/arrow_column_to_doris_column.h"

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>

#include "arrow/array/array_binary.h"
#include "arrow/array/array_nested.h"
#include "arrow/scalar.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "gutil/casts.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/runtime/vdatetime_value.h"

#define FOR_ARROW_TYPES(M)                            \
    M(::arrow::Type::BOOL, TYPE_BOOLEAN)              \
    M(::arrow::Type::INT8, TYPE_TINYINT)              \
    M(::arrow::Type::UINT8, TYPE_TINYINT)             \
    M(::arrow::Type::INT16, TYPE_SMALLINT)            \
    M(::arrow::Type::UINT16, TYPE_SMALLINT)           \
    M(::arrow::Type::INT32, TYPE_INT)                 \
    M(::arrow::Type::UINT32, TYPE_INT)                \
    M(::arrow::Type::INT64, TYPE_BIGINT)              \
    M(::arrow::Type::UINT64, TYPE_BIGINT)             \
    M(::arrow::Type::HALF_FLOAT, TYPE_FLOAT)          \
    M(::arrow::Type::FLOAT, TYPE_FLOAT)               \
    M(::arrow::Type::DOUBLE, TYPE_DOUBLE)             \
    M(::arrow::Type::BINARY, TYPE_VARCHAR)            \
    M(::arrow::Type::FIXED_SIZE_BINARY, TYPE_VARCHAR) \
    M(::arrow::Type::STRING, TYPE_VARCHAR)            \
    M(::arrow::Type::TIMESTAMP, TYPE_DATETIME)        \
    M(::arrow::Type::DATE32, TYPE_DATE)               \
    M(::arrow::Type::DATE64, TYPE_DATETIME)           \
    M(::arrow::Type::DECIMAL, TYPE_DECIMALV2)

#define FOR_ARROW_NUMERIC_TYPES(M)      \
    M(arrow::Type::UINT8, UInt8)        \
    M(arrow::Type::INT8, Int8)          \
    M(arrow::Type::INT16, Int16)        \
    M(arrow::Type::UINT16, UInt16)      \
    M(arrow::Type::INT32, Int32)        \
    M(arrow::Type::UINT32, UInt32)      \
    M(arrow::Type::UINT64, UInt64)      \
    M(arrow::Type::INT64, Int64)        \
    M(arrow::Type::HALF_FLOAT, Float32) \
    M(arrow::Type::FLOAT, Float32)      \
    M(arrow::Type::DOUBLE, Float64)

namespace doris::vectorized {

const PrimitiveType arrow_type_to_primitive_type(::arrow::Type::type type) {
    switch (type) {
        // TODO: convert arrow date type to datev2/datetimev2
#define DISPATCH(ARROW_TYPE, CPP_TYPE) \
    case ARROW_TYPE:                   \
        return CPP_TYPE;
        FOR_ARROW_TYPES(DISPATCH)
#undef DISPATCH
    default:
        break;
    }
    return INVALID_TYPE;
}

static size_t fill_nullable_column(const arrow::Array* array, size_t array_idx,
                                   vectorized::ColumnNullable* nullable_column,
                                   size_t num_elements) {
    size_t null_elements_count = 0;
    NullMap& map_data = nullable_column->get_null_map_data();
    for (size_t i = 0; i < num_elements; ++i) {
        auto is_null = array->IsNull(array_idx + i);
        map_data.emplace_back(is_null);
        null_elements_count += is_null;
    }
    return null_elements_count;
}

/// Inserts chars and offsets right into internal column data to reduce an overhead.
/// Internal offsets are shifted by one to the right in comparison with Arrow ones. So the last offset should map to the end of all chars.
/// Also internal strings are null terminated.
static Status convert_column_with_string_data(const arrow::Array* array, size_t array_idx,
                                              MutableColumnPtr& data_column, size_t num_elements) {
    auto& column_chars_t = assert_cast<ColumnString&>(*data_column).get_chars();
    auto& column_offsets = assert_cast<ColumnString&>(*data_column).get_offsets();

    auto concrete_array = down_cast<const arrow::BinaryArray*>(array);
    std::shared_ptr<arrow::Buffer> buffer = concrete_array->value_data();

    for (size_t offset_i = array_idx; offset_i < array_idx + num_elements; ++offset_i) {
        if (!concrete_array->IsNull(offset_i) && buffer) {
            const auto* raw_data = buffer->data() + concrete_array->value_offset(offset_i);
            column_chars_t.insert(raw_data, raw_data + concrete_array->value_length(offset_i));
        }
        column_chars_t.emplace_back('\0');

        column_offsets.emplace_back(column_chars_t.size());
    }
    return Status::OK();
}

static Status convert_column_with_fixed_size_data(const arrow::Array* array, size_t array_idx,
                                                  MutableColumnPtr& data_column,
                                                  size_t num_elements) {
    auto& column_chars_t = assert_cast<ColumnString&>(*data_column).get_chars();
    auto& column_offsets = assert_cast<ColumnString&>(*data_column).get_offsets();

    auto concrete_array = down_cast<const arrow::FixedSizeBinaryArray*>(array);
    uint32_t width = concrete_array->byte_width();
    const auto* array_data = concrete_array->GetValue(array_idx);

    for (size_t offset_i = 0; offset_i < num_elements; ++offset_i) {
        if (!concrete_array->IsNull(offset_i)) {
            const auto* raw_data = array_data + (offset_i * width);
            column_chars_t.insert(raw_data, raw_data + width);
        }
        column_chars_t.emplace_back('\0');
        column_offsets.emplace_back(column_chars_t.size());
    }
    return Status::OK();
}

/// Inserts numeric data right into internal column data to reduce an overhead
template <typename NumericType, typename VectorType = ColumnVector<NumericType>>
static Status convert_column_with_numeric_data(const arrow::Array* array, size_t array_idx,
                                               MutableColumnPtr& data_column, size_t num_elements) {
    auto& column_data = static_cast<VectorType&>(*data_column).get_data();
    /// buffers[0] is a null bitmap and buffers[1] are actual values
    std::shared_ptr<arrow::Buffer> buffer = array->data()->buffers[1];
    const auto* raw_data = reinterpret_cast<const NumericType*>(buffer->data()) + array_idx;
    column_data.insert(raw_data, raw_data + num_elements);
    return Status::OK();
}

static Status convert_column_with_boolean_data(const arrow::Array* array, size_t array_idx,
                                               MutableColumnPtr& data_column, size_t num_elements) {
    auto& column_data = static_cast<ColumnVector<UInt8>&>(*data_column).get_data();
    auto concrete_array = down_cast<const arrow::BooleanArray*>(array);
    for (size_t bool_i = array_idx; bool_i < array_idx + num_elements; ++bool_i) {
        column_data.emplace_back(concrete_array->Value(bool_i));
    }
    return Status::OK();
}

static int64_t time_unit_divisor(arrow::TimeUnit::type unit) {
    // Doris only supports seconds
    switch (unit) {
    case arrow::TimeUnit::type::SECOND: {
        return 1L;
    }
    case arrow::TimeUnit::type::MILLI: {
        return 1000L;
    }
    case arrow::TimeUnit::type::MICRO: {
        return 1000000L;
    }
    case arrow::TimeUnit::type::NANO: {
        return 1000000000L;
    }
    default:
        return 0L;
    }
}

template <typename ArrowType>
static Status convert_column_with_timestamp_data(const arrow::Array* array, size_t array_idx,
                                                 MutableColumnPtr& data_column, size_t num_elements,
                                                 const cctz::time_zone& ctz) {
    auto& column_data = static_cast<ColumnVector<Int64>&>(*data_column).get_data();
    auto concrete_array = down_cast<const ArrowType*>(array);
    int64_t divisor = 1;
    int64_t multiplier = 1;
    if constexpr (std::is_same_v<ArrowType, arrow::TimestampArray>) {
        const auto type = std::static_pointer_cast<arrow::TimestampType>(array->type());
        divisor = time_unit_divisor(type->unit());
        if (divisor == 0L) {
            return Status::InternalError(fmt::format("Invalid Time Type:{}", type->name()));
        }
    } else if constexpr (std::is_same_v<ArrowType, arrow::Date32Array>) {
        multiplier = 24 * 60 * 60; // day => secs
    } else if constexpr (std::is_same_v<ArrowType, arrow::Date64Array>) {
        divisor = 1000; //ms => secs
    }

    for (size_t value_i = array_idx; value_i < array_idx + num_elements; ++value_i) {
        VecDateTimeValue v;
        v.from_unixtime(static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier,
                        ctz);
        if constexpr (std::is_same_v<ArrowType, arrow::Date32Array>) {
            v.cast_to_date();
        }
        column_data.emplace_back(binary_cast<VecDateTimeValue, Int64>(v));
    }
    return Status::OK();
}

template <typename ArrowType>
static Status convert_column_with_date_v2_data(const arrow::Array* array, size_t array_idx,
                                               MutableColumnPtr& data_column, size_t num_elements,
                                               const cctz::time_zone& ctz) {
    auto& column_data = static_cast<ColumnVector<UInt32>&>(*data_column).get_data();
    auto concrete_array = down_cast<const ArrowType*>(array);
    int64_t divisor = 1;
    int64_t multiplier = 1;
    if constexpr (std::is_same_v<ArrowType, arrow::TimestampArray>) {
        const auto type = std::static_pointer_cast<arrow::TimestampType>(array->type());
        divisor = time_unit_divisor(type->unit());
        if (divisor == 0L) {
            return Status::InternalError(fmt::format("Invalid Time Type:{}", type->name()));
        }
    } else if constexpr (std::is_same_v<ArrowType, arrow::Date32Array>) {
        multiplier = 24 * 60 * 60; // day => secs
    } else if constexpr (std::is_same_v<ArrowType, arrow::Date64Array>) {
        divisor = 1000; //ms => secs
    }

    for (size_t value_i = array_idx; value_i < array_idx + num_elements; ++value_i) {
        DateV2Value<DateV2ValueType> v;
        v.from_unixtime(static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier,
                        ctz);
        column_data.emplace_back(binary_cast<DateV2Value<DateV2ValueType>, UInt32>(v));
    }
    return Status::OK();
}

template <typename ArrowType>
static Status convert_column_with_datetime_v2_data(const arrow::Array* array, size_t array_idx,
                                                   MutableColumnPtr& data_column,
                                                   size_t num_elements,
                                                   const cctz::time_zone& ctz) {
    auto& column_data = static_cast<ColumnVector<UInt64>&>(*data_column).get_data();
    auto concrete_array = down_cast<const ArrowType*>(array);
    int64_t divisor = 1;
    int64_t multiplier = 1;
    if constexpr (std::is_same_v<ArrowType, arrow::TimestampArray>) {
        const auto type = std::static_pointer_cast<arrow::TimestampType>(array->type());
        divisor = time_unit_divisor(type->unit());
        if (divisor == 0L) {
            return Status::InternalError(fmt::format("Invalid Time Type:{}", type->name()));
        }
    } else if constexpr (std::is_same_v<ArrowType, arrow::Date32Array>) {
        multiplier = 24 * 60 * 60; // day => secs
    } else if constexpr (std::is_same_v<ArrowType, arrow::Date64Array>) {
        divisor = 1000; //ms => secs
    }

    for (size_t value_i = array_idx; value_i < array_idx + num_elements; ++value_i) {
        DateV2Value<DateTimeV2ValueType> v;
        v.from_unixtime(static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier,
                        ctz);
        column_data.emplace_back(binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(v));
    }
    return Status::OK();
}

static Status convert_column_with_decimal_data(const arrow::Array* array, size_t array_idx,
                                               MutableColumnPtr& data_column, size_t num_elements) {
    auto& column_data =
            static_cast<ColumnDecimal<vectorized::Decimal128>&>(*data_column).get_data();
    auto concrete_array = down_cast<const arrow::DecimalArray*>(array);
    const auto* arrow_decimal_type = static_cast<arrow::DecimalType*>(array->type().get());
    // TODO check precision
    //size_t precision = arrow_decimal_type->precision();
    const auto scale = arrow_decimal_type->scale();

    for (size_t value_i = array_idx; value_i < array_idx + num_elements; ++value_i) {
        auto value =
                *reinterpret_cast<const vectorized::Decimal128*>(concrete_array->Value(value_i));
        // convert scale to 9
        if (scale != 9) {
            value = convert_decimals<vectorized::DataTypeDecimal<vectorized::Decimal128>,
                                     vectorized::DataTypeDecimal<vectorized::Decimal128>>(value,
                                                                                          scale, 9);
        }
        column_data.emplace_back(value);
    }
    return Status::OK();
}

static Status convert_offset_from_list_column(const arrow::Array* array, size_t array_idx,
                                              MutableColumnPtr& data_column, size_t num_elements,
                                              size_t* start_idx_for_data, size_t* num_for_data) {
    auto& offsets_data = static_cast<ColumnArray&>(*data_column).get_offsets();
    auto concrete_array = down_cast<const arrow::ListArray*>(array);
    auto arrow_offsets_array = concrete_array->offsets();
    auto arrow_offsets = down_cast<arrow::Int32Array*>(arrow_offsets_array.get());
    auto prev_size = offsets_data.back();
    for (int64_t i = array_idx + 1; i < array_idx + num_elements + 1; ++i) {
        // convert to doris offset, start from offsets.back()
        offsets_data.emplace_back(prev_size + arrow_offsets->Value(i) -
                                  arrow_offsets->Value(array_idx));
    }
    *start_idx_for_data = arrow_offsets->Value(array_idx);
    *num_for_data = offsets_data.back() - prev_size;

    return Status::OK();
}

static Status convert_column_with_list_data(const arrow::Array* array, size_t array_idx,
                                            MutableColumnPtr& data_column, size_t num_elements,
                                            const cctz::time_zone& ctz,
                                            const DataTypePtr& nested_type) {
    size_t start_idx_of_data = 0;
    size_t num_of_data = 0;
    // get start idx and num of values from arrow offsets
    RETURN_IF_ERROR(convert_offset_from_list_column(array, array_idx, data_column, num_elements,
                                                    &start_idx_of_data, &num_of_data));
    auto& data_column_ptr = static_cast<ColumnArray&>(*data_column).get_data_ptr();
    auto concrete_array = down_cast<const arrow::ListArray*>(array);
    std::shared_ptr<arrow::Array> arrow_data = concrete_array->values();

    return arrow_column_to_doris_column(arrow_data.get(), start_idx_of_data, data_column_ptr,
                                        nested_type, num_of_data, ctz);
}

// For convenient unit test. Not use this in formal code.
Status arrow_column_to_doris_column(const arrow::Array* arrow_column, size_t arrow_batch_cur_idx,
                                    ColumnPtr& doris_column, const DataTypePtr& type,
                                    size_t num_elements, const std::string& timezone) {
    cctz::time_zone ctz;
    TimezoneUtils::find_cctz_time_zone(timezone, ctz);
    return arrow_column_to_doris_column(arrow_column, arrow_batch_cur_idx, doris_column, type,
                                        num_elements, ctz);
}

Status arrow_column_to_doris_column(const arrow::Array* arrow_column, size_t arrow_batch_cur_idx,
                                    ColumnPtr& doris_column, const DataTypePtr& type,
                                    size_t num_elements, const cctz::time_zone& ctz) {
    // src column always be nullable for simpify converting
    CHECK(doris_column->is_nullable());
    MutableColumnPtr data_column = nullptr;
    auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
            (*std::move(doris_column)).mutate().get());
    fill_nullable_column(arrow_column, arrow_batch_cur_idx, nullable_column, num_elements);
    data_column = nullable_column->get_nested_column_ptr();
    WhichDataType which_type(type);
    // process data
    switch (arrow_column->type()->id()) {
    case arrow::Type::STRING:
    case arrow::Type::BINARY:
        return convert_column_with_string_data(arrow_column, arrow_batch_cur_idx, data_column,
                                               num_elements);
    case arrow::Type::FIXED_SIZE_BINARY:
        return convert_column_with_fixed_size_data(arrow_column, arrow_batch_cur_idx, data_column,
                                                   num_elements);
#define DISPATCH(ARROW_NUMERIC_TYPE, CPP_NUMERIC_TYPE)             \
    case ARROW_NUMERIC_TYPE:                                       \
        return convert_column_with_numeric_data<CPP_NUMERIC_TYPE>( \
                arrow_column, arrow_batch_cur_idx, data_column, num_elements);
        FOR_ARROW_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    case arrow::Type::BOOL:
        return convert_column_with_boolean_data(arrow_column, arrow_batch_cur_idx, data_column,
                                                num_elements);
    case arrow::Type::DATE32:
        if (which_type.is_date_v2()) {
            return convert_column_with_date_v2_data<arrow::Date32Array>(
                    arrow_column, arrow_batch_cur_idx, data_column, num_elements, ctz);
        } else {
            return convert_column_with_timestamp_data<arrow::Date32Array>(
                    arrow_column, arrow_batch_cur_idx, data_column, num_elements, ctz);
        }
    case arrow::Type::DATE64:
        if (which_type.is_date_v2_or_datetime_v2()) {
            return convert_column_with_datetime_v2_data<arrow::Date64Array>(
                    arrow_column, arrow_batch_cur_idx, data_column, num_elements, ctz);
        } else {
            return convert_column_with_timestamp_data<arrow::Date64Array>(
                    arrow_column, arrow_batch_cur_idx, data_column, num_elements, ctz);
        }
    case arrow::Type::TIMESTAMP:
        if (which_type.is_date_v2_or_datetime_v2()) {
            return convert_column_with_datetime_v2_data<arrow::TimestampArray>(
                    arrow_column, arrow_batch_cur_idx, data_column, num_elements, ctz);
        } else {
            return convert_column_with_timestamp_data<arrow::TimestampArray>(
                    arrow_column, arrow_batch_cur_idx, data_column, num_elements, ctz);
        }
    case arrow::Type::DECIMAL:
        return convert_column_with_decimal_data(arrow_column, arrow_batch_cur_idx, data_column,
                                                num_elements);
    case arrow::Type::LIST:
        CHECK(type->have_subtypes());
        return convert_column_with_list_data(
                arrow_column, arrow_batch_cur_idx, data_column, num_elements, ctz,
                (reinterpret_cast<const DataTypeArray*>(type.get()))->get_nested_type());
    default:
        break;
    }
    return Status::NotSupported(
            fmt::format("Not support arrow type:{}", arrow_column->type()->name()));
}
} // namespace doris::vectorized
