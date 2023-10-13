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

#include "vec/exec/format/parquet/fix_length_plain_decoder.h"

#include <gen_cpp/parquet_types.h>
#include <stdint.h>
#include <string.h>

#include <memory>
#include <vector>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "util/bit_util.h"
#include "util/slice.h"
#include "vec/columns/column.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exec/format/format_common.h"
#include "vec/exec/format/parquet/parquet_common.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
namespace vectorized {
template <typename T>
class ColumnDecimal;
template <typename T>
class ColumnVector;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

Status FixLengthPlainDecoder::skip_values(size_t num_values) {
    _offset += _type_length * num_values;
    if (UNLIKELY(_offset > _data->size)) {
        return Status::IOError("Out-of-bounds access in parquet data decoder");
    }
    return Status::OK();
}

Status FixLengthPlainDecoder::decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                            ColumnSelectVector& select_vector,
                                            bool is_dict_filter) {
    if (select_vector.has_filter()) {
        return _decode_values<true>(doris_column, data_type, select_vector, is_dict_filter);
    } else {
        return _decode_values<false>(doris_column, data_type, select_vector, is_dict_filter);
    }
}

template <bool has_filter>
Status FixLengthPlainDecoder::_decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                             ColumnSelectVector& select_vector,
                                             bool is_dict_filter) {
    size_t non_null_size = select_vector.num_values() - select_vector.num_nulls();
    if (UNLIKELY(_offset + _type_length * non_null_size > _data->size)) {
        return Status::IOError("Out-of-bounds access in parquet data decoder");
    }
    TypeIndex logical_type = remove_nullable(data_type)->get_type_id();
    switch (logical_type) {
#define DISPATCH(NUMERIC_TYPE, CPP_NUMERIC_TYPE, PHYSICAL_TYPE)                           \
    case NUMERIC_TYPE:                                                                    \
        if (_physical_type == tparquet::Type::INT32) {                                    \
            return _decode_numeric<CPP_NUMERIC_TYPE, Int32, has_filter>(doris_column,     \
                                                                        select_vector);   \
        } else if (_physical_type == tparquet::Type::INT64) {                             \
            return _decode_numeric<CPP_NUMERIC_TYPE, Int64, has_filter>(doris_column,     \
                                                                        select_vector);   \
        } else if (_physical_type == tparquet::Type::FLOAT) {                             \
            return _decode_numeric<CPP_NUMERIC_TYPE, Float32, has_filter>(doris_column,   \
                                                                          select_vector); \
        } else if (_physical_type == tparquet::Type::DOUBLE) {                            \
            return _decode_numeric<CPP_NUMERIC_TYPE, Float64, has_filter>(doris_column,   \
                                                                          select_vector); \
        } else {                                                                          \
            break;                                                                        \
        }
        FOR_LOGICAL_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    case TypeIndex::Date:
        if (_physical_type == tparquet::Type::INT32) {
            return _decode_date<VecDateTimeValue, Int64, has_filter>(doris_column, select_vector);
        }
        break;
    case TypeIndex::DateV2:
        if (_physical_type == tparquet::Type::INT32) {
            return _decode_date<DateV2Value<DateV2ValueType>, UInt32, has_filter>(doris_column,
                                                                                  select_vector);
        }
        break;
    case TypeIndex::DateTime:
        if (_physical_type == tparquet::Type::INT96) {
            return _decode_datetime96<VecDateTimeValue, Int64, has_filter>(doris_column,
                                                                           select_vector);
        } else if (_physical_type == tparquet::Type::INT64) {
            return _decode_datetime64<VecDateTimeValue, Int64, has_filter>(doris_column,
                                                                           select_vector);
        }
        break;
    case TypeIndex::DateTimeV2:
        // Spark can set the timestamp precision by the following configuration:
        // spark.sql.parquet.outputTimestampType = INT96(NANOS), TIMESTAMP_MICROS, TIMESTAMP_MILLIS
        if (_physical_type == tparquet::Type::INT96) {
            return _decode_datetime96<DateV2Value<DateTimeV2ValueType>, UInt64, has_filter>(
                    doris_column, select_vector);
        } else if (_physical_type == tparquet::Type::INT64) {
            return _decode_datetime64<DateV2Value<DateTimeV2ValueType>, UInt64, has_filter>(
                    doris_column, select_vector);
        }
        break;
    case TypeIndex::Decimal32:
        if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            return _decode_binary_decimal<Int32, has_filter>(doris_column, data_type,
                                                             select_vector);
        } else if (_physical_type == tparquet::Type::INT32) {
            return _decode_primitive_decimal<Int32, Int32, has_filter>(doris_column, data_type,
                                                                       select_vector);
        } else if (_physical_type == tparquet::Type::INT64) {
            return _decode_primitive_decimal<Int32, Int64, has_filter>(doris_column, data_type,
                                                                       select_vector);
        }
        break;
    case TypeIndex::Decimal64:
        if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            return _decode_binary_decimal<Int64, has_filter>(doris_column, data_type,
                                                             select_vector);
        } else if (_physical_type == tparquet::Type::INT32) {
            return _decode_primitive_decimal<Int64, Int32, has_filter>(doris_column, data_type,
                                                                       select_vector);
        } else if (_physical_type == tparquet::Type::INT64) {
            return _decode_primitive_decimal<Int64, Int64, has_filter>(doris_column, data_type,
                                                                       select_vector);
        }
        break;
    case TypeIndex::Decimal128:
        if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            return _decode_binary_decimal<Int128, has_filter>(doris_column, data_type,
                                                              select_vector);
        } else if (_physical_type == tparquet::Type::INT32) {
            return _decode_primitive_decimal<Int128, Int32, has_filter>(doris_column, data_type,
                                                                        select_vector);
        } else if (_physical_type == tparquet::Type::INT64) {
            return _decode_primitive_decimal<Int128, Int64, has_filter>(doris_column, data_type,
                                                                        select_vector);
        }
        break;
    case TypeIndex::Decimal128I:
        if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            return _decode_binary_decimal<Int128, has_filter>(doris_column, data_type,
                                                              select_vector);
        } else if (_physical_type == tparquet::Type::INT32) {
            return _decode_primitive_decimal<Int128, Int32, has_filter>(doris_column, data_type,
                                                                        select_vector);
        } else if (_physical_type == tparquet::Type::INT64) {
            return _decode_primitive_decimal<Int128, Int64, has_filter>(doris_column, data_type,
                                                                        select_vector);
        }
        break;
    case TypeIndex::String:
        [[fallthrough]];
    case TypeIndex::FixedString:
        if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            return _decode_string<has_filter>(doris_column, select_vector);
        }
        break;
    default:
        break;
    }

    return Status::InvalidArgument("Can't decode parquet physical type {} to doris logical type {}",
                                   tparquet::to_string(_physical_type), getTypeName(logical_type));
}

template <bool has_filter>
Status FixLengthPlainDecoder::_decode_string(MutableColumnPtr& doris_column,
                                             ColumnSelectVector& select_vector) {
    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            std::vector<StringRef> string_values;
            string_values.reserve(run_length);
            for (size_t i = 0; i < run_length; ++i) {
                char* buf_start = _data->data + _offset;
                string_values.emplace_back(buf_start, _type_length);
                _offset += _type_length;
            }
            doris_column->insert_many_strings(&string_values[0], run_length);
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            doris_column->insert_many_defaults(run_length);
            break;
        }
        case ColumnSelectVector::FILTERED_CONTENT: {
            _offset += _type_length * run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_NULL: {
            // do nothing
            break;
        }
        }
    }
    return Status::OK();
}
template <typename Numeric, typename PhysicalType, bool has_filter>
Status FixLengthPlainDecoder::_decode_numeric(MutableColumnPtr& doris_column,
                                              ColumnSelectVector& select_vector) {
    auto& column_data = static_cast<ColumnVector<Numeric>&>(*doris_column).get_data();
    size_t data_index = column_data.size();
    column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (size_t i = 0; i < run_length; ++i) {
                char* buf_start = _data->data + _offset;
                column_data[data_index++] = *(PhysicalType*)buf_start;
                _offset += _type_length;
            }
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            data_index += run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_CONTENT: {
            _offset += _type_length * run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_NULL: {
            // do nothing
            break;
        }
        }
    }
    return Status::OK();
}

template <typename CppType, typename ColumnType, bool has_filter>
Status FixLengthPlainDecoder::_decode_date(MutableColumnPtr& doris_column,
                                           ColumnSelectVector& select_vector) {
    auto& column_data = static_cast<ColumnVector<ColumnType>&>(*doris_column).get_data();
    size_t data_index = column_data.size();
    column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
    ColumnSelectVector::DataReadType read_type;
    date_day_offset_dict& date_dict = date_day_offset_dict::get();

    while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (size_t i = 0; i < run_length; ++i) {
                char* buf_start = _data->data + _offset;
                int64_t date_value = static_cast<int64_t>(*reinterpret_cast<int32_t*>(buf_start)) +
                                     _decode_params->offset_days;
                if constexpr (std::is_same_v<CppType, VecDateTimeValue>) {
                    auto& v = reinterpret_cast<CppType&>(column_data[data_index++]);
                    v.create_from_date_v2(date_dict[date_value], TIME_DATE);
                    // we should cast to date if using date v1.
                    v.cast_to_date();
                } else {
                    reinterpret_cast<CppType&>(column_data[data_index++]) = date_dict[date_value];
                }
                _offset += _type_length;
            }
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            data_index += run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_CONTENT: {
            _offset += _type_length * run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_NULL: {
            // do nothing
            break;
        }
        }
    }
    return Status::OK();
}

template <typename CppType, typename ColumnType, bool has_filter>
Status FixLengthPlainDecoder::_decode_datetime64(MutableColumnPtr& doris_column,
                                                 ColumnSelectVector& select_vector) {
    auto& column_data = static_cast<ColumnVector<ColumnType>&>(*doris_column).get_data();
    size_t data_index = column_data.size();
    column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (size_t i = 0; i < run_length; ++i) {
                char* buf_start = _data->data + _offset;
                int64_t& date_value = *reinterpret_cast<int64_t*>(buf_start);
                auto& v = reinterpret_cast<CppType&>(column_data[data_index++]);
                v.from_unixtime(date_value / _decode_params->second_mask, *_decode_params->ctz);
                if constexpr (std::is_same_v<CppType, DateV2Value<DateTimeV2ValueType>>) {
                    // nanoseconds will be ignored.
                    v.set_microsecond((date_value % _decode_params->second_mask) *
                                      _decode_params->scale_to_nano_factor / 1000);
                    // TODO: the precision of datetime v1
                }
                _offset += _type_length;
            }
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            data_index += run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_CONTENT: {
            _offset += _type_length * run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_NULL: {
            // do nothing
            break;
        }
        }
    }
    return Status::OK();
}

template <typename CppType, typename ColumnType, bool has_filter>
Status FixLengthPlainDecoder::_decode_datetime96(MutableColumnPtr& doris_column,
                                                 ColumnSelectVector& select_vector) {
    auto& column_data = static_cast<ColumnVector<ColumnType>&>(*doris_column).get_data();
    size_t data_index = column_data.size();
    column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (size_t i = 0; i < run_length; ++i) {
                char* buf_start = _data->data + _offset;
                ParquetInt96& datetime96 = *reinterpret_cast<ParquetInt96*>(buf_start);
                auto& v = reinterpret_cast<CppType&>(column_data[data_index++]);
                int64_t micros = datetime96.to_timestamp_micros();
                v.from_unixtime(micros / 1000000, *_decode_params->ctz);
                if constexpr (std::is_same_v<CppType, DateV2Value<DateTimeV2ValueType>>) {
                    // spark.sql.parquet.outputTimestampType = INT96(NANOS) will lost precision.
                    // only keep microseconds.
                    v.set_microsecond(micros % 1000000);
                }
                _offset += _type_length;
            }
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            data_index += run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_CONTENT: {
            _offset += _type_length * run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_NULL: {
            // do nothing
            break;
        }
        }
    }
    return Status::OK();
}

template <typename DecimalPrimitiveType, bool has_filter>
Status FixLengthPlainDecoder::_decode_binary_decimal(MutableColumnPtr& doris_column,
                                                     DataTypePtr& data_type,
                                                     ColumnSelectVector& select_vector) {
    init_decimal_converter<DecimalPrimitiveType>(data_type);
    DecimalScaleParams& scale_params = _decode_params->decimal_scale;
#define M(FixedTypeLength, ValueCopyType, ScaleType)                                              \
    case FixedTypeLength:                                                                         \
        return _decode_binary_decimal_internal<DecimalPrimitiveType, has_filter, FixedTypeLength, \
                                               ValueCopyType, ScaleType>(doris_column, data_type, \
                                                                         select_vector);

#define APPLY_FOR_DECIMALS(ScaleType) \
    M(1, int64_t, ScaleType)          \
    M(2, int64_t, ScaleType)          \
    M(3, int64_t, ScaleType)          \
    M(4, int64_t, ScaleType)          \
    M(5, int64_t, ScaleType)          \
    M(6, int64_t, ScaleType)          \
    M(7, int64_t, ScaleType)          \
    M(8, int64_t, ScaleType)          \
    M(9, int128_t, ScaleType)         \
    M(10, int128_t, ScaleType)        \
    M(11, int128_t, ScaleType)        \
    M(12, int128_t, ScaleType)        \
    M(13, int128_t, ScaleType)        \
    M(14, int128_t, ScaleType)        \
    M(15, int128_t, ScaleType)        \
    M(16, int128_t, ScaleType)

    if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {
        switch (_type_length) {
            APPLY_FOR_DECIMALS(DecimalScaleParams::SCALE_UP)
        default:
            LOG(FATAL) << "__builtin_unreachable";
            __builtin_unreachable();
        }
    } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {
        switch (_type_length) {
            APPLY_FOR_DECIMALS(DecimalScaleParams::SCALE_DOWN)
        default:
            LOG(FATAL) << "__builtin_unreachable";
            __builtin_unreachable();
        }
    } else {
        switch (_type_length) {
            APPLY_FOR_DECIMALS(DecimalScaleParams::NO_SCALE)
        default:
            LOG(FATAL) << "__builtin_unreachable";
            __builtin_unreachable();
        }
    }
    return Status::OK();
#undef APPLY_FOR_DECIMALS
#undef M
}

template <typename DecimalPrimitiveType, bool has_filter, int fixed_type_length,
          typename ValueCopyType, DecimalScaleParams::ScaleType ScaleType>
Status FixLengthPlainDecoder::_decode_binary_decimal_internal(MutableColumnPtr& doris_column,
                                                              DataTypePtr& data_type,
                                                              ColumnSelectVector& select_vector) {
    auto& column_data =
            static_cast<ColumnDecimal<Decimal<DecimalPrimitiveType>>&>(*doris_column).get_data();
    size_t data_index = column_data.size();
    column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
    DecimalScaleParams& scale_params = _decode_params->decimal_scale;

    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (size_t i = 0; i < run_length; ++i) {
                char* buf_start = _data->data + _offset;
                // When Decimal in parquet is stored in byte arrays, binary and fixed,
                // the unscaled number must be encoded as two's complement using big-endian byte order.
                DecimalPrimitiveType result_value = 0;
                ValueCopyType value = 0;
                memcpy(reinterpret_cast<char*>(&value), buf_start, fixed_type_length);
                value = BitUtil::big_endian_to_host(value);
                value = value >> ((sizeof(value) - fixed_type_length) * 8);
                result_value = value;
                if constexpr (ScaleType == DecimalScaleParams::SCALE_UP) {
                    result_value *= scale_params.scale_factor;
                } else if constexpr (ScaleType == DecimalScaleParams::SCALE_DOWN) {
                    result_value /= scale_params.scale_factor;
                } else if constexpr (ScaleType == DecimalScaleParams::NO_SCALE) {
                    // do nothing
                } else {
                    LOG(FATAL) << "__builtin_unreachable";
                    __builtin_unreachable();
                }
                auto& v = reinterpret_cast<DecimalPrimitiveType&>(column_data[data_index++]);
                v = (DecimalPrimitiveType)result_value;
                _offset += fixed_type_length;
            }
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            data_index += run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_CONTENT: {
            _offset += _type_length * run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_NULL: {
            // do nothing
            break;
        }
        }
    }
    return Status::OK();
}

template <typename DecimalPrimitiveType, typename DecimalPhysicalType, bool has_filter>
Status FixLengthPlainDecoder::_decode_primitive_decimal(MutableColumnPtr& doris_column,
                                                        DataTypePtr& data_type,
                                                        ColumnSelectVector& select_vector) {
    init_decimal_converter<DecimalPrimitiveType>(data_type);
    DecimalScaleParams& scale_params = _decode_params->decimal_scale;
#define M(FixedTypeLength, T, ScaleType)                                                      \
    case FixedTypeLength:                                                                     \
        return _decode_primitive_decimal_internal<DecimalPrimitiveType, DecimalPhysicalType,  \
                                                  has_filter, FixedTypeLength, T, ScaleType>( \
                doris_column, data_type, select_vector);

#define APPLY_FOR_DECIMALS(ScaleType) \
    M(1, int64_t, ScaleType)          \
    M(2, int64_t, ScaleType)          \
    M(3, int64_t, ScaleType)          \
    M(4, int64_t, ScaleType)          \
    M(5, int64_t, ScaleType)          \
    M(6, int64_t, ScaleType)          \
    M(7, int64_t, ScaleType)          \
    M(8, int64_t, ScaleType)          \
    M(9, int128_t, ScaleType)         \
    M(10, int128_t, ScaleType)        \
    M(11, int128_t, ScaleType)        \
    M(12, int128_t, ScaleType)        \
    M(13, int128_t, ScaleType)        \
    M(14, int128_t, ScaleType)        \
    M(15, int128_t, ScaleType)        \
    M(16, int128_t, ScaleType)

    if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {
        switch (_type_length) {
            APPLY_FOR_DECIMALS(DecimalScaleParams::SCALE_UP)
        default:
            LOG(FATAL) << "__builtin_unreachable";
            __builtin_unreachable();
        }
    } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {
        switch (_type_length) {
            APPLY_FOR_DECIMALS(DecimalScaleParams::SCALE_DOWN)
        default:
            LOG(FATAL) << "__builtin_unreachable";
            __builtin_unreachable();
        }
    } else {
        switch (_type_length) {
            APPLY_FOR_DECIMALS(DecimalScaleParams::NO_SCALE)
        default:
            LOG(FATAL) << "__builtin_unreachable";
            __builtin_unreachable();
        }
    }
    return Status::OK();
#undef APPLY_FOR_DECIMALS
#undef M
}

template <typename DecimalPrimitiveType, typename DecimalPhysicalType, bool has_filter,
          int fixed_type_length, typename ValueCopyType, DecimalScaleParams::ScaleType ScaleType>
Status FixLengthPlainDecoder::_decode_primitive_decimal_internal(
        MutableColumnPtr& doris_column, DataTypePtr& data_type, ColumnSelectVector& select_vector) {
    auto& column_data =
            static_cast<ColumnDecimal<Decimal<DecimalPrimitiveType>>&>(*doris_column).get_data();
    size_t data_index = column_data.size();
    column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
    DecimalScaleParams& scale_params = _decode_params->decimal_scale;

    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (size_t i = 0; i < run_length; ++i) {
                char* buf_start = _data->data + _offset;
                ValueCopyType value = *reinterpret_cast<DecimalPhysicalType*>(buf_start);
                if constexpr (ScaleType == DecimalScaleParams::SCALE_UP) {
                    value *= scale_params.scale_factor;
                } else if constexpr (ScaleType == DecimalScaleParams::SCALE_DOWN) {
                    value /= scale_params.scale_factor;
                } else if constexpr (ScaleType == DecimalScaleParams::NO_SCALE) {
                    // do nothing
                } else {
                    LOG(FATAL) << "__builtin_unreachable";
                    __builtin_unreachable();
                }
                auto& v = reinterpret_cast<DecimalPrimitiveType&>(column_data[data_index++]);
                v = (DecimalPrimitiveType)value;
                _offset += _type_length;
            }
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            data_index += run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_CONTENT: {
            _offset += _type_length * run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_NULL: {
            // do nothing
            break;
        }
        }
    }
    return Status::OK();
}
} // namespace doris::vectorized
