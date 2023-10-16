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

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/parquet_types.h>

#include <algorithm>
#include <functional>
#include <ostream>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "gen_cpp/descriptors.pb.h"
#include "gutil/endian.h"
#include "io/file_factory.h"
#include "olap/olap_common.h"
#include "util/coding.h"
#include "util/slice.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exec/format/format_common.h"
#include "vec/exec/format/parquet/decoder.h"
#include "vec/exec/format/parquet/parquet_common.h"
namespace doris::vectorized {

namespace ParquetConvert {

template <tparquet::Type::type ParquetType>
struct PhysicalTypeTraits {};

template <>
struct PhysicalTypeTraits<tparquet::Type::INT32> {
    using DataType = int32_t;
    using ColumnType = ColumnVector<DataType>;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::INT64> {
    using DataType = int64_t;
    using ColumnType = ColumnVector<DataType>;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::FLOAT> {
    using DataType = float;
    using ColumnType = ColumnVector<DataType>;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::DOUBLE> {
    using DataType = double;
    using ColumnType = ColumnVector<DataType>;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::BYTE_ARRAY> {
    using DataType = String;
    using ColumnType = ColumnString;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::FIXED_LEN_BYTE_ARRAY> {
    using DataType = String;
    using ColumnType = ColumnString;
};

template <>
struct PhysicalTypeTraits<tparquet::Type::INT96> {
    using DataType = Int128;
    using ColumnType = ColumnVector<DataType>;
};

#define FOR_LOGICAL_NUMERIC_TYPES(M)        \
    M(TypeIndex::Int8, Int8, Int32)         \
    M(TypeIndex::UInt8, UInt8, Int32)       \
    M(TypeIndex::Int16, Int16, Int32)       \
    M(TypeIndex::UInt16, UInt16, Int32)     \
    M(TypeIndex::Int32, Int32, Int32)       \
    M(TypeIndex::UInt32, UInt32, Int32)     \
    M(TypeIndex::Int64, Int64, Int64)       \
    M(TypeIndex::UInt64, UInt64, Int64)     \
    M(TypeIndex::Float32, Float32, Float32) \
    M(TypeIndex::Float64, Float64, Float64)

#define FOR_LOGICAL_DECIMAL_TYPES(M)             \
    M(TypeIndex::Decimal32, Decimal32, Int32)    \
    M(TypeIndex::Decimal64, Decimal64, Int64)    \
    M(TypeIndex::Decimal128, Decimal128, Int128) \
    M(TypeIndex::Decimal128I, Decimal128, Int128)

struct ConvertParams {
    // schema.logicalType.TIMESTAMP.isAdjustedToUTC == false
    static const cctz::time_zone utc0;
    // schema.logicalType.TIMESTAMP.isAdjustedToUTC == true, we should set the time zone
    cctz::time_zone* ctz = nullptr;
    size_t offset_days = 0;
    int64_t second_mask = 1;
    int64_t scale_to_nano_factor = 1;
    DecimalScaleParams decimal_scale;
    FieldSchema* field_schema = nullptr;
    size_t start_idx = 0;

    void init(FieldSchema* field_schema_, cctz::time_zone* ctz_) {
        field_schema = field_schema_;
        if (ctz_ != nullptr) {
            ctz = ctz_;
        }
        const auto& schema = field_schema->parquet_schema;
        if (schema.__isset.logicalType && schema.logicalType.__isset.TIMESTAMP) {
            const auto& timestamp_info = schema.logicalType.TIMESTAMP;
            if (!timestamp_info.isAdjustedToUTC) {
                // should set timezone to utc+0
                ctz = const_cast<cctz::time_zone*>(&utc0);
            }
            const auto& time_unit = timestamp_info.unit;
            if (time_unit.__isset.MILLIS) {
                second_mask = 1000;
                scale_to_nano_factor = 1000000;
            } else if (time_unit.__isset.MICROS) {
                second_mask = 1000000;
                scale_to_nano_factor = 1000;
            } else if (time_unit.__isset.NANOS) {
                second_mask = 1000000000;
                scale_to_nano_factor = 1;
            }
        } else if (schema.__isset.converted_type) {
            const auto& converted_type = schema.converted_type;
            if (converted_type == tparquet::ConvertedType::TIMESTAMP_MILLIS) {
                second_mask = 1000;
                scale_to_nano_factor = 1000000;
            } else if (converted_type == tparquet::ConvertedType::TIMESTAMP_MICROS) {
                second_mask = 1000000;
                scale_to_nano_factor = 1000;
            }
        }

        if (ctz) {
            VecDateTimeValue t;
            t.from_unixtime(0, *ctz);
            offset_days = t.day() == 31 ? 0 : 1;
        }
    }

    template <typename DecimalPrimitiveType>
    void init_decimal_converter(DataTypePtr& data_type) {
        if (field_schema == nullptr || decimal_scale.scale_type != DecimalScaleParams::NOT_INIT) {
            return;
        }
        auto scale = field_schema->parquet_schema.scale;
        auto* decimal_type = static_cast<DataTypeDecimal<Decimal<DecimalPrimitiveType>>*>(
                const_cast<IDataType*>(remove_nullable(data_type).get()));
        auto dest_scale = decimal_type->get_scale();
        if (dest_scale > scale) {
            decimal_scale.scale_type = DecimalScaleParams::SCALE_UP;
            decimal_scale.scale_factor =
                    DecimalScaleParams::get_scale_factor<DecimalPrimitiveType>(dest_scale - scale);
        } else if (dest_scale < scale) {
            decimal_scale.scale_type = DecimalScaleParams::SCALE_DOWN;
            decimal_scale.scale_factor =
                    DecimalScaleParams::get_scale_factor<DecimalPrimitiveType>(scale - dest_scale);
        } else {
            decimal_scale.scale_type = DecimalScaleParams::NO_SCALE;
            decimal_scale.scale_factor = 1;
        }
    }
};

Status convert_data_type_from_parquet(tparquet::Type::type parquet_type, PrimitiveType,
                                      vectorized::DataTypePtr& ans_data_type, DataTypePtr& src_type,
                                      bool* need_convert);

struct ColumnConvert {
    virtual Status convert(const IColumn* src_col, IColumn* dst_col) { return Status::OK(); }

    virtual ~ColumnConvert() = default;

    void convert_null(const IColumn** src_col, IColumn** dst_col) const {
        size_t rows = (*src_col)->size();
        if ((*src_col)->is_nullable()) {
            auto src_nullable_column = static_cast<const ColumnNullable*>(*src_col);
            auto dst_nullable_column = static_cast<ColumnNullable*>(*dst_col);
            auto& src_null_data = src_nullable_column->get_null_map_column().get_data();
            dst_nullable_column->get_null_map_column().resize(_convert_params->start_idx + rows);
            auto& dst_null_data = dst_nullable_column->get_null_map_column().get_data();
            for (auto j = 0; j < rows; j++) {
                dst_null_data[_convert_params->start_idx + j] = src_null_data[j];
            }

            *src_col = &src_nullable_column->get_nested_column();
            *dst_col = &dst_nullable_column->get_nested_column();
        }
    }

public:
    ConvertParams* _convert_params;
};

template <typename src_type, typename dst_type, bool is_nullable>
struct NumberColumnConvert : public ColumnConvert {
    Status convert(const IColumn* src_col, IColumn* dst_col) override;
};

template <typename src_type, typename dst_type, bool is_nullable>
Status NumberColumnConvert<src_type, dst_type, is_nullable>::convert(const IColumn* src_col,
                                                                     IColumn* dst_col) {
    size_t rows = src_col->size();
    if constexpr (is_nullable) {
        convert_null(&src_col, &dst_col);
    }
    auto& src_data = static_cast<const ColumnVector<src_type>*>(src_col)->get_data();
    dst_col->resize(_convert_params->start_idx + rows);
    auto& data = static_cast<ColumnVector<dst_type>*>(dst_col)->get_data();

    for (int i = 0; i < rows; i++) {
        dst_type value = static_cast<dst_type>(src_data[i]);
        data[_convert_params->start_idx + i] = value;
    }

    return Status::OK();
}
template <typename src_type, bool is_nullable>
struct NumberColumnToStringConvert : public ColumnConvert {
    Status convert(const IColumn* src_col, IColumn* dst_col) override;
};

template <typename src_type, bool is_nullable>
Status NumberColumnToStringConvert<src_type, is_nullable>::convert(const IColumn* src_col,
                                                                   IColumn* dst_col) {
    size_t rows = src_col->size();
    if constexpr (is_nullable) {
        convert_null(&src_col, &dst_col);
    }
    auto& src_data = static_cast<const ColumnVector<src_type>*>(src_col)->get_data();
    auto str_col = static_cast<ColumnString*>(dst_col);
    for (int i = 0; i < rows; i++) {
        std::string value = std::to_string(src_data[i]);
        str_col->insert_data(value.data(), value.size());
    }
    return Status::OK();
}

template <bool is_nullable>
struct int128totimestamp : public ColumnConvert {
public:
    [[nodiscard]] static uint64_t to_timestamp_micros(uint32_t hi, uint64_t lo) {
        return (hi - ParquetInt96::JULIAN_EPOCH_OFFSET_DAYS) * ParquetInt96::MICROS_IN_DAY +
               lo / ParquetInt96::NANOS_PER_MICROSECOND;
    }
    Status convert(const IColumn* src_col, IColumn* dst_col) override {
        size_t rows = src_col->size();
        if constexpr (is_nullable) {
            convert_null(&src_col, &dst_col);
        }
        auto& src_data = static_cast<const ColumnVector<Int128>*>(src_col)->get_data();
        dst_col->resize(_convert_params->start_idx + rows);
        auto& data = static_cast<ColumnVector<UInt64>*>(dst_col)->get_data();

        for (int i = 0; i < rows; i++) {
            __int128 x = src_data[i];
            uint32_t hi = x >> 64;
            uint64_t lo = (x << 64) >> 64;
            auto& num = data[_convert_params->start_idx + i];
            auto& value = reinterpret_cast<DateV2Value<DateTimeV2ValueType>&>(num);
            int64_t micros = to_timestamp_micros(hi, lo);
            value.from_unixtime(micros / 1000000, *_convert_params->ctz);
            value.set_microsecond(micros % 1000000);
        }
        return Status::OK();
    }
};

template <bool is_nullable>
struct int64totimestamp : public ColumnConvert {
public:
    Status convert(const IColumn* src_col, IColumn* dst_col) override {
        size_t rows = src_col->size();
        if constexpr (is_nullable) {
            convert_null(&src_col, &dst_col);
        }
        dst_col->resize(_convert_params->start_idx + rows);

        auto& src_data = static_cast<const ColumnVector<Int64>*>(src_col)->get_data();
        auto& data = static_cast<ColumnVector<UInt64>*>(dst_col)->get_data();
        for (int i = 0; i < rows; i++) {
            int64 x = src_data[i];
            dst_col = static_cast<ColumnVector<UInt64>*>(dst_col);
            auto& num = data[_convert_params->start_idx + i];
            auto& value = reinterpret_cast<DateV2Value<DateTimeV2ValueType>&>(num);
            value.from_unixtime(x / _convert_params->second_mask, *_convert_params->ctz);
            value.set_microsecond((x % _convert_params->second_mask) *
                                  _convert_params->scale_to_nano_factor / 1000);
        }
        return Status::OK();
    }
};

template <bool is_nullable>
class int32todate : public ColumnConvert {
public:
    Status convert(const IColumn* src_col, IColumn* dst_col) override {
        size_t rows = src_col->size();
        if constexpr (is_nullable) {
            convert_null(&src_col, &dst_col);
        }
        dst_col->resize(_convert_params->start_idx + rows);

        auto& src_data = static_cast<const ColumnVector<int32>*>(src_col)->get_data();
        auto& data = static_cast<ColumnDateV2*>(dst_col)->get_data();
        date_day_offset_dict& date_dict = date_day_offset_dict::get();

        for (int i = 0; i < rows; i++) {
            auto& value = reinterpret_cast<DateV2Value<DateV2ValueType>&>(
                    data[_convert_params->start_idx + i]);
            int64_t date_value = (int64_t)src_data[i] + _convert_params->offset_days;
            value = date_dict[date_value];
        }

        return Status::OK();
    }
};

template <typename DecimalType, bool is_nullable, DecimalScaleParams::ScaleType ScaleType>
class stringtodecimal : public ColumnConvert {
public:
    Status convert(const IColumn* src_col, IColumn* dst_col) override {
        size_t rows = src_col->size();
        if constexpr (is_nullable) {
            convert_null(&src_col, &dst_col);
        }
        DecimalScaleParams& scale_params = _convert_params->decimal_scale;
        auto buf = static_cast<const ColumnString*>(src_col)->get_chars().data();
        auto& offset = static_cast<const ColumnString*>(src_col)->get_offsets();
        dst_col->resize(_convert_params->start_idx + rows);

        auto& data = static_cast<ColumnDecimal<DecimalType>*>(dst_col)->get_data();
        for (int i = 0; i < rows; i++) {
            int len = offset[i] - offset[i - 1];
            // When Decimal in parquet is stored in byte arrays, binary and fixed,
            // the unscaled number must be encoded as two's complement using big-endian byte order.
            Int128 value = 0;
            memcpy(reinterpret_cast<char*>(&value), buf + offset[i - 1], len);
            value = BitUtil::big_endian_to_host(value);
            value = value >> ((sizeof(value) - len) * 8);
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
            auto& v = reinterpret_cast<DecimalType&>(data[_convert_params->start_idx + i]);
            v = (DecimalType)value;
        }

        return Status::OK();
    }
};
template <typename NumberType, typename DecimalPhysicalType, bool is_nullable,
          DecimalScaleParams::ScaleType ScaleType>
class numbertodecimal : public ColumnConvert {
public:
    Status convert(const IColumn* src_col, IColumn* dst_col) override {
        size_t rows = src_col->size();
        if constexpr (is_nullable) {
            convert_null(&src_col, &dst_col);
        }
        auto* src_data = static_cast<const ColumnVector<NumberType>*>(src_col)->get_data().data();
        dst_col->resize(_convert_params->start_idx + rows);

        DecimalScaleParams& scale_params = _convert_params->decimal_scale;
        auto* data = static_cast<ColumnDecimal<Decimal<DecimalPhysicalType>>*>(dst_col)
                             ->get_data()
                             .data();

        for (int i = 0; i < rows; i++) {
            Int128 value = src_data[i];
            if constexpr (ScaleType == DecimalScaleParams::SCALE_UP) {
                value *= scale_params.scale_factor;
            } else if constexpr (ScaleType == DecimalScaleParams::SCALE_DOWN) {
                value /= scale_params.scale_factor;
            }
            data[_convert_params->start_idx + i] = (DecimalPhysicalType)value;
        }
        return Status::OK();
    }
};

template <bool is_nullable>
class stringtodecimalstring : public ColumnConvert {
public:
    Status convert(const IColumn* src_col, IColumn* dst_col) override {
        size_t rows = src_col->size();
        if constexpr (is_nullable) {
            convert_null(&src_col, &dst_col);
        }
        auto buf = static_cast<const ColumnString*>(src_col)->get_chars().data();
        auto& offset = static_cast<const ColumnString*>(src_col)->get_offsets();

        auto data = static_cast<ColumnString*>(dst_col);
        for (int i = 0; i < rows; i++) {
            int len = offset[i] - offset[i - 1];
            // When Decimal in parquet is stored in byte arrays, binary and fixed,
            // the unscaled number must be encoded as two's complement using big-endian byte order.
            Int64 value = 0;
            memcpy(reinterpret_cast<char*>(&value), buf + offset[i - 1], len);
            value = BitUtil::big_endian_to_host(value);
            value = value >> ((sizeof(value) - len) * 8);
            std::string ans = reinterpret_cast<Decimal64&>(value).to_string(
                    _convert_params->field_schema->parquet_schema.scale);
            data->insert_data(ans.data(), ans.size());
        }
        return Status::OK();
    }
};

template <bool is_nullable>
class int128totimestampstring : public ColumnConvert {
public:
    Status convert(const IColumn* src_col, IColumn* dst_col) override {
        size_t rows = src_col->size();
        if constexpr (is_nullable) {
            convert_null(&src_col, &dst_col);
        }

        auto& src_data = static_cast<const ColumnVector<Int128>*>(src_col)->get_data();
        auto data = static_cast<ColumnString*>(dst_col);

        for (int i = 0; i < rows; i++) {
            __int128 x = src_data[i];
            uint32_t hi = x >> 64;
            uint64_t lo = (x << 64) >> 64;
            uint64_t num = 0;
            auto& value = reinterpret_cast<DateV2Value<DateTimeV2ValueType>&>(num);
            int64_t micros = int128totimestamp<is_nullable>::to_timestamp_micros(hi, lo);
            value.from_unixtime(micros / 1000000, *_convert_params->ctz);
            value.set_microsecond(micros % 1000000);
            std::string buf;
            buf.resize(20);
            char* end = value.to_string(buf.data());
            data->insert_data(buf.data(), end - buf.data());
        }

        return Status::OK();
    }
};

template <bool is_nullable = true>
inline Status get_converter_impl(std::shared_ptr<const IDataType> src_data_type,
                                 PrimitiveType show_type,
                                 std::shared_ptr<const IDataType> dst_data_type,
                                 std::unique_ptr<ColumnConvert>* converter,
                                 ConvertParams* convert_params) {
    auto src_type = src_data_type->get_type_id();
    auto dst_type = dst_data_type->get_type_id();
    switch (dst_type) {
#define DISPATCH(NUMERIC_TYPE, CPP_NUMERIC_TYPE, PHYSICAL_TYPE)                                    \
    case NUMERIC_TYPE:                                                                             \
        switch (src_type) {                                                                        \
        case TypeIndex::UInt8:                                                                     \
            *converter =                                                                           \
                    std::make_unique<NumberColumnConvert<UInt8, CPP_NUMERIC_TYPE, is_nullable>>(); \
            break;                                                                                 \
        case TypeIndex::Int32:                                                                     \
            *converter =                                                                           \
                    std::make_unique<NumberColumnConvert<Int32, CPP_NUMERIC_TYPE, is_nullable>>(); \
            break;                                                                                 \
        case TypeIndex::Int64:                                                                     \
            *converter =                                                                           \
                    std::make_unique<NumberColumnConvert<Int64, CPP_NUMERIC_TYPE, is_nullable>>(); \
            break;                                                                                 \
        case TypeIndex::Float32:                                                                   \
            *converter = std::make_unique<                                                         \
                    NumberColumnConvert<Float32, CPP_NUMERIC_TYPE, is_nullable>>();                \
            break;                                                                                 \
        case TypeIndex::Float64:                                                                   \
            *converter = std::make_unique<                                                         \
                    NumberColumnConvert<Float64, CPP_NUMERIC_TYPE, is_nullable>>();                \
            break;                                                                                 \
        case TypeIndex::Int128:                                                                    \
            *converter = std::make_unique<                                                         \
                    NumberColumnConvert<Int128, CPP_NUMERIC_TYPE, is_nullable>>();                 \
            break;                                                                                 \
        default:                                                                                   \
            break;                                                                                 \
        }                                                                                          \
        break;
        FOR_LOGICAL_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    case TypeIndex::String: {
        if (src_type == TypeIndex::String) {
            if (show_type == PrimitiveType::TYPE_DECIMAL64) {
                *converter = std::make_unique<stringtodecimalstring<is_nullable>>();
                break;
            }
        } else if (src_type == TypeIndex::Int128) {
            *converter = std::make_unique<int128totimestampstring<is_nullable>>();
            break;
        }

        switch (src_type) {
#define DISPATCH1(NUMERIC_TYPE, CPP_NUMERIC_TYPE, PHYSICAL_TYPE)                                \
    case NUMERIC_TYPE:                                                                          \
        *converter =                                                                            \
                std::make_unique<NumberColumnToStringConvert<CPP_NUMERIC_TYPE, is_nullable>>(); \
        break;
            FOR_LOGICAL_NUMERIC_TYPES(DISPATCH1)
#undef DISPATCH1
        default:
            break;
        }
        break;
    }
    case TypeIndex::DateV2:
        if (src_type == TypeIndex::Int32) {
            *converter = std::make_unique<int32todate<is_nullable>>();
        }
        break;
    case TypeIndex::DateTimeV2:
        if (src_type == TypeIndex::Int128) {
            *converter = std::make_unique<int128totimestamp<is_nullable>>();
        } else if (src_type == TypeIndex::Int64) {
            *converter = std::make_unique<int64totimestamp<is_nullable>>();
        }
        break;
#define DISPATCH2(TypeIndex_DECIMAL_TYPE, DECIMAL_TYPE, PRIMARY_TYPE)                             \
    case TypeIndex_DECIMAL_TYPE: {                                                                \
        convert_params->init_decimal_converter<PRIMARY_TYPE>(dst_data_type);                      \
        DecimalScaleParams& scale_params = convert_params->decimal_scale;                         \
        if (src_type == TypeIndex::Int128) {                                                      \
            if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {                        \
                *converter = std::make_unique<numbertodecimal<Int128, PRIMARY_TYPE, is_nullable,  \
                                                              DecimalScaleParams::SCALE_UP>>();   \
            } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {               \
                *converter = std::make_unique<numbertodecimal<Int128, PRIMARY_TYPE, is_nullable,  \
                                                              DecimalScaleParams::SCALE_DOWN>>(); \
            } else {                                                                              \
                *converter = std::make_unique<numbertodecimal<Int128, PRIMARY_TYPE, is_nullable,  \
                                                              DecimalScaleParams::NO_SCALE>>();   \
            }                                                                                     \
        } else if (src_type == TypeIndex::String) {                                               \
            if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {                        \
                *converter = std::make_unique<stringtodecimal<DECIMAL_TYPE, is_nullable,          \
                                                              DecimalScaleParams::SCALE_UP>>();   \
            } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {               \
                *converter = std::make_unique<stringtodecimal<DECIMAL_TYPE, is_nullable,          \
                                                              DecimalScaleParams::SCALE_DOWN>>(); \
            } else {                                                                              \
                *converter = std::make_unique<stringtodecimal<DECIMAL_TYPE, is_nullable,          \
                                                              DecimalScaleParams::NO_SCALE>>();   \
            }                                                                                     \
        } else if (src_type == TypeIndex::Int32) {                                                \
            if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {                        \
                *converter = std::make_unique<numbertodecimal<Int32, PRIMARY_TYPE, is_nullable,   \
                                                              DecimalScaleParams::SCALE_UP>>();   \
            } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {               \
                *converter = std::make_unique<numbertodecimal<Int32, PRIMARY_TYPE, is_nullable,   \
                                                              DecimalScaleParams::SCALE_DOWN>>(); \
            } else {                                                                              \
                *converter = std::make_unique<numbertodecimal<Int32, PRIMARY_TYPE, is_nullable,   \
                                                              DecimalScaleParams::NO_SCALE>>();   \
            }                                                                                     \
        } else if (src_type == TypeIndex::Int64) {                                                \
            if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {                        \
                *converter = std::make_unique<numbertodecimal<Int64, PRIMARY_TYPE, is_nullable,   \
                                                              DecimalScaleParams::SCALE_UP>>();   \
            } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {               \
                *converter = std::make_unique<numbertodecimal<Int64, PRIMARY_TYPE, is_nullable,   \
                                                              DecimalScaleParams::SCALE_DOWN>>(); \
            } else {                                                                              \
                *converter = std::make_unique<numbertodecimal<Int64, PRIMARY_TYPE, is_nullable,   \
                                                              DecimalScaleParams::NO_SCALE>>();   \
            }                                                                                     \
        }                                                                                         \
        break;                                                                                    \
    }

        FOR_LOGICAL_DECIMAL_TYPES(DISPATCH2)
#undef DISPATCH2
    default:
        break;
    }

    if (*converter == nullptr) {
        return Status::NotSupported("Can't cast type {} to type {}", getTypeName(src_type),
                                    getTypeName(dst_type));
    }
    (*converter)->_convert_params = convert_params;
    return Status::OK();
}

Status get_converter(std::shared_ptr<const IDataType> src_type, PrimitiveType show_type,
                     std::shared_ptr<const IDataType> dst_type,
                     std::unique_ptr<ColumnConvert>* converter, ConvertParams* convert_param);

}; // namespace ParquetConvert

}; // namespace doris::vectorized