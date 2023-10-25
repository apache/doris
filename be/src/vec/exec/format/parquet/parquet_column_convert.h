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
#include "gutil/strings/numbers.h"
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
struct PhysicalTypeTraits<tparquet::Type::BOOLEAN> {
    using DataType = uint8;
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
    using DataType = ParquetInt96;
    using ColumnType = ColumnVector<Int8>;
};

#define FOR_LOGICAL_NUMERIC_TYPES(M)        \
    M(TypeIndex::Int8, Int8, Int32)         \
    M(TypeIndex::Int16, Int16, Int32)       \
    M(TypeIndex::Int32, Int32, Int32)       \
    M(TypeIndex::Int64, Int64, Int64)       \
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

    void init(FieldSchema* field_schema_, cctz::time_zone* ctz_, size_t start_idx_ = 0) {
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
        start_idx = start_idx_;
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

/*
* parquet_physical_type : The type of data stored in parquet.
* Read data into columns returned by get_column according to the physical type of parquet.
* show_type : The data format that should be displayed.
* doris_column : What type of column does the upper layer need to put the data in.
*
* example :
*      In hive, if decimal is stored as FIXED_LENBYTE_ARRAY in parquet,
*  then we use `ALTER TABLE TableName CHANGE COLUMN Col_Decimal Col_Decimal String;`
*  to convert this column to string type.
*      parquet_type : FIXED_LEN_BYTE_ARRAY.
*      ans_data_type : ColumnInt8
*      show_type : Decimal.
*      doris_column : ColumnString.
*/
ColumnPtr get_column(tparquet::Type::type parquet_physical_type, PrimitiveType show_type,
                     ColumnPtr& doris_column, DataTypePtr& doris_type, bool* need_convert);

struct ColumnConvert {
    virtual Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) { return Status::OK(); }

    virtual ~ColumnConvert() = default;

    void convert_null(ColumnPtr& src_col, MutableColumnPtr& dst_col) {
        src_col = remove_nullable(src_col);
        dst_col = remove_nullable(dst_col->get_ptr())->assume_mutable();
    }

public:
    ConvertParams* _convert_params;
};

template <tparquet::Type::type parquet_physical_type, typename dst_type>
struct NumberToNumberConvert : public ColumnConvert {
    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        using ColumnType = typename PhysicalTypeTraits<parquet_physical_type>::ColumnType;
        convert_null(src_col, dst_col);

        size_t rows = src_col->size();
        auto& src_data = static_cast<const ColumnType*>(src_col.get())->get_data();

        dst_col->resize(_convert_params->start_idx + rows);
        auto& data = static_cast<ColumnVector<dst_type>&>(*dst_col.get()).get_data();
        for (int i = 0; i < rows; i++) {
            dst_type value = static_cast<dst_type>(src_data[i]);
            data[_convert_params->start_idx + i] = value;
        }

        return Status::OK();
    }
};

template <tparquet::Type::type parquet_physical_type>
struct NumberToStringConvert : public ColumnConvert {
    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        using ColumnType = typename PhysicalTypeTraits<parquet_physical_type>::ColumnType;
        convert_null(src_col, dst_col);

        size_t rows = src_col->size();
        auto& src_data = static_cast<const ColumnType*>(src_col.get())->get_data();

        char buf[100];
        auto str_col = static_cast<ColumnString*>(dst_col.get());
        for (int i = 0; i < rows; i++) {
            if constexpr (parquet_physical_type == tparquet::Type::FLOAT) {
                int len = FastFloatToBuffer(src_data[i], buf, true);
                str_col->insert_data(buf, len);

            } else if constexpr (parquet_physical_type == tparquet::Type::DOUBLE) {
                int len = FastDoubleToBuffer(src_data[i], buf, true);
                str_col->insert_data(buf, len);
            } else if constexpr (parquet_physical_type == tparquet::Type::INT32) {
                char* end = FastInt32ToBufferLeft(src_data[i], buf);
                str_col->insert_data(buf, end - buf);

            } else if constexpr (parquet_physical_type == tparquet::Type::INT64) {
                char* end = FastInt64ToBufferLeft(src_data[i], buf);
                str_col->insert_data(buf, end - buf);

            } else {
                string value = std::to_string(src_data[i]);
                str_col->insert_data(value.data(), value.size());
            }
        }
        return Status::OK();
    }
};

struct Int96toTimestamp : public ColumnConvert {
public:
    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        convert_null(src_col, dst_col);

        size_t rows = src_col->size() / sizeof(ParquetInt96);
        auto& src_data = static_cast<const ColumnVector<Int8>*>(src_col.get())->get_data();
        auto ParquetInt96_data = (ParquetInt96*)src_data.data();
        dst_col->resize(_convert_params->start_idx + rows);
        auto& data = static_cast<ColumnVector<UInt64>*>(dst_col.get())->get_data();

        for (int i = 0; i < rows; i++) {
            ParquetInt96 x = ParquetInt96_data[i];
            auto& num = data[_convert_params->start_idx + i];
            auto& value = reinterpret_cast<DateV2Value<DateTimeV2ValueType>&>(num);
            int64_t micros = x.to_timestamp_micros();
            value.from_unixtime(micros / 1000000, *_convert_params->ctz);
            value.set_microsecond(micros % 1000000);
        }
        return Status::OK();
    }
};

struct Int64ToTimestamp : public ColumnConvert {
public:
    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        convert_null(src_col, dst_col);

        size_t rows = src_col->size();
        dst_col->resize(_convert_params->start_idx + rows);

        auto src_data = static_cast<const ColumnVector<int64_t>*>(src_col.get())->get_data().data();
        auto& data = static_cast<ColumnVector<UInt64>*>(dst_col.get())->get_data();

        for (int i = 0; i < rows; i++) {
            int64_t x = src_data[i];
            auto& num = data[_convert_params->start_idx + i];
            auto& value = reinterpret_cast<DateV2Value<DateTimeV2ValueType>&>(num);
            value.from_unixtime(x / _convert_params->second_mask, *_convert_params->ctz);
            value.set_microsecond((x % _convert_params->second_mask) *
                                  (_convert_params->scale_to_nano_factor / 1000));
        }
        return Status::OK();
    }
};

class Int32ToDate : public ColumnConvert {
public:
    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        convert_null(src_col, dst_col);

        size_t rows = src_col->size();
        dst_col->resize(_convert_params->start_idx + rows);

        auto& src_data = static_cast<const ColumnVector<int32>*>(src_col.get())->get_data();
        auto& data = static_cast<ColumnDateV2*>(dst_col.get())->get_data();
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

template <typename DecimalType, typename ValueCopyType, DecimalScaleParams::ScaleType ScaleType>
class StringToDecimal : public ColumnConvert {
public:
    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        convert_null(src_col, dst_col);

        size_t rows = src_col->size();
        DecimalScaleParams& scale_params = _convert_params->decimal_scale;
        auto buf = static_cast<const ColumnString*>(src_col.get())->get_chars().data();
        auto& offset = static_cast<const ColumnString*>(src_col.get())->get_offsets();
        dst_col->resize(_convert_params->start_idx + rows);

        auto& data = static_cast<ColumnDecimal<DecimalType>*>(dst_col.get())->get_data();
        for (int i = 0; i < rows; i++) {
            size_t len = offset[i] - offset[i - 1];
            // When Decimal in parquet is stored in byte arrays, binary and fixed,
            // the unscaled number must be encoded as two's complement using big-endian byte order.
            ValueCopyType value = 0;
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
template <typename NumberType, typename DecimalPhysicalType, typename ValueCopyType,
          DecimalScaleParams::ScaleType ScaleType>
class NumberToDecimal : public ColumnConvert {
public:
    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        convert_null(src_col, dst_col);

        size_t rows = src_col->size();
        auto* src_data =
                static_cast<const ColumnVector<NumberType>*>(src_col.get())->get_data().data();
        dst_col->resize(_convert_params->start_idx + rows);

        DecimalScaleParams& scale_params = _convert_params->decimal_scale;
        auto* data = static_cast<ColumnDecimal<Decimal<DecimalPhysicalType>>*>(dst_col.get())
                             ->get_data()
                             .data();

        for (int i = 0; i < rows; i++) {
            ValueCopyType value = src_data[i];
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

template <typename DecimalType, typename ValueCopyType>
class StringToDecimalString : public ColumnConvert {
public:
    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        convert_null(src_col, dst_col);

        size_t rows = src_col->size();

        auto buf = static_cast<const ColumnString*>(src_col.get())->get_chars().data();
        auto& offset = static_cast<const ColumnString*>(src_col.get())->get_offsets();

        auto data = static_cast<ColumnString*>(dst_col.get());
        for (int i = 0; i < rows; i++) {
            int len = offset[i] - offset[i - 1];
            // When Decimal in parquet is stored in byte arrays, binary and fixed,
            // the unscaled number must be encoded as two's complement using big-endian byte order.
            ValueCopyType value = 0;
            memcpy(reinterpret_cast<char*>(&value), buf + offset[i - 1], len);
            value = BitUtil::big_endian_to_host(value);
            value = value >> ((sizeof(value) - len) * 8);
            std::string ans = reinterpret_cast<DecimalType&>(value).to_string(
                    _convert_params->field_schema->parquet_schema.scale);
            data->insert_data(ans.data(), ans.size());
        }
        return Status::OK();
    }
};

class Int32ToDateString : public ColumnConvert {
public:
    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        convert_null(src_col, dst_col);

        size_t rows = src_col->size();

        auto& src_data = static_cast<const ColumnVector<int32>*>(src_col.get())->get_data();
        date_day_offset_dict& date_dict = date_day_offset_dict::get();

        auto str_col = static_cast<ColumnString*>(dst_col.get());
        char buf[50];
        for (int i = 0; i < rows; i++) {
            int64_t date_value = (int64_t)src_data[i] + _convert_params->offset_days;
            DateV2Value<DateV2ValueType> value = date_dict[date_value];
            char* end = value.to_string(buf);
            str_col->insert_data(buf, end - buf);
        }

        return Status::OK();
    }
};

class Int96ToTimestampString : public ColumnConvert {
public:
    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        convert_null(src_col, dst_col);

        auto& src_data = static_cast<const ColumnVector<Int8>*>(src_col.get())->get_data();
        auto dst_data = static_cast<ColumnString*>(dst_col.get());

        size_t rows = src_col->size() / sizeof(ParquetInt96);
        ParquetInt96* data = (ParquetInt96*)src_data.data();

        char buf[50];
        for (int i = 0; i < rows; i++) {
            uint64_t num = 0;
            auto& value = reinterpret_cast<DateV2Value<DateTimeV2ValueType>&>(num);
            int64_t micros = data[i].to_timestamp_micros();
            value.from_unixtime(micros / 1000000, *_convert_params->ctz);
            value.set_microsecond(micros % 1000000);
            char* end = value.to_string(buf);
            dst_data->insert_data(buf, end - buf);
        }
        return Status::OK();
    }
};

inline Status get_converter(tparquet::Type::type parquet_physical_type, PrimitiveType show_type,
                            std::shared_ptr<const IDataType> dst_data_type,
                            std::unique_ptr<ColumnConvert>* converter,
                            ConvertParams* convert_params) {
    auto dst_type = remove_nullable(dst_data_type)->get_type_id();
    switch (dst_type) {
#define DISPATCH(NUMERIC_TYPE, CPP_NUMERIC_TYPE, PHYSICAL_TYPE)                          \
    case NUMERIC_TYPE:                                                                   \
        switch (parquet_physical_type) {                                                 \
        case tparquet::Type::BOOLEAN:                                                    \
            *converter = std::make_unique<                                               \
                    NumberToNumberConvert<tparquet::Type::BOOLEAN, CPP_NUMERIC_TYPE>>(); \
            break;                                                                       \
        case tparquet::Type::INT32:                                                      \
            *converter = std::make_unique<                                               \
                    NumberToNumberConvert<tparquet::Type::INT32, CPP_NUMERIC_TYPE>>();   \
            break;                                                                       \
        case tparquet::Type::INT64:                                                      \
            *converter = std::make_unique<                                               \
                    NumberToNumberConvert<tparquet::Type::INT64, CPP_NUMERIC_TYPE>>();   \
            break;                                                                       \
        case tparquet::Type::FLOAT:                                                      \
            *converter = std::make_unique<                                               \
                    NumberToNumberConvert<tparquet::Type::FLOAT, CPP_NUMERIC_TYPE>>();   \
            break;                                                                       \
        case tparquet::Type::DOUBLE:                                                     \
            *converter = std::make_unique<                                               \
                    NumberToNumberConvert<tparquet::Type::DOUBLE, CPP_NUMERIC_TYPE>>();  \
            break;                                                                       \
        default:                                                                         \
            break;                                                                       \
        }                                                                                \
        break;
        FOR_LOGICAL_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    case TypeIndex::String: {
        if (tparquet::Type::FIXED_LEN_BYTE_ARRAY == parquet_physical_type) {
            if (show_type == PrimitiveType::TYPE_DECIMAL32) {
                *converter = std::make_unique<StringToDecimalString<Decimal32, Int32>>();
                break;
            } else if (show_type == PrimitiveType::TYPE_DECIMAL64) {
                *converter = std::make_unique<StringToDecimalString<Decimal64, Int64>>();
                break;
            } else if (show_type == PrimitiveType::TYPE_DECIMALV2) {
                *converter = std::make_unique<StringToDecimalString<Decimal128, Int128>>();
                break;
            } else if (show_type == PrimitiveType::TYPE_DECIMAL128I) {
                *converter = std::make_unique<StringToDecimalString<Decimal128, Int128>>();
                break;
            }

        } else if (tparquet::Type::INT96 == parquet_physical_type) {
            *converter = std::make_unique<Int96ToTimestampString>();
            break;
        } else if (tparquet::Type::INT32 == parquet_physical_type) {
            if (show_type == PrimitiveType::TYPE_DATEV2) {
                *converter = std::make_unique<Int32ToDateString>();
                break;
            }
        }

        if (parquet_physical_type == tparquet::Type::BOOLEAN) {
            *converter = std::make_unique<NumberToStringConvert<tparquet::Type::BOOLEAN>>();
        } else if (parquet_physical_type == tparquet::Type::INT32) {
            *converter = std::make_unique<NumberToStringConvert<tparquet::Type::INT32>>();

        } else if (parquet_physical_type == tparquet::Type::INT64) {
            *converter = std::make_unique<NumberToStringConvert<tparquet::Type::INT64>>();

        } else if (parquet_physical_type == tparquet::Type::FLOAT) {
            *converter = std::make_unique<NumberToStringConvert<tparquet::Type::FLOAT>>();

        } else if (parquet_physical_type == tparquet::Type::DOUBLE) {
            *converter = std::make_unique<NumberToStringConvert<tparquet::Type::DOUBLE>>();
        }

        break;
    }
    case TypeIndex::DateV2:
        if (tparquet::Type::INT32 == parquet_physical_type) {
            *converter = std::make_unique<Int32ToDate>();
        }
        break;
    case TypeIndex::DateTimeV2:
        if (tparquet::Type::INT96 == parquet_physical_type) {
            *converter = std::make_unique<Int96toTimestamp>();
        } else if (tparquet::Type::INT64 == parquet_physical_type) {
            *converter = std::make_unique<Int64ToTimestamp>();
        }
        break;
#define DISPATCH2(TypeIndex_DECIMAL_TYPE, DECIMAL_TYPE, PRIMARY_TYPE)                             \
    case TypeIndex_DECIMAL_TYPE: {                                                                \
        convert_params->init_decimal_converter<PRIMARY_TYPE>(dst_data_type);                      \
        DecimalScaleParams& scale_params = convert_params->decimal_scale;                         \
        if (tparquet::Type::FIXED_LEN_BYTE_ARRAY == parquet_physical_type) {                      \
            size_t string_length = convert_params->field_schema->parquet_schema.type_length;      \
            if (string_length <= 8) {                                                             \
                if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {                    \
                    *converter =                                                                  \
                            std::make_unique<StringToDecimal<DECIMAL_TYPE, int64_t,               \
                                                             DecimalScaleParams::SCALE_UP>>();    \
                } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {           \
                    *converter =                                                                  \
                            std::make_unique<StringToDecimal<DECIMAL_TYPE, int64_t,               \
                                                             DecimalScaleParams::SCALE_DOWN>>();  \
                } else {                                                                          \
                    *converter =                                                                  \
                            std::make_unique<StringToDecimal<DECIMAL_TYPE, int64_t,               \
                                                             DecimalScaleParams::NO_SCALE>>();    \
                }                                                                                 \
            } else if (string_length <= 16) {                                                     \
                if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {                    \
                    *converter =                                                                  \
                            std::make_unique<StringToDecimal<DECIMAL_TYPE, int128_t,              \
                                                             DecimalScaleParams::SCALE_UP>>();    \
                } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {           \
                    *converter =                                                                  \
                            std::make_unique<StringToDecimal<DECIMAL_TYPE, int128_t,              \
                                                             DecimalScaleParams::SCALE_DOWN>>();  \
                } else {                                                                          \
                    *converter =                                                                  \
                            std::make_unique<StringToDecimal<DECIMAL_TYPE, int128_t,              \
                                                             DecimalScaleParams::NO_SCALE>>();    \
                }                                                                                 \
            }                                                                                     \
        } else if (tparquet::Type::INT32 == parquet_physical_type) {                              \
            if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {                        \
                *converter = std::make_unique<NumberToDecimal<Int32, PRIMARY_TYPE, int64_t,       \
                                                              DecimalScaleParams::SCALE_UP>>();   \
            } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {               \
                *converter = std::make_unique<NumberToDecimal<Int32, PRIMARY_TYPE, int64_t,       \
                                                              DecimalScaleParams::SCALE_DOWN>>(); \
            } else {                                                                              \
                *converter = std::make_unique<NumberToDecimal<Int32, PRIMARY_TYPE, int64_t,       \
                                                              DecimalScaleParams::NO_SCALE>>();   \
            }                                                                                     \
        } else if (tparquet::Type::INT64 == parquet_physical_type) {                              \
            if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {                        \
                *converter = std::make_unique<NumberToDecimal<Int64, PRIMARY_TYPE, int64_t,       \
                                                              DecimalScaleParams::SCALE_UP>>();   \
            } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {               \
                *converter = std::make_unique<NumberToDecimal<Int64, PRIMARY_TYPE, int64_t,       \
                                                              DecimalScaleParams::SCALE_DOWN>>(); \
            } else {                                                                              \
                *converter = std::make_unique<NumberToDecimal<Int64, PRIMARY_TYPE, int64_t,       \
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
        return Status::NotSupported("Can't cast type parquet physical {} to doris logical type {}",
                                    tparquet::to_string(parquet_physical_type),
                                    getTypeName(dst_type));
    }
    (*converter)->_convert_params = convert_params;
    return Status::OK();
}

}; // namespace ParquetConvert

}; // namespace doris::vectorized