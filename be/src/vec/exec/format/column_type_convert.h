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

#pragma once
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
#include "vec/io/io_helper.h"

namespace doris::vectorized::converter {

template <PrimitiveType type>
constexpr bool is_decimal_type_const() {
    return type == TYPE_DECIMALV2 || type == TYPE_DECIMAL32 || type == TYPE_DECIMAL64 ||
           type == TYPE_DECIMAL128I || type == TYPE_DECIMAL256;
}

class ColumnTypeConverter {
protected:
    ColumnPtr _cached_src_column = nullptr;
    DataTypePtr _cached_src_type = nullptr;

public:
    static std::unique_ptr<ColumnTypeConverter> get_converter(const TypeDescriptor& src_type,
                                                              const DataTypePtr& dst_type);

    static bool is_numeric_type(PrimitiveType type);

    static bool is_decimal_type(PrimitiveType type);

    ColumnTypeConverter() = default;
    virtual ~ColumnTypeConverter() = default;

    virtual Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) { return Status::OK(); }

    virtual bool support() { return true; }

    ColumnPtr get_column(const TypeDescriptor& src_type, ColumnPtr& dst_column,
                         const DataTypePtr& dst_type);

    const DataTypePtr& get_type() { return _cached_src_type; }
};

class UnsupportedConverter : public ColumnTypeConverter {
    bool support() override { return false; }
};

template <PrimitiveType SrcPrimitiveType, PrimitiveType DstPrimitiveType>
class NumericToNumericConverter : public ColumnTypeConverter {
    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        using SrcColumnType = typename PrimitiveTypeTraits<SrcPrimitiveType>::ColumnType;
        using DstCppType = typename PrimitiveTypeTraits<DstPrimitiveType>::CppType;
        using DstColumnType = typename PrimitiveTypeTraits<DstPrimitiveType>::ColumnType;
        ColumnPtr from_col = remove_nullable(src_col);
        MutableColumnPtr to_col = remove_nullable(dst_col->get_ptr())->assume_mutable();

        size_t rows = from_col->size();
        auto& src_data = static_cast<const SrcColumnType*>(from_col.get())->get_data();
        size_t start_idx = to_col->size();
        to_col->resize(start_idx + rows);
        auto& data = static_cast<DstColumnType&>(*to_col.get()).get_data();
        for (int i = 0; i < rows; ++i) {
            data[start_idx + i] = static_cast<DstCppType>(src_data[i]);
        }

        return Status::OK();
    }
};

template <PrimitiveType SrcPrimitiveType>
class NumericToStringConverter : public ColumnTypeConverter {
    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        using SrcColumnType = typename PrimitiveTypeTraits<SrcPrimitiveType>::ColumnType;
        ColumnPtr from_col = remove_nullable(src_col);
        MutableColumnPtr to_col = remove_nullable(dst_col->get_ptr())->assume_mutable();

        size_t rows = from_col->size();
        auto& src_data = static_cast<const SrcColumnType*>(from_col.get())->get_data();
        auto& string_col = static_cast<ColumnString&>(*to_col.get());
        for (int i = 0; i < rows; ++i) {
            if constexpr (SrcPrimitiveType == TYPE_LARGEINT) {
                string value = int128_to_string(src_data[i]);
                string_col.insert_data(value.data(), value.size());
            } else {
                string value = std::to_string(src_data[i]);
                string_col.insert_data(value.data(), value.size());
            }
        }

        return Status::OK();
    }
};

template <PrimitiveType SrcPrimitiveType>
class DecimalToStringConverter : public ColumnTypeConverter {
private:
    int _scale;

public:
    DecimalToStringConverter(int scale) : _scale(scale) {}

    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        using SrcColumnType = typename PrimitiveTypeTraits<SrcPrimitiveType>::ColumnType;
        ColumnPtr from_col = remove_nullable(src_col);
        MutableColumnPtr to_col = remove_nullable(dst_col->get_ptr())->assume_mutable();

        size_t rows = from_col->size();
        auto& src_data = static_cast<const SrcColumnType*>(from_col.get())->get_data();
        auto& string_col = static_cast<ColumnString&>(*to_col.get());
        for (int i = 0; i < rows; ++i) {
            std::string value = src_data[i].to_string(_scale);
            string_col.insert_data(value.data(), value.size());
        }

        return Status::OK();
    }
};

template <PrimitiveType SrcPrimitiveType>
class TimeToStringConverter : public ColumnTypeConverter {
    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        using SrcCppType = typename PrimitiveTypeTraits<SrcPrimitiveType>::CppType;
        using SrcColumnType = typename PrimitiveTypeTraits<SrcPrimitiveType>::ColumnType;
        ColumnPtr from_col = remove_nullable(src_col);
        MutableColumnPtr to_col = remove_nullable(dst_col->get_ptr())->assume_mutable();

        size_t rows = from_col->size();
        auto& src_data = static_cast<const SrcColumnType*>(from_col.get())->get_data();
        auto& string_col = static_cast<ColumnString&>(*to_col.get());
        char buf[50];
        for (int i = 0; i < rows; ++i) {
            char* end = (reinterpret_cast<const SrcCppType&>(src_data[i])).to_string(buf);
            string_col.insert_data(buf, end - buf);
        }

        return Status::OK();
    }
};

template <PrimitiveType DstPrimitiveType>
class SafeCastString {
public:
    virtual bool safe_cast_string(
            const char* startptr, const int buffer_size,
            PrimitiveTypeTraits<DstPrimitiveType>::ColumnType::value_type* value);
};

template <>
class SafeCastString<TYPE_BOOLEAN> {
private:
    const char* VALID_TRUE_STR = "true";
    const char* VALID_FALSE_STR = "false";

public:
    // Cast string to bool
    // "ture" -> true, "false" -> false
    // or parse string as int value
    bool safe_cast_string(const char* startptr, const int buffer_size,
                          PrimitiveTypeTraits<TYPE_BOOLEAN>::ColumnType::value_type* value) {
        if (buffer_size == 4 && strncmp(startptr, VALID_TRUE_STR, buffer_size) == 0) {
            *value = 1;
            return true;
        }
        if (buffer_size == 5 && strncmp(startptr, VALID_FALSE_STR, buffer_size) == 0) {
            *value = 0;
            return true;
        }

        int32 cast_to_int = 0;
        bool can_cast = safe_strto32(startptr, buffer_size, &cast_to_int);
        *value = cast_to_int == 0 ? 0 : 1;
        return can_cast;
    }
};

template <>
class SafeCastString<TYPE_TINYINT> {
public:
    bool safe_cast_string(const char* startptr, const int buffer_size,
                          PrimitiveTypeTraits<TYPE_TINYINT>::ColumnType::value_type* value) {
        int32 cast_to_int = 0;
        bool can_cast = safe_strto32(startptr, buffer_size, &cast_to_int);
        *value = cast_to_int;
        return can_cast && cast_to_int <= std::numeric_limits<int8>::max() &&
               cast_to_int >= std::numeric_limits<int8>::min();
    }
};

template <>
class SafeCastString<TYPE_SMALLINT> {
public:
    bool safe_cast_string(const char* startptr, const int buffer_size,
                          PrimitiveTypeTraits<TYPE_SMALLINT>::ColumnType::value_type* value) {
        int32 cast_to_int = 0;
        bool can_cast = safe_strto32(startptr, buffer_size, &cast_to_int);
        *value = cast_to_int;
        return can_cast && cast_to_int <= std::numeric_limits<int16>::max() &&
               cast_to_int >= std::numeric_limits<int16>::min();
    }
};

template <>
class SafeCastString<TYPE_INT> {
public:
    bool safe_cast_string(const char* startptr, const int buffer_size,
                          PrimitiveTypeTraits<TYPE_INT>::ColumnType::value_type* value) {
        int32 cast_to_int = 0;
        bool can_cast = safe_strto32(startptr, buffer_size, &cast_to_int);
        *value = cast_to_int;
        return can_cast;
    }
};

template <>
class SafeCastString<TYPE_BIGINT> {
public:
    bool safe_cast_string(const char* startptr, const int buffer_size,
                          PrimitiveTypeTraits<TYPE_BIGINT>::ColumnType::value_type* value) {
        int64 cast_to_int = 0;
        bool can_cast = safe_strto64(startptr, buffer_size, &cast_to_int);
        *value = cast_to_int;
        return can_cast;
    }
};

template <>
class SafeCastString<TYPE_LARGEINT> {
public:
    bool safe_cast_string(const char* startptr, const int buffer_size,
                          PrimitiveTypeTraits<TYPE_LARGEINT>::ColumnType::value_type* value) {
        int64 cast_to_int = 0;
        bool can_cast = safe_strto64(startptr, buffer_size, &cast_to_int);
        *value = cast_to_int;
        return can_cast;
    }
};

template <>
class SafeCastString<TYPE_FLOAT> {
public:
    bool safe_cast_string(const char* startptr, const int buffer_size,
                          PrimitiveTypeTraits<TYPE_FLOAT>::ColumnType::value_type* value) {
        float cast_to_float = 0;
        bool can_cast = safe_strtof(std::string(startptr, buffer_size), &cast_to_float);
        *value = cast_to_float;
        return can_cast;
    }
};

template <>
class SafeCastString<TYPE_DOUBLE> {
public:
    bool safe_cast_string(const char* startptr, const int buffer_size,
                          PrimitiveTypeTraits<TYPE_DOUBLE>::ColumnType::value_type* value) {
        double cast_to_double = 0;
        bool can_cast = safe_strtod(std::string(startptr, buffer_size), &cast_to_double);
        *value = cast_to_double;
        return can_cast;
    }
};

template <>
class SafeCastString<TYPE_DATETIME> {
public:
    bool safe_cast_string(const char* startptr, const int buffer_size,
                          PrimitiveTypeTraits<TYPE_DATETIME>::ColumnType::value_type* value) {
        ReadBuffer buffer(reinterpret_cast<const unsigned char*>(startptr), buffer_size);
        return read_datetime_text_impl<Int64>(*value, buffer);
    }
};

template <>
class SafeCastString<TYPE_DATETIMEV2> {
public:
    bool safe_cast_string(const char* startptr, const int buffer_size,
                          PrimitiveTypeTraits<TYPE_DATETIMEV2>::ColumnType::value_type* value) {
        ReadBuffer buffer(reinterpret_cast<const unsigned char*>(startptr), buffer_size);
        return safe_cast_string(startptr, buffer_size, value, 6);
    }

    bool safe_cast_string(const char* startptr, const int buffer_size,
                          PrimitiveTypeTraits<TYPE_DATETIMEV2>::ColumnType::value_type* value,
                          int scale) {
        ReadBuffer buffer(reinterpret_cast<const unsigned char*>(startptr), buffer_size);
        return read_datetime_v2_text_impl<UInt64>(*value, buffer, scale);
    }
};

template <>
class SafeCastString<TYPE_DATE> {
public:
    bool safe_cast_string(const char* startptr, const int buffer_size,
                          PrimitiveTypeTraits<TYPE_DATE>::ColumnType::value_type* value) {
        ReadBuffer buffer(reinterpret_cast<const unsigned char*>(startptr), buffer_size);
        return read_date_text_impl<Int64>(*value, buffer);
    }
};

template <>
class SafeCastString<TYPE_DATEV2> {
public:
    bool safe_cast_string(const char* startptr, const int buffer_size,
                          PrimitiveTypeTraits<TYPE_DATEV2>::ColumnType::value_type* value) {
        ReadBuffer buffer(reinterpret_cast<const unsigned char*>(startptr), buffer_size);
        return read_date_v2_text_impl<UInt32>(*value, buffer);
    }
};

template <PrimitiveType DstPrimitiveType>
class SafeCastDecimalString {
public:
    using CppType = typename PrimitiveTypeTraits<DstPrimitiveType>::ColumnType::value_type;

    bool safe_cast_string(const char* startptr, const int buffer_size, CppType* value,
                          int precision, int scale) {
        ReadBuffer buffer(reinterpret_cast<const unsigned char*>(startptr), buffer_size);
        return read_decimal_text_impl<DstPrimitiveType, CppType>(
                       *value, buffer, precision, scale) == StringParser::PARSE_SUCCESS;
    }
};

template <PrimitiveType DstPrimitiveType>
class CastStringConverter : public ColumnTypeConverter {
private:
    DataTypePtr _dst_type_desc;

public:
    CastStringConverter() : _dst_type_desc(nullptr) {}
    CastStringConverter(DataTypePtr dst_type_desc) : _dst_type_desc(dst_type_desc) {}

    using DstCppType = typename PrimitiveTypeTraits<DstPrimitiveType>::ColumnType::value_type;
    using DstColumnType = typename PrimitiveTypeTraits<DstPrimitiveType>::ColumnType;

    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        ColumnPtr from_col = remove_nullable(src_col);
        NullMap* null_map = nullptr;
        if (dst_col->is_nullable()) {
            null_map = &reinterpret_cast<vectorized::ColumnNullable*>(dst_col.get())
                                ->get_null_map_data();
        }
        MutableColumnPtr to_col = remove_nullable(dst_col->get_ptr())->assume_mutable();

        size_t rows = from_col->size();
        auto& string_col = static_cast<ColumnString&>(*from_col->assume_mutable().get());
        size_t start_idx = to_col->size();
        to_col->resize(start_idx + rows);
        auto& data = static_cast<DstColumnType*>(to_col.get())->get_data();
        for (int i = 0; i < rows; ++i) {
            DstCppType value;
            auto string_value = string_col.get_data_at(i);
            bool can_cast = false;
            if constexpr (is_decimal_type_const<DstPrimitiveType>()) {
                SafeCastDecimalString<DstPrimitiveType> caster;
                can_cast = caster.safe_cast_string(string_value.data, string_value.size, &value,
                                                   _dst_type_desc->get_precision(),
                                                   _dst_type_desc->get_scale());
            } else if constexpr (DstPrimitiveType == TYPE_DATETIMEV2) {
                SafeCastString<TYPE_DATETIMEV2> caster;
                can_cast = caster.safe_cast_string(string_value.data, string_value.size, &value,
                                                   _dst_type_desc->get_scale());
            } else {
                SafeCastString<DstPrimitiveType> caster;
                can_cast = caster.safe_cast_string(string_value.data, string_value.size, &value);
            }
            if (can_cast) {
                data[start_idx + i] = value;
            } else {
                if (null_map == nullptr) {
                    return Status::InternalError("Failed to cast string '{}'",
                                                 string_value.to_string());
                } else {
                    (*null_map)[start_idx + i] = 1;
                }
            }
        }

        return Status::OK();
    }
};

// only support date & datetime v2
template <PrimitiveType SrcPrimitiveType, PrimitiveType DstPrimitiveType>
class V2DateConverter : public ColumnTypeConverter {
    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        using SrcColumnType = typename PrimitiveTypeTraits<SrcPrimitiveType>::ColumnType;
        using DstColumnType = typename PrimitiveTypeTraits<DstPrimitiveType>::ColumnType;
        using SrcCppType = typename PrimitiveTypeTraits<SrcPrimitiveType>::CppType;
        using DstCppType = typename PrimitiveTypeTraits<DstPrimitiveType>::CppType;

        ColumnPtr from_col = remove_nullable(src_col);
        MutableColumnPtr to_col = remove_nullable(dst_col->get_ptr())->assume_mutable();

        size_t rows = from_col->size();
        auto& src_data = static_cast<const SrcColumnType*>(from_col.get())->get_data();
        size_t start_idx = to_col->size();
        to_col->resize(start_idx + rows);
        auto& data = static_cast<DstColumnType&>(*to_col.get()).get_data();
        for (int i = 0; i < rows; ++i) {
            const auto& src_value = reinterpret_cast<const SrcCppType&>(src_data[i]);
            auto& dst_value = reinterpret_cast<DstCppType&>(data[start_idx + i]);
            dst_value.set_time(src_value.year(), src_value.month(), src_value.day(),
                               src_value.hour(), src_value.minute(), src_value.second(),
                               src_value.microsecond());
        }

        return Status::OK();
    }
};

template <PrimitiveType SrcPrimitiveType, PrimitiveType DstPrimitiveType>
class NumericToDecimalConverter : public ColumnTypeConverter {
private:
    int _scale;

public:
    NumericToDecimalConverter(int scale) : _scale(scale) {}

    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        using SrcColumnType = typename PrimitiveTypeTraits<SrcPrimitiveType>::ColumnType;
        using DstColumnType = typename PrimitiveTypeTraits<DstPrimitiveType>::ColumnType;
        using DstNativeType =
                typename PrimitiveTypeTraits<DstPrimitiveType>::ColumnType::value_type::NativeType;
        using DstCppType = typename PrimitiveTypeTraits<DstPrimitiveType>::ColumnType::value_type;

        ColumnPtr from_col = remove_nullable(src_col);
        MutableColumnPtr to_col = remove_nullable(dst_col->get_ptr())->assume_mutable();

        size_t rows = from_col->size();
        auto& src_data = static_cast<const SrcColumnType*>(from_col.get())->get_data();
        size_t start_idx = to_col->size();
        to_col->resize(start_idx + rows);
        auto& data = static_cast<DstColumnType&>(*to_col.get()).get_data();
        int64_t scale_factor = 1;
        if (_scale > DecimalV2Value::SCALE) {
            scale_factor = common::exp10_i64(_scale - DecimalV2Value::SCALE);
        } else if (_scale < DecimalV2Value::SCALE) {
            scale_factor = common::exp10_i64(DecimalV2Value::SCALE - _scale);
        }

        for (int i = 0; i < rows; ++i) {
            if constexpr (SrcPrimitiveType == TYPE_FLOAT || SrcPrimitiveType == TYPE_DOUBLE) {
                DecimalV2Value decimal_value;
                if constexpr (SrcPrimitiveType == TYPE_FLOAT) {
                    decimal_value.assign_from_float(src_data[i]);
                } else {
                    decimal_value.assign_from_double(src_data[i]);
                }
                int128_t decimal_int128 = reinterpret_cast<int128_t&>(decimal_value);
                if (_scale > DecimalV2Value::SCALE) {
                    decimal_int128 *= scale_factor;
                } else if (_scale < DecimalV2Value::SCALE) {
                    decimal_int128 /= scale_factor;
                }
                auto& v = reinterpret_cast<DstNativeType&>(data[start_idx + i]);
                v = (DstNativeType)decimal_int128;
            } else {
                data[start_idx + i] = DstCppType::from_int_frac(src_data[i], 0, _scale);
            }
        }

        return Status::OK();
    }
};

template <PrimitiveType SrcPrimitiveType, PrimitiveType DstPrimitiveType>
class DecimalToNumericConverter : public ColumnTypeConverter {
private:
    int _scale;

public:
    DecimalToNumericConverter(int scale) : _scale(scale) {}

    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        using SrcColumnType = typename PrimitiveTypeTraits<SrcPrimitiveType>::ColumnType;
        using DstColumnType = typename PrimitiveTypeTraits<DstPrimitiveType>::ColumnType;
        using DstCppType = typename PrimitiveTypeTraits<DstPrimitiveType>::CppType;

        ColumnPtr from_col = remove_nullable(src_col);
        MutableColumnPtr to_col = remove_nullable(dst_col->get_ptr())->assume_mutable();

        size_t rows = from_col->size();
        auto& src_data = static_cast<const SrcColumnType*>(from_col.get())->get_data();
        size_t start_idx = to_col->size();
        to_col->resize(start_idx + rows);
        auto& data = static_cast<DstColumnType&>(*to_col.get()).get_data();

        int64_t scale_factor = common::exp10_i64(_scale);
        for (int i = 0; i < rows; ++i) {
            if constexpr (DstPrimitiveType == TYPE_FLOAT || DstPrimitiveType == TYPE_DOUBLE) {
                data[start_idx + i] =
                        static_cast<DstCppType>(src_data[i].value / (double)scale_factor);
            } else {
                data[start_idx + i] = static_cast<DstCppType>(src_data[i].value / scale_factor);
            }
        }

        return Status::OK();
    }
};

} // namespace doris::vectorized::converter
