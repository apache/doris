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

#include <absl/strings/numbers.h>
#include <cctz/time_zone.h>

#include <cstdint>
#include <utility>

#include "util/to_string.h"
#include "vec/columns/column_string.h"
#include "vec/common/arithmetic_overflow.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/io/io_helper.h"

namespace doris::vectorized::converter {
#include "common/compile_check_begin.h"

enum FileFormat { COMMON, ORC, PARQUET };

template <PrimitiveType type>
constexpr bool is_decimal_type() {
    return type == TYPE_DECIMALV2 || type == TYPE_DECIMAL32 || type == TYPE_DECIMAL64 ||
           type == TYPE_DECIMAL128I || type == TYPE_DECIMAL256;
}

template <PrimitiveType type>
constexpr bool is_integer_type() {
    return type == TYPE_INT || type == TYPE_TINYINT || type == TYPE_SMALLINT ||
           type == TYPE_BIGINT || type == TYPE_LARGEINT;
}

template <PrimitiveType type>
constexpr bool is_real_type() {
    return type == TYPE_FLOAT || type == TYPE_DOUBLE;
}

template <PrimitiveType type>
constexpr bool is_numeric_type() {
    return is_integer_type<type>() || is_real_type<type>();
}

/**
 * Unified schema change interface for all format readers:
 *
 * First, read the data according to the column type of the file into source column
 * Second, convert source column to the destination column with type planned by FE
 */
class ColumnTypeConverter {
protected:
    // The cached column to read data according to the column type of the file
    // Then, it will be converted to destination column, so this column can be reuse in next loop
    ColumnPtr _cached_src_column = nullptr;
    // The column type generated from file meta(eg. parquet footer)
    DataTypePtr _cached_src_type = nullptr;
    // Error message to show unsupported converter if support() return false;
    std::string _error_msg;

public:
    /**
     * Get the converter to change column type
     * @param src_type colum type from file meta data
     * @param dst_type column type from FE planner(the changed column type)
     */
    static std::unique_ptr<ColumnTypeConverter> get_converter(const DataTypePtr& src_type,
                                                              const DataTypePtr& dst_type,
                                                              FileFormat file_format);

    ColumnTypeConverter() = default;
    virtual ~ColumnTypeConverter() = default;

    /**
     * Converter source column to destination column. If the converter is not consistent,
     * the source column is `_cached_src_column`, otherwise, `src_col` and `dst_col` are the
     * same column, and with nothing to do.
     */
    virtual Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) { return Status::OK(); }

    virtual bool support() { return true; }

    virtual bool is_consistent() { return false; }

    /**
     * Get the column to read data from file with the type from file meta data.
     * If the converter is not consistent, the returned column is `_cached_src_column`.
     * For performance reasons, the null map of `_cached_src_column` is a reference from
     * the null map of `dst_column`, so there is no need to convert null map in `convert()`.
     *
     * According to the hive standard, if certain values fail to be converted(eg. string `row1` to int value),
     * these values are replaced by nulls.
     */
    ColumnPtr get_column(const DataTypePtr& src_type, ColumnPtr& dst_column,
                         const DataTypePtr& dst_type);

    /**
     * Get the column type from file meta data.
     */
    const DataTypePtr& get_type() { return _cached_src_type; }

    std::string get_error_msg() { return _error_msg; };
};

/**
 * No type conversion occurred, or compatible type conversion
 *
 * Compatible type conversion:
 * conversion within string, char and varchar
 * conversion from decimal(p1, s1) to decimal(p2, s2), because the scale change of decimal type is resolved in decode process
 */
class ConsistentConverter : public ColumnTypeConverter {
    bool is_consistent() override { return true; }
};

/**
 * Unsupported type change, eg. from int to date
 */
class UnsupportedConverter : public ColumnTypeConverter {
public:
    UnsupportedConverter(const DataTypePtr& src_type, const DataTypePtr& dst_type) {
        std::string src_type_str = src_type->get_name();
        std::string dst_type_str = dst_type->get_name();
        _error_msg = src_type_str + " => " + dst_type_str;
    }

    bool support() override { return false; }

    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        return Status::InternalError("Unsupported type change: {}", _error_msg);
    }
};

template <PrimitiveType SrcPrimitiveType, PrimitiveType DstPrimitiveType>
    requires(is_integer_type<SrcPrimitiveType>() && is_integer_type<DstPrimitiveType>())
class IntegerToIntegerConverter : public ColumnTypeConverter {
public:
    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        using SrcColumnType = typename PrimitiveTypeTraits<SrcPrimitiveType>::ColumnType;
        using SrcCppType = typename PrimitiveTypeTraits<SrcPrimitiveType>::CppType;
        using DstColumnType = typename PrimitiveTypeTraits<DstPrimitiveType>::ColumnType;
        using DstCppType = typename PrimitiveTypeTraits<DstPrimitiveType>::CppType;
        ColumnPtr from_col = remove_nullable(src_col);
        MutableColumnPtr to_col = remove_nullable(dst_col->get_ptr())->assume_mutable();

        size_t rows = from_col->size();
        auto& src_data = static_cast<const SrcColumnType*>(from_col.get())->get_data();
        size_t start_idx = to_col->size();
        to_col->resize(start_idx + rows);
        auto& data = static_cast<DstColumnType&>(*to_col.get()).get_data();
        for (int i = 0; i < rows; ++i) {
            if constexpr (sizeof(DstCppType) < sizeof(SrcCppType)) {
                SrcCppType src_value = src_data[i];
                if ((SrcCppType)std::numeric_limits<DstCppType>::min() > src_value ||
                    src_value > (SrcCppType)std::numeric_limits<DstCppType>::max()) {
                    return Status::InternalError("Failed to cast value '{}' to {} column",
                                                 src_value, dst_col->get_name());
                }
            }

            data[start_idx + i] = static_cast<DstCppType>(src_data[i]);
        }
        return Status::OK();
    }
};

template <PrimitiveType SrcPrimitiveType, PrimitiveType DstPrimitiveType>
    requires(is_numeric_type<SrcPrimitiveType>() && is_real_type<DstPrimitiveType>())
class NumericToFloatPointConverter : public ColumnTypeConverter {
    static constexpr long MIN_EXACT_DOUBLE = -(1L << 52);    // -2^52
    static constexpr long MAX_EXACT_DOUBLE = (1L << 52) - 1; // 2^52 - 1
    static constexpr long MIN_EXACT_FLOAT = -(1L << 23);     // -2^23
    static constexpr long MAX_EXACT_FLOAT = (1L << 23) - 1;  // 2^23 - 1

    bool overflow(typename PrimitiveTypeTraits<SrcPrimitiveType>::CppType value) const {
        if constexpr (DstPrimitiveType == TYPE_DOUBLE) {
            return value < MIN_EXACT_DOUBLE || value > MAX_EXACT_DOUBLE;
        } else if constexpr (DstPrimitiveType == TYPE_FLOAT) {
            return value < MIN_EXACT_FLOAT || value > MAX_EXACT_FLOAT;
        }
        return true; // Default case, should not occur
    }

public:
    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        using SrcColumnType = typename PrimitiveTypeTraits<SrcPrimitiveType>::ColumnType;
        using SrcCppType = typename PrimitiveTypeTraits<SrcPrimitiveType>::CppType;
        using DstColumnType = typename PrimitiveTypeTraits<DstPrimitiveType>::ColumnType;
        using DstCppType = typename PrimitiveTypeTraits<DstPrimitiveType>::CppType;
        ColumnPtr from_col = remove_nullable(src_col);
        MutableColumnPtr to_col = remove_nullable(dst_col->get_ptr())->assume_mutable();

        NullMap* null_map = nullptr;
        if (dst_col->is_nullable()) {
            null_map =
                    &static_cast<vectorized::ColumnNullable*>(dst_col.get())->get_null_map_data();
        }

        size_t rows = from_col->size();
        auto& src_data = static_cast<const SrcColumnType*>(from_col.get())->get_data();
        size_t start_idx = to_col->size();
        to_col->resize(start_idx + rows);
        auto& data = static_cast<DstColumnType&>(*to_col.get()).get_data();
        for (int i = 0; i < rows; ++i) {
            SrcCppType src_value = src_data[i];
            if constexpr (is_integer_type<SrcPrimitiveType>()) {
                if (overflow(src_value)) {
                    if (null_map == nullptr) {
                        return Status::InternalError("Failed to cast value '{}' to {} column",
                                                     src_value, dst_col->get_name());
                    } else {
                        (*null_map)[start_idx + i] = 1;
                    }
                }
            }

            data[start_idx + i] = static_cast<DstCppType>(src_value);
        }
        return Status::OK();
    }
};

class BooleanToStringConverter : public ColumnTypeConverter {
public:
    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        using SrcColumnType = typename PrimitiveTypeTraits<TYPE_BOOLEAN>::ColumnType;
        ColumnPtr from_col = remove_nullable(src_col);
        MutableColumnPtr to_col = remove_nullable(dst_col->get_ptr())->assume_mutable();

        size_t rows = from_col->size();
        auto& src_data = static_cast<const SrcColumnType*>(from_col.get())->get_data();
        auto& string_col = static_cast<ColumnString&>(*to_col.get());
        for (int i = 0; i < rows; ++i) {
            std::string value = src_data[i] != 0 ? "TRUE" : "FALSE";
            string_col.insert_data(value.data(), value.size());
        }
        return Status::OK();
    }
};

template <PrimitiveType SrcPrimitiveType, FileFormat fileFormat = COMMON>
    requires(is_numeric_type<SrcPrimitiveType>())
class NumericToStringConverter : public ColumnTypeConverter {
private:
public:
    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        using SrcColumnType = typename PrimitiveTypeTraits<SrcPrimitiveType>::ColumnType;
        ColumnPtr from_col = remove_nullable(src_col);
        MutableColumnPtr to_col = remove_nullable(dst_col->get_ptr())->assume_mutable();

        NullMap* null_map = nullptr;
        if (dst_col->is_nullable()) {
            null_map = &reinterpret_cast<vectorized::ColumnNullable*>(dst_col.get())
                                ->get_null_map_data();
        }

        size_t rows = from_col->size();
        size_t start_idx = to_col->size();
        auto& src_data = static_cast<const SrcColumnType*>(from_col.get())->get_data();
        auto& string_col = static_cast<ColumnString&>(*to_col.get());
        for (int i = 0; i < rows; ++i) {
            if constexpr (SrcPrimitiveType == TYPE_FLOAT || SrcPrimitiveType == TYPE_DOUBLE) {
                if (fileFormat == FileFormat::ORC && std::isnan(src_data[i])) {
                    if (null_map == nullptr) {
                        return Status::InternalError("Failed to cast value '{}' to {} column",
                                                     src_data[i], dst_col->get_name());
                    } else {
                        (*null_map)[start_idx + i] = 1;
                    }
                }
                char buf[128];
                int strlen;
                strlen = fast_to_buffer(src_data[i], buf);
                string_col.insert_data(buf, strlen);
            } else {
                std::string value;
                if constexpr (SrcPrimitiveType == TYPE_LARGEINT) {
                    value = int128_to_string(src_data[i]);
                } else {
                    value = std::to_string(src_data[i]);
                }
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

template <PrimitiveType SrcPrimitiveType, bool boundLength = false>
class TimeToStringConverter : public ColumnTypeConverter {
public:
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
            int len = (reinterpret_cast<const SrcCppType&>(src_data[i])).to_buffer(buf);
            string_col.insert_data(buf, len);
        }

        return Status::OK();
    }
};

template <PrimitiveType DstPrimitiveType, FileFormat = COMMON>
struct SafeCastString {};

template <>
struct SafeCastString<TYPE_BOOLEAN> {
    // Ref: https://github.com/apache/hive/blob/4df4d75bf1e16fe0af75aad0b4179c34c07fc975/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/primitive/PrimitiveObjectInspectorUtils.java#L559
    static const std::set<std::string> FALSE_VALUES;
    static bool safe_cast_string(const char* startptr, size_t buffer_size,
                                 PrimitiveTypeTraits<TYPE_BOOLEAN>::ColumnType::value_type* value) {
        std::string str_value(startptr, buffer_size);
        std::transform(str_value.begin(), str_value.end(), str_value.begin(), ::tolower);
        bool is_false = (FALSE_VALUES.find(str_value) != FALSE_VALUES.end());
        *value = is_false ? 0 : 1;
        return true;
    }
};

//Apache Hive reads 0 as false, numeric string as true and non-numeric string as null for ORC file format
// https://github.com/apache/orc/blob/fb1c4cb9461d207db652fc253396e57640ed805b/java/core/src/java/org/apache/orc/impl/ConvertTreeReaderFactory.java#L567
template <>
struct SafeCastString<TYPE_BOOLEAN, ORC> {
    static bool safe_cast_string(const char* startptr, size_t buffer_size,
                                 PrimitiveTypeTraits<TYPE_BOOLEAN>::ColumnType::value_type* value) {
        std::string str_value(startptr, buffer_size);
        int64_t cast_to_long = 0;
        bool can_cast = absl::SimpleAtoi({startptr, buffer_size}, &cast_to_long);
        *value = cast_to_long == 0 ? 0 : 1;
        return can_cast;
    }
};

template <>
struct SafeCastString<TYPE_TINYINT> {
    static bool safe_cast_string(const char* startptr, size_t buffer_size,
                                 PrimitiveTypeTraits<TYPE_TINYINT>::ColumnType::value_type* value) {
        int32_t cast_to_int = 0;
        bool can_cast = absl::SimpleAtoi({startptr, buffer_size}, &cast_to_int);
        if (can_cast && cast_to_int <= std::numeric_limits<int8_t>::max() &&
            cast_to_int >= std::numeric_limits<int8_t>::min()) {
            *value = static_cast<int8_t>(cast_to_int);
            return true;
        } else {
            return false;
        }
    }
};

template <>
struct SafeCastString<TYPE_SMALLINT> {
    static bool safe_cast_string(
            const char* startptr, size_t buffer_size,
            PrimitiveTypeTraits<TYPE_SMALLINT>::ColumnType::value_type* value) {
        int32_t cast_to_int = 0;
        bool can_cast = absl::SimpleAtoi({startptr, buffer_size}, &cast_to_int);
        if (can_cast && cast_to_int <= std::numeric_limits<int16_t>::max() &&
            cast_to_int >= std::numeric_limits<int16_t>::min()) {
            *value = static_cast<int16_t>(cast_to_int);
            return true;
        } else {
            return false;
        }
    }
};

template <>
struct SafeCastString<TYPE_INT> {
    static bool safe_cast_string(const char* startptr, size_t buffer_size,
                                 PrimitiveTypeTraits<TYPE_INT>::ColumnType::value_type* value) {
        int32_t cast_to_int = 0;
        bool can_cast = absl::SimpleAtoi({startptr, buffer_size}, &cast_to_int);
        *value = cast_to_int;
        return can_cast;
    }
};

template <>
struct SafeCastString<TYPE_BIGINT> {
    static bool safe_cast_string(const char* startptr, size_t buffer_size,
                                 PrimitiveTypeTraits<TYPE_BIGINT>::ColumnType::value_type* value) {
        int64_t cast_to_int = 0;
        bool can_cast = absl::SimpleAtoi({startptr, buffer_size}, &cast_to_int);
        *value = cast_to_int;
        return can_cast;
    }
};

template <>
struct SafeCastString<TYPE_LARGEINT> {
    static bool safe_cast_string(
            const char* startptr, size_t buffer_size,
            PrimitiveTypeTraits<TYPE_LARGEINT>::ColumnType::value_type* value) {
        StringRef str_ref(reinterpret_cast<const unsigned char*>(startptr), buffer_size);
        return try_read_int_text<Int128>(*value, str_ref);
    }
};

template <FileFormat fileFormat>
struct SafeCastString<TYPE_FLOAT, fileFormat> {
    static bool safe_cast_string(const char* startptr, size_t buffer_size,
                                 PrimitiveTypeTraits<TYPE_FLOAT>::ColumnType::value_type* value) {
        float cast_to_float = 0;
        bool can_cast = absl::SimpleAtof({startptr, buffer_size}, &cast_to_float);
        if (can_cast && fileFormat == ORC) {
            // Apache Hive reads Float.NaN as null when coerced to varchar for ORC file format.
            if (std::isnan(cast_to_float)) {
                return false;
            }
        }
        *value = cast_to_float;
        return can_cast;
    }
};

template <FileFormat fileFormat>
struct SafeCastString<TYPE_DOUBLE, fileFormat> {
    static bool safe_cast_string(const char* startptr, size_t buffer_size,
                                 PrimitiveTypeTraits<TYPE_DOUBLE>::ColumnType::value_type* value) {
        double cast_to_double = 0;
        bool can_cast = absl::SimpleAtod({startptr, buffer_size}, &cast_to_double);
        if (can_cast && fileFormat == ORC) {
            if (std::isnan(cast_to_double)) {
                return false;
            }
        }
        *value = cast_to_double;
        return can_cast;
    }
};

template <>
struct SafeCastString<TYPE_DATETIME> {
    static bool safe_cast_string(
            const char* startptr, size_t buffer_size,
            PrimitiveTypeTraits<TYPE_DATETIME>::ColumnType::value_type* value) {
        StringRef buffer(reinterpret_cast<const unsigned char*>(startptr), buffer_size);
        return read_datetime_text_impl<Int64>(*value, buffer);
    }
};

template <>
struct SafeCastString<TYPE_DATETIMEV2> {
    static bool safe_cast_string(
            const char* startptr, size_t buffer_size,
            PrimitiveTypeTraits<TYPE_DATETIMEV2>::ColumnType::value_type* value, int scale) {
        StringRef buffer(reinterpret_cast<const unsigned char*>(startptr), buffer_size);
        return read_datetime_v2_text_impl<UInt64>(*value, buffer, scale);
    }
};

template <>
struct SafeCastString<TYPE_DATE> {
    static bool safe_cast_string(const char* startptr, size_t buffer_size,
                                 PrimitiveTypeTraits<TYPE_DATE>::ColumnType::value_type* value) {
        StringRef buffer(reinterpret_cast<const unsigned char*>(startptr), buffer_size);
        return read_date_text_impl<Int64>(*value, buffer);
    }
};

template <>
struct SafeCastString<TYPE_DATEV2> {
    static bool safe_cast_string(const char* startptr, size_t buffer_size,
                                 PrimitiveTypeTraits<TYPE_DATEV2>::ColumnType::value_type* value) {
        StringRef buffer(reinterpret_cast<const unsigned char*>(startptr), buffer_size);
        return read_date_v2_text_impl<UInt32>(*value, buffer);
    }
};

template <PrimitiveType DstPrimitiveType>
struct SafeCastDecimalString {
    using CppType = typename PrimitiveTypeTraits<DstPrimitiveType>::ColumnType::value_type;

    static bool safe_cast_string(const char* startptr, size_t buffer_size, CppType* value,
                                 int precision, int scale) {
        StringRef str_ref(reinterpret_cast<const unsigned char*>(startptr), buffer_size);
        return read_decimal_text_impl<DstPrimitiveType, CppType>(
                       *value, str_ref, precision, scale) == StringParser::PARSE_SUCCESS;
    }
};

template <PrimitiveType DstPrimitiveType, FileFormat fileFormat>
class CastStringConverter : public ColumnTypeConverter {
private:
    DataTypePtr _dst_type_desc;

public:
    CastStringConverter() = default;
    CastStringConverter(DataTypePtr dst_type_desc) : _dst_type_desc(std::move(dst_type_desc)) {}

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
        auto& string_col =
                *assert_cast<const ColumnString*>(from_col.get())->assume_mutable().get();
        size_t start_idx = to_col->size();
        to_col->resize(start_idx + rows);
        auto& data = static_cast<DstColumnType*>(to_col.get())->get_data();
        for (int i = 0; i < rows; ++i) {
            DstCppType& value = data[start_idx + i];
            auto string_value = string_col.get_data_at(i);
            bool can_cast = false;
            if constexpr (is_decimal_type<DstPrimitiveType>()) {
                can_cast = SafeCastDecimalString<DstPrimitiveType>::safe_cast_string(
                        string_value.data, string_value.size, &value,
                        _dst_type_desc->get_precision(), _dst_type_desc->get_scale());
            } else if constexpr (DstPrimitiveType == TYPE_DATETIMEV2) {
                can_cast = SafeCastString<TYPE_DATETIMEV2>::safe_cast_string(
                        string_value.data, string_value.size, &value, _dst_type_desc->get_scale());
            } else if constexpr (DstPrimitiveType == TYPE_BOOLEAN && fileFormat == ORC) {
                can_cast = SafeCastString<TYPE_BOOLEAN, ORC>::safe_cast_string(
                        string_value.data, string_value.size, &value);
            } else {
                can_cast = SafeCastString<DstPrimitiveType>::safe_cast_string(
                        string_value.data, string_value.size, &value);
            }

            if (!can_cast) {
                if (null_map == nullptr) {
                    return Status::InternalError("Failed to cast string '{}' to not null column",
                                                 string_value.to_string());
                } else {
                    (*null_map)[start_idx + i] = 1;
                }
            }
        }

        return Status::OK();
    }
};

template <PrimitiveType DstPrimitiveType>
class DateTimeToNumericConverter : public ColumnTypeConverter {
public:
    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        using SrcColumnType = typename PrimitiveTypeTraits<TYPE_DATETIMEV2>::ColumnType;
        using DstColumnType = typename PrimitiveTypeTraits<DstPrimitiveType>::ColumnType;
        using SrcCppType = typename PrimitiveTypeTraits<TYPE_DATETIMEV2>::CppType;
        using DstCppType = typename PrimitiveTypeTraits<DstPrimitiveType>::CppType;

        ColumnPtr from_col = remove_nullable(src_col);
        MutableColumnPtr to_col = remove_nullable(dst_col->get_ptr())->assume_mutable();

        NullMap* null_map = nullptr;
        if (dst_col->is_nullable()) {
            null_map = &reinterpret_cast<vectorized::ColumnNullable*>(dst_col.get())
                                ->get_null_map_data();
        }

        size_t rows = from_col->size();
        auto& src_data = static_cast<const SrcColumnType*>(from_col.get())->get_data();
        size_t start_idx = to_col->size();
        to_col->resize(start_idx + rows);
        auto& data = static_cast<DstColumnType&>(*to_col.get()).get_data();

        for (int i = 0; i < rows; ++i) {
            const SrcCppType& src_value = src_data[i];
            auto& dst_value = reinterpret_cast<DstCppType&>(data[start_idx + i]);

            int64_t ts_s = 0;
            if (!src_value.unix_timestamp(&ts_s, cctz::utc_time_zone())) {
                if (null_map == nullptr) {
                    return Status::InternalError("Failed to cast value '{}' to {} column",
                                                 src_data[i], dst_col->get_name());
                } else {
                    (*null_map)[start_idx + i] = 1;
                }
            }
            auto micro = src_value.microsecond();
            int64_t ts_ms = ts_s * 1000 + micro / 1000;
            if constexpr (DstPrimitiveType != TYPE_LARGEINT && DstPrimitiveType != TYPE_BIGINT) {
                if ((Int64)std::numeric_limits<DstCppType>::min() > ts_ms ||
                    ts_ms > (Int64)std::numeric_limits<DstCppType>::max()) {
                    if (null_map == nullptr) {
                        return Status::InternalError("Failed to cast value '{}' to {} column",
                                                     src_data[i], dst_col->get_name());
                    } else {
                        (*null_map)[start_idx + i] = 1;
                    }
                }
            }
            dst_value = static_cast<DstCppType>(ts_ms);
        }
        return Status::OK();
    }
};

// only support date & datetime v2
template <PrimitiveType SrcPrimitiveType, PrimitiveType DstPrimitiveType>
class TimeV2Converter : public ColumnTypeConverter {
public:
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
            dst_value.unchecked_set_time(src_value.year(), src_value.month(), src_value.day(),
                                         src_value.hour(), src_value.minute(), src_value.second(),
                                         src_value.microsecond());
        }

        return Status::OK();
    }
};

template <PrimitiveType SrcPrimitiveType, PrimitiveType DstPrimitiveType>
    requires(is_numeric_type<SrcPrimitiveType>() && is_decimal_type<DstPrimitiveType>())
class NumericToDecimalConverter : public ColumnTypeConverter {
private:
    int _precision;
    int _scale;

public:
    NumericToDecimalConverter(int precision, int scale) : _precision(precision), _scale(scale) {}

    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        using SrcColumnType = typename PrimitiveTypeTraits<SrcPrimitiveType>::ColumnType;
        using SrcCppType = typename PrimitiveTypeTraits<SrcPrimitiveType>::CppType;
        using DstColumnType = typename PrimitiveTypeTraits<DstPrimitiveType>::ColumnType;
        using DstNativeType =
                typename PrimitiveTypeTraits<DstPrimitiveType>::ColumnType::value_type::NativeType;
        using DstDorisType = typename PrimitiveTypeTraits<DstPrimitiveType>::ColumnType::value_type;

        ColumnPtr from_col = remove_nullable(src_col);
        MutableColumnPtr to_col = remove_nullable(dst_col->get_ptr())->assume_mutable();

        NullMap* null_map = nullptr;
        if (dst_col->is_nullable()) {
            null_map = &reinterpret_cast<vectorized::ColumnNullable*>(dst_col.get())
                                ->get_null_map_data();
        }

        size_t rows = from_col->size();
        auto& src_data = static_cast<const SrcColumnType*>(from_col.get())->get_data();
        size_t start_idx = to_col->size();
        to_col->resize(start_idx + rows);
        auto& data = static_cast<DstColumnType&>(*to_col.get()).get_data();

        auto max_result = DataTypeDecimal<DstPrimitiveType>::get_max_digits_number(_precision);
        auto multiplier = DataTypeDecimal<DstPrimitiveType>::get_scale_multiplier(_scale);

        for (int i = 0; i < rows; ++i) {
            const SrcCppType& src_value = src_data[i];
            DstDorisType& res = data[start_idx + i];

            if constexpr (is_integer_type<SrcPrimitiveType>()) {
                if constexpr (sizeof(DstNativeType) < sizeof(SrcCppType)) {
                    if (src_value > std::numeric_limits<DstNativeType>::max() ||
                        src_value < std::numeric_limits<DstNativeType>::min()) {
                        if (null_map == nullptr) {
                            return Status::InternalError("Failed to cast value '{}' to {} column",
                                                         src_data[i], dst_col->get_name());
                        } else {
                            (*null_map)[start_idx + i] = 1;
                        }
                    }
                }
                if (common::mul_overflow(static_cast<DstNativeType>(src_value), multiplier,
                                         res.value)) {
                    if (null_map == nullptr) {
                        return Status::InternalError("Failed to cast value '{}' to {} column",
                                                     src_data[i], dst_col->get_name());
                    } else {
                        (*null_map)[start_idx + i] = 1;
                    }
                } else {
                    if (res.value > max_result || res.value < -max_result) {
                        if (null_map == nullptr) {
                            return Status::InternalError("Failed to cast value '{}' to {} column",
                                                         src_data[i], dst_col->get_name());
                        } else {
                            (*null_map)[start_idx + i] = 1;
                        }
                    }
                }
            } else {
                SrcCppType dst_value = src_value * static_cast<SrcCppType>(multiplier);
                res = static_cast<DstDorisType>(dst_value);
                if (UNLIKELY(!std::isfinite(src_value) ||
                             dst_value > static_cast<SrcCppType>(max_result) ||
                             dst_value < static_cast<SrcCppType>(-max_result))) {
                    if (null_map == nullptr) {
                        return Status::InternalError("Failed to cast value '{}' to {} column",
                                                     src_data[i], dst_col->get_name());
                    } else {
                        (*null_map)[start_idx + i] = 1;
                    }
                }
            }
        }
        return Status::OK();
    }
};

template <PrimitiveType SrcPrimitiveType, PrimitiveType DstPrimitiveType>
    requires(is_numeric_type<DstPrimitiveType>() && is_decimal_type<SrcPrimitiveType>())
class DecimalToNumericConverter : public ColumnTypeConverter {
private:
    int _scale;

public:
    DecimalToNumericConverter(int scale) : _scale(scale) {}

    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        using SrcColumnType = typename PrimitiveTypeTraits<SrcPrimitiveType>::ColumnType;
        using SrcNativeType =
                typename PrimitiveTypeTraits<SrcPrimitiveType>::ColumnType::value_type::NativeType;
        using DstColumnType = typename PrimitiveTypeTraits<DstPrimitiveType>::ColumnType;
        using DstCppType = typename PrimitiveTypeTraits<DstPrimitiveType>::CppType;

        ColumnPtr from_col = remove_nullable(src_col);
        MutableColumnPtr to_col = remove_nullable(dst_col->get_ptr())->assume_mutable();

        size_t rows = from_col->size();
        auto& src_data = static_cast<const SrcColumnType*>(from_col.get())->get_data();
        size_t start_idx = to_col->size();
        to_col->resize(start_idx + rows);
        auto& data = static_cast<DstColumnType&>(*to_col.get()).get_data();

        NullMap* null_map = nullptr;
        if (dst_col->is_nullable()) {
            null_map = &reinterpret_cast<vectorized::ColumnNullable*>(dst_col.get())
                                ->get_null_map_data();
        }

        SrcNativeType scale_factor;
        if constexpr (sizeof(SrcNativeType) <= sizeof(int)) {
            scale_factor = common::exp10_i32(_scale);
        } else if constexpr (sizeof(SrcNativeType) <= sizeof(int64_t)) {
            scale_factor = common::exp10_i64(_scale);
        } else if constexpr (sizeof(SrcNativeType) <= sizeof(__int128)) {
            scale_factor = common::exp10_i128(_scale);
        } else if constexpr (sizeof(SrcNativeType) <= sizeof(wide::Int256)) {
            scale_factor = common::exp10_i256(_scale);
        }

        for (int i = 0; i < rows; ++i) {
            if constexpr (DstPrimitiveType == TYPE_FLOAT || DstPrimitiveType == TYPE_DOUBLE) {
                data[start_idx + i] = static_cast<DstCppType>(
                        static_cast<double>(src_data[i].value) / (double)scale_factor);
            } else {
                SrcNativeType tmp_value = src_data[i].value / scale_factor;

                if constexpr (sizeof(SrcNativeType) > sizeof(DstCppType)) {
                    if ((SrcNativeType)std::numeric_limits<DstCppType>::min() > tmp_value ||
                        tmp_value > (SrcNativeType)std::numeric_limits<DstCppType>::max()) {
                        if (null_map == nullptr) {
                            return Status::InternalError("Failed to cast value '{}' to {} column",
                                                         src_data[i].to_string(_scale),
                                                         dst_col->get_name());
                        } else {
                            (*null_map)[start_idx + i] = 1;
                        }
                    }
                }

                data[start_idx + i] = static_cast<DstCppType>(tmp_value);
            }
        }

        return Status::OK();
    }
};

template <PrimitiveType SrcDecimalPrimitiveType, PrimitiveType DstDecimalPrimitiveType>
class DecimalToDecimalConverter : public ColumnTypeConverter {
private:
    int _from_precision;
    int _from_scale;
    int _to_precision;
    int _to_scale;

public:
    DecimalToDecimalConverter(int from_precision, int from_scale, int to_precision, int to_scale)
            : _from_precision(from_precision),
              _from_scale(from_scale),
              _to_precision(to_precision),
              _to_scale(to_scale) {}

    Status convert(ColumnPtr& src_col, MutableColumnPtr& dst_col) override {
        using SrcColumnType = typename PrimitiveTypeTraits<SrcDecimalPrimitiveType>::ColumnType;
        using DstColumnType = typename PrimitiveTypeTraits<DstDecimalPrimitiveType>::ColumnType;
        using SrcNativeType = typename PrimitiveTypeTraits<
                SrcDecimalPrimitiveType>::ColumnType::value_type::NativeType;
        using DstNativeType = typename PrimitiveTypeTraits<
                DstDecimalPrimitiveType>::ColumnType::value_type::NativeType;
        using MaxNativeType = std::conditional_t<(sizeof(SrcNativeType) > sizeof(DstNativeType)),
                                                 SrcNativeType, DstNativeType>;
        constexpr PrimitiveType MaxPrimitiveType = sizeof(SrcNativeType) > sizeof(DstNativeType)
                                                           ? SrcDecimalPrimitiveType
                                                           : DstDecimalPrimitiveType;

        auto max_result =
                DataTypeDecimal<DstDecimalPrimitiveType>::get_max_digits_number(_to_precision);
        bool narrow_integral = (_to_precision - _to_scale) < (_from_precision - _from_scale);

        ColumnPtr from_col = remove_nullable(src_col);
        MutableColumnPtr to_col = remove_nullable(dst_col->get_ptr())->assume_mutable();

        size_t rows = from_col->size();
        auto& src_data = static_cast<const SrcColumnType*>(from_col.get())->get_data();
        size_t start_idx = to_col->size();
        to_col->resize(start_idx + rows);
        auto& data = static_cast<DstColumnType&>(*to_col.get()).get_data();

        for (int i = 0; i < rows; ++i) {
            SrcNativeType src_value = src_data[i].value;
            DstNativeType& res_value = data[start_idx + i].value;

            if (_to_scale > _from_scale) {
                const MaxNativeType multiplier =
                        DataTypeDecimal<MaxPrimitiveType>::get_scale_multiplier(_to_scale -
                                                                                _from_scale);
                MaxNativeType res;
                if (common::mul_overflow<MaxNativeType>(src_value, multiplier, res)) {
                    return Status::InternalError("Failed to cast value '{}' to {} column",
                                                 src_data[i].to_string(_from_scale),
                                                 dst_col->get_name());
                } else {
                    if (res > max_result || res < -max_result) {
                        return Status::InternalError("Failed to cast value '{}' to {} column",
                                                     src_data[i].to_string(_from_scale),
                                                     dst_col->get_name());
                    } else {
                        res_value = static_cast<DstNativeType>(res);
                    }
                }
            } else if (_to_scale == _from_scale) {
                res_value = static_cast<DstNativeType>(src_value);
                if (narrow_integral && (src_value > max_result || src_value < -max_result)) {
                    return Status::InternalError("Failed to cast value '{}' to {} column",
                                                 src_data[i].to_string(_from_scale),
                                                 dst_col->get_name());
                }
            } else {
                MaxNativeType multiplier = DataTypeDecimal<MaxPrimitiveType>::get_scale_multiplier(
                        _from_scale - _to_scale);
                MaxNativeType res = src_value / multiplier;
                if (src_value % multiplier != 0 || res > max_result || res < -max_result) {
                    return Status::InternalError("Failed to cast value '{}' to {} column",
                                                 src_data[i].to_string(_from_scale),
                                                 dst_col->get_name());
                }
                res_value = static_cast<DstNativeType>(res);
            }
        }
        return Status::OK();
    }
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized::converter
