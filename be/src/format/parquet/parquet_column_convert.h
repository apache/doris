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

#include <cctz/time_zone.h>
#include <gen_cpp/parquet_types.h>
#include <libdivide.h>

#include <chrono>
#include <limits>

#include "common/cast_set.h"
#include "core/column/column_fixed_length_object.h"
#include "core/column/column_varbinary.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/primitive_type.h"
#include "core/extended_types.h"
#include "core/field.h"
#include "core/types.h"
#include "format/column_type_convert.h"
#include "format/format_common.h"
#include "format/parquet/decoder.h"
#include "format/parquet/parquet_common.h"
#include "format/parquet/schema_desc.h"
#include "util/timezone_utils.h"

namespace doris::parquet {
namespace detail {

inline bool try_split_local_time(int64_t local_time, uint16_t* year, uint8_t* month, uint8_t* day,
                                 uint8_t* hour, uint8_t* minute, uint8_t* second) {
    static const libdivide::divider<int64_t> fast_div_86400(86400);
    static const libdivide::divider<int64_t> fast_div_3600(3600);
    static const libdivide::divider<int64_t> fast_div_60(60);
    static constexpr int64_t kMinSupportedDays = -365LL * 10000;
    static constexpr int64_t kMaxSupportedDays = 365LL * 10000;

    int64_t days = local_time / fast_div_86400;
    int64_t second_of_day = local_time - days * 86400;
    if (second_of_day < 0) {
        second_of_day += 86400;
        --days;
    }
    if (days < kMinSupportedDays || days > kMaxSupportedDays) {
        return false;
    }

    const auto ymd = std::chrono::year_month_day {std::chrono::sys_days {std::chrono::days {days}}};
    const int y = static_cast<int>(ymd.year());
    if (y < 0 || y > std::numeric_limits<uint16_t>::max()) {
        return false;
    }

    const int64_t h = second_of_day / fast_div_3600;
    const int64_t minute_second = second_of_day - h * 3600;
    const int64_t m = minute_second / fast_div_60;
    const int64_t s = minute_second - m * 60;

    *year = static_cast<uint16_t>(y);
    *month = static_cast<uint8_t>(static_cast<unsigned>(ymd.month()));
    *day = static_cast<uint8_t>(static_cast<unsigned>(ymd.day()));
    *hour = static_cast<uint8_t>(h);
    *minute = static_cast<uint8_t>(m);
    *second = static_cast<uint8_t>(s);
    return true;
}

template <typename DateType>
inline bool try_convert_timestamp_with_fixed_offset(DateType& value, int64_t epoch_seconds,
                                                    int32_t offset_seconds) {
    uint16_t year = 0;
    uint8_t month = 0;
    uint8_t day = 0;
    uint8_t hour = 0;
    uint8_t minute = 0;
    uint8_t second = 0;
    if (!try_split_local_time(epoch_seconds + offset_seconds, &year, &month, &day, &hour, &minute,
                              &second)) {
        return false;
    }
    // The caller sets sub-second precision immediately after this conversion.
    value.unchecked_set_time(year, month, day, hour, minute, second, 0);
    return true;
}

template <typename DateType>
inline bool try_convert_timestamp_with_lookup(DateType& value, int64_t epoch_seconds,
                                              const cctz::time_zone& ctz) {
    static const auto epoch = std::chrono::time_point_cast<cctz::sys_seconds>(
            std::chrono::system_clock::from_time_t(0));
    cctz::time_point<cctz::sys_seconds> t = epoch + cctz::seconds(epoch_seconds);
    const int32_t offset = ctz.lookup_offset(t).offset;
    return try_convert_timestamp_with_fixed_offset(value, epoch_seconds, offset);
}

} // namespace detail

struct ConvertParams {
    // schema.logicalType.TIMESTAMP.isAdjustedToUTC == false
    static const cctz::time_zone utc0;
    // schema.logicalType.TIMESTAMP.isAdjustedToUTC == true, we should set local time zone
    const cctz::time_zone* ctz = nullptr;
    bool is_fixed_offset = false;
    int32_t fixed_offset_seconds = 0;
    int64_t second_mask = 1;
    int64_t scale_to_nano_factor = 1;
    const FieldSchema* field_schema = nullptr;

    //For UInt8 -> Int16,UInt16 -> Int32,UInt32 -> Int64,UInt64 -> Int128.
    bool is_type_compatibility = false;

    /**
     * Some frameworks like paimon maybe writes non-standard parquet files. Timestamp field doesn't have
     * logicalType or converted_type to indicates its precision. We have to reset the time mask.
     */
    void reset_time_scale_if_missing(int scale) {
        const auto& schema = field_schema->parquet_schema;
        if (!schema.__isset.logicalType && !schema.__isset.converted_type) {
            int ts_scale = 9;
            if (scale <= 3) {
                ts_scale = 3;
            } else if (scale <= 6) {
                ts_scale = 6;
            }
            second_mask = common::exp10_i64(ts_scale);
            scale_to_nano_factor = common::exp10_i64(9 - ts_scale);

            // The missing parque metadata makes it impossible for us to know the time zone information,
            // so we default to UTC here.
            if (ctz == nullptr) {
                ctz = &utc0;
            }
        }
    }

    void init(const FieldSchema* field_schema_, const cctz::time_zone* ctz_) {
        field_schema = field_schema_;
        if (ctz_ != nullptr) {
            ctz = ctz_;
        }
        const auto& schema = field_schema->parquet_schema;
        if (schema.__isset.logicalType && schema.logicalType.__isset.TIMESTAMP) {
            const auto& timestamp_info = schema.logicalType.TIMESTAMP;
            if (!timestamp_info.isAdjustedToUTC) {
                // should set timezone to utc+0
                // Reference: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#instant-semantics-timestamps-normalized-to-utc
                // If isAdjustedToUTC = false, the reader should display the same value no mater what local time zone is. For example:
                // When a timestamp is stored as `1970-01-03 12:00:00`,
                // if isAdjustedToUTC = true, UTC8 should read as `1970-01-03 20:00:00`, UTC6 should read as `1970-01-03 18:00:00`
                // if isAdjustedToUTC = false, UTC8 and UTC6 should read as `1970-01-03 12:00:00`, which is the same as `1970-01-03 12:00:00` in UTC0
                ctz = &utc0;
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

        if (ctz != nullptr) {
            is_fixed_offset =
                    TimezoneUtils::try_get_fixed_offset_seconds(*ctz, &fixed_offset_seconds);
        }
        is_type_compatibility = field_schema_->is_type_compatibility;
    }
};

inline IColumn* get_mutable_inner_column(ColumnPtr& column) {
    column = IColumn::mutate(std::move(column));
    auto mutable_column = column->assert_mutable();
    if (mutable_column->is_nullable()) {
        return &assert_cast<ColumnNullable*>(mutable_column.get())->get_nested_column();
    }
    return mutable_column.get();
}

inline size_t get_mutable_inner_column_size(const ColumnPtr& column) {
    if (column->is_nullable()) {
        const auto* nullable = assert_cast<const ColumnNullable*>(column.get());
        return nullable->get_nested_column().size();
    }
    return column->size();
}

inline size_t get_null_map_size_or_inner_column_size(const ColumnPtr& column) {
    if (column->is_nullable()) {
        const auto* nullable = assert_cast<const ColumnNullable*>(column.get());
        return nullable->get_null_map_column().size();
    }
    return column->size();
}

inline size_t get_appended_null_map_start(const ColumnPtr& column, size_t new_rows) {
    if (!column->is_nullable()) {
        return 0;
    }
    const auto* nullable = assert_cast<const ColumnNullable*>(column.get());
    const size_t null_map_size = nullable->get_null_map_column().size();
    DCHECK_GE(null_map_size, new_rows);
    return null_map_size - new_rows;
}

inline void align_null_map(ColumnPtr& src_column, ColumnPtr& dst_column, size_t old_null_map_size,
                           size_t new_rows, size_t src_null_map_start = 0) {
    if (!dst_column->is_nullable()) {
        return;
    }

    dst_column = IColumn::mutate(std::move(dst_column));
    auto* dst_nullable = assert_cast<ColumnNullable*>(dst_column->assert_mutable().get());
    auto& dst_null_map = dst_nullable->get_null_map_column();
    const size_t expected_rows = old_null_map_size + new_rows;
    if (dst_null_map.size() == expected_rows) {
        return;
    }
    DCHECK_EQ(dst_null_map.size(), old_null_map_size);
    if (src_column->is_nullable()) {
        const auto* src_nullable = assert_cast<const ColumnNullable*>(src_column.get());
        DCHECK_GE(src_nullable->get_null_map_column().size(), src_null_map_start + new_rows);
        dst_null_map.insert_range_from(src_nullable->get_null_map_column(), src_null_map_start,
                                       new_rows);
    } else {
        dst_null_map.insert_many_vals(0, new_rows);
    }
}

struct FixedLengthPhysicalData {
    const uint8_t* data = nullptr;
    size_t byte_size = 0;
    size_t rows = 0;
};

inline FixedLengthPhysicalData get_fixed_length_physical_data(const IColumn& column,
                                                              size_t type_length) {
    if (const auto fixed_length_column = check_and_get_column<ColumnFixedLengthObject>(column)) {
        DCHECK_EQ(fixed_length_column->item_size(), type_length);
        return {fixed_length_column->get_data().data(), fixed_length_column->byte_size(),
                fixed_length_column->size()};
    }

    const auto& uint8_column = assert_cast<const ColumnUInt8&>(column);
    DCHECK_EQ(uint8_column.size() % type_length, 0);
    return {uint8_column.get_data().data(), uint8_column.size(), uint8_column.size() / type_length};
}

/**
 * Convert parquet physical column to logical column
 * In parquet document(https://github.com/apache/parquet-format/blob/master/LogicalTypes.md),
 * Logical or converted type is the data type of column, physical type is the stored type of column chunk.
 * eg, decimal type can be stored as INT32, INT64, BYTE_ARRAY, FIXED_LENGTH_BYTE_ARRAY.
 * So there is a convert process from physical type to logical type.
 * In addition, Schema change will bring about a change in logical type.
 *
 * `PhysicalToLogicalConverter` strips away the conversion of logical type, and reuse `ColumnTypeConverter`
 * to resolve schema change, allowing parquet reader to only focus on the conversion of physical types.
 *
 * Therefore, tow layers converters are designed:
 * First, read parquet data with the physical type
 * Second, convert physical type to logical type
 * Third, convert logical type to the final type planned by FE(schema change)
 *
 * Ultimate performance optimization:
 * 1. If process of (First => Second) is consistent, eg. from BYTE_ARRAY to string, no additional copies and conversions will be introduced;
 * 2. If process of (Second => Third) is consistent, no additional copies and conversions will be introduced;
 * 3. Null maps are owned by each temporary nullable column, and only appended null slices are
 *    copied between conversion stages;
 * 4. Only create one physical column in physical conversion, and reused in each loop;
 * 5. Only create one logical column in logical conversion, and reused in each loop;
 * 6. FIXED_LENGTH_BYTE_ARRAY is read as ColumnFixedLengthObject instead of ColumnString, so
 *    the decoder can copy fixed-size values as a whole while keeping nullable row counts valid.
 */
class PhysicalToLogicalConverter {
protected:
    ColumnPtr _cached_src_physical_column = nullptr;
    DataTypePtr _cached_src_physical_type = nullptr;
    std::unique_ptr<converter::ColumnTypeConverter> _logical_converter = nullptr;

    std::string _error_msg;

    std::unique_ptr<ConvertParams> _convert_params;

public:
    static std::unique_ptr<PhysicalToLogicalConverter> get_converter(
            const FieldSchema* field_schema, DataTypePtr src_logical_type,
            const DataTypePtr& dst_logical_type, const cctz::time_zone* ctz,
            bool is_dict_filter = false);

    static bool is_parquet_native_type(PrimitiveType type);

    static bool is_decimal_type(PrimitiveType type);

    PhysicalToLogicalConverter() = default;
    virtual ~PhysicalToLogicalConverter() = default;

    virtual Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) {
        return Status::OK();
    }

    Status convert(ColumnPtr& src_physical_col, DataTypePtr src_logical_type,
                   const DataTypePtr& dst_logical_type, ColumnPtr& dst_logical_col,
                   bool is_dict_filter) {
        if (is_dict_filter) {
            src_logical_type = DataTypeFactory::instance().create_data_type(
                    PrimitiveType::TYPE_INT, dst_logical_type->is_nullable());
        }
        if (is_consistent() && _logical_converter->is_consistent()) {
            dst_logical_col = std::move(src_physical_col);
            return Status::OK();
        }
        if (_logical_converter->is_consistent()) {
            const size_t old_rows = get_mutable_inner_column_size(dst_logical_col);
            const size_t old_null_map_size =
                    get_null_map_size_or_inner_column_size(dst_logical_col);
            RETURN_IF_ERROR(physical_convert(src_physical_col, dst_logical_col));
            const size_t new_rows = get_mutable_inner_column_size(dst_logical_col) - old_rows;
            align_null_map(src_physical_col, dst_logical_col, old_null_map_size, new_rows,
                           get_appended_null_map_start(src_physical_col, new_rows));
            return Status::OK();
        }

        ColumnPtr src_logical_column;
        if (is_consistent()) {
            src_logical_column = src_physical_col;
        } else {
            src_logical_column = _logical_converter->get_column(src_logical_type, dst_logical_col,
                                                                dst_logical_type);
        }
        const size_t src_old_rows = get_mutable_inner_column_size(src_logical_column);
        const size_t src_old_null_map_size =
                get_null_map_size_or_inner_column_size(src_logical_column);
        RETURN_IF_ERROR(physical_convert(src_physical_col, src_logical_column));
        const size_t src_new_rows =
                get_mutable_inner_column_size(src_logical_column) - src_old_rows;
        align_null_map(src_physical_col, src_logical_column, src_old_null_map_size, src_new_rows,
                       get_appended_null_map_start(src_physical_col, src_new_rows));

        dst_logical_col = IColumn::mutate(std::move(dst_logical_col));
        const size_t dst_old_rows = get_mutable_inner_column_size(dst_logical_col);
        const size_t dst_old_null_map_size =
                get_null_map_size_or_inner_column_size(dst_logical_col);
        auto converted_column = dst_logical_col->assert_mutable();
        RETURN_IF_ERROR(_logical_converter->convert(src_logical_column, converted_column));
        const size_t dst_new_rows = get_mutable_inner_column_size(dst_logical_col) - dst_old_rows;
        align_null_map(src_logical_column, dst_logical_col, dst_old_null_map_size, dst_new_rows,
                       get_appended_null_map_start(src_logical_column, dst_new_rows));
        return Status::OK();
    }

    virtual ColumnPtr get_physical_column(tparquet::Type::type src_physical_type,
                                          DataTypePtr src_logical_type,
                                          ColumnPtr& dst_logical_column,
                                          const DataTypePtr& dst_logical_type, bool is_dict_filter);

    DataTypePtr& get_physical_type() { return _cached_src_physical_type; }

    bool read_directly_into_dst_logical_column() {
        return !_convert_params->is_type_compatibility && is_consistent() &&
               _logical_converter->is_consistent();
    }

    virtual bool is_consistent() { return false; }

    virtual bool support() { return true; }

    std::string get_error_msg() { return _error_msg; }
};

class ConsistentPhysicalConverter : public PhysicalToLogicalConverter {
    bool is_consistent() override { return true; }
};

class UnsupportedConverter : public PhysicalToLogicalConverter {
public:
    UnsupportedConverter(std::string error_msg) { _error_msg = error_msg; }

    UnsupportedConverter(tparquet::Type::type src_physical_type,
                         const DataTypePtr& src_logical_type) {
        std::string src_physical_str = tparquet::to_string(src_physical_type);
        std::string src_logical_str = src_logical_type->get_name();
        _error_msg = src_physical_str + " => " + src_logical_str;
    }

    bool support() override { return false; }

    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        return Status::InternalError("Unsupported physical to logical type: {}", _error_msg);
    }
};

// for tinyint, smallint
template <PrimitiveType IntPrimitiveType>
class LittleIntPhysicalConverter : public PhysicalToLogicalConverter {
    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        using DstCppType = typename PrimitiveTypeTraits<IntPrimitiveType>::CppType;
        using DstColumnType = typename PrimitiveTypeTraits<IntPrimitiveType>::ColumnType;
        ColumnPtr from_col = remove_nullable(src_physical_col);
        IColumn* to_col = get_mutable_inner_column(src_logical_column);

        size_t rows = from_col->size();
        // always comes from tparquet::Type::INT32
        auto& src_data = assert_cast<const ColumnInt32*>(from_col.get())->get_data();
        size_t start_idx = to_col->size();
        to_col->resize(start_idx + rows);
        auto& data = assert_cast<DstColumnType&>(*to_col).get_data();
        for (int i = 0; i < rows; ++i) {
            data[start_idx + i] = static_cast<DstCppType>(src_data[i]);
        }

        return Status::OK();
    }
};

template <PrimitiveType type>
struct UnsignedTypeTraits;

template <>
struct UnsignedTypeTraits<TYPE_SMALLINT> {
    using UnsignedCppType = UInt8;
    //https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#unsigned-integers
    //INT(8, false), INT(16, false), and INT(32, false) must annotate an int32 primitive type and INT(64, false)
    //must annotate an int64 primitive type.
    using StorageCppType = Int32;
    using StorageColumnType = ColumnInt32;
};

template <>
struct UnsignedTypeTraits<TYPE_INT> {
    using UnsignedCppType = UInt16;
    using StorageCppType = Int32;
    using StorageColumnType = ColumnInt32;
};

template <>
struct UnsignedTypeTraits<TYPE_BIGINT> {
    using UnsignedCppType = UInt32;
    using StorageCppType = Int32;
    using StorageColumnType = ColumnInt32;
};

template <>
struct UnsignedTypeTraits<TYPE_LARGEINT> {
    using UnsignedCppType = UInt64;
    using StorageCppType = Int64;
    using StorageColumnType = ColumnInt64;
};

template <PrimitiveType IntPrimitiveType>
class UnsignedIntegerConverter : public PhysicalToLogicalConverter {
    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        using UnsignedCppType = typename UnsignedTypeTraits<IntPrimitiveType>::UnsignedCppType;
        using StorageCppType = typename UnsignedTypeTraits<IntPrimitiveType>::StorageCppType;
        using StorageColumnType = typename UnsignedTypeTraits<IntPrimitiveType>::StorageColumnType;
        using DstColumnType = typename PrimitiveTypeTraits<IntPrimitiveType>::ColumnType;

        ColumnPtr from_col = remove_nullable(src_physical_col);
        IColumn* to_col = get_mutable_inner_column(src_logical_column);
        auto& src_data = assert_cast<const StorageColumnType*>(from_col.get())->get_data();

        size_t rows = src_data.size();
        size_t start_idx = to_col->size();
        to_col->resize(start_idx + rows);
        auto& data = assert_cast<DstColumnType&>(*to_col).get_data();

        for (int i = 0; i < rows; i++) {
            StorageCppType src_value = src_data[i];
            auto unsigned_value = static_cast<UnsignedCppType>(src_value);
            data[start_idx + i] = unsigned_value;
        }

        return Status::OK();
    }
};

class FixedSizeBinaryConverter : public PhysicalToLogicalConverter {
private:
    int _type_length;

public:
    FixedSizeBinaryConverter(int type_length) : _type_length(type_length) {}

    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        ColumnPtr from_col = remove_nullable(src_physical_col);
        IColumn* to_col = get_mutable_inner_column(src_logical_column);

        const auto src_data = get_fixed_length_physical_data(*from_col, _type_length);
        size_t length = src_data.byte_size;
        size_t num_values = src_data.rows;
        auto& string_col = static_cast<ColumnString&>(*to_col);
        auto& offsets = string_col.get_offsets();
        auto& chars = string_col.get_chars();

        size_t origin_size = chars.size();
        chars.resize(origin_size + length);
        memcpy(chars.data() + origin_size, src_data.data, length);

        origin_size = offsets.size();
        offsets.resize(origin_size + num_values);
        auto end_offset = offsets[origin_size - 1];
        for (int i = 0; i < num_values; ++i) {
            end_offset += _type_length;
            offsets[origin_size + i] = end_offset;
        }

        return Status::OK();
    }
};

class Float16PhysicalConverter : public PhysicalToLogicalConverter {
private:
    int _type_length;

public:
    Float16PhysicalConverter(int type_length) : _type_length(type_length) {
        DCHECK_EQ(_type_length, 2);
    }

    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        ColumnPtr from_col = remove_nullable(src_physical_col);
        IColumn* to_col = get_mutable_inner_column(src_logical_column);

        const auto src_data = get_fixed_length_physical_data(*from_col, _type_length);
        size_t num_values = src_data.rows;
        auto* to_float_column = assert_cast<ColumnFloat32*>(to_col);
        size_t start_idx = to_float_column->size();
        to_float_column->resize(start_idx + num_values);
        auto& to_float_column_data = to_float_column->get_data();
        const auto* ptr = src_data.data;
        for (int i = 0; i < num_values; ++i) {
            size_t offset = i * _type_length;
            const auto* data_ptr = ptr + offset;
            uint16_t raw;
            memcpy(&raw, data_ptr, sizeof(uint16_t));
            float value = half_to_float(raw);
            to_float_column_data[start_idx + i] = value;
        }

        return Status::OK();
    }

    float half_to_float(uint16_t h) {
        // uint16_t h: half precision floating point
        // bit 15:       sign（1 bit）
        // bits 14..10 : exponent（5 bits）
        // bits 9..0   : mantissa（10 bits）

        // sign bit placed to float32 bit31
        uint32_t sign = (h & 0x8000U) << 16; // 0x8000 << 16 = 0x8000_0000
        // exponent:（5 bits）
        uint32_t exp = (h & 0x7C00U) >> 10; // 0x7C00 = 0111 1100 0000 (half exponent mask)
        // mantissa（10 bits）
        uint32_t mant = (h & 0x03FFU); // 10-bit fraction

        // cases：Zero/Subnormal, Normal, Inf/NaN
        if (exp == 0) {
            // exp==0: Zero or Subnormal ----------
            if (mant == 0) {
                // ±0.0
                // sign = either 0x00000000 or 0x80000000
                return std::bit_cast<float>(sign);
            } else {
                // ---------- Subnormal ----------
                // half subnormal:
                //    value = (-1)^sign * (mant / 2^10) * 2^(1 - bias)
                // half bias = 15 → exponent = 1 - 15 = -14
                float f = (static_cast<float>(mant) / 1024.0F) * std::powf(2.0F, -14.0F);
                return sign ? -f : f;
            }
        } else if (exp == 0x1F) {
            // exp==31: Inf or NaN ----------
            // float32:
            //    exponent = 255 (0xFF)
            //    mantissa = mant << 13
            uint32_t f = sign | 0x7F800000U | (mant << 13);
            return std::bit_cast<float>(f);
        } else {
            // Normalized ----------
            // float32 exponent:
            //   exp32 = exp16 - bias16 + bias32
            //   bias16 = 15
            //   bias32 = 127
            //
            // so: exp32 = exp + (127 - 15)
            uint32_t f = sign | ((exp + (127 - 15)) << 23) // place to float32 exponent
                         | (mant << 13);                   // mantissa align to 23 bits
            return std::bit_cast<float>(f);
        }
    }
};

class UUIDVarBinaryConverter : public PhysicalToLogicalConverter {
public:
    UUIDVarBinaryConverter(int type_length) : _type_length(type_length) {}

    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        DCHECK(!is_column_const(*src_physical_col)) << src_physical_col->dump_structure();
        DCHECK(!is_column_const(*src_logical_column)) << src_logical_column->dump_structure();
        const ColumnPtr from_col = remove_nullable(src_physical_col);
        const auto src_data = get_fixed_length_physical_data(*from_col, _type_length);

        IColumn* to_col = get_mutable_inner_column(src_logical_column);
        auto* to_varbinary_column = assert_cast<ColumnVarbinary*>(to_col);
        size_t num_values = src_data.rows;
        const auto* ptr = src_data.data;

        for (int i = 0; i < num_values; ++i) {
            auto offset = i * _type_length;
            const char* data_ptr = reinterpret_cast<const char*>(ptr + offset);
            to_varbinary_column->insert_data(data_ptr, _type_length);
        }
        return Status::OK();
    }

private:
    int _type_length;
};

template <PrimitiveType DecimalPType>
class FixedSizeToDecimal : public PhysicalToLogicalConverter {
public:
    using DecimalType = typename PrimitiveTypeTraits<DecimalPType>::CppType;
    FixedSizeToDecimal(int32_t type_length) : _type_length(type_length) {}

    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        ColumnPtr src_col = remove_nullable(src_physical_col);
        IColumn* dst_col = get_mutable_inner_column(src_logical_column);

#define M(FixedTypeLength, ValueCopyType) \
    case FixedTypeLength:                 \
        return _convert_internal<FixedTypeLength, ValueCopyType>(src_col, dst_col);

#define APPLY_FOR_DECIMALS() \
    M(1, int64_t)            \
    M(2, int64_t)            \
    M(3, int64_t)            \
    M(4, int64_t)            \
    M(5, int64_t)            \
    M(6, int64_t)            \
    M(7, int64_t)            \
    M(8, int64_t)            \
    M(9, int128_t)           \
    M(10, int128_t)          \
    M(11, int128_t)          \
    M(12, int128_t)          \
    M(13, int128_t)          \
    M(14, int128_t)          \
    M(15, int128_t)          \
    M(16, int128_t)          \
    M(17, wide::Int256)      \
    M(18, wide::Int256)      \
    M(19, wide::Int256)      \
    M(20, wide::Int256)      \
    M(21, wide::Int256)      \
    M(22, wide::Int256)      \
    M(23, wide::Int256)      \
    M(24, wide::Int256)      \
    M(25, wide::Int256)      \
    M(26, wide::Int256)      \
    M(27, wide::Int256)      \
    M(28, wide::Int256)      \
    M(29, wide::Int256)      \
    M(30, wide::Int256)      \
    M(31, wide::Int256)      \
    M(32, wide::Int256)

        switch (_type_length) {
            APPLY_FOR_DECIMALS()
        default:
            throw Exception(Status::FatalError("__builtin_unreachable"));
        }
        return Status::OK();
#undef APPLY_FOR_DECIMALS
#undef M
    }

    template <int fixed_type_length, typename ValueCopyType>
    Status _convert_internal(ColumnPtr& src_col, IColumn* dst_col) {
        const auto src_data = get_fixed_length_physical_data(*src_col, fixed_type_length);
        size_t rows = src_data.rows;
        const auto* buf = src_data.data;
        size_t start_idx = dst_col->size();
        dst_col->resize(start_idx + rows);

        auto& data = static_cast<ColumnDecimal<DecimalPType>*>(dst_col)->get_data();
        size_t offset = 0;
        for (int i = 0; i < rows; i++) {
            // When Decimal in parquet is stored in byte arrays, binary and fixed,
            // the unscaled number must be encoded as two's complement using big-endian byte order.
            ValueCopyType value = 0;
            memcpy(reinterpret_cast<char*>(&value), buf + offset, sizeof(value));
            offset += fixed_type_length;
            value = to_endian<std::endian::big>(value);
            value = value >> ((sizeof(value) - fixed_type_length) * 8);
            auto& v = reinterpret_cast<DecimalType&>(data[start_idx + i]);
            v = (DecimalType)value;
        }

        return Status::OK();
    }

private:
    int32_t _type_length;
};

template <PrimitiveType DecimalPType>
class StringToDecimal : public PhysicalToLogicalConverter {
    using DecimalType = typename PrimitiveTypeTraits<DecimalPType>::CppType;
    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        using ValueCopyType = DecimalType::NativeType;
        ColumnPtr src_col = remove_nullable(src_physical_col);
        IColumn* dst_col = get_mutable_inner_column(src_logical_column);

        size_t rows = src_col->size();
        auto buf = static_cast<const ColumnString*>(src_col.get())->get_chars().data();
        auto& offset = static_cast<const ColumnString*>(src_col.get())->get_offsets();
        size_t start_idx = dst_col->size();
        dst_col->resize(start_idx + rows);

        auto& data = static_cast<ColumnDecimal<DecimalPType>*>(dst_col)->get_data();
        for (int i = 0; i < rows; i++) {
            size_t len = offset[i] - offset[i - 1];
            // When Decimal in parquet is stored in byte arrays, binary and fixed,
            // the unscaled number must be encoded as two's complement using big-endian byte order.
            ValueCopyType value = 0;
            if (len > 0) {
                memcpy(reinterpret_cast<char*>(&value), buf + offset[i - 1], len);
                value = to_endian<std::endian::big>(value);
                value = value >> ((sizeof(value) - len) * 8);
            }
            auto& v = reinterpret_cast<DecimalType&>(data[start_idx + i]);
            v = (DecimalType)value;
        }

        return Status::OK();
    }
};

template <PrimitiveType NumberType, PrimitiveType DecimalPType>
class NumberToDecimal : public PhysicalToLogicalConverter {
    using DecimalType = typename PrimitiveTypeTraits<DecimalPType>::CppType;
    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        using ValueCopyType = typename DecimalType::NativeType;
        ColumnPtr src_col = remove_nullable(src_physical_col);
        IColumn* dst_col = get_mutable_inner_column(src_logical_column);

        size_t rows = src_col->size();
        auto* src_data =
                static_cast<const ColumnVector<NumberType>*>(src_col.get())->get_data().data();
        size_t start_idx = dst_col->size();
        dst_col->resize(start_idx + rows);

        auto* data = static_cast<ColumnDecimal<DecimalPType>*>(dst_col)->get_data().data();

        for (int i = 0; i < rows; i++) {
            ValueCopyType value;
            if constexpr (std::is_same_v<DecimalType, Decimal256>) {
                value = src_data[i];
            } else {
                value = cast_set<ValueCopyType, typename PrimitiveTypeTraits<NumberType>::CppType,
                                 false>(src_data[i]);
            }

            data[start_idx + i] = (DecimalType)value;
        }
        return Status::OK();
    }
};

class Int32ToDate : public PhysicalToLogicalConverter {
    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        ColumnPtr src_col = remove_nullable(src_physical_col);
        IColumn* dst_col = get_mutable_inner_column(src_logical_column);

        size_t rows = src_col->size();
        size_t start_idx = dst_col->size();
        dst_col->reserve(start_idx + rows);

        auto& src_data = static_cast<const ColumnInt32*>(src_col.get())->get_data();
        auto& data = static_cast<ColumnDateV2*>(dst_col)->get_data();
        date_day_offset_dict& date_dict = date_day_offset_dict::get();

        for (int i = 0; i < rows; i++) {
            data.push_back_without_reserve(date_dict[src_data[i]].to_date_int_val());
        }

        return Status::OK();
    }
};

struct Int64ToTimestamp : public PhysicalToLogicalConverter {
    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        ColumnPtr src_col = remove_nullable(src_physical_col);
        IColumn* dst_col = get_mutable_inner_column(src_logical_column);

        size_t rows = src_col->size();
        size_t start_idx = dst_col->size();
        dst_col->resize(start_idx + rows);

        auto src_data = static_cast<const ColumnInt64*>(src_col.get())->get_data().data();
        auto& data = static_cast<ColumnDateTimeV2*>(dst_col)->get_data();

        for (int i = 0; i < rows; i++) {
            int64_t x = src_data[i];
            auto& num = data[start_idx + i];
            auto& value = reinterpret_cast<DateV2Value<DateTimeV2ValueType>&>(num);
            const int64_t epoch_seconds = x / _convert_params->second_mask;
            if (_convert_params->is_fixed_offset) {
                if (!detail::try_convert_timestamp_with_fixed_offset(
                            value, epoch_seconds, _convert_params->fixed_offset_seconds)) {
                    value.from_unixtime(epoch_seconds, *_convert_params->ctz);
                }
            } else if (!detail::try_convert_timestamp_with_lookup(value, epoch_seconds,
                                                                  *_convert_params->ctz)) {
                value.from_unixtime(epoch_seconds, *_convert_params->ctz);
            }
            value.set_microsecond((x % _convert_params->second_mask) *
                                  (_convert_params->scale_to_nano_factor / 1000));
        }
        return Status::OK();
    }
};

struct Int64ToTimestampTz : public PhysicalToLogicalConverter {
    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        ColumnPtr src_col = remove_nullable(src_physical_col);
        IColumn* dst_col = get_mutable_inner_column(src_logical_column);

        size_t rows = src_col->size();
        size_t start_idx = dst_col->size();
        dst_col->resize(start_idx + rows);

        const auto& src_data = assert_cast<const ColumnInt64*>(src_col.get())->get_data();
        auto& dest_data = assert_cast<ColumnTimeStampTz*>(dst_col)->get_data();
        static const cctz::time_zone UTC = cctz::utc_time_zone();

        for (int i = 0; i < rows; i++) {
            int64_t x = src_data[i];
            auto& tz = dest_data[start_idx + i];
            tz.from_unixtime(x / _convert_params->second_mask, UTC);
            tz.set_microsecond((x % _convert_params->second_mask) *
                               (_convert_params->scale_to_nano_factor / 1000));
        }
        return Status::OK();
    }
};

struct Int96toTimestamp : public PhysicalToLogicalConverter {
    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        ColumnPtr src_col = remove_nullable(src_physical_col);
        IColumn* dst_col = get_mutable_inner_column(src_logical_column);

        size_t rows = src_col->size() / sizeof(ParquetInt96);
        auto& src_data = static_cast<const ColumnInt8*>(src_col.get())->get_data();
        auto ParquetInt96_data = (ParquetInt96*)src_data.data();
        size_t start_idx = dst_col->size();
        dst_col->resize(start_idx + rows);
        auto& data = static_cast<ColumnDateTimeV2*>(dst_col)->get_data();

        for (int i = 0; i < rows; i++) {
            ParquetInt96 src_cell_data = ParquetInt96_data[i];
            auto& dst_value =
                    reinterpret_cast<DateV2Value<DateTimeV2ValueType>&>(data[start_idx + i]);

            int64_t timestamp_with_micros = src_cell_data.to_timestamp_micros();
            const int64_t epoch_seconds = timestamp_with_micros / 1000000;
            if (_convert_params->is_fixed_offset) {
                if (!detail::try_convert_timestamp_with_fixed_offset(
                            dst_value, epoch_seconds, _convert_params->fixed_offset_seconds)) {
                    dst_value.from_unixtime(epoch_seconds, *_convert_params->ctz);
                }
            } else if (!detail::try_convert_timestamp_with_lookup(dst_value, epoch_seconds,
                                                                  *_convert_params->ctz)) {
                dst_value.from_unixtime(epoch_seconds, *_convert_params->ctz);
            }
            dst_value.set_microsecond(timestamp_with_micros % 1000000);
        }
        return Status::OK();
    }
};

struct Int96toTimestampTz : public PhysicalToLogicalConverter {
    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        ColumnPtr src_col = remove_nullable(src_physical_col);
        IColumn* dst_col = get_mutable_inner_column(src_logical_column);

        size_t rows = src_col->size() / sizeof(ParquetInt96);
        const auto& src_data = assert_cast<const ColumnInt8*>(src_col.get())->get_data();
        auto* ParquetInt96_data = (ParquetInt96*)src_data.data();
        size_t start_idx = dst_col->size();
        dst_col->resize(start_idx + rows);
        auto& data = assert_cast<ColumnTimeStampTz*>(dst_col)->get_data();
        static const cctz::time_zone UTC = cctz::utc_time_zone();

        for (int i = 0; i < rows; i++) {
            ParquetInt96 src_cell_data = ParquetInt96_data[i];
            int64_t timestamp_with_micros = src_cell_data.to_timestamp_micros();
            auto& tz = data[start_idx + i];
            tz.from_unixtime(timestamp_with_micros / 1000000, UTC);
            tz.set_microsecond(timestamp_with_micros % 1000000);
        }
        return Status::OK();
    }
};

} // namespace doris::parquet
