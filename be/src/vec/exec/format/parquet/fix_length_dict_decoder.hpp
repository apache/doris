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

#include "util/bit_util.h"
#include "vec/columns/column_dictionary.h"
#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exec/format/parquet/decoder.h"

namespace doris::vectorized {

template <typename T>
class FixLengthDictDecoder final : public BaseDictDecoder {
public:
    FixLengthDictDecoder(tparquet::Type::type physical_type)
            : BaseDictDecoder(), _physical_type(physical_type) {};
    ~FixLengthDictDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector, bool is_dict_filter) override {
        if (select_vector.has_filter()) {
            return _decode_values<true>(doris_column, data_type, select_vector, is_dict_filter);
        } else {
            return _decode_values<false>(doris_column, data_type, select_vector, is_dict_filter);
        }
    }

    template <bool has_filter>
    Status _decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                          ColumnSelectVector& select_vector, bool is_dict_filter) {
        size_t non_null_size = select_vector.num_values() - select_vector.num_nulls();
        if (doris_column->is_column_dictionary() &&
            assert_cast<ColumnDictI32&>(*doris_column).dict_size() == 0) {
            std::vector<StringRef> dict_items;
            dict_items.reserve(_dict_items.size());
            for (int i = 0; i < _dict_items.size(); ++i) {
                dict_items.emplace_back((char*)(&_dict_items[i]), _type_length);
            }
            assert_cast<ColumnDictI32&>(*doris_column)
                    .insert_many_dict_data(&dict_items[0], dict_items.size());
        }
        if (doris_column->is_column_dictionary()) {
            ColumnDictI32& dict_column = assert_cast<ColumnDictI32&>(*doris_column);
            if (dict_column.dict_size() == 0) {
                std::vector<StringRef> dict_items;
                dict_items.reserve(_dict_items.size());
                for (int i = 0; i < _dict_items.size(); ++i) {
                    dict_items.emplace_back((char*)(&_dict_items[i]), _type_length);
                }
                dict_column.insert_many_dict_data(&dict_items[0], dict_items.size());
            }
        }
        _indexes.resize(non_null_size);
        _index_batch_decoder->GetBatch(&_indexes[0], non_null_size);

        if (doris_column->is_column_dictionary() || is_dict_filter) {
            return _decode_dict_values<has_filter>(doris_column, select_vector, is_dict_filter);
        }

        TypeIndex logical_type = remove_nullable(data_type)->get_type_id();
        switch (logical_type) {
#define DISPATCH(NUMERIC_TYPE, CPP_NUMERIC_TYPE, PHYSICAL_TYPE)                                   \
    case NUMERIC_TYPE:                                                                            \
        if constexpr (!std::is_same_v<T, ParquetInt96>) {                                         \
            return _decode_numeric<CPP_NUMERIC_TYPE, T, has_filter>(doris_column, select_vector); \
        }
            FOR_LOGICAL_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
        case TypeIndex::Date:
            if constexpr (std::is_same_v<T, Int32>) {
                return _decode_date<VecDateTimeValue, Int64, has_filter>(doris_column,
                                                                         select_vector);
            }
            break;
        case TypeIndex::DateV2:
            if constexpr (std::is_same_v<T, Int32>) {
                return _decode_date<DateV2Value<DateV2ValueType>, UInt32, has_filter>(
                        doris_column, select_vector);
            }
            break;
        case TypeIndex::DateTime:
            if constexpr (std::is_same_v<T, ParquetInt96>) {
                return _decode_datetime96<VecDateTimeValue, Int64, has_filter>(doris_column,
                                                                               select_vector);
            } else if constexpr (std::is_same_v<T, Int64>) {
                return _decode_datetime64<VecDateTimeValue, Int64, has_filter>(doris_column,
                                                                               select_vector);
            }
            break;
        case TypeIndex::DateTimeV2:
            // Spark can set the timestamp precision by the following configuration:
            // spark.sql.parquet.outputTimestampType = INT96(NANOS), TIMESTAMP_MICROS, TIMESTAMP_MILLIS
            if constexpr (std::is_same_v<T, ParquetInt96>) {
                return _decode_datetime96<DateV2Value<DateTimeV2ValueType>, UInt64, has_filter>(
                        doris_column, select_vector);
            } else if constexpr (std::is_same_v<T, Int64>) {
                return _decode_datetime64<DateV2Value<DateTimeV2ValueType>, UInt64, has_filter>(
                        doris_column, select_vector);
            }
            break;
        case TypeIndex::Decimal32:
            if constexpr (std::is_same_v<T, Int32>) {
                return _decode_primitive_decimal<Int32, Int32, has_filter>(doris_column, data_type,
                                                                           select_vector);
            } else if constexpr (std::is_same_v<T, Int64>) {
                return _decode_primitive_decimal<Int32, Int64, has_filter>(doris_column, data_type,
                                                                           select_vector);
            }
            break;
        case TypeIndex::Decimal64:
            if constexpr (std::is_same_v<T, Int32>) {
                return _decode_primitive_decimal<Int64, Int32, has_filter>(doris_column, data_type,
                                                                           select_vector);
            } else if constexpr (std::is_same_v<T, Int64>) {
                return _decode_primitive_decimal<Int64, Int64, has_filter>(doris_column, data_type,
                                                                           select_vector);
            }
            break;
        case TypeIndex::Decimal128:
            if constexpr (std::is_same_v<T, Int32>) {
                return _decode_primitive_decimal<Int128, Int32, has_filter>(doris_column, data_type,
                                                                            select_vector);
            } else if constexpr (std::is_same_v<T, Int64>) {
                return _decode_primitive_decimal<Int128, Int64, has_filter>(doris_column, data_type,
                                                                            select_vector);
            }
            break;
        case TypeIndex::Decimal128I:
            if constexpr (std::is_same_v<T, Int32>) {
                return _decode_primitive_decimal<Int128, Int32, has_filter>(doris_column, data_type,
                                                                            select_vector);
            } else if constexpr (std::is_same_v<T, Int64>) {
                return _decode_primitive_decimal<Int128, Int64, has_filter>(doris_column, data_type,
                                                                            select_vector);
            }
            break;
        case TypeIndex::String:
            [[fallthrough]];
        case TypeIndex::FixedString:
            break;
        default:
            break;
        }

        return Status::InvalidArgument(
                "Can't decode parquet physical type {} to doris logical type {}",
                tparquet::to_string(_physical_type), getTypeName(logical_type));
    }

    Status set_dict(std::unique_ptr<uint8_t[]>& dict, int32_t length, size_t num_values) override {
        if (num_values * _type_length != length) {
            return Status::Corruption("Wrong dictionary data for fixed length type");
        }
        _dict = std::move(dict);
        char* dict_item_address = reinterpret_cast<char*>(_dict.get());
        _dict_items.resize(num_values);
        for (size_t i = 0; i < num_values; ++i) {
            _dict_items[i] = *(T*)dict_item_address;
            dict_item_address += _type_length;
        }
        return Status::OK();
    }

protected:
    template <typename Numeric, typename PhysicalType, bool has_filter>
    Status _decode_numeric(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector) {
        auto& column_data = static_cast<ColumnVector<Numeric>&>(*doris_column).get_data();
        size_t data_index = column_data.size();
        column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
        size_t dict_index = 0;
        ColumnSelectVector::DataReadType read_type;
        while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                for (size_t i = 0; i < run_length; ++i) {
                    column_data[data_index++] =
                            static_cast<PhysicalType>(_dict_items[_indexes[dict_index++]]);
                }
                break;
            }
            case ColumnSelectVector::NULL_DATA: {
                data_index += run_length;
                break;
            }
            case ColumnSelectVector::FILTERED_CONTENT: {
                dict_index += run_length;
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
    Status _decode_date(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector) {
        auto& column_data = static_cast<ColumnVector<ColumnType>&>(*doris_column).get_data();
        size_t data_index = column_data.size();
        column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
        size_t dict_index = 0;
        date_day_offset_dict& date_dict = date_day_offset_dict::get();
        ColumnSelectVector::DataReadType read_type;
        while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                for (size_t i = 0; i < run_length; ++i) {
                    int64_t date_value =
                            _dict_items[_indexes[dict_index++]] + _decode_params->offset_days;
                    if constexpr (std::is_same_v<CppType, VecDateTimeValue>) {
                        auto& v = reinterpret_cast<CppType&>(column_data[data_index++]);
                        v.create_from_date_v2(date_dict[date_value], TIME_DATE);
                        // we should cast to date if using date v1.
                        v.cast_to_date();
                    } else {
                        reinterpret_cast<CppType&>(column_data[data_index++]) =
                                date_dict[date_value];
                    }
                }
                break;
            }
            case ColumnSelectVector::NULL_DATA: {
                data_index += run_length;
                break;
            }
            case ColumnSelectVector::FILTERED_CONTENT: {
                dict_index += run_length;
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
    Status _decode_datetime64(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector) {
        auto& column_data = static_cast<ColumnVector<ColumnType>&>(*doris_column).get_data();
        size_t data_index = column_data.size();
        column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
        size_t dict_index = 0;
        ColumnSelectVector::DataReadType read_type;
        while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                for (size_t i = 0; i < run_length; ++i) {
                    int64_t date_value = _dict_items[_indexes[dict_index++]];
                    auto& v = reinterpret_cast<CppType&>(column_data[data_index++]);
                    v.from_unixtime(date_value / _decode_params->second_mask, *_decode_params->ctz);
                    if constexpr (std::is_same_v<CppType, DateV2Value<DateTimeV2ValueType>>) {
                        // nanoseconds will be ignored.
                        v.set_microsecond((date_value % _decode_params->second_mask) *
                                          _decode_params->scale_to_nano_factor / 1000);
                        // TODO: the precision of datetime v1
                    }
                }
                break;
            }
            case ColumnSelectVector::NULL_DATA: {
                data_index += run_length;
                break;
            }
            case ColumnSelectVector::FILTERED_CONTENT: {
                dict_index += run_length;
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
    Status _decode_datetime96(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector) {
        auto& column_data = static_cast<ColumnVector<ColumnType>&>(*doris_column).get_data();
        size_t data_index = column_data.size();
        column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
        size_t dict_index = 0;
        ColumnSelectVector::DataReadType read_type;
        while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                for (size_t i = 0; i < run_length; ++i) {
                    ParquetInt96& datetime96 = _dict_items[_indexes[dict_index++]];
                    auto& v = reinterpret_cast<CppType&>(column_data[data_index++]);
                    int64_t micros = datetime96.to_timestamp_micros();
                    v.from_unixtime(micros / 1000000, *_decode_params->ctz);
                    if constexpr (std::is_same_v<CppType, DateV2Value<DateTimeV2ValueType>>) {
                        // spark.sql.parquet.outputTimestampType = INT96(NANOS) will lost precision.
                        // only keep microseconds.
                        v.set_microsecond(micros % 1000000);
                    }
                }
                break;
            }
            case ColumnSelectVector::NULL_DATA: {
                data_index += run_length;
                break;
            }
            case ColumnSelectVector::FILTERED_CONTENT: {
                dict_index += run_length;
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
    Status _decode_primitive_decimal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                     ColumnSelectVector& select_vector) {
        init_decimal_converter<DecimalPrimitiveType>(data_type);
        DecimalScaleParams& scale_params = _decode_params->decimal_scale;
#define M(FixedTypeLength, ValueCopyType, ScaleType)                                          \
    case FixedTypeLength:                                                                     \
        return _decode_primitive_decimal_internal<DecimalPrimitiveType, DecimalPhysicalType,  \
                                                  has_filter, FixedTypeLength, ValueCopyType, \
                                                  ScaleType>(doris_column, data_type,         \
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

    template <typename DecimalPrimitiveType, typename DecimalPhysicalType, bool has_filter,
              int fixed_type_length, typename ValueCopyType,
              DecimalScaleParams::ScaleType ScaleType>
    Status _decode_primitive_decimal_internal(MutableColumnPtr& doris_column,
                                              DataTypePtr& data_type,
                                              ColumnSelectVector& select_vector) {
        auto& column_data =
                static_cast<ColumnDecimal<Decimal<DecimalPrimitiveType>>&>(*doris_column)
                        .get_data();
        size_t data_index = column_data.size();
        column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
        size_t dict_index = 0;
        DecimalScaleParams& scale_params = _decode_params->decimal_scale;

        ColumnSelectVector::DataReadType read_type;
        while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                for (size_t i = 0; i < run_length; ++i) {
                    ValueCopyType value = static_cast<T>(_dict_items[_indexes[dict_index++]]);
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
                }
                break;
            }
            case ColumnSelectVector::NULL_DATA: {
                data_index += run_length;
                break;
            }
            case ColumnSelectVector::FILTERED_CONTENT: {
                dict_index += run_length;
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

    tparquet::Type::type _physical_type;

    // For dictionary encoding
    std::vector<T> _dict_items;
};

template <>
class FixLengthDictDecoder<char*> final : public BaseDictDecoder {
public:
    FixLengthDictDecoder(tparquet::Type::type physical_type)
            : BaseDictDecoder(), _physical_type(physical_type) {};
    ~FixLengthDictDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector, bool is_dict_filter) override {
        if (select_vector.has_filter()) {
            return _decode_values<true>(doris_column, data_type, select_vector, is_dict_filter);
        } else {
            return _decode_values<false>(doris_column, data_type, select_vector, is_dict_filter);
        }
    }

    template <bool has_filter>
    Status _decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                          ColumnSelectVector& select_vector, bool is_dict_filter) {
        size_t non_null_size = select_vector.num_values() - select_vector.num_nulls();
        if (doris_column->is_column_dictionary() &&
            assert_cast<ColumnDictI32&>(*doris_column).dict_size() == 0) {
            std::vector<StringRef> dict_items;
            dict_items.reserve(_dict_items.size());
            for (int i = 0; i < _dict_items.size(); ++i) {
                dict_items.emplace_back(_dict_items[i], _type_length);
            }
            assert_cast<ColumnDictI32&>(*doris_column)
                    .insert_many_dict_data(&dict_items[0], dict_items.size());
        }
        _indexes.resize(non_null_size);
        _index_batch_decoder->GetBatch(&_indexes[0], non_null_size);

        if (doris_column->is_column_dictionary() || is_dict_filter) {
            return _decode_dict_values<has_filter>(doris_column, select_vector, is_dict_filter);
        }

        TypeIndex logical_type = remove_nullable(data_type)->get_type_id();
        switch (logical_type) {
        case TypeIndex::Decimal32:
            if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
                return _decode_binary_decimal<Int32, has_filter>(doris_column, data_type,
                                                                 select_vector);
            }
            break;
        case TypeIndex::Decimal64:
            if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
                return _decode_binary_decimal<Int64, has_filter>(doris_column, data_type,
                                                                 select_vector);
            }
            break;
        case TypeIndex::Decimal128:
            if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
                return _decode_binary_decimal<Int128, has_filter>(doris_column, data_type,
                                                                  select_vector);
            }
            break;
        case TypeIndex::Decimal128I:
            if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
                return _decode_binary_decimal<Int128, has_filter>(doris_column, data_type,
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

        return Status::InvalidArgument(
                "Can't decode parquet physical type {} to doris logical type {}",
                tparquet::to_string(_physical_type), getTypeName(logical_type));
    }

    Status skip_values(size_t num_values) override {
        _indexes.resize(num_values);
        _index_batch_decoder->GetBatch(&_indexes[0], num_values);
        return Status::OK();
    }

    Status set_dict(std::unique_ptr<uint8_t[]>& dict, int32_t length, size_t num_values) override {
        if (num_values * _type_length != length) {
            return Status::Corruption("Wrong dictionary data for fixed length type");
        }
        _dict = std::move(dict);
        char* dict_item_address = reinterpret_cast<char*>(_dict.get());
        _dict_items.resize(num_values);
        _dict_value_to_code.reserve(num_values);
        for (size_t i = 0; i < num_values; ++i) {
            _dict_items[i] = dict_item_address;
            _dict_value_to_code[StringRef(_dict_items[i], _type_length)] = i;
            dict_item_address += _type_length;
        }
        return Status::OK();
    }

    Status read_dict_values_to_column(MutableColumnPtr& doris_column) override {
        size_t dict_items_size = _dict_items.size();
        std::vector<StringRef> dict_values(dict_items_size);
        for (size_t i = 0; i < dict_items_size; ++i) {
            dict_values.emplace_back(_dict_items[i], _type_length);
        }
        doris_column->insert_many_strings(&dict_values[0], dict_items_size);
        return Status::OK();
    }

    Status get_dict_codes(const ColumnString* string_column,
                          std::vector<int32_t>* dict_codes) override {
        size_t size = string_column->size();
        dict_codes->reserve(size);
        for (int i = 0; i < size; ++i) {
            StringRef dict_value = string_column->get_data_at(i);
            dict_codes->emplace_back(_dict_value_to_code[dict_value]);
        }
        return Status::OK();
    }

    MutableColumnPtr convert_dict_column_to_string_column(const ColumnInt32* dict_column) override {
        auto res = ColumnString::create();
        std::vector<StringRef> dict_values(dict_column->size());
        const auto& data = dict_column->get_data();
        for (size_t i = 0; i < dict_column->size(); ++i) {
            dict_values.emplace_back(_dict_items[data[i]], _type_length);
        }
        res->insert_many_strings(&dict_values[0], dict_values.size());
        return res;
    }

protected:
    template <typename DecimalPrimitiveType, bool has_filter>
    Status _decode_binary_decimal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
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

    template <bool has_filter>
    Status _decode_string(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector) {
        size_t dict_index = 0;
        ColumnSelectVector::DataReadType read_type;
        while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                std::vector<StringRef> string_values;
                string_values.reserve(run_length);
                for (size_t i = 0; i < run_length; ++i) {
                    string_values.emplace_back(_dict_items[_indexes[dict_index++]], _type_length);
                }
                doris_column->insert_many_strings(&string_values[0], run_length);
                break;
            }
            case ColumnSelectVector::NULL_DATA: {
                doris_column->insert_many_defaults(run_length);
                break;
            }
            case ColumnSelectVector::FILTERED_CONTENT: {
                dict_index += run_length;
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

    tparquet::Type::type _physical_type;

    // For dictionary encoding
    std::vector<char*> _dict_items;
    std::unordered_map<StringRef, int32_t> _dict_value_to_code;

private:
    template <typename DecimalPrimitiveType, bool has_filter, int fixed_type_length,
              typename ValueCopyType, DecimalScaleParams::ScaleType ScaleType>
    Status _decode_binary_decimal_internal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                           ColumnSelectVector& select_vector) {
        auto& column_data =
                static_cast<ColumnDecimal<Decimal<DecimalPrimitiveType>>&>(*doris_column)
                        .get_data();
        size_t data_index = column_data.size();
        column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
        size_t dict_index = 0;
        DecimalScaleParams& scale_params = _decode_params->decimal_scale;

        ColumnSelectVector::DataReadType read_type;
        while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                for (size_t i = 0; i < run_length; ++i) {
                    char* buf_start = _dict_items[_indexes[dict_index++]];
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
                }
                break;
            }
            case ColumnSelectVector::NULL_DATA: {
                data_index += run_length;
                break;
            }
            case ColumnSelectVector::FILTERED_CONTENT: {
                dict_index += run_length;
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
};

} // namespace doris::vectorized
