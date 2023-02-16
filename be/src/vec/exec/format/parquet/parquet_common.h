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

#include <cstdint>

#include "common/status.h"
#include "gen_cpp/parquet_types.h"
#include "gutil/endian.h"
#include "schema_desc.h"
#include "util/bit_stream_utils.inline.h"
#include "util/coding.h"
#include "util/rle_encoding.h"
#include "util/simd/bits.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_dictionary.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/common/int_exp.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exec/format/format_common.h"

namespace doris::vectorized {

using level_t = int16_t;

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

struct RowRange {
    RowRange() = default;
    RowRange(int64_t first, int64_t last) : first_row(first), last_row(last) {}

    int64_t first_row;
    int64_t last_row;

    bool operator<(const RowRange& range) const { return first_row < range.first_row; }

    std::string debug_string() const {
        std::stringstream ss;
        ss << "[" << first_row << "," << last_row << ")";
        return ss.str();
    }
};

struct ParquetReadColumn {
    ParquetReadColumn(int parquet_col_id, const std::string& file_slot_name)
            : _parquet_col_id(parquet_col_id), _file_slot_name(file_slot_name) {};

    int _parquet_col_id;
    const std::string& _file_slot_name;
};

struct ParquetInt96 {
    uint64_t lo; // time of nanoseconds in a day
    uint32_t hi; // days from julian epoch

    inline uint64_t to_timestamp_micros() const {
        return (hi - JULIAN_EPOCH_OFFSET_DAYS) * MICROS_IN_DAY + lo / NANOS_PER_MICROSECOND;
    }

    static const uint32_t JULIAN_EPOCH_OFFSET_DAYS;
    static const uint64_t MICROS_IN_DAY;
    static const uint64_t NANOS_PER_MICROSECOND;
};

struct DecodeParams {
    // schema.logicalType.TIMESTAMP.isAdjustedToUTC == false
    static const cctz::time_zone utc0;
    // schema.logicalType.TIMESTAMP.isAdjustedToUTC == true, we should set the time zone
    cctz::time_zone* ctz = nullptr;
    int64_t second_mask = 1;
    int64_t scale_to_nano_factor = 1;
    DecimalScaleParams decimal_scale;
};

class ColumnSelectVector {
public:
    enum DataReadType : uint8_t { CONTENT = 0, NULL_DATA, FILTERED_CONTENT, FILTERED_NULL };

    ColumnSelectVector(const uint8_t* filter_map, size_t filter_map_size, bool filter_all);

    ColumnSelectVector() = default;

    void build(const uint8_t* filter_map, size_t filter_map_size, bool filter_all);

    const uint8_t* filter_map() { return _filter_map; }

    size_t num_values() const { return _num_values; }

    size_t num_nulls() const { return _num_nulls; }

    size_t num_filtered() const { return _num_filtered; }

    double filter_ratio() const { return _has_filter ? _filter_ratio : 0; }

    void fallback_filter() { _has_filter = false; }

    bool has_filter() const { return _has_filter; }

    bool can_filter_all(size_t remaining_num_values);

    bool filter_all() const { return _filter_all; }

    void skip(size_t num_values);

    void reset() {
        if (_has_filter) {
            _filter_map_index = 0;
        }
    }

    size_t get_next_run(DataReadType* data_read_type);

    void set_run_length_null_map(const std::vector<uint16_t>& run_length_null_map,
                                 size_t num_values, NullMap* null_map = nullptr);

private:
    std::vector<DataReadType> _data_map;
    // the length of non-null values and null values are arranged in turn.
    const std::vector<uint16_t>* _run_length_null_map;
    bool _has_filter = false;
    // only used when the whole batch is skipped
    bool _filter_all = false;
    const uint8_t* _filter_map = nullptr;
    size_t _filter_map_size = 0;
    double _filter_ratio = 0;
    size_t _filter_map_index = 0;

    // generated in set_run_length_null_map
    size_t _num_values;
    size_t _num_nulls;
    size_t _num_filtered;
    size_t _read_index;
};

class Decoder {
public:
    Decoder() = default;
    virtual ~Decoder() = default;

    static Status get_decoder(tparquet::Type::type type, tparquet::Encoding::type encoding,
                              std::unique_ptr<Decoder>& decoder);

    // The type with fix length
    void set_type_length(int32_t type_length) { _type_length = type_length; }

    // Set the data to be decoded
    virtual void set_data(Slice* data) {
        _data = data;
        _offset = 0;
    }

    void init(FieldSchema* field_schema, cctz::time_zone* ctz);

    template <typename DecimalPrimitiveType>
    void init_decimal_converter(DataTypePtr& data_type);

    // Write the decoded values batch to doris's column
    virtual Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                 ColumnSelectVector& select_vector) = 0;

    virtual Status skip_values(size_t num_values) = 0;

    virtual Status set_dict(std::unique_ptr<uint8_t[]>& dict, int32_t length, size_t num_values) {
        return Status::NotSupported("set_dict is not supported");
    }

protected:
    int32_t _type_length;
    Slice* _data = nullptr;
    uint32_t _offset = 0;
    FieldSchema* _field_schema = nullptr;
    std::unique_ptr<DecodeParams> _decode_params = nullptr;
};

template <typename DecimalPrimitiveType>
void Decoder::init_decimal_converter(DataTypePtr& data_type) {
    if (_decode_params == nullptr || _field_schema == nullptr ||
        _decode_params->decimal_scale.scale_type != DecimalScaleParams::NOT_INIT) {
        return;
    }
    auto scale = _field_schema->parquet_schema.scale;
    auto* decimal_type = reinterpret_cast<DataTypeDecimal<Decimal<DecimalPrimitiveType>>*>(
            const_cast<IDataType*>(remove_nullable(data_type).get()));
    auto dest_scale = decimal_type->get_scale();
    if (dest_scale > scale) {
        _decode_params->decimal_scale.scale_type = DecimalScaleParams::SCALE_UP;
        _decode_params->decimal_scale.scale_factor =
                DecimalScaleParams::get_scale_factor<DecimalPrimitiveType>(dest_scale - scale);
    } else if (dest_scale < scale) {
        _decode_params->decimal_scale.scale_type = DecimalScaleParams::SCALE_DOWN;
        _decode_params->decimal_scale.scale_factor =
                DecimalScaleParams::get_scale_factor<DecimalPrimitiveType>(scale - dest_scale);
    } else {
        _decode_params->decimal_scale.scale_type = DecimalScaleParams::NO_SCALE;
        _decode_params->decimal_scale.scale_factor = 1;
    }
}

class FixLengthDecoder final : public Decoder {
public:
    FixLengthDecoder(tparquet::Type::type physical_type) : _physical_type(physical_type) {};
    ~FixLengthDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector) override;

    Status skip_values(size_t num_values) override;

    void set_data(Slice* data) override;

protected:
    template <typename Numeric>
    Status _decode_numeric(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector);

    template <typename CppType, typename ColumnType>
    Status _decode_date(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector);

    template <typename CppType, typename ColumnType>
    Status _decode_datetime64(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector);

    template <typename CppType, typename ColumnType>
    Status _decode_datetime96(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector);

    template <typename DecimalPrimitiveType>
    Status _decode_binary_decimal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                  ColumnSelectVector& select_vector);

    template <typename DecimalPrimitiveType, typename DecimalPhysicalType>
    Status _decode_primitive_decimal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                     ColumnSelectVector& select_vector);

    Status _decode_string(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector);

    tparquet::Type::type _physical_type;
};

template <typename Numeric>
Status FixLengthDecoder::_decode_numeric(MutableColumnPtr& doris_column,
                                         ColumnSelectVector& select_vector) {
    auto& column_data = static_cast<ColumnVector<Numeric>&>(*doris_column).get_data();
    size_t data_index = column_data.size();
    column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (size_t i = 0; i < run_length; ++i) {
                char* buf_start = _data->data + _offset;
                column_data[data_index++] = *(Numeric*)buf_start;
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

template <typename CppType, typename ColumnType>
Status FixLengthDecoder::_decode_date(MutableColumnPtr& doris_column,
                                      ColumnSelectVector& select_vector) {
    auto& column_data = static_cast<ColumnVector<ColumnType>&>(*doris_column).get_data();
    size_t data_index = column_data.size();
    column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (size_t i = 0; i < run_length; ++i) {
                char* buf_start = _data->data + _offset;
                int64_t date_value = static_cast<int64_t>(*reinterpret_cast<int32_t*>(buf_start));
                auto& v = reinterpret_cast<CppType&>(column_data[data_index++]);
                v.from_unixtime(date_value * 24 * 60 * 60, *_decode_params->ctz); // day to seconds
                if constexpr (std::is_same_v<CppType, VecDateTimeValue>) {
                    // we should cast to date if using date v1.
                    v.cast_to_date();
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

template <typename CppType, typename ColumnType>
Status FixLengthDecoder::_decode_datetime64(MutableColumnPtr& doris_column,
                                            ColumnSelectVector& select_vector) {
    auto& column_data = static_cast<ColumnVector<ColumnType>&>(*doris_column).get_data();
    size_t data_index = column_data.size();
    column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run(&read_type)) {
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

template <typename CppType, typename ColumnType>
Status FixLengthDecoder::_decode_datetime96(MutableColumnPtr& doris_column,
                                            ColumnSelectVector& select_vector) {
    auto& column_data = static_cast<ColumnVector<ColumnType>&>(*doris_column).get_data();
    size_t data_index = column_data.size();
    column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run(&read_type)) {
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

template <typename DecimalPrimitiveType>
Status FixLengthDecoder::_decode_binary_decimal(MutableColumnPtr& doris_column,
                                                DataTypePtr& data_type,
                                                ColumnSelectVector& select_vector) {
    init_decimal_converter<DecimalPrimitiveType>(data_type);
    auto& column_data =
            static_cast<ColumnDecimal<Decimal<DecimalPrimitiveType>>&>(*doris_column).get_data();
    size_t data_index = column_data.size();
    column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
    DecimalScaleParams& scale_params = _decode_params->decimal_scale;

    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (size_t i = 0; i < run_length; ++i) {
                char* buf_start = _data->data + _offset;
                // When Decimal in parquet is stored in byte arrays, binary and fixed,
                // the unscaled number must be encoded as two's complement using big-endian byte order.
                Int128 value = buf_start[0] & 0x80 ? -1 : 0;
                memcpy(reinterpret_cast<char*>(&value) + sizeof(Int128) - _type_length, buf_start,
                       _type_length);
                value = BigEndian::ToHost128(value);
                if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {
                    value *= scale_params.scale_factor;
                } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {
                    value /= scale_params.scale_factor;
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

template <typename DecimalPrimitiveType, typename DecimalPhysicalType>
Status FixLengthDecoder::_decode_primitive_decimal(MutableColumnPtr& doris_column,
                                                   DataTypePtr& data_type,
                                                   ColumnSelectVector& select_vector) {
    init_decimal_converter<DecimalPrimitiveType>(data_type);
    auto& column_data =
            static_cast<ColumnDecimal<Decimal<DecimalPrimitiveType>>&>(*doris_column).get_data();
    size_t data_index = column_data.size();
    column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
    DecimalScaleParams& scale_params = _decode_params->decimal_scale;

    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (size_t i = 0; i < run_length; ++i) {
                char* buf_start = _data->data + _offset;
                // we should use decimal128 to scale up/down
                Int128 value = *reinterpret_cast<DecimalPhysicalType*>(buf_start);
                if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {
                    value *= scale_params.scale_factor;
                } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {
                    value /= scale_params.scale_factor;
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

template <typename T>
class FixLengthDictDecoder final : public Decoder {
public:
    FixLengthDictDecoder(tparquet::Type::type physical_type) : _physical_type(physical_type) {};
    ~FixLengthDictDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector) override {
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
        _indexes.resize(non_null_size);
        _index_batch_decoder->GetBatch(&_indexes[0], non_null_size);

        if (doris_column->is_column_dictionary()) {
            return _decode_dict_values(doris_column, select_vector);
        }

        TypeIndex logical_type = remove_nullable(data_type)->get_type_id();
        switch (logical_type) {
#define DISPATCH(NUMERIC_TYPE, CPP_NUMERIC_TYPE, PHYSICAL_TYPE)                    \
    case NUMERIC_TYPE:                                                             \
        if constexpr (std::is_same_v<T, PHYSICAL_TYPE>) {                          \
            return _decode_numeric<CPP_NUMERIC_TYPE>(doris_column, select_vector); \
        }
            FOR_LOGICAL_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
        case TypeIndex::Date:
            if constexpr (std::is_same_v<T, Int32>) {
                return _decode_date<VecDateTimeValue, Int64>(doris_column, select_vector);
            }
            break;
        case TypeIndex::DateV2:
            if constexpr (std::is_same_v<T, Int32>) {
                return _decode_date<DateV2Value<DateV2ValueType>, UInt32>(doris_column,
                                                                          select_vector);
            }
            break;
        case TypeIndex::DateTime:
            if constexpr (std::is_same_v<T, ParquetInt96>) {
                return _decode_datetime96<VecDateTimeValue, Int64>(doris_column, select_vector);
            } else if constexpr (std::is_same_v<T, Int64>) {
                return _decode_datetime64<VecDateTimeValue, Int64>(doris_column, select_vector);
            }
            break;
        case TypeIndex::DateTimeV2:
            // Spark can set the timestamp precision by the following configuration:
            // spark.sql.parquet.outputTimestampType = INT96(NANOS), TIMESTAMP_MICROS, TIMESTAMP_MILLIS
            if constexpr (std::is_same_v<T, ParquetInt96>) {
                return _decode_datetime96<DateV2Value<DateTimeV2ValueType>, UInt64>(doris_column,
                                                                                    select_vector);
            } else if constexpr (std::is_same_v<T, Int64>) {
                return _decode_datetime64<DateV2Value<DateTimeV2ValueType>, UInt64>(doris_column,
                                                                                    select_vector);
            }
            break;
        case TypeIndex::Decimal32:
            if constexpr (std::is_same_v<T, Int32>) {
                return _decode_primitive_decimal<Int32, Int32>(doris_column, data_type,
                                                               select_vector);
            } else if constexpr (std::is_same_v<T, Int64>) {
                return _decode_primitive_decimal<Int32, Int64>(doris_column, data_type,
                                                               select_vector);
            }
            break;
        case TypeIndex::Decimal64:
            if constexpr (std::is_same_v<T, Int32>) {
                return _decode_primitive_decimal<Int64, Int32>(doris_column, data_type,
                                                               select_vector);
            } else if constexpr (std::is_same_v<T, Int64>) {
                return _decode_primitive_decimal<Int64, Int64>(doris_column, data_type,
                                                               select_vector);
            }
            break;
        case TypeIndex::Decimal128:
            if constexpr (std::is_same_v<T, Int32>) {
                return _decode_primitive_decimal<Int128, Int32>(doris_column, data_type,
                                                                select_vector);
            } else if constexpr (std::is_same_v<T, Int64>) {
                return _decode_primitive_decimal<Int128, Int64>(doris_column, data_type,
                                                                select_vector);
            }
            break;
        case TypeIndex::Decimal128I:
            if constexpr (std::is_same_v<T, Int32>) {
                return _decode_primitive_decimal<Int128, Int32>(doris_column, data_type,
                                                                select_vector);
            } else if constexpr (std::is_same_v<T, Int64>) {
                return _decode_primitive_decimal<Int128, Int64>(doris_column, data_type,
                                                                select_vector);
            }
            break;
        case TypeIndex::String:
        case TypeIndex::FixedString:
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
        for (size_t i = 0; i < num_values; ++i) {
            _dict_items[i] = *(T*)dict_item_address;
            dict_item_address += _type_length;
        }
        return Status::OK();
    }

    void set_data(Slice* data) override {
        _data = data;
        _offset = 0;
        uint8_t bit_width = *data->data;
        _index_batch_decoder.reset(
                new RleBatchDecoder<uint32_t>(reinterpret_cast<uint8_t*>(data->data) + 1,
                                              static_cast<int>(data->size) - 1, bit_width));
    }

protected:
    template <typename Numeric>
    Status _decode_numeric(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector) {
        auto& column_data = static_cast<ColumnVector<Numeric>&>(*doris_column).get_data();
        size_t data_index = column_data.size();
        column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
        size_t dict_index = 0;
        ColumnSelectVector::DataReadType read_type;
        while (size_t run_length = select_vector.get_next_run(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                for (size_t i = 0; i < run_length; ++i) {
                    column_data[data_index++] =
                            static_cast<Numeric>(_dict_items[_indexes[dict_index++]]);
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

    template <typename CppType, typename ColumnType>
    Status _decode_date(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector) {
        auto& column_data = static_cast<ColumnVector<ColumnType>&>(*doris_column).get_data();
        size_t data_index = column_data.size();
        column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
        size_t dict_index = 0;
        ColumnSelectVector::DataReadType read_type;
        while (size_t run_length = select_vector.get_next_run(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                for (size_t i = 0; i < run_length; ++i) {
                    int64_t date_value = _dict_items[_indexes[dict_index++]];
                    auto& v = reinterpret_cast<CppType&>(column_data[data_index++]);
                    v.from_unixtime(date_value * 24 * 60 * 60,
                                    *_decode_params->ctz); // day to seconds
                    if constexpr (std::is_same_v<CppType, VecDateTimeValue>) {
                        // we should cast to date if using date v1.
                        v.cast_to_date();
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

    template <typename CppType, typename ColumnType>
    Status _decode_datetime64(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector) {
        auto& column_data = static_cast<ColumnVector<ColumnType>&>(*doris_column).get_data();
        size_t data_index = column_data.size();
        column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
        size_t dict_index = 0;
        ColumnSelectVector::DataReadType read_type;
        while (size_t run_length = select_vector.get_next_run(&read_type)) {
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

    template <typename CppType, typename ColumnType>
    Status _decode_datetime96(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector) {
        auto& column_data = static_cast<ColumnVector<ColumnType>&>(*doris_column).get_data();
        size_t data_index = column_data.size();
        column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
        size_t dict_index = 0;
        ColumnSelectVector::DataReadType read_type;
        while (size_t run_length = select_vector.get_next_run(&read_type)) {
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

    template <typename DecimalPrimitiveType, typename DecimalPhysicalType>
    Status _decode_primitive_decimal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                     ColumnSelectVector& select_vector) {
        init_decimal_converter<DecimalPrimitiveType>(data_type);
        auto& column_data =
                static_cast<ColumnDecimal<Decimal<DecimalPrimitiveType>>&>(*doris_column)
                        .get_data();
        size_t data_index = column_data.size();
        column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
        size_t dict_index = 0;
        DecimalScaleParams& scale_params = _decode_params->decimal_scale;

        ColumnSelectVector::DataReadType read_type;
        while (size_t run_length = select_vector.get_next_run(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                for (size_t i = 0; i < run_length; ++i) {
                    // we should use decimal128 to scale up/down
                    Int128 value = static_cast<Int128>(_dict_items[_indexes[dict_index++]]);
                    if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {
                        value *= scale_params.scale_factor;
                    } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {
                        value /= scale_params.scale_factor;
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

    /**
     * Decode dictionary-coded values into doris_column, ensure that doris_column is ColumnDictI32 type,
     * and the coded values must be read into _indexes previously.
     */
    Status _decode_dict_values(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector) {
        DCHECK(doris_column->is_column_dictionary());
        size_t dict_index = 0;
        ColumnSelectVector::DataReadType read_type;
        auto& column_data = assert_cast<ColumnDictI32&>(*doris_column).get_data();
        while (size_t run_length = select_vector.get_next_run(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                uint32_t* start_index = &_indexes[0];
                column_data.insert(start_index + dict_index, start_index + dict_index + run_length);
                dict_index += run_length;
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
                break;
            }
            }
        }
        return Status::OK();
    }

    tparquet::Type::type _physical_type;

    // For dictionary encoding
    std::vector<T> _dict_items;
    std::unique_ptr<uint8_t[]> _dict = nullptr;
    std::unique_ptr<RleBatchDecoder<uint32_t>> _index_batch_decoder = nullptr;
    std::vector<uint32_t> _indexes;
};

template <>
class FixLengthDictDecoder<char*> final : public Decoder {
public:
    FixLengthDictDecoder(tparquet::Type::type physical_type) : _physical_type(physical_type) {};
    ~FixLengthDictDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector) override {
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

        if (doris_column->is_column_dictionary()) {
            return _decode_dict_values(doris_column, select_vector);
        }

        TypeIndex logical_type = remove_nullable(data_type)->get_type_id();
        switch (logical_type) {
        case TypeIndex::Decimal32:
            if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
                return _decode_binary_decimal<Int32>(doris_column, data_type, select_vector);
            }
            break;
        case TypeIndex::Decimal64:
            if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
                return _decode_binary_decimal<Int64>(doris_column, data_type, select_vector);
            }
            break;
        case TypeIndex::Decimal128:
            if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
                return _decode_binary_decimal<Int128>(doris_column, data_type, select_vector);
            }
            break;
        case TypeIndex::Decimal128I:
            if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
                return _decode_binary_decimal<Int128>(doris_column, data_type, select_vector);
            }
            break;
        case TypeIndex::String:
        case TypeIndex::FixedString:
            if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
                return _decode_string(doris_column, select_vector);
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
        for (size_t i = 0; i < num_values; ++i) {
            _dict_items[i] = dict_item_address;
            dict_item_address += _type_length;
        }
        return Status::OK();
    }

    void set_data(Slice* data) override {
        _data = data;
        _offset = 0;
        uint8_t bit_width = *data->data;
        _index_batch_decoder.reset(
                new RleBatchDecoder<uint32_t>(reinterpret_cast<uint8_t*>(data->data) + 1,
                                              static_cast<int>(data->size) - 1, bit_width));
    }

protected:
    template <typename DecimalPrimitiveType>
    Status _decode_binary_decimal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                  ColumnSelectVector& select_vector) {
        init_decimal_converter<DecimalPrimitiveType>(data_type);
        auto& column_data =
                static_cast<ColumnDecimal<Decimal<DecimalPrimitiveType>>&>(*doris_column)
                        .get_data();
        size_t data_index = column_data.size();
        column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
        size_t dict_index = 0;
        DecimalScaleParams& scale_params = _decode_params->decimal_scale;

        ColumnSelectVector::DataReadType read_type;
        while (size_t run_length = select_vector.get_next_run(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                for (size_t i = 0; i < run_length; ++i) {
                    char* buf_start = _dict_items[_indexes[dict_index++]];
                    // When Decimal in parquet is stored in byte arrays, binary and fixed,
                    // the unscaled number must be encoded as two's complement using big-endian byte order.
                    Int128 value = buf_start[0] & 0x80 ? -1 : 0;
                    memcpy(reinterpret_cast<char*>(&value) + sizeof(Int128) - _type_length,
                           buf_start, _type_length);
                    value = BigEndian::ToHost128(value);
                    if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {
                        value *= scale_params.scale_factor;
                    } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {
                        value /= scale_params.scale_factor;
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

    Status _decode_string(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector) {
        size_t dict_index = 0;
        ColumnSelectVector::DataReadType read_type;
        while (size_t run_length = select_vector.get_next_run(&read_type)) {
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

    /**
     * Decode dictionary-coded values into doris_column, ensure that doris_column is ColumnDictI32 type,
     * and the coded values must be read into _indexes previously.
     */
    Status _decode_dict_values(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector) {
        DCHECK(doris_column->is_column_dictionary());
        size_t dict_index = 0;
        ColumnSelectVector::DataReadType read_type;
        auto& column_data = assert_cast<ColumnDictI32&>(*doris_column).get_data();
        while (size_t run_length = select_vector.get_next_run(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                uint32_t* start_index = &_indexes[0];
                column_data.insert(start_index + dict_index, start_index + dict_index + run_length);
                dict_index += run_length;
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
                break;
            }
            }
        }
        return Status::OK();
    }

    tparquet::Type::type _physical_type;

    // For dictionary encoding
    std::vector<char*> _dict_items;
    std::unique_ptr<uint8_t[]> _dict = nullptr;
    std::unique_ptr<RleBatchDecoder<uint32_t>> _index_batch_decoder = nullptr;
    std::vector<uint32_t> _indexes;
};

class ByteArrayDecoder final : public Decoder {
public:
    ByteArrayDecoder() = default;
    ~ByteArrayDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector) override;

    Status skip_values(size_t num_values) override;

    void set_data(Slice* data) override;

protected:
    template <typename DecimalPrimitiveType>
    Status _decode_binary_decimal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                  ColumnSelectVector& select_vector);
};

template <typename DecimalPrimitiveType>
Status ByteArrayDecoder::_decode_binary_decimal(MutableColumnPtr& doris_column,
                                                DataTypePtr& data_type,
                                                ColumnSelectVector& select_vector) {
    init_decimal_converter<DecimalPrimitiveType>(data_type);
    auto& column_data =
            static_cast<ColumnDecimal<Decimal<DecimalPrimitiveType>>&>(*doris_column).get_data();
    size_t data_index = column_data.size();
    column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
    DecimalScaleParams& scale_params = _decode_params->decimal_scale;
    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (size_t i = 0; i < run_length; ++i) {
                if (UNLIKELY(_offset + 4 > _data->size)) {
                    return Status::IOError("Can't read byte array length from plain decoder");
                }
                uint32_t length =
                        decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data->data) + _offset);
                _offset += 4;
                char* buf_start = _data->data + _offset;
                _offset += length;
                // When Decimal in parquet is stored in byte arrays, binary and fixed,
                // the unscaled number must be encoded as two's complement using big-endian byte order.
                Int128 value = buf_start[0] & 0x80 ? -1 : 0;
                memcpy(reinterpret_cast<char*>(&value) + sizeof(Int128) - length, buf_start,
                       length);
                value = BigEndian::ToHost128(value);
                if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {
                    value *= scale_params.scale_factor;
                } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {
                    value /= scale_params.scale_factor;
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

class ByteArrayDictDecoder final : public Decoder {
public:
    ByteArrayDictDecoder() = default;
    ~ByteArrayDictDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector) override;

    Status skip_values(size_t num_values) override;

    void set_data(Slice* data) override;

    Status set_dict(std::unique_ptr<uint8_t[]>& dict, int32_t length, size_t num_values) override;

protected:
    template <typename DecimalPrimitiveType>
    Status _decode_binary_decimal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                  ColumnSelectVector& select_vector);
    /**
     * Decode dictionary-coded values into doris_column, ensure that doris_column is ColumnDictI32 type,
     * and the coded values must be read into _indexes previously.
     */
    Status _decode_dict_values(MutableColumnPtr& doris_column, ColumnSelectVector& select_vector);

    // For dictionary encoding
    std::unique_ptr<uint8_t[]> _dict = nullptr;
    std::vector<StringRef> _dict_items;
    std::vector<uint8_t> _dict_data;
    std::unique_ptr<RleBatchDecoder<uint32_t>> _index_batch_decoder = nullptr;
    std::vector<uint32_t> _indexes;
    size_t _max_value_length;
};

template <typename DecimalPrimitiveType>
Status ByteArrayDictDecoder::_decode_binary_decimal(MutableColumnPtr& doris_column,
                                                    DataTypePtr& data_type,
                                                    ColumnSelectVector& select_vector) {
    init_decimal_converter<DecimalPrimitiveType>(data_type);
    auto& column_data =
            static_cast<ColumnDecimal<Decimal<DecimalPrimitiveType>>&>(*doris_column).get_data();
    size_t data_index = column_data.size();
    column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
    size_t dict_index = 0;
    DecimalScaleParams& scale_params = _decode_params->decimal_scale;
    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (size_t i = 0; i < run_length; ++i) {
                StringRef& slice = _dict_items[_indexes[dict_index++]];
                char* buf_start = const_cast<char*>(slice.data);
                uint32_t length = (uint32_t)slice.size;
                // When Decimal in parquet is stored in byte arrays, binary and fixed,
                // the unscaled number must be encoded as two's complement using big-endian byte order.
                Int128 value = buf_start[0] & 0x80 ? -1 : 0;
                memcpy(reinterpret_cast<char*>(&value) + sizeof(Int128) - length, buf_start,
                       length);
                value = BigEndian::ToHost128(value);
                if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {
                    value *= scale_params.scale_factor;
                } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {
                    value /= scale_params.scale_factor;
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

/// Decoder bit-packed boolean-encoded values.
/// Implementation from https://github.com/apache/impala/blob/master/be/src/exec/parquet/parquet-bool-decoder.h
class BoolPlainDecoder final : public Decoder {
public:
    BoolPlainDecoder() = default;
    ~BoolPlainDecoder() override = default;

    // Set the data to be decoded
    void set_data(Slice* data) override {
        bool_values_.Reset((const uint8_t*)data->data, data->size);
        num_unpacked_values_ = 0;
        unpacked_value_idx_ = 0;
        _offset = 0;
    }

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector) override;

    Status skip_values(size_t num_values) override;

protected:
    inline bool _decode_value(bool* value) {
        if (LIKELY(unpacked_value_idx_ < num_unpacked_values_)) {
            *value = unpacked_values_[unpacked_value_idx_++];
        } else {
            num_unpacked_values_ =
                    bool_values_.UnpackBatch(1, UNPACKED_BUFFER_LEN, &unpacked_values_[0]);
            if (UNLIKELY(num_unpacked_values_ == 0)) {
                return false;
            }
            *value = unpacked_values_[0];
            unpacked_value_idx_ = 1;
        }
        return true;
    }

    /// A buffer to store unpacked values. Must be a multiple of 32 size to use the
    /// batch-oriented interface of BatchedBitReader. We use uint8_t instead of bool because
    /// bit unpacking is only supported for unsigned integers. The values are converted to
    /// bool when returned to the user.
    static const int UNPACKED_BUFFER_LEN = 128;
    uint8_t unpacked_values_[UNPACKED_BUFFER_LEN];

    /// The number of valid values in 'unpacked_values_'.
    int num_unpacked_values_ = 0;

    /// The next value to return from 'unpacked_values_'.
    int unpacked_value_idx_ = 0;

    /// Bit packed decoder, used if 'encoding_' is PLAIN.
    BatchedBitReader bool_values_;
};

} // namespace doris::vectorized
