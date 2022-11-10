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
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/common/int_exp.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exec/format/format_common.h"

namespace doris::vectorized {

using level_t = int16_t;

struct RowRange {
    RowRange() {}
    RowRange(int64_t first, int64_t last) : first_row(first), last_row(last) {}

    int64_t first_row;
    int64_t last_row;
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

    inline uint64_t to_timestamp_micros() const;

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

    uint16_t get_next_run(DataReadType* data_read_type);

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

    Status set_dict(std::unique_ptr<uint8_t[]>& dict, int32_t length, size_t num_values) override;

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

#define _FIXED_GET_DATA_OFFSET(index) \
    _has_dict ? _dict_items[_indexes[index]] : _data->data + _offset

#define _FIXED_SHIFT_DATA_OFFSET() \
    if (!_has_dict) _offset += _type_length

    tparquet::Type::type _physical_type;
    // For dictionary encoding
    bool _has_dict = false;
    std::unique_ptr<uint8_t[]> _dict = nullptr;
    std::vector<char*> _dict_items;
    std::unique_ptr<RleBatchDecoder<uint32_t>> _index_batch_decoder = nullptr;
    std::vector<uint32_t> _indexes;
};

template <typename Numeric>
Status FixLengthDecoder::_decode_numeric(MutableColumnPtr& doris_column,
                                         ColumnSelectVector& select_vector) {
    auto& column_data = static_cast<ColumnVector<Numeric>&>(*doris_column).get_data();
    size_t data_index = column_data.size();
    column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
    size_t dict_index = 0;
    ColumnSelectVector::DataReadType read_type;
    while (uint16_t run_length = select_vector.get_next_run(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (int i = 0; i < run_length; ++i) {
                char* buf_start = _FIXED_GET_DATA_OFFSET(dict_index++);
                column_data[data_index++] = *(Numeric*)buf_start;
                _FIXED_SHIFT_DATA_OFFSET();
            }
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            data_index += run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_CONTENT: {
            if (_has_dict) {
                dict_index += run_length;
            } else {
                _offset += _type_length * run_length;
            }
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
    size_t dict_index = 0;
    ColumnSelectVector::DataReadType read_type;
    while (uint16_t run_length = select_vector.get_next_run(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (int i = 0; i < run_length; ++i) {
                char* buf_start = _FIXED_GET_DATA_OFFSET(dict_index++);
                int64_t date_value = static_cast<int64_t>(*reinterpret_cast<int32_t*>(buf_start));
                auto& v = reinterpret_cast<CppType&>(column_data[data_index++]);
                v.from_unixtime(date_value * 24 * 60 * 60, *_decode_params->ctz); // day to seconds
                if constexpr (std::is_same_v<CppType, VecDateTimeValue>) {
                    // we should cast to date if using date v1.
                    v.cast_to_date();
                }
                _FIXED_SHIFT_DATA_OFFSET();
            }
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            data_index += run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_CONTENT: {
            if (_has_dict) {
                dict_index += run_length;
            } else {
                _offset += _type_length * run_length;
            }
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
    size_t dict_index = 0;
    int64_t scale_to_micro = _decode_params->scale_to_nano_factor / 1000;
    ColumnSelectVector::DataReadType read_type;
    while (uint16_t run_length = select_vector.get_next_run(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (int i = 0; i < run_length; ++i) {
                char* buf_start = _FIXED_GET_DATA_OFFSET(dict_index++);
                int64_t& date_value = *reinterpret_cast<int64_t*>(buf_start);
                auto& v = reinterpret_cast<CppType&>(column_data[data_index++]);
                v.from_unixtime(date_value / _decode_params->second_mask, *_decode_params->ctz);
                if constexpr (std::is_same_v<CppType, DateV2Value<DateTimeV2ValueType>>) {
                    // nanoseconds will be ignored.
                    v.set_microsecond((date_value % _decode_params->second_mask) * scale_to_micro);
                    // TODO: the precision of datetime v1
                }
                _FIXED_SHIFT_DATA_OFFSET();
            }
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            data_index += run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_CONTENT: {
            if (_has_dict) {
                dict_index += run_length;
            } else {
                _offset += _type_length * run_length;
            }
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
    size_t dict_index = 0;
    ColumnSelectVector::DataReadType read_type;
    while (uint16_t run_length = select_vector.get_next_run(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (int i = 0; i < run_length; ++i) {
                char* buf_start = _FIXED_GET_DATA_OFFSET(dict_index++);
                ParquetInt96& datetime96 = *reinterpret_cast<ParquetInt96*>(buf_start);
                auto& v = reinterpret_cast<CppType&>(column_data[data_index++]);
                int64_t micros = datetime96.to_timestamp_micros();
                v.from_unixtime(micros / 1000000, *_decode_params->ctz);
                if constexpr (std::is_same_v<CppType, DateV2Value<DateTimeV2ValueType>>) {
                    // spark.sql.parquet.outputTimestampType = INT96(NANOS) will lost precision.
                    // only keep microseconds.
                    v.set_microsecond(micros % 1000000);
                }
                _FIXED_SHIFT_DATA_OFFSET();
            }
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            data_index += run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_CONTENT: {
            if (_has_dict) {
                dict_index += run_length;
            } else {
                _offset += _type_length * run_length;
            }
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
    size_t dict_index = 0;
    DecimalScaleParams& scale_params = _decode_params->decimal_scale;

    ColumnSelectVector::DataReadType read_type;
    while (uint16_t run_length = select_vector.get_next_run(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (int i = 0; i < run_length; ++i) {
                char* buf_start = _FIXED_GET_DATA_OFFSET(dict_index++);
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
                _FIXED_SHIFT_DATA_OFFSET();
            }
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            data_index += run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_CONTENT: {
            if (_has_dict) {
                dict_index += run_length;
            } else {
                _offset += _type_length * run_length;
            }
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
    size_t dict_index = 0;
    DecimalScaleParams& scale_params = _decode_params->decimal_scale;

    ColumnSelectVector::DataReadType read_type;
    while (uint16_t run_length = select_vector.get_next_run(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (int i = 0; i < run_length; ++i) {
                char* buf_start = _FIXED_GET_DATA_OFFSET(dict_index++);
                // we should use decimal128 to scale up/down
                Int128 value = *reinterpret_cast<DecimalPhysicalType*>(buf_start);
                if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {
                    value *= scale_params.scale_factor;
                } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {
                    value /= scale_params.scale_factor;
                }
                auto& v = reinterpret_cast<DecimalPrimitiveType&>(column_data[data_index++]);
                v = (DecimalPrimitiveType)value;
                _FIXED_SHIFT_DATA_OFFSET();
            }
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            data_index += run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_CONTENT: {
            if (_has_dict) {
                dict_index += run_length;
            } else {
                _offset += _type_length * run_length;
            }
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

class ByteArrayDecoder final : public Decoder {
public:
    ByteArrayDecoder() = default;
    ~ByteArrayDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         ColumnSelectVector& select_vector) override;

    Status skip_values(size_t num_values) override;

    void set_data(Slice* data) override;

    Status set_dict(std::unique_ptr<uint8_t[]>& dict, int32_t length, size_t num_values) override;

protected:
    template <typename DecimalPrimitiveType>
    Status _decode_binary_decimal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                  ColumnSelectVector& select_vector);

    // For dictionary encoding
    bool _has_dict = false;
    std::unique_ptr<uint8_t[]> _dict = nullptr;
    std::vector<StringRef> _dict_items;
    std::unique_ptr<RleBatchDecoder<uint32_t>> _index_batch_decoder = nullptr;
    std::vector<uint32_t> _indexes;
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
    size_t dict_index = 0;
    DecimalScaleParams& scale_params = _decode_params->decimal_scale;
    ColumnSelectVector::DataReadType read_type;
    while (uint16_t run_length = select_vector.get_next_run(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            for (int i = 0; i < run_length; ++i) {
                char* buf_start;
                uint32_t length;
                if (_has_dict) {
                    StringRef& slice = _dict_items[_indexes[dict_index++]];
                    buf_start = const_cast<char*>(slice.data);
                    length = (uint32_t)slice.size;
                } else {
                    if (UNLIKELY(_offset + 4 > _data->size)) {
                        return Status::IOError("Can't read byte array length from plain decoder");
                    }
                    length = decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data->data) +
                                               _offset);
                    _offset += 4;
                    buf_start = _data->data + _offset;
                    _offset += length;
                }
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
            if (_has_dict) {
                dict_index += run_length;
            } else {
                _offset += _type_length * run_length;
            }
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
