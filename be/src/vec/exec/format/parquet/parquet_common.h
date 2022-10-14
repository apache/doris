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
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/common/int_exp.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"

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

struct DecimalScaleParams {
    enum ScaleType {
        NOT_INIT,
        NO_SCALE,
        SCALE_UP,
        SCALE_DOWN,
    };
    ScaleType scale_type = ScaleType::NOT_INIT;
    int32_t scale_factor = 1;

    template <typename DecimalPrimitiveType>
    static inline constexpr DecimalPrimitiveType get_scale_factor(int32_t n) {
        if constexpr (std::is_same_v<DecimalPrimitiveType, Int32>) {
            return common::exp10_i32(n);
        } else if constexpr (std::is_same_v<DecimalPrimitiveType, Int64>) {
            return common::exp10_i64(n);
        } else if constexpr (std::is_same_v<DecimalPrimitiveType, Int128>) {
            return common::exp10_i128(n);
        } else {
            return DecimalPrimitiveType(1);
        }
    }
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
    Status decode_values(ColumnPtr& doris_column, DataTypePtr& data_type, size_t num_values);

    virtual Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                 size_t num_values) = 0;

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
                         size_t num_values) override;

    Status skip_values(size_t num_values) override;

    Status set_dict(std::unique_ptr<uint8_t[]>& dict, int32_t length, size_t num_values) override;

    void set_data(Slice* data) override;

protected:
    template <typename ShortIntType>
    Status _decode_short_int(MutableColumnPtr& doris_column, size_t num_values);

    template <typename Numeric>
    Status _decode_numeric(MutableColumnPtr& doris_column, size_t num_values);

    template <typename CppType, typename ColumnType>
    Status _decode_date(MutableColumnPtr& doris_column, TypeIndex& logical_type, size_t num_values);

    template <typename CppType, typename ColumnType>
    Status _decode_datetime64(MutableColumnPtr& doris_column, TypeIndex& logical_type,
                              size_t num_values);

    template <typename CppType, typename ColumnType>
    Status _decode_datetime96(MutableColumnPtr& doris_column, TypeIndex& logical_type,
                              size_t num_values);

    template <typename DecimalPrimitiveType>
    Status _decode_binary_decimal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                  size_t num_values);

    template <typename DecimalPrimitiveType, typename DecimalPhysicalType>
    Status _decode_primitive_decimal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                     size_t num_values);

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

template <typename ShortIntType>
Status FixLengthDecoder::_decode_short_int(MutableColumnPtr& doris_column, size_t num_values) {
    if (UNLIKELY(_physical_type != tparquet::Type::INT32)) {
        return Status::InternalError("Short int can only be decoded from INT32");
    }
    auto& column_data = static_cast<ColumnVector<ShortIntType>&>(*doris_column).get_data();
    auto origin_size = column_data.size();
    column_data.resize(origin_size + num_values);
    for (int i = 0; i < num_values; ++i) {
        char* buf_start = _FIXED_GET_DATA_OFFSET(i);
        column_data[origin_size + i] = *(ShortIntType*)buf_start;
        _FIXED_SHIFT_DATA_OFFSET();
    }
    return Status::OK();
}

template <typename Numeric>
Status FixLengthDecoder::_decode_numeric(MutableColumnPtr& doris_column, size_t num_values) {
    auto& column_data = static_cast<ColumnVector<Numeric>&>(*doris_column).get_data();
    if (_has_dict) {
        auto origin_size = column_data.size();
        column_data.resize(origin_size + num_values);
        for (int i = 0; i < num_values; ++i) {
            char* buf_start = _FIXED_GET_DATA_OFFSET(i);
            column_data[origin_size + i] = *(Numeric*)buf_start;
        }
    } else {
        const auto* raw_data = reinterpret_cast<const Numeric*>(_data->data + _offset);
        column_data.insert(raw_data, raw_data + num_values);
        _offset += _type_length * num_values;
    }
    return Status::OK();
}

template <typename CppType, typename ColumnType>
Status FixLengthDecoder::_decode_date(MutableColumnPtr& doris_column, TypeIndex& logical_type,
                                      size_t num_values) {
    auto& column_data = static_cast<ColumnVector<ColumnType>&>(*doris_column).get_data();
    auto origin_size = column_data.size();
    column_data.resize(origin_size + num_values);
    for (int i = 0; i < num_values; ++i) {
        char* buf_start = _FIXED_GET_DATA_OFFSET(i);
        int64_t date_value = static_cast<int64_t>(*reinterpret_cast<int32_t*>(buf_start));
        auto& v = reinterpret_cast<CppType&>(column_data[origin_size + i]);
        v.from_unixtime(date_value * 24 * 60 * 60, *_decode_params->ctz); // day to seconds
        if constexpr (std::is_same_v<CppType, VecDateTimeValue>) {
            // we should cast to date if using date v1.
            v.cast_to_date();
        }
        _FIXED_SHIFT_DATA_OFFSET();
    }
    return Status::OK();
}

template <typename CppType, typename ColumnType>
Status FixLengthDecoder::_decode_datetime64(MutableColumnPtr& doris_column, TypeIndex& logical_type,
                                            size_t num_values) {
    auto& column_data = static_cast<ColumnVector<ColumnType>&>(*doris_column).get_data();
    auto origin_size = column_data.size();
    column_data.resize(origin_size + num_values);
    int64_t scale_to_micro = _decode_params->scale_to_nano_factor / 1000;
    for (int i = 0; i < num_values; i++) {
        char* buf_start = _FIXED_GET_DATA_OFFSET(i);
        int64_t& date_value = *reinterpret_cast<int64_t*>(buf_start);
        auto& v = reinterpret_cast<CppType&>(column_data[origin_size + i]);
        v.from_unixtime(date_value / _decode_params->second_mask, *_decode_params->ctz);
        if constexpr (std::is_same_v<CppType, DateV2Value<DateTimeV2ValueType>>) {
            // nanoseconds will be ignored.
            v.set_microsecond((date_value % _decode_params->second_mask) * scale_to_micro);
        }
        _FIXED_SHIFT_DATA_OFFSET();
    }
    return Status::OK();
}

template <typename CppType, typename ColumnType>
Status FixLengthDecoder::_decode_datetime96(MutableColumnPtr& doris_column, TypeIndex& logical_type,
                                            size_t num_values) {
    auto& column_data = static_cast<ColumnVector<ColumnType>&>(*doris_column).get_data();
    auto origin_size = column_data.size();
    column_data.resize(origin_size + num_values);
    for (int i = 0; i < num_values; ++i) {
        char* buf_start = _FIXED_GET_DATA_OFFSET(i);
        ParquetInt96& datetime96 = *reinterpret_cast<ParquetInt96*>(buf_start);
        auto& v = reinterpret_cast<CppType&>(column_data[origin_size + i]);
        int64_t micros = datetime96.to_timestamp_micros();
        v.from_unixtime(micros / 1000000, *_decode_params->ctz);
        if constexpr (std::is_same_v<CppType, DateV2Value<DateTimeV2ValueType>>) {
            // spark.sql.parquet.outputTimestampType = INT96(NANOS) will lost precision.
            // only keep microseconds.
            v.set_microsecond(micros % 1000000);
        }
        _FIXED_SHIFT_DATA_OFFSET();
    }
    return Status::OK();
}

template <typename DecimalPrimitiveType>
Status FixLengthDecoder::_decode_binary_decimal(MutableColumnPtr& doris_column,
                                                DataTypePtr& data_type, size_t num_values) {
    init_decimal_converter<DecimalPrimitiveType>(data_type);
    auto& column_data =
            static_cast<ColumnDecimal<Decimal<DecimalPrimitiveType>>&>(*doris_column).get_data();
    auto origin_size = column_data.size();
    column_data.resize(origin_size + num_values);
    DecimalScaleParams& scale_params = _decode_params->decimal_scale;
    for (int i = 0; i < num_values; ++i) {
        char* buf_start = _FIXED_GET_DATA_OFFSET(i);
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
        auto& v = reinterpret_cast<DecimalPrimitiveType&>(column_data[origin_size + i]);
        v = (DecimalPrimitiveType)value;
        _FIXED_SHIFT_DATA_OFFSET();
    }
    return Status::OK();
}

template <typename DecimalPrimitiveType, typename DecimalPhysicalType>
Status FixLengthDecoder::_decode_primitive_decimal(MutableColumnPtr& doris_column,
                                                   DataTypePtr& data_type, size_t num_values) {
    init_decimal_converter<DecimalPrimitiveType>(data_type);
    auto& column_data =
            static_cast<ColumnDecimal<Decimal<DecimalPrimitiveType>>&>(*doris_column).get_data();
    auto origin_size = column_data.size();
    column_data.resize(origin_size + num_values);
    DecimalScaleParams& scale_params = _decode_params->decimal_scale;
    for (int i = 0; i < num_values; ++i) {
        char* buf_start = _FIXED_GET_DATA_OFFSET(i);
        // we should use decimal128 to scale up/down
        Int128 value = *reinterpret_cast<DecimalPhysicalType*>(buf_start);
        if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {
            value *= scale_params.scale_factor;
        } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {
            value /= scale_params.scale_factor;
        }
        auto& v = reinterpret_cast<DecimalPrimitiveType&>(column_data[origin_size + i]);
        v = (DecimalPrimitiveType)value;
        _FIXED_SHIFT_DATA_OFFSET();
    }
    return Status::OK();
}

class ByteArrayDecoder final : public Decoder {
public:
    ByteArrayDecoder() = default;
    ~ByteArrayDecoder() override = default;

    Status decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                         size_t num_values) override;

    Status skip_values(size_t num_values) override;

    void set_data(Slice* data) override;

    Status set_dict(std::unique_ptr<uint8_t[]>& dict, int32_t length, size_t num_values) override;

protected:
    template <typename DecimalPrimitiveType>
    Status _decode_binary_decimal(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                  size_t num_values);

    // For dictionary encoding
    bool _has_dict = false;
    std::unique_ptr<uint8_t[]> _dict = nullptr;
    std::vector<StringRef> _dict_items;
    std::unique_ptr<RleBatchDecoder<uint32_t>> _index_batch_decoder = nullptr;
    std::vector<uint32_t> _indexes;
};

template <typename DecimalPrimitiveType>
Status ByteArrayDecoder::_decode_binary_decimal(MutableColumnPtr& doris_column,
                                                DataTypePtr& data_type, size_t num_values) {
    init_decimal_converter<DecimalPrimitiveType>(data_type);
    auto& column_data =
            static_cast<ColumnDecimal<Decimal<DecimalPrimitiveType>>&>(*doris_column).get_data();
    auto origin_size = column_data.size();
    column_data.resize(origin_size + num_values);
    DecimalScaleParams& scale_params = _decode_params->decimal_scale;
    for (int i = 0; i < num_values; ++i) {
        char* buf_start;
        uint32_t length;
        if (_has_dict) {
            StringRef& slice = _dict_items[_indexes[i]];
            buf_start = const_cast<char*>(slice.data);
            length = (uint32_t)slice.size;
        } else {
            if (UNLIKELY(_offset + 4 > _data->size)) {
                return Status::IOError("Can't read byte array length from plain decoder");
            }
            length = decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data->data) + _offset);
            _offset += 4;
            buf_start = _data->data + _offset;
            _offset += length;
        }
        // When Decimal in parquet is stored in byte arrays, binary and fixed,
        // the unscaled number must be encoded as two's complement using big-endian byte order.
        Int128 value = buf_start[0] & 0x80 ? -1 : 0;
        memcpy(reinterpret_cast<char*>(&value) + sizeof(Int128) - length, buf_start, length);
        value = BigEndian::ToHost128(value);
        if (scale_params.scale_type == DecimalScaleParams::SCALE_UP) {
            value *= scale_params.scale_factor;
        } else if (scale_params.scale_type == DecimalScaleParams::SCALE_DOWN) {
            value /= scale_params.scale_factor;
        }
        auto& v = reinterpret_cast<DecimalPrimitiveType&>(column_data[origin_size + i]);
        v = (DecimalPrimitiveType)value;
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
                         size_t num_values) override;

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
