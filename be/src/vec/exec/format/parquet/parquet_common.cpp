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

#include "parquet_common.h"

#include "util/coding.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

const cctz::time_zone DecodeParams::utc0 = cctz::utc_time_zone();

const uint32_t ParquetInt96::JULIAN_EPOCH_OFFSET_DAYS = 2440588;
const uint64_t ParquetInt96::MICROS_IN_DAY = 86400000000;
const uint64_t ParquetInt96::NANOS_PER_MICROSECOND = 1000;

inline uint64_t ParquetInt96::to_timestamp_micros() const {
    return (hi - JULIAN_EPOCH_OFFSET_DAYS) * MICROS_IN_DAY + lo / NANOS_PER_MICROSECOND;
}

#define FOR_LOGICAL_NUMERIC_TYPES(M) \
    M(TypeIndex::Int32, Int32)       \
    M(TypeIndex::UInt32, UInt32)     \
    M(TypeIndex::Int64, Int64)       \
    M(TypeIndex::UInt64, UInt64)     \
    M(TypeIndex::Float32, Float32)   \
    M(TypeIndex::Float64, Float64)

#define FOR_SHORT_INT_TYPES(M) \
    M(TypeIndex::Int8, Int8)   \
    M(TypeIndex::UInt8, UInt8) \
    M(TypeIndex::Int16, Int16) \
    M(TypeIndex::UInt16, UInt16)

Status Decoder::get_decoder(tparquet::Type::type type, tparquet::Encoding::type encoding,
                            std::unique_ptr<Decoder>& decoder) {
    switch (encoding) {
    case tparquet::Encoding::PLAIN:
    case tparquet::Encoding::RLE_DICTIONARY:
        switch (type) {
        case tparquet::Type::BOOLEAN:
            if (encoding != tparquet::Encoding::PLAIN) {
                return Status::InternalError("Bool type can't has dictionary page");
            }
            decoder.reset(new BoolPlainDecoder());
            break;
        case tparquet::Type::BYTE_ARRAY:
            decoder.reset(new ByteArrayDecoder());
            break;
        case tparquet::Type::INT32:
        case tparquet::Type::INT64:
        case tparquet::Type::INT96:
        case tparquet::Type::FLOAT:
        case tparquet::Type::DOUBLE:
        case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
            decoder.reset(new FixLengthDecoder(type));
            break;
        default:
            return Status::InternalError("Unsupported type {}(encoding={}) in parquet decoder",
                                         tparquet::to_string(type), tparquet::to_string(encoding));
        }
        break;
    default:
        return Status::InternalError("Unsupported encoding {}(type={}) in parquet decoder",
                                     tparquet::to_string(encoding), tparquet::to_string(type));
    }
    return Status::OK();
}

void Decoder::init(FieldSchema* field_schema, cctz::time_zone* ctz) {
    _field_schema = field_schema;
    if (_decode_params == nullptr) {
        _decode_params.reset(new DecodeParams());
    }
    if (ctz != nullptr) {
        _decode_params->ctz = ctz;
    }
    const auto& schema = field_schema->parquet_schema;
    if (schema.__isset.logicalType && schema.logicalType.__isset.TIMESTAMP) {
        const auto& timestamp_info = schema.logicalType.TIMESTAMP;
        if (!timestamp_info.isAdjustedToUTC) {
            // should set timezone to utc+0
            _decode_params->ctz = const_cast<cctz::time_zone*>(&_decode_params->utc0);
        }
        const auto& time_unit = timestamp_info.unit;
        if (time_unit.__isset.MILLIS) {
            _decode_params->second_mask = 1000;
            _decode_params->scale_to_nano_factor = 1000000;
        } else if (time_unit.__isset.MICROS) {
            _decode_params->second_mask = 1000000;
            _decode_params->scale_to_nano_factor = 1000;
        } else if (time_unit.__isset.NANOS) {
            _decode_params->second_mask = 1000000000;
            _decode_params->scale_to_nano_factor = 1;
        }
    } else if (schema.__isset.converted_type) {
        const auto& converted_type = schema.converted_type;
        if (converted_type == tparquet::ConvertedType::TIMESTAMP_MILLIS) {
            _decode_params->second_mask = 1000;
            _decode_params->scale_to_nano_factor = 1000000;
        } else if (converted_type == tparquet::ConvertedType::TIMESTAMP_MICROS) {
            _decode_params->second_mask = 1000000;
            _decode_params->scale_to_nano_factor = 1000;
        }
    }
}

Status Decoder::decode_values(ColumnPtr& doris_column, DataTypePtr& data_type, size_t num_values) {
    CHECK(doris_column->is_nullable());
    auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
            (*std::move(doris_column)).mutate().get());
    MutableColumnPtr data_column = nullable_column->get_nested_column_ptr();
    return decode_values(data_column, data_type, num_values);
}

Status FixLengthDecoder::set_dict(std::unique_ptr<uint8_t[]>& dict, int32_t length,
                                  size_t num_values) {
    if (num_values * _type_length != length) {
        return Status::Corruption("Wrong dictionary data for fixed length type");
    }
    _has_dict = true;
    _dict = std::move(dict);
    char* dict_item_address = reinterpret_cast<char*>(_dict.get());
    _dict_items.resize(num_values);
    for (size_t i = 0; i < num_values; ++i) {
        _dict_items[i] = dict_item_address;
        dict_item_address += _type_length;
    }
    return Status::OK();
}

void FixLengthDecoder::set_data(Slice* data) {
    _data = data;
    _offset = 0;
    if (_has_dict) {
        uint8_t bit_width = *data->data;
        _index_batch_decoder.reset(
                new RleBatchDecoder<uint32_t>(reinterpret_cast<uint8_t*>(data->data) + 1,
                                              static_cast<int>(data->size) - 1, bit_width));
    }
}

Status FixLengthDecoder::skip_values(size_t num_values) {
    if (_has_dict) {
        _indexes.resize(num_values);
        _index_batch_decoder->GetBatch(&_indexes[0], num_values);
    } else {
        _offset += _type_length * num_values;
        if (UNLIKELY(_offset > _data->size)) {
            return Status::IOError("Out-of-bounds access in parquet data decoder");
        }
    }
    return Status::OK();
}

Status FixLengthDecoder::decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                       size_t num_values) {
    if (_has_dict) {
        _indexes.resize(num_values);
        _index_batch_decoder->GetBatch(&_indexes[0], num_values);
    } else if (UNLIKELY(_offset + _type_length * num_values > _data->size)) {
        return Status::IOError("Out-of-bounds access in parquet data decoder");
    }
    TypeIndex logical_type = remove_nullable(data_type)->get_type_id();
    switch (logical_type) {
#define DISPATCH(SHORT_INT_TYPE, CPP_SHORT_INT_TYPE) \
    case SHORT_INT_TYPE:                             \
        return _decode_short_int<CPP_SHORT_INT_TYPE>(doris_column, num_values);
        FOR_SHORT_INT_TYPES(DISPATCH)
#undef DISPATCH
#define DISPATCH(NUMERIC_TYPE, CPP_NUMERIC_TYPE) \
    case NUMERIC_TYPE:                           \
        return _decode_numeric<CPP_NUMERIC_TYPE>(doris_column, num_values);
        FOR_LOGICAL_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    case TypeIndex::Date:
        if (_physical_type == tparquet::Type::INT32) {
            return _decode_date<VecDateTimeValue, Int64>(doris_column, logical_type, num_values);
        }
        break;
    case TypeIndex::DateV2:
        if (_physical_type == tparquet::Type::INT32) {
            return _decode_date<DateV2Value<DateV2ValueType>, UInt32>(doris_column, logical_type,
                                                                      num_values);
        }
        break;
    case TypeIndex::DateTime:
        if (_physical_type == tparquet::Type::INT96) {
            return _decode_datetime96<VecDateTimeValue, Int64>(doris_column, logical_type,
                                                               num_values);
        } else if (_physical_type == tparquet::Type::INT64) {
            return _decode_datetime64<VecDateTimeValue, Int64>(doris_column, logical_type,
                                                               num_values);
        }
        break;
    case TypeIndex::DateTimeV2:
        // Spark can set the timestamp precision by the following configuration:
        // spark.sql.parquet.outputTimestampType = INT96(NANOS), TIMESTAMP_MICROS, TIMESTAMP_MILLIS
        if (_physical_type == tparquet::Type::INT96) {
            return _decode_datetime96<DateV2Value<DateTimeV2ValueType>, UInt64>(
                    doris_column, logical_type, num_values);
        } else if (_physical_type == tparquet::Type::INT64) {
            return _decode_datetime64<DateV2Value<DateTimeV2ValueType>, UInt64>(
                    doris_column, logical_type, num_values);
        }
        break;
    case TypeIndex::Decimal32:
        if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            return _decode_binary_decimal<Int32>(doris_column, data_type, num_values);
        } else if (_physical_type == tparquet::Type::INT32) {
            return _decode_primitive_decimal<Int32, Int32>(doris_column, data_type, num_values);
        } else if (_physical_type == tparquet::Type::INT64) {
            return _decode_primitive_decimal<Int32, Int64>(doris_column, data_type, num_values);
        }
        break;
    case TypeIndex::Decimal64:
        if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            return _decode_binary_decimal<Int64>(doris_column, data_type, num_values);
        } else if (_physical_type == tparquet::Type::INT32) {
            return _decode_primitive_decimal<Int64, Int32>(doris_column, data_type, num_values);
        } else if (_physical_type == tparquet::Type::INT64) {
            return _decode_primitive_decimal<Int64, Int64>(doris_column, data_type, num_values);
        }
        break;
    case TypeIndex::Decimal128:
        if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            return _decode_binary_decimal<Int128>(doris_column, data_type, num_values);
        } else if (_physical_type == tparquet::Type::INT32) {
            return _decode_primitive_decimal<Int128, Int32>(doris_column, data_type, num_values);
        } else if (_physical_type == tparquet::Type::INT64) {
            return _decode_primitive_decimal<Int128, Int64>(doris_column, data_type, num_values);
        }
        break;
    case TypeIndex::String:
    case TypeIndex::FixedString:
        if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            for (int i = 0; i < num_values; ++i) {
                char* buf_start = _FIXED_GET_DATA_OFFSET(i);
                doris_column->insert_data(buf_start, _type_length);
                _FIXED_SHIFT_DATA_OFFSET();
            }
            return Status::OK();
        }
        break;
    default:
        break;
    }

    return Status::InvalidArgument("Can't decode parquet physical type {} to doris logical type {}",
                                   tparquet::to_string(_physical_type), getTypeName(logical_type));
}

Status ByteArrayDecoder::set_dict(std::unique_ptr<uint8_t[]>& dict, int32_t length,
                                  size_t num_values) {
    _has_dict = true;
    _dict = std::move(dict);
    _dict_items.reserve(num_values);
    uint32_t offset_cursor = 0;
    char* dict_item_address = reinterpret_cast<char*>(_dict.get());
    for (int i = 0; i < num_values; ++i) {
        uint32_t l = decode_fixed32_le(_dict.get() + offset_cursor);
        offset_cursor += 4;
        _dict_items.emplace_back(dict_item_address + offset_cursor, l);
        offset_cursor += l;
        if (offset_cursor > length) {
            return Status::Corruption("Wrong data length in dictionary");
        }
    }
    if (offset_cursor != length) {
        return Status::Corruption("Wrong dictionary data for byte array type");
    }
    return Status::OK();
}

void ByteArrayDecoder::set_data(Slice* data) {
    _data = data;
    _offset = 0;
    if (_has_dict) {
        uint8_t bit_width = *data->data;
        _index_batch_decoder.reset(
                new RleBatchDecoder<uint32_t>(reinterpret_cast<uint8_t*>(data->data) + 1,
                                              static_cast<int>(data->size) - 1, bit_width));
    }
}

Status ByteArrayDecoder::skip_values(size_t num_values) {
    if (_has_dict) {
        _indexes.resize(num_values);
        _index_batch_decoder->GetBatch(&_indexes[0], num_values);
    } else {
        for (int i = 0; i < num_values; ++i) {
            if (UNLIKELY(_offset + 4 > _data->size)) {
                return Status::IOError("Can't read byte array length from plain decoder");
            }
            uint32_t length =
                    decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data->data) + _offset);
            _offset += 4;
            if (UNLIKELY(_offset + length) > _data->size) {
                return Status::IOError("Can't skip enough bytes in plain decoder");
            }
            _offset += length;
        }
    }
    return Status::OK();
}

Status ByteArrayDecoder::decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                       size_t num_values) {
    if (_has_dict) {
        _indexes.resize(num_values);
        _index_batch_decoder->GetBatch(&_indexes[0], num_values);
    }
    TypeIndex logical_type = remove_nullable(data_type)->get_type_id();
    switch (logical_type) {
    case TypeIndex::String:
    case TypeIndex::FixedString: {
        std::vector<StringRef> string_values;
        string_values.reserve(num_values);
        for (int i = 0; i < num_values; ++i) {
            if (_has_dict) {
                string_values.emplace_back(_dict_items[_indexes[i]]);
            } else {
                if (UNLIKELY(_offset + 4 > _data->size)) {
                    return Status::IOError("Can't read byte array length from plain decoder");
                }
                uint32_t length =
                        decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data->data) + _offset);
                _offset += 4;
                if (UNLIKELY(_offset + length) > _data->size) {
                    return Status::IOError("Can't read enough bytes in plain decoder");
                }
                string_values.emplace_back(_data->data + _offset, length);
                _offset += length;
            }
        }
        doris_column->insert_many_strings(&string_values[0], num_values);
        return Status::OK();
    }
    case TypeIndex::Decimal32:
        return _decode_binary_decimal<Int32>(doris_column, data_type, num_values);
    case TypeIndex::Decimal64:
        return _decode_binary_decimal<Int64>(doris_column, data_type, num_values);
    case TypeIndex::Decimal128:
        return _decode_binary_decimal<Int128>(doris_column, data_type, num_values);
    default:
        break;
    }
    return Status::InvalidArgument(
            "Can't decode parquet physical type BYTE_ARRAY to doris logical type {}",
            getTypeName(logical_type));
}

Status BoolPlainDecoder::skip_values(size_t num_values) {
    int skip_cached = std::min(num_unpacked_values_ - unpacked_value_idx_, (int)num_values);
    unpacked_value_idx_ += skip_cached;
    if (skip_cached == num_values) {
        return Status::OK();
    }
    int num_remaining = num_values - skip_cached;
    int num_to_skip = BitUtil::RoundDownToPowerOf2(num_remaining, 32);
    if (num_to_skip > 0) {
        bool_values_.SkipBatch(1, num_to_skip);
    }
    num_remaining -= num_to_skip;
    if (num_remaining > 0) {
        DCHECK_LE(num_remaining, UNPACKED_BUFFER_LEN);
        num_unpacked_values_ =
                bool_values_.UnpackBatch(1, UNPACKED_BUFFER_LEN, &unpacked_values_[0]);
        if (UNLIKELY(num_unpacked_values_ < num_remaining)) {
            return Status::IOError("Can't skip enough booleans in plain decoder");
        }
        unpacked_value_idx_ = num_remaining;
    }
    return Status::OK();
}

Status BoolPlainDecoder::decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                       size_t num_values) {
    auto& column_data = static_cast<ColumnVector<UInt8>&>(*doris_column).get_data();
    int has_read = 0;
    while (has_read < num_values) {
        int loop_read =
                std::min((int)num_values - has_read, num_unpacked_values_ - unpacked_value_idx_);
        if (loop_read > 0) {
            column_data.insert(unpacked_values_ + unpacked_value_idx_,
                               unpacked_values_ + unpacked_value_idx_ + loop_read);
            unpacked_value_idx_ += loop_read;
            has_read += loop_read;
        }
        if (unpacked_value_idx_ == num_unpacked_values_) {
            num_unpacked_values_ =
                    bool_values_.UnpackBatch(1, UNPACKED_BUFFER_LEN, &unpacked_values_[0]);
            unpacked_value_idx_ = 0;
            if (num_unpacked_values_ == 0) {
                break;
            }
        }
    }
    if (UNLIKELY(has_read < num_values)) {
        return Status::IOError("Can't read enough booleans in plain decoder");
    }
    return Status::OK();
}
} // namespace doris::vectorized
