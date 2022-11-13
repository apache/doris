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
    M(TypeIndex::Int8, Int8)         \
    M(TypeIndex::UInt8, UInt8)       \
    M(TypeIndex::Int16, Int16)       \
    M(TypeIndex::UInt16, UInt16)     \
    M(TypeIndex::Int32, Int32)       \
    M(TypeIndex::UInt32, UInt32)     \
    M(TypeIndex::Int64, Int64)       \
    M(TypeIndex::UInt64, UInt64)     \
    M(TypeIndex::Float32, Float32)   \
    M(TypeIndex::Float64, Float64)

ColumnSelectVector::ColumnSelectVector(const uint8_t* filter_map, size_t filter_map_size,
                                       bool filter_all) {
    build(filter_map, filter_map_size, filter_all);
}

void ColumnSelectVector::build(const uint8_t* filter_map, size_t filter_map_size, bool filter_all) {
    _filter_all = filter_all;
    _filter_map = filter_map;
    _filter_map_size = filter_map_size;
    if (filter_all) {
        _has_filter = true;
        _filter_ratio = 1;
    } else if (filter_map == nullptr) {
        _has_filter = false;
        _filter_ratio = 0;
    } else {
        size_t filter_count =
                simd::count_zero_num(reinterpret_cast<const int8_t*>(filter_map), filter_map_size);
        if (filter_count == filter_map_size) {
            _has_filter = true;
            _filter_all = true;
            _filter_ratio = 1;
        } else if (filter_count > 0 && filter_map_size > 0) {
            _has_filter = true;
            _filter_ratio = (double)filter_count / filter_map_size;
        } else {
            _has_filter = false;
            _filter_ratio = 0;
        }
    }
}

void ColumnSelectVector::set_run_length_null_map(const std::vector<uint16_t>& run_length_null_map,
                                                 size_t num_values, NullMap* null_map) {
    _num_values = num_values;
    _num_nulls = 0;
    _read_index = 0;
    size_t map_index = 0;
    bool is_null = false;
    if (_has_filter) {
        // No run length null map is generated when _filter_all = true
        DCHECK(!_filter_all);
        _data_map.resize(num_values);
        for (auto& run_length : run_length_null_map) {
            if (is_null) {
                _num_nulls += run_length;
                for (uint16_t i = 0; i < run_length; ++i) {
                    _data_map[map_index++] = FILTERED_NULL;
                }
            } else {
                for (uint16_t i = 0; i < run_length; ++i) {
                    _data_map[map_index++] = FILTERED_CONTENT;
                }
            }
            is_null = !is_null;
        }
        uint16_t num_read = 0;
        DCHECK_LE(_filter_map_index + num_values, _filter_map_size);
        for (size_t i = 0; i < num_values; ++i) {
            if (_filter_map[_filter_map_index++]) {
                _data_map[i] = _data_map[i] == FILTERED_NULL ? NULL_DATA : CONTENT;
                num_read++;
            }
        }
        _num_filtered = num_values - num_read;
        if (null_map != nullptr && num_read > 0) {
            NullMap& map_data_column = *null_map;
            auto null_map_index = map_data_column.size();
            map_data_column.resize(null_map_index + num_read);
            for (size_t i = 0; i < num_values; ++i) {
                if (_data_map[i] == CONTENT) {
                    map_data_column[null_map_index++] = (UInt8) false;
                } else if (_data_map[i] == NULL_DATA) {
                    map_data_column[null_map_index++] = (UInt8) true;
                }
            }
        }
    } else {
        _num_filtered = 0;
        _run_length_null_map = &run_length_null_map;
        if (null_map != nullptr) {
            NullMap& map_data_column = *null_map;
            auto null_map_index = map_data_column.size();
            map_data_column.resize(null_map_index + num_values);
            for (auto& run_length : run_length_null_map) {
                if (is_null) {
                    _num_nulls += run_length;
                    for (int i = 0; i < run_length; ++i) {
                        map_data_column[null_map_index++] = (UInt8) true;
                    }
                } else {
                    for (int i = 0; i < run_length; ++i) {
                        map_data_column[null_map_index++] = (UInt8) false;
                    }
                }
                is_null = !is_null;
            }
        }
    }
}

bool ColumnSelectVector::can_filter_all(size_t remaining_num_values) {
    if (!_has_filter) {
        return false;
    }
    if (_filter_all) {
        // all data in normal columns can be skipped when _filter_all = true,
        // so the remaining_num_values should be less than the remaining filter map size.
        DCHECK_LE(remaining_num_values + _filter_map_index, _filter_map_size);
        // return true always, to make sure that the data in normal columns can be skipped.
        return true;
    }
    if (remaining_num_values + _filter_map_index > _filter_map_size) {
        return false;
    }
    return simd::count_zero_num(reinterpret_cast<const int8_t*>(_filter_map + _filter_map_index),
                                remaining_num_values) == remaining_num_values;
}

void ColumnSelectVector::skip(size_t num_values) {
    _filter_map_index += num_values;
}

uint16_t ColumnSelectVector::get_next_run(DataReadType* data_read_type) {
    if (_has_filter) {
        if (_read_index == _num_values) {
            return 0;
        }
        const DataReadType& type = _data_map[_read_index++];
        uint16_t run_length = 1;
        while (_read_index < _num_values) {
            if (_data_map[_read_index] == type) {
                run_length++;
                _read_index++;
            } else {
                break;
            }
        }
        *data_read_type = type;
        return run_length;
    } else {
        uint16_t run_length = 0;
        while (run_length == 0) {
            if (_read_index == (*_run_length_null_map).size()) {
                return 0;
            }
            *data_read_type = _read_index % 2 == 0 ? CONTENT : NULL_DATA;
            run_length = (*_run_length_null_map)[_read_index++];
        }
        return run_length;
    }
}

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
                                       ColumnSelectVector& select_vector) {
    size_t non_null_size = select_vector.num_values() - select_vector.num_nulls();
    if (_has_dict) {
        _indexes.resize(non_null_size);
        _index_batch_decoder->GetBatch(&_indexes[0], non_null_size);
    } else if (UNLIKELY(_offset + _type_length * non_null_size > _data->size)) {
        return Status::IOError("Out-of-bounds access in parquet data decoder");
    }
    TypeIndex logical_type = remove_nullable(data_type)->get_type_id();
    switch (logical_type) {
#define DISPATCH(NUMERIC_TYPE, CPP_NUMERIC_TYPE) \
    case NUMERIC_TYPE:                           \
        return _decode_numeric<CPP_NUMERIC_TYPE>(doris_column, select_vector);
        FOR_LOGICAL_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    case TypeIndex::Date:
        if (_physical_type == tparquet::Type::INT32) {
            return _decode_date<VecDateTimeValue, Int64>(doris_column, select_vector);
        }
        break;
    case TypeIndex::DateV2:
        if (_physical_type == tparquet::Type::INT32) {
            return _decode_date<DateV2Value<DateV2ValueType>, UInt32>(doris_column, select_vector);
        }
        break;
    case TypeIndex::DateTime:
        if (_physical_type == tparquet::Type::INT96) {
            return _decode_datetime96<VecDateTimeValue, Int64>(doris_column, select_vector);
        } else if (_physical_type == tparquet::Type::INT64) {
            return _decode_datetime64<VecDateTimeValue, Int64>(doris_column, select_vector);
        }
        break;
    case TypeIndex::DateTimeV2:
        // Spark can set the timestamp precision by the following configuration:
        // spark.sql.parquet.outputTimestampType = INT96(NANOS), TIMESTAMP_MICROS, TIMESTAMP_MILLIS
        if (_physical_type == tparquet::Type::INT96) {
            return _decode_datetime96<DateV2Value<DateTimeV2ValueType>, UInt64>(doris_column,
                                                                                select_vector);
        } else if (_physical_type == tparquet::Type::INT64) {
            return _decode_datetime64<DateV2Value<DateTimeV2ValueType>, UInt64>(doris_column,
                                                                                select_vector);
        }
        break;
    case TypeIndex::Decimal32:
        if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            return _decode_binary_decimal<Int32>(doris_column, data_type, select_vector);
        } else if (_physical_type == tparquet::Type::INT32) {
            return _decode_primitive_decimal<Int32, Int32>(doris_column, data_type, select_vector);
        } else if (_physical_type == tparquet::Type::INT64) {
            return _decode_primitive_decimal<Int32, Int64>(doris_column, data_type, select_vector);
        }
        break;
    case TypeIndex::Decimal64:
        if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            return _decode_binary_decimal<Int64>(doris_column, data_type, select_vector);
        } else if (_physical_type == tparquet::Type::INT32) {
            return _decode_primitive_decimal<Int64, Int32>(doris_column, data_type, select_vector);
        } else if (_physical_type == tparquet::Type::INT64) {
            return _decode_primitive_decimal<Int64, Int64>(doris_column, data_type, select_vector);
        }
        break;
    case TypeIndex::Decimal128:
        if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            return _decode_binary_decimal<Int128>(doris_column, data_type, select_vector);
        } else if (_physical_type == tparquet::Type::INT32) {
            return _decode_primitive_decimal<Int128, Int32>(doris_column, data_type, select_vector);
        } else if (_physical_type == tparquet::Type::INT64) {
            return _decode_primitive_decimal<Int128, Int64>(doris_column, data_type, select_vector);
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

    return Status::InvalidArgument("Can't decode parquet physical type {} to doris logical type {}",
                                   tparquet::to_string(_physical_type), getTypeName(logical_type));
}

Status FixLengthDecoder::_decode_string(MutableColumnPtr& doris_column,
                                        ColumnSelectVector& select_vector) {
    size_t dict_index = 0;
    ColumnSelectVector::DataReadType read_type;
    while (uint16_t run_length = select_vector.get_next_run(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            std::vector<StringRef> string_values;
            string_values.reserve(run_length);
            for (int i = 0; i < run_length; ++i) {
                char* buf_start = _FIXED_GET_DATA_OFFSET(dict_index++);
                string_values.emplace_back(buf_start, _type_length);
                _FIXED_SHIFT_DATA_OFFSET();
            }
            doris_column->insert_many_strings(&string_values[0], run_length);
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            doris_column->insert_many_defaults(run_length);
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
                                       ColumnSelectVector& select_vector) {
    size_t non_null_size = select_vector.num_values() - select_vector.num_nulls();
    if (_has_dict) {
        _indexes.resize(non_null_size);
        _index_batch_decoder->GetBatch(&_indexes[0], non_null_size);
    }
    TypeIndex logical_type = remove_nullable(data_type)->get_type_id();
    switch (logical_type) {
    case TypeIndex::String:
    case TypeIndex::FixedString: {
        size_t dict_index = 0;

        ColumnSelectVector::DataReadType read_type;
        while (uint16_t run_length = select_vector.get_next_run(&read_type)) {
            switch (read_type) {
            case ColumnSelectVector::CONTENT: {
                std::vector<StringRef> string_values;
                string_values.reserve(run_length);
                for (int i = 0; i < run_length; ++i) {
                    if (_has_dict) {
                        string_values.emplace_back(_dict_items[_indexes[dict_index++]]);
                    } else {
                        if (UNLIKELY(_offset + 4 > _data->size)) {
                            return Status::IOError(
                                    "Can't read byte array length from plain decoder");
                        }
                        uint32_t length = decode_fixed32_le(
                                reinterpret_cast<const uint8_t*>(_data->data) + _offset);
                        _offset += 4;
                        if (UNLIKELY(_offset + length) > _data->size) {
                            return Status::IOError("Can't read enough bytes in plain decoder");
                        }
                        string_values.emplace_back(_data->data + _offset, length);
                        _offset += length;
                    }
                }
                doris_column->insert_many_strings(&string_values[0], run_length);
                break;
            }
            case ColumnSelectVector::NULL_DATA: {
                doris_column->insert_many_defaults(run_length);
                break;
            }
            case ColumnSelectVector::FILTERED_CONTENT: {
                if (_has_dict) {
                    dict_index += run_length;
                } else {
                    for (int i = 0; i < run_length; ++i) {
                        if (UNLIKELY(_offset + 4 > _data->size)) {
                            return Status::IOError(
                                    "Can't read byte array length from plain decoder");
                        }
                        uint32_t length = decode_fixed32_le(
                                reinterpret_cast<const uint8_t*>(_data->data) + _offset);
                        _offset += 4;
                        if (UNLIKELY(_offset + length) > _data->size) {
                            return Status::IOError("Can't read enough bytes in plain decoder");
                        }
                        _offset += length;
                    }
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
    case TypeIndex::Decimal32:
        return _decode_binary_decimal<Int32>(doris_column, data_type, select_vector);
    case TypeIndex::Decimal64:
        return _decode_binary_decimal<Int64>(doris_column, data_type, select_vector);
    case TypeIndex::Decimal128:
        return _decode_binary_decimal<Int128>(doris_column, data_type, select_vector);
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
                                       ColumnSelectVector& select_vector) {
    auto& column_data = static_cast<ColumnVector<UInt8>&>(*doris_column).get_data();
    size_t data_index = column_data.size();
    column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());

    ColumnSelectVector::DataReadType read_type;
    while (uint16_t run_length = select_vector.get_next_run(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            bool value;
            for (int i = 0; i < run_length; ++i) {
                if (UNLIKELY(!_decode_value(&value))) {
                    return Status::IOError("Can't read enough booleans in plain decoder");
                }
                column_data[data_index++] = (UInt8)value;
            }
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            data_index += run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_CONTENT: {
            bool value;
            for (int i = 0; i < run_length; ++i) {
                if (UNLIKELY(!_decode_value(&value))) {
                    return Status::IOError("Can't read enough booleans in plain decoder");
                }
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
} // namespace doris::vectorized
