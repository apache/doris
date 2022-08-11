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

namespace doris::vectorized {

Status Decoder::getDecoder(tparquet::Type::type type, tparquet::Encoding::type encoding,
                           std::unique_ptr<Decoder>& decoder) {
    switch (encoding) {
    case tparquet::Encoding::PLAIN:
        switch (type) {
        case tparquet::Type::BOOLEAN:
            decoder.reset(new BoolPlainDecoder());
            break;
        case tparquet::Type::INT32:
            decoder.reset(new PlainDecoder<Int32>());
            break;
        case tparquet::Type::INT64:
            decoder.reset(new PlainDecoder<Int64>());
            break;
        case tparquet::Type::FLOAT:
            decoder.reset(new PlainDecoder<Float32>());
            break;
        case tparquet::Type::DOUBLE:
            decoder.reset(new PlainDecoder<Float64>());
            break;
        case tparquet::Type::BYTE_ARRAY:
            decoder.reset(new BAPlainDecoder());
            break;
        case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
            decoder.reset(new FixedLengthBAPlainDecoder());
            break;
        default:
            return Status::InternalError("Unsupported plain type {} in parquet decoder",
                                         tparquet::to_string(type));
        }
    case tparquet::Encoding::RLE_DICTIONARY:
        break;
    default:
        return Status::InternalError("Unsupported encoding {} in parquet decoder",
                                     tparquet::to_string(encoding));
    }
    return Status::OK();
}

Status Decoder::decode_values(ColumnPtr& doris_column, size_t num_values) {
    CHECK(doris_column->is_nullable());
    auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
            (*std::move(doris_column)).mutate().get());
    MutableColumnPtr data_column = nullable_column->get_nested_column_ptr();
    return _decode_values(data_column, num_values);
}

Status FixedLengthBAPlainDecoder::decode_values(Slice& slice, size_t num_values) {
    size_t to_read_bytes = _type_length * num_values;
    if (UNLIKELY(_offset + to_read_bytes > _data->size)) {
        return Status::IOError("Out-of-bounds access in parquet data decoder");
    }
    // insert '\0' into the end of each binary
    if (UNLIKELY(to_read_bytes + num_values > slice.size)) {
        return Status::IOError("Slice does not have enough space to write out the decoding data");
    }
    uint32_t slice_offset = 0;
    for (int i = 0; i < num_values; ++i) {
        memcpy(slice.data + slice_offset, _data->data + _offset, _type_length);
        slice_offset += _type_length + 1;
        slice.data[slice_offset - 1] = '\0';
        _offset += _type_length;
    }
    return Status::OK();
}

Status FixedLengthBAPlainDecoder::skip_values(size_t num_values) {
    _offset += _type_length * num_values;
    if (UNLIKELY(_offset > _data->size)) {
        return Status::IOError("Out-of-bounds access in parquet data decoder");
    }
    return Status::OK();
}

Status FixedLengthBAPlainDecoder::_decode_values(MutableColumnPtr& doris_column,
                                                 size_t num_values) {
    if (UNLIKELY(_offset + _type_length * num_values > _data->size)) {
        return Status::IOError("Out-of-bounds access in parquet data decoder");
    }
    auto& column_chars_t = assert_cast<ColumnString&>(*doris_column).get_chars();
    auto& column_offsets = assert_cast<ColumnString&>(*doris_column).get_offsets();
    for (int i = 0; i < num_values; ++i) {
        column_chars_t.insert(_data->data + _offset, _data->data + _offset + _type_length);
        column_chars_t.emplace_back('\0');
        column_offsets.emplace_back(column_chars_t.size());
        _offset += _type_length;
    }
    return Status::OK();
}

Status BAPlainDecoder::decode_values(Slice& slice, size_t num_values) {
    uint32_t slice_offset = 0;
    for (int i = 0; i < num_values; ++i) {
        if (UNLIKELY(_offset + 4 > _data->size)) {
            return Status::IOError("Can't read byte array length from plain decoder");
        }
        uint32_t length =
                decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data->data) + _offset);
        _offset += 4;
        if (UNLIKELY(_offset + length) > _data->size) {
            return Status::IOError("Can't read enough bytes in plain decoder");
        }
        memcpy(slice.data + slice_offset, _data->data + _offset, length);
        slice_offset += length + 1;
        slice.data[slice_offset - 1] = '\0';
        _offset += length;
    }
    return Status::OK();
}

Status BAPlainDecoder::skip_values(size_t num_values) {
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
    return Status::OK();
}

Status BAPlainDecoder::_decode_values(MutableColumnPtr& doris_column, size_t num_values) {
    auto& column_chars_t = assert_cast<ColumnString&>(*doris_column).get_chars();
    auto& column_offsets = assert_cast<ColumnString&>(*doris_column).get_offsets();
    for (int i = 0; i < num_values; ++i) {
        if (UNLIKELY(_offset + 4 > _data->size)) {
            return Status::IOError("Can't read byte array length from plain decoder");
        }
        uint32_t length =
                decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data->data) + _offset);
        _offset += 4;
        if (UNLIKELY(_offset + length) > _data->size) {
            return Status::IOError("Can't read enough bytes in plain decoder");
        }
        column_chars_t.insert(_data->data + _offset, _data->data + _offset + length);
        column_chars_t.emplace_back('\0');
        column_offsets.emplace_back(column_chars_t.size());
        _offset += length;
    }
    return Status::OK();
}

Status BoolPlainDecoder::decode_values(Slice& slice, size_t num_values) {
    bool value;
    for (int i = 0; i < num_values; ++i) {
        if (UNLIKELY(!_decode_value(&value))) {
            return Status::IOError("Can't read enough booleans in plain decoder");
        }
        slice.data[i] = value ? 1 : 0;
    }
    return Status::OK();
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

Status BoolPlainDecoder::_decode_values(MutableColumnPtr& doris_column, size_t num_values) {
    auto& column_data = static_cast<ColumnVector<UInt8>&>(*doris_column).get_data();
    bool value;
    for (int i = 0; i < num_values; ++i) {
        if (UNLIKELY(!_decode_value(&value))) {
            return Status::IOError("Can't read enough booleans in plain decoder");
        }
        column_data.emplace_back(value);
    }
    return Status::OK();
}
} // namespace doris::vectorized
