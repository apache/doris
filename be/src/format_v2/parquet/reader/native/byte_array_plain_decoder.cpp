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

#include "format_v2/parquet/reader/native/byte_array_plain_decoder.h"

#include <algorithm>
#include <limits>
#include <vector>

#include "core/column/column.h"
#include "core/data_type/data_type_nullable.h"
#include "core/string_ref.h"

namespace doris::format::parquet::native {
namespace {
Status read_length(const Slice* data, size_t* offset, uint32_t* length) {
    if (UNLIKELY(*offset > data->size || data->size - *offset < sizeof(uint32_t))) {
        return Status::IOError("Can't read byte array length from plain decoder");
    }
    *length = decode_fixed32_le(reinterpret_cast<const uint8_t*>(data->data) + *offset);
    *offset += sizeof(uint32_t);
    return Status::OK();
}
} // namespace

Status ByteArrayPlainDecoder::decode_binary_values(size_t num_values,
                                                   ParquetBinaryValueConsumer& consumer) {
    _payload_offsets.clear();
    _payload_offsets.reserve(num_values);
    _value_offsets.clear();
    _value_offsets.reserve(num_values + 1);
    _value_offsets.push_back(0);
    for (size_t row = 0; row < num_values; ++row) {
        uint32_t length = 0;
        RETURN_IF_ERROR(read_length(_data, &_offset, &length));
        if (UNLIKELY(_offset > _data->size || length > _data->size - _offset)) {
            return Status::IOError("Can't read enough bytes in Parquet plain decoder");
        }
        if (UNLIKELY(_offset > std::numeric_limits<uint32_t>::max() ||
                     length > std::numeric_limits<uint32_t>::max() - _value_offsets.back())) {
            return Status::IOError("Parquet plain BYTE_ARRAY batch exceeds uint32 offsets");
        }
        _payload_offsets.push_back(static_cast<uint32_t>(_offset));
        _value_offsets.push_back(_value_offsets.back() + length);
        _offset += length;
    }
    _value_spans.clear();
    if (num_values != 0) {
        _value_spans.push_back({.first = 0, .count = num_values});
    }
    return consumer.consume_plain_byte_array(_data->data, _payload_offsets.data(),
                                             _value_offsets.data(), num_values, _value_spans);
}

Status ByteArrayPlainDecoder::decode_selected_binary_values(const ParquetSelection& selection,
                                                            ParquetBinaryValueConsumer& consumer) {
    _payload_offsets.clear();
    _payload_offsets.reserve(selection.selected_values);
    _value_offsets.clear();
    _value_offsets.reserve(selection.selected_values + 1);
    _value_offsets.push_back(0);
    _value_spans.clear();
    _value_spans.reserve(selection.ranges.size());
    size_t range_index = 0;
    size_t selected_in_range = 0;
    for (size_t row = 0; row < selection.total_values; ++row) {
        uint32_t length = 0;
        RETURN_IF_ERROR(read_length(_data, &_offset, &length));
        if (UNLIKELY(_offset > _data->size || length > _data->size - _offset)) {
            return Status::IOError("Can't read enough bytes in Parquet plain selection decoder");
        }
        while (range_index < selection.ranges.size() &&
               row >= selection.ranges[range_index].first + selection.ranges[range_index].count) {
            if (selected_in_range != 0) {
                _value_spans.push_back({.first = _payload_offsets.size() - selected_in_range,
                                        .count = selected_in_range});
                selected_in_range = 0;
            }
            ++range_index;
        }
        if (range_index < selection.ranges.size() && row >= selection.ranges[range_index].first) {
            if (UNLIKELY(_offset > std::numeric_limits<uint32_t>::max() ||
                         length > std::numeric_limits<uint32_t>::max() - _value_offsets.back())) {
                return Status::IOError("Parquet plain BYTE_ARRAY selection exceeds uint32 offsets");
            }
            _payload_offsets.push_back(static_cast<uint32_t>(_offset));
            _value_offsets.push_back(_value_offsets.back() + length);
            ++selected_in_range;
        }
        _offset += length;
    }
    if (selected_in_range != 0) {
        _value_spans.push_back(
                {.first = _payload_offsets.size() - selected_in_range, .count = selected_in_range});
    }
    DORIS_CHECK_EQ(_payload_offsets.size(), selection.selected_values);
    if (_payload_offsets.empty()) {
        return Status::OK();
    }
    return consumer.consume_plain_byte_array(_data->data, _payload_offsets.data(),
                                             _value_offsets.data(), _payload_offsets.size(),
                                             _value_spans);
}

Status ByteArrayPlainDecoder::skip_values(size_t num_values) {
    for (int i = 0; i < num_values; ++i) {
        uint32_t length = 0;
        RETURN_IF_ERROR(read_length(_data, &_offset, &length));
        if (UNLIKELY(_offset > _data->size || length > _data->size - _offset)) {
            return Status::IOError("Can't skip enough bytes in plain decoder");
        }
        _offset += length;
    }
    return Status::OK();
}

} // namespace doris::format::parquet::native
