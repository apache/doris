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

#include "format_v2/parquet/reader/native/byte_stream_split_decoder.h"

#include <cstdint>
#include <limits>

#include "core/column/column_fixed_length_object.h"
#include "util/byte_stream_split.h"

namespace doris::format::parquet::native {
Status ByteStreamSplitDecoder::decode_fixed_values(size_t num_values,
                                                   ParquetFixedValueConsumer& consumer) {
    const size_t byte_size = num_values * static_cast<size_t>(_type_length);
    if (UNLIKELY(_offset > _data->size || byte_size > _data->size - _offset)) {
        return Status::IOError("Out-of-bounds access in Parquet byte-stream-split decoder");
    }
    DORIS_CHECK_EQ(_data->size % static_cast<size_t>(_type_length), 0);
    const int64_t stride = static_cast<int64_t>(_data->size / _type_length);
    _decoded_values.resize(byte_size);
    byte_stream_split_decode(reinterpret_cast<const uint8_t*>(_data->data), _type_length,
                             _offset / _type_length, num_values, stride, _decoded_values.data());
    _offset += byte_size;
    return consumer.consume(_decoded_values.data(), num_values, static_cast<size_t>(_type_length));
}

Status ByteStreamSplitDecoder::decode_selected_fixed_values(const ParquetSelection& selection,
                                                            ParquetFixedValueConsumer& consumer) {
    DORIS_CHECK(_type_length > 0);
    const size_t value_width = static_cast<size_t>(_type_length);
    if (UNLIKELY(selection.total_values > std::numeric_limits<size_t>::max() / value_width ||
                 selection.selected_values > std::numeric_limits<size_t>::max() / value_width)) {
        return Status::IOError("Parquet byte-stream-split selection byte size overflows");
    }
    const size_t input_bytes = selection.total_values * value_width;
    if (UNLIKELY(_offset > _data->size || input_bytes > _data->size - _offset)) {
        return Status::IOError(
                "Out-of-bounds access in Parquet byte-stream-split selection decoder");
    }
    DORIS_CHECK_EQ(_data->size % value_width, 0);
    const int64_t stride = static_cast<int64_t>(_data->size / value_width);
    _decoded_values.resize(selection.selected_values * value_width);
    size_t output = 0;
    const size_t first_row = _offset / value_width;
    for (const auto& range : selection.ranges) {
        byte_stream_split_decode(reinterpret_cast<const uint8_t*>(_data->data), _type_length,
                                 first_row + range.first, range.count, stride,
                                 _decoded_values.data() + output * value_width);
        output += range.count;
    }
    DORIS_CHECK_EQ(output, selection.selected_values);
    _offset += input_bytes;
    return consumer.consume(_decoded_values.data(), output, value_width);
}

Status ByteStreamSplitDecoder::skip_values(size_t num_values) {
    _offset += _type_length * num_values;
    if (UNLIKELY(_offset > _data->size)) {
        return Status::IOError(
                "Out-of-bounds access in parquet data decoder: offset = {}, size = {}", _offset,
                _data->size);
    }
    return Status::OK();
}

} // namespace doris::format::parquet::native
