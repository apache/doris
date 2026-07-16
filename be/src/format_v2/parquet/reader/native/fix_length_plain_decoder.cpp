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

#include "format_v2/parquet/reader/native/fix_length_plain_decoder.h"

#include <cstring>
#include <limits>

namespace doris::format::parquet::native {

Status FixLengthPlainDecoder::decode_fixed_values(size_t num_values,
                                                  ParquetFixedValueConsumer& consumer) {
    const size_t byte_size = num_values * static_cast<size_t>(_type_length);
    if (UNLIKELY(_offset > _data->size || byte_size > _data->size - _offset)) {
        return Status::IOError("Out-of-bounds access in Parquet plain decoder");
    }
    RETURN_IF_ERROR(consumer.consume(reinterpret_cast<const uint8_t*>(_data->data) + _offset,
                                     num_values, static_cast<size_t>(_type_length)));
    _offset += byte_size;
    return Status::OK();
}

Status FixLengthPlainDecoder::decode_selected_fixed_values(const ParquetSelection& selection,
                                                           ParquetFixedValueConsumer& consumer) {
    DORIS_CHECK(_type_length > 0);
    const size_t value_width = static_cast<size_t>(_type_length);
    if (UNLIKELY(selection.total_values > std::numeric_limits<size_t>::max() / value_width ||
                 selection.selected_values > std::numeric_limits<size_t>::max() / value_width)) {
        return Status::IOError("Parquet plain selection byte size overflows");
    }
    const size_t input_bytes = selection.total_values * value_width;
    if (UNLIKELY(_offset > _data->size || input_bytes > _data->size - _offset)) {
        return Status::IOError("Out-of-bounds access in Parquet plain selection decoder");
    }
    _selected_values.resize(selection.selected_values * value_width);
    size_t output_offset = 0;
    for (const auto& range : selection.ranges) {
        const size_t range_bytes = range.count * value_width;
        memcpy(_selected_values.data() + output_offset,
               reinterpret_cast<const uint8_t*>(_data->data) + _offset + range.first * value_width,
               range_bytes);
        output_offset += range_bytes;
    }
    DORIS_CHECK_EQ(output_offset, _selected_values.size());
    _offset += input_bytes;
    if (_selected_values.empty()) {
        return Status::OK();
    }
    return consumer.consume(_selected_values.data(), selection.selected_values, value_width);
}

Status FixLengthPlainDecoder::skip_values(size_t num_values) {
    _offset += _type_length * num_values;
    if (UNLIKELY(_offset > _data->size)) {
        return Status::IOError("Out-of-bounds access in parquet data decoder");
    }
    return Status::OK();
}

} // namespace doris::format::parquet::native
