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
#include <vector>

#include "core/column/column.h"
#include "core/data_type/data_type_nullable.h"
#include "core/string_ref.h"

namespace doris::format::parquet::native {
namespace {
Status read_length(const Slice* data, uint32_t* offset, uint32_t* length) {
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
    _binary_values.clear();
    _binary_values.reserve(num_values);
    for (size_t row = 0; row < num_values; ++row) {
        uint32_t length = 0;
        RETURN_IF_ERROR(read_length(_data, &_offset, &length));
        if (UNLIKELY(_offset > _data->size || length > _data->size - _offset)) {
            return Status::IOError("Can't read enough bytes in Parquet plain decoder");
        }
        _binary_values.emplace_back(_data->data + _offset, length);
        _offset += length;
    }
    return consumer.consume(_binary_values.data(), _binary_values.size());
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
