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

#include "format_v2/parquet/reader/native/bool_rle_decoder.h"

#include <glog/logging.h>

#include <algorithm>
#include <ostream>
#include <string>

#include "core/column/column_vector.h"
#include "core/types.h"
#include "format/parquet/parquet_common.h"
#include "util/coding.h"
#include "util/slice.h"

namespace doris::format::parquet::native {
Status BoolRLEDecoder::set_data(Slice* slice) {
    _data = slice;
    _num_bytes = slice->size;
    _offset = 0;
    if (_num_bytes < 4) {
        return Status::IOError("Received invalid length : " + std::to_string(_num_bytes) +
                               " (corrupt data page?)");
    }
    // Load the first 4 bytes in little-endian, which indicates the length
    const auto* data = reinterpret_cast<const uint8_t*>(_data->data);
    uint32_t num_bytes = decode_fixed32_le(data);
    if (num_bytes > static_cast<uint32_t>(_num_bytes - 4)) {
        return Status::IOError("Received invalid number of bytes : " + std::to_string(num_bytes) +
                               " (corrupt data page?)");
    }
    _num_bytes = num_bytes;
    auto decoder_data = data + 4;
    _decoder = RleDecoder<uint8_t>(decoder_data, num_bytes, 1);
    return Status::OK();
}

Status BoolRLEDecoder::skip_values(size_t num_values) {
    _decoder.Skip(num_values);
    return Status::OK();
}

Status BoolRLEDecoder::decode_fixed_values(size_t num_values, ParquetFixedValueConsumer& consumer) {
    _values.resize(num_values);
    if (!_decoder.get_values(_values.data(), num_values)) {
        return Status::IOError("Can't read enough booleans in Parquet RLE decoder");
    }
    return consumer.consume(_values.data(), _values.size(), sizeof(uint8_t));
}

} // namespace doris::format::parquet::native
