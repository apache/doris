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

Status FixLengthPlainDecoder::skip_values(size_t num_values) {
    _offset += _type_length * num_values;
    if (UNLIKELY(_offset > _data->size)) {
        return Status::IOError("Out-of-bounds access in parquet data decoder");
    }
    return Status::OK();
}

} // namespace doris::format::parquet::native
