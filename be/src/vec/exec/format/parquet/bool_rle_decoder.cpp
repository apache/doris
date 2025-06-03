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

#include "vec/exec/format/parquet/bool_rle_decoder.h"

#include <glog/logging.h>

#include <algorithm>
#include <ostream>
#include <string>

#include "util/coding.h"
#include "util/slice.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"
#include "vec/exec/format/parquet/parquet_common.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
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

Status BoolRLEDecoder::decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                     ColumnSelectVector& select_vector, bool is_dict_filter) {
    if (select_vector.has_filter()) {
        return _decode_values<true>(doris_column, data_type, select_vector, is_dict_filter);
    } else {
        return _decode_values<false>(doris_column, data_type, select_vector, is_dict_filter);
    }
}

template <bool has_filter>
Status BoolRLEDecoder::_decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                      ColumnSelectVector& select_vector, bool is_dict_filter) {
    auto& column_data = assert_cast<ColumnUInt8*>(doris_column.get())->get_data();
    size_t data_index = column_data.size();
    column_data.resize(data_index + select_vector.num_values() - select_vector.num_filtered());
    size_t max_values = select_vector.num_values() - select_vector.num_nulls();
    _values.resize(max_values);
    if (!_decoder.get_values(_values.data(), max_values)) {
        return Status::IOError("Can't read enough booleans in rle decoder");
    }
    size_t current_value_idx = 0;
    ColumnSelectVector::DataReadType read_type;
    while (size_t run_length = select_vector.get_next_run<has_filter>(&read_type)) {
        switch (read_type) {
        case ColumnSelectVector::CONTENT: {
            bool value; // Can't use uint8_t directly, we should correct it.
            for (size_t i = 0; i < run_length; ++i) {
                DCHECK(current_value_idx < max_values)
                        << current_value_idx << " vs. " << max_values;
                value = _values[current_value_idx++];
                column_data[data_index++] = (UInt8)value;
            }
            break;
        }
        case ColumnSelectVector::NULL_DATA: {
            data_index += run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_CONTENT: {
            current_value_idx += run_length;
            break;
        }
        case ColumnSelectVector::FILTERED_NULL: {
            break;
        }
        }
    }
    return Status::OK();
}
#include "common/compile_check_end.h"

} // namespace doris::vectorized
