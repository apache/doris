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

#include "vec/exec/format/parquet/delta_bit_pack_decoder.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
Status DeltaLengthByteArrayDecoder::_decode_lengths() {
    RETURN_IF_ERROR(_len_decoder.set_bit_reader(_bit_reader));
    // get the number of encoded lengths
    int num_length = _len_decoder.valid_values_count();
    _buffered_length.resize(num_length);

    // decode all the lengths. all the lengths are buffered in buffered_length_.
    uint32_t ret;
    RETURN_IF_ERROR(_len_decoder.decode(_buffered_length.data(), num_length, &ret));
    DCHECK_EQ(ret, num_length);
    _length_idx = 0;
    _num_valid_values = num_length;
    return Status::OK();
}

Status DeltaLengthByteArrayDecoder::_get_internal(Slice* buffer, int max_values,
                                                  int* out_num_values) {
    // Decode up to `max_values` strings into an internal buffer
    // and reference them into `buffer`.
    max_values = std::min(max_values, _num_valid_values);
    if (max_values == 0) {
        *out_num_values = 0;
        return Status::OK();
    }

    int32_t data_size = 0;
    const int32_t* length_ptr = _buffered_length.data() + _length_idx;
    for (int i = 0; i < max_values; ++i) {
        int32_t len = length_ptr[i];
        if (len < 0) [[unlikely]] {
            return Status::InvalidArgument("Negative string delta length");
        }
        buffer[i].size = len;
        if (common::add_overflow(data_size, len, data_size)) {
            return Status::InvalidArgument("Excess expansion in DELTA_(LENGTH_)BYTE_ARRAY");
        }
    }
    _length_idx += max_values;

    _buffered_data.resize(data_size);
    char* data_ptr = _buffered_data.data();
    for (int j = 0; j < data_size; j++) {
        if (!_bit_reader->GetValue(8, data_ptr + j)) {
            return Status::IOError("Get length bytes EOF");
        }
    }

    for (int i = 0; i < max_values; ++i) {
        buffer[i].data = data_ptr;
        data_ptr += buffer[i].size;
    }
    // this->num_values_ -= max_values;
    _num_valid_values -= max_values;
    *out_num_values = max_values;
    return Status::OK();
}

Status DeltaByteArrayDecoder::_get_internal(Slice* buffer, int max_values, int* out_num_values) {
    // Decode up to `max_values` strings into an internal buffer
    // and reference them into `buffer`.
    max_values = std::min(max_values, _num_valid_values);
    if (max_values == 0) {
        *out_num_values = max_values;
        return Status::OK();
    }

    int suffix_read;
    RETURN_IF_ERROR(_suffix_decoder.decode(buffer, max_values, &suffix_read));
    if (suffix_read != max_values) [[unlikely]] {
        return Status::IOError("Read {}, expecting {} from suffix decoder",
                               std::to_string(suffix_read), std::to_string(max_values));
    }

    int64_t data_size = 0;
    const int32_t* prefix_len_ptr = _buffered_prefix_length.data() + _prefix_len_offset;
    for (int i = 0; i < max_values; ++i) {
        if (prefix_len_ptr[i] < 0) [[unlikely]] {
            return Status::InvalidArgument("negative prefix length in DELTA_BYTE_ARRAY");
        }
        if (common::add_overflow(data_size, static_cast<int64_t>(prefix_len_ptr[i]), data_size) ||
            common::add_overflow(data_size, static_cast<int64_t>(buffer[i].size), data_size))
                [[unlikely]] {
            return Status::InvalidArgument("excess expansion in DELTA_BYTE_ARRAY");
        }
    }
    _buffered_data.resize(data_size);

    std::string_view prefix {_last_value};

    char* data_ptr = _buffered_data.data();
    for (int i = 0; i < max_values; ++i) {
        if (static_cast<size_t>(prefix_len_ptr[i]) > prefix.length()) [[unlikely]] {
            return Status::InvalidArgument("prefix length too large in DELTA_BYTE_ARRAY");
        }
        memcpy(data_ptr, prefix.data(), prefix_len_ptr[i]);
        // buffer[i] currently points to the string suffix
        memcpy(data_ptr + prefix_len_ptr[i], buffer[i].data, buffer[i].size);
        buffer[i].data = data_ptr;
        buffer[i].size += prefix_len_ptr[i];
        data_ptr += buffer[i].size;
        prefix = std::string_view {buffer[i].data, buffer[i].size};
    }
    _prefix_len_offset += max_values;
    _num_valid_values -= max_values;
    _last_value = std::string {prefix};

    if (_num_valid_values == 0) {
        _last_value_in_previous_page = _last_value;
    }
    *out_num_values = max_values;
    return Status::OK();
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized
