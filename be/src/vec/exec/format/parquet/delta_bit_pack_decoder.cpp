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

#include "delta_bit_pack_decoder.h"

#include <string.h>

#include <algorithm>
#include <string_view>

#include "vec/columns/column.h"
#include "vec/common/arithmetic_overflow.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

template <typename T>
Status DeltaBitPackDecoder<T>::_init_header() {
    if (!_bit_reader->GetVlqInt(&_values_per_block) ||
        !_bit_reader->GetVlqInt(&_mini_blocks_per_block) ||
        !_bit_reader->GetVlqInt(&_total_value_count) ||
        !_bit_reader->GetZigZagVlqInt(&_last_value)) {
        return Status::IOError("Init header eof");
    }
    if (_values_per_block == 0) {
        return Status::InvalidArgument("Cannot have zero value per block");
    }
    if (_values_per_block % 128 != 0) {
        return Status::InvalidArgument(
                "the number of values in a block must be multiple of 128, but it's " +
                std::to_string(_values_per_block));
    }
    if (_mini_blocks_per_block == 0) {
        return Status::InvalidArgument("Cannot have zero miniblock per block");
    }
    _values_per_mini_block = _values_per_block / _mini_blocks_per_block;
    if (_values_per_mini_block == 0) {
        return Status::InvalidArgument("Cannot have zero value per miniblock");
    }
    if (_values_per_mini_block % 32 != 0) {
        return Status::InvalidArgument(
                "The number of values in a miniblock must be multiple of 32, but it's " +
                std::to_string(_values_per_mini_block));
    }
    _total_values_remaining = _total_value_count;
    _delta_bit_widths.resize(_mini_blocks_per_block);
    // init as empty property
    _block_initialized = false;
    _values_remaining_current_mini_block = 0;
    return Status::OK();
}

template <typename T>
Status DeltaBitPackDecoder<T>::_init_block() {
    DCHECK_GT(_total_values_remaining, 0) << "InitBlock called at EOF";
    if (!_bit_reader->GetZigZagVlqInt(&_min_delta)) {
        return Status::IOError("Init block eof");
    }

    // read the bitwidth of each miniblock
    uint8_t* bit_width_data = _delta_bit_widths.data();
    for (uint32_t i = 0; i < _mini_blocks_per_block; ++i) {
        if (!_bit_reader->GetAligned<uint8_t>(1, bit_width_data + i)) {
            return Status::IOError("Decode bit-width EOF");
        }
        // Note that non-conformant bitwidth entries are allowed by the Parquet spec
        // for extraneous miniblocks in the last block (GH-14923), so we check
        // the bitwidths when actually using them (see InitMiniBlock()).
    }
    _mini_block_idx = 0;
    _block_initialized = true;
    RETURN_IF_ERROR(_init_mini_block(bit_width_data[0]));
    return Status::OK();
}

template <typename T>
Status DeltaBitPackDecoder<T>::_init_mini_block(int bit_width) {
    if (PREDICT_FALSE(bit_width > kMaxDeltaBitWidth)) {
        return Status::InvalidArgument("delta bit width larger than integer bit width");
    }
    _delta_bit_width = bit_width;
    _values_remaining_current_mini_block = _values_per_mini_block;
    return Status::OK();
}

template <typename T>
Status DeltaBitPackDecoder<T>::_get_internal(T* buffer, int num_values, int* out_num_values) {
    num_values = static_cast<int>(std::min<int64_t>(num_values, _total_values_remaining));
    if (num_values == 0) {
        *out_num_values = 0;
        return Status::OK();
    }
    int i = 0;
    while (i < num_values) {
        if (PREDICT_FALSE(_values_remaining_current_mini_block == 0)) {
            if (PREDICT_FALSE(!_block_initialized)) {
                buffer[i++] = _last_value;
                DCHECK_EQ(i, 1); // we're at the beginning of the page
                if (i == num_values) {
                    // When block is uninitialized and i reaches num_values we have two
                    // different possibilities:
                    // 1. _total_value_count == 1, which means that the page may have only
                    // one value (encoded in the header), and we should not initialize
                    // any block.
                    // 2. _total_value_count != 1, which means we should initialize the
                    // incoming block for subsequent reads.
                    if (_total_value_count != 1) {
                        RETURN_IF_ERROR(_init_block());
                    }
                    break;
                }
                RETURN_IF_ERROR(_init_block());
            } else {
                ++_mini_block_idx;
                if (_mini_block_idx < _mini_blocks_per_block) {
                    RETURN_IF_ERROR(_init_mini_block(_delta_bit_widths.data()[_mini_block_idx]));
                } else {
                    RETURN_IF_ERROR(_init_block());
                }
            }
        }

        int values_decode = std::min(_values_remaining_current_mini_block,
                                     static_cast<uint32_t>(num_values - i));
        for (int j = 0; j < values_decode; ++j) {
            if (!_bit_reader->GetValue(_delta_bit_width, buffer + i + j)) {
                return Status::IOError("Get batch EOF");
            }
        }
        for (int j = 0; j < values_decode; ++j) {
            // Addition between min_delta, packed int and last_value should be treated as
            // unsigned addition. Overflow is as expected.
            buffer[i + j] = static_cast<UT>(_min_delta) + static_cast<UT>(buffer[i + j]) +
                            static_cast<UT>(_last_value);
            _last_value = buffer[i + j];
        }
        _values_remaining_current_mini_block -= values_decode;
        i += values_decode;
    }
    _total_values_remaining -= num_values;

    if (PREDICT_FALSE(_total_values_remaining == 0)) {
        if (!_bit_reader->Advance(_delta_bit_width * _values_remaining_current_mini_block)) {
            return Status::IOError("Skip padding EOF");
        }
        _values_remaining_current_mini_block = 0;
    }
    *out_num_values = num_values;
    return Status::OK();
}

void DeltaLengthByteArrayDecoder::_decode_lengths() {
    _len_decoder.set_bit_reader(_bit_reader);
    // get the number of encoded lengths
    int num_length = _len_decoder.valid_values_count();
    _buffered_length.resize(num_length);

    // decode all the lengths. all the lengths are buffered in buffered_length_.
    int ret;
    Status st = _len_decoder.decode(_buffered_length.data(), num_length, &ret);
    if (st != Status::OK()) {
        LOG(FATAL) << "Fail to decode delta length, status: " << st;
    }
    DCHECK_EQ(ret, num_length);
    _length_idx = 0;
    _num_valid_values = num_length;
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
        if (PREDICT_FALSE(len < 0)) {
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
    if (PREDICT_FALSE(suffix_read != max_values)) {
        return Status::IOError("Read {}, expecting {} from suffix decoder",
                               std::to_string(suffix_read), std::to_string(max_values));
    }

    int64_t data_size = 0;
    const int32_t* prefix_len_ptr = _buffered_prefix_length.data() + _prefix_len_offset;
    for (int i = 0; i < max_values; ++i) {
        if (PREDICT_FALSE(prefix_len_ptr[i] < 0)) {
            return Status::InvalidArgument("negative prefix length in DELTA_BYTE_ARRAY");
        }
        if (PREDICT_FALSE(common::add_overflow(data_size, static_cast<int64_t>(prefix_len_ptr[i]),
                                               data_size) ||
                          common::add_overflow(data_size, static_cast<int64_t>(buffer[i].size),
                                               data_size))) {
            return Status::InvalidArgument("excess expansion in DELTA_BYTE_ARRAY");
        }
    }
    _buffered_data.resize(data_size);

    std::string_view prefix {_last_value};

    char* data_ptr = _buffered_data.data();
    for (int i = 0; i < max_values; ++i) {
        if (PREDICT_FALSE(static_cast<size_t>(prefix_len_ptr[i]) > prefix.length())) {
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
} // namespace doris::vectorized
