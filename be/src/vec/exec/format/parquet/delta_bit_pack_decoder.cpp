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

namespace doris::vectorized {

template <typename T>
Status DeltaBitPackDecoder<T>::_init_header() {
    if (!_decoder->GetVlqInt(&_values_per_block) || !_decoder->GetVlqInt(&_mini_blocks_per_block) ||
        !_decoder->GetVlqInt(&_total_value_count) || !_decoder->GetZigZagVlqInt(&_last_value)) {
        return Status::EndOfFile("Init header eof");
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
    // init as empty property
    _delta_bit_widths = Slice();
    _delta_bit_widths.size = _mini_blocks_per_block;

    _block_initialized = false;
    _values_remaining_current_mini_block = 0;
    return Status::OK();
}

template <typename T>
Status DeltaBitPackDecoder<T>::_init_block() {
    DCHECK_GT(_total_values_remaining, 0) << "InitBlock called at EOF";
    if (!_decoder->GetZigZagVlqInt(&_min_delta)) {
        return Status::EndOfFile("Init block eof");
    }

    // read the bitwidth of each miniblock
    uint8_t* bit_width_data = reinterpret_cast<uint8_t*>(_delta_bit_widths.mutable_data());
    for (uint32_t i = 0; i < _mini_blocks_per_block; ++i) {
        if (!_decoder->GetAligned<uint8_t>(1, bit_width_data + i)) {
            return Status::EndOfFile("Decode bit-width EOF");
        }
        // Note that non-conformant bitwidth entries are allowed by the Parquet spec
        // for extraneous miniblocks in the last block (GH-14923), so we check
        // the bitwidths when actually using them (see InitMiniBlock()).
    }
    _mini_block_idx = 0;
    _block_initialized = true;
    _init_mini_block(bit_width_data[0]);
    return Status::OK();
}

template <typename T>
Status DeltaBitPackDecoder<T>::_init_mini_block(int bit_width) {
    if (bit_width > kMaxDeltaBitWidth) {
        return Status::InvalidArgument("delta bit width larger than integer bit width");
    }
    _delta_bit_width = bit_width;
    _values_remaining_current_mini_block = _values_per_mini_block;
    return Status::OK();
}

template <typename T>
Status DeltaBitPackDecoder<T>::_get_internal(T* buffer, int num_values, int* out) {
    num_values = static_cast<int>(std::min<int64_t>(num_values, _total_values_remaining));
    if (num_values == 0) {
        *out = 0;
        return Status::OK();
    }

    int i = 0;
    while (i < num_values) {
        if (_values_remaining_current_mini_block == 0) {
            if (!_block_initialized) {
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
                    RETURN_IF_ERROR(_init_mini_block(_delta_bit_widths.data[_mini_block_idx]));
                } else {
                    RETURN_IF_ERROR(_init_block());
                }
            }
        }

        int values_decode = std::min(_values_remaining_current_mini_block,
                                     static_cast<uint32_t>(num_values - i));
        for (int j = 0; j < values_decode; ++j) {
            if (!_decoder->GetValue(_delta_bit_width, buffer + i + j)) {
                return Status::EndOfFile("get batch EOF");
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

    if (_total_values_remaining == 0) {
        uint32_t padding_bits = _values_remaining_current_mini_block * _delta_bit_width;
        // skip the padding bits
        if (!_decoder->Advance(padding_bits)) {
            return Status::EndOfFile("Skip padding_bits EOF");
        }
        _values_remaining_current_mini_block = 0;
    }
    *out = num_values;
    return Status::OK();
}
} // namespace doris::vectorized