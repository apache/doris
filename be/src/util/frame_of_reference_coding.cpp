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

#include "util/frame_of_reference_coding.h"

#include <algorithm>
#include <cstring>

#include "util/bit_util.h"
#include "util/coding.h"

namespace doris {

static inline uint8_t bits(const uint64_t v) {
    return v == 0 ? 0 : 64 - __builtin_clzll(v);
}

template<typename T>
const T* ForEncoder<T>::copy_value(const T *p_data, size_t count) {
    memcpy(&_buffered_values[_buffered_values_num], p_data, count * sizeof(T));
    _buffered_values_num += count;
    p_data += count;
    return p_data;
}

template<typename T>
void ForEncoder<T>::put_batch(const T* in_data, size_t count) {
    if (_buffered_values_num + count < ForCoding::FRAME_VALUE_NUM) {
        copy_value(in_data, count);
        _values_num += count;
        return;
    }

    // 1. padding one frame
    size_t padding_num = ForCoding::FRAME_VALUE_NUM - _buffered_values_num;
    in_data = copy_value(in_data, padding_num);
    bit_packing_one_frame_value(_buffered_values);

    // 2. process frame by frame
    size_t frame_size = (count - padding_num) / ForCoding::FRAME_VALUE_NUM;
    for (size_t i = 0; i < frame_size; i++) {
        // directly encode value to the bit_writer, don't buffer the value
        _buffered_values_num = ForCoding::FRAME_VALUE_NUM;
        bit_packing_one_frame_value(in_data);
        in_data += ForCoding::FRAME_VALUE_NUM;
    }

    // 3. process remaining value
    size_t remaining_num = (count - padding_num) % ForCoding::FRAME_VALUE_NUM;
    if (remaining_num > 0) {
        copy_value(in_data, remaining_num);
    }

    _values_num += count;
}

// todo(kks): improve this method by SIMD instructions

// Use as few bit as possible to store a piece of integer data.
// param[in] input: the integer list need to pack
// param[in] in_num: the number integer need to pack
// param[in] bit_width: how many bit we use to store each integer data
// param[out] out: the packed result

// For example:
// The input is int32 list: 1, 2, 4, 8 and bit_width is 4
// The output will be: 0001 0010 0100 1000
template<typename T>
void ForEncoder<T>::bit_pack(T *input, uint8_t in_num, int bit_width, uint8_t *output) {
    if (in_num == 0 || bit_width == 0) {
        return;
    }

    T in_mask = 0;
    int bit_index = 0;
    *output = 0;
    for (int i = 0; i < in_num; i++) {
        in_mask = 1 << (bit_width - 1);
        for (int k = 0; k < bit_width; k++) {
            if (bit_index > 7) {
                bit_index = 0;
                output++;
                *output = 0;
            }
            *output |= (((input[i] & in_mask) >> (bit_width - k - 1)) << (7 - bit_index));
            in_mask >>= 1;
            bit_index++;
        }
    }
}

template<typename T>
void ForEncoder<T>::bit_packing_one_frame_value(const T* input) {
    T min = input[0];
    T max = input[0];
    bool is_ascending = true;
    uint8_t bit_width = 0;

    for (uint8_t i = 1; i < _buffered_values_num; ++i) {
        if (is_ascending) {
            if (input[i] < input[i - 1]) {
                is_ascending = false;
            } else {
                bit_width = std::max(bit_width, bits(input[i]- input[i - 1])) ;
            }
        }

        if (input[i] < min) {
            min = input[i];
            continue;
        }

        if (input[i] > max) {
            max = input[i];
        }
    }

    if (sizeof(T) == 8) {
        put_fixed64_le(_buffer, min);
    } else {
        put_fixed32_le(_buffer, min);
    }

    // improve for ascending order input, we could use fewer bit
    T delta_values[ForCoding::FRAME_VALUE_NUM];
    u_int8_t order_flag = 0;
    if (is_ascending) {
        delta_values[0] = 0;
        for (uint8_t i = 1; i < _buffered_values_num; ++i) {
            delta_values[i] = input[i] - input[i - 1];
        }
        order_flag = 1;
    } else {
        bit_width = bits(static_cast<T>(max - min));
        for (uint8_t i = 0; i < _buffered_values_num; ++i) {
            delta_values[i] = input[i] - min;
        }
    }
    // 2 bit order_flag + 6 bit bit_width
    uint8_t order_flag_and_bit_width = order_flag << 6 | bit_width;
    _order_flag_and_bit_widths.push_back(order_flag_and_bit_width);

    uint32_t packing_len = BitUtil::Ceil(_buffered_values_num * bit_width, 8);

    _buffer->reserve(_buffer->size() + packing_len);
    bit_pack(delta_values, _buffered_values_num, bit_width, _buffer->data() + _buffer->size());
    _buffer->resize(_buffer->size() + packing_len);

    _buffered_values_num = 0;
}

template<typename T>
uint32_t ForEncoder<T>::flush() {
    if (_buffered_values_num != 0) {
        bit_packing_one_frame_value(_buffered_values);
    }

    // write the footer:
    // 1 _order_flags and _bit_widths
    for (auto value: _order_flag_and_bit_widths) {
        _buffer->append(&value, 1);
    }
    // 2 _values_num
    put_fixed32_le(_buffer, _values_num);

    return _buffer->size();
}


template<typename T>
bool ForDecoder<T>::init() {
    // When row count is zero, the minimum footer size is 4:
    // only has ValuesNum(4)
    if (_buffer_len < 4) {
        return false;
    }

    _values_num = decode_fixed32_le(_buffer + _buffer_len - 4);
    _frame_count = _values_num / ForCoding::FRAME_VALUE_NUM +
                   (_values_num % ForCoding::FRAME_VALUE_NUM != 0);

    if (_values_num % ForCoding::FRAME_VALUE_NUM != 0) {
        _last_frame_num = _values_num % ForCoding::FRAME_VALUE_NUM;
    }

    size_t bit_width_offset = _buffer_len - 4 - _frame_count;
    if (bit_width_offset < 0) {
        return false;
    }
    
    // read order_flags, bit_widths and compute frame_offsets
    uint8_t mask = (1 << 6) - 1;
    u_int32_t frame_start_offset = 0;
    for (uint32_t i = 0; i < _frame_count; i++ ) {
        uint32_t order_flag_and_bit_width = decode_fixed8(_buffer + bit_width_offset);

        uint8_t bit_width = order_flag_and_bit_width & mask;
        uint8_t order_flag = order_flag_and_bit_width >> 6;
        _bit_widths.push_back(bit_width);
        _order_flags.push_back(order_flag);

        bit_width_offset += 1;

        _frame_offsets.push_back(frame_start_offset);
        if (sizeof(T) == 8) {
            frame_start_offset +=  bit_width * ForCoding::FRAME_VALUE_NUM / 8 + 8;
        } else {
            frame_start_offset +=  bit_width * ForCoding::FRAME_VALUE_NUM / 8 + 4;
        }
    }

    return true;
}

// todo(kks): improve this method by SIMD instructions
// The reverse of bit_pack method, get original integer data list from packed bits
// param[in] input: the packed bits need to unpack
// param[in] in_num: the integer number in packed bits
// param[in] bit_width: how many bit we used to store each integer data
// param[out] output: the original integer data list
template<typename T>
void ForDecoder<T>::bit_unpack(const uint8_t *input, uint8_t in_num, int bit_width, T *output) {
    unsigned char in_mask = 0x80;
    int bit_index = 0;
    while (in_num > 0) {
        *output = 0;
        for (int i = 0; i < bit_width; i++) {
            if (bit_index > 7) {
                input++;
                bit_index = 0;
            }
            *output |= ((T)((*input & (in_mask >> bit_index)) >> (7 - bit_index))) << (bit_width - i - 1);
            bit_index++;
        }
        output++;
        in_num--;
    }
}

template<typename T>
void ForDecoder<T>::decode_current_frame(T* output) {
    _current_decoded_frame = _current_index / ForCoding::FRAME_VALUE_NUM;
    uint8_t frame_value_num = ForCoding::FRAME_VALUE_NUM;
    if (_current_decoded_frame == _frame_count - 1) {
        frame_value_num = _last_frame_num;
    }

    uint32_t base_offset = _frame_offsets[_current_decoded_frame];
    T min = 0;
    uint32_t delta_offset = 0;
    if (sizeof(T) == 8) {
        min = decode_fixed64_le(_buffer + base_offset);
        delta_offset = base_offset + 8;
    } else {
        min = decode_fixed32_le(_buffer + base_offset);
        delta_offset = base_offset + 4;
    }

    uint8_t bit_width = _bit_widths[_current_decoded_frame];
    bool is_ascending = _order_flags[_current_decoded_frame];

    T delta_values[ForCoding::FRAME_VALUE_NUM] = {0};
    bit_unpack(_buffer + delta_offset, frame_value_num, bit_width, delta_values);
    if (is_ascending) {
        T pre_value = min;
        for (uint8_t i = 0; i < frame_value_num; i ++) {
            T value = delta_values[i] + pre_value;
            output[i] = value;
            pre_value = value;
        }
    } else {
        for (uint8_t i = 0; i < frame_value_num; i ++) {
            output[i] = delta_values[i] + min;
        }
    }
}

template<typename T>
T* ForDecoder<T>::copy_value(T* val, size_t count) {
    memcpy(val, &_out_buffer[_current_index % ForCoding::FRAME_VALUE_NUM], sizeof(T) * count);
    _current_index += count;
    val += count;
    return val;
}

template<typename T>
bool ForDecoder<T>::get_batch(T* val, size_t count) {
    if (_current_index + count > _values_num) {
        return false;
    }

    if (need_decode_frame()) {
        decode_current_frame(_out_buffer);
    }

    if (_current_index + count < ForCoding::FRAME_VALUE_NUM * (_current_decoded_frame + 1)) {
        copy_value(val, count);
        return true;
    }

    // 1. padding one frame
    size_t padding_num = ForCoding::FRAME_VALUE_NUM * (_current_decoded_frame + 1) - _current_index;
    val = copy_value(val, padding_num);

    // 2. process frame by frame
    size_t frame_size = (count - padding_num) / ForCoding::FRAME_VALUE_NUM;
    for (size_t i = 0; i < frame_size; i++) {
        // directly decode value to the output, don't  buffer the value
        decode_current_frame(val);
        _current_index += ForCoding::FRAME_VALUE_NUM;
        val += ForCoding::FRAME_VALUE_NUM;
    }

    // 3. process remaining value
    size_t remaining_num = (count - padding_num) % ForCoding::FRAME_VALUE_NUM;
    if (remaining_num > 0) {
        decode_current_frame(_out_buffer);
        val = copy_value(val, remaining_num);
    }

    return true;
}

template<typename T>
bool ForDecoder<T>::skip(int32_t skip_num) {
    if (_current_index + skip_num >= _values_num || _current_index + skip_num < 0) {
        return false;
    }
    _current_index = _current_index + skip_num;
    return true;
}

template class ForEncoder<int8_t>;
template class ForEncoder<int16_t>;
template class ForEncoder<int32_t>;
template class ForEncoder<int64_t>;
template class ForEncoder<uint8_t>;
template class ForEncoder<uint16_t>;
template class ForEncoder<uint32_t>;
template class ForEncoder<uint64_t>;

template class ForDecoder<int8_t>;
template class ForDecoder<int16_t>;
template class ForDecoder<int32_t>;
template class ForDecoder<int64_t>;
template class ForDecoder<uint8_t>;
template class ForDecoder<uint16_t>;
template class ForDecoder<uint32_t>;
template class ForDecoder<uint64_t>;
}
