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

template <typename T>
const T* ForEncoder<T>::copy_value(const T* p_data, size_t count) {
    memcpy(&_buffered_values[_buffered_values_num], p_data, count * sizeof(T));
    _buffered_values_num += count;
    p_data += count;
    return p_data;
}

template <typename T>
void ForEncoder<T>::put_batch(const T* in_data, size_t count) {
    if (_buffered_values_num + count < FRAME_VALUE_NUM) {
        copy_value(in_data, count);
        _values_num += count;
        return;
    }

    // 1. padding one frame
    size_t padding_num = FRAME_VALUE_NUM - _buffered_values_num;
    in_data = copy_value(in_data, padding_num);
    bit_packing_one_frame_value(_buffered_values);

    // 2. process frame by frame
    size_t frame_size = (count - padding_num) / FRAME_VALUE_NUM;
    for (size_t i = 0; i < frame_size; i++) {
        // directly encode value to the bit_writer, don't buffer the value
        _buffered_values_num = FRAME_VALUE_NUM;
        bit_packing_one_frame_value(in_data);
        in_data += FRAME_VALUE_NUM;
    }

    // 3. process remaining value
    size_t remaining_num = (count - padding_num) % FRAME_VALUE_NUM;
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
template <typename T>
void ForEncoder<T>::bit_pack(const T* input, uint8_t in_num, int bit_width, uint8_t* output) {
    if (in_num == 0 || bit_width == 0) {
        return;
    }

    T in_mask = 0;
    int bit_index = 0;
    *output = 0;
    for (int i = 0; i < in_num; i++) {
        in_mask = ((T)1) << (bit_width - 1);
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

template <typename T>
void ForEncoder<T>::bit_packing_one_frame_value(const T* input) {
    T min = input[0];
    T max = input[0];
    bool is_ascending = true;
    uint8_t bit_width = 0;
    T half_max_delta = numeric_limits_max() >> 1;
    bool is_keep_original_value = false;

    // 1. make sure order_flag, save_original_value, and find max&min.
    for (uint8_t i = 1; i < _buffered_values_num; ++i) {
        if (is_ascending) {
            if (input[i] < input[i - 1]) {
                is_ascending = false;
            } else {
                if ((input[i] >> 1) - (input[i - 1] >> 1) > half_max_delta) { // overflow
                    is_keep_original_value = true;
                } else {
                    bit_width = std::max(bit_width, bits(input[i] - input[i - 1]));
                }
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
    if (!is_ascending) {
        if ((max >> 1) - (min >> 1) > half_max_delta) {
            is_keep_original_value = true;
        }
    }

    // 2. save min value.
    if (sizeof(T) == 16) {
        put_fixed128_le(_buffer, min);
    } else if (sizeof(T) == 8) {
        put_fixed64_le(_buffer, min);
    } else {
        put_fixed32_le(_buffer, min);
    }

    // 3.1 save original value.
    if (is_keep_original_value) {
        bit_width = sizeof(T) * 8;
        uint32_t len = _buffered_values_num * bit_width;
        _buffer->reserve(_buffer->size() + len);
        size_t origin_size = _buffer->size();
        _buffer->resize(origin_size + len);
        bit_pack(input, _buffered_values_num, bit_width, _buffer->data() + origin_size);
    } else {
        // 3.2 bit pack.
        // improve for ascending order input, we could use fewer bit
        T delta_values[FRAME_VALUE_NUM];
        if (is_ascending) {
            delta_values[0] = 0;
            for (uint8_t i = 1; i < _buffered_values_num; ++i) {
                delta_values[i] = input[i] - input[i - 1];
            }
        } else {
            bit_width = bits(static_cast<T>(max - min));
            for (uint8_t i = 0; i < _buffered_values_num; ++i) {
                delta_values[i] = input[i] - min;
            }
        }

        uint32_t packing_len = BitUtil::Ceil(_buffered_values_num * bit_width, 8);

        _buffer->reserve(_buffer->size() + packing_len);
        size_t origin_size = _buffer->size();
        _buffer->resize(origin_size + packing_len);
        bit_pack(delta_values, _buffered_values_num, bit_width, _buffer->data() + origin_size);
    }
    uint8_t storage_format = 0;
    if (is_keep_original_value) {
        storage_format = 2;
    } else if (is_ascending) {
        storage_format = 1;
    }
    _storage_formats.push_back(storage_format);
    _bit_widths.push_back(bit_width);

    _buffered_values_num = 0;
}

template <typename T>
uint32_t ForEncoder<T>::flush() {
    if (_buffered_values_num != 0) {
        bit_packing_one_frame_value(_buffered_values);
    }

    // write the footer:
    // 1 _storage_formats and bit_widths
    DCHECK(_storage_formats.size() == _bit_widths.size())
            << "Size of _storage_formats and _bit_widths should be equal.";
    for (size_t i = 0; i < _storage_formats.size(); i++) {
        _buffer->append(&_storage_formats[i], 1);
        _buffer->append(&_bit_widths[i], 1);
    }
    // 2 frame_value_num and values_num
    uint8_t frame_value_num = FRAME_VALUE_NUM;
    _buffer->append(&frame_value_num, 1);
    put_fixed32_le(_buffer, _values_num);

    return _buffer->size();
}

template <typename T>
const T ForEncoder<T>::numeric_limits_max() {
    return std::numeric_limits<T>::max();
}

template <>
const uint24_t ForEncoder<uint24_t>::numeric_limits_max() {
    return 0XFFFFFF;
}

template <typename T>
bool ForDecoder<T>::init() {
    // When row count is zero, the minimum footer size is 5:
    // only has ValuesNum(4) + FrameValueNum(1)
    if (_buffer_len < 5) {
        return false;
    }

    _max_frame_size = decode_fixed8(_buffer + _buffer_len - 5);
    _values_num = decode_fixed32_le(_buffer + _buffer_len - 4);
    _frame_count = _values_num / _max_frame_size + (_values_num % _max_frame_size != 0);
    _last_frame_size = _max_frame_size - (_max_frame_size * _frame_count - _values_num);

    size_t bit_width_offset = _buffer_len - 5 - _frame_count * 2;
    if (bit_width_offset < 0) {
        return false;
    }

    // read _storage_formats, bit_widths and compute frame_offsets
    u_int32_t frame_start_offset = 0;
    for (uint32_t i = 0; i < _frame_count; i++) {
        uint8_t order_flag = decode_fixed8(_buffer + bit_width_offset);
        uint8_t bit_width = decode_fixed8(_buffer + bit_width_offset + 1);
        _bit_widths.push_back(bit_width);
        _storage_formats.push_back(order_flag);

        bit_width_offset += 2;

        _frame_offsets.push_back(frame_start_offset);
        if (sizeof(T) == 16) {
            frame_start_offset += bit_width * _max_frame_size / 8 + 16;
        } else if (sizeof(T) == 8) {
            frame_start_offset += bit_width * _max_frame_size / 8 + 8;
        } else {
            frame_start_offset += bit_width * _max_frame_size / 8 + 4;
        }
    }

    _out_buffer.reserve(_max_frame_size);
    _parsed = true;

    return true;
}

// todo(kks): improve this method by SIMD instructions
// The reverse of bit_pack method, get original integer data list from packed bits
// param[in] input: the packed bits need to unpack
// param[in] in_num: the integer number in packed bits
// param[in] bit_width: how many bit we used to store each integer data
// param[out] output: the original integer data list
template <typename T>
void ForDecoder<T>::bit_unpack(const uint8_t* input, uint8_t in_num, int bit_width, T* output) {
    unsigned char in_mask = 0x80;
    int bit_index = 0;
    while (in_num > 0) {
        *output = 0;
        for (int i = 0; i < bit_width; i++) {
            if (bit_index > 7) {
                input++;
                bit_index = 0;
            }
            *output |= ((T)((*input & (in_mask >> bit_index)) >> (7 - bit_index)))
                       << (bit_width - i - 1);
            bit_index++;
        }
        output++;
        in_num--;
    }
}

template <typename T>
void ForDecoder<T>::decode_current_frame(T* output) {
    uint32_t frame_index = _current_index / _max_frame_size;
    if (frame_index == _current_decoded_frame) {
        return; // current frame already decoded
    }
    _current_decoded_frame = frame_index;
    uint8_t current_frame_size = frame_size(frame_index);

    uint32_t base_offset = _frame_offsets[_current_decoded_frame];
    T min = 0;
    uint32_t delta_offset = 0;
    if (sizeof(T) == 16) {
        min = decode_fixed128_le(_buffer + base_offset);
        delta_offset = base_offset + 16;
    } else if (sizeof(T) == 8) {
        min = decode_fixed64_le(_buffer + base_offset);
        delta_offset = base_offset + 8;
    } else {
        min = decode_fixed32_le(_buffer + base_offset);
        delta_offset = base_offset + 4;
    }

    uint8_t bit_width = _bit_widths[_current_decoded_frame];

    bool is_original_value = _storage_formats[_current_decoded_frame] == 2;
    if (is_original_value) {
        bit_unpack(_buffer + delta_offset, current_frame_size, bit_width, output);
    } else {
        bool is_ascending = _storage_formats[_current_decoded_frame] == 1;
        std::vector<T> delta_values(current_frame_size);
        bit_unpack(_buffer + delta_offset, current_frame_size, bit_width, delta_values.data());
        if (is_ascending) {
            T pre_value = min;
            for (uint8_t i = 0; i < current_frame_size; i++) {
                T value = delta_values[i] + pre_value;
                output[i] = value;
                pre_value = value;
            }
        } else {
            for (uint8_t i = 0; i < current_frame_size; i++) {
                output[i] = delta_values[i] + min;
            }
        }
    }
}

template <typename T>
T ForDecoder<T>::decode_frame_min_value(uint32_t frame_index) {
    uint32_t min_offset = _frame_offsets[frame_index];
    T min = 0;
    if (sizeof(T) == 16) {
        min = decode_fixed128_le(_buffer + min_offset);
    } else if (sizeof(T) == 8) {
        min = decode_fixed64_le(_buffer + min_offset);
    } else {
        min = decode_fixed32_le(_buffer + min_offset);
    }
    return min;
}

template <typename T>
T* ForDecoder<T>::copy_value(T* val, size_t count) {
    memcpy(val, &_out_buffer[_current_index % _max_frame_size], sizeof(T) * count);
    _current_index += count;
    val += count;
    return val;
}

template <typename T>
bool ForDecoder<T>::get_batch(T* val, size_t count) {
    if (_current_index + count > _values_num) {
        return false;
    }

    decode_current_frame(_out_buffer.data());

    if (_current_index + count < _max_frame_size * (_current_decoded_frame + 1)) {
        copy_value(val, count);
        return true;
    }

    // 1. padding one frame
    size_t padding_num = _max_frame_size * (_current_decoded_frame + 1) - _current_index;
    val = copy_value(val, padding_num);

    // 2. process frame by frame
    size_t frame_count = (count - padding_num) / _max_frame_size;
    for (size_t i = 0; i < frame_count; i++) {
        // directly decode value to the output, don't  buffer the value
        decode_current_frame(val);
        _current_index += _max_frame_size;
        val += _max_frame_size;
    }

    // 3. process remaining value
    size_t remaining_num = (count - padding_num) % _max_frame_size;
    if (remaining_num > 0) {
        decode_current_frame(_out_buffer.data());
        val = copy_value(val, remaining_num);
    }

    return true;
}

template <typename T>
bool ForDecoder<T>::skip(int32_t skip_num) {
    if (_current_index + skip_num >= _values_num || _current_index + skip_num < 0) {
        return false;
    }
    _current_index = _current_index + skip_num;
    return true;
}

template <typename T>
uint32_t ForDecoder<T>::seek_last_frame_before_value(T target) {
    // first of all, find the first frame >= target
    uint32_t left = 0;
    uint32_t right = _frame_count;
    while (left < right) {
        uint32_t mid = left + (right - left) / 2;
        T midValue = decode_frame_min_value(mid);
        if (midValue < target) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    // after loop, left is the first frame >= target
    if (left == 0) {
        // all frames are >= target, not found
        return _frame_count;
    }
    // otherwise previous frame is the last frame < target
    return left - 1;
}

template <typename T>
bool ForDecoder<T>::seek_lower_bound_inside_frame(uint32_t frame_index, T target,
                                                  bool* exact_match) {
    _current_index = frame_index * _max_frame_size;
    decode_current_frame(_out_buffer.data());
    auto end = _out_buffer.begin() + frame_size(frame_index);
    auto pos = std::lower_bound(_out_buffer.begin(), end, target);
    if (pos != end) { // found in this frame
        uint32_t pos_in_frame = std::distance(_out_buffer.begin(), pos);
        *exact_match = _out_buffer[pos_in_frame] == target;
        _current_index += pos_in_frame;
        return true;
    }
    return false;
}

template <typename T>
bool ForDecoder<T>::seek_at_or_after_value(const void* value, bool* exact_match) {
    T target = *reinterpret_cast<const T*>(value);
    uint32_t frame_to_search = seek_last_frame_before_value(target);
    if (frame_to_search == _frame_count) {
        // all frames are >= target, the searched value must the be first value
        _current_index = 0;
        decode_current_frame(_out_buffer.data());
        *exact_match = _out_buffer[0] == target;
        return true;
    }
    // binary search inside the last frame < target
    bool found = seek_lower_bound_inside_frame(frame_to_search, target, exact_match);
    // if not found, all values in the last frame are less than target.
    // then the searched value must be the first value of the next frame.
    if (!found && frame_to_search < _frame_count - 1) {
        _current_index = (frame_to_search + 1) * _max_frame_size;
        decode_current_frame(_out_buffer.data());
        *exact_match = _out_buffer[0] == target;
        return true;
    }
    return found;
}

template class ForEncoder<int8_t>;
template class ForEncoder<int16_t>;
template class ForEncoder<int32_t>;
template class ForEncoder<int64_t>;
template class ForEncoder<int128_t>;
template class ForEncoder<uint8_t>;
template class ForEncoder<uint16_t>;
template class ForEncoder<uint32_t>;
template class ForEncoder<uint64_t>;
template class ForEncoder<uint24_t>;
template class ForEncoder<uint128_t>;

template class ForDecoder<int8_t>;
template class ForDecoder<int16_t>;
template class ForDecoder<int32_t>;
template class ForDecoder<int64_t>;
template class ForDecoder<int128_t>;
template class ForDecoder<uint8_t>;
template class ForDecoder<uint16_t>;
template class ForDecoder<uint32_t>;
template class ForDecoder<uint64_t>;
template class ForDecoder<uint24_t>;
template class ForDecoder<uint128_t>;
} // namespace doris
