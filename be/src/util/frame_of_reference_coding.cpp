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

#include <glog/logging.h>
#include <sys/types.h>

#include <algorithm>
#include <cstring>
#include <iostream>
#include <iterator>
#include <limits>

#include "common/cast_set.h"
#include "util/bit_util.h"
#include "util/coding.h"
#include "vec/common/endian.h"

namespace doris {
#include "common/compile_check_begin.h"

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

template <typename T>
void ForEncoder<T>::bit_pack_8(const T* input, uint8_t in_num, int bit_width, uint8_t* output) {
    int64_t s = 0;
    uint8_t output_mask = 255;
    int tail_count = in_num & 7;              // the remainder of in_num modulo 8
    int full_batch_size = (in_num >> 3) << 3; // Adjust in_num to a multiple of 8

    for (int i = 0; i < full_batch_size; i += 8) {
        // Put the 8 numbers in the input into s in order, each number occupies bit_width bit
        s |= static_cast<int64_t>(input[i + 7]);
        s |= (static_cast<int64_t>(input[i + 6])) << bit_width;
        s |= (static_cast<int64_t>(input[i + 5])) << (2 * bit_width);
        s |= (static_cast<int64_t>(input[i + 4])) << (3 * bit_width);
        s |= (static_cast<int64_t>(input[i + 3])) << (4 * bit_width);
        s |= (static_cast<int64_t>(input[i + 2])) << (5 * bit_width);
        s |= (static_cast<int64_t>(input[i + 1])) << (6 * bit_width);
        s |= (static_cast<int64_t>(input[i])) << (7 * bit_width);

        // Starting with the highest valid bit, take out 8 bits in sequence
        // perform an AND operation with output_mask to ensure that only 8 bits are valid
        // (bit_width - j - 1) << 3 used to calculate how many bits need to be removed at the end
        for (int j = 0; j < bit_width; j++) {
            output[j] = (s >> ((bit_width - j - 1) << 3)) & output_mask;
        }
        output += bit_width;
        s = 0;
    }

    // remainder
    int byte = tail_count * bit_width; // How many bits are left to store
    int bytes = (byte + 7) >> 3;       // How many more bytes are needed to store the rest of input

    // Put the tail_count numbers in the input into s in order, each number occupies bit_width bit
    for (int i = 0; i < tail_count; i++) {
        s |= (static_cast<int64_t>(input[i + full_batch_size]))
             << ((tail_count - i - 1) * bit_width);
    }

    // If byte is not a multiple of 8 and therefore needs to be padded with 0 at the end
    s <<= (bytes << 3) - byte;

    // Starting with the highest valid bit, take out 8 bits in sequence
    // perform an AND operation with output_mask to ensure that only 8 bits are valid.
    // (bytes - i - 1) << 3 used to calculate how many bits need to be removed at the end
    for (int i = 0; i < bytes; i++) {
        output[i] = (s >> ((bytes - i - 1) << 3)) & output_mask;
    }
}

template <typename T>
template <typename U>
void ForEncoder<T>::bit_pack_4(const T* input, uint8_t in_num, int bit_width, uint8_t* output) {
    U s = 0;
    uint8_t output_mask = 255;
    int tail_count = in_num & 3;              // the remainder of in_num modulo 4
    int full_batch_size = (in_num >> 2) << 2; // Adjust in_num to a multiple of 4
    int output_size = 0;                      // How many outputs can be processed at a time
    int bit_width_remainder =
            (bit_width << 2) & 7; // How many bits will be left after processing 4 numbers at a time
    int extra_bit = 0;            // Extra bits after each process

    for (int i = 0; i < full_batch_size; i += 4) {
        // Put the 4 numbers in the input into s in order, each number occupies bit_width bit
        // The reason for using s<<=bit_width first is that there are unprocessed bits in the previous loop
        s <<= bit_width;
        s |= (static_cast<U>(input[i]));
        s <<= bit_width;
        s |= (static_cast<U>(input[i + 1]));
        s <<= bit_width;
        s |= (static_cast<U>(input[i + 2]));
        s <<= bit_width;
        s |= (static_cast<U>(input[i + 3]));

        // ((bit_width * 4) + extra_bit) / 8: There are bit_width*4 bits to be processed in s,
        // and there are extra_bit bits left over from the last loop,
        // divide by 8 to calculate how much output can be processed in this loop.
        output_size = ((bit_width << 2) + extra_bit) >> 3;

        // Each loop will leave bit_width_remainder bit unprocessed,
        // last loop will leave extra_bit bit, eventually will leave
        // (extra_bit + bit_width_remainder) & 7 bit unprocessed
        extra_bit = (extra_bit + bit_width_remainder) & 7;

        // Starting with the highest valid bit, take out 8 bits in sequence
        // perform an AND operation with output_mask to ensure that only 8 bits are valid
        // (output_size-j-1)<<3 used to calculate how many bits need to be removed at the end
        // But since there are still extra_bit bits that can't be processed, need to add the extra_bit
        for (int j = 0; j < output_size; j++) {
            output[j] = (s >> (((output_size - j - 1) << 3) + extra_bit)) & output_mask;
        }
        output += output_size;

        // s retains the post extra_bit bit as it is not processed
        s &= (1 << extra_bit) - 1;
    }

    // remainder
    int byte = tail_count * bit_width;     // How many bits are left to store
    if (extra_bit != 0) byte += extra_bit; // add extra_bit bit as it is not processed
    int bytes = (byte + 7) >> 3; // How many more bytes are needed to store the rest of input

    // Put the tail_count numbers in the input into s in order, each number occupies bit_width bit
    for (int i = 0; i < tail_count; i++) {
        s <<= bit_width;
        s |= (input[i + full_batch_size]);
    }

    // If byte is not a multiple of 8 and therefore needs to be padded with 0 at the end
    s <<= (bytes << 3) - byte;

    // Starting with the highest valid bit, take out 8 bits in sequence
    // perform an AND operation with output_mask to ensure that only 8 bits are valid.
    // (bytes - i - 1) << 3 used to calculate how many bits need to be removed at the end
    for (int i = 0; i < bytes; i++) {
        output[i] = (s >> (((bytes - i - 1) << 3))) & output_mask;
    }
}

template <typename T>
void ForEncoder<T>::bit_pack_1(const T* input, uint8_t in_num, int bit_width, uint8_t* output) {
    int output_mask = 255;
    int need_bit = 0; // still need

    for (int i = 0; i < in_num; i++) {
        T x = input[i];
        int width = bit_width;
        if (need_bit) {
            // The last time we take away the high 8 - need_bit,
            // we need to make up the rest of the need_bit from the width.
            // Use width - need_bit to compute high need_bit bits
            *output |= x >> (width - need_bit);
            output++;
            // There are need_bit bits being used, so subtract
            width -= need_bit;
        }
        int num = width >> 3;      // How many outputs can be processed at a time
        int remainder = width & 7; // How many bits are left to store

        // Starting with the highest valid bit, take out 8 bits in sequence
        // perform an AND operation with output_mask to ensure that only 8 bits are valid
        // (num-j-1)<<3 used to calculate how many bits need to be removed at the end
        // But since there are still remainder bits that can't be processed, need to add the remainder
        for (int j = 0; j < num; j++) {
            *output = cast_set<uint8_t>((x >> (((num - j - 1) << 3) + remainder)) & output_mask);
            output++;
        }
        if (remainder) {
            // Process the last remaining remainder bit.
            // y = (x & ((1 << remainder) - 1)) extract the last remainder bits.
            // ouput = y << (8 - reaminder)  Use the high 8 - remainder bit
            *output = cast_set<uint8_t>((x & ((1 << remainder) - 1)) << (8 - remainder));
            // Already have remainder bits, next time need 8-remainder bits
            need_bit = 8 - remainder;
        } else {
            need_bit = 0;
        }
    }
}

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
    /*
        bit_width <= 8 : pack_8 > pack_16 > pack_32
        bit_width <= 16 : pack_4 > pack_8 > pack_16
        bit_width <= 32 : pack_4 >= pack_2 > pack_8 
        (pack_4 and pack_2 have nearly similar execution times, but pack_4 utilizes space more efficiently)
        bit_width <= 64 : pack_1 > pack_4
    */
    if (bit_width <= 8) {
        bit_pack_8(input, in_num, bit_width, output);
    } else if (bit_width <= 16) {
        bit_pack_4<int64_t>(input, in_num, bit_width, output);
    } else if (bit_width <= 32) {
        bit_pack_4<__int128_t>(input, in_num, bit_width, output);
    } else {
        bit_pack_1(input, in_num, bit_width, output);
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
        put_fixed128_le(_buffer, static_cast<uint128_t>(min));
    } else if (sizeof(T) == 8) {
        put_fixed64_le(_buffer, static_cast<uint64_t>(min));
    } else {
        put_fixed32_le(_buffer, static_cast<uint32_t>(min));
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

    return cast_set<uint32_t>(_buffer->size());
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
    _last_frame_size =
            cast_set<uint8_t>(_max_frame_size - (_max_frame_size * _frame_count - _values_num));

    size_t bit_width_offset = _buffer_len - 5 - _frame_count * 2;

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

    _out_buffer.resize(_max_frame_size);
    _parsed = true;

    return true;
}

// todo(kks): improve this method by SIMD instructions

template <typename T>
template <typename U>
void ForDecoder<T>::bit_unpack_optimize(const uint8_t* input, uint8_t in_num, int bit_width,
                                        T* output) {
    static_assert(std::is_same<U, int64_t>::value || std::is_same<U, __int128_t>::value,
                  "bit_unpack_optimize only supports U = int64_t or __int128_t");
    constexpr int u_size = sizeof(U);                   // Size of U
    constexpr int u_size_shift = (u_size == 8) ? 3 : 4; // log2(u_size)
    int valid_bit = 0;                                  // How many valid bits
    int need_bit = 0;                                   // still need
    size_t input_size = (in_num * bit_width + 7) >> 3;  // input's size
    int full_batch_size =
            cast_set<int>((input_size >> u_size_shift)
                          << u_size_shift);     // Adjust input_size to a multiple of u_size
    int tail_count = input_size & (u_size - 1); // The remainder of input_size modulo u_size.
    // The number of bits in input to adjust to multiples of 8 and thus more
    int more_bit = cast_set<int>((input_size << 3) - (in_num * bit_width));

    // to ensure that only bit_width bits are valid
    T output_mask;
    if (bit_width >= static_cast<int>(sizeof(T) * 8)) {
        output_mask = static_cast<T>(~T(0));
    } else {
        output_mask = static_cast<T>((static_cast<T>(1) << bit_width) - 1);
    }

    U s = 0; // Temporary buffer for bitstream: aggregates input bytes into a large integer for unpacking

    for (int i = 0; i < full_batch_size; i += u_size) {
        s = 0;

        s = to_endian<std::endian::big>(*((U*)(input + i)));

        // Determine what the valid bits are based on u_size
        valid_bit = u_size << 3;

        // If input_size is exactly a multiple of 8, then need to remove the last more_bit in the last loop.
        if (tail_count == 0 && i == full_batch_size - u_size) {
            valid_bit -= more_bit;
            s >>= more_bit;
        }

        if (need_bit) {
            // The last time we take away the high bit_width - need_bit,
            // we need to make up the rest of the need_bit from the width.
            // Use valid_bit - need_bit to compute high need_bit bits of s
            // perform an AND operation to ensure that only need_bit bits are valid
            auto mask = (static_cast<U>(1) << need_bit) - 1;
            auto shifted = s >> (valid_bit - need_bit);
            auto masked_result = shifted & mask;
            if constexpr (sizeof(T) <= 4) {
                *output |= static_cast<T>(static_cast<uint32_t>(masked_result));
            } else {
                *output |= static_cast<T>(masked_result);
            }
            output++;
            valid_bit -= need_bit;
        }

        int num = valid_bit / bit_width;             // How many outputs can be processed at a time
        int remainder = valid_bit - num * bit_width; // How many bits are left to store

        // Starting with the highest valid bit, take out bit_width bits in sequence
        // perform an AND operation with output_mask to ensure that only bit_width bits are valid
        // (num-j-1) * bit_width used to calculate how many bits need to be removed at the end
        // But since there are still remainder bits that can't be processed, need to add the remainder
        for (int j = 0; j < num; j++) {
            *output =
                    static_cast<T>((s >> (((num - j - 1) * bit_width) + remainder)) & output_mask);
            output++;
        }

        if (remainder) {
            // Process the last remaining remainder bit.
            // y = (s & ((static_cast<U>(1) << remainder) - 1)) extract the last remainder bits.
            // output = y << (bit_width - remainder) Use the high bit_width - remainder bit
            if constexpr (sizeof(T) <= 4) {
                auto masked_value = static_cast<T>(
                        static_cast<uint32_t>(s & ((static_cast<U>(1) << remainder) - 1)));
                *output = static_cast<T>(masked_value << (bit_width - remainder));
            } else {
                auto masked_value = static_cast<T>((s & ((static_cast<U>(1) << remainder) - 1)));
                *output = static_cast<T>(masked_value << (bit_width - remainder));
            }
            // Already have remainder bits, next time need bit_width - remainder bits
            need_bit = bit_width - remainder;
        } else {
            need_bit = 0;
        }
    }

    // remainder
    if (tail_count) {
        // Put the tail_count numbers in the input into s in order, each number occupies 8 bit
        for (int i = 0; i < tail_count; i++) {
            s <<= 8;
            s |= input[full_batch_size + i];
        }

        // tail * 8 is the number of bits that are left to process
        // tail * 8 - more_bit is to remove the last more_bit
        valid_bit = (tail_count << 3) - more_bit;
        s >>= more_bit;

        // same as before
        if (need_bit) {
            if constexpr (sizeof(T) <= 4) {
                *output |= static_cast<T>(static_cast<uint32_t>(
                        (s >> (valid_bit - need_bit)) & ((static_cast<U>(1) << need_bit) - 1)));
            } else {
                *output |= static_cast<T>((s >> (valid_bit - need_bit)) &
                                          ((static_cast<U>(1) << need_bit) - 1));
            }
            output++;
            valid_bit -= need_bit;
        }

        int num = valid_bit / bit_width; // How many outputs can be processed at a time

        // same as before
        for (int j = 0; j < num; j++) {
            *output = static_cast<T>((s >> (((num - j - 1) * bit_width))) & output_mask);
            output++;
        }
    }
}

// The reverse of bit_pack method, get original integer data list from packed bits
// param[in] input: the packed bits need to unpack
// param[in] in_num: the integer number in packed bits
// param[in] bit_width: how many bit we used to store each integer data
// param[out] output: the original integer data list
template <typename T>
void ForDecoder<T>::bit_unpack(const uint8_t* input, uint8_t in_num, int bit_width, T* output) {
    /*
        When 32 < bit_width <= 64 unrolling the loop 16 times is more efficient than unrolling it 8 times.
        When bit_width > 64, we must use __int128_t and unroll the loop 16 times.
    */
    if (bit_width <= 32) {
        bit_unpack_optimize<int64_t>(input, in_num, bit_width, output);
    } else {
        bit_unpack_optimize<__int128_t>(input, in_num, bit_width, output);
    }
}

template <typename T>
void ForDecoder<T>::decode_current_frame(T* output) {
    uint32_t frame_index = _current_index / _max_frame_size;
    if (frame_index == _current_decoded_frame) {
        return; // current frame already decoded
    }
    _current_decoded_frame = frame_index;
    uint8_t current_frame_size = cast_set<uint8_t>(frame_size(frame_index));

    uint32_t base_offset = _frame_offsets[_current_decoded_frame];
    T min = 0;
    uint32_t delta_offset = 0;
    if constexpr (sizeof(T) == 16) {
        min = static_cast<T>(decode_fixed128_le(_buffer + base_offset));
        delta_offset = base_offset + 16;
    } else if constexpr (sizeof(T) == 8) {
        min = static_cast<T>(decode_fixed64_le(_buffer + base_offset));
        delta_offset = base_offset + 8;
    } else {
        min = static_cast<T>(decode_fixed32_le(_buffer + base_offset));
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
    if constexpr (sizeof(T) == 16) {
        min = static_cast<T>(decode_fixed128_le(_buffer + min_offset));
    } else if constexpr (sizeof(T) == 8) {
        min = static_cast<T>(decode_fixed64_le(_buffer + min_offset));
    } else {
        min = static_cast<T>(decode_fixed32_le(_buffer + min_offset));
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
    if (_current_index + skip_num >= _values_num) {
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
        auto pos_in_frame = cast_set<uint32_t>(std::distance(_out_buffer.begin(), pos));
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
#include "common/compile_check_end.h"
} // namespace doris
