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

#pragma once

#include <stdint.h>

#include <cstdlib>
#include <vector>

#include "olap/olap_common.h"
#include "olap/uint24.h"
#include "util/faststring.h"

namespace doris {

inline uint8_t leading_zeroes(const uint64_t v) {
    if (v == 0) {
        return 64;
    }
    return __builtin_clzll(v);
}

inline uint8_t bits_less_than_64(const uint64_t v) {
    return 64 - leading_zeroes(v);
}

// See https://stackoverflow.com/questions/28423405/counting-the-number-of-leading-zeros-in-a-128-bit-integer
inline uint8_t bits_may_more_than_64(const uint128_t v) {
    // See https://stackoverflow.com/questions/49580083/builtin-clz-returns-incorrect-value-for-input-zero
    if (v == 0) {
        return 0;
    }
    uint64_t hi = v >> 64;
    uint64_t lo = v;
    int z[3] = {leading_zeroes(hi), leading_zeroes(lo) + 64, 128};
    int idx = !hi + ((!lo) & (!hi));
    return 128 - z[idx];
}

template <typename T>
uint8_t bits(const T v) {
    if (sizeof(T) <= 8) {
        return bits_less_than_64(v);
    } else {
        return bits_may_more_than_64(v);
    }
}

// The implementation for frame-of-reference coding
// The detail of frame-of-reference coding, please refer to
// https://lemire.me/blog/2012/02/08/effective-compression-using-frame-of-reference-and-delta-coding/
// and https://www.elastic.co/cn/blog/frame-of-reference-and-roaring-bitmaps
//
// The encoded data format is as follows:
//
// 1. Body:
//      BitPackingFrame * FrameCount
// 2. Footer:
//
//      (8 bit StorageFormat + 8 bit BitWidth) * FrameCount
//       8 bit FrameValueNum
//      32 bit ValuesNum
//
// There are currently three storage formats
// (1) if the StorageFormat == 0: When input data order is not ascending and the BitPackingFrame format is:
//          MinValue, (Value[i] - MinVale) * FrameValueNum
//
// (2) if the StorageFormat == 1: When input data order is ascending and the BitPackingFrame format is:
//      MinValue, (Value[i] - Value[i - 1]) * FrameValueNum
//
// (3) if the StorageFormat == 2:  When overflow occurs when using (1) or (2) and save original values:
//      MinValue, (Value[i]) * FrameValueNum
//
// len(MinValue) can be 32(uint32_t), 64(uint64_t), 128(uint128_t)
//
// The OrderFlag is 1 represents ascending order, 0 represents  not ascending order
// The last frame value num maybe less than 128
template <typename T>
class ForEncoder {
public:
    explicit ForEncoder(faststring* buffer) : _buffer(buffer) {}

    void put(const T value) { return put_batch(&value, 1); }

    void put_batch(const T* value, size_t count);

    // Flushes any pending values to the underlying buffer.
    // Returns the total number of bytes written
    uint32_t flush();

    // underlying buffer size + footer meta size.
    // Note: should call this method before flush.
    uint32_t len() { return _buffer->size() + _storage_formats.size() + _bit_widths.size() + 5; }

    // Resets all the state in the encoder.
    void clear() {
        _values_num = 0;
        _buffered_values_num = 0;
        _buffer->clear();
    }

private:
    void bit_pack(const T* input, uint8_t in_num, int bit_width, uint8_t* output);

    void bit_packing_one_frame_value(const T* input);

    const T* copy_value(const T* val, size_t count);

    const T numeric_limits_max();

    uint32_t _values_num = 0;
    uint8_t _buffered_values_num = 0;
    static const uint8_t FRAME_VALUE_NUM = 128;
    T _buffered_values[FRAME_VALUE_NUM];

    faststring* _buffer = nullptr;
    std::vector<uint8_t> _storage_formats;
    std::vector<uint8_t> _bit_widths;
};

template <typename T>
class ForDecoder {
public:
    explicit ForDecoder(const uint8_t* in_buffer, size_t buffer_len)
            : _buffer(in_buffer), _buffer_len(buffer_len), _parsed(false) {}

    // read footer metadata
    bool init();

    bool get(T* val) { return get_batch(val, 1); }

    // Gets the next batch value.  Returns false if there are no more.
    bool get_batch(T* val, size_t count);

    // The skip_num is positive means move forwards
    // The skip_num is negative means move backwards
    bool skip(int32_t skip_num);

    bool seek_at_or_after_value(const void* value, bool* exact_match);

    uint32_t current_index() const { return _current_index; }

    uint32_t count() const { return _values_num; }

private:
    void bit_unpack(const uint8_t* input, uint8_t in_num, int bit_width, T* output);

    uint32_t frame_size(uint32_t frame_index) {
        return (frame_index == _frame_count - 1) ? _last_frame_size : _max_frame_size;
    }

    void decode_current_frame(T* output);

    T decode_frame_min_value(uint32_t frame_index);

    // Return index of the last frame which contains value < target.
    // Return `_frame_count - 1` when all frames are < target.
    // Return `_frame_count` when not found (all frames are >= target).
    uint32_t seek_last_frame_before_value(T target);

    // Seek to the first value in frame that >= target.
    // Return true when found and update exact_match.
    // Return false otherwise.
    bool seek_lower_bound_inside_frame(uint32_t frame_index, T target, bool* exact_match);

    T* copy_value(T* val, size_t count);

    const uint8_t* _buffer = nullptr;
    size_t _buffer_len = 0;
    bool _parsed = false;

    uint8_t _max_frame_size = 0;
    uint8_t _last_frame_size = 0;
    uint32_t _values_num = 0;
    uint32_t _frame_count = 0;
    std::vector<uint32_t> _frame_offsets;
    std::vector<uint8_t> _bit_widths;
    std::vector<uint8_t> _storage_formats;

    uint32_t _current_index = 0;
    uint32_t _current_decoded_frame = -1;
    std::vector<T> _out_buffer; // store values of decoded frame
};
} // namespace doris
