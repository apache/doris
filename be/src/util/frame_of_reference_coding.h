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

#ifndef DORIS_FRAME_OF_REFERENCE_CODING_H
#define DORIS_FRAME_OF_REFERENCE_CODING_H


#include <cstdlib>
#include <iostream>

#include "util/bit_stream_utils.h"
#include "util/bit_stream_utils.inline.h"
#include "util/faststring.h"

namespace doris {
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
//      (2 bit OrderFlag + 6 bit BitWidth) * FrameCount
//       8 bit FrameValueNum
//      32 bit ValuesNum
//
// The not ascending order BitPackingFrame format:
//      MinValue, (Value - MinVale) * FrameValueNum
//
// The ascending order BitPackingFrame format:
//      MinValue, (Value[i] - Value[i - 1]) * FrameValueNum
//
// The OrderFlag is 1 represents ascending order, 0 represents  not ascending order
// The last frame value num maybe less than 128
template<typename T>
class ForEncoder {
public:
    explicit ForEncoder(faststring* buffer): _buffer(buffer) {}

    void put(const T value) {
        return put_batch(&value, 1);
    }

    void put_batch(const T* value, size_t count);

    // Flushes any pending values to the underlying buffer.
    // Returns the total number of bytes written
    uint32_t flush();

    // underlying buffer size + footer meta size.
    // Note: should call this method before flush.
    uint32_t len() {
        return _buffer->size() + _order_flag_and_bit_widths.size() + 5;
    }

    // Resets all the state in the encoder.
    void clear() {
        _values_num = 0;
        _buffered_values_num = 0;
        _buffer->clear();
    }

private:
    void bit_pack(T *input, uint8_t in_num, int bit_width, uint8_t *output);

    void bit_packing_one_frame_value(const T* input);

    const T* copy_value(const T* val, size_t count);

    uint32_t _values_num = 0;
    uint8_t _buffered_values_num = 0;
    static const uint8_t FRAME_VALUE_NUM = 128;
    T _buffered_values[FRAME_VALUE_NUM];

    faststring* _buffer;
    std::vector<uint8_t> _order_flag_and_bit_widths;
};

template<typename T>
class ForDecoder {
public:
    explicit ForDecoder(const uint8_t* in_buffer, size_t buffer_len)
        :_buffer (in_buffer),
         _buffer_len (buffer_len){}

    // read footer metadata
    bool init();

    bool get(T* val) {
        return get_batch(val, 1);
    }

    // Gets the next batch value.  Returns false if there are no more.
    bool get_batch(T* val, size_t count);

    // The skip_num is positive means move forwards
    // The skip_num is negative means move backwards
    bool skip(int32_t skip_num);

    uint32_t count() const {
        return _values_num;
    }

private:

    void bit_unpack(const uint8_t *input, uint8_t in_num, int bit_width, T *output);

    void decode_current_frame(T* output);

    T* copy_value(T* val, size_t count);

    bool need_decode_frame() {
        return !(_frame_value_num * _current_decoded_frame < _current_index
                 && _current_index < _frame_value_num * (_current_decoded_frame + 1));
    }

    uint8_t _frame_value_num = 0;
    uint32_t _values_num = 0;
    uint32_t _frame_count = 0;
    uint32_t _current_index = 0;
    uint32_t _current_decoded_frame = -1;
    std::vector<T> _out_buffer;
    std::vector<uint32_t> _frame_offsets;
    std::vector<uint8_t> _bit_widths;
    std::vector<uint8_t> _order_flags;
    const uint8_t* _buffer;
    size_t _buffer_len = 0;
};
}


#endif //DORIS_FRAME_OF_REFERENCE_CODING_H
