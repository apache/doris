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

#include "olap/byte_buffer.h"
#include "olap/olap_define.h"

namespace doris {

class OutStream;
class ReadOnlyFileStream;

namespace ser {

// ZigZag transformation: put the sign bit to the lowest bit, and flip the other bits when it is negative
inline int64_t zig_zag_encode(int64_t value) {
    return (value << 1) ^ (value >> 63);
}

// ZigZag decoding
inline int64_t zig_zag_decode(int64_t value) {
    return (((uint64_t)value) >> 1) ^ -(value & 1);
}

// Variable-length encoding writes unsigned data, and variable-length encoding uses the highest bit to indicate whether to terminate:
//-1 there will be data behind
//-0 this is the last byte of the data
// The so-called unsigned data means that the data is not easy to appear. The sign bit is 1, and the subsequent consecutive 0; or from the sign bit
// 1 Continuous occurrence. This situation is prone to occur when the signature data represents a negative number. under these circumstances,
// Variable length coding cannot effectively reduce the code length, for this, please use write_var_signed.
Status write_var_unsigned(OutStream* stream, int64_t value);

// Write signed data with variable length encoding, in order to avoid the problem of continuous 1s in the high bits of negative numbers, the data is ZigZag transformed
inline Status write_var_signed(OutStream* stream, int64_t value) {
    return write_var_unsigned(stream, zig_zag_encode(value));
}

// Read in write_var_unsigned encoded data
Status read_var_unsigned(ReadOnlyFileStream* stream, int64_t* value);

// Read in write_var_signed encoded data
inline Status read_var_signed(ReadOnlyFileStream* stream, int64_t* value) {
    Status res = read_var_unsigned(stream, value);

    if (res.ok()) {
        *value = zig_zag_decode(*value);
    }

    return res;
}

// The bit_width in RunLengthIntegerWriter is all 5bit encoding,
// so it supports up to 2^5=32 bit lengths. However, it needs to represent at most 1~64 bits,
// a total of 64 bit lengths, so in 64 bit lengths Take 32 types.
// The remaining 32 bit lengths that are not in these 32 types are aligned up to the nearest bit length.
// FixedBitSize gives 32 bit lengths
enum FixedBitSize {
    ONE = 0,
    TWO,
    THREE,
    FOUR,
    FIVE,
    SIX,
    SEVEN,
    EIGHT,
    NINE,
    TEN,
    ELEVEN,
    TWELVE,
    THIRTEEN,
    FOURTEEN,
    FIFTEEN,
    SIXTEEN,
    SEVENTEEN,
    EIGHTEEN,
    NINETEEN,
    TWENTY,
    TWENTYONE,
    TWENTYTWO,
    TWENTYTHREE,
    TWENTYFOUR,
    TWENTYSIX,
    TWENTYEIGHT,
    THIRTY,
    THIRTYTWO,
    FORTY,
    FORTYEIGHT,
    FIFTYSIX,
    SIXTYFOUR
};

inline uint32_t used_bits(uint64_t value) {
    // counting leading zero, builtin function, this will generate BSR(Bit Scan Reverse)
    // instruction for X86
    if (value == 0) {
        return 0;
    }
    return 64 - __builtin_clzll(value);
}

inline void compute_hists(int64_t* data, uint16_t count, uint16_t hists[65]) {
    memset(hists, 0, sizeof(uint16_t) * 65);
    // compute the histogram
    for (uint32_t i = 0; i < count; i++) {
        hists[used_bits(data[i])]++;
    }
}

// Returns the FixedBiteSize greater than or equal to n and closest to n
inline uint32_t get_closet_fixed_bits(uint32_t n) {
    static uint8_t bits_map[65] = {
            1,                              // 0
            1,  2,  3,  4,  5,  6,  7,  8,  // 1 - 8
            9,  10, 11, 12, 13, 14, 15, 16, // 9 - 16
            17, 18, 19, 20, 21, 22, 23, 24, // 17 - 24
            26, 26, 28, 28, 30, 30, 32, 32, // 25 - 32
            40, 40, 40, 40, 40, 40, 40, 40, // 33 - 40
            48, 48, 48, 48, 48, 48, 48, 48, // 41 - 48
            56, 56, 56, 56, 56, 56, 56, 56, // 49 - 56
            64, 64, 64, 64, 64, 64, 64, 64, // 57 - 64
    };
    return bits_map[n];
}

inline uint32_t percentile_bits_with_hist(uint16_t hists[65], uint16_t count, double p) {
    int32_t per_len = (int32_t)(count * (1.0 - p));
    // return the bits required by pth percentile length
    for (int32_t i = 64; i >= 0; i--) {
        per_len -= hists[i];
        if (per_len < 0) {
            return get_closet_fixed_bits(i);
        }
    }
    return 0;
}

// First calculate the bit length of value (the highest bit of 1), and then use get_closet_fixed_bits
// Return the closest FixedBiteSize
uint32_t find_closet_num_bits(int64_t value);

// Read n bytes in big endian order and convert to long
Status bytes_to_long_be(ReadOnlyFileStream* stream, int32_t n, int64_t* value);

// Encode the bit length as one of 32 fixed-length bits, and the return value is between 0 and 31
uint32_t encode_bit_width(uint32_t n);

// Decode the result of encode_bit_width encoding
uint32_t decode_bit_width(uint32_t n);

// Sort the data in data according to the bit length, and return the maximum bit length under a given ratio p.
// For example: p == 1.0, which means the maximum bit length of all data
// p == 0.9, which means the maximum bit length of 90% of the data with the shortest bit position
// p == 0.5, which means the maximum bit length of the 50% data with the shortest bit position
uint32_t percentile_bits(int64_t* data, uint16_t count, double p);

// Output a set of integers to output in a compact manner
Status write_ints(OutStream* output, int64_t* data, uint32_t count, uint32_t bit_width);

// Read the data output by write_ints
Status read_ints(ReadOnlyFileStream* input, int64_t* data, uint32_t count, uint32_t bit_width);

// Do not want to use Guava LongMath.checkedSubtract() here as it will throw
// ArithmeticException in case of overflow
inline bool is_safe_subtract(int64_t left, int64_t right) {
    return ((left ^ right) >= 0) | ((left ^ (left - right)) >= 0);
}

} // namespace ser
} // namespace doris
