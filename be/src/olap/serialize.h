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

#ifndef DORIS_BE_SRC_OLAP_COLUMN_FILE_SERIALIZE_H
#define DORIS_BE_SRC_OLAP_COLUMN_FILE_SERIALIZE_H

#include "olap/byte_buffer.h"
#include "olap/olap_define.h"

namespace doris {

class OutStream;
class ReadOnlyFileStream;

namespace ser {

// ZigZag变换: 将符号位放到最低位, 且在负数时翻转其他各位
inline int64_t zig_zag_encode(int64_t value) {
    return (value << 1) ^ (value >> 63);
}

// ZigZag解码
inline int64_t zig_zag_decode(int64_t value) {
    return (((uint64_t)value) >> 1) ^ -(value & 1);
}

// 以变长编码写入unsigned数据, 变长编码使用最高位表示是否终止:
//     - 1 后续还有数据
//     - 0 这是最后一个字节的数据
// 所谓unsigned数据, 指数据不容易出现符号位为1, 后续连续为0的情况; 或者从符号位
// 起连续出现1的情况. 而signed数据表示负数时, 容易出现这种情况, 在这种情况下,
// 无法有效利用变长编码减少码长, 为此请使用write_var_signed.
OLAPStatus write_var_unsigned(OutStream* stream, int64_t value);

// 以变长编码写入signed数据, 为了避免负数高位连续的1的问题, 将数据进行ZigZag变换
inline OLAPStatus write_var_signed(OutStream* stream, int64_t value) {
    return write_var_unsigned(stream, zig_zag_encode(value));
}

// 读入write_var_unsigned编码的数据
OLAPStatus read_var_unsigned(ReadOnlyFileStream* stream, int64_t* value);

// 读入write_var_signed编码的数据
inline OLAPStatus read_var_signed(ReadOnlyFileStream* stream, int64_t* value) {
    OLAPStatus res = read_var_unsigned(stream, value);

    if (OLAP_SUCCESS == res) {
        *value = zig_zag_decode(*value);
    }

    return res;
}

// 在RunLengthIntegerWriter中的bit_width都是5bit编码, 这样最多支持2^5=32种比特位
// 长. 然而, 需要表示最多1~64位, 共64种比特位长, 于是在64种比特位长中取32种. 对
// 其他剩余32个不在这32种的比特长度向上对齐到最接近的一个比特位长.
// FixedBitSize给出了32种比特位长
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

// 返回大于等于n且最接近n的FixedBiteSize
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

// 首先计算value的比特位长(1所在的最高位), 再使用get_closet_fixed_bits
// 返回最接近的FixedBiteSize
uint32_t find_closet_num_bits(int64_t value);

// Read n bytes in big endian order and convert to long
OLAPStatus bytes_to_long_be(ReadOnlyFileStream* stream, int32_t n, int64_t* value);

// 将位长编码为32个定长比特位之一, 返回值为0~31之间
uint32_t encode_bit_width(uint32_t n);

// 解码encode_bit_width编码的结果
uint32_t decode_bit_width(uint32_t n);

// 将data中的数据按比特位长排序, 返回给定比例p下, 最大位长.
// 例如: p == 1.0, 表示所有的数据的最大位长
//       p == 0.9, 表示比特位最短的90%的数据的最大位长
//       p == 0.5, 表示比特位最短的50%的数据的最大位长
uint32_t percentile_bits(int64_t* data, uint16_t count, double p);

// 以紧致方式向output输出一组整数
OLAPStatus write_ints(OutStream* output, int64_t* data, uint32_t count, uint32_t bit_width);

// 读取write_ints输出的数据
OLAPStatus read_ints(ReadOnlyFileStream* input, int64_t* data, uint32_t count, uint32_t bit_width);

// Do not want to use Guava LongMath.checkedSubtract() here as it will throw
// ArithmeticException in case of overflow
inline bool is_safe_subtract(int64_t left, int64_t right) {
    return ((left ^ right) >= 0) | ((left ^ (left - right)) >= 0);
}

} // namespace ser
} // namespace doris

#endif
