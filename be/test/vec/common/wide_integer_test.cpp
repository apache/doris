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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/tests/gtest_wide_integer.cpp
// and modified by Doris

#include <gtest/gtest.h>

#include "vec/common/uint128.h"
#include "vec/core/types.h"

namespace doris::vectorized {
TEST(WideInteger, Conversions) {
    ASSERT_EQ(UInt64(UInt128(12345678901234567890ULL)), 12345678901234567890ULL);
    ASSERT_EQ(UInt64(UInt256(12345678901234567890ULL)), 12345678901234567890ULL);

    ASSERT_EQ(__uint128_t(UInt128(12345678901234567890ULL)), 12345678901234567890ULL);
    ASSERT_EQ(__uint128_t(UInt256(12345678901234567890ULL)), 12345678901234567890ULL);

    ASSERT_EQ((UInt64(UInt128(123.456))), 123);
    ASSERT_EQ((UInt64(UInt256(123.456))), 123);

    ASSERT_EQ(UInt64(UInt128(123.456F)), 123);
    ASSERT_EQ(UInt64(UInt256(123.456F)), 123);

    ASSERT_EQ(Float64(UInt128(1) * 1000000000 * 1000000000 * 1000000000 * 1000000000), 1e36);

    ASSERT_EQ(Float64(UInt256(1) * 1000000000 * 1000000000 * 1000000000 * 1000000000 * 1000000000 *
                      1000000000 * 1000000000 * 1000000000),
              1e72);
}

TEST(WideInteger, Arithmetic) {
    Int128 minus_one = -1;
    Int128 zero = 0;

    zero += -1;
    ASSERT_EQ(zero, -1);
    ASSERT_EQ(zero, minus_one);

    zero += minus_one;
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    ASSERT_EQ(0, memcmp(&zero, "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFE",
                        sizeof(zero)));
#else
    ASSERT_EQ(0, memcmp(&zero, "\xFE\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
                        sizeof(zero)));
#endif
    zero += 2;
    ASSERT_EQ(zero, 0);

    ASSERT_EQ(UInt256(12345678901234567890ULL) * 12345678901234567890ULL / 12345678901234567890ULL,
              12345678901234567890ULL);
    ASSERT_EQ(UInt256(12345678901234567890ULL) * UInt256(12345678901234567890ULL) /
                      12345678901234567890ULL,
              12345678901234567890ULL);
    ASSERT_EQ(UInt256(12345678901234567890ULL) * 12345678901234567890ULL /
                      UInt256(12345678901234567890ULL),
              12345678901234567890ULL);
    ASSERT_EQ(UInt256(12345678901234567890ULL) * 12345678901234567890ULL / 12345678901234567890ULL,
              UInt256(12345678901234567890ULL));
    ASSERT_EQ(UInt128(12345678901234567890ULL) * 12345678901234567890ULL /
                      UInt128(12345678901234567890ULL),
              12345678901234567890ULL);
    ASSERT_EQ(UInt256(12345678901234567890ULL) * UInt128(12345678901234567890ULL) /
                      12345678901234567890ULL,
              12345678901234567890ULL);

    ASSERT_EQ(Int128(0) + Int32(-1), Int128(-1));
}

TEST(WideInteger, DecimalArithmetic) {
    Decimal128V3 zero {};
    Decimal32 addend = -1000;

    zero += Decimal128V3(addend);
    ASSERT_EQ(zero.value, -1000);

    zero += addend;
    ASSERT_EQ(zero.value, -2000);
}

TEST(WideInteger, FromDouble) {
    /// Check that we are being able to convert double to big integer without the help of floating point instructions.
    /// (a prototype of a function that we may need)

    double f = -123.456;
    UInt64 u;
    memcpy(&u, &f, sizeof(f));

    bool is_negative = u >> 63;
    uint16_t exponent = (u >> 52) & (((1ULL << 12) - 1) >> 1);
    int16_t normalized_exponent = exponent - 1023;
    UInt64 mantissa = u & ((1ULL << 52) - 1);

    // std::cerr << is_negative << ", " << normalized_exponent << ", " << mantissa << "\n";

    /// x = sign * (2 ^ normalized_exponent + mantissa * 2 ^ (normalized_exponent - mantissa_bits))

    Int128 res = 0;

    if (normalized_exponent >= 128) {
    } else {
        res = mantissa;
        if (normalized_exponent > 52) {
            res <<= (normalized_exponent - 52);
        } else {
            res >>= (52 - normalized_exponent);
        }

        if (normalized_exponent > 0) {
            res += Int128(1) << normalized_exponent;
        }
    }

    if (is_negative) {
        res = -res;
    }

    ASSERT_EQ(res, -123);
}

TEST(WideInteger, Shift) {
    Int128 x = 1;

    auto y = x << 64;

#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    ASSERT_EQ(0, memcmp(&y, "\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
                        sizeof(Int128)));
#else
    ASSERT_EQ(0, memcmp(&y, "\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00",
                        sizeof(Int128)));
#endif
    auto z = y << 11;
    auto a = x << 11;
    ASSERT_EQ(a, 2048);

    z >>= 64;
    ASSERT_EQ(z, a);

    x = -1;
    y = x << 16;

#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    ASSERT_EQ(0, memcmp(&y, "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x00\x00",
                        sizeof(Int128)));
#else
    ASSERT_EQ(0, memcmp(&y, "\x00\x00\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
                        sizeof(Int128)));
#endif
    y >>= 16;
    ASSERT_EQ(0, memcmp(&y, "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
                        sizeof(Int128)));

    y <<= 64;
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    ASSERT_EQ(0, memcmp(&y, "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x00\x00\x00\x00\x00\x00\x00\x00",
                        sizeof(Int128)));
#else
    ASSERT_EQ(0, memcmp(&y, "\x00\x00\x00\x00\x00\x00\x00\x00\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
                        sizeof(Int128)));
#endif
    y >>= 32;
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    ASSERT_EQ(0, memcmp(&y, "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x00\x00\x00\x00",
                        sizeof(Int128)));
#else
    ASSERT_EQ(0, memcmp(&y, "\x00\x00\x00\x00\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
                        sizeof(Int128)));
#endif

    y <<= 64;
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    ASSERT_EQ(0, memcmp(&y, "\xFF\xFF\xFF\xFF\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
                        sizeof(Int128)));
#else
    ASSERT_EQ(0, memcmp(&y, "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xFF\xFF\xFF\xFF",
                        sizeof(Int128)));
#endif
}

} // namespace doris::vectorized
