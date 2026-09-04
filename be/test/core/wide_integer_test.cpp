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

#include <cstdint>
#include <random>

#include "core/types.h"
#include "core/uint128.h"

namespace doris {
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

TEST(WideInteger, SingleLimbDivisorFastPath) {
    // A 256-bit dividend with all four 64-bit limbs populated.
    const UInt256 n = (UInt256(0xFEDCBA9876543210ULL) << 192) |
                      (UInt256(0x1122334455667788ULL) << 128) |
                      (UInt256(0x99AABBCCDDEEFF00ULL) << 64) | UInt256(0x0123456789ABCDEFULL);

    // Divisors that all fit in a single 64-bit limb (exercise the fast path).
    const UInt256 single_limb_divisors[] = {
            UInt256(1ULL),
            UInt256(2ULL),
            UInt256(3ULL),
            UInt256(7ULL),
            UInt256(10ULL),
            UInt256(1000000000ULL),
            UInt256(1000000000000000000ULL), // 10^18
            UInt256(0x8000000000000000ULL),  // 2^63
            UInt256(0xFFFFFFFFFFFFFFFFULL),  // 2^64 - 1, the largest single limb
    };
    for (const UInt256& d : single_limb_divisors) {
        const UInt256 q = n / d;
        const UInt256 r = n % d;
        // q * d + r must reconstruct n, and the remainder must be strictly less than d.
        ASSERT_EQ(q * d + r, n);
        ASSERT_TRUE(r < d);
    }
}

TEST(WideInteger, MultiLimbDivisorBoundary) {
    const UInt256 n = (UInt256(0xFEDCBA9876543210ULL) << 192) |
                      (UInt256(0x1122334455667788ULL) << 128) |
                      (UInt256(0x99AABBCCDDEEFF00ULL) << 64) | UInt256(0x0123456789ABCDEFULL);

    // Divisors that need two or more limbs must take the general path, not the fast path.
    const UInt256 multi_limb_divisors[] = {
            UInt256(1ULL) << 64,                   // 2^64: first value past single limb
            (UInt256(1ULL) << 64) + UInt256(1ULL), // 2^64 + 1
            (UInt256(1ULL) << 100) + UInt256(12345ULL),
            (UInt256(1ULL) << 192) + UInt256(0xDEADBEEFULL),
    };
    for (const UInt256& d : multi_limb_divisors) {
        const UInt256 q = n / d;
        const UInt256 r = n % d;
        ASSERT_EQ(q * d + r, n);
        ASSERT_TRUE(r < d);
    }
}

TEST(WideInteger, SingleLimbKnownAnswers) {
    // 10^38 / 10^19 == 10^19 exactly (10^19 fits in a single 64-bit limb).
    const UInt256 p19 = UInt256(10000000000000000000ULL); // 10^19
    const UInt256 p38 = p19 * p19;                        // 10^38
    ASSERT_EQ(p38 / p19, p19);
    ASSERT_EQ(p38 % p19, UInt256(0ULL));

    // Exact division and non-zero remainder with a small divisor.
    ASSERT_EQ(UInt256(1000ULL) / UInt256(7ULL), UInt256(142ULL));
    ASSERT_EQ(UInt256(1000ULL) % UInt256(7ULL), UInt256(6ULL));

    // Dividend smaller than divisor -> quotient 0, remainder is the dividend.
    ASSERT_EQ(UInt256(5ULL) / UInt256(9999999967ULL), UInt256(0ULL));
    ASSERT_EQ(UInt256(5ULL) % UInt256(9999999967ULL), UInt256(5ULL));
}

TEST(WideInteger, SingleLimbSignedDivision) {
    // The fast path runs on the unsigned magnitudes; sign handling stays in the wrappers.
    const Int256 n = (Int256(0x0011223344556677LL) << 128) | Int256(0x8899AABBCCDDEEFFLL);
    const Int256 d = 1000000007; // prime, single limb

    ASSERT_EQ((-n) / d, -(n / d));
    ASSERT_EQ(n / (-d), -(n / d));
    ASSERT_EQ((-n) / (-d), n / d);

    // Truncation-toward-zero semantics for the remainder sign.
    ASSERT_EQ((-n) % d, -(n % d));
    ASSERT_EQ(n % (-d), n % d);
}

TEST(WideInteger, TwoLimbDivisorDifferential) {
    // Force the two-limb (65..128-bit divisor) Knuth Algorithm D path and check it
    // against independently constructed ground truth. We pick bounded q, d, r FIRST,
    // then build n = q*d + r, so the expected quotient and remainder are known
    // without ever invoking the operators under test (no fixed-width circular
    // reconstruction that a wrapped-around wrong q could still satisfy).
    //
    // Path selection: d has its high 64-bit limb set (>= 2^64) so it misses the
    // single-limb fast path, and q >= 2^64 together with d >= 2^64 gives
    // n = q*d + r >= 2^128, so the numerator carries bits above 127 and the
    // operands-fit-128 fast path is skipped -> divide() lands in divide_knuth.
    //
    // Overflow safety: q < 2^127 and d < 2^128 give q*d < 2^255; r < d < 2^128, so
    // n = q*d + r < 2^256 and never wraps the 256-bit accumulator.
    std::mt19937_64 rng(0xC0FFEE1234ULL);
    auto rnd = [&rng]() { return rng(); };
    auto make128 = [](uint64_t hi, uint64_t lo) { return (UInt256(hi) << 64) | UInt256(lo); };
    for (int iter = 0; iter < 20000; ++iter) {
        uint64_t d_hi = rnd();
        if (d_hi == 0) {
            d_hi = 1; // high limb non-zero -> divisor genuinely spans two limbs
        }
        const UInt256 d = make128(d_hi, rnd());

        uint64_t q_hi = rnd() >> 1; // < 2^63 so q < 2^127
        if (q_hi == 0) {
            q_hi = 1; // q >= 2^64 so that q*d >= 2^128
        }
        const UInt256 q = make128(q_hi, rnd());

        // r < (d_hi << 64) <= d by construction, so 0 <= r < d without dividing.
        const uint64_t r_hi = rnd() % d_hi;
        const UInt256 r = make128(r_hi, rnd());

        const UInt256 n = q * d + r;

        ASSERT_EQ(n / d, q) << "iter " << iter;
        ASSERT_EQ(n % d, r) << "iter " << iter;
    }
}

TEST(WideInteger, TwoLimbDivisorIdentity) {
    // Full 256-bit dividends divided by random 2-limb (128-bit) divisors. No native
    // oracle exists for 256-bit, so verify q*d + r == n with 0 <= r < d. q*d <= n < 2^256
    // so the multiplication does not overflow.
    std::mt19937_64 rng(0xBADC0DE99ULL);
    auto rnd = [&rng]() { return rng(); };
    auto make256 = [](uint64_t w3, uint64_t w2, uint64_t w1, uint64_t w0) {
        return (UInt256(w3) << 192) | (UInt256(w2) << 128) | (UInt256(w1) << 64) | UInt256(w0);
    };
    for (int iter = 0; iter < 20000; ++iter) {
        const UInt256 n = make256(rnd(), rnd(), rnd(), rnd());
        uint64_t hi = rnd();
        if (hi == 0) {
            hi = 1; // ensure the divisor spans two limbs (>= 2^64)
        }
        const UInt256 d = (UInt256(hi) << 64) | UInt256(rnd());

        const UInt256 q = n / d;
        const UInt256 r = n % d;
        ASSERT_EQ(q * d + r, n) << "iter " << iter;
        ASSERT_TRUE(r < d) << "iter " << iter;
    }
}

TEST(WideInteger, TwoLimbDivisorBoundaries) {
    const UInt256 n = (UInt256(0xFEDCBA9876543210ULL) << 192) |
                      (UInt256(0x1122334455667788ULL) << 128) |
                      (UInt256(0x99AABBCCDDEEFF00ULL) << 64) | UInt256(0x0123456789ABCDEFULL);

    const UInt256 two_limb_divisors[] = {
            UInt256(1ULL) << 64,                   // 2^64: smallest 2-limb divisor
            (UInt256(1ULL) << 64) + UInt256(1ULL), // 2^64 + 1
            UInt256(1ULL) << 127,                  // high bit only (s == 0 after normalize)
            (UInt256(1ULL) << 127) + UInt256(0x123456789AULL),
            (UInt256(1ULL) << 128) - UInt256(1ULL), // 2^128 - 1: largest 2-limb divisor
            (UInt256(10000000000000000000ULL) * UInt256(100ULL)), // 10^21, needs two limbs
    };
    for (const UInt256& d : two_limb_divisors) {
        const UInt256 q = n / d;
        const UInt256 r = n % d;
        ASSERT_EQ(q * d + r, n);
        ASSERT_TRUE(r < d);
    }

    // Known answer: 10^38 / 10^20 == 10^18, remainder 0 (10^20 is a 2-limb divisor).
    const UInt256 p19 = UInt256(10000000000000000000ULL);
    const UInt256 p38 = p19 * p19;
    const UInt256 p20 = p19 * UInt256(10ULL);
    const UInt256 p18 = UInt256(1000000000000000000ULL);
    ASSERT_EQ(p38 / p20, p18);
    ASSERT_EQ(p38 % p20, UInt256(0ULL));
}

TEST(WideInteger, WideDivisorRegression) {
    // Divisors wider than 128 bits take the generic binary long-division fallback;
    // confirm that path still satisfies the division identity.
    std::mt19937_64 rng(0x5EED1234ULL);
    auto rnd = [&rng]() { return rng(); };
    auto make256 = [](uint64_t w3, uint64_t w2, uint64_t w1, uint64_t w0) {
        return (UInt256(w3) << 192) | (UInt256(w2) << 128) | (UInt256(w1) << 64) | UInt256(w0);
    };
    for (int iter = 0; iter < 5000; ++iter) {
        const UInt256 n = make256(rnd(), rnd(), rnd(), rnd());
        uint64_t w2 = rnd();
        if (w2 == 0) {
            w2 = 1; // force the divisor past 128 bits (third limb non-zero)
        }
        const UInt256 d = make256(0, w2, rnd(), rnd());

        const UInt256 q = n / d;
        const UInt256 r = n % d;
        ASSERT_EQ(q * d + r, n) << "iter " << iter;
        ASSERT_TRUE(r < d) << "iter " << iter;
    }
}

TEST(WideInteger, TwoLimbSignedDivision) {
    const Int256 n = (Int256(0x0011223344556677LL) << 192) | (Int256(0x18293A4B5C6D7E8FLL) << 128) |
                     (Int256(0x1122334455667788LL) << 64) | Int256(0x99AABBCCDDEEFF01LL);
    const Int256 d = (Int256(0x0000000100000002LL) << 64) | Int256(0x0000000300000005LL);

    ASSERT_EQ((-n) / d, -(n / d));
    ASSERT_EQ(n / (-d), -(n / d));
    ASSERT_EQ((-n) / (-d), n / d);
    ASSERT_EQ((-n) % d, -(n % d));
    ASSERT_EQ(n % (-d), n % d);
}

// The 256-bit divide() fast path (Decimal256): when both operands fit in 128
// bits it takes a single hardware __int128 divide instead of the shift-subtract
// software loop. These tests pin it to be bit-exact with the slow path and with
// native __int128, and confirm the slow path (high limbs set) still works.
TEST(WideInteger, Divide256FastPathFits128) {
    // Values whose magnitudes fit in 128 bits -> fast path. Oracle = __int128.
    // Cover both limbs (values > 2^64), exact and inexact quotients, r==0, a<b.
    const unsigned __int128 hi = (unsigned __int128)(12345678901234567890ULL);
    const std::pair<unsigned __int128, unsigned __int128> cases[] = {
            {(unsigned __int128)1000000000000000000ULL, 7},
            {(hi << 64) | 0x0123456789abcdefULL, 1000000007ULL},
            {(hi << 60), (unsigned __int128)999983ULL},
            {(unsigned __int128)42, (unsigned __int128)100},       // a < b -> q=0, r=a
            {(unsigned __int128)1000000, (unsigned __int128)1000}, // exact -> r=0
            {~(unsigned __int128)0, (unsigned __int128)3},         // max 128-bit numerator
            {(hi << 64) | hi, hi},
    };
    for (auto [a128, b128] : cases) {
        UInt256 a(a128);
        UInt256 b(b128);
        UInt256 q = a / b;
        UInt256 r = a % b;
        ASSERT_EQ(__uint128_t(q), a128 / b128);
        ASSERT_EQ(__uint128_t(r), a128 % b128);
        // Division identity within 256-bit arithmetic.
        ASSERT_EQ(q * b + r, a);
    }
}

TEST(WideInteger, Divide256SlowPathHighLimbs) {
    // Numerator needs bits above 128. The small (<=64-bit) divisors here now route
    // through the layer-2 word-wise fast path; only the wide_divisor case below
    // (divisor > 128 bits) genuinely exercises the shift-subtract software loop.
    // Either way the results must reconstruct exactly.
    UInt256 big = UInt256(12345678901234567890ULL) << 130; // bits set above 128
    big += UInt256(0xdeadbeefcafef00dULL);

    for (uint64_t d :
         {uint64_t(1), uint64_t(2), uint64_t(1000000007ULL), uint64_t(9999999999999999999ULL)}) {
        UInt256 divisor(d);
        UInt256 q = big / divisor;
        UInt256 r = big % divisor;
        ASSERT_TRUE(r < divisor);
        ASSERT_EQ(q * divisor + r, big); // reconstruct exactly
    }

    // Divisor exceeds 128 bits -> both operands wide, so this is the true slow path.
    UInt256 wide_divisor = UInt256(1000000009ULL) << 100;
    UInt256 q = big / wide_divisor;
    UInt256 r = big % wide_divisor;
    ASSERT_TRUE(r < wide_divisor);
    ASSERT_EQ(q * wide_divisor + r, big);
}

TEST(WideInteger, Divide256WideNumeratorSmallDivisor) {
    // The CEIL/ROUND `x / 10^k` hot shape: a full-width Decimal256 numerator with
    // a small (<=64-bit) divisor. This exercises the word-wise long-division fast
    // path (layer 2), which the "both fit 128" path (layer 1) does not cover.
    // Build a numerator that genuinely needs bits above 128.
    UInt256 x = UInt256(0xABCDEF0123456789ULL);
    x <<= 64;
    x += UInt256(0x1122334455667788ULL);
    x <<= 64;
    x += UInt256(0x99AABBCCDDEEFF00ULL); // ~192 significant bits

    // Powers of ten (the real rounding divisors) plus a couple of primes.
    for (uint64_t d : {uint64_t(1), uint64_t(10), uint64_t(1000), uint64_t(1000000000ULL),
                       uint64_t(1000000000000000000ULL), uint64_t(1000000007ULL),
                       uint64_t(9999999999999999999ULL)}) {
        UInt256 divisor(d);
        UInt256 q = x / divisor;
        UInt256 r = x % divisor;
        ASSERT_TRUE(r < divisor);
        ASSERT_EQ(q * divisor + r, x); // bit-exact reconstruction
    }

    // The quotient of a wide value by a small divisor still needs high limbs, so
    // this really is the wide path, not an accidental fit-128.
    UInt256 q10 = x / UInt256(10);
    ASSERT_TRUE(q10 > (UInt256(1) << 128));
}

TEST(WideInteger, Divide256SignedNegative) {
    // Sign handling is applied by operator_slash around the unsigned divide; the
    // fast path must not disturb it. Both operands fit 128 bits.
    Int256 a = Int256(1000000000000000000LL) * Int256(1000000000LL); // 1e27, fits 128b
    Int256 b = 999983;
    ASSERT_EQ(a / b, Int256(1000000000000000000LL) * Int256(1000000000LL) / Int256(999983));
    ASSERT_EQ((-a) / b, -(a / b));
    ASSERT_EQ(a / (-b), -(a / b));
    ASSERT_EQ((-a) / (-b), a / b);
    // Remainder sign follows the dividend (truncated division).
    ASSERT_EQ((-a) % b, -(a % b));
    ASSERT_EQ(a % b + (a / b) * b, a);
}

TEST(WideInteger, Divide256BoundaryAt128Bits) {
    // Exactly 2^128 in the numerator forces a high limb -> slow path; 2^128 - 1
    // is the largest fast-path numerator. Both must be correct.
    UInt256 two_128 = UInt256(1) << 128;
    UInt256 max_128 = two_128 - 1; // all low 128 bits set, high limbs zero
    UInt256 d(1000000007ULL);

    UInt256 q1 = max_128 / d, r1 = max_128 % d;
    ASSERT_EQ(q1 * d + r1, max_128);
    ASSERT_TRUE(r1 < d);

    UInt256 q2 = two_128 / d, r2 = two_128 % d;
    ASSERT_EQ(q2 * d + r2, two_128);
    ASSERT_TRUE(r2 < d);

    // The two results differ by exactly the extra unit.
    ASSERT_EQ(two_128 - max_128, UInt256(1));
}

TEST(WideInteger, Divide256ByZeroThrows) {
    // A zero denominator must still throw on the wide path (fast path skips it).
    UInt256 a(123456789ULL);
    UInt256 zero(0);
    bool threw = false;
    try {
        UInt256 q = a / zero;
        (void)q;
    } catch (...) {
        threw = true;
    }
    ASSERT_TRUE(threw);
}

// Assemble a 256-bit value from four explicit 64-bit limbs (limb0 = least
// significant). Lets tests place bits in any limb, including the top one.
static UInt256 u256_from_limbs(uint64_t l3, uint64_t l2, uint64_t l1, uint64_t l0) {
    UInt256 v(l3);
    v <<= 64;
    v += UInt256(l2);
    v <<= 64;
    v += UInt256(l1);
    v <<= 64;
    v += UInt256(l0);
    return v;
}

// Randomized layer-1 fast path ("both operands fit 128 bits") against a native
// __int128 oracle -- the strongest possible reference, hardware division. Every
// generated (a, b) has zero high limbs so it is guaranteed to take layer 1.
TEST(WideInteger, Divide256FastPathFits128Fuzz) {
    std::mt19937_64 rng(0xD1A5C0DE12345678ULL);
    std::uniform_int_distribution<uint64_t> anybits;

    for (int iter = 0; iter < 50000; ++iter) {
        unsigned __int128 a = (static_cast<unsigned __int128>(anybits(rng)) << 64) | anybits(rng);
        unsigned __int128 b = (static_cast<unsigned __int128>(anybits(rng)) << 64) | anybits(rng);
        if (b == 0) {
            b = 1;
        }
        // Occasionally shrink one side so a < b, a fits 64, exact multiples, etc.
        switch (iter & 7) {
        case 0:
            a &= 0xFFFFFFFFFFFFFFFFULL;
            break; // a fits 64 bits
        case 1:
            b &= 0xFFFFFFFFULL;
            if (b == 0) b = 1;
            break; // small divisor
        case 2:
            a = b + (a % (b ? b : 1));
            break; // a slightly >= b
        case 3:
            b = a ? (a / 2 + 1) : 1;
            break; // q around 1..2
        default:
            break;
        }

        UInt256 wa(a), wb(b);
        UInt256 q = wa / wb;
        UInt256 r = wa % wb;
        ASSERT_EQ(__uint128_t(q), a / b) << "iter=" << iter;
        ASSERT_EQ(__uint128_t(r), a % b) << "iter=" << iter;
    }
}

// Randomized layer-2 fast path (wide dividend / <=64-bit divisor) with an
// INDEPENDENT ground-truth oracle. Instead of computing q,r from the operation
// and checking q*d+r==x (which a wrapped-around wrong q could still satisfy), we
// pick q and r FIRST, build x = q*d + r under bounds that cannot overflow 2^256,
// then require the divide to recover exactly that q and r. This is the check the
// existing reconstruction tests cannot make.
TEST(WideInteger, Divide256WideBySmallGroundTruth) {
    std::mt19937_64 rng(0xC0FFEE5EED9901ULL);
    std::uniform_int_distribution<uint64_t> anybits;

    const uint64_t edge_divisors[] = {1ULL,
                                      2ULL,
                                      10ULL,
                                      1000000000000000000ULL,  // 10^18
                                      10000000000000000000ULL, // 10^19
                                      (1ULL << 63),            // 2^63
                                      0xFFFFFFFFFFFFFFFFULL,   // 2^64 - 1
                                      1000000007ULL};
    const size_t edge_count = sizeof(edge_divisors) / sizeof(uint64_t);

    for (int iter = 0; iter < 50000; ++iter) {
        // q uses limbs 0..2 (up to 192 bits). With d < 2^64, q*d < 2^256, so
        // x = q*d + r (r < d < 2^64) never overflows -> division is exact.
        UInt256 q = u256_from_limbs(0, anybits(rng), anybits(rng), anybits(rng));

        uint64_t d = ((iter & 3) == 0) ? edge_divisors[(iter >> 2) % edge_count] : anybits(rng);
        if (d == 0) {
            d = 1;
        }
        uint64_t r = (d == 1) ? 0 : (anybits(rng) % d); // 0 <= r < d

        UInt256 dv(d);
        UInt256 x = q * dv + UInt256(r);

        ASSERT_EQ(x / dv, q) << "iter=" << iter << " d=" << d;
        ASSERT_EQ(x % dv, UInt256(r)) << "iter=" << iter << " d=" << d;
    }

    // Also pin the all-limbs-set extreme: numerator = 2^256 - 1. We cannot build
    // this from a known q, so we fall back to reconstruction + r < d as a smoke
    // test for the largest possible dividend (the randomized cases above are the
    // real ground-truth guarantee).
    UInt256 maxv = ~UInt256(0);
    for (uint64_t d : edge_divisors) {
        UInt256 dv(d);
        UInt256 q = maxv / dv;
        UInt256 r = maxv % dv;
        ASSERT_TRUE(r < dv) << "d=" << d;
        ASSERT_EQ(q * dv + r, maxv) << "d=" << d;
    }
}

// Signed wide dividend / small divisor with an INDEPENDENT oracle for both sign
// and magnitude. Truncated division: q truncates toward zero, and the remainder
// takes the sign of the dividend. We construct a positive (q_abs, r_abs, d), then
// check all four sign combinations against values derived without calling divide.
TEST(WideInteger, Divide256SignedWideGroundTruth) {
    std::mt19937_64 rng(0x5160EDA7A1234567ULL);
    std::uniform_int_distribution<uint64_t> anybits;

    for (int iter = 0; iter < 20000; ++iter) {
        // Cap q_abs to ~189 bits so the signed magnitude X = q_abs*d + r_abs stays
        // well below 2^255 for any d < 2^64 (no signed overflow), while still
        // needing bits above 128 -> genuinely the wide path.
        uint64_t l2 = anybits(rng) & ((1ULL << 61) - 1);
        UInt256 q_abs_u = u256_from_limbs(0, l2, anybits(rng), anybits(rng));

        uint64_t d = anybits(rng);
        if (d < 2) {
            d = 2; // keep room for a nonzero remainder
        }
        uint64_t r_abs = anybits(rng) % d; // 0 <= r_abs < d

        UInt256 x_u = q_abs_u * UInt256(d) + UInt256(r_abs);
        Int256 X(x_u);
        Int256 D(d);
        Int256 Q(q_abs_u);
        Int256 R(r_abs);

        // ++ : x/d
        ASSERT_EQ(X / D, Q) << "iter=" << iter;
        ASSERT_EQ(X % D, R) << "iter=" << iter;
        // -+ : (-x)/d = -q, remainder follows dividend sign -> -r
        ASSERT_EQ((-X) / D, -Q) << "iter=" << iter;
        ASSERT_EQ((-X) % D, -R) << "iter=" << iter;
        // +- : x/(-d) = -q, remainder still follows dividend sign -> +r
        ASSERT_EQ(X / (-D), -Q) << "iter=" << iter;
        ASSERT_EQ(X % (-D), R) << "iter=" << iter;
        // -- : (-x)/(-d) = q, remainder -> -r
        ASSERT_EQ((-X) / (-D), Q) << "iter=" << iter;
        ASSERT_EQ((-X) % (-D), -R) << "iter=" << iter;
    }
}

} // namespace doris
