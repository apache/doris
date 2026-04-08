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

// the following code are modified from RocksDB:
// https://github.com/facebook/rocksdb/blob/master/util/crc32c_test.cc

#include <crc32c/crc32c.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <string.h>
#include <zlib.h>

#include <cstdint>
#include <limits>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "util/hash_util.hpp"
#include "util/slice.h"

namespace doris {

TEST(CRC, StandardResults) {
    // Original Fast_CRC32 tests.
    // From rfc3720 section B.4.
    char buf[32];

    memset(buf, 0, sizeof(buf));
    EXPECT_EQ(0x8a9136aaU, crc32c::Crc32c(buf, sizeof(buf)));

    memset(buf, 0xff, sizeof(buf));
    EXPECT_EQ(0x62a8ab43U, crc32c::Crc32c(buf, sizeof(buf)));

    for (int i = 0; i < 32; i++) {
        buf[i] = static_cast<char>(i);
    }
    EXPECT_EQ(0x46dd794eU, crc32c::Crc32c(buf, sizeof(buf)));

    for (int i = 0; i < 32; i++) {
        buf[i] = static_cast<char>(31 - i);
    }
    EXPECT_EQ(0x113fdb5cU, crc32c::Crc32c(buf, sizeof(buf)));

    unsigned char data[48] = {
            0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
            0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };
    EXPECT_EQ(0xd9963a56, crc32c::Crc32c(reinterpret_cast<char*>(data), sizeof(data)));
}

TEST(CRC, Values) {
    EXPECT_NE(crc32c::Crc32c(std::string("a")), crc32c::Crc32c(std::string("foo")));
}

TEST(CRC, Extend) {
    auto s1 = std::string("hello ");
    auto s2 = std::string("world");
    EXPECT_EQ(crc32c::Crc32c(std::string("hello world")),
              crc32c::Extend(crc32c::Crc32c(s1), (const uint8_t*)s2.data(), s2.size()));
    std::vector<std::string_view> slices = {s1, s2};
    EXPECT_EQ(crc32c::Crc32c(std::string("hello world")),
              crc32c::Extend(crc32c::Crc32c(slices[0]), (const uint8_t*)s2.data(), s2.size()));
}

} // namespace doris

namespace doris {

// Helper: compute crc32c via crc32c::Crc32c for a value of type T
template <typename T>
uint32_t crc32c_reference(const T& value, uint32_t seed) {
    return crc32c::Extend(seed, reinterpret_cast<const uint8_t*>(&value), sizeof(T));
}

// Helper: compute zlib crc32 for a value of type T
template <typename T>
uint32_t zlib_crc32_reference(const T& value, uint32_t seed) {
    return HashUtil::zlib_crc_hash(&value, sizeof(T), seed);
}

/*
todo: fix those cases when we have a new release version; do not consider the compatibility issue
use following code to replace the old crc32c_fixed function in hash_util.hpp
template <typename T>
static uint32_t crc32c_fixed(const T& value, uint32_t hash) {
    uint32_t crc = hash ^ 0xFFFFFFFFU;
    if constexpr (sizeof(T) == 1) {
        crc = _mm_crc32_u8(crc, *reinterpret_cast<const uint8_t*>(&value));
    } else if constexpr (sizeof(T) == 2) {
        crc = _mm_crc32_u16(crc, *reinterpret_cast<const uint16_t*>(&value));
    } else if constexpr (sizeof(T) == 4) {
        crc = _mm_crc32_u32(crc, *reinterpret_cast<const uint32_t*>(&value));
    } else if constexpr (sizeof(T) == 8) {
        crc = (uint32_t)_mm_crc32_u64(crc, *reinterpret_cast<const uint64_t*>(&value));
    } else {
        return crc32c_extend(hash, (const uint8_t*)&value, sizeof(T));
    }
    return crc ^ 0xFFFFFFFFU;
}
// ==================== crc32c_fixed tests ====================
TEST(CRC32CFixed, Uint8Values) {
    uint8_t values[] = {0, 1, 127, 128, 255};
    for (uint32_t seed : {0U, 1U, 0xFFFFFFFFU, 0xDEADBEEFU}) {
        for (auto v : values) {
            EXPECT_EQ(HashUtil::crc32c_fixed(v, seed), crc32c_reference(v, seed))
                    << "uint8_t v=" << (int)v << " seed=" << seed;
        }
    }
}

TEST(CRC32CFixed, Uint16Values) {
    uint16_t values[] = {0, 1, 255, 256, 1000, 32767, 65535};
    for (uint32_t seed : {0U, 1U, 0xFFFFFFFFU, 0x12345678U}) {
        for (auto v : values) {
            EXPECT_EQ(HashUtil::crc32c_fixed(v, seed), crc32c_reference(v, seed))
                    << "uint16_t v=" << v << " seed=" << seed;
        }
    }
}

TEST(CRC32CFixed, Int32Values) {
    int32_t values[] = {0,
                        1,
                        -1,
                        42,
                        -42,
                        1000000,
                        -1000000,
                        std::numeric_limits<int32_t>::min(),
                        std::numeric_limits<int32_t>::max()};
    for (uint32_t seed : {0U, 0xFFFFFFFFU, 0xCAFEBABEU}) {
        for (auto v : values) {
            EXPECT_EQ(HashUtil::crc32c_fixed(v, seed), crc32c_reference(v, seed))
                    << "int32_t v=" << v << " seed=" << seed;
        }
    }
}

TEST(CRC32CFixed, Uint32Values) {
    uint32_t values[] = {0, 1, 0xFF, 0xFFFF, 0xFFFFFFFF, 0xDEADBEEF, 0x12345678};
    for (uint32_t seed : {0U, 0xFFFFFFFFU, 0xABCD1234U}) {
        for (auto v : values) {
            EXPECT_EQ(HashUtil::crc32c_fixed(v, seed), crc32c_reference(v, seed))
                    << "uint32_t v=" << v << " seed=" << seed;
        }
    }
}

TEST(CRC32CFixed, Int64Values) {
    int64_t values[] = {0,
                        1,
                        -1,
                        1000000000LL,
                        -1000000000LL,
                        std::numeric_limits<int64_t>::min(),
                        std::numeric_limits<int64_t>::max(),
                        0x0102030405060708LL,
                        -0x0102030405060708LL};
    for (uint32_t seed : {0U, 0xFFFFFFFFU, 0x87654321U}) {
        for (auto v : values) {
            EXPECT_EQ(HashUtil::crc32c_fixed(v, seed), crc32c_reference(v, seed))
                    << "int64_t v=" << v << " seed=" << seed;
        }
    }
}

TEST(CRC32CFixed, Uint64Values) {
    uint64_t values[] = {0,
                         1,
                         0xFFFFFFFFFFFFFFFFULL,
                         0xDEADBEEFCAFEBABEULL,
                         0x0123456789ABCDEFULL,
                         0xFF00FF00FF00FF00ULL};
    for (uint32_t seed : {0U, 0xFFFFFFFFU, 0x11111111U}) {
        for (auto v : values) {
            EXPECT_EQ(HashUtil::crc32c_fixed(v, seed), crc32c_reference(v, seed))
                    << "uint64_t v=" << v << " seed=" << seed;
        }
    }
}

TEST(CRC32CFixed, FloatValues) {
    float values[] = {0.0f,
                      -0.0f,
                      1.0f,
                      -1.0f,
                      3.14f,
                      std::numeric_limits<float>::min(),
                      std::numeric_limits<float>::max(),
                      std::numeric_limits<float>::infinity()};
    for (uint32_t seed : {0U, 0xFFFFFFFFU}) {
        for (auto v : values) {
            EXPECT_EQ(HashUtil::crc32c_fixed(v, seed), crc32c_reference(v, seed))
                    << "float v=" << v << " seed=" << seed;
        }
    }
}

TEST(CRC32CFixed, DoubleValues) {
    double values[] = {0.0,
                       -0.0,
                       1.0,
                       -1.0,
                       3.141592653589793,
                       1e100,
                       -1e100,
                       std::numeric_limits<double>::infinity()};
    for (uint32_t seed : {0U, 0xFFFFFFFFU}) {
        for (auto v : values) {
            EXPECT_EQ(HashUtil::crc32c_fixed(v, seed), crc32c_reference(v, seed))
                    << "double v=" << v << " seed=" << seed;
        }
    }
}

TEST(CRC32CFixed, NullHash) {
    // crc32c_null should match crc32c_fixed with int(0)
    for (uint32_t seed : {0U, 0xFFFFFFFFU, 0xDEADBEEFU}) {
        int zero = 0;
        EXPECT_EQ(HashUtil::crc32c_null(seed), HashUtil::crc32c_fixed(zero, seed));
        EXPECT_EQ(HashUtil::crc32c_null(seed), crc32c_reference(zero, seed));
    }
}
*/
// ==================== zlib_crc32_fixed tests ====================

TEST(ZlibCRC32Fixed, Uint8Values) {
    uint8_t values[] = {0, 1, 42, 127, 128, 255};
    for (uint32_t seed : {0U, 1U, 0xFFFFFFFFU, 0xDEADBEEFU}) {
        for (auto v : values) {
            EXPECT_EQ(HashUtil::zlib_crc32_fixed(v, seed), zlib_crc32_reference(v, seed))
                    << "uint8_t v=" << (int)v << " seed=" << seed;
        }
    }
}

TEST(ZlibCRC32Fixed, Int16Values) {
    int16_t values[] = {0, 1, -1, 256, -256, 32767, -32768};
    for (uint32_t seed : {0U, 0xFFFFFFFFU, 0x12345678U}) {
        for (auto v : values) {
            EXPECT_EQ(HashUtil::zlib_crc32_fixed(v, seed), zlib_crc32_reference(v, seed))
                    << "int16_t v=" << v << " seed=" << seed;
        }
    }
}

TEST(ZlibCRC32Fixed, Uint16Values) {
    uint16_t values[] = {0, 1, 255, 256, 1000, 32767, 65535};
    for (uint32_t seed : {0U, 0xFFFFFFFFU, 0xABCDEF00U}) {
        for (auto v : values) {
            EXPECT_EQ(HashUtil::zlib_crc32_fixed(v, seed), zlib_crc32_reference(v, seed))
                    << "uint16_t v=" << v << " seed=" << seed;
        }
    }
}

TEST(ZlibCRC32Fixed, Int32Values) {
    int32_t values[] = {0,
                        1,
                        -1,
                        42,
                        -42,
                        1000000,
                        -1000000,
                        std::numeric_limits<int32_t>::min(),
                        std::numeric_limits<int32_t>::max()};
    for (uint32_t seed : {0U, 0xFFFFFFFFU, 0xCAFEBABEU}) {
        for (auto v : values) {
            EXPECT_EQ(HashUtil::zlib_crc32_fixed(v, seed), zlib_crc32_reference(v, seed))
                    << "int32_t v=" << v << " seed=" << seed;
        }
    }
}

TEST(ZlibCRC32Fixed, Uint32Values) {
    uint32_t values[] = {0, 1, 0xFF, 0xFFFF, 0xFFFFFFFF, 0xDEADBEEF, 0x12345678};
    for (uint32_t seed : {0U, 0xFFFFFFFFU, 0xABCD1234U}) {
        for (auto v : values) {
            EXPECT_EQ(HashUtil::zlib_crc32_fixed(v, seed), zlib_crc32_reference(v, seed))
                    << "uint32_t v=" << v << " seed=" << seed;
        }
    }
}

TEST(ZlibCRC32Fixed, Int64Values) {
    int64_t values[] = {0,
                        1,
                        -1,
                        1000000000LL,
                        -1000000000LL,
                        std::numeric_limits<int64_t>::min(),
                        std::numeric_limits<int64_t>::max(),
                        0x0102030405060708LL,
                        -0x0102030405060708LL};
    for (uint32_t seed : {0U, 0xFFFFFFFFU, 0x87654321U}) {
        for (auto v : values) {
            EXPECT_EQ(HashUtil::zlib_crc32_fixed(v, seed), zlib_crc32_reference(v, seed))
                    << "int64_t v=" << v << " seed=" << seed;
        }
    }
}

TEST(ZlibCRC32Fixed, Uint64Values) {
    uint64_t values[] = {0,
                         1,
                         0xFFFFFFFFFFFFFFFFULL,
                         0xDEADBEEFCAFEBABEULL,
                         0x0123456789ABCDEFULL,
                         0xFF00FF00FF00FF00ULL};
    for (uint32_t seed : {0U, 0xFFFFFFFFU, 0x11111111U}) {
        for (auto v : values) {
            EXPECT_EQ(HashUtil::zlib_crc32_fixed(v, seed), zlib_crc32_reference(v, seed))
                    << "uint64_t v=" << v << " seed=" << seed;
        }
    }
}

TEST(ZlibCRC32Fixed, FloatValues) {
    float values[] = {0.0f,
                      -0.0f,
                      1.0f,
                      -1.0f,
                      3.14f,
                      1e10f,
                      -1e10f,
                      std::numeric_limits<float>::min(),
                      std::numeric_limits<float>::max(),
                      std::numeric_limits<float>::infinity()};
    for (uint32_t seed : {0U, 0xFFFFFFFFU}) {
        for (auto v : values) {
            EXPECT_EQ(HashUtil::zlib_crc32_fixed(v, seed), zlib_crc32_reference(v, seed))
                    << "float v=" << v << " seed=" << seed;
        }
    }
}

TEST(ZlibCRC32Fixed, DoubleValues) {
    double values[] = {0.0,
                       -0.0,
                       1.0,
                       -1.0,
                       3.141592653589793,
                       1e100,
                       -1e100,
                       1e-300,
                       std::numeric_limits<double>::infinity()};
    for (uint32_t seed : {0U, 0xFFFFFFFFU}) {
        for (auto v : values) {
            EXPECT_EQ(HashUtil::zlib_crc32_fixed(v, seed), zlib_crc32_reference(v, seed))
                    << "double v=" << v << " seed=" << seed;
        }
    }
}

TEST(ZlibCRC32Fixed, NullHash) {
    // zlib_crc_hash_null should match zlib_crc32_fixed with int(0)
    for (uint32_t seed : {0U, 0xFFFFFFFFU, 0xDEADBEEFU}) {
        int zero = 0;
        EXPECT_EQ(HashUtil::zlib_crc_hash_null(seed), HashUtil::zlib_crc32_fixed(zero, seed));
        EXPECT_EQ(HashUtil::zlib_crc_hash_null(seed), zlib_crc32_reference(zero, seed));
    }
}

// ==================== Cross-validation: fixed vs non-fixed should differ ====================

TEST(CRC32Fixed, CRC32CVsZlibDiffer) {
    // CRC32C and standard CRC32 use different polynomials, so results should differ
    // (except possibly by coincidence on some values, but not systematically)
    int32_t v = 12345678;
    uint32_t seed = 0;
    uint32_t crc32c_result = HashUtil::crc32c_fixed(v, seed);
    uint32_t zlib_result = HashUtil::zlib_crc32_fixed(v, seed);
    EXPECT_NE(crc32c_result, zlib_result)
            << "CRC32C and zlib CRC32 should produce different results for non-trivial input";
}

// ==================== Chaining: verify incremental hashing ====================
/*
TEST(CRC32CFixed, IncrementalChaining) {
    // Hash two int32 values incrementally and compare with hashing 8 bytes at once
    int32_t a = 0x11223344;
    int32_t b = 0x55667788;
    uint32_t seed = 0;

    uint32_t chained = HashUtil::crc32c_fixed(a, seed);
    chained = HashUtil::crc32c_fixed(b, chained);

    // Reference: hash the 8 bytes sequentially via crc32c::Extend
    uint8_t buf[8];
    memcpy(buf, &a, 4);
    memcpy(buf + 4, &b, 4);
    uint32_t reference = crc32c::Extend(seed, buf, 8);

    EXPECT_EQ(chained, reference);
}
*/
TEST(ZlibCRC32Fixed, IncrementalChaining) {
    // Hash two int32 values incrementally and compare with hashing 8 bytes at once
    int32_t a = 0x11223344;
    int32_t b = 0x55667788;
    uint32_t seed = 0;

    uint32_t chained = HashUtil::zlib_crc32_fixed(a, seed);
    chained = HashUtil::zlib_crc32_fixed(b, chained);

    // Reference: hash the 8 bytes sequentially via zlib crc32
    uint8_t buf[8];
    memcpy(buf, &a, 4);
    memcpy(buf + 4, &b, 4);
    uint32_t reference = (uint32_t)crc32(seed, buf, 8);

    EXPECT_EQ(chained, reference);
}
/*
// ==================== Exhaustive 1-byte test ====================

TEST(CRC32CFixed, AllByteValues) {
    for (int i = 0; i <= 255; i++) {
        uint8_t v = static_cast<uint8_t>(i);
        uint32_t seed = 0x12345678U;
        EXPECT_EQ(HashUtil::crc32c_fixed(v, seed), crc32c_reference(v, seed)) << "byte=" << i;
    }
}

TEST(ZlibCRC32Fixed, AllByteValues) {
    for (int i = 0; i <= 255; i++) {
        uint8_t v = static_cast<uint8_t>(i);
        uint32_t seed = 0x12345678U;
        EXPECT_EQ(HashUtil::zlib_crc32_fixed(v, seed), zlib_crc32_reference(v, seed))
                << "byte=" << i;
    }
}

// ==================== Sequential pattern ====================

TEST(CRC32CFixed, SequentialInt32) {
    // Hash a sequence of increasing int32 values, verify each against reference
    uint32_t seed = 0;
    for (int32_t i = -500; i <= 500; i++) {
        EXPECT_EQ(HashUtil::crc32c_fixed(i, seed), crc32c_reference(i, seed)) << "i=" << i;
    }
}

TEST(ZlibCRC32Fixed, SequentialInt32) {
    uint32_t seed = 0;
    for (int32_t i = -500; i <= 500; i++) {
        EXPECT_EQ(HashUtil::zlib_crc32_fixed(i, seed), zlib_crc32_reference(i, seed)) << "i=" << i;
    }
}
*/
// ==================== Large 16-byte type fallback test ====================

TEST(ZlibCRC32Fixed, LargeTypeFallback) {
    // __int128 is 16 bytes, should hit the fallback path to zlib crc32()
    __int128 value = static_cast<__int128>(0x0102030405060708ULL) << 64 | 0x090A0B0C0D0E0F10ULL;
    uint32_t seed = 0;
    uint32_t fixed_result = HashUtil::zlib_crc32_fixed(value, seed);
    uint32_t ref_result = HashUtil::zlib_crc_hash(&value, sizeof(value), seed);
    EXPECT_EQ(fixed_result, ref_result);
}

} // namespace doris
