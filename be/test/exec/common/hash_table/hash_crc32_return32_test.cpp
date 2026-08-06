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

#include "exec/common/hash_table/hash_crc32_return32.h"

#include <gtest/gtest.h>

#include <cstring>
#include <string>

namespace doris {

class HashCRC32Return32Test : public ::testing::Test {};

// --- UInt8 golden values ---

TEST_F(HashCRC32Return32Test, UInt8Zero) {
    HashCRC32Return32<UInt8> hasher;
    EXPECT_EQ(hasher(0), 2911022254U);
}

TEST_F(HashCRC32Return32Test, UInt8One) {
    HashCRC32Return32<UInt8> hasher;
    EXPECT_EQ(hasher(1), 1609117613U);
}

TEST_F(HashCRC32Return32Test, UInt8Max) {
    HashCRC32Return32<UInt8> hasher;
    EXPECT_EQ(hasher(255), 16777215U);
}

// --- UInt16 golden values ---

TEST_F(HashCRC32Return32Test, UInt16Zero) {
    HashCRC32Return32<UInt16> hasher;
    EXPECT_EQ(hasher(0), 245270573U);
}

TEST_F(HashCRC32Return32Test, UInt16One) {
    HashCRC32Return32<UInt16> hasher;
    EXPECT_EQ(hasher(1), 490475610U);
}

TEST_F(HashCRC32Return32Test, UInt16_12345) {
    HashCRC32Return32<UInt16> hasher;
    EXPECT_EQ(hasher(12345), 4035283152U);
}

// --- UInt32 golden values ---

TEST_F(HashCRC32Return32Test, UInt32Zero) {
    HashCRC32Return32<UInt32> hasher;
    EXPECT_EQ(hasher(0), 3080238136U);
}

TEST_F(HashCRC32Return32Test, UInt32One) {
    HashCRC32Return32<UInt32> hasher;
    EXPECT_EQ(hasher(1), 1792876160U);
}

TEST_F(HashCRC32Return32Test, UInt32DeadBeef) {
    HashCRC32Return32<UInt32> hasher;
    EXPECT_EQ(hasher(0xDEADBEEF), 3187779884U);
}

// --- UInt64 golden values ---

TEST_F(HashCRC32Return32Test, UInt64Zero) {
    HashCRC32Return32<UInt64> hasher;
    EXPECT_EQ(hasher(0), 1943489909U);
}

TEST_F(HashCRC32Return32Test, UInt64One) {
    HashCRC32Return32<UInt64> hasher;
    EXPECT_EQ(hasher(1), 988491858U);
}

TEST_F(HashCRC32Return32Test, UInt64DeadBeefCafeBabe) {
    HashCRC32Return32<UInt64> hasher;
    EXPECT_EQ(hasher(0xDEADBEEFCAFEBABEULL), 1310665699U);
}

// --- UInt128 golden values ---

TEST_F(HashCRC32Return32Test, UInt128_1_2) {
    HashCRC32Return32<UInt128> hasher;
    UInt128 x;
    x.items[0] = 1; // low
    x.items[1] = 2; // high
    EXPECT_EQ(hasher(x), 3724251813U);
}

TEST_F(HashCRC32Return32Test, UInt128AllZero) {
    HashCRC32Return32<UInt128> hasher;
    UInt128 x;
    x.items[0] = 0;
    x.items[1] = 0;
    EXPECT_EQ(hasher(x), 3180291349U);
}

// --- UInt256 golden values ---

TEST_F(HashCRC32Return32Test, UInt256_1234) {
    HashCRC32Return32<UInt256> hasher;
    UInt256 x;
    x.items[0] = 1;
    x.items[1] = 2;
    x.items[2] = 3;
    x.items[3] = 4;
    EXPECT_EQ(hasher(x), 190712045U);
}

TEST_F(HashCRC32Return32Test, UInt256AllZero) {
    HashCRC32Return32<UInt256> hasher;
    UInt256 x;
    x.items[0] = 0;
    x.items[1] = 0;
    x.items[2] = 0;
    x.items[3] = 0;
    EXPECT_EQ(hasher(x), 1970194773U);
}

// --- UInt72 golden values ---

TEST_F(HashCRC32Return32Test, UInt72NonZero) {
    HashCRC32Return32<UInt72> hasher;
    UInt72 x;
    x.a = 0x42;
    x.b = 0x123456789ABCDEF0ULL;
    EXPECT_EQ(hasher(x), 3536150414U);
}

TEST_F(HashCRC32Return32Test, UInt72AllZero) {
    HashCRC32Return32<UInt72> hasher;
    UInt72 x;
    x.a = 0;
    x.b = 0;
    EXPECT_EQ(hasher(x), 1142593372U);
}

// --- UInt96 golden values ---

TEST_F(HashCRC32Return32Test, UInt96NonZero) {
    HashCRC32Return32<UInt96> hasher;
    UInt96 x;
    x.a = 0xDEAD;
    x.b = 0xBEEFCAFEBABE0000ULL;
    EXPECT_EQ(hasher(x), 512267721U);
}

TEST_F(HashCRC32Return32Test, UInt96AllZero) {
    HashCRC32Return32<UInt96> hasher;
    UInt96 x;
    x.a = 0;
    x.b = 0;
    EXPECT_EQ(hasher(x), 3567209122U);
}

// --- UInt104 golden values ---

TEST_F(HashCRC32Return32Test, UInt104NonZero) {
    HashCRC32Return32<UInt104> hasher;
    UInt104 x;
    x.a = 0xFF;
    x.b = 0x12345678;
    x.c = 0xABCDEF0123456789ULL;
    EXPECT_EQ(hasher(x), 1609808253U);
}

TEST_F(HashCRC32Return32Test, UInt104AllZero) {
    HashCRC32Return32<UInt104> hasher;
    UInt104 x;
    x.a = 0;
    x.b = 0;
    x.c = 0;
    EXPECT_EQ(hasher(x), 1134844443U);
}

// --- UInt136 golden values ---

TEST_F(HashCRC32Return32Test, UInt136NonZero) {
    HashCRC32Return32<UInt136> hasher;
    UInt136 x;
    x.a = 0xAB;
    x.b = 0x1111111111111111ULL;
    x.c = 0x2222222222222222ULL;
    EXPECT_EQ(hasher(x), 2052761058U);
}

TEST_F(HashCRC32Return32Test, UInt136AllZero) {
    HashCRC32Return32<UInt136> hasher;
    UInt136 x;
    x.a = 0;
    x.b = 0;
    x.c = 0;
    EXPECT_EQ(hasher(x), 621960214U);
}

// --- StringRef golden values ---

TEST_F(HashCRC32Return32Test, StringRefEmpty) {
    HashCRC32Return32<StringRef> hasher;
    StringRef s("", 0);
    EXPECT_EQ(hasher(s), 0U);
}

TEST_F(HashCRC32Return32Test, StringRefShort) {
    HashCRC32Return32<StringRef> hasher;
    StringRef s("ab", 2);
    EXPECT_EQ(hasher(s), 4090249022U);
}

TEST_F(HashCRC32Return32Test, StringRefMedium) {
    HashCRC32Return32<StringRef> hasher;
    StringRef s1("hello", 5);
    EXPECT_EQ(hasher(s1), 349252686U);

    StringRef s2("doris", 5);
    EXPECT_EQ(hasher(s2), 1446130382U);
}

TEST_F(HashCRC32Return32Test, StringRefLong) {
    HashCRC32Return32<StringRef> hasher;
    StringRef s("hello world 1234", 16);
    EXPECT_EQ(hasher(s), 3941215920U);
}

TEST_F(HashCRC32Return32Test, StringRefExact8) {
    HashCRC32Return32<StringRef> hasher;
    StringRef s1("abcdefgh", 8);
    EXPECT_EQ(hasher(s1), 1017768486U);

    StringRef s2("12345678", 8);
    EXPECT_EQ(hasher(s2), 3164243031U);
}

// --- Determinism: same input always produces same hash ---

TEST_F(HashCRC32Return32Test, Deterministic) {
    HashCRC32Return32<UInt32> hasher32;
    uint32_t first = hasher32(42);
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(hasher32(42), first);
    }

    HashCRC32Return32<UInt64> hasher64;
    uint32_t first64 = hasher64(0xDEADBEEFULL);
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(hasher64(0xDEADBEEFULL), first64);
    }
}

// --- Different inputs produce different hashes ---

TEST_F(HashCRC32Return32Test, DifferentInputsDifferentHashes) {
    HashCRC32Return32<UInt32> hasher;
    EXPECT_NE(hasher(0), hasher(1));
    EXPECT_NE(hasher(1), hasher(2));
    EXPECT_NE(hasher(0), hasher(0xFFFFFFFF));

    HashCRC32Return32<UInt64> hasher64;
    EXPECT_NE(hasher64(0), hasher64(1));
    EXPECT_NE(hasher64(100), hasher64(200));
}

// --- Different widths produce different hashes for same numeric value ---

TEST_F(HashCRC32Return32Test, DifferentWidthsDifferentHashes) {
    HashCRC32Return32<UInt8> h8;
    HashCRC32Return32<UInt16> h16;
    HashCRC32Return32<UInt32> h32;
    HashCRC32Return32<UInt64> h64;

    EXPECT_NE(h8(1), h16(1));
    EXPECT_NE(h16(1), h32(1));
    EXPECT_NE(h32(1), h64(1));
}

} // namespace doris
