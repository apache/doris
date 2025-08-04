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

#include "meta-store/versionstamp.h"

#include <bthread/bthread.h>
#include <bthread/countdown_event.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <iostream>
#include <random>
#include <variant>
#include <vector>

#include "common/util.h"
#include "meta-store/codec.h"
#include "meta-store/keys.h"

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(VersionstampTest, ByteSwap) {
    using namespace doris::cloud;

    // Test byte swapping functions
    constexpr uint64_t original64 = 0x0102030405060708;
    constexpr uint64_t swapped64 = Versionstamp::byteswap64(original64);
    EXPECT_EQ(swapped64, 0x0807060504030201);

    constexpr uint16_t original16 = 0x0102;
    constexpr uint16_t swapped16 = Versionstamp::byteswap16(original16);
    EXPECT_EQ(swapped16, 0x0201);
}

TEST(VersionstampTest, Usage) {
    using namespace doris::cloud;

    // Create a versionstamp from a byte array
    constexpr uint8_t data[10] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A};
    constexpr Versionstamp vs(data);

    // Check the minimum and maximum versionstamps
    EXPECT_EQ(Versionstamp::min() < vs, true);
    EXPECT_EQ(Versionstamp::max() > vs, true);

    // Check equality and ordering
    constexpr Versionstamp vs2(data);
    EXPECT_EQ(vs == vs2, true);
    EXPECT_EQ(vs < Versionstamp::max(), true);

    // Create a versionstamp from a version and order
    constexpr uint64_t version = 0x0102030405060708;
    constexpr uint16_t order = 0x090A;
    constexpr Versionstamp vs3(version, order);
    EXPECT_EQ(vs3.version(), version);
    EXPECT_EQ(vs3.order(), order);

    constexpr Versionstamp vs4(vs.version(), vs.order());
    EXPECT_EQ(vs4.version(), vs.version());
    EXPECT_EQ(vs4.order(), vs.order());
    EXPECT_EQ(vs4.data(), vs.data());

    // The default constructor creates a zeroed versionstamp
    constexpr Versionstamp vs_default;
    EXPECT_EQ(vs_default.version(), 0);
    EXPECT_EQ(vs_default.order(), 0);
    EXPECT_EQ(vs_default == Versionstamp::min(), true);
}

TEST(VersionstampTest, EncodeDecode) {
    using namespace doris::cloud;

    // Create a versionstamp
    uint8_t data[10] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A};
    Versionstamp vs(data);

    // Encode the versionstamp
    std::string encoded;
    uint32_t index = encode_versionstamp(vs, &encoded);
    EXPECT_EQ(encoded.size(), 11); // 1 byte for tag + 10 bytes for versionstamp
    EXPECT_EQ(index, 1);           // The index of the versionstamp in the buffer

    // Decode the versionstamp
    std::string_view encoded_view(encoded);
    Versionstamp decoded_vs;
    int result = decode_versionstamp(&encoded_view, &decoded_vs);
    EXPECT_EQ(result, 0);
    EXPECT_EQ(decoded_vs == vs, true); // Check if data matches
}

TEST(VersionstampTest, Compare) {
    using namespace doris::cloud;

    Versionstamp v1(0x0001, 1);
    Versionstamp v2(0x0001, 2);
    Versionstamp v3(0x0100, 1);

    {
        EXPECT_EQ(v1.version(), 0x0001);
        EXPECT_EQ(v1.order(), 1);
        EXPECT_EQ(v2.version(), 0x0001);
        EXPECT_EQ(v2.order(), 2);
        EXPECT_EQ(v3.version(), 0x0100);
        EXPECT_EQ(v3.order(), 1);
    }

    {
        // Test comparison operators
        EXPECT_EQ(v1 < v2, true);
        EXPECT_EQ(v1 > v2, false);
        EXPECT_EQ(v1 <= v2, true);
        EXPECT_EQ(v1 >= v2, false);
        EXPECT_EQ(v1 == v2, false);

        EXPECT_EQ(v2 < v3, true);
        EXPECT_EQ(v2 > v3, false);
        EXPECT_EQ(v2 <= v3, true);
        EXPECT_EQ(v2 >= v3, false);
        EXPECT_EQ(v2 == v3, false);

        EXPECT_EQ(v1 < v3, true);
        EXPECT_EQ(v1 > v3, false);
        EXPECT_EQ(v1 <= v3, true);
        EXPECT_EQ(v1 >= v3, false);
        EXPECT_EQ(v1 == v3, false);
    }

    {
        EXPECT_EQ(v1.order() < v2.order(), true);
        EXPECT_EQ(v1.version() < v3.version(), true);
    }

    std::string encoded("\x00\x00\x00\x00\x00\x00\x00\x01\x00\x01", 10);
    for (size_t i = 0; i < 10; ++i) {
        EXPECT_EQ(encoded.data()[i], v1.data().data()[i]) << "i: " << i;
    }
}
