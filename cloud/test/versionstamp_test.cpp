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
#include "meta-service/codec.h"
#include "meta-service/keys.h"

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(VersionstampTest, Usage) {
    using namespace doris::cloud;

    // Create a versionstamp from a byte array
    uint8_t data[10] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A};
    Versionstamp vs(data);

    // Check the minimum and maximum versionstamps
    EXPECT_EQ(Versionstamp::min() < vs, true);
    EXPECT_EQ(Versionstamp::max() > vs, true);

    // Check equality and ordering
    Versionstamp vs2(data);
    EXPECT_EQ(vs == vs2, true);
    EXPECT_EQ(vs < Versionstamp::max(), true);

    // Create a versionstamp from a version and order
    uint64_t version = 0x0102030405060708;
    uint16_t order = 0x090A;
    Versionstamp vs3(version, order);
    EXPECT_EQ(vs3.version(), version);
    EXPECT_EQ(vs3.order(), order);

    // The default constructor creates a zeroed versionstamp
    Versionstamp vs_default;
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
