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

#include "olap/rowset/segment_v2/inverted_index/util/fixed_bit_set.h"

#include <gtest/gtest.h>

namespace doris::segment_v2::inverted_index {

class FixedBitSetTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(FixedBitSetTest, Initialization) {
    FixedBitSet bitset(64);
    EXPECT_EQ(64, bitset.length());
    for (int32_t i = 0; i < 64; ++i) {
        EXPECT_FALSE(bitset.get(i));
    }
}

TEST_F(FixedBitSetTest, SetAndGet) {
    FixedBitSet bitset(128);
    bitset.set(0);
    bitset.set(63);
    bitset.set(64);
    bitset.set(127);

    EXPECT_TRUE(bitset.get(0));
    EXPECT_TRUE(bitset.get(63));
    EXPECT_TRUE(bitset.get(64));
    EXPECT_TRUE(bitset.get(127));

    for (int32_t i = 1; i < 63; ++i) {
        EXPECT_FALSE(bitset.get(i));
    }
}

TEST_F(FixedBitSetTest, ClearSingleBit) {
    FixedBitSet bitset(64);
    bitset.set(32);
    EXPECT_TRUE(bitset.get(32));

    bitset.clear(32);
    EXPECT_FALSE(bitset.get(32));
}

TEST_F(FixedBitSetTest, ClearRange) {
    FixedBitSet bitset(256);
    for (int32_t i = 0; i < 256; ++i) {
        bitset.set(i);
    }

    bitset.clear(64, 192);

    for (int32_t i = 0; i < 64; ++i) {
        EXPECT_TRUE(bitset.get(i));
    }
    for (int32_t i = 64; i < 192; ++i) {
        EXPECT_FALSE(bitset.get(i));
    }
    for (int32_t i = 192; i < 256; ++i) {
        EXPECT_TRUE(bitset.get(i));
    }
}

TEST_F(FixedBitSetTest, Cardinality) {
    FixedBitSet bitset(100);
    EXPECT_EQ(0, bitset.cardinality());

    for (int32_t i = 0; i < 100; i += 2) {
        bitset.set(i);
    }
    EXPECT_EQ(50, bitset.cardinality());
}

TEST_F(FixedBitSetTest, Intersects) {
    FixedBitSet bitset1(128);
    FixedBitSet bitset2(128);

    bitset1.set(64);
    bitset2.set(65);
    EXPECT_FALSE(bitset1.intersects(bitset2));

    bitset2.set(64);
    EXPECT_TRUE(bitset1.intersects(bitset2));
}

TEST_F(FixedBitSetTest, BitwiseOr) {
    FixedBitSet bitset1(128);
    FixedBitSet bitset2(128);

    bitset1.set(0);
    bitset1.set(64);
    bitset2.set(64);
    bitset2.set(127);

    bitset1.bit_set_or(bitset2);

    EXPECT_TRUE(bitset1.get(0));
    EXPECT_TRUE(bitset1.get(64));
    EXPECT_TRUE(bitset1.get(127));
}

TEST_F(FixedBitSetTest, NextSetBit) {
    FixedBitSet bitset(256);
    bitset.set(10);
    bitset.set(100);
    bitset.set(200);

    EXPECT_EQ(10, bitset.next_set_bit(0));
    EXPECT_EQ(100, bitset.next_set_bit(11));
    EXPECT_EQ(200, bitset.next_set_bit(101));
    EXPECT_EQ(std::numeric_limits<int32_t>::max(), bitset.next_set_bit(201));
}

TEST_F(FixedBitSetTest, EnsureCapacity) {
    FixedBitSet bitset(64);
    bitset.ensure_capacity(128);

    bitset.set(127);
    EXPECT_TRUE(bitset.get(127));
}

} // namespace doris::segment_v2::inverted_index