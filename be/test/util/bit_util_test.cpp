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

#include "util/bit_util.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <boost/utility/binary.hpp>

#include "gtest/gtest_pred_impl.h"

namespace doris {

TEST(BitUtil, Ceil) {
    EXPECT_EQ(BitUtil::ceil(0, 1), 0);
    EXPECT_EQ(BitUtil::ceil(1, 1), 1);
    EXPECT_EQ(BitUtil::ceil(1, 2), 1);
    EXPECT_EQ(BitUtil::ceil(1, 8), 1);
    EXPECT_EQ(BitUtil::ceil(7, 8), 1);
    EXPECT_EQ(BitUtil::ceil(8, 8), 1);
    EXPECT_EQ(BitUtil::ceil(9, 8), 2);
}

TEST(BitUtil, Popcount) {
    EXPECT_EQ(BitUtil::popcount(BOOST_BINARY(0 1 0 1 0 1 0 1)), 4);
    EXPECT_EQ(BitUtil::popcount_no_hw(BOOST_BINARY(0 1 0 1 0 1 0 1)), 4);
    EXPECT_EQ(BitUtil::popcount(BOOST_BINARY(1 1 1 1 0 1 0 1)), 6);
    EXPECT_EQ(BitUtil::popcount_no_hw(BOOST_BINARY(1 1 1 1 0 1 0 1)), 6);
    EXPECT_EQ(BitUtil::popcount(BOOST_BINARY(1 1 1 1 1 1 1 1)), 8);
    EXPECT_EQ(BitUtil::popcount_no_hw(BOOST_BINARY(1 1 1 1 1 1 1 1)), 8);
    EXPECT_EQ(BitUtil::popcount(0), 0);
    EXPECT_EQ(BitUtil::popcount_no_hw(0), 0);
}

TEST(BitUtil, BigEndianToHost) {
    uint16_t v16 = 0x1234;
    uint32_t v32 = 0x12345678;
    uint64_t v64 = 0x123456789abcdef0;
    unsigned __int128 v128 = ((__int128)0x123456789abcdef0LL << 64) | 0x123456789abcdef0LL;
    wide::UInt256 v256 =
            wide::UInt256(0x123456789abcdef0) << 192 | wide::UInt256(0x123456789abcdef0) << 128 |
            wide::UInt256(0x123456789abcdef0) << 64 | wide::UInt256(0x123456789abcdef0);
    EXPECT_EQ(BitUtil::big_endian_to_host(v16), 0x3412);
    EXPECT_EQ(BitUtil::big_endian_to_host(v32), 0x78563412);
    EXPECT_EQ(BitUtil::big_endian_to_host(v64), 0xf0debc9a78563412);
    EXPECT_EQ(BitUtil::big_endian_to_host(v128),
              ((__int128)0xf0debc9a78563412LL << 64) | 0xf0debc9a78563412LL);
    EXPECT_EQ(BitUtil::big_endian_to_host(v256),
              wide::UInt256(0xf0debc9a78563412) << 192 | wide::UInt256(0xf0debc9a78563412) << 128 |
                      wide::UInt256(0xf0debc9a78563412) << 64 | wide::UInt256(0xf0debc9a78563412));
}

} // namespace doris
