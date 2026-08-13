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

#include "exec/sink/paimon_native_row_hash.h"

#include <gtest/gtest.h>

#include <cstdint>

namespace doris::paimon_native {

TEST(PaimonNativeRowHashTest, MatchesPaimonBinaryRowPrimitiveGoldenValues) {
    BinaryRowEncoder empty(0);
    EXPECT_EQ(empty.bytes().size(), 8);
    EXPECT_EQ(empty.hash(), -1670924195);

    BinaryRowEncoder row(1);
    ASSERT_TRUE(row.write_int(0, 1));
    EXPECT_EQ(row.bytes().size(), 16);
    EXPECT_EQ(row.hash(), 1465514398);

    row.reset();
    ASSERT_TRUE(row.write_int(0, -7));
    EXPECT_EQ(row.hash(), 1485184587);

    row.reset();
    ASSERT_TRUE(row.set_null(0));
    EXPECT_EQ(row.hash(), -1748325344);

    BinaryRowEncoder pair(2);
    ASSERT_TRUE(pair.write_int(0, 1));
    ASSERT_TRUE(pair.write_int(1, 2));
    EXPECT_EQ(pair.bytes().size(), 24);
    EXPECT_EQ(pair.hash(), 1909828896);
}

TEST(PaimonNativeRowHashTest, MatchesPaimonBinaryRowStringGoldenValues) {
    BinaryRowEncoder row(1);
    ASSERT_TRUE(row.write_string(0, "abc"));
    EXPECT_EQ(row.bytes().size(), 16);
    EXPECT_EQ(row.hash(), -101922419);

    row.reset();
    ASSERT_TRUE(row.write_string(0, "abcdefgh"));
    EXPECT_EQ(row.bytes().size(), 24);
    EXPECT_EQ(row.hash(), -843760178);
}

TEST(PaimonNativeRowHashTest, MatchesPaimonBinaryRowMixedGoldenValue) {
    BinaryRowEncoder row(8);
    ASSERT_TRUE(row.write_boolean(0, true));
    ASSERT_TRUE(row.write_tinyint(1, -2));
    ASSERT_TRUE(row.write_smallint(2, 300));
    ASSERT_TRUE(row.write_int(3, 123456));
    ASSERT_TRUE(row.write_bigint(4, -9000000000LL));
    ASSERT_TRUE(row.write_float(5, 1.25F));
    ASSERT_TRUE(row.write_double(6, -3.5));
    ASSERT_TRUE(row.write_string(7, "hello world"));

    EXPECT_EQ(row.bytes().size(), 88);
    EXPECT_EQ(row.hash(), 224602692);
}

TEST(PaimonNativeRowHashTest, RejectsInvalidEncoderPosition) {
    BinaryRowEncoder row(1);
    EXPECT_FALSE(row.write_int(1, 1));
}

TEST(PaimonNativeRowHashTest, MatchesPaimonDefaultBucketGoldenValues) {
    const int32_t integer_key_hash = 1465514398;

    ASSERT_TRUE(default_bucket(integer_key_hash, 4).has_value());
    EXPECT_EQ(*default_bucket(integer_key_hash, 4), 2);

    // Paimon uses Math.abs(value % divisor), not floorMod.
    EXPECT_EQ(*default_bucket(-7, 4), 3);
}

TEST(PaimonNativeRowHashTest, RejectsInvalidRoutingInput) {
    EXPECT_FALSE(default_bucket(1, 0).has_value());
}

} // namespace doris::paimon_native
