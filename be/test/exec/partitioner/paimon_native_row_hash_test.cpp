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

#include "exec/partitioner/external/paimon_native_row_hash.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>

namespace doris::paimon_native {

TEST(PaimonNativeRowHashTest, MatchesPaimonBinaryRowGoldenValues) {
    BinaryRowEncoder empty(0);
    EXPECT_EQ(empty.bytes().size(), 8);
    EXPECT_EQ(empty.hash(), -1670924195);

    BinaryRowEncoder row(1);
    ASSERT_TRUE(row.write_int(0, 1));
    EXPECT_EQ(row.bytes().size(), 16);
    EXPECT_EQ(row.hash(), 1465514398);

    row.reset();
    ASSERT_TRUE(row.set_null(0));
    EXPECT_EQ(row.hash(), -1748325344);

    row.reset();
    ASSERT_TRUE(row.write_string(0, "abcdefgh"));
    EXPECT_EQ(row.bytes().size(), 24);
    EXPECT_EQ(row.hash(), -843760178);

    row.reset();
    ASSERT_TRUE(row.write_string(0, "abc"));
    EXPECT_EQ(row.bytes().size(), 16);
    EXPECT_EQ(row.hash(), -101922419);

    BinaryRowEncoder mixed(2);
    ASSERT_TRUE(mixed.write_int(0, 1));
    ASSERT_TRUE(mixed.write_string(1, "abc"));
    EXPECT_EQ(mixed.hash(), 261371745);
}

TEST(PaimonNativeRowHashTest, MatchesDefaultBucketAndChannelComputer) {
    ASSERT_EQ(default_bucket(1465514398, 4), 2);
    ASSERT_EQ(default_bucket(-7, 4), 3);
    ASSERT_FALSE(default_bucket(1, 0).has_value());

    ASSERT_EQ(fixed_bucket_channel(-1670924195, 2, 8), 5);
    ASSERT_EQ(fixed_bucket_channel(std::numeric_limits<int32_t>::min(), 1, 8), 0);
    ASSERT_FALSE(fixed_bucket_channel(1, 0, 0).has_value());
}

TEST(PaimonNativeRowHashTest, MatchesDynamicBucketAssigner) {
    // BucketAssigner.computeAssigner uses abs(partitionHash % channels) as the
    // partition start and abs(keyHash % assigners) as the assigner offset.
    EXPECT_EQ(dynamic_bucket_assigner_channel(-1670924195, 1465514398, 8, 3), 4);
    EXPECT_EQ(dynamic_bucket_assigner_channel(-7, -7, 8, 3), 0);
    EXPECT_EQ(dynamic_bucket_assigner_channel(std::numeric_limits<int32_t>::min(),
                                              std::numeric_limits<int32_t>::min(), 8, 3),
              2);
    EXPECT_FALSE(dynamic_bucket_assigner_channel(1, 1, 0, 1).has_value());
    EXPECT_FALSE(dynamic_bucket_assigner_channel(1, 1, 2, 3).has_value());
}

} // namespace doris::paimon_native
