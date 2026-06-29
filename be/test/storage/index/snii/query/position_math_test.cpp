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

#include "snii/query/internal/position_math.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <vector>

#include "common/status.h"

using doris::Status; // RETURN_IF_ERROR expands to bare Status  // NOLINT(misc-unused-using-decls)

TEST(SniiPositionMath, AddsOffsetWhenRepresentable) {
    uint32_t out = 0;
    EXPECT_TRUE(snii::query::internal::add_position_offset(41, 1, &out));
    EXPECT_EQ(out, 42U);
    EXPECT_TRUE(snii::query::internal::add_position_offset(std::numeric_limits<uint32_t>::max() - 2,
                                                           2, &out));
    EXPECT_EQ(out, std::numeric_limits<uint32_t>::max());
}

TEST(SniiPositionMath, RejectsWraparound) {
    uint32_t out = 7;
    EXPECT_FALSE(snii::query::internal::add_position_offset(std::numeric_limits<uint32_t>::max(), 1,
                                                            &out));
    EXPECT_EQ(out, 7U);
}

TEST(SniiPositionMath, BuildsDenseOffsets) {
    std::vector<uint32_t> offsets;
    EXPECT_TRUE(snii::query::internal::build_position_offsets(4, &offsets));
    EXPECT_EQ(offsets, (std::vector<uint32_t> {0U, 1U, 2U, 3U}));
}

TEST(SniiPositionMath, RejectsUnrepresentableOffsetCount) {
    std::vector<uint32_t> offsets = {9};
    EXPECT_FALSE(snii::query::internal::build_position_offsets(std::numeric_limits<size_t>::max(),
                                                               &offsets));
    EXPECT_EQ(offsets, (std::vector<uint32_t> {9U}));
}
