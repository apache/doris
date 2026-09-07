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

#include "core/value/uuid_value.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace doris {

TEST(UUIDValueTest, ParseAndFormat) {
    UUIDValueType value;
    ASSERT_TRUE(UUIDValue::from_string(value, "550e8400-e29b-41d4-a716-446655440000"));
    EXPECT_EQ(UUIDValue::to_string(value), "550e8400-e29b-41d4-a716-446655440000");
    EXPECT_EQ(UUIDValue::version(value), 4);

    UUIDValueType uppercase;
    ASSERT_TRUE(UUIDValue::from_string(uppercase, "550E8400-E29B-41D4-A716-446655440000"));
    EXPECT_EQ(uppercase, value);

    UUIDValueType compact;
    ASSERT_TRUE(UUIDValue::from_string(compact, "550e8400e29b41d4a716446655440000"));
    EXPECT_EQ(compact, value);

    UUIDValueType zero = 1;
    ASSERT_TRUE(UUIDValue::from_string(zero, "00000000-0000-0000-0000-000000000000"));
    EXPECT_EQ(zero, 0);
    EXPECT_EQ(UUIDValue::to_string(zero), "00000000-0000-0000-0000-000000000000");
}

TEST(UUIDValueTest, RejectInvalidText) {
    const std::vector<std::string> invalid = {"",
                                              "550e8400-e29b-41d4-a716-44665544000",
                                              "550e8400-e29b-41d4-a716-4466554400000",
                                              "550e8400e29b-41d4-a716-446655440000",
                                              "550e8400-e29b-41d4-a7164466-55440000",
                                              "550e8400-e29b-41d4-a716-44665544000g",
                                              "{550e8400-e29b-41d4-a716-446655440000}"};
    for (const auto& text : invalid) {
        UUIDValueType value = 0;
        EXPECT_FALSE(UUIDValue::from_string(value, text)) << text;
    }
}

TEST(UUIDValueTest, NumericOrderingMatchesCanonicalOrdering) {
    UUIDValueType smaller;
    UUIDValueType larger;
    ASSERT_TRUE(UUIDValue::from_string(smaller, "00000000-0000-0000-ffff-ffffffffffff"));
    ASSERT_TRUE(UUIDValue::from_string(larger, "00000000-0000-0001-0000-000000000000"));
    EXPECT_LT(smaller, larger);
}

} // namespace doris
