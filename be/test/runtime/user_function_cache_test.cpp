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

#include "runtime/user_function_cache.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <string>

#include "gtest/gtest.h"

namespace doris {

class UserFunctionCacheTest : public ::testing::Test {
protected:
    UserFunctionCache ufc;
};

TEST_F(UserFunctionCacheTest, SplitStringByChecksumTest) {
    // Test valid string format
    std::string valid_str =
            "7119053928154065546.20c8228267b6c9ce620fddb39467d3eb.postgresql-42.5.0.jar";
    auto result = ufc._split_string_by_checksum(valid_str);
    ASSERT_EQ(result.size(), 4);
    EXPECT_EQ(result[0], "7119053928154065546");
    EXPECT_EQ(result[1], "20c8228267b6c9ce620fddb39467d3eb");
    EXPECT_EQ(result[2], "postgresql-42.5.0");
    EXPECT_EQ(result[3], "jar");
}

} // namespace doris
