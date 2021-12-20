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

#include "olap/decimal12.h"

#include <gtest/gtest.h>

namespace doris {

TEST(FieldInfoTest, Add) {
    int64_t a_integer = 9223372036854775806L;
    int a_fraction = 1;
    decimal12_t a = {a_integer, a_fraction};
    decimal12_t b = {1, 0};
    a += b;
    ASSERT_EQ(a_integer + 1, a.integer);
    ASSERT_EQ(a_fraction, a.fraction);

    a.integer = -9223372036854775807L;
    a.fraction = -1;
    b.integer = 0;
    a += b;
    ASSERT_EQ(-9223372036854775807L, a.integer);
    ASSERT_EQ(-1, a.fraction);

    a.integer = -1;
    a.fraction = 0;
    b.integer = -7;
    b.fraction = 0;
    a += b;
    ASSERT_EQ(-8, a.integer);
    ASSERT_EQ(0, a.fraction);

    a.integer = 0;
    a.fraction = -1;
    b.integer = 0;
    b.fraction = -2;
    a += b;
    ASSERT_EQ(0, a.integer);
    ASSERT_EQ(-3, a.fraction);

    a.integer = 0;
    a.fraction = -999999999;
    b.integer = 0;
    b.fraction = -1;
    a += b;
    ASSERT_EQ(-1, a.integer);
    ASSERT_EQ(0, a.fraction);

    a.integer = -8;
    a.fraction = 0;
    b.integer = 2;
    b.fraction = 0;
    a += b;
    ASSERT_EQ(-6, a.integer);
    ASSERT_EQ(0, a.fraction);
}

} // namespace doris

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
