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

#include "olap/field_info.h"

#include <gtest/gtest.h>

namespace doris {

TEST(FieldInfoTest, HandleNoneZeroInput) {
    int64_t a_integer = 9223372036854775806L;
    int a_fraction = 1;
    decimal12_t a(a_integer, a_fraction);
    decimal12_t b(1, 0);
    a += b;
    ASSERT_EQ(a_integer + 1, a.integer);
    ASSERT_EQ(a_fraction, a.fraction);
    
    a.integer = -9223372036854775807L;
    a.fraction = -1;
    b.integer = 0;
    a += b;
    ASSERT_EQ(-9223372036854775807L, a.integer);
    ASSERT_EQ(-1, a.fraction);
}

} // namespace doris

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv); 
    return RUN_ALL_TESTS();
}
