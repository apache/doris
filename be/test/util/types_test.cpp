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

#include "util/types.h"

#include <gtest/gtest.h>

#include "runtime/large_int_value.h"

namespace doris {

class TypesTest : public ::testing::Test {
protected:
    TypesTest() {}
    virtual ~TypesTest() {}
};

TEST_F(TypesTest, packed_int128) {
    // check align
    ASSERT_EQ(1, alignof(PackedInt128));

    // check assign
    __int128 test_value = 123456789987654321;
    test_value *= 1000000000000000000UL;
    test_value += 123456789987654321UL;
    char buf[30];
    *reinterpret_cast<PackedInt128*>(buf + 1) = test_value;
    ASSERT_EQ(reinterpret_cast<PackedInt128*>(buf + 1)->value, test_value);
    LOG(INFO) << reinterpret_cast<PackedInt128*>(buf + 1)->value;
    {
        char buf2[64];
        *reinterpret_cast<PackedInt128*>(buf2 + 7) = *reinterpret_cast<PackedInt128*>(buf + 1);
        reinterpret_cast<PackedInt128*>(buf2 + 7)->value += 100;
        ASSERT_EQ(reinterpret_cast<PackedInt128*>(buf2 + 7)->value, test_value + 100);
        LOG(INFO) << reinterpret_cast<PackedInt128*>(buf2 + 7)->value;
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
