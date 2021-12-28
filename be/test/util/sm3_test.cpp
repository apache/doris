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

#include "util/sm3.h"

#include <gtest/gtest.h>

namespace doris {

class SM3Test : public testing::Test {
public:
    SM3Test() {}
    virtual ~SM3Test() {}
};

TEST_F(SM3Test, empty) {
    SM3Digest digest;
    digest.digest();
    ASSERT_STREQ("1ab21d8355cfa17f8e61194831e81a8f22bec8c728fefb747ed035eb5082aa2b",
                 digest.hex().c_str());
}

TEST_F(SM3Test, normal) {
    {
        SM3Digest digest;
        digest.update("abc", 3);
        digest.digest();
        ASSERT_STREQ("66c7f0f462eeedd9d1f2d46bdc10e4e24167c4875cf2f7a2297da02b8f4ba8e0",
                     digest.hex().c_str());
    }
    {
        SM3Digest digest;
        digest.update("0123456789", 10);
        digest.digest();
        ASSERT_STREQ("09093b72553f5d9d622d6c62f5ffd916ee959679b1bd4d169c3e12aa8328e743",
                     digest.hex().c_str());
    }

    {
        SM3Digest digest;
        digest.update("01234", 5);
        digest.update("56789", 5);
        digest.digest();
        ASSERT_STREQ("09093b72553f5d9d622d6c62f5ffd916ee959679b1bd4d169c3e12aa8328e743",
                     digest.hex().c_str());
    }
}

} // namespace doris

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
