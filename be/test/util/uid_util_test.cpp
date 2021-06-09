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

#include "util/uid_util.h"

#include <gtest/gtest.h>

#include <iostream>

namespace doris {
class UidUtilTest : public testing::Test {
public:
    UidUtilTest() {}
    virtual ~UidUtilTest() {}
};

TEST_F(UidUtilTest, UniqueId) {
    {
        UniqueId id = UniqueId::gen_uid();
        std::string hex_str = id.to_string();
        ASSERT_STRNE("0000000000000000-0000000000000000", hex_str.c_str());
    }
    {
        UniqueId id(123456789, 987654321);
        std::string hex_str = id.to_string();
        ASSERT_STREQ("00000000075bcd15-000000003ade68b1", hex_str.c_str());
        UniqueId id2("00000000075bcd15", "000000003ade68b1");
        ASSERT_TRUE(id == id2);
    }
    {
        PUniqueId puid;
        puid.set_hi(12345678987654321);
        puid.set_lo(98765432123456789);
        UniqueId id(puid);
        std::string hex_str = id.to_string();
        ASSERT_STREQ("002bdc546291f4b1-015ee2a321ce7d15", hex_str.c_str());
        UniqueId id2("002bdc546291f4b1", "015ee2a321ce7d15");
        ASSERT_TRUE(id == id2);
    }
    {
        TUniqueId tuid;
        tuid.__set_hi(12345678987654321);
        tuid.__set_lo(98765432123456789);
        UniqueId id(tuid);
        std::string hex_str = id.to_string();
        ASSERT_STREQ("002bdc546291f4b1-015ee2a321ce7d15", hex_str.c_str());
        UniqueId id2("002bdc546291f4b1", "015ee2a321ce7d15");
        ASSERT_TRUE(id == id2);
    }
    {
        TUniqueId tuid;
        tuid.__set_hi(12345678987654321);
        tuid.__set_lo(98765432123456789);
        UniqueId id(tuid);
        std::stringstream ss;
        ss << id;
        ASSERT_STREQ("002bdc546291f4b1-015ee2a321ce7d15", ss.str().c_str());
        UniqueId id2("002bdc546291f4b1", "015ee2a321ce7d15");
        ASSERT_TRUE(id == id2);
    }

    {
        TUniqueId tuid;
        tuid.__set_hi(12345678987654321);
        tuid.__set_lo(98765432123456789);
        UniqueId id(tuid);
        std::stringstream ss;
        ss << id;
        ASSERT_STREQ("002bdc546291f4b1-015ee2a321ce7d15", ss.str().c_str());
        UniqueId id2("002bdc546291f4b1", "015ee2a321ce7d15");
        ASSERT_TRUE(id == id2);
        ASSERT_FALSE(id != id2);
        UniqueId id3("002bdc546291f4b1", "015ee2a321ce7d16");
        ASSERT_TRUE(id != id3);
        ASSERT_FALSE(id == id3);
    }
}

TEST_F(UidUtilTest, Hash) {
    std::hash<UniqueId> hasher;
    UniqueId uid(1, 2);
    {
        TUniqueId tuid;
        tuid.__set_hi(1);
        tuid.__set_lo(2);

        ASSERT_EQ(hasher(uid), hasher(tuid));
        ASSERT_TRUE(uid == UniqueId(tuid));
    }
    {
        TUniqueId tuid;
        tuid.__set_hi(1);
        tuid.__set_lo(1);

        ASSERT_NE(hasher(uid), hasher(tuid));
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}
