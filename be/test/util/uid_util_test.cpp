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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"

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
        EXPECT_STRNE("0000000000000000-0000000000000000", hex_str.c_str());
    }
    {
        UniqueId id(123456789, 987654321);
        std::string hex_str = id.to_string();
        EXPECT_STREQ("00000000075bcd15-000000003ade68b1", hex_str.c_str());
        UniqueId id2("00000000075bcd15", "000000003ade68b1");
        EXPECT_TRUE(id == id2);
    }
    {
        PUniqueId puid;
        puid.set_hi(12345678987654321);
        puid.set_lo(98765432123456789);
        UniqueId id(puid);
        std::string hex_str = id.to_string();
        EXPECT_STREQ("002bdc546291f4b1-015ee2a321ce7d15", hex_str.c_str());
        UniqueId id2("002bdc546291f4b1", "015ee2a321ce7d15");
        EXPECT_TRUE(id == id2);
    }
    {
        TUniqueId tuid;
        tuid.__set_hi(12345678987654321);
        tuid.__set_lo(98765432123456789);
        UniqueId id(tuid);
        std::string hex_str = id.to_string();
        EXPECT_STREQ("002bdc546291f4b1-015ee2a321ce7d15", hex_str.c_str());
        UniqueId id2("002bdc546291f4b1", "015ee2a321ce7d15");
        EXPECT_TRUE(id == id2);
    }
    {
        TUniqueId tuid;
        tuid.__set_hi(12345678987654321);
        tuid.__set_lo(98765432123456789);
        UniqueId id(tuid);
        std::stringstream ss;
        ss << id;
        EXPECT_STREQ("002bdc546291f4b1-015ee2a321ce7d15", ss.str().c_str());
        UniqueId id2("002bdc546291f4b1", "015ee2a321ce7d15");
        EXPECT_TRUE(id == id2);
    }

    {
        TUniqueId tuid;
        tuid.__set_hi(12345678987654321);
        tuid.__set_lo(98765432123456789);
        UniqueId id(tuid);
        std::stringstream ss;
        ss << id;
        EXPECT_STREQ("002bdc546291f4b1-015ee2a321ce7d15", ss.str().c_str());
        UniqueId id2("002bdc546291f4b1", "015ee2a321ce7d15");
        EXPECT_TRUE(id == id2);
        EXPECT_FALSE(id != id2);
        UniqueId id3("002bdc546291f4b1", "015ee2a321ce7d16");
        EXPECT_TRUE(id != id3);
        EXPECT_FALSE(id == id3);
    }
}

TEST_F(UidUtilTest, Hash) {
    std::hash<UniqueId> hasher;
    UniqueId uid(1, 2);
    {
        TUniqueId tuid;
        tuid.__set_hi(1);
        tuid.__set_lo(2);

        EXPECT_EQ(hasher(uid), hasher(tuid));
        EXPECT_TRUE(uid == UniqueId(tuid));
    }
    {
        TUniqueId tuid;
        tuid.__set_hi(1);
        tuid.__set_lo(1);

        EXPECT_NE(hasher(uid), hasher(tuid));
    }
}

TEST_F(UidUtilTest, ValidHexId) {
    std::string s = "abcd-1234";
    TUniqueId id {};
    bool ok = parse_id(s, &id);
    EXPECT_TRUE(ok);
    EXPECT_EQ(id.hi, 0xabcdu);
    EXPECT_EQ(id.lo, 0x1234u);
    EXPECT_EQ(s, "abcd:1234");
}

TEST_F(UidUtilTest, ValidLargeHexId) {
    std::string s = "deadbeef-100";
    TUniqueId id {};
    bool ok = parse_id(s, &id);
    EXPECT_TRUE(ok);
    EXPECT_EQ(id.hi, 0xdeadbeefu);
    EXPECT_EQ(id.lo, 0x100u);
    EXPECT_EQ(s, "deadbeef:100");
}

TEST_F(UidUtilTest, InvalidHexHiPart) {
    std::string s = "xyz-123";
    TUniqueId id {};
    bool ok = parse_id(s, &id);
    EXPECT_FALSE(ok);
}

TEST_F(UidUtilTest, InvalidHexLoPart) {
    std::string s = "1234-gggg";
    TUniqueId id {};
    bool ok = parse_id(s, &id);
    EXPECT_FALSE(ok);
}

TEST_F(UidUtilTest, MissingDash) {
    std::string s = "12345678";
    TUniqueId id {};
    bool ok = parse_id(s, &id);
    EXPECT_FALSE(ok);
}

} // namespace doris
