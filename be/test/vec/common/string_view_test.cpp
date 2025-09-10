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

#include "vec/common/string_view.h"

#include <gtest/gtest.h>

#include <cstring>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "vec/common/string_ref.h"

namespace doris {

class StringViewTest : public ::testing::Test {};

static std::string make_bytes(size_t n, uint8_t seed = 0x30) {
    std::string s;
    s.resize(n);
    for (size_t i = 0; i < n; ++i) {
        s[i] = static_cast<char>(seed + i);
    }
    if (n >= 6) {
        s[n / 3] = '\0';
        s[(2 * n) / 3] = '\0';
    }
    return s;
}

TEST_F(StringViewTest, EmptyAndBasics) {
    StringView sv;
    EXPECT_TRUE(sv.empty());
    EXPECT_EQ(sv.size(), 0U);
    EXPECT_TRUE(sv.isInline());

    StringView a("abc");
    EXPECT_FALSE(a.empty());
    EXPECT_EQ(a.size(), 3U);
    EXPECT_TRUE(a.isInline());
    EXPECT_EQ(std::string(a), std::string("abc"));

    std::string s12(12, 'x');
    StringView b(s12);
    EXPECT_TRUE(b.isInline());
    EXPECT_EQ(b.size(), 12U);

    std::string s13(13, 'y');
    StringView c(s13);
    EXPECT_FALSE(c.isInline());
    EXPECT_EQ(c.size(), 13U);
}

TEST_F(StringViewTest, DataPointerInlineVsOutline) {
    std::string small = "hello";
    StringView si(small);
    EXPECT_TRUE(si.isInline());
    EXPECT_NE(si.data(), small.data()); // inline stores its own bytes

    std::string big = make_bytes(16);
    StringView so(big);
    EXPECT_FALSE(so.isInline());
    EXPECT_EQ(so.data(), big.data()); // outline holds external pointer
}

TEST_F(StringViewTest, EqualityAndCompare) {
    StringView a("abcd");
    StringView b("abcd");
    EXPECT_TRUE(a == b);
    EXPECT_EQ(a.compare(b), 0);

    StringView c("abce");
    EXPECT_FALSE(a == c);
    EXPECT_LT(a.compare(c), 0); // 'd' < 'e'

    // different length, same prefix
    StringView d("ab");
    StringView e("abc");
    EXPECT_LT(d.compare(e), 0);
    EXPECT_GT(e.compare(d), 0);

    // same first 4 bytes, differ later (exercise non-prefix compare path)
    std::string s1 = std::string("abcd") + std::string("XXXX");
    std::string s2 = std::string("abcd") + std::string("YYYY");
    StringView x(s1);
    StringView y(s2);
    EXPECT_NE(x.compare(y), 0);
}

TEST_F(StringViewTest, EmbeddedNulls) {
    std::string raw = std::string("ab\0cd\0ef", 8);
    StringView sv(raw);
    EXPECT_EQ(sv.size(), 8U);
    // string conversion preserves bytes
    std::string s = static_cast<std::string>(sv);
    EXPECT_EQ(s.size(), 8U);
    EXPECT_EQ(::memcmp(s.data(), raw.data(), 8), 0);

    // equality with same content containing nulls
    StringView sv2(raw);
    EXPECT_TRUE(sv == sv2);
    EXPECT_EQ(sv.compare(sv2), 0);
}

TEST_F(StringViewTest, ConversionsAndIteration) {
    std::string src = make_bytes(10);
    const StringView sv(src);

    // to_string_ref
    auto ref = sv.to_string_ref();
    EXPECT_EQ(ref.size, sv.size());
    EXPECT_EQ(::memcmp(ref.data, sv.data(), sv.size()), 0);

    // to std::string_view (lvalue only)
    std::string_view v = static_cast<std::string_view>(sv);
    EXPECT_EQ(v.size(), sv.size());
    EXPECT_EQ(::memcmp(v.data(), sv.data(), sv.size()), 0);

    // begin()/end()
    std::string via_iter(sv.begin(), sv.end());
    EXPECT_EQ(via_iter.size(), sv.size());
    EXPECT_EQ(::memcmp(via_iter.data(), sv.data(), sv.size()), 0);
}

TEST_F(StringViewTest, OstreamWrite) {
    std::string raw = std::string("12\0\0", 4);
    StringView sv(raw);
    std::ostringstream oss;
    oss << sv; // write() respects size; embedded nulls are preserved
    std::string out = oss.str();
    EXPECT_EQ(out.size(), raw.size());
    EXPECT_EQ(::memcmp(out.data(), raw.data(), raw.size()), 0);
}

} // namespace doris
