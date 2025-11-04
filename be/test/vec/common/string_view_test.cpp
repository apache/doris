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

TEST_F(StringViewTest, NonInlineEqualityAndCompare) {
    // Create two large (> kInlineSize) equal strings
    std::string base_a = make_bytes(24, 0x41); // length 24
    std::string base_b = base_a;               // identical
    StringView sva(base_a);
    StringView svb(base_b);
    EXPECT_FALSE(sva.isInline());
    EXPECT_FALSE(svb.isInline());
    EXPECT_TRUE(sva == svb);
    EXPECT_EQ(sva.compare(svb), 0);
    EXPECT_TRUE((sva <=> svb) == std::strong_ordering::equal);

    // Same prefix (first 4 bytes) but differ later (exercise memcmp tail path for outline)
    std::string diff1 = base_a;
    std::string diff2 = base_a;
    diff2[15] ^= 0x01; // change one byte after prefix region
    StringView svd1(diff1);
    StringView svd2(diff2);
    EXPECT_NE(svd1.compare(svd2), 0);
    EXPECT_NE(svd1 == svd2, true);

    // Prefix decides ordering: modify first byte so prefix_as_int differs
    std::string p1 = base_a;
    std::string p2 = base_a;
    p2[0] = static_cast<char>(p2[0] + 1);
    StringView svp1(p1), svp2(p2);
    int cmp = svp1.compare(svp2);
    EXPECT_LT(cmp, 0);
    EXPECT_TRUE((svp1 <=> svp2) == std::strong_ordering::less);
}

TEST_F(StringViewTest, StrConversionInlineAndNonInline) {
    std::string inl = "abcd"; // inline
    StringView svi(inl);
    std::string out_inl = svi.str();
    EXPECT_EQ(out_inl.size(), inl.size());
    EXPECT_EQ(out_inl, inl);

    // Non-inline with embedded nulls
    std::string big = make_bytes(20, 0x50); // ensure > 12
    big[5] = '\0';
    big[14] = '\0';
    StringView svb(big);
    EXPECT_FALSE(svb.isInline());
    std::string out_big = svb.str();
    EXPECT_EQ(out_big.size(), big.size());
    EXPECT_EQ(::memcmp(out_big.data(), big.data(), big.size()), 0);
}

TEST_F(StringViewTest, ThreeWayComparisonOrdering) {
    StringView a("abcd");           // inline
    StringView b("abce");           // inline > a
    auto tmp_long = make_bytes(30); // create std::string first (avoid rvalue deleted ctor)
    StringView c(tmp_long);         // non-inline
    StringView d(c);                // identical non-inline
    // a vs b
    EXPECT_TRUE((a <=> b) == std::strong_ordering::less);
    EXPECT_TRUE((b <=> a) == std::strong_ordering::greater);
    // c vs d equal
    EXPECT_TRUE((c <=> d) == std::strong_ordering::equal);
    // a (short) vs c (long)
    auto ord = (a <=> c);
    int cmp = a.compare(c);
    if (cmp < 0) {
        EXPECT_TRUE(ord == std::strong_ordering::less);
    } else if (cmp > 0) {
        EXPECT_TRUE(ord == std::strong_ordering::greater);
    } else {
        EXPECT_TRUE(ord == std::strong_ordering::equal);
    }
}

TEST_F(StringViewTest, DumpHex) {
    // Empty
    StringView empty;
    EXPECT_EQ(empty.dump_hex(), "X''");

    // Inline with known bytes
    const unsigned char bytes_inline[] = {0x00, 0x01, 0x0A, 0x1F, 0x7F};
    StringView svi(reinterpret_cast<const char*>(bytes_inline), sizeof(bytes_inline));
    EXPECT_TRUE(svi.isInline());
    EXPECT_EQ(svi.dump_hex(), "X'00010A1F7F'");

    // Non-inline, length > 12
    std::string big = make_bytes(16, 0x20); // bytes 0x20,0x21,...
    StringView svb(big);
    EXPECT_FALSE(svb.isInline());
    // Build expected
    std::ostringstream oss;
    oss << "X'";
    for (unsigned char c : big) {
        static const char* kHex = "0123456789ABCDEF";
        oss << kHex[c >> 4] << kHex[c & 0x0F];
    }
    oss << "'";
    EXPECT_EQ(svb.dump_hex(), oss.str());
}

} // namespace doris
