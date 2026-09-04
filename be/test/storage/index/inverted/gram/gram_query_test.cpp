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

#include "storage/index/inverted/gram/gram_query.h"

#include <gtest/gtest.h>

namespace doris::segment_v2::gram {

TEST(GramQueryTest, AndOrShortCircuit) {
    EXPECT_TRUE(GramQuery::and_(GramQuery::all(), GramQuery::of_gram("abc")).op ==
                GramQuery::Op::AND);
    EXPECT_TRUE(GramQuery::and_(GramQuery::none(), GramQuery::of_gram("abc")).is_none());
    EXPECT_TRUE(GramQuery::or_(GramQuery::all(), GramQuery::of_gram("abc")).is_all());
    EXPECT_EQ(GramQuery::or_(GramQuery::none(), GramQuery::of_gram("abc")).to_debug_string(),
              "(\"abc\")");
}

TEST(GramQueryTest, FlattenDedupeAbsorb) {
    auto q = GramQuery::and_(GramQuery::and_(GramQuery::of_gram("abc"), GramQuery::of_gram("bcd")),
                             GramQuery::of_gram("abc"));
    EXPECT_EQ(q.op, GramQuery::Op::AND);
    EXPECT_EQ(q.grams.size(), 2U);
    // abc is already in the AND, so the child OR(abc|xyz) is always true and gets absorbed
    auto r = GramQuery::and_(q,
                             GramQuery::or_(GramQuery::of_gram("abc"), GramQuery::of_gram("xyz")));
    EXPECT_EQ(r.leaf_count(), 2U);
    // The gram "x" is already in the OR, so a child AND containing "x" is absorbed (the other
    // direction of absorption): x | (x & y) -> x
    auto t = GramQuery::or_(GramQuery::of_gram("x"),
                            GramQuery::and_(GramQuery::of_gram("x"), GramQuery::of_gram("y")));
    EXPECT_EQ(t.to_debug_string(), "(\"x\")");
    // Inside an OR, a subset AND absorbs its superset: (a&b) | (a&b&c) -> (a&b)
    auto s = GramQuery::or_(
            GramQuery::and_(GramQuery::of_gram("a"), GramQuery::of_gram("b")),
            GramQuery::and_(GramQuery::and_(GramQuery::of_gram("a"), GramQuery::of_gram("b")),
                            GramQuery::of_gram("c")));
    EXPECT_EQ(s.to_debug_string(), "(\"a\" & \"b\")");
}

TEST(GramQueryTest, SerializeRoundTrip) {
    auto q = GramQuery::and_(
            GramQuery::of_gram("or: co"),
            GramQuery::or_(GramQuery::and_(GramQuery::of_gram("Una"), GramQuery::of_gram("abl")),
                           GramQuery::of_gram("Int,ernal)")));
    std::string text = q.serialize();
    GramQuery back;
    ASSERT_TRUE(GramQuery::parse(text, &back).ok());
    EXPECT_EQ(back.serialize(), text);
    EXPECT_EQ(back.to_debug_string(), q.to_debug_string());
    EXPECT_EQ(GramQuery::all().serialize(), "*");
    EXPECT_EQ(GramQuery::none().serialize(), "!");
    GramQuery bad;
    EXPECT_FALSE(GramQuery::parse("&(", &bad).ok());
}

TEST(GramQueryTest, ParseRejectsMalformedInput) {
    // A 65-level nested "&(" prefix must be rejected by the depth cap before the stack overflows.
    std::string deep;
    for (int i = 0; i < 65; i++) {
        deep += "&(";
    }
    GramQuery too_deep;
    EXPECT_FALSE(GramQuery::parse(deep, &too_deep).ok());

    GramQuery bad;
    EXPECT_FALSE(GramQuery::parse("&()", &bad).ok());      // AND with no operand
    EXPECT_FALSE(GramQuery::parse("&(,)", &bad).ok());     // leading comma -> empty item
    EXPECT_FALSE(GramQuery::parse("&(a,,b)", &bad).ok());  // consecutive commas -> empty item
    EXPECT_FALSE(GramQuery::parse("&(YQ==,)", &bad).ok()); // trailing comma -> empty item
    EXPECT_FALSE(GramQuery::parse("&(@@)", &bad).ok());    // invalid base64
    EXPECT_FALSE(GramQuery::parse("*x", &bad).ok());       // trailing input

    // On a parse failure *out must keep its value from before the call, with no half-built tree
    // left behind.
    GramQuery preserved = GramQuery::of_gram("z");
    EXPECT_FALSE(GramQuery::parse("&()", &preserved).ok());
    EXPECT_EQ(preserved.to_debug_string(), "(\"z\")");
}

TEST(GramQueryTest, ParseRebuildsViaCombinators) {
    // "&(*)" has a single operand *, folded through the and_ combinator: and_(all(), all())
    // short-circuits to all() instead of literally producing an AND node holding one ALL
    // sub-query.
    GramQuery all_query;
    ASSERT_TRUE(GramQuery::parse("&(*)", &all_query).ok());
    EXPECT_TRUE(all_query.is_all());

    // The gram "a" (YQ==) is already in the OR, so the child AND(a,b) containing "a" is absorbed
    // while being rebuilt through the or_ combinator, instead of being assembled verbatim into a
    // tree that or_() would never produce.
    GramQuery absorbed;
    ASSERT_TRUE(GramQuery::parse("|(YQ==,&(YQ==,Yg==))", &absorbed).ok());
    EXPECT_EQ(absorbed.to_debug_string(), "(\"a\")");
}

} // namespace doris::segment_v2::gram
