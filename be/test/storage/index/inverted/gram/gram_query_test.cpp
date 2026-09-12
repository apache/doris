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

TEST(GramQueryTest, StructuralKeyIgnoresOperandOrder) {
    auto left = GramQuery::or_(GramQuery::of_gram("a"), GramQuery::of_gram("b"));
    auto right = GramQuery::or_(GramQuery::of_gram("c"), GramQuery::of_gram("d"));
    auto query = GramQuery::and_(left, right);
    EXPECT_EQ(query.structural_key(), GramQuery::and_(right, left).structural_key());
    EXPECT_EQ(left.structural_key(),
              GramQuery::or_(GramQuery::of_gram("b"), GramQuery::of_gram("a")).structural_key());
    auto deduplicated = GramQuery::and_(query, left);
    EXPECT_EQ(deduplicated.leaf_count(), 4U);
    EXPECT_EQ(deduplicated.structural_key(), query.structural_key());
}

TEST(GramQueryTest, StructuralKeyDistinguishesGramBytesFromOperators) {
    EXPECT_NE(GramQuery::of_gram("a,b").structural_key(),
              GramQuery::and_(GramQuery::of_gram("a"), GramQuery::of_gram("b")).structural_key());
    EXPECT_NE(GramQuery::of_gram("*").structural_key(), GramQuery::all().structural_key());
    EXPECT_NE(GramQuery::of_gram("!").structural_key(), GramQuery::none().structural_key());
    EXPECT_NE(GramQuery::all().structural_key(), GramQuery::none().structural_key());
    EXPECT_NE(GramQuery::of_gram(std::string("a\0b", 3)).structural_key(),
              GramQuery::of_gram("a").structural_key());
}

// A combination that crosses the size a compiler would discard is answered with ALL instead of
// being grown, deduplicated and sorted first. Without this, compiling a long high-entropy
// pattern was quadratic in its length and threw the result away: 4,096 characters cost seconds
// of CPU per segment for a query that was already ALL by character 256.
TEST(GramQueryTest, CombiningStopsOnceThePlanIsPastTheSizeAnyoneWouldUse) {
    GramQuery wide = GramQuery::of_gram("aaa");
    for (size_t i = 0; i < GramQuery::kMaxLeaves + 8; ++i) {
        std::string gram = "b";
        gram += static_cast<char>('a' + (i % 26));
        gram += static_cast<char>('a' + (i / 26));
        wide = GramQuery::and_(std::move(wide), GramQuery::of_gram(gram));
        EXPECT_LE(wide.leaf_count(), GramQuery::kMaxLeaves)
                << "an AND grew past the cap at step " << i;
        if (wide.is_all()) {
            break;
        }
    }
    EXPECT_TRUE(wide.is_all()) << "the AND should have collapsed once it crossed the cap";

    GramQuery alternatives = GramQuery::none();
    for (size_t i = 0; i < GramQuery::kMaxLeaves + 8; ++i) {
        std::string gram = "c";
        gram += static_cast<char>('a' + (i % 26));
        gram += static_cast<char>('a' + (i / 26));
        alternatives = GramQuery::or_(std::move(alternatives), GramQuery::of_gram(gram));
        EXPECT_LE(alternatives.leaf_count(), GramQuery::kMaxLeaves)
                << "an OR grew past the cap at step " << i;
        if (alternatives.is_all()) {
            break;
        }
    }
    EXPECT_TRUE(alternatives.is_all()) << "the OR should have collapsed once it crossed the cap";
}

} // namespace doris::segment_v2::gram
