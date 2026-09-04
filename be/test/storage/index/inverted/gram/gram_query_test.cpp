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
    // AND 内已有 abc，则子 OR(abc|xyz) 恒真，被吸收
    auto r = GramQuery::and_(q,
                             GramQuery::or_(GramQuery::of_gram("abc"), GramQuery::of_gram("xyz")));
    EXPECT_EQ(r.leaf_count(), 2U);
    // OR 内已有 gram "x"，则含 "x" 的子 AND 被吸收（吸收律的另一个方向）：x | (x & y) → x
    auto t = GramQuery::or_(GramQuery::of_gram("x"),
                            GramQuery::and_(GramQuery::of_gram("x"), GramQuery::of_gram("y")));
    EXPECT_EQ(t.to_debug_string(), "(\"x\")");
    // OR 内 AND 子集吸收超集：(a&b) | (a&b&c) → (a&b)
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
    // 65 层嵌套的 "&(" 前缀必须在爆栈之前被深度上限拒绝。
    std::string deep;
    for (int i = 0; i < 65; i++) {
        deep += "&(";
    }
    GramQuery too_deep;
    EXPECT_FALSE(GramQuery::parse(deep, &too_deep).ok());

    GramQuery bad;
    EXPECT_FALSE(GramQuery::parse("&()", &bad).ok());      // 零操作数的 AND
    EXPECT_FALSE(GramQuery::parse("&(,)", &bad).ok());     // 开头逗号 → 空 item
    EXPECT_FALSE(GramQuery::parse("&(a,,b)", &bad).ok());  // 连续逗号 → 空 item
    EXPECT_FALSE(GramQuery::parse("&(YQ==,)", &bad).ok()); // 结尾逗号 → 空 item
    EXPECT_FALSE(GramQuery::parse("&(@@)", &bad).ok());    // 非法 base64
    EXPECT_FALSE(GramQuery::parse("*x", &bad).ok());       // 尾随输入

    // 解析失败时 *out 必须保持调用前的值，不能残留半成品树。
    GramQuery preserved = GramQuery::of_gram("z");
    EXPECT_FALSE(GramQuery::parse("&()", &preserved).ok());
    EXPECT_EQ(preserved.to_debug_string(), "(\"z\")");
}

TEST(GramQueryTest, ParseRebuildsViaCombinators) {
    // "&(*)" 只有一个操作数 *，按 and_ 组合子折叠：and_(all(), all()) 短路为 all()，
    // 而不是原样产出一个持有单个 ALL 子查询的 AND 节点。
    GramQuery all_query;
    ASSERT_TRUE(GramQuery::parse("&(*)", &all_query).ok());
    EXPECT_TRUE(all_query.is_all());

    // OR 内已有 gram "a"（YQ==），含 "a" 的子 AND(a,b) 按 or_ 组合子重建时被吸收，
    // 而不是被原样拼装成一棵 or_() 本不会产出的树。
    GramQuery absorbed;
    ASSERT_TRUE(GramQuery::parse("|(YQ==,&(YQ==,Yg==))", &absorbed).ok());
    EXPECT_EQ(absorbed.to_debug_string(), "(\"a\")");
}

} // namespace doris::segment_v2::gram
