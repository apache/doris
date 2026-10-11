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

#include "storage/index/inverted/gram/regex_ast.h"

#include <gtest/gtest.h>

namespace doris::segment_v2::gram {

static std::string dump(const RegexNode* n) {
    using T = RegexNode::Type;
    switch (n->type) {
    case T::EMPTY:
        return "e";
    case T::LIT:
        return "'" + n->lit + "'";
    case T::CLASS: {
        if (n->big_class) {
            return "[big]";
        }
        std::string s = "[";
        for (const auto& c : n->cls) {
            s += c;
        }
        return s + "]";
    }
    case T::ANY:
        return ".";
    case T::CAT: {
        std::string s = "cat(";
        for (const auto& k : n->kids) {
            s += dump(k.get()) + ",";
        }
        return s + ")";
    }
    case T::ALT: {
        std::string s = "alt(";
        for (const auto& k : n->kids) {
            s += dump(k.get()) + ",";
        }
        return s + ")";
    }
    case T::STAR:
        return "star(" + dump(n->kids[0].get()) + ")";
    case T::PLUS:
        return "plus(" + dump(n->kids[0].get()) + ")";
    case T::QUEST:
        return "quest(" + dump(n->kids[0].get()) + ")";
    case T::REPEAT:
        return "rep(" + dump(n->kids[0].get()) + "," + std::to_string(n->rmin) + "," +
               std::to_string(n->rmax) + ")";
    }
    return "?";
}

static std::string parse_dump(const std::string& re) {
    std::unique_ptr<RegexNode> root;
    bool icase = false;
    Status st = parse_regex(re, &root, &icase);
    if (!st.ok()) {
        return "ERR";
    }
    return dump(root.get());
}

TEST(RegexAstTest, Basics) {
    EXPECT_EQ(parse_dump("abc"), "cat('a','b','c',)");
    EXPECT_EQ(parse_dump("a|bc"), "alt(cat('a',),cat('b','c',),)");
    EXPECT_EQ(parse_dump("a.*b"), "cat('a',star(.),'b',)");
    EXPECT_EQ(parse_dump("(ab)+c?"), "cat(plus(cat('a','b',)),quest('c'),)");
    EXPECT_EQ(parse_dump("x{2,3}"), "cat(rep('x',2,3),)");
    EXPECT_EQ(parse_dump("^\\d{3}-\\d{4}$"), "cat(e,rep([big],3,3),'-',rep([big],4,4),e,)");
}

TEST(RegexAstTest, ClassesAndEscapes) {
    EXPECT_EQ(parse_dump("[ab]"), "cat([ab],)");
    EXPECT_EQ(parse_dump("[a-z]"), "cat([big],)");
    EXPECT_EQ(parse_dump("[^a]"), "cat([big],)");
    EXPECT_EQ(parse_dump("\\.\\Qa.b\\E"), "cat('.','a','.','b',)");
    EXPECT_EQ(parse_dump("\\x41"), "cat('A',)");
    EXPECT_EQ(parse_dump("手机"), "cat('手','机',)");
}

TEST(RegexAstTest, FlagsAndErrors) {
    std::unique_ptr<RegexNode> root;
    bool icase = false;
    ASSERT_TRUE(parse_regex("(?i)ab", &root, &icase).ok());
    EXPECT_TRUE(icase);
    EXPECT_EQ(dump(root.get()), "cat([Aa],[Bb],)");
    EXPECT_EQ(parse_dump("(ab"), "ERR");
    EXPECT_EQ(parse_dump("*a"), "ERR");
    EXPECT_EQ(parse_dump("[ab"), "ERR");
    EXPECT_EQ(parse_dump("a\\"), "ERR");
}

// Covers Important 1 of fix round 1: a bare '\' at the end of a class (`[a\` / `[a-\`) must not
// read past the end of the input. A non-NUL-terminated string_view (a prefix cut out of a longer
// buffer, rather than the NUL-terminated std::string the prototype always passed) verifies that
// the parser relies solely on the string_view's own length to detect the end of input, not on the
// string being NUL-terminated.
TEST(RegexAstTest, ClassTrailingBackslashAtEof) {
    std::string buf1 = "[a\\XYZ";
    std::string_view view1 = std::string_view(buf1).substr(0, 3); // "[a\"
    std::unique_ptr<RegexNode> root1;
    bool icase1 = false;
    EXPECT_FALSE(parse_regex(view1, &root1, &icase1).ok());

    std::string buf2 = "[a-\\XYZ";
    std::string_view view2 = std::string_view(buf2).substr(0, 4); // "[a-\"
    std::unique_ptr<RegexNode> root2;
    bool icase2 = false;
    EXPECT_FALSE(parse_regex(view2, &root2, &icase2).ok());
}

// Covers Important 2 of fix round 1: cls must be empty whenever big_class=true (the invariant in
// the RegexNode comment). Under (?i) the small-class expansion can double <= 4 original code
// points to more than 4 items and degrade to a large class, and cls must then be cleared too;
// below the cap it must still be an ordinary enumerable small class.
TEST(RegexAstTest, IcaseClassBigClassInvariant) {
    std::unique_ptr<RegexNode> root;
    bool icase = false;
    ASSERT_TRUE(parse_regex("(?i)[abc]", &root, &icase).ok());
    ASSERT_EQ(root->kids.size(), 1U);
    const RegexNode* cls_node = root->kids[0].get();
    EXPECT_EQ(cls_node->type, RegexNode::Type::CLASS);
    EXPECT_TRUE(cls_node->big_class);
    EXPECT_TRUE(cls_node->cls.empty());

    std::unique_ptr<RegexNode> root2;
    bool icase2 = false;
    ASSERT_TRUE(parse_regex("(?i)[ab]", &root2, &icase2).ok());
    ASSERT_EQ(root2->kids.size(), 1U);
    const RegexNode* cls_node2 = root2->kids[0].get();
    EXPECT_EQ(cls_node2->type, RegexNode::Type::CLASS);
    EXPECT_FALSE(cls_node2->big_class);
    EXPECT_EQ(cls_node2->cls, (std::vector<std::string> {"A", "B", "a", "b"}));
}

// Regression test for the kMaxNestingDepth (64) group nesting cap: both a run of bare (unclosed)
// opening parentheses and a balanced deep nesting must error out past the cap, rather than keep
// recursing until the stack overflows or a malformed tree is produced.
TEST(RegexAstTest, NestingDepthCapped) {
    EXPECT_EQ(parse_dump(std::string(70, '(')), "ERR");
    EXPECT_EQ(parse_dump(std::string(70, '(') + "a" + std::string(70, ')')), "ERR");
}

// Covers Ruling R12: `\x` escape hardening. The bare `\xHH` form must have exactly two hex
// digits, and fewer than two (end of string, or a non-hex character) is always an error, matching
// RE2's rejection of `\x4`; the `\x{...}` form must have valid hex content and be closed, or it
// is an error as well.
TEST(RegexAstTest, HexEscapeHardening) {
    EXPECT_EQ(parse_dump("\\x4"), "ERR");
    EXPECT_EQ(parse_dump("\\x"), "ERR");
    EXPECT_EQ(parse_dump("\\x4g"), "ERR");
    EXPECT_EQ(parse_dump("\\x41"), "cat('A',)");
    EXPECT_EQ(parse_dump("\\x{41}"), "cat('A',)");
    EXPECT_EQ(parse_dump("[\\x4]"), "ERR");
    EXPECT_EQ(parse_dump("\\x{4"), "ERR");
}

// Boost skips whitespace around repeat bounds, while Hyperscan and RE2 read such braces as literal
// text, so a spaced repeat has no reading the gram query can rely on.
TEST(RegexAstTest, BoundedRepeatWithWhitespaceIsRejected) {
    for (const char* re :
         {"a{ 3 }", "a{3 }", "a{ 3}", "a{\t3}", "a{3, 4}", "a{3 ,4}", "a{ 3 , 4 }", "a{3, }"}) {
        EXPECT_EQ(parse_dump(re), "ERR") << re;
    }
    // Braces that no engine reads as a repeat stay literal text.
    EXPECT_EQ(parse_dump("a{ x}"), "cat('a','{',' ','x','}',)");
    EXPECT_EQ(parse_dump("a{3 x}"), "cat('a','{','3',' ','x','}',)");
}

// RE2 reads a count of ten or more digits, or one with a leading zero, as literal text, while
// Hyperscan and Boost read a number.
TEST(RegexAstTest, RepeatCountsSomeEngineReadsAsTextAreRejected) {
    for (const char* re : {"a{1000000000}", "a{2147483648}", "a{1,4294967297}", "a{0,4294967296}",
                           "a{01}", "a{1,02}", "a{00}"}) {
        EXPECT_EQ(parse_dump(re), "ERR") << re;
    }
}

// Braces every engine reads the same way keep their meaning.
TEST(RegexAstTest, RepeatCountsEveryEngineReadsAlikeKeepTheirShape) {
    EXPECT_EQ(parse_dump("a{0}"), "cat(rep('a',0,0),)");
    EXPECT_EQ(parse_dump("a{0,2}"), "cat(rep('a',0,2),)");
    EXPECT_EQ(parse_dump("a{3,}"), "cat(rep('a',3,-1),)");
    EXPECT_EQ(parse_dump("a{2,1000}"), "cat(rep('a',2,1000),)");
    EXPECT_EQ(parse_dump("a{999999999}"), "cat(rep('a',999999999,999999999),)");
    EXPECT_EQ(parse_dump("a{,3}"), "cat('a','{',',','3','}',)");
}

// Boost reads [.x.] and [=x=] as one collating or equivalence element and closes the class after
// it, while RE2 closes the class at the first ']'.
TEST(RegexAstTest, CollatingAndEquivalenceElementsAreRejected) {
    for (const char* re : {"[[.a.]]x", "[[=a=]]x", "[b[.a.]]x", "[^[=a=]]x", "x[[.space.]]"}) {
        EXPECT_EQ(parse_dump(re), "ERR") << re;
    }
    EXPECT_EQ(parse_dump("[[:alpha:]]x"), "cat([big],'x',)");
    EXPECT_EQ(parse_dump("[[a]x"), "cat([[a],'x',)");
}

// At top level Boost reads \< \> \` \' as word and buffer anchors, while Hyperscan and RE2 read
// the escaped character. Inside a class every engine reads the character.
TEST(RegexAstTest, BoostAnchorEscapesAreRejected) {
    for (const char* re : {"\\<x", "x\\>", "\\`x", "x\\'"}) {
        EXPECT_EQ(parse_dump(re), "ERR") << re;
    }
    EXPECT_EQ(parse_dump("[\\<\\>]x"), "cat([<>],'x',)");
}

// Boost reads a sign directly in front of a repeat count, while Hyperscan and RE2 read the braces
// as literal text, so a signed count has no reading the gram query can rely on.
TEST(RegexAstTest, SignedRepeatCountsAreRejected) {
    for (const char* re : {"a{+3}", "a{ +3 }", "a{+3 }", "a{+03}", "a{-0}", "a{-3}", "a{+1,}",
                           "a{+1,4}", "a{-1,4}", "a{1,+4}", "a{1,-4}", "a{1, -4}"}) {
        EXPECT_EQ(parse_dump(re), "ERR") << re;
    }
    // A sign that no digit follows makes no count for any engine, so the braces stay literal.
    EXPECT_EQ(parse_dump("a{+ 3}"), "cat('a',plus('{'),' ','3','}',)");
    EXPECT_EQ(parse_dump("a{3,+}"), "cat('a','{','3',plus(','),'}',)");
    EXPECT_EQ(parse_dump("a{++3}"), "cat('a',plus(plus('{')),'3','}',)");
}

} // namespace doris::segment_v2::gram
