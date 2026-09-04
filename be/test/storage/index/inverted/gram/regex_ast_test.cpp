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
    EXPECT_EQ(parse_dump("\\.\\Qa.b\\E"), "cat('.',cat('a','.','b',),)");
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

// 覆盖修复轮 1 的 Important 1：类内以裸 '\' 结尾（`[a\` / `[a-\`）不得越界读
// 输入之外的内存。用非 NUL 结尾的 string_view（从更长的缓冲区截出前缀，
// 而不是像原型那样总传入 NUL 结尾的 std::string）验证解析器只依赖
// string_view 自身的长度来判断输入结束，不依赖字符串是否 NUL 结尾。
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

// 覆盖修复轮 1 的 Important 2：big_class=true 时 cls 必须为空（RegexNode
// 注释的不变式）。(?i) 下小类展开可能把 ≤4 个原始码点翻倍到 >4 项而退化为
// 大类，此时 cls 也必须被清空；未超过上限时仍应是正常的可枚举小类。
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

// 覆盖 kMaxNestingDepth（64）分组嵌套上限的回归测试：纯左括号（未闭合）与配平的
// 深嵌套括号都必须在超过上限时报错，而不是继续递归直至爆栈或产出畸形树。
TEST(RegexAstTest, NestingDepthCapped) {
    EXPECT_EQ(parse_dump(std::string(70, '(')), "ERR");
    EXPECT_EQ(parse_dump(std::string(70, '(') + "a" + std::string(70, ')')), "ERR");
}

// 覆盖 Ruling R12：`\x` 转义硬化。裸 `\xHH` 形式必须恰好两位十六进制数字，
// 不足两位（到达串尾或遇到非十六进制字符）一律报错，与 RE2 拒绝 `\x4` 的
// 行为对齐；`\x{...}` 形式必须有合法的十六进制内容且闭合，否则同样报错。
TEST(RegexAstTest, HexEscapeHardening) {
    EXPECT_EQ(parse_dump("\\x4"), "ERR");
    EXPECT_EQ(parse_dump("\\x"), "ERR");
    EXPECT_EQ(parse_dump("\\x4g"), "ERR");
    EXPECT_EQ(parse_dump("\\x41"), "cat('A',)");
    EXPECT_EQ(parse_dump("\\x{41}"), "cat('A',)");
    EXPECT_EQ(parse_dump("[\\x4]"), "ERR");
    EXPECT_EQ(parse_dump("\\x{4"), "ERR");
}

} // namespace doris::segment_v2::gram
