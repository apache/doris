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

#include "exec/common/variant_util.h"

#include <string>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_variant_v2.h"
#include "gtest/gtest.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::variant_util {

static TabletSchema make_variant_schema() {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* column = schema_pb.add_column();
    column->set_unique_id(1);
    column->set_name("v");
    column->set_type("VARIANT");
    column->set_is_key(false);
    column->set_is_nullable(false);

    TabletSchema tablet_schema;
    tablet_schema.init_from_pb(schema_pb);
    return tablet_schema;
}

TEST(VariantUtilTest, ParseVariantColumnsKeepsV2PhysicalColumn) {
    auto variant = ColumnVariantV2::create();
    const auto* expected_column = variant.get();
    Block block;
    block.insert({std::move(variant), std::make_shared<DataTypeVariantV2>(), "v"});

    Status st = parse_and_materialize_variant_columns(block, make_variant_schema(), {0});
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(block.get_by_position(0).column.get(), expected_column);
}

TEST(VariantUtilTest, ParseVariantColumnsRejectsNonV2PhysicalColumn) {
    Block block;
    block.insert({ColumnString::create(), std::make_shared<DataTypeVariantV2>(), "v"});

    Status st = parse_and_materialize_variant_columns(block, make_variant_schema(), {0});
    ASSERT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("ColumnVariantV2"), std::string::npos);
}

TEST(VariantUtilTest, GlobToRegex) {
    struct Case {
        std::string glob;
        std::string expected_regex;
    };
    const std::vector<Case> cases = {
            {"*", "^.*$"},
            {"?", "^.$"},
            {"a?b", "^a.b$"},
            {"a*b", "^a.*b$"},
            {"a**b", "^a.*.*b$"},
            {"a??b", "^a..b$"},
            {"?*", "^..*$"},
            {"*?", "^.*.$"},
            {"a.b", "^a\\.b$"},
            {"a+b", "^a\\+b$"},
            {"a{b}", "^a\\{b\\}$"},
            {R"(a\*b)", R"(^a\*b$)"},
            {"a\\?b", "^a\\?b$"},
            {"a\\[b", "^a\\[b$"},
            {"abc\\", "^abc\\\\$"},
            {"a|b", "^a\\|b$"},
            {"a(b)c", "^a\\(b\\)c$"},
            {"a^b", "^a\\^b$"},
            {"a$b", "^a\\$b$"},
            {"int_[0-9]", "^int_[0-9]$"},
            {"int_[!0-9]", "^int_[^0-9]$"},
            {"int_[^0-9]", "^int_[^0-9]$"},
            {"a[\\-]b", "^a[-]b$"},
            {"a[b-d]e", "^a[b-d]e$"},
            {"a[\\]]b", "^a[]]b$"},
            {"a[\\!]b", "^a[!]b$"},
            {"", "^$"},
            {"a[[]b", "^a[[]b$"},
            {"a[]b", "^a[]b$"},
            {"[]", "^[]$"},
            {"[!]", "^[^]$"},
            {"[^]", "^[^]$"},
            {"\\", "^\\\\$"},
            {"\\*", "^\\*$"},
            {"a\\*b", "^a\\*b$"},
            {"a[!\\]]b", "^a[^]]b$"},
    };

    for (const auto& test_case : cases) {
        std::string regex;
        Status st = glob_to_regex(test_case.glob, &regex);
        EXPECT_TRUE(st.ok()) << st.to_string() << " pattern=" << test_case.glob;
        EXPECT_EQ(regex, test_case.expected_regex) << "pattern=" << test_case.glob;
    }

    std::string regex;
    EXPECT_FALSE(glob_to_regex("int_[0-9", &regex).ok());
    EXPECT_FALSE(glob_to_regex("a[\\]b", &regex).ok());
}

TEST(VariantUtilTest, GlobMatchRe2) {
    struct Case {
        std::string glob;
        std::string candidate;
        bool expected;
    };
    const std::vector<Case> cases = {
            {"*", "", true},
            {"*", "abc", true},
            {"?", "a", true},
            {"?", "", false},
            {"a?b", "acb", true},
            {"a?b", "ab", false},
            {"a*b", "ab", true},
            {"a*b", "axxxb", true},
            {"a**b", "ab", true},
            {"a**b", "axxxb", true},
            {"?*", "", false},
            {"?*", "a", true},
            {"*?", "", false},
            {"*?", "a", true},
            {"a*b", "a/b", true},
            {"a.b", "a.b", true},
            {"a.b", "acb", false},
            {"a+b", "a+b", true},
            {"a{b}", "a{b}", true},
            {"a|b", "a|b", true},
            {"a|b", "ab", false},
            {"a(b)c", "a(b)c", true},
            {"a(b)c", "abc", false},
            {"a^b", "a^b", true},
            {"a^b", "ab", false},
            {"a$b", "a$b", true},
            {"a$b", "ab", false},
            {"a[b-d]e", "ace", true},
            {"a[b-d]e", "aee", false},
            {"a[\\]]b", "a]b", true},
            {"a[\\]]b", "a[b", false},
            {"a[\\!]b", "a!b", true},
            {"a[\\!]b", "a]b", false},
            {"[]", "a", false},
            {"[!]", "]", false},
            {"\\", "\\", true},
            {"\\*", "\\abc", false},
            {"a[!\\]]b", "aXb", true},
            {"a[!\\]]b", "a]b", false},
            {"a[]b", "aXb", false},
            {"a[[]b", "a[b", true},
            {R"(a\*b)", "a*b", true},
            {R"(a\?b)", "a?b", true},
            {R"(a\[b)", "a[b", true},
            {R"(abc\)", R"(abc\)", true},
            {"int_[0-9]", "int_1", true},
            {"int_[0-9]", "int_a", false},
            {"int_[!0-9]", "int_a", true},
            {"int_[!0-9]", "int_1", false},
            {"int_[^0-9]", "int_b", true},
            {"int_[^0-9]", "int_2", false},
            {R"(a[\-]b)", "a-b", true},
            {"", "", true},
            {"", "a", false},
    };

    for (const auto& test_case : cases) {
        bool matched = glob_match_re2(test_case.glob, test_case.candidate);
        EXPECT_EQ(matched, test_case.expected)
                << "pattern=" << test_case.glob << " candidate=" << test_case.candidate;
    }

    EXPECT_FALSE(glob_match_re2("int_[0-9", "int_1"));
    EXPECT_FALSE(glob_match_re2("a[\\]b", "a]b"));
}

} // namespace doris::variant_util
