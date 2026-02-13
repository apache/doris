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

#include "testutil/variant_util.h"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "gen_cpp/olap_file.pb.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "olap/tablet_schema.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_variant.h"
#include "vec/common/variant_util.h"
#include "vec/core/block.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_variant.h"

namespace doris::vectorized::variant_util {

static vectorized::ColumnString::MutablePtr _make_json_column(
        const std::vector<std::string_view>& rows) {
    auto col = vectorized::ColumnString::create();
    for (const auto& s : rows) {
        col->insert_data(s.data(), s.size());
    }
    return col;
}

static uint64_t _splitmix64(uint64_t x) {
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

static constexpr size_t kPerfNestedDim = 10;
static constexpr size_t kPerfNestedLeafCount =
        kPerfNestedDim * kPerfNestedDim * kPerfNestedDim * kPerfNestedDim;

static std::string _path_of_leaf_id(size_t leaf_id) {
    const size_t g = leaf_id / (kPerfNestedDim * kPerfNestedDim * kPerfNestedDim);
    const size_t s = (leaf_id / (kPerfNestedDim * kPerfNestedDim)) % kPerfNestedDim;
    const size_t t = (leaf_id / kPerfNestedDim) % kPerfNestedDim;
    const size_t k = leaf_id % kPerfNestedDim;
    std::string path;
    path.reserve(16);
    path += "g";
    path.push_back(static_cast<char>('0' + g));
    path += ".s";
    path.push_back(static_cast<char>('0' + s));
    path += ".t";
    path.push_back(static_cast<char>('0' + t));
    path += ".k";
    path.push_back(static_cast<char>('0' + k));
    return path;
}

static std::string _build_nested_json_row(size_t row_idx, uint64_t seed) {
    std::string root;
    root.reserve(220000);
    root.push_back('{');
    bool first_g = true;
    for (size_t g = 0; g < kPerfNestedDim; ++g) {
        std::string g_obj;
        g_obj.push_back('{');
        bool first_s = true;
        for (size_t s = 0; s < kPerfNestedDim; ++s) {
            std::string s_obj;
            s_obj.push_back('{');
            bool first_t = true;
            for (size_t t = 0; t < kPerfNestedDim; ++t) {
                std::string t_obj;
                t_obj.push_back('{');
                bool first_k = true;
                for (size_t k = 0; k < kPerfNestedDim; ++k) {
                    const size_t leaf_id =
                            ((g * kPerfNestedDim + s) * kPerfNestedDim + t) * kPerfNestedDim + k;
                    // Keep many nested columns per row to stress skip-pattern matching.
                    if (!first_k) {
                        t_obj.push_back(',');
                    }
                    first_k = false;
                    const uint64_t value =
                            _splitmix64(seed ^ (static_cast<uint64_t>(row_idx) << 32) ^ leaf_id) %
                            1000003ULL;
                    t_obj += "\"k";
                    t_obj.push_back(static_cast<char>('0' + k));
                    t_obj += "\":";
                    t_obj += std::to_string(value);
                }
                if (!first_k) {
                    t_obj.push_back('}');
                    if (!first_t) {
                        s_obj.push_back(',');
                    }
                    first_t = false;
                    s_obj += "\"t";
                    s_obj.push_back(static_cast<char>('0' + t));
                    s_obj += "\":";
                    s_obj += t_obj;
                }
            }
            if (!first_t) {
                s_obj.push_back('}');
                if (!first_s) {
                    g_obj.push_back(',');
                }
                first_s = false;
                g_obj += "\"s";
                g_obj.push_back(static_cast<char>('0' + s));
                g_obj += "\":";
                g_obj += s_obj;
            }
        }
        if (!first_s) {
            g_obj.push_back('}');
            if (!first_g) {
                root.push_back(',');
            }
            first_g = false;
            root += "\"g";
            root.push_back(static_cast<char>('0' + g));
            root += "\":";
            root += g_obj;
        }
    }
    root += ",\"meta\":{\"row_id\":";
    root += std::to_string(row_idx);
    root += ",\"rand\":";
    root += std::to_string(_splitmix64(seed + row_idx) % 9973ULL);
    root += "}}";
    return root;
}

static std::vector<std::string> _build_nested_json_rows(size_t rows, uint64_t seed) {
    std::vector<std::string> result;
    result.reserve(rows);
    for (size_t i = 0; i < rows; ++i) {
        result.emplace_back(_build_nested_json_row(i, seed));
    }
    return result;
}

static vectorized::ColumnString::MutablePtr _make_json_column(
        const std::vector<std::string>& rows) {
    auto col = vectorized::ColumnString::create();
    for (const auto& row : rows) {
        col->insert_data(row.data(), row.size());
    }
    return col;
}

static std::vector<std::pair<std::string, PatternTypePB>> _build_skip_patterns_for_perf() {
    std::vector<std::pair<std::string, PatternTypePB>> patterns;
    patterns.reserve(96);

    // Exact match patterns.
    for (size_t leaf_id = 0; leaf_id < kPerfNestedLeafCount; leaf_id += 211) {
        patterns.emplace_back(_path_of_leaf_id(leaf_id), PatternTypePB::SKIP_NAME);
    }

    // Unmatched glob patterns to amplify old per-pattern matching cost.
    for (int i = 0; i < 30; ++i) {
        patterns.emplace_back("x" + std::to_string(i) + "*.s?.t?.k?",
                              PatternTypePB::SKIP_NAME_GLOB);
    }

    // Matched glob patterns.
    for (size_t g = 0; g < kPerfNestedDim; ++g) {
        std::string pattern = "g";
        pattern.push_back(static_cast<char>('0' + g));
        pattern += ".s?.t?.k[02468]";
        patterns.emplace_back(std::move(pattern), PatternTypePB::SKIP_NAME_GLOB);
    }

    return patterns;
}

struct PerfParseResult {
    vectorized::ColumnVariant::MutablePtr column;
    int64_t elapsed_ms = 0;
};

static PerfParseResult _run_parse_perf(const vectorized::ColumnString& json_column,
                                       const vectorized::ParseConfig& config) {
    auto variant = vectorized::ColumnVariant::create(0);
    const auto start = std::chrono::steady_clock::now();
    parse_json_to_variant(*variant, json_column, config);
    const auto end = std::chrono::steady_clock::now();
    PerfParseResult result;
    result.column = std::move(variant);
    result.elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    return result;
}

TEST(VariantUtilTest, ParseDocValueToSubcolumns_FillsDefaultsAndValues) {
    const std::vector<std::string_view> jsons = {
            R"({"a":1,"b":"x"})", //
            R"({"b":"y"})",       //
            R"({"a":3})",         //
    };

    auto variant = vectorized::ColumnVariant::create(0);
    auto json_col = _make_json_column(jsons);

    vectorized::ParseConfig cfg;
    cfg.enable_flatten_nested = false;
    cfg.parse_to = vectorized::ParseConfig::ParseTo::OnlyDocValueColumn;
    parse_json_to_variant(*variant, *json_col, cfg);

    EXPECT_TRUE(variant->is_doc_mode());

    auto subcolumns = materialize_docs_to_subcolumns_map(*variant);
    ASSERT_TRUE(subcolumns.contains("a"));
    ASSERT_TRUE(subcolumns.contains("b"));

    auto& a = subcolumns.at("a");
    auto& b = subcolumns.at("b");
    a.finalize();
    b.finalize();
    EXPECT_EQ(a.size(), jsons.size());
    EXPECT_EQ(b.size(), jsons.size());

    vectorized::FieldWithDataType fa;
    a.get(0, fa);
    EXPECT_EQ(fa.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(fa.field.get<TYPE_BIGINT>(), 1);

    a.get(1, fa);
    EXPECT_EQ(fa.field.get_type(), PrimitiveType::TYPE_NULL); // missing

    a.get(2, fa);
    EXPECT_EQ(fa.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(fa.field.get<TYPE_BIGINT>(), 3);

    vectorized::FieldWithDataType fb;
    b.get(0, fb);
    EXPECT_EQ(fb.field.get_type(), PrimitiveType::TYPE_STRING);
    EXPECT_EQ(fb.field.get<TYPE_STRING>(), "x");

    b.get(1, fb);
    EXPECT_EQ(fb.field.get_type(), PrimitiveType::TYPE_STRING);
    EXPECT_EQ(fb.field.get<TYPE_STRING>(), "y");

    b.get(2, fb);
    EXPECT_EQ(fb.field.get_type(), PrimitiveType::TYPE_NULL); // missing
}

TEST(VariantUtilTest, ParseOnlyDocValueColumn_SerializesMixedTypes) {
    const std::vector<std::string_view> jsons = {
            R"({"b":true,"d":1.5,"u":18446744073709551615,"arr":[1,2,3],"arr2":[[1],[2]],"s":"x"})",
            R"({"b":false,"arr":[4],"s":"y"})",
    };

    auto variant = vectorized::ColumnVariant::create(0);
    auto json_col = _make_json_column(jsons);

    vectorized::ParseConfig cfg;
    cfg.enable_flatten_nested = false;
    cfg.parse_to = vectorized::ParseConfig::ParseTo::OnlyDocValueColumn;
    parse_json_to_variant(*variant, *json_col, cfg);

    EXPECT_TRUE(variant->is_doc_mode());

    auto subcolumns = materialize_docs_to_subcolumns_map(*variant);
    ASSERT_TRUE(subcolumns.contains("b"));
    ASSERT_TRUE(subcolumns.contains("d"));
    ASSERT_TRUE(subcolumns.contains("u"));
    ASSERT_TRUE(subcolumns.contains("arr"));
    ASSERT_TRUE(subcolumns.contains("arr2"));
    ASSERT_TRUE(subcolumns.contains("s"));

    auto& b = subcolumns.at("b");
    auto& d = subcolumns.at("d");
    auto& u = subcolumns.at("u");
    auto& arr = subcolumns.at("arr");
    auto& arr2 = subcolumns.at("arr2");
    auto& s = subcolumns.at("s");
    b.finalize();
    d.finalize();
    u.finalize();
    arr.finalize();
    arr2.finalize();
    s.finalize();

    vectorized::FieldWithDataType f;
    b.get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BOOLEAN);
    EXPECT_EQ(f.field.get<TYPE_BOOLEAN>(), true);
    b.get(1, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BOOLEAN);
    EXPECT_EQ(f.field.get<TYPE_BOOLEAN>(), false);

    d.get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_DOUBLE);
    EXPECT_EQ(f.field.get<TYPE_DOUBLE>(), 1.5);
    d.get(1, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_NULL);

    u.get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_LARGEINT);
    EXPECT_EQ(f.field.get<TYPE_LARGEINT>(), static_cast<int128_t>(18446744073709551615ULL));
    u.get(1, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_NULL);

    arr.get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_ARRAY);
    arr.get(1, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_ARRAY);

    arr2.get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_JSONB);
    arr2.get(1, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_NULL);

    s.get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_STRING);
    EXPECT_EQ(f.field.get<TYPE_STRING>(), "x");
    s.get(1, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_STRING);
    EXPECT_EQ(f.field.get<TYPE_STRING>(), "y");
}

TEST(VariantUtilTest, ParseVariantColumns_ScalarJsonStringToSubcolumns) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* c = schema_pb.add_column();
    c->set_unique_id(1);
    c->set_name("v");
    c->set_type("VARIANT");
    c->set_is_key(false);
    c->set_is_nullable(false);
    // doc mode disabled
    c->set_variant_enable_doc_mode(false);

    TabletSchema tablet_schema;
    tablet_schema.init_from_pb(schema_pb);

    auto variant = vectorized::ColumnVariant::create(0);
    doris::VariantUtil::insert_root_scalar_field(
            *variant, vectorized::Field::create_field<TYPE_STRING>(String(R"({"a":1})")));
    doris::VariantUtil::insert_root_scalar_field(
            *variant, vectorized::Field::create_field<TYPE_STRING>(String(R"({"a":2})")));

    vectorized::Block block;
    block.insert({variant->get_ptr(), std::make_shared<vectorized::DataTypeVariant>(0), "v"});

    const std::vector<uint32_t> column_pos {0};
    Status st = parse_and_materialize_variant_columns(block, tablet_schema, column_pos);
    EXPECT_TRUE(st.ok()) << st.to_string();

    const auto& col0 = *block.get_by_position(0).column;
    const auto& out = assert_cast<const vectorized::ColumnVariant&>(col0);

    const auto* sub_a = out.get_subcolumn(vectorized::PathInData("a"));
    ASSERT_TRUE(sub_a != nullptr);
    vectorized::FieldWithDataType f;
    sub_a->get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 1);
    sub_a->get(1, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 2);
}

TEST(VariantUtilTest, ParseVariantColumns_DocModeBinaryToSubcolumns) {
    const std::vector<std::string_view> jsons = {
            R"({"a":1,"b":"x"})", //
            R"({"a":2,"b":"y"})", //
    };

    // Build a doc-mode ColumnVariant: Only root in subcolumns, others stored in doc snapshot column.
    auto variant = vectorized::ColumnVariant::create(0);
    auto json_col = _make_json_column(jsons);
    vectorized::ParseConfig cfg;
    cfg.enable_flatten_nested = false;
    cfg.parse_to = vectorized::ParseConfig::ParseTo::OnlyDocValueColumn;
    parse_json_to_variant(*variant, *json_col, cfg);
    ASSERT_TRUE(variant->is_doc_mode());

    vectorized::Block block;
    block.insert({variant->get_ptr(), std::make_shared<vectorized::DataTypeVariant>(0), "v"});

    vectorized::ParseConfig parse_cfg;
    parse_cfg.enable_flatten_nested = false;
    parse_cfg.parse_to = vectorized::ParseConfig::ParseTo::OnlyDocValueColumn;
    Status st =
            parse_and_materialize_variant_columns(block, std::vector<uint32_t> {0}, {parse_cfg});
    EXPECT_TRUE(st.ok()) << st.to_string();

    const auto& out =
            assert_cast<const vectorized::ColumnVariant&>(*block.get_by_position(0).column);
    EXPECT_TRUE(out.is_doc_mode());

    const auto* sub_a = out.get_subcolumn(vectorized::PathInData("a"));
    const auto* sub_b = out.get_subcolumn(vectorized::PathInData("b"));
    ASSERT_TRUE(sub_a == nullptr);
    ASSERT_TRUE(sub_b == nullptr);

    auto docs_subcolumns = materialize_docs_to_subcolumns_map(out);
    ASSERT_TRUE(docs_subcolumns.contains("a"));
    ASSERT_TRUE(docs_subcolumns.contains("b"));
    auto& materialized_a = docs_subcolumns.at("a");
    auto& materialized_b = docs_subcolumns.at("b");
    materialized_a.finalize();
    materialized_b.finalize();

    vectorized::FieldWithDataType f;
    materialized_a.get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 1);
    materialized_a.get(1, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 2);

    materialized_b.get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_STRING);
    EXPECT_EQ(f.field.get<TYPE_STRING>(), "x");
    materialized_b.get(1, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_STRING);
    EXPECT_EQ(f.field.get<TYPE_STRING>(), "y");
}

TEST(VariantUtilTest, ParseVariantColumns_DocModeRejectOnlySubcolumnsConfig) {
    const std::vector<std::string_view> jsons = {R"({"a":1})"};
    auto variant = vectorized::ColumnVariant::create(0);
    auto json_col = _make_json_column(jsons);

    vectorized::ParseConfig cfg;
    cfg.enable_flatten_nested = false;
    cfg.parse_to = vectorized::ParseConfig::ParseTo::OnlyDocValueColumn;
    parse_json_to_variant(*variant, *json_col, cfg);
    ASSERT_TRUE(variant->is_doc_mode());

    vectorized::Block block;
    block.insert({variant->get_ptr(), std::make_shared<vectorized::DataTypeVariant>(0), "v"});

    vectorized::ParseConfig parse_cfg;
    parse_cfg.enable_flatten_nested = false;
    parse_cfg.parse_to = vectorized::ParseConfig::ParseTo::OnlyDocValueColumn;
    Status st =
            parse_and_materialize_variant_columns(block, std::vector<uint32_t> {0}, {parse_cfg});
    EXPECT_TRUE(st.ok()) << st.to_string();
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
    Status st = glob_to_regex("int_[0-9", &regex);
    EXPECT_FALSE(st.ok());

    st = glob_to_regex("a[\\]b", &regex);
    EXPECT_FALSE(st.ok());
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

TEST(VariantUtilTest, ShouldSkipPathLegacyPatterns) {
    std::vector<std::pair<std::string, PatternTypePB>> skip_patterns = {
            {"secret", PatternTypePB::SKIP_NAME},
            {"debug_*", PatternTypePB::SKIP_NAME_GLOB},
            {"typed_*", PatternTypePB::MATCH_NAME_GLOB},
    };

    EXPECT_TRUE(should_skip_path(skip_patterns, "secret"));
    EXPECT_TRUE(should_skip_path(skip_patterns, "debug_field"));
    EXPECT_FALSE(should_skip_path(skip_patterns, "typed_field"));
    EXPECT_FALSE(should_skip_path(skip_patterns, "other"));
}

TEST(VariantUtilTest, PatternTypeHelpers) {
    EXPECT_TRUE(is_typed_path_pattern_type(PatternTypePB::MATCH_NAME));
    EXPECT_TRUE(is_typed_path_pattern_type(PatternTypePB::MATCH_NAME_GLOB));
    EXPECT_FALSE(is_typed_path_pattern_type(PatternTypePB::SKIP_NAME));
    EXPECT_FALSE(is_typed_path_pattern_type(PatternTypePB::SKIP_NAME_GLOB));

    EXPECT_TRUE(is_skip_exact_path_pattern_type(PatternTypePB::SKIP_NAME));
    EXPECT_FALSE(is_skip_exact_path_pattern_type(PatternTypePB::SKIP_NAME_GLOB));
    EXPECT_TRUE(is_skip_glob_path_pattern_type(PatternTypePB::SKIP_NAME_GLOB));
    EXPECT_FALSE(is_skip_glob_path_pattern_type(PatternTypePB::MATCH_NAME_GLOB));
}

TEST(VariantUtilTest, BuildCompiledSkipMatcherRejectsNullOutPointer) {
    std::vector<std::pair<std::string, PatternTypePB>> skip_patterns = {
            {"secret", PatternTypePB::SKIP_NAME},
    };
    Status st = build_compiled_skip_matcher(skip_patterns, true, nullptr);
    EXPECT_FALSE(st.ok());
}

TEST(VariantUtilTest, BuildCompiledSkipMatcherMixedPatterns) {
    std::vector<std::pair<std::string, PatternTypePB>> skip_patterns = {
            {"secret", PatternTypePB::SKIP_NAME},
            {"debug_*", PatternTypePB::SKIP_NAME_GLOB},
            {"[invalid", PatternTypePB::SKIP_NAME_GLOB},
            {"typed_*", PatternTypePB::MATCH_NAME_GLOB},
    };

    std::shared_ptr<const CompiledSkipMatcher> matcher;
    Status st = build_compiled_skip_matcher(skip_patterns, false, &matcher);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(matcher != nullptr);

    EXPECT_TRUE(should_skip_path(*matcher, "secret"));
    EXPECT_TRUE(should_skip_path(*matcher, "debug_field"));
    EXPECT_FALSE(should_skip_path(*matcher, "typed_field"));
    EXPECT_FALSE(should_skip_path(*matcher, "other"));
}

TEST(VariantUtilTest, BuildCompiledSkipMatcherWithRe2Set) {
    std::vector<std::pair<std::string, PatternTypePB>> skip_patterns;
    for (int i = 0; i < 40; ++i) {
        skip_patterns.emplace_back("k" + std::to_string(i) + "_*", PatternTypePB::SKIP_NAME_GLOB);
    }

    std::shared_ptr<const CompiledSkipMatcher> matcher;
    Status st = build_compiled_skip_matcher(skip_patterns, true, &matcher);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(matcher != nullptr);

    EXPECT_TRUE(should_skip_path(*matcher, "k1_abc"));
    EXPECT_TRUE(should_skip_path(*matcher, "k39_abc"));
    EXPECT_FALSE(should_skip_path(*matcher, "unknown_abc"));
}

TEST(VariantUtilTest, ParseVariantColumnsApplySkipPatternsFromSchemaChildren) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* c = schema_pb.add_column();
    c->set_unique_id(1);
    c->set_name("v");
    c->set_type("VARIANT");
    c->set_is_key(false);
    c->set_is_nullable(false);
    c->set_variant_enable_doc_mode(false);

    // Typed path: should not be skipped.
    auto* typed = c->add_children_columns();
    typed->set_unique_id(2);
    typed->set_name("num_*");
    typed->set_type("BIGINT");
    typed->set_is_key(false);
    typed->set_is_nullable(true);
    typed->set_pattern_type(PatternTypePB::MATCH_NAME_GLOB);

    // Skip exact.
    auto* skip_exact = c->add_children_columns();
    skip_exact->set_unique_id(3);
    skip_exact->set_name("secret");
    skip_exact->set_type("STRING");
    skip_exact->set_is_key(false);
    skip_exact->set_is_nullable(true);
    skip_exact->set_pattern_type(PatternTypePB::SKIP_NAME);

    // Skip glob.
    auto* skip_glob = c->add_children_columns();
    skip_glob->set_unique_id(4);
    skip_glob->set_name("debug_*");
    skip_glob->set_type("STRING");
    skip_glob->set_is_key(false);
    skip_glob->set_is_nullable(true);
    skip_glob->set_pattern_type(PatternTypePB::SKIP_NAME_GLOB);

    TabletSchema tablet_schema;
    tablet_schema.init_from_pb(schema_pb);

    auto variant = vectorized::ColumnVariant::create(0);
    doris::VariantUtil::insert_root_scalar_field(
            *variant, vectorized::Field::create_field<TYPE_STRING>(
                              String(R"({"secret":1,"debug_a":2,"keep":3,"num_a":4})")));
    doris::VariantUtil::insert_root_scalar_field(
            *variant, vectorized::Field::create_field<TYPE_STRING>(
                              String(R"({"secret":5,"debug_b":6,"keep":7,"num_b":8})")));

    vectorized::Block block;
    block.insert({variant->get_ptr(), std::make_shared<vectorized::DataTypeVariant>(0), "v"});

    Status st =
            parse_and_materialize_variant_columns(block, tablet_schema, std::vector<uint32_t> {0});
    ASSERT_TRUE(st.ok()) << st.to_string();

    const auto& col0 = *block.get_by_position(0).column;
    const auto& out = assert_cast<const vectorized::ColumnVariant&>(col0);

    EXPECT_EQ(nullptr, out.get_subcolumn(vectorized::PathInData("secret")));
    EXPECT_EQ(nullptr, out.get_subcolumn(vectorized::PathInData("debug_a")));
    EXPECT_EQ(nullptr, out.get_subcolumn(vectorized::PathInData("debug_b")));

    const auto* sub_keep = out.get_subcolumn(vectorized::PathInData("keep"));
    const auto* sub_num_a = out.get_subcolumn(vectorized::PathInData("num_a"));
    const auto* sub_num_b = out.get_subcolumn(vectorized::PathInData("num_b"));
    ASSERT_TRUE(sub_keep != nullptr);
    ASSERT_TRUE(sub_num_a != nullptr);
    ASSERT_TRUE(sub_num_b != nullptr);
}

TEST(VariantUtilTest, SkipPatternCacheHitsAcrossRows) {
    constexpr size_t kRows = 64;
    std::vector<std::string> json_rows;
    json_rows.reserve(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        json_rows.emplace_back("{\"secret\":" + std::to_string(i) +
                               ",\"keep\":" + std::to_string(i + 1) + "}");
    }

    auto json_column = _make_json_column(json_rows);

    std::vector<std::pair<std::string, PatternTypePB>> skip_patterns = {
            {"secret", PatternTypePB::SKIP_NAME},
    };
    std::shared_ptr<const CompiledSkipMatcher> matcher;
    Status st = build_compiled_skip_matcher(skip_patterns, true, &matcher);
    ASSERT_TRUE(st.ok()) << st.to_string();

    vectorized::ParseConfig cfg;
    cfg.enable_flatten_nested = false;
    cfg.parse_to = vectorized::ParseConfig::ParseTo::OnlySubcolumns;
    cfg.skip_path_patterns = &skip_patterns;
    cfg.compiled_skip_matcher = matcher;
    cfg.skip_result_cache_capacity = 8;
    cfg.adaptive_skip_result_cache_capacity = true;
    vectorized::SkipCacheStats cache_stats;
    cfg.skip_cache_stats = &cache_stats;

    auto out = vectorized::ColumnVariant::create(0);
    parse_json_to_variant(*out, *json_column, cfg);

    const double hit_rate = cache_stats.lookup_count > 0
                                    ? static_cast<double>(cache_stats.hit_count) /
                                              static_cast<double>(cache_stats.lookup_count)
                                    : 0.0;
    const double miss_rate = cache_stats.lookup_count > 0
                                     ? static_cast<double>(cache_stats.miss_count) /
                                               static_cast<double>(cache_stats.lookup_count)
                                     : 0.0;
    LOG(INFO) << "skip cache cross-row stats: "
              << "lookups=" << cache_stats.lookup_count << ", "
              << "hits=" << cache_stats.hit_count << ", "
              << "misses=" << cache_stats.miss_count << ", "
              << "hit_rate=" << hit_rate << ", "
              << "miss_rate=" << miss_rate << ", "
              << "inserts=" << cache_stats.insert_count << ", "
              << "evicts=" << cache_stats.evict_count;

    EXPECT_EQ(nullptr, out->get_subcolumn(vectorized::PathInData("secret")));
    EXPECT_TRUE(out->get_subcolumn(vectorized::PathInData("keep")) != nullptr);
    // Each row has unique object keys within the row, so cache hits here must be cross-row hits.
    EXPECT_EQ(cache_stats.lookup_count, kRows * 2);
    EXPECT_EQ(cache_stats.hit_count, (kRows - 1) * 2);
    EXPECT_EQ(cache_stats.miss_count, 2);
    EXPECT_EQ(cache_stats.insert_count, 2);
    EXPECT_EQ(cache_stats.evict_count, 0);
}

TEST(VariantUtilTest, AdaptiveSkipPatternCacheRoundsUpCapacity) {
    std::vector<std::string> json_rows = {
            R"({"a":1,"b":1,"c":1})",
            R"({"a":2,"b":2,"c":2,"d":2})",
    };
    auto json_column = _make_json_column(json_rows);

    std::vector<std::pair<std::string, PatternTypePB>> skip_patterns = {
            {"a", PatternTypePB::SKIP_NAME},
            {"b", PatternTypePB::SKIP_NAME},
            {"c", PatternTypePB::SKIP_NAME},
            {"d", PatternTypePB::SKIP_NAME},
    };
    std::shared_ptr<const CompiledSkipMatcher> matcher;
    Status st = build_compiled_skip_matcher(skip_patterns, true, &matcher);
    ASSERT_TRUE(st.ok()) << st.to_string();

    vectorized::ParseConfig cfg;
    cfg.enable_flatten_nested = false;
    cfg.parse_to = vectorized::ParseConfig::ParseTo::OnlySubcolumns;
    cfg.skip_path_patterns = &skip_patterns;
    cfg.compiled_skip_matcher = matcher;
    cfg.skip_result_cache_capacity = 1;
    cfg.adaptive_skip_result_cache_capacity = true;
    vectorized::SkipCacheStats cache_stats;
    cfg.skip_cache_stats = &cache_stats;

    auto out = vectorized::ColumnVariant::create(0);
    parse_json_to_variant(*out, *json_column, cfg);

    EXPECT_EQ(nullptr, out->get_subcolumn(vectorized::PathInData("a")));
    EXPECT_EQ(nullptr, out->get_subcolumn(vectorized::PathInData("b")));
    EXPECT_EQ(nullptr, out->get_subcolumn(vectorized::PathInData("c")));
    EXPECT_EQ(nullptr, out->get_subcolumn(vectorized::PathInData("d")));

    // First row learns 3 skipped keys and rounds capacity up to 4, so inserting 'd' on second row
    // should not evict any cached key.
    EXPECT_EQ(cache_stats.insert_count, 4);
    EXPECT_EQ(cache_stats.evict_count, 0);
}

TEST(VariantUtilTest, SkipPatternPerfCompareOptimizationMatrix) {
    if (std::getenv("DORIS_RUN_VARIANT_SKIP_PERF_UT") == nullptr) {
        GTEST_SKIP() << "Set DORIS_RUN_VARIANT_SKIP_PERF_UT=1 to run this heavy perf test.";
    }

    constexpr size_t kRows = 1000;
    constexpr uint64_t kSeed = 0x20260211ULL;
    const auto json_rows = _build_nested_json_rows(kRows, kSeed);
    const auto json_column = _make_json_column(json_rows);
    const auto skip_patterns = _build_skip_patterns_for_perf();

    vectorized::ParseConfig no_skip_config;
    no_skip_config.enable_flatten_nested = false;
    no_skip_config.parse_to = vectorized::ParseConfig::ParseTo::OnlySubcolumns;

    vectorized::ParseConfig legacy_config;
    legacy_config.enable_flatten_nested = false;
    legacy_config.parse_to = vectorized::ParseConfig::ParseTo::OnlySubcolumns;
    legacy_config.skip_path_patterns = &skip_patterns;
    legacy_config.compiled_skip_matcher = nullptr;
    legacy_config.skip_result_cache_capacity = 0;
    legacy_config.adaptive_skip_result_cache_capacity = false;

    std::shared_ptr<const CompiledSkipMatcher> compiled_matcher_with_re2_set;
    Status st = build_compiled_skip_matcher(skip_patterns, true, &compiled_matcher_with_re2_set);
    ASSERT_TRUE(st.ok()) << st.to_string();

    std::shared_ptr<const CompiledSkipMatcher> compiled_matcher_without_re2_set;
    st = build_compiled_skip_matcher(skip_patterns, false, &compiled_matcher_without_re2_set);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // 3) current optimization - is_skipped cache and RE2::Set both disabled.
    vectorized::ParseConfig optimized_no_cache_no_re2set_config = legacy_config;
    optimized_no_cache_no_re2set_config.compiled_skip_matcher = compiled_matcher_without_re2_set;
    optimized_no_cache_no_re2set_config.skip_result_cache_capacity = 0;
    optimized_no_cache_no_re2set_config.adaptive_skip_result_cache_capacity = false;

    // 4) current optimization - is_skipped cache disabled.
    vectorized::ParseConfig optimized_no_cache_config = legacy_config;
    optimized_no_cache_config.compiled_skip_matcher = compiled_matcher_with_re2_set;
    optimized_no_cache_config.skip_result_cache_capacity = 0;
    optimized_no_cache_config.adaptive_skip_result_cache_capacity = false;

    // 5) current optimization - RE2::Set disabled.
    vectorized::ParseConfig optimized_no_re2set_config = legacy_config;
    optimized_no_re2set_config.compiled_skip_matcher = compiled_matcher_without_re2_set;
    optimized_no_re2set_config.skip_result_cache_capacity = 256;
    optimized_no_re2set_config.adaptive_skip_result_cache_capacity = true;

    // 6) current optimization.
    vectorized::ParseConfig optimized_config = legacy_config;
    optimized_config.compiled_skip_matcher = compiled_matcher_with_re2_set;
    optimized_config.skip_result_cache_capacity = 256;
    optimized_config.adaptive_skip_result_cache_capacity = true;

    auto no_skip_result = _run_parse_perf(*json_column, no_skip_config);
    auto legacy_result = _run_parse_perf(*json_column, legacy_config);
    auto optimized_no_cache_no_re2set_result =
            _run_parse_perf(*json_column, optimized_no_cache_no_re2set_config);
    auto optimized_no_cache_result = _run_parse_perf(*json_column, optimized_no_cache_config);
    auto optimized_no_re2set_result = _run_parse_perf(*json_column, optimized_no_re2set_config);
    auto optimized_result = _run_parse_perf(*json_column, optimized_config);

    ASSERT_EQ(no_skip_result.column->size(), kRows);
    ASSERT_EQ(legacy_result.column->size(), kRows);
    ASSERT_EQ(optimized_no_cache_no_re2set_result.column->size(), kRows);
    ASSERT_EQ(optimized_no_cache_result.column->size(), kRows);
    ASSERT_EQ(optimized_no_re2set_result.column->size(), kRows);
    ASSERT_EQ(optimized_result.column->size(), kRows);

    vectorized::DataTypeSerDe::FormatOptions options;
    bool found_no_skip_difference = false;
    for (size_t row = 0; row < kRows; row += 97) {
        std::string no_skip_row;
        std::string legacy_row;
        std::string optimized_no_cache_no_re2set_row;
        std::string optimized_no_cache_row;
        std::string optimized_no_re2set_row;
        std::string optimized_row;
        no_skip_result.column->serialize_one_row_to_string(row, &no_skip_row, options);
        legacy_result.column->serialize_one_row_to_string(row, &legacy_row, options);
        optimized_no_cache_no_re2set_result.column->serialize_one_row_to_string(
                row, &optimized_no_cache_no_re2set_row, options);
        optimized_no_cache_result.column->serialize_one_row_to_string(row, &optimized_no_cache_row,
                                                                      options);
        optimized_no_re2set_result.column->serialize_one_row_to_string(row, &optimized_no_re2set_row,
                                                                       options);
        optimized_result.column->serialize_one_row_to_string(row, &optimized_row, options);
        if (!found_no_skip_difference && no_skip_row != legacy_row) {
            found_no_skip_difference = true;
        }
        ASSERT_EQ(legacy_row, optimized_no_cache_no_re2set_row) << "row=" << row;
        ASSERT_EQ(legacy_row, optimized_no_cache_row) << "row=" << row;
        ASSERT_EQ(legacy_row, optimized_no_re2set_row) << "row=" << row;
        ASSERT_EQ(legacy_row, optimized_row) << "row=" << row;
    }
    ASSERT_TRUE(found_no_skip_difference)
            << "no-skip output should differ from skip-enabled output on sampled rows";

    const auto safe_speedup = [](int64_t faster, int64_t slower) -> double {
        return slower > 0 ? static_cast<double>(faster) / static_cast<double>(slower) : 0.0;
    };

    LOG(INFO) << "skip-pattern perf matrix (" << kRows << " rows, " << kPerfNestedLeafCount
              << " nested columns, same random data): "
              << "no_skip_ms=" << no_skip_result.elapsed_ms << ", "
              << "legacy_ms=" << legacy_result.elapsed_ms << ", "
              << "opt_no_cache_no_re2set_ms=" << optimized_no_cache_no_re2set_result.elapsed_ms
              << ", opt_no_cache_ms=" << optimized_no_cache_result.elapsed_ms
              << ", opt_no_re2set_ms=" << optimized_no_re2set_result.elapsed_ms
              << ", optimized_ms=" << optimized_result.elapsed_ms
              << ", speedup_opt_no_cache_no_re2set_vs_legacy="
              << safe_speedup(legacy_result.elapsed_ms,
                              optimized_no_cache_no_re2set_result.elapsed_ms)
              << ", speedup_opt_no_cache_vs_opt_no_cache_no_re2set="
              << safe_speedup(optimized_no_cache_no_re2set_result.elapsed_ms,
                              optimized_no_cache_result.elapsed_ms)
              << ", speedup_optimized_vs_opt_no_re2set="
              << safe_speedup(optimized_no_re2set_result.elapsed_ms, optimized_result.elapsed_ms)
              << ", speedup_optimized_vs_opt_no_cache="
              << safe_speedup(optimized_no_cache_result.elapsed_ms, optimized_result.elapsed_ms)
              << ", speedup_optimized_vs_legacy="
              << safe_speedup(legacy_result.elapsed_ms, optimized_result.elapsed_ms)
              << ", skip_patterns=" << skip_patterns.size();
}

} // namespace doris::vectorized::variant_util
