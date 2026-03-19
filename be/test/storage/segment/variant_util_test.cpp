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

#include <gen_cpp/olap_file.pb.h>

#include <string>
#include <string_view>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/data_type/data_type_variant.h"
#include "core/field.h"
#include "exec/common/variant_util.h"
#include "gtest/gtest.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::variant_util {

static ColumnString::MutablePtr _make_json_column(const std::vector<std::string_view>& rows) {
    auto col = ColumnString::create();
    for (const auto& s : rows) {
        col->insert_data(s.data(), s.size());
    }
    return col;
}

TEST(VariantUtilTest, ParseDocValueToSubcolumns_FillsDefaultsAndValues) {
    const std::vector<std::string_view> jsons = {
            R"({"a":1,"b":"x"})", //
            R"({"b":"y"})",       //
            R"({"a":3})",         //
    };

    auto variant = ColumnVariant::create(0);
    auto json_col = _make_json_column(jsons);

    ParseConfig cfg;
    cfg.deprecated_enable_flatten_nested = false;
    cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
    // Use single-row API to bypass batch sampling logic, directly testing doc_value parsing
    for (size_t i = 0; i < json_col->size(); ++i) {
        StringRef ref = json_col->get_data_at(i);
        parse_json_to_variant(*variant, ref, nullptr, cfg);
    }
    variant->finalize();

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

    FieldWithDataType fa;
    a.get(0, fa);
    EXPECT_EQ(fa.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(fa.field.get<TYPE_BIGINT>(), 1);

    a.get(1, fa);
    EXPECT_EQ(fa.field.get_type(), PrimitiveType::TYPE_NULL); // missing

    a.get(2, fa);
    EXPECT_EQ(fa.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(fa.field.get<TYPE_BIGINT>(), 3);

    FieldWithDataType fb;
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

    auto variant = ColumnVariant::create(0);
    auto json_col = _make_json_column(jsons);

    ParseConfig cfg;
    cfg.deprecated_enable_flatten_nested = false;
    cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
    // Use single-row API to bypass batch sampling logic, directly testing doc_value parsing
    for (size_t i = 0; i < json_col->size(); ++i) {
        StringRef ref = json_col->get_data_at(i);
        parse_json_to_variant(*variant, ref, nullptr, cfg);
    }
    variant->finalize();

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

    FieldWithDataType f;
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

    auto variant = ColumnVariant::create(0);
    doris::VariantUtil::insert_root_scalar_field(
            *variant, Field::create_field<TYPE_STRING>(String(R"({"a":1})")));
    doris::VariantUtil::insert_root_scalar_field(
            *variant, Field::create_field<TYPE_STRING>(String(R"({"a":2})")));

    Block block;
    block.insert({variant->get_ptr(), std::make_shared<DataTypeVariant>(0), "v"});

    const std::vector<uint32_t> column_pos {0};
    Status st = parse_and_materialize_variant_columns(block, tablet_schema, column_pos);
    EXPECT_TRUE(st.ok()) << st.to_string();

    const auto& col0 = *block.get_by_position(0).column;
    const auto& out = assert_cast<const ColumnVariant&>(col0);

    const auto* sub_a = out.get_subcolumn(PathInData("a"));
    ASSERT_TRUE(sub_a != nullptr);
    FieldWithDataType f;
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
    auto variant = ColumnVariant::create(0);
    auto json_col = _make_json_column(jsons);
    ParseConfig cfg;
    cfg.deprecated_enable_flatten_nested = false;
    cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
    // Use single-row API to bypass batch sampling logic, directly testing doc_value parsing
    for (size_t i = 0; i < json_col->size(); ++i) {
        StringRef ref = json_col->get_data_at(i);
        parse_json_to_variant(*variant, ref, nullptr, cfg);
    }
    variant->finalize();
    ASSERT_TRUE(variant->is_doc_mode());

    Block block;
    block.insert({variant->get_ptr(), std::make_shared<DataTypeVariant>(0), "v"});

    ParseConfig parse_cfg;
    parse_cfg.deprecated_enable_flatten_nested = false;
    parse_cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
    Status st =
            parse_and_materialize_variant_columns(block, std::vector<uint32_t> {0}, {parse_cfg});
    EXPECT_TRUE(st.ok()) << st.to_string();

    const auto& out = assert_cast<const ColumnVariant&>(*block.get_by_position(0).column);
    EXPECT_TRUE(out.is_doc_mode());

    const auto* sub_a = out.get_subcolumn(PathInData("a"));
    const auto* sub_b = out.get_subcolumn(PathInData("b"));
    ASSERT_TRUE(sub_a == nullptr);
    ASSERT_TRUE(sub_b == nullptr);

    auto docs_subcolumns = materialize_docs_to_subcolumns_map(out);
    ASSERT_TRUE(docs_subcolumns.contains("a"));
    ASSERT_TRUE(docs_subcolumns.contains("b"));
    auto& materialized_a = docs_subcolumns.at("a");
    auto& materialized_b = docs_subcolumns.at("b");
    materialized_a.finalize();
    materialized_b.finalize();

    FieldWithDataType f;
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
    auto variant = ColumnVariant::create(0);
    auto json_col = _make_json_column(jsons);

    ParseConfig cfg;
    cfg.deprecated_enable_flatten_nested = false;
    cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
    // Use single-row API to bypass batch sampling logic, directly testing doc_value parsing
    for (size_t i = 0; i < json_col->size(); ++i) {
        StringRef ref = json_col->get_data_at(i);
        parse_json_to_variant(*variant, ref, nullptr, cfg);
    }
    variant->finalize();
    ASSERT_TRUE(variant->is_doc_mode());

    Block block;
    block.insert({variant->get_ptr(), std::make_shared<DataTypeVariant>(0), "v"});

    ParseConfig parse_cfg;
    parse_cfg.deprecated_enable_flatten_nested = false;
    parse_cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
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

// ============================================================
// Tests for doc-mode dynamic parsing strategy (sampling logic)
// ============================================================

// When OnlyDocValueColumn is requested via the batch API but unique paths < 2048,
// the sampling logic should auto-switch to subcolumn mode for better efficiency.
TEST(VariantUtilTest, DocModeDynamic_FewPaths_UsesSubcolumn) {
    const std::vector<std::string_view> jsons = {
            R"({"a":1,"b":"x"})",
            R"({"b":"y","c":3.14})",
            R"({"a":2})",
    };

    auto variant = ColumnVariant::create(0);
    auto json_col = _make_json_column(jsons);

    ParseConfig cfg;
    cfg.enable_flatten_nested = false;
    cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
    cfg.max_subcolumns_count = 2048;

    // Use the BATCH API which has the sampling logic
    parse_json_to_variant(*variant, *json_col, cfg);

    // With only 3 unique paths (a, b, c), far below 2048 threshold,
    // the sampling logic should have chosen subcolumn mode.
    EXPECT_FALSE(variant->is_doc_mode()) << "Few paths should auto-switch to subcolumn mode";

    // Verify subcolumns are correctly populated
    const auto* sub_a = variant->get_subcolumn(PathInData("a"));
    const auto* sub_b = variant->get_subcolumn(PathInData("b"));
    const auto* sub_c = variant->get_subcolumn(PathInData("c"));
    ASSERT_TRUE(sub_a != nullptr);
    ASSERT_TRUE(sub_b != nullptr);
    ASSERT_TRUE(sub_c != nullptr);

    FieldWithDataType f;
    sub_a->get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 1);

    sub_b->get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_STRING);
    EXPECT_EQ(f.field.get<TYPE_STRING>(), "x");

    sub_b->get(1, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_STRING);
    EXPECT_EQ(f.field.get<TYPE_STRING>(), "y");

    sub_c->get(1, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_DOUBLE);
    EXPECT_EQ(f.field.get<TYPE_DOUBLE>(), 3.14);
}

// When OnlyDocValueColumn is requested via the batch API and unique paths >= 2048,
// the sampling logic should keep docvalue mode.
TEST(VariantUtilTest, DocModeDynamic_ManyPaths_KeepsDocValue) {
    // Generate JSON rows with >= 2048 unique keys.
    // Each row has ~50 unique keys, so 50 rows × 50 keys = 2500+ unique keys.
    constexpr int kRows = 50;
    constexpr int kKeysPerRow = 50;

    auto json_col = ColumnString::create();
    for (int row = 0; row < kRows; ++row) {
        std::string json = "{";
        for (int k = 0; k < kKeysPerRow; ++k) {
            int key_id = row * kKeysPerRow + k;
            if (k > 0) json += ",";
            json += "\"k" + std::to_string(key_id) + "\":" + std::to_string(key_id);
        }
        json += "}";
        json_col->insert_data(json.data(), json.size());
    }

    auto variant = ColumnVariant::create(0);
    ParseConfig cfg;
    cfg.enable_flatten_nested = false;
    cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;

    // Use the BATCH API which has the sampling logic
    parse_json_to_variant(*variant, *json_col, cfg);

    // With 2500 unique paths (>= 2048 threshold),
    // the sampling logic should keep docvalue mode.
    EXPECT_TRUE(variant->is_doc_mode()) << "Many paths should keep docvalue mode";
    EXPECT_EQ(variant->rows(), kRows);

    // Verify the doc value column contains data by materializing a few paths
    auto subcolumns = materialize_docs_to_subcolumns_map(*variant);
    EXPECT_TRUE(subcolumns.contains("k0"));
    EXPECT_TRUE(subcolumns.contains("k100"));

    auto& k0 = subcolumns.at("k0");
    k0.finalize();
    FieldWithDataType f;
    k0.get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 0);
}

// Verify the sampling works correctly when total rows < sample size (512).
// All rows are sampled, and the decision is based on the complete data.
TEST(VariantUtilTest, DocModeDynamic_FewerRowsThanSampleSize) {
    // Only 2 rows, both with few paths
    const std::vector<std::string_view> jsons = {
            R"({"x":10})",
            R"({"y":20})",
    };

    auto variant = ColumnVariant::create(0);
    auto json_col = _make_json_column(jsons);

    ParseConfig cfg;
    cfg.enable_flatten_nested = false;
    cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
    cfg.max_subcolumns_count = 2048;

    parse_json_to_variant(*variant, *json_col, cfg);

    // 2 unique paths << 2048, should auto-switch to subcolumns
    EXPECT_FALSE(variant->is_doc_mode());
    EXPECT_EQ(variant->rows(), 2);

    const auto* sub_x = variant->get_subcolumn(PathInData("x"));
    const auto* sub_y = variant->get_subcolumn(PathInData("y"));
    ASSERT_TRUE(sub_x != nullptr);
    ASSERT_TRUE(sub_y != nullptr);

    FieldWithDataType f;
    sub_x->get(0, f);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 10);

    sub_y->get(1, f);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 20);
}

// Non-doc-mode should be completely unaffected by the sampling logic.
TEST(VariantUtilTest, DocModeDynamic_NonDocModeUnchanged) {
    const std::vector<std::string_view> jsons = {
            R"({"a":1,"b":"x"})",
            R"({"a":2,"b":"y"})",
    };

    auto variant = ColumnVariant::create(0);
    auto json_col = _make_json_column(jsons);

    ParseConfig cfg;
    cfg.enable_flatten_nested = false;
    cfg.parse_to = ParseConfig::ParseTo::OnlySubcolumns;

    parse_json_to_variant(*variant, *json_col, cfg);

    // Should always be subcolumn mode regardless of path count
    EXPECT_FALSE(variant->is_doc_mode());

    const auto* sub_a = variant->get_subcolumn(PathInData("a"));
    ASSERT_TRUE(sub_a != nullptr);
    FieldWithDataType f;
    sub_a->get(0, f);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 1);
    sub_a->get(1, f);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 2);
}

// ============================================================
// Tests for mixed doc-value + subcolumn compaction support
// ============================================================

// VariantExtendedInfo: has_doc_value_segments defaults to false.
TEST(VariantUtilTest, ExtendedInfo_DefaultFlags) {
    VariantExtendedInfo info;
    EXPECT_FALSE(info.has_doc_value_segments);
}

// VariantExtendedInfo: only one flag needed — !has_doc_value_segments means all downgraded.
TEST(VariantUtilTest, ExtendedInfo_SingleFlagSufficient) {
    // Simulating "all doc segments" scenario
    VariantExtendedInfo all_doc;
    all_doc.has_doc_value_segments = true;
    EXPECT_TRUE(all_doc.has_doc_value_segments);

    // Simulating "all downgraded" scenario
    VariantExtendedInfo all_sub;
    // has_doc_value_segments stays false
    EXPECT_FALSE(all_sub.has_doc_value_segments);

    // Simulating "mixed" scenario - at least one doc segment
    VariantExtendedInfo mixed;
    mixed.has_doc_value_segments = true;
    EXPECT_TRUE(mixed.has_doc_value_segments);
}

// is_doc_mode() reflects actual ColumnVariant state.
// Doc mode: only root subcolumn + non-empty serialized_doc_value_column.
TEST(VariantUtilTest, IsDocMode_DocValueParsing) {
    auto variant = ColumnVariant::create(0);
    auto json_col = _make_json_column({R"({"a":1,"b":"x"})"});

    ParseConfig cfg;
    cfg.enable_flatten_nested = false;
    cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;

    // Single-row API bypasses sampling, forces doc-value mode
    StringRef ref = json_col->get_data_at(0);
    parse_json_to_variant(*variant, ref, nullptr, cfg);
    variant->finalize();

    EXPECT_TRUE(variant->is_doc_mode());
    EXPECT_EQ(variant->get_subcolumns().size(), 1); // only root
}

// is_doc_mode() false for subcolumn-only (downgraded) data.
TEST(VariantUtilTest, IsDocMode_SubcolumnParsing) {
    auto variant = ColumnVariant::create(0);
    auto json_col = _make_json_column({
            R"({"a":1,"b":"x"})",
            R"({"a":2,"b":"y"})",
    });

    ParseConfig cfg;
    cfg.enable_flatten_nested = false;
    cfg.parse_to = ParseConfig::ParseTo::OnlySubcolumns;

    parse_json_to_variant(*variant, *json_col, cfg);

    EXPECT_FALSE(variant->is_doc_mode());
    EXPECT_GT(variant->get_subcolumns().size(), 1); // root + subcolumns
}

// Mixing doc-value data and subcolumn data via insert_range_from results in is_doc_mode()=false.
// This verifies that the compaction writer will correctly detect mixed data.
TEST(VariantUtilTest, IsDocMode_MixedInsertRangeFrom) {
    // Create a doc-value variant (simulating doc-value segment data)
    auto doc_variant = ColumnVariant::create(0);
    {
        auto json_col = _make_json_column({R"({"a":1,"b":"x"})"});
        ParseConfig cfg;
        cfg.enable_flatten_nested = false;
        cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
        StringRef ref = json_col->get_data_at(0);
        parse_json_to_variant(*doc_variant, ref, nullptr, cfg);
        doc_variant->finalize();
        ASSERT_TRUE(doc_variant->is_doc_mode());
    }

    // Create a subcolumn variant (simulating downgraded segment data)
    auto sub_variant = ColumnVariant::create(0);
    {
        auto json_col = _make_json_column({R"({"a":2,"b":"y"})"});
        ParseConfig cfg;
        cfg.enable_flatten_nested = false;
        cfg.parse_to = ParseConfig::ParseTo::OnlySubcolumns;
        parse_json_to_variant(*sub_variant, *json_col, cfg);
        ASSERT_FALSE(sub_variant->is_doc_mode());
    }

    // Merge both into a single ColumnVariant (simulating compaction merge)
    auto merged = ColumnVariant::create(0);
    merged->insert_range_from(*doc_variant, 0, doc_variant->rows());
    merged->insert_range_from(*sub_variant, 0, sub_variant->rows());

    // After merging, the mixed structures are unified to doc_value mode.
    // convert_subcolumns_to_doc_value() converts subcolumn data to doc_value entries.
    EXPECT_TRUE(merged->is_doc_mode());
    EXPECT_EQ(merged->rows(), 2);
    EXPECT_EQ(merged->get_subcolumns().size(), 1);
}

// Writer finalize branch detection: doc variant (is_doc_mode=true) should have
// subcolumns.size()==1 and non-empty doc_value_column.
TEST(VariantUtilTest, FinalizeDocMode_WriterBranchDetection) {
    // Doc mode variant: subcolumns.size()==1, and doc_value has data
    auto doc_variant = ColumnVariant::create(0);
    auto json_col = _make_json_column({R"({"x":10,"y":20})"});
    ParseConfig cfg;
    cfg.enable_flatten_nested = false;
    cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
    StringRef ref = json_col->get_data_at(0);
    parse_json_to_variant(*doc_variant, ref, nullptr, cfg);
    doc_variant->finalize();

    EXPECT_TRUE(doc_variant->is_doc_mode());
    // Branch 1: variant_enable_doc_mode && is_doc_mode -> _process_binary_column (doc writer)

    // Subcolumn-only variant: subcolumns.size()>1, doc_value empty
    auto sub_variant = ColumnVariant::create(0);
    auto json_col2 = _make_json_column({R"({"x":10,"y":20})"});
    ParseConfig cfg2;
    cfg2.enable_flatten_nested = false;
    cfg2.parse_to = ParseConfig::ParseTo::OnlySubcolumns;
    parse_json_to_variant(*sub_variant, *json_col2, cfg2);

    EXPECT_FALSE(sub_variant->is_doc_mode());
    EXPECT_GT(sub_variant->get_subcolumns().size(), 1);
    // Branch 2: variant_enable_doc_mode && !is_doc_mode -> _process_subcolumns only
    // Branch 3: !variant_enable_doc_mode -> _process_subcolumns + _process_binary_column (sparse)
}
// insert_from: src is doc_value, dst has subcolumns → convert dst to doc_value
TEST(VariantUtilTest, InsertFrom_SrcDocDstSub) {
    // Build dst with subcolumns (insert 2 subcolumn rows first)
    auto dst = ColumnVariant::create(0);
    {
        auto json_col = _make_json_column({R"({"a":1,"b":"x"})", R"({"a":2,"b":"y"})"});
        ParseConfig cfg;
        cfg.enable_flatten_nested = false;
        cfg.parse_to = ParseConfig::ParseTo::OnlySubcolumns;
        parse_json_to_variant(*dst, *json_col, cfg);
    }
    ASSERT_FALSE(dst->is_doc_mode());
    ASSERT_GT(dst->get_subcolumns().size(), 1);

    // Build src with doc_value (1 row)
    auto src = ColumnVariant::create(0);
    {
        auto json_col = _make_json_column({R"({"a":3,"b":"z"})"});
        ParseConfig cfg;
        cfg.enable_flatten_nested = false;
        cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
        StringRef ref = json_col->get_data_at(0);
        parse_json_to_variant(*src, ref, nullptr, cfg);
        src->finalize();
    }
    ASSERT_TRUE(src->is_doc_mode());

    // insert_from: src=doc, dst=sub → triggers convert_subcolumns_to_doc_value on dst
    dst->insert_from(*src, 0);
    EXPECT_EQ(dst->rows(), 3);
    EXPECT_TRUE(dst->is_doc_mode());
    EXPECT_EQ(dst->get_subcolumns().size(), 1);
}

// insert_from: dst is doc_value, src has subcolumns → serialize src row as doc_value entry
TEST(VariantUtilTest, InsertFrom_DstDocSrcSub) {
    // Build dst with doc_value (2 rows)
    auto dst = ColumnVariant::create(0);
    {
        auto json_col = _make_json_column({R"({"a":1,"b":"x"})", R"({"a":2,"b":"y"})"});
        ParseConfig cfg;
        cfg.enable_flatten_nested = false;
        cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
        for (size_t i = 0; i < json_col->size(); ++i) {
            StringRef ref = json_col->get_data_at(i);
            parse_json_to_variant(*dst, ref, nullptr, cfg);
        }
        dst->finalize();
    }
    ASSERT_TRUE(dst->is_doc_mode());

    // Build src with subcolumns (1 row)
    auto src = ColumnVariant::create(0);
    {
        auto json_col = _make_json_column({R"({"a":3,"b":"z"})"});
        ParseConfig cfg;
        cfg.enable_flatten_nested = false;
        cfg.parse_to = ParseConfig::ParseTo::OnlySubcolumns;
        parse_json_to_variant(*src, *json_col, cfg);
    }
    ASSERT_FALSE(src->is_doc_mode());
    ASSERT_GT(src->get_subcolumns().size(), 1);

    // insert_from: dst=doc, src=sub → serialize src row as doc_value entry
    dst->insert_from(*src, 0);
    EXPECT_EQ(dst->rows(), 3);
    EXPECT_TRUE(dst->is_doc_mode());
    EXPECT_EQ(dst->get_subcolumns().size(), 1);
}

// insert_range_from: dst is doc_value, src has subcolumns → batch serialize
TEST(VariantUtilTest, InsertRangeFrom_DstDocSrcSub) {
    // Build dst with doc_value (1 row)
    auto dst = ColumnVariant::create(0);
    {
        auto json_col = _make_json_column({R"({"a":1,"b":"x"})"});
        ParseConfig cfg;
        cfg.enable_flatten_nested = false;
        cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
        StringRef ref = json_col->get_data_at(0);
        parse_json_to_variant(*dst, ref, nullptr, cfg);
        dst->finalize();
    }
    ASSERT_TRUE(dst->is_doc_mode());

    // Build src with subcolumns (2 rows)
    auto src = ColumnVariant::create(0);
    {
        auto json_col = _make_json_column({R"({"a":2,"b":"y"})", R"({"a":3,"b":"z"})"});
        ParseConfig cfg;
        cfg.enable_flatten_nested = false;
        cfg.parse_to = ParseConfig::ParseTo::OnlySubcolumns;
        parse_json_to_variant(*src, *json_col, cfg);
    }
    ASSERT_FALSE(src->is_doc_mode());

    // insert_range_from: dst=doc, src=sub → batch serialize src rows as doc_value entries
    dst->insert_range_from(*src, 0, src->rows());
    EXPECT_EQ(dst->rows(), 3);
    EXPECT_TRUE(dst->is_doc_mode());
    EXPECT_EQ(dst->get_subcolumns().size(), 1);
}

// insert_range_from: src is doc_value, dst has subcolumns → convert dst first
TEST(VariantUtilTest, InsertRangeFrom_SrcDocDstSub) {
    // Build dst with subcolumns (2 rows)
    auto dst = ColumnVariant::create(0);
    {
        auto json_col = _make_json_column({R"({"a":1,"b":"x"})", R"({"a":2,"b":"y"})"});
        ParseConfig cfg;
        cfg.enable_flatten_nested = false;
        cfg.parse_to = ParseConfig::ParseTo::OnlySubcolumns;
        parse_json_to_variant(*dst, *json_col, cfg);
    }
    ASSERT_FALSE(dst->is_doc_mode());
    ASSERT_GT(dst->get_subcolumns().size(), 1);

    // Build src with doc_value (2 rows)
    auto src = ColumnVariant::create(0);
    {
        auto json_col = _make_json_column({R"({"a":3,"b":"z"})", R"({"a":4,"b":"w"})"});
        ParseConfig cfg;
        cfg.enable_flatten_nested = false;
        cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
        for (size_t i = 0; i < json_col->size(); ++i) {
            StringRef ref = json_col->get_data_at(i);
            parse_json_to_variant(*src, ref, nullptr, cfg);
        }
        src->finalize();
    }
    ASSERT_TRUE(src->is_doc_mode());

    // insert_range_from: src=doc, dst=sub → triggers convert_subcolumns_to_doc_value on dst
    dst->insert_range_from(*src, 0, src->rows());
    EXPECT_EQ(dst->rows(), 4);
    EXPECT_TRUE(dst->is_doc_mode());
    EXPECT_EQ(dst->get_subcolumns().size(), 1);
}

// insert_from: multiple insert_from calls with mixed modes accumulate correctly
TEST(VariantUtilTest, InsertFrom_SrcDocDstSub_MultiRow) {
    // Build dst with subcolumns (1 row)
    auto dst = ColumnVariant::create(0);
    {
        auto json_col = _make_json_column({R"({"x":1})"});
        ParseConfig cfg;
        cfg.enable_flatten_nested = false;
        cfg.parse_to = ParseConfig::ParseTo::OnlySubcolumns;
        parse_json_to_variant(*dst, *json_col, cfg);
    }
    ASSERT_FALSE(dst->is_doc_mode());
    ASSERT_GT(dst->get_subcolumns().size(), 1);

    // Build src with doc_value (3 rows)
    auto src = ColumnVariant::create(0);
    {
        auto json_col = _make_json_column({R"({"x":2})", R"({"x":3})", R"({"x":4})"});
        ParseConfig cfg;
        cfg.enable_flatten_nested = false;
        cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
        for (size_t i = 0; i < json_col->size(); ++i) {
            StringRef ref = json_col->get_data_at(i);
            parse_json_to_variant(*src, ref, nullptr, cfg);
        }
        src->finalize();
    }
    ASSERT_TRUE(src->is_doc_mode());

    // First insert_from: src=doc, dst=sub → converts dst to doc, then fast insert
    dst->insert_from(*src, 0);
    EXPECT_EQ(dst->rows(), 2);
    EXPECT_TRUE(dst->is_doc_mode());

    // Second & third: now both are doc mode, root-only fast path
    dst->insert_from(*src, 1);
    dst->insert_from(*src, 2);
    EXPECT_EQ(dst->rows(), 4);
    EXPECT_TRUE(dst->is_doc_mode());
}

} // namespace doris::variant_util
