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

#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "common/config.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/data_type/data_type_variant.h"
#include "core/field.h"
#include "core/value/jsonb_value.h"
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

static TabletSchema _make_variant_schema(bool enable_doc_mode = false,
                                         bool enable_nested_group = false) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* c = schema_pb.add_column();
    c->set_unique_id(1);
    c->set_name("v");
    c->set_type("VARIANT");
    c->set_is_key(false);
    c->set_is_nullable(false);
    c->set_variant_enable_doc_mode(enable_doc_mode);
    c->set_variant_enable_nested_group(enable_nested_group);

    TabletSchema tablet_schema;
    tablet_schema.init_from_pb(schema_pb);
    return tablet_schema;
}

static Block _make_scalar_variant_block(const std::vector<std::string>& jsons) {
    auto variant = ColumnVariant::create(0, false);
    for (const auto& json : jsons) {
        doris::VariantUtil::insert_root_scalar_field(
                *variant, Field::create_field<TYPE_STRING>(String(json)));
    }

    Block block;
    block.insert({variant->get_ptr(), std::make_shared<DataTypeVariant>(0, false), "v"});
    return block;
}

class ScopedDuplicateJsonPathCheck {
public:
    explicit ScopedDuplicateJsonPathCheck(bool value)
            : _old_value(config::variant_enable_duplicate_json_path_check) {
        config::variant_enable_duplicate_json_path_check = value;
    }
    ~ScopedDuplicateJsonPathCheck() {
        config::variant_enable_duplicate_json_path_check = _old_value;
    }

private:
    bool _old_value;
};

TEST(VariantUtilTest, ParseDocValueToSubcolumns_FillsDefaultsAndValues) {
    const std::vector<std::string_view> jsons = {
            R"({"a":1,"b":"x"})", //
            R"({"b":"y"})",       //
            R"({"a":3})",         //
    };

    auto variant = ColumnVariant::create(0, true);
    auto json_col = _make_json_column(jsons);

    ParseConfig cfg;
    cfg.deprecated_enable_flatten_nested = false;
    cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
    parse_json_to_variant(*variant, *json_col, cfg);

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

TEST(VariantUtilTest, MaterializeDocsToSubcolumnsMap_ExpectedUniquePathsPreservesValues) {
    const std::vector<std::string_view> jsons = {
            R"({"a":1,"b":"x"})", //
            R"({"b":"y","c":2})", //
            R"({"a":3,"c":4})",   //
    };

    auto variant = ColumnVariant::create(0, true);
    auto json_col = _make_json_column(jsons);

    ParseConfig cfg;
    cfg.deprecated_enable_flatten_nested = false;
    cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
    parse_json_to_variant(*variant, *json_col, cfg);

    auto default_subcolumns = materialize_docs_to_subcolumns_map(*variant);
    auto subcolumns = materialize_docs_to_subcolumns_map(*variant, 3);
    ASSERT_EQ(subcolumns.size(), default_subcolumns.size());
    ASSERT_EQ(subcolumns.size(), 3);

    auto& a = subcolumns.at("a");
    auto& b = subcolumns.at("b");
    auto& c = subcolumns.at("c");
    a.finalize();
    b.finalize();
    c.finalize();
    EXPECT_EQ(a.size(), jsons.size());
    EXPECT_EQ(b.size(), jsons.size());
    EXPECT_EQ(c.size(), jsons.size());

    FieldWithDataType f;
    a.get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 1);
    a.get(1, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_NULL);
    a.get(2, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 3);

    b.get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_STRING);
    EXPECT_EQ(f.field.get<TYPE_STRING>(), "x");
    b.get(1, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_STRING);
    EXPECT_EQ(f.field.get<TYPE_STRING>(), "y");
    b.get(2, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_NULL);

    c.get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_NULL);
    c.get(1, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 2);
    c.get(2, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 4);
}

TEST(VariantUtilTest, ParseOnlyDocValueColumn_SerializesMixedTypes) {
    const std::vector<std::string_view> jsons = {
            R"({"b":true,"d":1.5,"u":18446744073709551615,"arr":[1,2,3],"arr2":[[1],[2]],"s":"x"})",
            R"({"b":false,"arr":[4],"s":"y"})",
    };

    auto variant = ColumnVariant::create(0, true);
    auto json_col = _make_json_column(jsons);

    ParseConfig cfg;
    cfg.deprecated_enable_flatten_nested = false;
    cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
    parse_json_to_variant(*variant, *json_col, cfg);

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

TEST(VariantUtilTest, ParseDuplicateJsonPathsKeepsFirstValue) {
    ScopedDuplicateJsonPathCheck check_guard(true);
    const std::vector<std::string_view> jsons = {
            R"({"a":42,"a":{"b":42}})", R"({"a":123,"a":"123"})",       R"({"a.b":1,"a":{"b":2}})",
            R"({"a":{"b":3},"a.b":4})", R"({"a":{"b":5},"a":{"c":6}})",
    };

    auto variant = ColumnVariant::create(0, false);
    auto json_col = _make_json_column(jsons);

    ParseConfig cfg;
    cfg.deprecated_enable_flatten_nested = false;
    cfg.check_duplicate_json_path = true;
    cfg.parse_to = ParseConfig::ParseTo::OnlySubcolumns;
    parse_json_to_variant(*variant, *json_col, cfg);
    ASSERT_TRUE(variant->sanitize().ok());

    const auto* sub_a = variant->get_subcolumn(PathInData("a"));
    const auto* sub_ab = variant->get_subcolumn(PathInData("a.b"));
    const auto* sub_ac = variant->get_subcolumn(PathInData("a.c"));
    ASSERT_NE(sub_a, nullptr);
    ASSERT_NE(sub_ab, nullptr);
    ASSERT_NE(sub_ac, nullptr);

    FieldWithDataType f;
    sub_a->get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 42);
    sub_a->get(1, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 123);

    sub_ab->get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 42);
    sub_ab->get(1, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_NULL);
    sub_ab->get(2, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 1);
    sub_ab->get(3, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 3);
    sub_ab->get(4, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 5);

    sub_ac->get(4, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 6);
}

TEST(VariantUtilTest, ParseDuplicateJsonPathsKeepsFirstArrayOrScalarValue) {
    ScopedDuplicateJsonPathCheck check_guard(true);
    const std::vector<std::string_view> jsons = {
            R"({"a":[1],"a":2})",
            R"({"a":2,"a":[1]})",
    };

    auto variant = ColumnVariant::create(0, false);
    auto json_col = _make_json_column(jsons);

    ParseConfig cfg;
    cfg.deprecated_enable_flatten_nested = false;
    cfg.check_duplicate_json_path = true;
    cfg.parse_to = ParseConfig::ParseTo::OnlySubcolumns;
    parse_json_to_variant(*variant, *json_col, cfg);
    ASSERT_TRUE(variant->sanitize().ok());

    const auto* sub_a = variant->get_subcolumn(PathInData("a"));
    ASSERT_NE(sub_a, nullptr);

    FieldWithDataType f;
    sub_a->get(0, f);
    ASSERT_EQ(f.field.get_type(), PrimitiveType::TYPE_JSONB);
    const auto& first = f.field.get<TYPE_JSONB>();
    EXPECT_EQ(JsonbToJson::jsonb_to_json_string(first.get_value(), first.get_size()), "[1]");

    sub_a->get(1, f);
    ASSERT_EQ(f.field.get_type(), PrimitiveType::TYPE_JSONB);
    const auto& second = f.field.get<TYPE_JSONB>();
    EXPECT_EQ(JsonbToJson::jsonb_to_json_string(second.get_value(), second.get_size()), "2");
}

TEST(VariantUtilTest, ParseDuplicateJsonPathsInDocModeKeepsFirstValue) {
    ScopedDuplicateJsonPathCheck check_guard(true);
    const std::vector<std::string_view> jsons = {
            R"({"a":42,"a":{"b":42}})", R"({"a":123,"a":"123"})",       R"({"a.b":1,"a":{"b":2}})",
            R"({"a":{"b":3},"a.b":4})", R"({"a":{"b":5},"a":{"c":6}})",
    };

    auto variant = ColumnVariant::create(0, true);
    auto json_col = _make_json_column(jsons);

    ParseConfig cfg;
    cfg.deprecated_enable_flatten_nested = false;
    cfg.check_duplicate_json_path = true;
    cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
    parse_json_to_variant(*variant, *json_col, cfg);
    ASSERT_TRUE(variant->sanitize().ok());

    auto subcolumns = materialize_docs_to_subcolumns_map(*variant);
    ASSERT_TRUE(subcolumns.contains("a"));
    ASSERT_TRUE(subcolumns.contains("a.b"));
    ASSERT_TRUE(subcolumns.contains("a.c"));

    auto& sub_a = subcolumns.at("a");
    auto& sub_ab = subcolumns.at("a.b");
    auto& sub_ac = subcolumns.at("a.c");
    sub_a.finalize();
    sub_ab.finalize();
    sub_ac.finalize();

    FieldWithDataType f;
    sub_a.get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 42);
    sub_a.get(1, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 123);

    sub_ab.get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 42);
    sub_ab.get(1, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_NULL);
    sub_ab.get(2, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 1);
    sub_ab.get(3, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 3);
    sub_ab.get(4, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 5);

    sub_ac.get(4, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 6);
}

TEST(VariantUtilTest, ParseDuplicateJsonPathsInDocModeKeepsFirstArrayOrScalarValue) {
    ScopedDuplicateJsonPathCheck check_guard(true);
    const std::vector<std::string_view> jsons = {
            R"({"a":[1],"a":2})",
            R"({"a":2,"a":[1]})",
    };

    auto variant = ColumnVariant::create(0, true);
    auto json_col = _make_json_column(jsons);

    ParseConfig cfg;
    cfg.deprecated_enable_flatten_nested = false;
    cfg.check_duplicate_json_path = true;
    cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
    parse_json_to_variant(*variant, *json_col, cfg);
    ASSERT_TRUE(variant->sanitize().ok());

    auto subcolumns = materialize_docs_to_subcolumns_map(*variant);
    ASSERT_TRUE(subcolumns.contains("a"));

    auto& sub_a = subcolumns.at("a");
    sub_a.finalize();

    FieldWithDataType f;
    sub_a.get(0, f);
    ASSERT_EQ(f.field.get_type(), PrimitiveType::TYPE_JSONB);
    const auto& first = f.field.get<TYPE_JSONB>();
    EXPECT_EQ(JsonbToJson::jsonb_to_json_string(first.get_value(), first.get_size()), "[1]");

    sub_a.get(1, f);
    ASSERT_EQ(f.field.get_type(), PrimitiveType::TYPE_JSONB);
    const auto& second = f.field.get<TYPE_JSONB>();
    EXPECT_EQ(JsonbToJson::jsonb_to_json_string(second.get_value(), second.get_size()), "2");
}

TEST(VariantUtilTest, ParseDuplicateJsonPathsCheckDisabledByDefault) {
    ScopedDuplicateJsonPathCheck check_guard(false);
    const std::vector<std::string_view> jsons = {
            R"({"a":123,"a":"123"})",
    };

    auto variant = ColumnVariant::create(0, false);
    auto json_col = _make_json_column(jsons);

    ParseConfig cfg;
    cfg.deprecated_enable_flatten_nested = false;
    EXPECT_THROW(parse_json_to_variant(*variant, *json_col, cfg), Exception);
}

TEST(VariantUtilTest, ParseVariantColumns_StorageNonDocScalarJsonToDocValueKv) {
    TabletSchema tablet_schema = _make_variant_schema(false);
    std::vector<std::string> jsons {R"({"a":1})", R"({"a":2})"};
    Block block = _make_scalar_variant_block(jsons);

    const std::vector<uint32_t> column_pos {0};
    Status st = parse_and_materialize_variant_columns(block, tablet_schema, column_pos);
    EXPECT_TRUE(st.ok()) << st.to_string();

    const auto& col0 = *block.get_by_position(0).column;
    const auto& out = assert_cast<const ColumnVariant&>(col0);

    const auto* sub_a = out.get_subcolumn(PathInData("a"));
    ASSERT_TRUE(sub_a == nullptr);

    const auto& doc_offsets = out.serialized_doc_value_column_offsets();
    ASSERT_EQ(doc_offsets.size(), jsons.size());
    EXPECT_EQ(doc_offsets.back(), jsons.size());

    auto docs_subcolumns = materialize_docs_to_subcolumns_map(out);
    ASSERT_TRUE(docs_subcolumns.contains("a"));
    auto& materialized_a = docs_subcolumns.at("a");
    materialized_a.finalize();

    FieldWithDataType f;
    materialized_a.get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 1);
    materialized_a.get(1, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 2);
}

TEST(VariantUtilTest, SparseStorageParseUsesDocValueKvInsteadOfManySubcolumns) {
    constexpr int kRows = 1000;
    std::vector<std::string> jsons;
    jsons.reserve(kRows);
    for (int i = 0; i < kRows; ++i) {
        jsons.push_back("{\"k" + std::to_string(i) + "\":\"" + std::to_string(i) + "\"}");
    }

    ParseConfig old_parse_cfg;
    old_parse_cfg.deprecated_enable_flatten_nested = false;
    old_parse_cfg.parse_to = ParseConfig::ParseTo::OnlySubcolumns;

    Block old_block = _make_scalar_variant_block(jsons);
    Status old_st = parse_and_materialize_variant_columns(old_block, std::vector<uint32_t> {0},
                                                          {old_parse_cfg});
    ASSERT_TRUE(old_st.ok()) << old_st.to_string();
    const auto& old_variant =
            assert_cast<const ColumnVariant&>(*old_block.get_by_position(0).column);

    TabletSchema tablet_schema = _make_variant_schema(false);
    Block new_block = _make_scalar_variant_block(jsons);
    Status new_st = parse_and_materialize_variant_columns(new_block, tablet_schema, {0});
    ASSERT_TRUE(new_st.ok()) << new_st.to_string();
    const auto& new_variant =
            assert_cast<const ColumnVariant&>(*new_block.get_by_position(0).column);

    const size_t old_subcolumns = old_variant.get_subcolumns().size();
    const size_t new_subcolumns = new_variant.get_subcolumns().size();
    const size_t old_bytes = old_variant.allocated_bytes();
    const size_t new_bytes = new_variant.allocated_bytes();

    std::cout << "sparse variant parse memory old_subcolumns=" << old_subcolumns
              << " new_subcolumns=" << new_subcolumns << " old_bytes=" << old_bytes
              << " new_bytes=" << new_bytes << std::endl;

    EXPECT_GE(old_subcolumns, static_cast<size_t>(kRows));
    EXPECT_LE(new_subcolumns, static_cast<size_t>(1));
    EXPECT_LT(new_bytes, old_bytes);

    const auto& doc_offsets = new_variant.serialized_doc_value_column_offsets();
    ASSERT_EQ(doc_offsets.size(), kRows);
    EXPECT_EQ(doc_offsets.back(), kRows);
}

TEST(VariantUtilTest, ParseVariantColumns_StorageNonDocDocValueKvSkipsInvalidRoot) {
    TabletSchema tablet_schema = _make_variant_schema(false);
    Block invalid_root_block = _make_scalar_variant_block({R"([])"});

    Status st = parse_and_materialize_variant_columns(invalid_root_block, tablet_schema, {0});
    ASSERT_TRUE(st.ok()) << st.to_string();

    const auto& invalid_root_variant =
            assert_cast<const ColumnVariant&>(*invalid_root_block.get_by_position(0).column);
    EXPECT_TRUE(invalid_root_variant.is_null_root());
    ASSERT_EQ(invalid_root_variant.serialized_doc_value_column_offsets().size(), 1);
    EXPECT_EQ(invalid_root_variant.serialized_doc_value_column_offsets().back(), 0);

    Block scalar_root_block = _make_scalar_variant_block({R"(100)"});
    st = parse_and_materialize_variant_columns(scalar_root_block, tablet_schema, {0});
    ASSERT_TRUE(st.ok()) << st.to_string();

    const auto& scalar_root_variant =
            assert_cast<const ColumnVariant&>(*scalar_root_block.get_by_position(0).column);
    ASSERT_TRUE(scalar_root_variant.is_scalar_variant());
    DataTypeSerDe::FormatOptions options;
    std::string value;
    scalar_root_variant.serialize_one_row_to_string(0, &value, options);
    EXPECT_EQ(value, "100");
}

TEST(VariantUtilTest, ParseNullableScalarVariantDetachesNestedAlias) {
    auto variant = ColumnVariant::create(0, false);
    doris::VariantUtil::insert_root_scalar_field(*variant, Field::create_field<TYPE_INT>(123));
    ColumnPtr variant_ptr = std::move(variant);

    auto null_map = ColumnUInt8::create();
    null_map->insert_value(0);
    ColumnPtr nullable_variant = ColumnNullable::create(variant_ptr, null_map->get_ptr());
    variant_ptr.reset();
    ColumnPtr nullable_alias = nullable_variant;

    Block block;
    block.insert(
            {nullable_variant, make_nullable(std::make_shared<DataTypeVariant>(0, false)), "v"});

    ParseConfig parse_cfg;
    parse_cfg.deprecated_enable_flatten_nested = false;
    parse_cfg.parse_to = ParseConfig::ParseTo::OnlySubcolumns;
    Status st =
            parse_and_materialize_variant_columns(block, std::vector<uint32_t> {0}, {parse_cfg});
    EXPECT_TRUE(st.ok()) << st.to_string();

    const auto& alias_nullable = assert_cast<const ColumnNullable&>(*nullable_alias);
    const auto& alias_variant =
            assert_cast<const ColumnVariant&>(alias_nullable.get_nested_column());
    EXPECT_TRUE(alias_variant.is_scalar_variant());
    EXPECT_EQ(alias_variant.get_root_type()->get_primitive_type(), PrimitiveType::TYPE_INT);

    EXPECT_TRUE(block.get_by_position(0).column->is_nullable());
}

TEST(VariantUtilTest, ParseVariantColumns_DocModeBinaryToSubcolumns) {
    const std::vector<std::string_view> jsons = {
            R"({"a":1,"b":"x"})", //
            R"({"a":2,"b":"y"})", //
    };

    // Build a doc-mode ColumnVariant: Only root in subcolumns, others stored in doc snapshot column.
    auto variant = ColumnVariant::create(0, true);
    auto json_col = _make_json_column(jsons);
    ParseConfig cfg;
    cfg.deprecated_enable_flatten_nested = false;
    cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
    parse_json_to_variant(*variant, *json_col, cfg);
    Block block;
    block.insert({variant->get_ptr(), std::make_shared<DataTypeVariant>(0, true), "v"});

    ParseConfig parse_cfg;
    parse_cfg.deprecated_enable_flatten_nested = false;
    parse_cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
    Status st =
            parse_and_materialize_variant_columns(block, std::vector<uint32_t> {0}, {parse_cfg});
    EXPECT_TRUE(st.ok()) << st.to_string();

    const auto& out = assert_cast<const ColumnVariant&>(*block.get_by_position(0).column);
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
    auto variant = ColumnVariant::create(0, true);
    auto json_col = _make_json_column(jsons);

    ParseConfig cfg;
    cfg.deprecated_enable_flatten_nested = false;
    cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
    parse_json_to_variant(*variant, *json_col, cfg);
    Block block;
    block.insert({variant->get_ptr(), std::make_shared<DataTypeVariant>(0, true), "v"});

    ParseConfig parse_cfg;
    parse_cfg.deprecated_enable_flatten_nested = false;
    parse_cfg.parse_to = ParseConfig::ParseTo::OnlyDocValueColumn;
    Status st =
            parse_and_materialize_variant_columns(block, std::vector<uint32_t> {0}, {parse_cfg});
    EXPECT_TRUE(st.ok()) << st.to_string();
}

} // namespace doris::variant_util
