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

#include <initializer_list>
#include <string>
#include <string_view>
#include <vector>

#include "common/config.h"
#include "core/block/block.h"
#include "core/column/column_complex.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_variant.h"
#include "core/field.h"
#include "core/value/bitmap_value.h"
#include "core/value/jsonb_value.h"
#include "exec/common/variant_util.h"
#include "gtest/gtest.h"
#include "storage/segment/vertical_segment_writer.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"

namespace doris::variant_util {

namespace {

constexpr uint64_t TEST_VARIANT_PATCH_PATH_MARKER_MASK = 1ULL << 63;
constexpr uint64_t TEST_VARIANT_PATCH_PATH_MARKER_CLASS_SHIFT = 62;
constexpr uint64_t TEST_VARIANT_PATCH_PATH_MARKER_BYTE_BITS = 8;
constexpr uint64_t TEST_VARIANT_PATCH_PATH_MARKER_POS_BITS = 12;
constexpr uint64_t TEST_VARIANT_PATCH_PATH_MARKER_INDEX_BITS = 11;
constexpr uint64_t TEST_VARIANT_PATCH_PATH_MARKER_POS_SHIFT =
        TEST_VARIANT_PATCH_PATH_MARKER_BYTE_BITS;
constexpr uint64_t TEST_VARIANT_PATCH_PATH_MARKER_INDEX_SHIFT =
        TEST_VARIANT_PATCH_PATH_MARKER_POS_SHIFT + TEST_VARIANT_PATCH_PATH_MARKER_POS_BITS;
constexpr uint64_t TEST_VARIANT_PATCH_PATH_MARKER_UID_SHIFT =
        TEST_VARIANT_PATCH_PATH_MARKER_INDEX_SHIFT + TEST_VARIANT_PATCH_PATH_MARKER_INDEX_BITS;
constexpr uint64_t TEST_VARIANT_PATCH_PATH_MARKER_BYTE_MASK =
        (1ULL << TEST_VARIANT_PATCH_PATH_MARKER_BYTE_BITS) - 1;

static uint64_t _test_variant_patch_path_length_marker(int32_t variant_col_unique_id,
                                                       uint64_t path_index, uint64_t length) {
    return TEST_VARIANT_PATCH_PATH_MARKER_MASK |
           (static_cast<uint64_t>(variant_col_unique_id)
            << TEST_VARIANT_PATCH_PATH_MARKER_UID_SHIFT) |
           (path_index << TEST_VARIANT_PATCH_PATH_MARKER_INDEX_SHIFT) | length;
}

static uint64_t _test_variant_patch_path_byte_marker(int32_t variant_col_unique_id,
                                                     uint64_t path_index, uint64_t byte_pos,
                                                     uint8_t byte) {
    return TEST_VARIANT_PATCH_PATH_MARKER_MASK |
           (1ULL << TEST_VARIANT_PATCH_PATH_MARKER_CLASS_SHIFT) |
           (static_cast<uint64_t>(variant_col_unique_id)
            << TEST_VARIANT_PATCH_PATH_MARKER_UID_SHIFT) |
           (path_index << TEST_VARIANT_PATCH_PATH_MARKER_INDEX_SHIFT) |
           (byte_pos << TEST_VARIANT_PATCH_PATH_MARKER_POS_SHIFT) | byte;
}

static void _add_test_encoded_patch_path(BitmapValue* markers, int32_t variant_col_unique_id,
                                         uint64_t path_index, std::string_view encoded_path) {
    markers->add(_test_variant_patch_path_length_marker(variant_col_unique_id, path_index,
                                                        encoded_path.size()));
    for (uint64_t i = 0; i < encoded_path.size(); ++i) {
        markers->add(_test_variant_patch_path_byte_marker(
                variant_col_unique_id, path_index, i,
                static_cast<uint8_t>(static_cast<unsigned char>(encoded_path[i]))));
    }
}

static std::string _test_encode_single_part_path(std::string_view key) {
    std::string encoded;
    auto append_u32 = [&encoded](uint32_t value) {
        encoded.push_back(static_cast<char>(value & 0xFF));
        encoded.push_back(static_cast<char>((value >> 8) & 0xFF));
        encoded.push_back(static_cast<char>((value >> 16) & 0xFF));
        encoded.push_back(static_cast<char>((value >> 24) & 0xFF));
    };
    append_u32(1);
    append_u32(static_cast<uint32_t>(key.size()));
    encoded.append(key.data(), key.size());
    encoded.push_back(0);
    encoded.push_back(0);
    return encoded;
}

static ColumnPtr _make_nullable_variant_column(ColumnVariant::MutablePtr variant,
                                               bool is_null = false) {
    auto null_map = ColumnUInt8::create(variant->size(), is_null ? 1 : 0);
    return ColumnNullable::create(std::move(variant), std::move(null_map));
}

static ColumnString::MutablePtr _make_json_column(const std::vector<std::string_view>& rows) {
    auto col = ColumnString::create();
    for (const auto& s : rows) {
        col->insert_data(s.data(), s.size());
    }
    return col;
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

static ColumnVariant::MutablePtr _make_variant_column(
        const std::vector<std::string_view>& rows, bool doc_mode = false,
        ParseConfig::ParseTo parse_to = ParseConfig::ParseTo::OnlySubcolumns,
        bool record_empty_object_path = false) {
    auto variant = ColumnVariant::create(0, doc_mode);
    auto json_col = _make_json_column(rows);
    ParseConfig cfg;
    cfg.deprecated_enable_flatten_nested = false;
    cfg.parse_to = parse_to;
    cfg.record_empty_object_path = record_empty_object_path;
    parse_json_to_variant(*variant, *json_col, cfg);
    variant->finalize();
    return variant;
}

static ColumnVariant::MutablePtr _make_raw_string_variant_column(std::string_view value) {
    auto variant = ColumnVariant::create(0, false);
    doris::VariantUtil::insert_root_scalar_field(*variant,
                                                 Field::create_field<TYPE_STRING>(String(value)));
    variant->finalize();
    return variant;
}

static ColumnVariant::MutablePtr _make_root_array_variant_column() {
    auto variant = ColumnVariant::create(0, false);
    doris::VariantUtil::insert_root_scalar_field(*variant,
                                                 doris::VariantUtil::get_field("array_int"));
    variant->finalize();
    return variant;
}

static ColumnVariant::MutablePtr _make_root_jsonb_variant_column(std::string_view value) {
    JsonBinaryValue jsonb_value;
    Status st = jsonb_value.from_json_string(value.data(), value.size());
    EXPECT_TRUE(st.ok()) << st.to_string();

    auto variant = ColumnVariant::create(0, false);
    doris::VariantUtil::insert_root_scalar_field(
            *variant,
            Field::create_field<TYPE_JSONB>(JsonbField(jsonb_value.value(), jsonb_value.size())));
    variant->finalize();
    return variant;
}

static std::string _make_nested_json(std::string_view key, int depth, std::string_view leaf) {
    std::string json;
    for (int i = 0; i < depth; ++i) {
        json += "{\"";
        json += key;
        json += "\":";
    }
    json += leaf;
    for (int i = 0; i < depth; ++i) {
        json += "}";
    }
    return json;
}

static FieldWithDataType _get_variant_field_at(const ColumnVariant& variant, size_t row_num,
                                               std::string_view path) {
    Field field;
    variant.get(row_num, field);
    EXPECT_EQ(field.get_type(), PrimitiveType::TYPE_VARIANT);
    const auto& object = field.get<TYPE_VARIANT>();
    auto it = path.empty() ? object.find(PathInData()) : object.find(PathInData(path));
    EXPECT_NE(it, object.end()) << path;
    if (it == object.end()) {
        return {};
    }
    return it->second;
}

static PathInData _make_path(std::initializer_list<std::string_view> keys) {
    PathInData::Parts parts;
    parts.reserve(keys.size());
    for (std::string_view key : keys) {
        parts.emplace_back(key, false, 0);
    }
    return PathInData(parts);
}

} // namespace

static FieldWithDataType _get_variant_field_by_path(const ColumnVariant& variant,
                                                    const PathInData& path) {
    Field field;
    variant.get(0, field);
    EXPECT_EQ(field.get_type(), PrimitiveType::TYPE_VARIANT);
    const auto& object = field.get<TYPE_VARIANT>();
    auto it = object.find(path);
    EXPECT_NE(it, object.end()) << path.get_path();
    if (it == object.end()) {
        return {};
    }
    return it->second;
}

static FieldWithDataType _get_variant_field(const ColumnVariant& variant, std::string_view path) {
    return _get_variant_field_at(variant, 0, path);
}

static void _expect_no_variant_path(const ColumnVariant& variant, std::string_view path) {
    Field row;
    variant.get(0, row);
    if (row.get_type() == PrimitiveType::TYPE_NULL) {
        return;
    }
    ASSERT_EQ(row.get_type(), PrimitiveType::TYPE_VARIANT);
    const auto& object = row.get<TYPE_VARIANT>();
    EXPECT_FALSE(object.contains(PathInData(path))) << path;
}

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

    auto variant = ColumnVariant::create(0, false);
    doris::VariantUtil::insert_root_scalar_field(
            *variant, Field::create_field<TYPE_STRING>(String(R"({"a":1})")));
    doris::VariantUtil::insert_root_scalar_field(
            *variant, Field::create_field<TYPE_STRING>(String(R"({"a":2})")));

    Block block;
    block.insert({variant->get_ptr(), std::make_shared<DataTypeVariant>(0, false), "v"});

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

TEST(VariantUtilTest, ParseVariantColumns_RejectsJsonNullWhenConfigured) {
    auto variant = ColumnVariant::create(0, false);
    doris::VariantUtil::insert_root_scalar_field(
            *variant, Field::create_field<TYPE_STRING>(String(R"({"a":null})")));

    Block block;
    block.insert({variant->get_ptr(), std::make_shared<DataTypeVariant>(0, false), "v"});

    ParseConfig parse_cfg;
    parse_cfg.deprecated_enable_flatten_nested = false;
    parse_cfg.reject_json_null_value = true;
    Status st =
            parse_and_materialize_variant_columns(block, std::vector<uint32_t> {0}, {parse_cfg});
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find(
                      "VARIANT flexible partial update does not support JSON null patch values"),
              std::string::npos);
}

TEST(VariantUtilTest, ParseVariantColumns_RejectsJsonNullInsideNestedArrayJsonbWhenConfigured) {
    auto variant = ColumnVariant::create(0, false);
    doris::VariantUtil::insert_root_scalar_field(
            *variant, Field::create_field<TYPE_STRING>(String(R"({"a":[{"b":null}]})")));

    Block block;
    block.insert({variant->get_ptr(), std::make_shared<DataTypeVariant>(0, false), "v"});

    ParseConfig parse_cfg;
    parse_cfg.deprecated_enable_flatten_nested = false;
    parse_cfg.reject_json_null_value = true;
    Status st =
            parse_and_materialize_variant_columns(block, std::vector<uint32_t> {0}, {parse_cfg});
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find(
                      "VARIANT flexible partial update does not support JSON null patch values"),
              std::string::npos);
}

TEST(VariantUtilTest, ParseVariantColumns_RecordsEmptyObjectPathWhenConfigured) {
    auto variant = _make_variant_column({R"({"a":{}})"}, false,
                                        ParseConfig::ParseTo::OnlySubcolumns, true);

    auto a = _get_variant_field(*variant, "a");
    ASSERT_EQ(a.field.get_type(), PrimitiveType::TYPE_JSONB);
    const auto& jsonb = a.field.get<TYPE_JSONB>();
    EXPECT_EQ(JsonbToJson::jsonb_to_json_string(jsonb.get_value(), jsonb.get_size()), "{}");
}

TEST(VariantUtilTest, ParseVariantColumns_FlexiblePatchRecordsEmptyObjectPath) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* c = schema_pb.add_column();
    c->set_unique_id(1);
    c->set_name("v");
    c->set_type("VARIANT");
    c->set_is_key(false);
    c->set_is_nullable(false);
    c->set_variant_enable_doc_mode(false);

    TabletSchema tablet_schema;
    tablet_schema.init_from_pb(schema_pb);

    auto variant = ColumnVariant::create(0, false);
    doris::VariantUtil::insert_root_scalar_field(
            *variant, Field::create_field<TYPE_STRING>(String(R"({"a":{}})")));

    Block block;
    block.insert({variant->get_ptr(), std::make_shared<DataTypeVariant>(0, false), "v"});

    Status st = parse_and_materialize_variant_columns(block, tablet_schema,
                                                      std::vector<uint32_t> {0}, true);
    EXPECT_TRUE(st.ok()) << st.to_string();

    const auto& out = assert_cast<const ColumnVariant&>(*block.get_by_position(0).column);
    auto a = _get_variant_field(out, "a");
    ASSERT_EQ(a.field.get_type(), PrimitiveType::TYPE_JSONB);
    const auto& jsonb = a.field.get<TYPE_JSONB>();
    EXPECT_EQ(JsonbToJson::jsonb_to_json_string(jsonb.get_value(), jsonb.get_size()), "{}");
}

TEST(VariantUtilTest, ParseVariantColumns_TabletSchemaNoVariantIsNoop) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* c = schema_pb.add_column();
    c->set_unique_id(1);
    c->set_name("k");
    c->set_type("INT");
    c->set_is_key(false);
    c->set_is_nullable(false);

    TabletSchema tablet_schema;
    tablet_schema.init_from_pb(schema_pb);

    auto int_col = ColumnInt32::create();
    int_col->insert_value(7);
    Block block;
    block.insert({int_col->get_ptr(), std::make_shared<DataTypeInt32>(), "k"});

    Status st = parse_and_materialize_variant_columns(block, tablet_schema,
                                                      std::vector<uint32_t> {0}, true);
    ASSERT_TRUE(st.ok()) << st.to_string();
    const auto& out = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    EXPECT_EQ(out.get_element(0), 7);
}

TEST(VariantUtilTest, ParseVariantColumns_NonStringScalarRootKeepsVariant) {
    auto variant = ColumnVariant::create(0, false);
    doris::VariantUtil::insert_root_scalar_field(*variant, Field::create_field<TYPE_INT>(7));

    Block block;
    block.insert({variant->get_ptr(), std::make_shared<DataTypeVariant>(0, false), "v"});

    ParseConfig parse_cfg;
    parse_cfg.deprecated_enable_flatten_nested = false;
    Status st =
            parse_and_materialize_variant_columns(block, std::vector<uint32_t> {0}, {parse_cfg});
    ASSERT_TRUE(st.ok()) << st.to_string();

    const auto& out = assert_cast<const ColumnVariant&>(*block.get_by_position(0).column);
    Field field;
    out.get(0, field);
    EXPECT_EQ(field.get_type(), PrimitiveType::TYPE_VARIANT);
}

TEST(VariantUtilTest, ParseVariantColumns_TabletSchemaDocModeUsesDocValueColumn) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* c = schema_pb.add_column();
    c->set_unique_id(1);
    c->set_name("v");
    c->set_type("VARIANT");
    c->set_is_key(false);
    c->set_is_nullable(false);
    c->set_variant_enable_doc_mode(true);

    TabletSchema tablet_schema;
    tablet_schema.init_from_pb(schema_pb);

    auto variant = ColumnVariant::create(0, true);
    doris::VariantUtil::insert_root_scalar_field(
            *variant, Field::create_field<TYPE_STRING>(String(R"({"a":1})")));

    Block block;
    block.insert({variant->get_ptr(), std::make_shared<DataTypeVariant>(0, true), "v"});

    Status st = parse_and_materialize_variant_columns(block, tablet_schema,
                                                      std::vector<uint32_t> {0}, false);
    ASSERT_TRUE(st.ok()) << st.to_string();

    const auto& out = assert_cast<const ColumnVariant&>(*block.get_by_position(0).column);
    auto docs_subcolumns = materialize_docs_to_subcolumns_map(out);
    ASSERT_TRUE(docs_subcolumns.contains("a"));
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

TEST(VariantUtilTest, MergeVariantPatch_MergesObjectPaths) {
    auto old_variant = _make_variant_column({R"({"a":1,"c":3,"nested":{"x":1}})"});
    auto patch_variant = _make_variant_column({R"({"a":10,"b":20,"nested":{"y":2}})"});
    auto merged_variant = ColumnVariant::create(0, false);

    Status st = merge_variant_patch(*old_variant, 0, *patch_variant, 0, *merged_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();
    merged_variant->finalize();

    auto a = _get_variant_field(*merged_variant, "a");
    EXPECT_EQ(a.field.get<TYPE_BIGINT>(), 10);
    auto b = _get_variant_field(*merged_variant, "b");
    EXPECT_EQ(b.field.get<TYPE_BIGINT>(), 20);
    auto c = _get_variant_field(*merged_variant, "c");
    EXPECT_EQ(c.field.get<TYPE_BIGINT>(), 3);
    auto nested_x = _get_variant_field(*merged_variant, "nested.x");
    EXPECT_EQ(nested_x.field.get<TYPE_BIGINT>(), 1);
    auto nested_y = _get_variant_field(*merged_variant, "nested.y");
    EXPECT_EQ(nested_y.field.get<TYPE_BIGINT>(), 2);
}

TEST(VariantUtilTest, MergeVariantPatch_RejectsRawStringRoot) {
    auto old_variant = _make_variant_column({R"({"a":1})"});
    auto patch_variant = _make_raw_string_variant_column(R"({"b":2})");
    auto merged_variant = ColumnVariant::create(0, false);

    Status st = merge_variant_patch(*old_variant, 0, *patch_variant, 0, *merged_variant);
    EXPECT_FALSE(st.ok());
}

TEST(VariantUtilTest, MergeVariantPatch_RejectsRawStringScalarRoot) {
    auto old_variant = _make_variant_column({R"({"a":1})"});
    auto patch_variant = _make_raw_string_variant_column("plain text");
    auto merged_variant = ColumnVariant::create(0, false);

    Status st = merge_variant_patch(*old_variant, 0, *patch_variant, 0, *merged_variant);
    EXPECT_FALSE(st.ok());
}

TEST(VariantUtilTest, MergeVariantPatch_MergesRootJsonbObjectBase) {
    auto old_variant = _make_root_jsonb_variant_column(R"({"a":1})");
    auto patch_variant = _make_variant_column({R"({"b":2})"});
    auto merged_variant = ColumnVariant::create(0, false);

    Status st = merge_variant_patch(*old_variant, 0, *patch_variant, 0, *merged_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();
    merged_variant->finalize();

    auto a = _get_variant_field(*merged_variant, "a");
    EXPECT_EQ(a.field.get<TYPE_BIGINT>(), 1);
    auto b = _get_variant_field(*merged_variant, "b");
    EXPECT_EQ(b.field.get<TYPE_BIGINT>(), 2);
}

TEST(VariantUtilTest, MergeVariantPatch_MergesRootJsonbObjectPatch) {
    auto old_variant = _make_variant_column({R"({"a":1})"});
    auto patch_variant = _make_root_jsonb_variant_column(R"({"b":2})");
    auto merged_variant = ColumnVariant::create(0, false);

    Status st = merge_variant_patch(*old_variant, 0, *patch_variant, 0, *merged_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();
    merged_variant->finalize();

    auto a = _get_variant_field(*merged_variant, "a");
    EXPECT_EQ(a.field.get<TYPE_BIGINT>(), 1);
    auto b = _get_variant_field(*merged_variant, "b");
    EXPECT_EQ(b.field.get<TYPE_BIGINT>(), 2);
}

TEST(VariantUtilTest, MergeVariantPatch_RejectsRootJsonbObjectPatchWithNull) {
    constexpr int32_t variant_col_unique_id = 100;
    auto old_variant = _make_variant_column({R"({"a":1})"});
    auto patch_variant = _make_root_jsonb_variant_column(R"({"a":null})");
    auto merged_variant = ColumnVariant::create(0, false);

    BitmapValue patch_path_markers;
    Status st =
            mark_variant_patch_paths(*patch_variant, 0, variant_col_unique_id, &patch_path_markers);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find(
                      "VARIANT flexible partial update does not support JSON null patch values"),
              std::string::npos);

    st = merge_variant_patch(*old_variant, 0, *patch_variant, 0, *merged_variant);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find(
                      "VARIANT flexible partial update does not support JSON null patch values"),
              std::string::npos);
}

TEST(VariantUtilTest, MergeVariantPatch_ReplacesConflictingPaths) {
    {
        auto old_variant = _make_variant_column({R"({"a":{"x":1},"b":2})"});
        auto patch_variant = _make_variant_column({R"({"a":3})"});
        auto merged_variant = ColumnVariant::create(0, false);

        Status st = merge_variant_patch(*old_variant, 0, *patch_variant, 0, *merged_variant);
        ASSERT_TRUE(st.ok()) << st.to_string();
        merged_variant->finalize();

        auto a = _get_variant_field(*merged_variant, "a");
        EXPECT_EQ(a.field.get<TYPE_BIGINT>(), 3);
        auto b = _get_variant_field(*merged_variant, "b");
        EXPECT_EQ(b.field.get<TYPE_BIGINT>(), 2);
        _expect_no_variant_path(*merged_variant, "a.x");
    }
    {
        auto old_variant = _make_variant_column({R"({"a":3,"b":2})"});
        auto patch_variant = _make_variant_column({R"({"a":{"y":4}})"});
        auto merged_variant = ColumnVariant::create(0, false);

        Status st = merge_variant_patch(*old_variant, 0, *patch_variant, 0, *merged_variant);
        ASSERT_TRUE(st.ok()) << st.to_string();
        merged_variant->finalize();

        auto a_y = _get_variant_field(*merged_variant, "a.y");
        EXPECT_EQ(a_y.field.get<TYPE_BIGINT>(), 4);
        auto b = _get_variant_field(*merged_variant, "b");
        EXPECT_EQ(b.field.get<TYPE_BIGINT>(), 2);
        _expect_no_variant_path(*merged_variant, "a");
    }
    {
        auto old_variant = _make_variant_column({R"({"a":{"x":1},"b":2})"});
        auto patch_variant = _make_variant_column({R"({"a":{}})"}, false,
                                                  ParseConfig::ParseTo::OnlySubcolumns, true);
        auto merged_variant = ColumnVariant::create(0, false);

        Status st = merge_variant_patch(*old_variant, 0, *patch_variant, 0, *merged_variant);
        ASSERT_TRUE(st.ok()) << st.to_string();
        merged_variant->finalize();

        auto a = _get_variant_field(*merged_variant, "a");
        ASSERT_EQ(a.field.get_type(), PrimitiveType::TYPE_JSONB);
        const auto& jsonb = a.field.get<TYPE_JSONB>();
        EXPECT_EQ(JsonbToJson::jsonb_to_json_string(jsonb.get_value(), jsonb.get_size()), "{}");
        auto b = _get_variant_field(*merged_variant, "b");
        EXPECT_EQ(b.field.get<TYPE_BIGINT>(), 2);
        _expect_no_variant_path(*merged_variant, "a.x");
    }
}

TEST(VariantUtilTest, MergeVariantPatch_RejectsRootArray) {
    auto old_variant = _make_variant_column({R"({"a":1})"});
    auto patch_variant = _make_root_array_variant_column();
    auto merged_variant = ColumnVariant::create(0, false);

    Status st = merge_variant_patch(*old_variant, 0, *patch_variant, 0, *merged_variant);
    EXPECT_FALSE(st.ok());
}

TEST(VariantUtilTest, MergeVariantPatch_RejectsNonObjectOldRootValues) {
    constexpr int32_t variant_col_unique_id = 100;
    auto patch_variant = _make_variant_column({R"({"a":2})"});
    BitmapValue patch_path_markers;
    Status st =
            mark_variant_patch_paths(*patch_variant, 0, variant_col_unique_id, &patch_path_markers);
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto expect_reject_old_value = [&](const ColumnVariant& old_variant) {
        auto merged_variant = ColumnVariant::create(0, false);
        Status merge_st = merge_variant_patch(old_variant, 0, *patch_variant, 0, *merged_variant);
        EXPECT_FALSE(merge_st.ok());
        EXPECT_NE(merge_st.to_string().find("VARIANT flexible partial update only supports "
                                            "patching JSON object old values"),
                  std::string::npos);

        merged_variant = ColumnVariant::create(0, false);
        merge_st = merge_variant_patch_by_path_markers(old_variant, 0, *patch_variant, 0,
                                                       variant_col_unique_id, patch_path_markers,
                                                       false, *merged_variant);
        EXPECT_FALSE(merge_st.ok());
        EXPECT_NE(merge_st.to_string().find("VARIANT flexible partial update only supports "
                                            "patching JSON object old values"),
                  std::string::npos);
    };

    expect_reject_old_value(*_make_raw_string_variant_column("plain text"));
    expect_reject_old_value(*_make_root_array_variant_column());
    expect_reject_old_value(*_make_root_jsonb_variant_column("null"));
}

TEST(VariantUtilTest, MergeVariantPatch_RejectsDocModeObjectPaths) {
    auto old_variant =
            _make_variant_column({R"({"a":1})"}, true, ParseConfig::ParseTo::OnlyDocValueColumn);
    auto patch_variant =
            _make_variant_column({R"({"b":2})"}, true, ParseConfig::ParseTo::OnlyDocValueColumn);
    auto merged_variant = ColumnVariant::create(0, true);

    Status st = merge_variant_patch(*old_variant, 0, *patch_variant, 0, *merged_variant);
    EXPECT_FALSE(st.ok());
}

TEST(VariantUtilTest, MergeVariantPatch_HandlesNullableVariantColumns) {
    auto old_variant = _make_nullable_variant_column(_make_variant_column({R"({"a":1,"b":3})"}));
    auto patch_variant = _make_nullable_variant_column(_make_variant_column({R"({"a":2})"}));
    auto dst_nested = ColumnVariant::create(0, false);
    auto* dst_variant = dst_nested.get();
    auto dst_variant_nullable =
            ColumnNullable::create(std::move(dst_nested), ColumnUInt8::create());

    Status st = merge_variant_patch(*old_variant, 0, *patch_variant, 0, *dst_variant_nullable);
    ASSERT_TRUE(st.ok()) << st.to_string();
    dst_variant->finalize();

    auto a = _get_variant_field(*dst_variant, "a");
    EXPECT_EQ(a.field.get<TYPE_BIGINT>(), 2);
    auto b = _get_variant_field(*dst_variant, "b");
    EXPECT_EQ(b.field.get<TYPE_BIGINT>(), 3);
}

TEST(VariantUtilTest, MergeVariantPatch_RejectsNullPatchRows) {
    constexpr int32_t variant_col_unique_id = 100;
    auto old_variant = _make_variant_column({R"({"a":1})"});
    auto null_patch = _make_nullable_variant_column(_make_variant_column({R"({"a":2})"}), true);

    BitmapValue patch_path_markers;
    Status st =
            mark_variant_patch_paths(*null_patch, 0, variant_col_unique_id, &patch_path_markers);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find(
                      "VARIANT flexible partial update only supports JSON object patch values"),
              std::string::npos);

    auto merged_variant = ColumnVariant::create(0, false);
    st = merge_variant_patch(*old_variant, 0, *null_patch, 0, *merged_variant);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find(
                      "VARIANT flexible partial update only supports JSON object patch values"),
              std::string::npos);

    st = merge_variant_patch_by_path_markers(*old_variant, 0, *null_patch, 0, variant_col_unique_id,
                                             patch_path_markers, false, *merged_variant);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find(
                      "VARIANT flexible partial update only supports JSON object patch values"),
              std::string::npos);
}

TEST(VariantUtilTest, MergeVariantPatchByPathMarkers_PreservesConcurrentPaths) {
    constexpr int32_t variant_col_unique_id = 100;
    auto latest_old = _make_variant_column({R"({"a":1,"b":3})"});
    auto flushed_full_value = _make_variant_column({R"({"a":2,"b":1})"});
    auto original_patch = _make_variant_column({R"({"a":2})"});
    BitmapValue patch_path_markers;
    Status st = mark_variant_patch_paths(*original_patch, 0, variant_col_unique_id,
                                         &patch_path_markers);
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto merged_variant = ColumnVariant::create(0, false);
    st = merge_variant_patch_by_path_markers(*latest_old, 0, *flushed_full_value, 0,
                                             variant_col_unique_id, patch_path_markers, false,
                                             *merged_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();
    merged_variant->finalize();

    auto a = _get_variant_field(*merged_variant, "a");
    EXPECT_EQ(a.field.get<TYPE_BIGINT>(), 2);
    auto b = _get_variant_field(*merged_variant, "b");
    EXPECT_EQ(b.field.get<TYPE_BIGINT>(), 3);
}

TEST(VariantUtilTest, MergeVariantPatchByPathMarkers_EmptyMarkersDropFlushedPatchPaths) {
    constexpr int32_t variant_col_unique_id = 100;
    auto latest_old = _make_variant_column({R"({"a":1,"b":3})"});
    auto flushed_full_value = _make_variant_column({R"({"a":2,"b":1})"});
    BitmapValue patch_path_markers;

    auto merged_variant = ColumnVariant::create(0, false);
    Status st = merge_variant_patch_by_path_markers(*latest_old, 0, *flushed_full_value, 0,
                                                    variant_col_unique_id, patch_path_markers,
                                                    false, *merged_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();
    merged_variant->finalize();

    auto a = _get_variant_field(*merged_variant, "a");
    EXPECT_EQ(a.field.get<TYPE_BIGINT>(), 1);
    auto b = _get_variant_field(*merged_variant, "b");
    EXPECT_EQ(b.field.get<TYPE_BIGINT>(), 3);
}

TEST(VariantUtilTest, MergeVariantPatchPathMarkers_IntersectsNonVariantSkipBits) {
    BitmapValue left;
    left.add(1);
    left.add(2);
    BitmapValue right;
    right.add(2);
    right.add(3);

    BitmapValue merged;
    Status st = merge_variant_patch_path_markers(left, right, &merged);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(merged.contains(1));
    EXPECT_TRUE(merged.contains(2));
    EXPECT_FALSE(merged.contains(3));
}

TEST(VariantUtilTest, MergeVariantPatchByPathMarkers_RejectsCorruptMarkers) {
    constexpr int32_t variant_col_unique_id = 100;
    auto latest_old = _make_variant_column({R"({"a":1})"});
    auto flushed_full_value = _make_variant_column({R"({"a":2})"});

    auto expect_merge_fails = [&](const BitmapValue& patch_path_markers,
                                  std::string_view expected) {
        auto merged_variant = ColumnVariant::create(0, false);
        Status st = merge_variant_patch_by_path_markers(*latest_old, 0, *flushed_full_value, 0,
                                                        variant_col_unique_id, patch_path_markers,
                                                        false, *merged_variant);
        EXPECT_FALSE(st.ok());
        EXPECT_NE(st.to_string().find(expected), std::string::npos) << st.to_string();
    };

    {
        BitmapValue patch_path_markers;
        patch_path_markers.add(
                _test_variant_patch_path_length_marker(variant_col_unique_id, 0, 4097));
        expect_merge_fails(patch_path_markers, "Invalid VARIANT patch path marker length");
    }
    {
        BitmapValue patch_path_markers;
        patch_path_markers.add(_test_variant_patch_path_byte_marker(variant_col_unique_id, 0, 0,
                                                                    static_cast<uint8_t>('a')));
        expect_merge_fails(patch_path_markers, "VARIANT patch path marker byte without length");
    }
    {
        BitmapValue patch_path_markers;
        patch_path_markers.add(_test_variant_patch_path_length_marker(variant_col_unique_id, 0, 1));
        patch_path_markers.add(_test_variant_patch_path_byte_marker(variant_col_unique_id, 0, 2,
                                                                    static_cast<uint8_t>('a')));
        expect_merge_fails(patch_path_markers, "VARIANT patch path marker byte exceeds length");
    }
    {
        BitmapValue patch_path_markers;
        std::string encoded_path(4, '\0');
        _add_test_encoded_patch_path(&patch_path_markers, variant_col_unique_id, 0, encoded_path);
        expect_merge_fails(patch_path_markers, "Invalid VARIANT patch path marker part count");
    }
    {
        BitmapValue patch_path_markers;
        std::string encoded_path;
        encoded_path.push_back(1);
        encoded_path.push_back(0);
        encoded_path.push_back(0);
        encoded_path.push_back(0);
        _add_test_encoded_patch_path(&patch_path_markers, variant_col_unique_id, 0, encoded_path);
        expect_merge_fails(patch_path_markers, "Invalid VARIANT patch path marker part payload");
    }
    {
        BitmapValue patch_path_markers;
        std::string encoded_path = _test_encode_single_part_path("a");
        encoded_path.push_back('x');
        _add_test_encoded_patch_path(&patch_path_markers, variant_col_unique_id, 0, encoded_path);
        expect_merge_fails(patch_path_markers, "Trailing bytes in VARIANT patch path marker");
    }
    {
        BitmapValue patch_path_markers;
        const std::string encoded_path = _test_encode_single_part_path("a");
        _add_test_encoded_patch_path(&patch_path_markers, variant_col_unique_id, 0, encoded_path);
        patch_path_markers.add(_test_variant_patch_path_length_marker(variant_col_unique_id, 0,
                                                                      encoded_path.size() + 1));
        expect_merge_fails(patch_path_markers, "Conflicting VARIANT patch path marker length");
    }
    {
        BitmapValue patch_path_markers;
        _add_test_encoded_patch_path(&patch_path_markers, variant_col_unique_id, 0,
                                     _test_encode_single_part_path("a"));
        for (uint64_t marker : patch_path_markers) {
            if ((marker & (1ULL << TEST_VARIANT_PATCH_PATH_MARKER_CLASS_SHIFT)) != 0) {
                uint8_t byte = marker & TEST_VARIANT_PATCH_PATH_MARKER_BYTE_MASK;
                patch_path_markers.add((marker & ~TEST_VARIANT_PATCH_PATH_MARKER_BYTE_MASK) |
                                       static_cast<uint8_t>(byte + 1));
                break;
            }
        }
        expect_merge_fails(patch_path_markers, "Conflicting VARIANT patch path marker byte");
    }
    {
        BitmapValue patch_path_markers;
        _add_test_encoded_patch_path(&patch_path_markers, variant_col_unique_id, 0,
                                     _test_encode_single_part_path("a"));
        for (uint64_t marker : patch_path_markers) {
            if ((marker & (1ULL << TEST_VARIANT_PATCH_PATH_MARKER_CLASS_SHIFT)) != 0) {
                patch_path_markers.remove(marker);
                break;
            }
        }
        expect_merge_fails(patch_path_markers, "Incomplete VARIANT patch path marker");
    }
}

TEST(VariantUtilTest, MarkVariantPatchPaths_RejectsRootArray) {
    constexpr int32_t variant_col_unique_id = 100;
    auto original_patch = _make_root_array_variant_column();
    BitmapValue patch_path_markers;
    Status st = mark_variant_patch_paths(*original_patch, 0, variant_col_unique_id,
                                         &patch_path_markers);
    EXPECT_FALSE(st.ok());
}

TEST(VariantUtilTest, MarkVariantPatchPaths_MarksRootJsonbObjectPatch) {
    constexpr int32_t variant_col_unique_id = 100;
    auto original_patch = _make_root_jsonb_variant_column(R"({"a":2})");
    BitmapValue patch_path_markers;
    Status st = mark_variant_patch_paths(*original_patch, 0, variant_col_unique_id,
                                         &patch_path_markers);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_GT(patch_path_markers.cardinality(), 0);
}

TEST(VariantUtilTest, MarkVariantPatchPaths_AllowsDeepPathWithinMarkerCapacity) {
    constexpr int32_t variant_col_unique_id = 100;
    const std::string key(200, 'a');
    const std::string json = _make_nested_json(key, 6, "1");
    auto original_patch = _make_variant_column({std::string_view(json)});

    BitmapValue patch_path_markers;
    Status st = mark_variant_patch_paths(*original_patch, 0, variant_col_unique_id,
                                         &patch_path_markers);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_GT(patch_path_markers.cardinality(), 1020);
}

TEST(VariantUtilTest, MarkVariantPatchPaths_RejectsPathBeyondMarkerCapacity) {
    constexpr int32_t variant_col_unique_id = 100;
    const std::string key(250, 'a');
    const std::string json = _make_nested_json(key, 20, "1");
    auto original_patch = _make_variant_column({std::string_view(json)});

    BitmapValue patch_path_markers;
    Status st = mark_variant_patch_paths(*original_patch, 0, variant_col_unique_id,
                                         &patch_path_markers);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find(
                      "VARIANT flexible partial update encoded patch path exceeds 4096 bytes"),
              std::string::npos);
}

TEST(VariantUtilTest, MarkVariantPatchPaths_RejectsTooManyPaths) {
    constexpr int32_t variant_col_unique_id = 100;
    std::string json = "{";
    for (int i = 0; i < 257; ++i) {
        if (i != 0) {
            json += ",";
        }
        json += "\"k";
        json += std::to_string(i);
        json += "\":";
        json += std::to_string(i);
    }
    json += "}";
    auto original_patch = _make_variant_column({std::string_view(json)});

    BitmapValue patch_path_markers;
    Status st = mark_variant_patch_paths(*original_patch, 0, variant_col_unique_id,
                                         &patch_path_markers);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find(
                      "VARIANT flexible partial update supports at most 256 patch paths per row"),
              std::string::npos);
}

TEST(VariantUtilTest, MarkVariantPatchPaths_RejectsTotalEncodedPathBytesBeyondLimit) {
    constexpr int32_t variant_col_unique_id = 100;
    std::string json = "{";
    for (int i = 0; i < 256; ++i) {
        if (i != 0) {
            json += ",";
        }
        std::string key = "k" + std::to_string(i) + "_";
        key.append(255 - key.size(), 'a');
        json += "\"";
        json += key;
        json += "\":";
        json += std::to_string(i);
    }
    json += "}";
    auto original_patch = _make_variant_column({std::string_view(json)});

    BitmapValue patch_path_markers;
    Status st = mark_variant_patch_paths(*original_patch, 0, variant_col_unique_id,
                                         &patch_path_markers);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("VARIANT flexible partial update encoded patch paths exceed "
                                  "65536 bytes per row"),
              std::string::npos);
}

TEST(VariantUtilTest, MarkVariantPatchPaths_RejectsTotalEncodedPathBytesAcrossColumns) {
    auto make_json = [] {
        std::string json = "{";
        for (int i = 0; i < 128; ++i) {
            if (i != 0) {
                json += ",";
            }
            std::string key = "k" + std::to_string(i) + "_";
            key.append(255 - key.size(), 'a');
            json += "\"";
            json += key;
            json += "\":";
            json += std::to_string(i);
        }
        json += "}";
        return json;
    };
    auto patch_v1 = _make_variant_column({make_json()});
    auto patch_v2 = _make_variant_column({make_json()});

    BitmapValue patch_path_markers;
    Status st = mark_variant_patch_paths(*patch_v1, 0, 100, &patch_path_markers);
    ASSERT_TRUE(st.ok()) << st.to_string();
    BitmapValue markers_before_v2 = patch_path_markers;
    st = mark_variant_patch_paths(*patch_v2, 0, 101, &patch_path_markers);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("VARIANT flexible partial update encoded patch paths exceed "
                                  "65536 bytes per row"),
              std::string::npos);
    EXPECT_EQ(patch_path_markers.cardinality(), markers_before_v2.cardinality());
}

TEST(VariantUtilTest, MarkVariantPatchPaths_RejectsRootJsonbArray) {
    constexpr int32_t variant_col_unique_id = 100;
    auto original_patch = _make_root_jsonb_variant_column(R"([1,2,3])");

    BitmapValue patch_path_markers;
    Status st = mark_variant_patch_paths(*original_patch, 0, variant_col_unique_id,
                                         &patch_path_markers);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find(
                      "VARIANT flexible partial update only supports JSON object patch values"),
              std::string::npos);
}

TEST(VariantUtilTest, MarkVariantPatchPaths_RejectsDocMode) {
    constexpr int32_t variant_col_unique_id = 100;
    auto original_patch =
            _make_variant_column({R"({"a":2})"}, true, ParseConfig::ParseTo::OnlyDocValueColumn);
    BitmapValue patch_path_markers;
    Status st = mark_variant_patch_paths(*original_patch, 0, variant_col_unique_id,
                                         &patch_path_markers);
    EXPECT_FALSE(st.ok());
}

TEST(VariantUtilTest, MergeVariantPatchByPathMarkers_EmptyObjectKeepsLatestOld) {
    constexpr int32_t variant_col_unique_id = 100;
    auto latest_old = _make_variant_column({R"({"a":1,"b":3})"});
    auto flushed_full_value = _make_variant_column({R"({"a":1,"b":1})"});
    auto original_patch = _make_variant_column({R"({})"});
    BitmapValue patch_path_markers;
    Status st = mark_variant_patch_paths(*original_patch, 0, variant_col_unique_id,
                                         &patch_path_markers);
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto merged_variant = ColumnVariant::create(0, false);
    st = merge_variant_patch_by_path_markers(*latest_old, 0, *flushed_full_value, 0,
                                             variant_col_unique_id, patch_path_markers, false,
                                             *merged_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();
    merged_variant->finalize();

    auto a = _get_variant_field(*merged_variant, "a");
    EXPECT_EQ(a.field.get<TYPE_BIGINT>(), 1);
    auto b = _get_variant_field(*merged_variant, "b");
    EXPECT_EQ(b.field.get<TYPE_BIGINT>(), 3);
}

TEST(VariantUtilTest, MergeVariantPatchByPathMarkers_IsolatesVariantColumns) {
    constexpr int32_t v1_unique_id = 100;
    constexpr int32_t v2_unique_id = 101;
    auto latest_old = _make_variant_column({R"({"a":1,"b":9})"});
    auto flushed_full_v1 = _make_variant_column({R"({"a":2,"b":1})"});
    auto original_patch_v1 = _make_variant_column({R"({"a":2})"});
    auto original_patch_v2 = _make_variant_column({R"({"b":8})"});
    BitmapValue patch_path_markers;
    Status st = mark_variant_patch_paths(*original_patch_v1, 0, v1_unique_id, &patch_path_markers);
    ASSERT_TRUE(st.ok()) << st.to_string();
    st = mark_variant_patch_paths(*original_patch_v2, 0, v2_unique_id, &patch_path_markers);
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto merged_variant = ColumnVariant::create(0, false);
    st = merge_variant_patch_by_path_markers(*latest_old, 0, *flushed_full_v1, 0, v1_unique_id,
                                             patch_path_markers, false, *merged_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();
    merged_variant->finalize();

    auto a = _get_variant_field(*merged_variant, "a");
    EXPECT_EQ(a.field.get<TYPE_BIGINT>(), 2);
    auto b = _get_variant_field(*merged_variant, "b");
    EXPECT_EQ(b.field.get<TYPE_BIGINT>(), 9);
}

TEST(VariantUtilTest, MergeVariantPatchByPathMarkers_DeletedOldUsesPatchOnly) {
    constexpr int32_t variant_col_unique_id = 100;
    auto deleted_old = _make_variant_column({R"({"a":1,"b":9})"});
    auto flushed_full_value = _make_variant_column({R"({"a":2,"b":1})"});
    auto original_patch = _make_variant_column({R"({"a":2})"});
    BitmapValue patch_path_markers;
    Status st = mark_variant_patch_paths(*original_patch, 0, variant_col_unique_id,
                                         &patch_path_markers);
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto merged_variant = ColumnVariant::create(0, false);
    st = merge_variant_patch_by_path_markers(*deleted_old, 0, *flushed_full_value, 0,
                                             variant_col_unique_id, patch_path_markers, true,
                                             *merged_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();
    merged_variant->finalize();

    auto a = _get_variant_field(*merged_variant, "a");
    EXPECT_EQ(a.field.get<TYPE_BIGINT>(), 2);
    _expect_no_variant_path(*merged_variant, "b");
}

TEST(VariantUtilTest, MergeVariantPatchByPathMarkers_EmptyObjectRemovesStaleSubpaths) {
    constexpr int32_t variant_col_unique_id = 100;
    auto latest_old = _make_variant_column({R"({"a":{"x":9},"b":3})"});
    auto flushed_full_value = _make_variant_column({R"({"a":{},"b":1})"}, false,
                                                   ParseConfig::ParseTo::OnlySubcolumns, true);
    auto original_patch = _make_variant_column({R"({"a":{}})"}, false,
                                               ParseConfig::ParseTo::OnlySubcolumns, true);
    BitmapValue patch_path_markers;
    Status st = mark_variant_patch_paths(*original_patch, 0, variant_col_unique_id,
                                         &patch_path_markers);
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto merged_variant = ColumnVariant::create(0, false);
    st = merge_variant_patch_by_path_markers(*latest_old, 0, *flushed_full_value, 0,
                                             variant_col_unique_id, patch_path_markers, false,
                                             *merged_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();
    merged_variant->finalize();

    auto a = _get_variant_field(*merged_variant, "a");
    ASSERT_EQ(a.field.get_type(), PrimitiveType::TYPE_JSONB);
    const auto& jsonb = a.field.get<TYPE_JSONB>();
    EXPECT_EQ(JsonbToJson::jsonb_to_json_string(jsonb.get_value(), jsonb.get_size()), "{}");
    auto b = _get_variant_field(*merged_variant, "b");
    EXPECT_EQ(b.field.get<TYPE_BIGINT>(), 3);
    _expect_no_variant_path(*merged_variant, "a.x");
}

TEST(VariantUtilTest, MergeVariantPatchByPathMarkers_PreservesSiblingChildPatch) {
    constexpr int32_t variant_col_unique_id = 100;
    auto latest_old = _make_variant_column({R"({"a":{"c":9},"x":1})"});
    auto flushed_full_value = _make_variant_column({R"({"a":{"b":1}})"});
    auto original_patch = _make_variant_column({R"({"a":{"b":1}})"});
    BitmapValue patch_path_markers;
    Status st = mark_variant_patch_paths(*original_patch, 0, variant_col_unique_id,
                                         &patch_path_markers);
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto merged_variant = ColumnVariant::create(0, false);
    st = merge_variant_patch_by_path_markers(*latest_old, 0, *flushed_full_value, 0,
                                             variant_col_unique_id, patch_path_markers, false,
                                             *merged_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();
    merged_variant->finalize();

    auto a_b = _get_variant_field(*merged_variant, "a.b");
    EXPECT_EQ(a_b.field.get<TYPE_BIGINT>(), 1);
    auto a_c = _get_variant_field(*merged_variant, "a.c");
    EXPECT_EQ(a_c.field.get<TYPE_BIGINT>(), 9);
    auto x = _get_variant_field(*merged_variant, "x");
    EXPECT_EQ(x.field.get<TYPE_BIGINT>(), 1);
}

TEST(VariantUtilTest, MergeVariantPatchByPathMarkers_DistinguishesDottedKeyFromNestedPath) {
    constexpr int32_t variant_col_unique_id = 100;
    auto latest_old = _make_variant_column({R"({"a.b":7,"a":{"c":9},"x":1})"});
    auto flushed_full_value = _make_variant_column({R"({"a":{"b":1}})"});
    auto original_patch = _make_variant_column({R"({"a":{"b":1}})"});
    BitmapValue patch_path_markers;
    Status st = mark_variant_patch_paths(*original_patch, 0, variant_col_unique_id,
                                         &patch_path_markers);
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto merged_variant = ColumnVariant::create(0, false);
    st = merge_variant_patch_by_path_markers(*latest_old, 0, *flushed_full_value, 0,
                                             variant_col_unique_id, patch_path_markers, false,
                                             *merged_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();
    merged_variant->finalize();

    auto nested_a_b = _get_variant_field_by_path(*merged_variant, _make_path({"a", "b"}));
    EXPECT_EQ(nested_a_b.field.get<TYPE_BIGINT>(), 1);
    auto dotted_a_b = _get_variant_field_by_path(*merged_variant, _make_path({"a.b"}));
    EXPECT_EQ(dotted_a_b.field.get<TYPE_BIGINT>(), 7);
    auto a_c = _get_variant_field(*merged_variant, "a.c");
    EXPECT_EQ(a_c.field.get<TYPE_BIGINT>(), 9);
    auto x = _get_variant_field(*merged_variant, "x");
    EXPECT_EQ(x.field.get<TYPE_BIGINT>(), 1);
}

TEST(VariantUtilTest, MergeVariantPatchByPathMarkers_ParentMarkerRemovesStaleSubpaths) {
    constexpr int32_t variant_col_unique_id = 100;
    auto latest_old = _make_variant_column({R"({"a":{"c":9},"x":1})"});
    auto flushed_full_value = _make_variant_column({R"({"a":{"b":1}})"});
    auto parent_patch = _make_variant_column({R"({"a":{}})"}, false,
                                             ParseConfig::ParseTo::OnlySubcolumns, true);
    auto child_patch = _make_variant_column({R"({"a":{"b":1}})"});
    BitmapValue patch_path_markers;
    Status st =
            mark_variant_patch_paths(*parent_patch, 0, variant_col_unique_id, &patch_path_markers);
    ASSERT_TRUE(st.ok()) << st.to_string();
    st = mark_variant_patch_paths(*child_patch, 0, variant_col_unique_id, &patch_path_markers);
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto merged_variant = ColumnVariant::create(0, false);
    st = merge_variant_patch_by_path_markers(*latest_old, 0, *flushed_full_value, 0,
                                             variant_col_unique_id, patch_path_markers, false,
                                             *merged_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();
    merged_variant->finalize();

    auto a_b = _get_variant_field(*merged_variant, "a.b");
    EXPECT_EQ(a_b.field.get<TYPE_BIGINT>(), 1);
    auto x = _get_variant_field(*merged_variant, "x");
    EXPECT_EQ(x.field.get<TYPE_BIGINT>(), 1);
    _expect_no_variant_path(*merged_variant, "a.c");
}

TEST(VariantUtilTest, MergeVariantPatchPathMarkers_RebuildsExactMetadata) {
    constexpr int32_t variant_col_unique_id = 100;
    auto latest_old = _make_variant_column({R"({"a":{"c":9},"x":1})"});
    auto flushed_full_value = _make_variant_column({R"({"a":{"b":1}})"});
    auto parent_patch = _make_variant_column({R"({"a":{}})"}, false,
                                             ParseConfig::ParseTo::OnlySubcolumns, true);
    auto child_patch = _make_variant_column({R"({"a":{"b":1}})"});

    BitmapValue parent_markers;
    parent_markers.add(variant_col_unique_id);
    Status st = mark_variant_patch_paths(*parent_patch, 0, variant_col_unique_id, &parent_markers);
    ASSERT_TRUE(st.ok()) << st.to_string();

    BitmapValue child_markers;
    st = mark_variant_patch_paths(*child_patch, 0, variant_col_unique_id, &child_markers);
    ASSERT_TRUE(st.ok()) << st.to_string();

    BitmapValue merged_markers;
    st = merge_variant_patch_path_markers(parent_markers, child_markers, &merged_markers);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_FALSE(merged_markers.contains(variant_col_unique_id));

    auto merged_variant = ColumnVariant::create(0, false);
    st = merge_variant_patch_by_path_markers(*latest_old, 0, *flushed_full_value, 0,
                                             variant_col_unique_id, merged_markers, false,
                                             *merged_variant);
    ASSERT_TRUE(st.ok()) << st.to_string();
    merged_variant->finalize();

    auto a_b = _get_variant_field(*merged_variant, "a.b");
    EXPECT_EQ(a_b.field.get<TYPE_BIGINT>(), 1);
    auto x = _get_variant_field(*merged_variant, "x");
    EXPECT_EQ(x.field.get<TYPE_BIGINT>(), 1);
    _expect_no_variant_path(*merged_variant, "a.c");
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

} // namespace doris::variant_util
