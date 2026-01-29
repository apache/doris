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
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "gen_cpp/olap_file.pb.h"
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
    parse_cfg.parse_to = vectorized::ParseConfig::ParseTo::BothSubcolumnsAndDocValueColumn;
    Status st =
            parse_and_materialize_variant_columns(block, std::vector<uint32_t> {0}, {parse_cfg});
    EXPECT_TRUE(st.ok()) << st.to_string();

    const auto& out =
            assert_cast<const vectorized::ColumnVariant&>(*block.get_by_position(0).column);
    EXPECT_FALSE(out.is_doc_mode());

    const auto* sub_a = out.get_subcolumn(vectorized::PathInData("a"));
    const auto* sub_b = out.get_subcolumn(vectorized::PathInData("b"));
    ASSERT_TRUE(sub_a != nullptr);
    ASSERT_TRUE(sub_b != nullptr);

    vectorized::FieldWithDataType f;
    sub_a->get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 1);
    sub_a->get(1, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_BIGINT);
    EXPECT_EQ(f.field.get<TYPE_BIGINT>(), 2);

    sub_b->get(0, f);
    EXPECT_EQ(f.field.get_type(), PrimitiveType::TYPE_STRING);
    EXPECT_EQ(f.field.get<TYPE_STRING>(), "x");
    sub_b->get(1, f);
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
    parse_cfg.parse_to = vectorized::ParseConfig::ParseTo::BothSubcolumnsAndDocValueColumn;
    Status st =
            parse_and_materialize_variant_columns(block, std::vector<uint32_t> {0}, {parse_cfg});
    EXPECT_TRUE(st.ok()) << st.to_string();
}

// Generate sparse JSON rows: each row contains `keys_per_row` keys chosen from
// `total_keys` distinct key names, so most subcolumns are mostly defaults.
static std::vector<std::string> _generate_sparse_jsons(size_t num_rows, size_t total_keys,
                                                       size_t keys_per_row, uint32_t seed = 42) {
    std::mt19937 rng(seed);
    std::uniform_int_distribution<size_t> key_dist(0, total_keys - 1);
    std::uniform_int_distribution<int64_t> val_dist(0, 1000000);

    std::vector<std::string> key_names(total_keys);
    for (size_t i = 0; i < total_keys; ++i) {
        key_names[i] = "k" + std::to_string(i);
    }

    std::vector<std::string> rows;
    rows.reserve(num_rows);
    for (size_t r = 0; r < num_rows; ++r) {
        std::string json = "{";
        // Pick `keys_per_row` distinct keys per row
        std::vector<size_t> chosen;
        chosen.reserve(keys_per_row);
        while (chosen.size() < keys_per_row) {
            size_t k = key_dist(rng);
            if (std::find(chosen.begin(), chosen.end(), k) == chosen.end()) {
                chosen.push_back(k);
            }
        }
        for (size_t i = 0; i < chosen.size(); ++i) {
            if (i > 0) json += ",";
            json += "\"" + key_names[chosen[i]] + "\":" + std::to_string(val_dist(rng));
        }
        json += "}";
        rows.push_back(std::move(json));
    }
    return rows;
}

TEST(VariantUtilTest, DISABLED_PerfMaterializeDocsSparse) {
    // Configuration: 100K rows, 500 distinct keys, 3 keys per row (~99.4% sparsity)
    constexpr size_t kNumRows = 100000;
    constexpr size_t kTotalKeys = 500;
    constexpr size_t kKeysPerRow = 3;
    constexpr int kWarmupIters = 2;
    constexpr int kBenchIters = 5;

    auto json_strings = _generate_sparse_jsons(kNumRows, kTotalKeys, kKeysPerRow);
    std::vector<std::string_view> json_views(json_strings.begin(), json_strings.end());

    // Build doc-mode ColumnVariant once
    auto variant_template = vectorized::ColumnVariant::create(0);
    auto json_col = _make_json_column(json_views);
    vectorized::ParseConfig cfg;
    cfg.enable_flatten_nested = false;
    cfg.parse_to = vectorized::ParseConfig::ParseTo::OnlyDocValueColumn;
    parse_json_to_variant(*variant_template, *json_col, cfg);
    ASSERT_TRUE(variant_template->is_doc_mode());

    std::cout << "=== materialize_docs_to_subcolumns_map Benchmark (Sparse Docs) ===" << std::endl;
    std::cout << "Rows: " << kNumRows << ", Total keys: " << kTotalKeys
              << ", Keys/row: " << kKeysPerRow
              << ", Sparsity: " << (1.0 - double(kKeysPerRow) / kTotalKeys) * 100 << "%"
              << std::endl;

    // Warmup
    for (int i = 0; i < kWarmupIters; ++i) {
        auto copy = variant_template->clone_resized(variant_template->size());
        const auto& variant_ref =
                assert_cast<const vectorized::ColumnVariant&>(*copy);
        auto result = materialize_docs_to_subcolumns_map(variant_ref);
        ASSERT_GT(result.size(), 0);
    }

    // Benchmark
    int64_t total_ns = 0;
    int64_t min_ns = std::numeric_limits<int64_t>::max();
    int64_t max_ns = 0;
    size_t subcolumn_count = 0;

    for (int i = 0; i < kBenchIters; ++i) {
        auto copy = variant_template->clone_resized(variant_template->size());
        const auto& variant_ref =
                assert_cast<const vectorized::ColumnVariant&>(*copy);

        auto start = std::chrono::high_resolution_clock::now();
        auto result = materialize_docs_to_subcolumns_map(variant_ref);
        auto end = std::chrono::high_resolution_clock::now();

        int64_t elapsed =
                std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        total_ns += elapsed;
        min_ns = std::min(min_ns, elapsed);
        max_ns = std::max(max_ns, elapsed);
        subcolumn_count = result.size();
    }

    double avg_ms = total_ns / 1e6 / kBenchIters;
    double min_ms = min_ns / 1e6;
    double max_ms = max_ns / 1e6;

    std::cout << "Subcolumns materialized: " << subcolumn_count << std::endl;
    std::cout << "Iterations: " << kBenchIters << std::endl;
    std::cout << "Avg: " << avg_ms << " ms" << std::endl;
    std::cout << "Min: " << min_ms << " ms" << std::endl;
    std::cout << "Max: " << max_ms << " ms" << std::endl;
    std::cout << "Throughput: " << (kNumRows / avg_ms * 1000) << " rows/s" << std::endl;
    std::cout << "=============================================================" << std::endl;
}

} // namespace doris::vectorized::variant_util