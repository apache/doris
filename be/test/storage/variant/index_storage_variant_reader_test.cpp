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

#include <gtest/gtest.h>

#include <algorithm>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "storage/variant/index_storage_variant_test_base.h"
#include "util/variant/variant_test_utils.h"

namespace doris::index_storage_test {
namespace {

constexpr int32_t kVariantUid = 2;

bool has_accepted_binding(const IndexReadResult& result, std::string_view logical_path) {
    return std::ranges::any_of(
            result.stats.inverted_index_stats.binding_diagnostics,
            [&](const std::string& diagnostic) {
                return diagnostic.find("result=accepted") != std::string::npos &&
                       diagnostic.find("logical_path=" + std::string(logical_path)) !=
                               std::string::npos;
            });
}

bool has_parent_inherited_binding(const IndexReadResult& result, std::string_view logical_path) {
    return std::ranges::any_of(
            result.stats.inverted_index_stats.binding_diagnostics,
            [&](const std::string& diagnostic) {
                return diagnostic.find("source=parent_inherited") != std::string::npos &&
                       diagnostic.find("logical_path=" + std::string(logical_path)) !=
                               std::string::npos;
            });
}

void expect_index_read(const IndexReadResult& result, int64_t expected_rows,
                       int64_t expected_filtered_rows, std::string_view logical_path) {
    EXPECT_EQ(result.rows_read, expected_rows);
    EXPECT_EQ(result.stats.rows_inverted_index_filtered, expected_filtered_rows);
    EXPECT_EQ(result.stats.inverted_index_downgrade_count, 0);
    EXPECT_TRUE(result.variant_v2_output_uids.contains(kVariantUid));
    EXPECT_TRUE(has_accepted_binding(result, logical_path));
}

void expect_variant_values(const IndexReadResult& result, int32_t unique_id,
                           const std::vector<std::optional<std::string>>& expected) {
    ASSERT_TRUE(result.variant_values_by_uid.contains(unique_id));
    const auto& actual = result.variant_values_by_uid.at(unique_id);
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t row = 0; row < expected.size(); ++row) {
        EXPECT_EQ(canonical_variant_value(actual[row]), canonical_variant_value(expected[row]))
                << "row=" << row;
    }
}

ColumnPtr make_nullable_doc_column(const std::vector<std::string>& jsons,
                                   const std::vector<uint8_t>& outer_nulls) {
    DORIS_CHECK_EQ(jsons.size(), outer_nulls.size());
    auto values = ColumnVariantV2::create();
    JsonStringToVariantEncoder encoder({.throw_on_invalid_json = true});
    for (const auto& json : jsons) {
        encoder.add_json({json.data(), json.size()});
    }
    VariantBatchBuilder encoded = encoder.finish_batch();
    for (size_t row = 0; row < jsons.size(); ++row) {
        insert_encoded_field(*values, VariantField::from_ref(encoded.value_at(row)));
    }

    auto null_map = ColumnUInt8::create();
    for (uint8_t value : outer_nulls) {
        null_map->insert_value(value);
    }
    return ColumnNullable::create(std::move(values), std::move(null_map));
}

} // namespace

// Behavior-level reader cases exercise the V2-only storage reader.
class IndexStorageVariantReaderTest : public IndexStorageTestFixture {};

TEST_F(IndexStorageVariantReaderTest,
       ParentInheritedIndexedPathFiltersRootBeforeAndAfterCompactionReload) {
    VariantColumnSpec variant;
    variant.unique_id = kVariantUid;
    variant.name = "v";

    // Keep `b` dynamic and index only the parent Variant column. A predefined path or field-pattern
    // index would take the direct binding branch and stop protecting V2 runtime type inference.
    const auto index_case =
            IndexStorageCaseBuilder("variant_parent_inherited_index")
                    .tablet_id(110063)
                    .variant_column(std::move(variant))
                    .inverted_index(IndexSpec::column_index(220501, "idx_parent", kVariantUid))
                    .rowset(0,
                            IndexDataSourceSpec::inline_variant({R"({"b":"one","residual":0})",
                                                                 R"({"b":"other","residual":1})"},
                                                                0))
                    .rowset(1,
                            IndexDataSourceSpec::inline_variant({R"({"b":"one","residual":2})",
                                                                 R"({"b":"other","residual":3})"},
                                                                100))
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto v2_read_schema = build_schema_with_variant_path_column(
            *tablet_schema(), kVariantUid, "v.b", FieldType::OLAP_FIELD_TYPE_VARIANT);
    ASSERT_NE(v2_read_schema, nullptr);
    auto readable = inject_reader_schema_for_rowsets(rowsets.value(), std::move(v2_read_schema));
    ASSERT_TRUE(readable.has_value()) << readable.error();
    const int32_t root_column_id = column_id_by_path("v");
    const int32_t path_column_id = column_id_by_path("v.b");
    ASSERT_GE(root_column_id, 0) << dump_schema_paths(*tablet_schema());
    ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());
    const auto& path_column = tablet_schema()->column(path_column_id);
    ASSERT_TRUE(path_column.is_variant_type());

    IndexReadOptions read_options;
    read_options.need_ordered_result = true;
    read_options.return_columns = {0, static_cast<uint32_t>(root_column_id),
                                   static_cast<uint32_t>(path_column_id)};
    read_options.target_cast_type_for_variants[path_column.name()] = nullable_string_target_type();
    read_options.predicates.push_back(string_equals(path_column_id, path_column.name(), "one"));
    read_options.collect_variant_values = true;
    auto verify_read = [&](const std::vector<RowsetSharedPtr>& source, std::string_view phase) {
        auto result = read_rowsets(source, read_options);
        ASSERT_TRUE(result.has_value()) << phase << ": " << result.error();
        expect_index_read(result.value(), 2, 2, "v.b");
        EXPECT_TRUE(has_parent_inherited_binding(result.value(), "v.b")) << phase;
        EXPECT_TRUE(result->variant_v2_output_uids.contains(-1)) << phase;
        expect_variant_values(result.value(), kVariantUid,
                              {R"({"b":"one","residual":0})", R"({"b":"one","residual":2})"});
        expect_variant_values(result.value(), -1, {R"("one")", R"("one")"});
    };
    verify_read(readable.value(), "before compaction");

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    auto reloaded = reload_rowsets({compacted.value()});
    ASSERT_TRUE(reloaded.has_value()) << reloaded.error();
    auto compacted_v2_schema = build_schema_with_variant_path_column(
            *tablet_schema(), kVariantUid, "v.b", FieldType::OLAP_FIELD_TYPE_VARIANT);
    ASSERT_NE(compacted_v2_schema, nullptr);
    auto readable_compacted =
            inject_reader_schema_for_rowsets(reloaded.value(), std::move(compacted_v2_schema));
    ASSERT_TRUE(readable_compacted.has_value()) << readable_compacted.error();
    ASSERT_EQ(column_id_by_path("v.b"), path_column_id);

    verify_read(readable_compacted.value(), "after compaction");
}

TEST_F(IndexStorageVariantReaderTest, DocResidualRootStaysCorrectAfterIndexedPathFiltering) {
    VariantColumnSpec variant;
    variant.unique_id = kVariantUid;
    variant.name = "v";
    variant.max_subcolumns_count = 2;
    variant.enable_doc_mode = true;
    variant.doc_materialization_min_rows = 0;
    variant.doc_hash_shard_count = 2;
    variant.predefined_paths = {
            VariantPathSpec {.path = "indexed",
                             .type = FieldType::OLAP_FIELD_TYPE_STRING,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = {},
                             .array_item_nullable = true},
    };

    const auto index_case =
            IndexStorageCaseBuilder("variant_doc_index_residual")
                    .tablet_id(110064)
                    .variant_column(std::move(variant))
                    .inverted_index(IndexSpec::field_pattern_index(220502, "idx_doc_indexed",
                                                                   kVariantUid, "indexed"))
                    .rowset(0, IndexDataSourceSpec::inline_variant(
                                       {R"({"indexed":"hit","residual":{"arr":[1,2]}})",
                                        R"({"indexed":"miss","residual":{"empty":{}}})",
                                        R"({"indexed":"hit","residual":{"nested":{"v":3}}})"},
                                       0))
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto v2_read_schema = build_schema_with_variant_path_column(
            *tablet_schema(), kVariantUid, "v.indexed", FieldType::OLAP_FIELD_TYPE_VARIANT);
    ASSERT_NE(v2_read_schema, nullptr);
    auto readable = inject_reader_schema_for_rowsets(rowsets.value(), std::move(v2_read_schema));
    ASSERT_TRUE(readable.has_value()) << readable.error();
    const int32_t root_column_id = column_id_by_path("v");
    const int32_t path_column_id = column_id_by_path("v.indexed");
    ASSERT_GE(root_column_id, 0) << dump_schema_paths(*tablet_schema());
    ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());
    const auto& path_column = tablet_schema()->column(path_column_id);
    ASSERT_TRUE(path_column.is_variant_type());

    IndexReadOptions read_options;
    read_options.need_ordered_result = true;
    read_options.return_columns = {0, static_cast<uint32_t>(root_column_id),
                                   static_cast<uint32_t>(path_column_id)};
    read_options.target_cast_type_for_variants[path_column.name()] = nullable_string_target_type();
    read_options.predicates.push_back(string_equals(path_column_id, path_column.name(), "hit"));
    read_options.collect_variant_values = true;
    auto read_result = read_rowsets(readable.value(), std::move(read_options));
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    expect_index_read(read_result.value(), 2, 1, "v.indexed");
    expect_variant_values(read_result.value(), kVariantUid,
                          {R"({"indexed":"hit","residual":{"arr":[1,2]}})",
                           R"({"indexed":"hit","residual":{"nested":{"v":3}}})"});
    EXPECT_GT(read_result->stats.variant_doc_value_column_iter_count, 0);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest macros inflate the matrix.
TEST_F(IndexStorageVariantReaderTest, DocV3ExactPrefixDefaultAndNullMatrix) {
    const std::vector<std::string> jsons {
            "null",
            "null",
            "{}",
            R"({"other":1})",
            R"({"exact":"x","obj":{"arr":[1,{"z":2}],"child":1}})",
            R"({"exact":[1,2],"obj":{"child":null}})",
    };
    const std::vector<uint8_t> outer_nulls {1, 0, 0, 0, 0, 0};

    VariantColumnSpec variant;
    variant.unique_id = kVariantUid;
    variant.name = "v";
    variant.nullable = true;
    variant.max_subcolumns_count = 2;
    variant.enable_doc_mode = true;
    variant.doc_materialization_min_rows = 100000;
    variant.doc_hash_shard_count = 2;

    IndexTabletOptions options;
    options.tablet_id = 110065;
    options.external_segment_meta = true;
    options.variant_columns = {std::move(variant)};
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.batches.push_back(
            IndexBatch::single_variant_column(make_nullable_doc_column(jsons, outer_nulls), 0));
    auto written = write_rowset(rowset);
    ASSERT_TRUE(written.has_value()) << written.error();

    auto read_schema = build_schema_with_variant_path_column(
            *tablet_schema(), kVariantUid, "v.exact", FieldType::OLAP_FIELD_TYPE_VARIANT);
    ASSERT_NE(read_schema, nullptr);
    read_schema = build_schema_with_variant_path_column(*read_schema, kVariantUid, "v.obj",
                                                        FieldType::OLAP_FIELD_TYPE_VARIANT);
    ASSERT_NE(read_schema, nullptr);
    read_schema = build_schema_with_variant_path_column(*read_schema, kVariantUid, "v.absent",
                                                        FieldType::OLAP_FIELD_TYPE_VARIANT);
    ASSERT_NE(read_schema, nullptr);
    auto readable = inject_reader_schema_for_rowsets({written.value()}, std::move(read_schema));
    ASSERT_TRUE(readable.has_value()) << readable.error();

    const int32_t root_column_id = column_id_by_path("v");
    const int32_t exact_column_id = column_id_by_path("v.exact");
    const int32_t prefix_column_id = column_id_by_path("v.obj");
    const int32_t absent_column_id = column_id_by_path("v.absent");
    ASSERT_GE(root_column_id, 0) << dump_schema_paths(*tablet_schema());
    ASSERT_GE(exact_column_id, 0) << dump_schema_paths(*tablet_schema());
    ASSERT_GE(prefix_column_id, 0) << dump_schema_paths(*tablet_schema());
    ASSERT_GE(absent_column_id, 0) << dump_schema_paths(*tablet_schema());

    auto read_column = [&](int32_t column_id) {
        IndexReadOptions read_options;
        read_options.need_ordered_result = true;
        read_options.return_columns = {0, static_cast<uint32_t>(column_id)};
        read_options.collect_variant_values = true;
        return read_rowsets(readable.value(), std::move(read_options));
    };

    auto verify_column = [&](int32_t column_id, int32_t unique_id,
                             int64_t OlapReaderStatistics::*route_counter,
                             const std::vector<std::optional<std::string>>& expected) {
        auto result = read_column(column_id);
        ASSERT_TRUE(result.has_value()) << result.error();
        EXPECT_TRUE(result->variant_v2_output_uids.contains(unique_id));
        EXPECT_GT(result->stats.*route_counter, 0);
        expect_variant_values(result.value(), unique_id, expected);
    };

    verify_column(root_column_id, kVariantUid,
                  &OlapReaderStatistics::variant_doc_value_column_iter_count,
                  {std::nullopt, "{}", "{}", R"({"other":1})",
                   R"({"exact":"x","obj":{"arr":[1,{"z":2}],"child":1}})", R"({"exact":[1,2]})"});
    verify_column(exact_column_id, -1, &OlapReaderStatistics::variant_subtree_sparse_iter_count,
                  {std::nullopt, std::nullopt, std::nullopt, std::nullopt, R"("x")", R"([1,2])"});
    verify_column(prefix_column_id, -1, &OlapReaderStatistics::variant_doc_value_column_iter_count,
                  {std::nullopt, std::nullopt, std::nullopt, std::nullopt,
                   R"({"arr":[1,{"z":2}],"child":1})", std::nullopt});
    verify_column(
            absent_column_id, -1, &OlapReaderStatistics::variant_subtree_default_iter_count,
            {std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt});
}

} // namespace doris::index_storage_test
