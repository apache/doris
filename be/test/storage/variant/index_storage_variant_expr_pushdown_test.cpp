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

#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/primitive_type.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "testutil/index_storage_test_util.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris::index_storage_test {
namespace {

constexpr int32_t kVariantUid = 2;
constexpr int64_t kIntPatternIndexId = 220201;
constexpr int64_t kArrayPatternIndexId = 220202;
constexpr int64_t kSparseIntPatternIndexId = 220203;
constexpr std::string_view kArrayPath = "inventors";

VariantColumnSpec typed_pattern_variant_column() {
    VariantColumnSpec variant;
    variant.unique_id = kVariantUid;
    variant.name = "v";
    variant.max_subcolumns_count = 2;
    variant.predefined_paths = {
            VariantPathSpec {.path = "int_*",
                             .type = FieldType::OLAP_FIELD_TYPE_INT,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME_GLOB,
                             .array_item_type = {},
                             .array_item_nullable = true},
    };
    return variant;
}

VariantColumnSpec array_text_variant_column() {
    VariantColumnSpec variant;
    variant.unique_id = kVariantUid;
    variant.name = "v";
    variant.nullable = true;
    variant.max_subcolumns_count = 10;
    variant.predefined_paths = {
            VariantPathSpec {.path = std::string(kArrayPath),
                             .type = FieldType::OLAP_FIELD_TYPE_ARRAY,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME,
                             .array_item_type = FieldType::OLAP_FIELD_TYPE_STRING,
                             .array_item_nullable = true},
    };
    return variant;
}

DataTypePtr nullable_array_text_type() {
    return make_nullable(
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeString>())));
}

VExprSPtr make_string_literal(std::string value) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::STRING_LITERAL);
    node.__set_type(create_type_desc(PrimitiveType::TYPE_STRING));
    node.__set_num_children(0);
    node.__set_is_nullable(false);

    TStringLiteral literal;
    literal.__set_value(std::move(value));
    node.__set_string_literal(literal);
    return VLiteral::create_shared(node);
}

VExprContextSPtr make_is_null_context(int32_t block_column_id, bool is_null) {
    TFunctionName fn_name;
    fn_name.__set_db_name("");
    fn_name.__set_function_name(is_null ? "is_null_pred" : "is_not_null_pred");

    TFunction fn;
    fn.__set_name(fn_name);
    fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
    fn.__set_arg_types({create_type_desc(PrimitiveType::TYPE_INT)});
    fn.__set_ret_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    fn.__set_has_var_args(false);

    TExprNode node;
    node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    node.__set_node_type(TExprNodeType::IS_NULL_PRED);
    node.__set_fn(fn);
    node.__set_num_children(1);
    node.__set_is_nullable(false);

    auto root = VectorizedFnCall::create_shared(node);
    root->add_child(std::make_shared<MockSlotRef>(
            block_column_id, make_nullable(std::make_shared<DataTypeInt32>())));
    return VExprContext::create_shared(root);
}

VExprContextSPtr make_array_contains_context(int32_t block_column_id, std::string value) {
    TFunctionName fn_name;
    fn_name.__set_db_name("");
    fn_name.__set_function_name("array_contains");

    TFunction fn;
    fn.__set_name(fn_name);
    fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
    fn.__set_arg_types({create_type_desc(PrimitiveType::TYPE_ARRAY),
                        create_type_desc(PrimitiveType::TYPE_STRING)});
    fn.__set_ret_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    fn.__set_has_var_args(false);

    TExprNode node;
    node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    node.__set_fn(fn);
    node.__set_num_children(2);
    node.__set_is_nullable(true);

    auto root = VectorizedFnCall::create_shared(node);
    root->add_child(std::make_shared<MockSlotRef>(block_column_id, nullable_array_text_type()));
    root->add_child(make_string_literal(std::move(value)));
    return VExprContext::create_shared(root);
}

IndexReadOptions make_is_null_read_options(const TabletSchema& schema, int32_t path_column_id,
                                           bool is_null) {
    const auto& path_column = schema.column(path_column_id);

    IndexReadOptions options;
    options.return_columns = {0, static_cast<uint32_t>(path_column_id)};
    options.target_cast_type_for_variants[path_column.name()] =
            make_nullable(std::make_shared<DataTypeInt32>());
    options.common_expr_ctxs_push_down.push_back(make_is_null_context(1, is_null));
    return options;
}

IndexReadOptions make_array_contains_read_options(const TabletSchema& schema,
                                                  int32_t path_column_id, std::string value) {
    const auto& path_column = schema.column(path_column_id);

    IndexReadOptions options;
    options.return_columns = {0, static_cast<uint32_t>(path_column_id)};
    options.target_cast_type_for_variants[path_column.name()] = nullable_array_text_type();
    options.common_expr_ctxs_push_down.push_back(make_array_contains_context(1, std::move(value)));
    return options;
}

} // namespace

class IndexStorageVariantExprPushdownTest : public IndexStorageTestFixture {};

TEST_F(IndexStorageVariantExprPushdownTest, IsNullExprUsesVariantFieldPatternIndex) {
    const auto index_case =
            IndexStorageCaseBuilder("variant_is_null_expr_field_pattern_index")
                    .tablet_id(110034)
                    .variant_column(typed_pattern_variant_column())
                    .inverted_index(IndexSpec::field_pattern_index(
                            kIntPatternIndexId, "idx_v_int_glob", kVariantUid, "int_*"))
                    .rowset(0, IndexDataSourceSpec::inline_variant(
                                       {R"({"int_1": 42})", R"({"int_2": 7})"}, 0))
                    .rowset(1, IndexDataSourceSpec::inline_variant(
                                       {R"({"int_1": null})", R"({"int_1": 8})"}, 100))
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(rowsets.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();
    const int32_t path_column_id = column_id_by_path("v.int_1");
    ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());

    auto not_null_read =
            read_rowsets(readable_rowsets.value(),
                         make_is_null_read_options(*tablet_schema(), path_column_id, false));
    ASSERT_TRUE(not_null_read.has_value()) << not_null_read.error();
    EXPECT_EQ(not_null_read->rows_read, 2);
    expect_index_filter_stats(not_null_read.value(), 2);
    expect_index_probe(not_null_read.value(), IndexProbeExpectation {
                                                      .source = IndexProbeSource::EXPR_PUSHDOWN,
                                                      .state = IndexProbeState::APPLIED,
                                                      .reason = IndexFallbackReason::NONE,
                                                      .column_uid = kVariantUid,
                                                      .variant_path = "int_1",
                                                      .index_id = kIntPatternIndexId,
                                                      .counts_toward_filter_stats = true,
                                                      .filtered_rows = 1,
                                              });
    expect_index_probe(not_null_read.value(), IndexProbeExpectation {
                                                      .source = IndexProbeSource::EXPR_PUSHDOWN,
                                                      .state = IndexProbeState::APPLIED,
                                                      .reason = IndexFallbackReason::NONE,
                                                      .column_uid = kVariantUid,
                                                      .variant_path = "int_1",
                                                      .index_id = kIntPatternIndexId,
                                                      .counts_toward_filter_stats = true,
                                                      .filtered_rows = 1,
                                              });

    auto null_read =
            read_rowsets(readable_rowsets.value(),
                         make_is_null_read_options(*tablet_schema(), path_column_id, true));
    ASSERT_TRUE(null_read.has_value()) << null_read.error();
    EXPECT_EQ(null_read->rows_read, 2);
    expect_index_filter_stats(null_read.value(), 2);
    expect_index_probe(null_read.value(), IndexProbeExpectation {
                                                  .source = IndexProbeSource::EXPR_PUSHDOWN,
                                                  .state = IndexProbeState::APPLIED,
                                                  .reason = IndexFallbackReason::NONE,
                                                  .column_uid = kVariantUid,
                                                  .variant_path = "int_1",
                                                  .index_id = kIntPatternIndexId,
                                                  .counts_toward_filter_stats = true,
                                                  .filtered_rows = 1,
                                          });
    expect_index_probe(null_read.value(), IndexProbeExpectation {
                                                  .source = IndexProbeSource::EXPR_PUSHDOWN,
                                                  .state = IndexProbeState::APPLIED,
                                                  .reason = IndexFallbackReason::NONE,
                                                  .column_uid = kVariantUid,
                                                  .variant_path = "int_1",
                                                  .index_id = kIntPatternIndexId,
                                                  .counts_toward_filter_stats = true,
                                                  .filtered_rows = 1,
                                          });

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 4);

    auto reloaded = reload_rowsets({compacted.value()});
    ASSERT_TRUE(reloaded.has_value()) << reloaded.error();
    auto readable_compacted = rowsets_with_variant_extended_schema(reloaded.value());
    ASSERT_TRUE(readable_compacted.has_value()) << readable_compacted.error();
    const int32_t compacted_path_column_id = column_id_by_path("v.int_1");
    ASSERT_EQ(compacted_path_column_id, path_column_id);

    auto compacted_not_null = read_rowsets(
            readable_compacted.value(),
            make_is_null_read_options(*tablet_schema(), compacted_path_column_id, false));
    ASSERT_TRUE(compacted_not_null.has_value()) << compacted_not_null.error();
    EXPECT_EQ(compacted_not_null->rows_read, 2);
    expect_index_filter_stats(compacted_not_null.value(), 2);
    expect_index_probe(compacted_not_null.value(),
                       IndexProbeExpectation {
                               .source = IndexProbeSource::EXPR_PUSHDOWN,
                               .state = IndexProbeState::APPLIED,
                               .reason = IndexFallbackReason::NONE,
                               .column_uid = kVariantUid,
                               .variant_path = "int_1",
                               .index_id = kIntPatternIndexId,
                               .counts_toward_filter_stats = true,
                               .filtered_rows = 2,
                       });

    auto compacted_null = read_rowsets(
            readable_compacted.value(),
            make_is_null_read_options(*tablet_schema(), compacted_path_column_id, true));
    ASSERT_TRUE(compacted_null.has_value()) << compacted_null.error();
    EXPECT_EQ(compacted_null->rows_read, 2);
    expect_index_filter_stats(compacted_null.value(), 2);
    expect_index_probe(compacted_null.value(), IndexProbeExpectation {
                                                       .source = IndexProbeSource::EXPR_PUSHDOWN,
                                                       .state = IndexProbeState::APPLIED,
                                                       .reason = IndexFallbackReason::NONE,
                                                       .column_uid = kVariantUid,
                                                       .variant_path = "int_1",
                                                       .index_id = kIntPatternIndexId,
                                                       .counts_toward_filter_stats = true,
                                                       .filtered_rows = 2,
                                               });
}

TEST_F(IndexStorageVariantExprPushdownTest, IsNullExprSkipsSparseVariantFieldPatternIndex) {
    VariantColumnSpec variant;
    variant.unique_id = kVariantUid;
    variant.name = "v";
    variant.max_subcolumns_count = 1;
    variant.sparse_hash_shard_count = 2;

    // "rare" is kept in the sparse map instead of a materialized typed subcolumn. The
    // field-pattern index metadata should not make sparse map entries look indexable unless the
    // writer creates a real sparse-path inverted index.
    const auto index_case =
            IndexStorageCaseBuilder("variant_sparse_is_null_expr_field_pattern_index")
                    .tablet_id(110038)
                    .variant_column(std::move(variant))
                    .inverted_index(IndexSpec::field_pattern_index(
                            kSparseIntPatternIndexId, "idx_v_sparse_rare", kVariantUid, "rare"))
                    .rowset(0, IndexDataSourceSpec::inline_variant(
                                       {R"({"hot": 1, "rare": 10})", R"({"hot": 2})",
                                        R"({"hot": 3, "rare": null})"},
                                       0))
                    .rowset(1, IndexDataSourceSpec::inline_variant(
                                       {R"({"hot": 4, "rare": 20})", R"({"hot": 5})"}, 100))
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto read_schema = build_schema_with_variant_path_column(*tablet_schema(), kVariantUid, "rare",
                                                             FieldType::OLAP_FIELD_TYPE_INT);
    ASSERT_NE(read_schema, nullptr);
    auto readable_rowsets =
            inject_reader_schema_for_rowsets(rowsets.value(), std::move(read_schema));
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();
    const int32_t path_column_id = column_id_by_path("v.rare");
    ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());

    auto not_null_read =
            read_rowsets(readable_rowsets.value(),
                         make_is_null_read_options(*tablet_schema(), path_column_id, false));
    ASSERT_TRUE(not_null_read.has_value()) << not_null_read.error();
    EXPECT_EQ(not_null_read->rows_read, 2);
    EXPECT_EQ(not_null_read->stats.rows_inverted_index_filtered, 0);
    expect_inverted_index_not_attempted(not_null_read.value());

    auto null_read =
            read_rowsets(readable_rowsets.value(),
                         make_is_null_read_options(*tablet_schema(), path_column_id, true));
    ASSERT_TRUE(null_read.has_value()) << null_read.error();
    EXPECT_EQ(null_read->rows_read, 3);
    EXPECT_EQ(null_read->stats.rows_inverted_index_filtered, 0);
    expect_inverted_index_not_attempted(null_read.value());
}

TEST_F(IndexStorageVariantExprPushdownTest, ArrayContainsUsesVariantArrayFieldPatternIndex) {
    const auto index_case =
            IndexStorageCaseBuilder("variant_array_contains_field_pattern_index")
                    .tablet_id(110035)
                    .variant_column(array_text_variant_column())
                    .inverted_index(IndexSpec::field_pattern_index(kArrayPatternIndexId,
                                                                   "idx_v_inventors", kVariantUid,
                                                                   std::string(kArrayPath)))
                    .rowset(0,
                            IndexDataSourceSpec::inline_variant(
                                    {R"({"inventors":["a", "b", "c"]})", R"({"inventors":[]})",
                                     R"({"inventors":["d", "e", "f", "g", "h", "i", "j", "k", "l"]})",
                                     R"({"inventors":null})",
                                     R"({"inventors":["m", "n", "o", "p", "q", "r", "s", "t", "u"]})",
                                     R"({"inventors":[null, null, null]})",
                                     R"({"inventors":["v", "w", "x", "y", "z"]})"},
                                    0))
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(rowsets.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();
    const int32_t path_column_id = column_id_by_path("v.inventors");
    ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());

    ScopedDebugPoint debug_point("array_func.array_contains", {{"result_bitmap", "1"}});
    auto read =
            read_rowsets(readable_rowsets.value(),
                         make_array_contains_read_options(*tablet_schema(), path_column_id, "w"));
    ASSERT_TRUE(read.has_value()) << read.error();
    EXPECT_GT(debug_point.execute_num(), 0);
    EXPECT_EQ(read->rows_read, 1);
    EXPECT_EQ(read->stats.rows_inverted_index_filtered, 6);
    expect_index_probe(read.value(), IndexProbeExpectation {
                                             .source = IndexProbeSource::EXPR_PUSHDOWN,
                                             .state = IndexProbeState::APPLIED,
                                             .reason = IndexFallbackReason::NONE,
                                             .column_uid = kVariantUid,
                                             .variant_path = std::string(kArrayPath),
                                             .index_id = kArrayPatternIndexId,
                                             .counts_toward_filter_stats = true,
                                             .filtered_rows = 6,
                                     });
}

TEST_F(IndexStorageVariantExprPushdownTest,
       ArrayContainsRecordsNoHitDisabledIndexAndCompactedReload) {
    const auto index_case =
            IndexStorageCaseBuilder("variant_array_contains_probe_matrix")
                    .tablet_id(110036)
                    .variant_column(array_text_variant_column())
                    .inverted_index(IndexSpec::field_pattern_index(kArrayPatternIndexId,
                                                                   "idx_v_inventors", kVariantUid,
                                                                   std::string(kArrayPath)))
                    .rowset(0, IndexDataSourceSpec::inline_variant(
                                       {R"({"inventors":["w", "amory"]})", R"({"inventors":[]})",
                                        R"({"inventors":null})"},
                                       0))
                    .rowset(1, IndexDataSourceSpec::inline_variant(
                                       {R"({"inventors":["w", "x"]})", R"({"inventors":[null]})",
                                        R"({"inventors":["m"]})", R"({"inventors":["n", null]})"},
                                       100))
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(rowsets.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();
    const int32_t path_column_id = column_id_by_path("v.inventors");
    ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());

    {
        ScopedDebugPoint debug_point("array_func.array_contains", {{"result_bitmap", "1"}});
        auto hit_read = read_rowsets(
                readable_rowsets.value(),
                make_array_contains_read_options(*tablet_schema(), path_column_id, "w"));
        ASSERT_TRUE(hit_read.has_value()) << hit_read.error();
        EXPECT_GT(debug_point.execute_num(), 0);
        EXPECT_EQ(hit_read->rows_read, 2);
        expect_index_filter_stats(hit_read.value(), 5);
        expect_index_probe_count(hit_read.value(),
                                 IndexProbeExpectation {
                                         .source = IndexProbeSource::EXPR_PUSHDOWN,
                                         .state = IndexProbeState::APPLIED,
                                         .reason = IndexFallbackReason::NONE,
                                         .column_uid = kVariantUid,
                                         .variant_path = std::string(kArrayPath),
                                         .index_id = kArrayPatternIndexId,
                                         .counts_toward_filter_stats = true,
                                         .filtered_rows = std::nullopt,
                                 },
                                 2);
    }

    {
        ScopedDebugPoint debug_point("array_func.array_contains", {{"result_bitmap", "0"}});
        auto miss_read = read_rowsets(
                readable_rowsets.value(),
                make_array_contains_read_options(*tablet_schema(), path_column_id, "missing"));
        ASSERT_TRUE(miss_read.has_value()) << miss_read.error();
        EXPECT_GT(debug_point.execute_num(), 0);
        EXPECT_EQ(miss_read->rows_read, 0);
        expect_index_filter_stats(miss_read.value(), 7);
        expect_index_probe_count(miss_read.value(),
                                 IndexProbeExpectation {
                                         .source = IndexProbeSource::EXPR_PUSHDOWN,
                                         .state = IndexProbeState::APPLIED,
                                         .reason = IndexFallbackReason::NONE,
                                         .column_uid = kVariantUid,
                                         .variant_path = std::string(kArrayPath),
                                         .index_id = kArrayPatternIndexId,
                                         .counts_toward_filter_stats = true,
                                         .filtered_rows = std::nullopt,
                                 },
                                 2);
    }

    auto disabled_options = make_array_contains_read_options(*tablet_schema(), path_column_id, "w");
    disabled_options.enable_inverted_index_query = false;
    auto disabled_read = read_rowsets(readable_rowsets.value(), std::move(disabled_options));
    ASSERT_TRUE(disabled_read.has_value()) << disabled_read.error();
    EXPECT_EQ(disabled_read->rows_read, 2);
    EXPECT_EQ(disabled_read->stats.rows_inverted_index_filtered, 0);
    expect_no_index_probe(disabled_read.value(), IndexProbeExpectation {
                                                         .source = IndexProbeSource::EXPR_PUSHDOWN,
                                                         .state = IndexProbeState::APPLIED,
                                                         .reason = std::nullopt,
                                                         .column_uid = kVariantUid,
                                                         .variant_path = std::string(kArrayPath),
                                                         .index_id = kArrayPatternIndexId,
                                                         .counts_toward_filter_stats = std::nullopt,
                                                         .filtered_rows = std::nullopt,
                                                 });

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 7);

    auto reloaded = reload_rowsets({compacted.value()});
    ASSERT_TRUE(reloaded.has_value()) << reloaded.error();
    auto readable_compacted = rowsets_with_variant_extended_schema(reloaded.value());
    ASSERT_TRUE(readable_compacted.has_value()) << readable_compacted.error();
    const int32_t compacted_path_column_id = column_id_by_path("v.inventors");
    ASSERT_EQ(compacted_path_column_id, path_column_id);

    ScopedDebugPoint debug_point("array_func.array_contains", {{"result_bitmap", "2"}});
    auto compacted_read = read_rowsets(
            readable_compacted.value(),
            make_array_contains_read_options(*tablet_schema(), compacted_path_column_id, "w"));
    ASSERT_TRUE(compacted_read.has_value()) << compacted_read.error();
    EXPECT_GT(debug_point.execute_num(), 0);
    EXPECT_EQ(compacted_read->rows_read, 2);
    expect_index_filter_stats(compacted_read.value(), 5);
    expect_index_probe_count(compacted_read.value(),
                             IndexProbeExpectation {
                                     .source = IndexProbeSource::EXPR_PUSHDOWN,
                                     .state = IndexProbeState::APPLIED,
                                     .reason = IndexFallbackReason::NONE,
                                     .column_uid = kVariantUid,
                                     .variant_path = std::string(kArrayPath),
                                     .index_id = kArrayPatternIndexId,
                                     .counts_toward_filter_stats = true,
                                     .filtered_rows = std::nullopt,
                             },
                             1);
}

TEST_F(IndexStorageVariantExprPushdownTest, ArrayContainsAllHitKeepsProbeStatsAtZeroFilteredRows) {
    const auto index_case =
            IndexStorageCaseBuilder("variant_array_contains_all_hit_probe_stats")
                    .tablet_id(110039)
                    .variant_column(array_text_variant_column())
                    .inverted_index(IndexSpec::field_pattern_index(kArrayPatternIndexId,
                                                                   "idx_v_inventors", kVariantUid,
                                                                   std::string(kArrayPath)))
                    .rowset(0,
                            IndexDataSourceSpec::inline_variant(
                                    {R"({"inventors":["w"]})", R"({"inventors":["w", "x"]})",
                                     R"({"inventors":["a", "w"]})", R"({"inventors":["w", null]})"},
                                    0))
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(rowsets.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();
    const int32_t path_column_id = column_id_by_path("v.inventors");
    ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());

    ScopedDebugPoint debug_point("array_func.array_contains", {{"result_bitmap", "4"}});
    auto read =
            read_rowsets(readable_rowsets.value(),
                         make_array_contains_read_options(*tablet_schema(), path_column_id, "w"));
    ASSERT_TRUE(read.has_value()) << read.error();
    EXPECT_GT(debug_point.execute_num(), 0);
    EXPECT_EQ(read->rows_read, 4);
    expect_index_filter_stats(read.value(), 0);
    expect_index_probe_count(read.value(),
                             IndexProbeExpectation {
                                     .source = IndexProbeSource::EXPR_PUSHDOWN,
                                     .state = IndexProbeState::APPLIED,
                                     .reason = IndexFallbackReason::NONE,
                                     .column_uid = kVariantUid,
                                     .variant_path = std::string(kArrayPath),
                                     .index_id = kArrayPatternIndexId,
                                     .segment_id = 0,
                                     .counts_toward_filter_stats = true,
                                     .input_rows = 4,
                                     .output_rows = 4,
                                     .filtered_rows = 0,
                             },
                             1);
}

// Mixed scalar/nested/unsupported JSON shapes keep semantic array_contains results while recording
// a precise index probe state.
TEST_F(IndexStorageVariantExprPushdownTest,
       ArrayContainsMixedNestedAndUnsupportedInputsRecordProbeState) {
    const auto index_case =
            IndexStorageCaseBuilder("variant_array_contains_mixed_nested_unsupported_probe")
                    .tablet_id(110043)
                    .variant_column(array_text_variant_column())
                    .inverted_index(IndexSpec::field_pattern_index(kArrayPatternIndexId,
                                                                   "idx_v_inventors", kVariantUid,
                                                                   std::string(kArrayPath)))
                    .rowset(0,
                            IndexDataSourceSpec::inline_variant(
                                    {R"({"inventors":["w", 1, true]})",
                                     R"({"inventors":[["w"], "x"]})",
                                     R"({"inventors":[{"name":"w"}, "y"]})", R"({"inventors":"w"})",
                                     R"({"inventors":{"name":"w"}})", R"({"inventors":["z"]})"},
                                    0))
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(rowsets.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();
    const int32_t path_column_id = column_id_by_path("v.inventors");
    ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());

    auto read =
            read_rowsets(readable_rowsets.value(),
                         make_array_contains_read_options(*tablet_schema(), path_column_id, "w"));
    ASSERT_TRUE(read.has_value()) << read.error();
    EXPECT_EQ(read->rows_read, 1);
    expect_index_probe(read.value(), IndexProbeExpectation {
                                             .source = IndexProbeSource::EXPR_PUSHDOWN,
                                             .state = IndexProbeState::APPLIED,
                                             .reason = IndexFallbackReason::NONE,
                                             .column_uid = kVariantUid,
                                             .variant_path = std::string(kArrayPath),
                                             .index_id = kArrayPatternIndexId,
                                             .counts_toward_filter_stats = true,
                                     });
}

} // namespace doris::index_storage_test
