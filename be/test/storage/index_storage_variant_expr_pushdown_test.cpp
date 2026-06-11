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
    options.target_cast_type_for_variants[path_column.name()] = std::make_shared<DataTypeInt32>();
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
                    .rowset(0,
                            IndexDataSourceSpec::inline_variant(
                                    {R"({"int_1": 42})", R"({"int_2": 7})", R"({"int_3": 8})"}, 0))
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
    EXPECT_EQ(not_null_read->rows_read, 1);
    EXPECT_EQ(not_null_read->stats.rows_inverted_index_filtered, 2);
    expect_index_probe(not_null_read.value(), IndexProbeExpectation {
                                                      .source = IndexProbeSource::EXPR_PUSHDOWN,
                                                      .state = IndexProbeState::APPLIED,
                                                      .reason = IndexFallbackReason::NONE,
                                                      .column_uid = kVariantUid,
                                                      .variant_path = "int_1",
                                                      .index_id = kIntPatternIndexId,
                                                      .counts_toward_filter_stats = true,
                                                      .filtered_rows = 2,
                                              });

    auto null_read =
            read_rowsets(readable_rowsets.value(),
                         make_is_null_read_options(*tablet_schema(), path_column_id, true));
    ASSERT_TRUE(null_read.has_value()) << null_read.error();
    EXPECT_EQ(null_read->rows_read, 2);
    EXPECT_EQ(null_read->stats.rows_inverted_index_filtered, 1);
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

} // namespace doris::index_storage_test
