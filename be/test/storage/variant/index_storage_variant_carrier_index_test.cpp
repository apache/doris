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
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/primitive_type.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "storage/predicate/predicate_creator.h"
#include "storage/segment/segment_loader.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/variant/index_storage_variant_test_base.h"
#include "exec/common/variant_util.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris::index_storage_test {
namespace {

constexpr int32_t kVariantUid = 2;
constexpr int64_t kRootIndexId = 310001;

VariantColumnSpec variant_with_b_template(std::optional<FieldType> type) {
    VariantColumnSpec column;
    column.unique_id = kVariantUid;
    column.max_subcolumns_count = 8;
    if (type.has_value()) {
        column.predefined_paths.push_back(
                VariantPathSpec {.path = "b*",
                                 .type = type.value(),
                                 .nullable = true,
                                 .pattern_type = PatternTypePB::MATCH_NAME_GLOB,
                                 .array_item_type = {},
                                 .array_item_nullable = true});
    }
    return column;
}

std::shared_ptr<ColumnPredicate> int_equals(int32_t column_id, std::string name, int32_t value) {
    return create_comparison_predicate<PredicateType::EQ>(
            column_id, std::move(name), std::make_shared<DataTypeInt32>(),
            Field::create_field<TYPE_INT>(value), false);
}

VExprSPtr make_literal(PrimitiveType type, const std::string& value) {
    TExprNode node;
    node.__set_type(create_type_desc(type));
    node.__set_num_children(0);
    node.__set_is_nullable(false);
    if (type == PrimitiveType::TYPE_STRING) {
        node.__set_node_type(TExprNodeType::STRING_LITERAL);
        TStringLiteral literal;
        literal.__set_value(value);
        node.__set_string_literal(literal);
    } else {
        node.__set_node_type(TExprNodeType::INT_LITERAL);
        TIntLiteral literal;
        literal.__set_value(std::stoll(value));
        node.__set_int_literal(literal);
    }
    return VLiteral::create_shared(node);
}

VExprSPtr make_fn(const std::string& name, TExprNodeType::type node_type,
                  std::vector<PrimitiveType> arg_types, PrimitiveType ret_type,
                  std::vector<VExprSPtr> children) {
    TFunctionName fn_name;
    fn_name.__set_db_name("");
    fn_name.__set_function_name(name);
    TFunction fn;
    fn.__set_name(fn_name);
    fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
    std::vector<TTypeDesc> args;
    for (auto t : arg_types) {
        args.push_back(create_type_desc(t));
    }
    fn.__set_arg_types(args);
    fn.__set_ret_type(create_type_desc(ret_type));
    fn.__set_has_var_args(false);
    TExprNode node;
    node.__set_type(create_type_desc(ret_type));
    node.__set_node_type(node_type);
    node.__set_fn(fn);
    node.__set_num_children(static_cast<int>(children.size()));
    node.__set_is_nullable(true);
    auto root = VectorizedFnCall::create_shared(node);
    for (auto& child : children) {
        root->add_child(child);
    }
    return root;
}

// Block position 1 holds v.b1 (return_columns = {0, cid}).
// (b1 + 1) > value, evaluated as a pushed-down common expression on the INT-bound slot.
VExprContextSPtr int_plus_one_greater(int64_t value) {
    auto slot = std::make_shared<MockSlotRef>(1, make_nullable(std::make_shared<DataTypeInt32>()));
    auto add = make_fn("add", TExprNodeType::ARITHMETIC_EXPR,
                       {PrimitiveType::TYPE_INT, PrimitiveType::TYPE_INT},
                       PrimitiveType::TYPE_INT,
                       {slot, make_literal(PrimitiveType::TYPE_INT, "1")});
    auto gt = make_fn("gt", TExprNodeType::BINARY_PRED,
                      {PrimitiveType::TYPE_INT, PrimitiveType::TYPE_INT},
                      PrimitiveType::TYPE_BOOLEAN,
                      {add, make_literal(PrimitiveType::TYPE_INT, std::to_string(value))});
    return VExprContext::create_shared(gt);
}

// like(b1, pattern) on the STRING-bound slot.
VExprContextSPtr string_like(const std::string& pattern) {
    auto slot = std::make_shared<MockSlotRef>(1, nullable_string_target_type());
    return VExprContext::create_shared(
            make_fn("like", TExprNodeType::FUNCTION_CALL,
                    {PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_STRING},
                    PrimitiveType::TYPE_BOOLEAN,
                    {slot, make_literal(PrimitiveType::TYPE_STRING, pattern)}));
}

} // namespace

// A root-level inverted index is inherited by template-typed paths. A compaction carrier keeps old
// dynamic values as JSONB and writes no physical index, so reads must scan it instead of failing.
class IndexStorageVariantCarrierIndexTest : public IndexStorageTestFixture {
protected:
    IndexTabletOptions options(const VariantColumnSpec& variant, int64_t tablet_id) {
        IndexTabletOptions result;
        result.tablet_id = tablet_id;
        result.variant_columns = {variant};
        result.inverted_indexes = {IndexSpec::column_index(kRootIndexId, "idx_v", kVariantUid)};
        return result;
    }

    Result<RowsetSharedPtr> write(int64_t version, const std::vector<std::string>& jsons) {
        IndexRowsetSpec rowset;
        rowset.version = version;
        rowset.batches.push_back(
                IndexBatch::single_variant(jsons, static_cast<int32_t>(version * 4)));
        auto written = write_rowset(rowset);
        if (written.has_value()) {
            SegmentLoader::instance()->erase_segments(*written.value()->rowset_meta());
        }
        return written;
    }

    void install_write_schema(const VariantColumnSpec& variant) {
        auto target = build_tablet_schema(options(variant, tablet()->tablet_id()));
        target->set_schema_version(tablet_schema()->schema_version() + 1);
        _tablet_schema = target;
        tablet()->tablet_meta()->_schema = target;
        tablet()->update_max_version_schema(target);
    }

    // FE sends the template-bound subcolumn; the segments keep their own rowset schemas. An empty
    // predicate value reads the subcolumn values without a filter.
    Result<IndexReadResult> query_b1(const std::vector<RowsetSharedPtr>& rowsets,
                                     FieldType template_type, bool use_index,
                                     std::optional<std::string> value = std::string("x1"),
                                     VExprContextSPtr expr = nullptr) {
        auto write_schema = _tablet_schema;
        auto read_schema = variant_util::VariantCompactionUtil::calculate_variant_extended_schema(
                rowsets, write_schema);
        if (read_schema == nullptr) {
            return ResultError(Status::InternalError("no extended schema"));
        }
        _tablet_schema = read_schema;
        const int32_t cid = column_id_by_path("v.b1");
        if (cid < 0) {
            _tablet_schema = write_schema;
            return ResultError(Status::InternalError("v.b1 is not in the read schema"));
        }
        // FE binds v['b1'] to the template type no matter what the segment stored.
        read_schema->mutable_column(cid).set_type(template_type);
        const auto& column = _tablet_schema->column(cid);
        IndexReadOptions read;
        read.return_columns = {0, static_cast<uint32_t>(cid)};
        read.enable_inverted_index_query = use_index;
        read.inverted_index_skip_threshold = 100;
        read.collect_string_values = !value.has_value();
        if (template_type == FieldType::OLAP_FIELD_TYPE_INT) {
            read.target_cast_type_for_variants[column.name()] =
                    make_nullable(std::make_shared<DataTypeInt32>());
            if (value.has_value()) {
                read.predicates.push_back(int_equals(cid, column.name(), std::stoi(*value)));
            }
        } else {
            read.target_cast_type_for_variants[column.name()] = nullable_string_target_type();
            if (value.has_value()) {
                read.predicates.push_back(string_equals(cid, column.name(), *value));
            }
        }
        if (expr != nullptr) {
            read.common_expr_ctxs_push_down.push_back(std::move(expr));
        }
        auto result = read_rowsets(rowsets, read);
        _tablet_schema = write_schema;
        return result;
    }

    // Old dynamic rowset, ADD template, recent typed rowset, then compaction into a JSONB carrier.
    void build_carrier(int64_t tablet_id, FieldType type, const std::vector<std::string>& old_rows,
                       const std::vector<std::string>& recent_rows,
                       std::vector<RowsetSharedPtr>* uncompacted, RowsetSharedPtr* compacted) {
        ASSERT_TRUE(create_tablet(options(variant_with_b_template(std::nullopt), tablet_id)).ok());
        auto old = write(0, old_rows);
        ASSERT_TRUE(old.has_value()) << old.error();
        install_write_schema(variant_with_b_template(type));
        auto recent = write(1, recent_rows);
        ASSERT_TRUE(recent.has_value()) << recent.error();
        *uncompacted = {old.value(), recent.value()};
        auto output = compact_rowsets_and_reload(IndexCompactionKind::CUMULATIVE, *uncompacted);
        ASSERT_TRUE(output.has_value()) << output.error();
        *compacted = output.value();
    }

    static std::vector<std::string> sorted_strings(const IndexReadResult& result) {
        std::vector<std::string> values;
        for (const auto& [uid, column_values] : result.string_values_by_uid) {
            for (const auto& v : column_values) {
                values.push_back(v.value_or("<null>"));
            }
        }
        std::sort(values.begin(), values.end());
        return values;
    }
};

// A STRING template added over old dynamic STRING b1: compaction writes b1 as a JSONB carrier.
// Typed predicates must return the same rows as the uncompacted read, with or without the index.
TEST_F(IndexStorageVariantCarrierIndexTest, AddedStringTemplateCarrierFiltersExactRows) {
    std::vector<RowsetSharedPtr> uncompacted;
    RowsetSharedPtr compacted;
    build_carrier(310100, FieldType::OLAP_FIELD_TYPE_STRING,
                  {R"({"b1":"x1","z":0})", R"({"b1":"x2","z":1})"}, {R"({"b1":"x1","z":2})"},
                  &uncompacted, &compacted);
    if (HasFatalFailure()) {
        return;
    }
    for (const auto& [value, expected] :
         std::vector<std::pair<std::string, int64_t>> {{"x1", 2}, {"x2", 1}, {"x9", 0}}) {
        auto baseline = query_b1(uncompacted, FieldType::OLAP_FIELD_TYPE_STRING, false, value);
        ASSERT_TRUE(baseline.has_value()) << baseline.error();
        ASSERT_EQ(baseline->rows_read, expected) << "uncompacted baseline, value=" << value;
        for (bool use_index : {false, true}) {
            auto result = query_b1({compacted}, FieldType::OLAP_FIELD_TYPE_STRING, use_index, value);
            ASSERT_TRUE(result.has_value()) << result.error();
            EXPECT_EQ(result->rows_read, expected)
                    << "compacted carrier, value=" << value << " use_index=" << use_index;
            // The JSONB carrier has no physical index file entry to accelerate the predicate.
            EXPECT_FALSE(result->inverted_index_used());
        }
    }
    auto baseline_values =
            query_b1(uncompacted, FieldType::OLAP_FIELD_TYPE_STRING, false, std::nullopt);
    ASSERT_TRUE(baseline_values.has_value()) << baseline_values.error();
    auto values = query_b1({compacted}, FieldType::OLAP_FIELD_TYPE_STRING, false, std::nullopt);
    ASSERT_TRUE(values.has_value()) << values.error();
    EXPECT_EQ(sorted_strings(*baseline_values), (std::vector<std::string> {"x1", "x1", "x2"}));
    EXPECT_EQ(sorted_strings(*values), sorted_strings(*baseline_values));
}

// An INT template added over old dynamic integer b1. Zone maps of the JSONB carrier must not be
// compared with an INT predicate (that crashed with a Field::get type mismatch).
TEST_F(IndexStorageVariantCarrierIndexTest, AddedIntTemplateCarrierFiltersExactRows) {
    std::vector<RowsetSharedPtr> uncompacted;
    RowsetSharedPtr compacted;
    build_carrier(310300, FieldType::OLAP_FIELD_TYPE_INT,
                  {R"({"b1":7,"z":0})", R"({"b1":8,"z":1})"}, {R"({"b1":7,"z":2})"},
                  &uncompacted, &compacted);
    if (HasFatalFailure()) {
        return;
    }
    for (const auto& [value, expected] :
         std::vector<std::pair<std::string, int64_t>> {{"7", 2}, {"8", 1}, {"9", 0}}) {
        auto baseline = query_b1(uncompacted, FieldType::OLAP_FIELD_TYPE_INT, false, value);
        ASSERT_TRUE(baseline.has_value()) << baseline.error();
        ASSERT_EQ(baseline->rows_read, expected) << "uncompacted baseline, value=" << value;
        for (bool use_index : {false, true}) {
            auto result = query_b1({compacted}, FieldType::OLAP_FIELD_TYPE_INT, use_index, value);
            ASSERT_TRUE(result.has_value()) << result.error();
            EXPECT_EQ(result->rows_read, expected)
                    << "compacted carrier, value=" << value << " use_index=" << use_index;
        }
    }
    auto all = query_b1({compacted}, FieldType::OLAP_FIELD_TYPE_INT, false, std::nullopt);
    ASSERT_TRUE(all.has_value()) << all.error();
    EXPECT_EQ(all->rows_read, 3);
}

// Control: a never-altered templated column still uses the template-derived root index.
TEST_F(IndexStorageVariantCarrierIndexTest, NeverAlteredTemplateKeepsInheritedIndex) {
    ASSERT_TRUE(create_tablet(options(variant_with_b_template(FieldType::OLAP_FIELD_TYPE_STRING),
                                      310200))
                        .ok());
    auto first = write(0, {R"({"b1":"x1","z":0})", R"({"b1":"x2","z":1})"});
    ASSERT_TRUE(first.has_value()) << first.error();
    auto second = write(1, {R"({"b1":"x3","z":2})"});
    ASSERT_TRUE(second.has_value()) << second.error();
    auto output = compact_rowsets_and_reload(IndexCompactionKind::CUMULATIVE,
                                             {first.value(), second.value()});
    ASSERT_TRUE(output.has_value()) << output.error();
    for (const auto& rowsets : std::vector<std::vector<RowsetSharedPtr>> {
                 {first.value(), second.value()}, {output.value()}}) {
        auto result = query_b1(rowsets, FieldType::OLAP_FIELD_TYPE_STRING, true);
        ASSERT_TRUE(result.has_value()) << result.error();
        EXPECT_EQ(result->rows_read, 1);
        EXPECT_TRUE(result->inverted_index_used());
    }
}

// Pushed-down common expressions (the TopN / runtime-filter / remaining-conjunct path) must see
// the template-typed value, not the JSONB carrier, and return the uncompacted result.
TEST_F(IndexStorageVariantCarrierIndexTest, AddedIntTemplateCarrierCommonExprExactRows) {
    std::vector<RowsetSharedPtr> uncompacted;
    RowsetSharedPtr compacted;
    build_carrier(310400, FieldType::OLAP_FIELD_TYPE_INT,
                  {R"({"b1":7,"z":0})", R"({"b1":8,"z":1})"}, {R"({"b1":7,"z":2})"},
                  &uncompacted, &compacted);
    if (HasFatalFailure()) {
        return;
    }
    for (const auto& [bound, expected] :
         std::vector<std::pair<int64_t, int64_t>> {{8, 1}, {7, 3}, {9, 0}}) {
        auto baseline = query_b1(uncompacted, FieldType::OLAP_FIELD_TYPE_INT, false, std::nullopt,
                                 int_plus_one_greater(bound));
        ASSERT_TRUE(baseline.has_value()) << baseline.error();
        ASSERT_EQ(baseline->rows_read, expected) << "uncompacted baseline, b1+1>" << bound;
        for (bool use_index : {false, true}) {
            auto result = query_b1({compacted}, FieldType::OLAP_FIELD_TYPE_INT, use_index,
                                   std::nullopt, int_plus_one_greater(bound));
            ASSERT_TRUE(result.has_value()) << result.error();
            EXPECT_EQ(result->rows_read, expected)
                    << "compacted carrier, b1+1>" << bound << " use_index=" << use_index;
        }
    }
}

TEST_F(IndexStorageVariantCarrierIndexTest, AddedStringTemplateCarrierLikeExactRows) {
    std::vector<RowsetSharedPtr> uncompacted;
    RowsetSharedPtr compacted;
    build_carrier(310500, FieldType::OLAP_FIELD_TYPE_STRING,
                  {R"({"b1":"x1","z":0})", R"({"b1":"y2","z":1})"}, {R"({"b1":"x3","z":2})"},
                  &uncompacted, &compacted);
    if (HasFatalFailure()) {
        return;
    }
    for (const auto& [pattern, expected] : std::vector<std::pair<std::string, int64_t>> {
                 {"x%", 2}, {"y2", 1}, {"%1", 1}, {"z%", 0}}) {
        auto baseline = query_b1(uncompacted, FieldType::OLAP_FIELD_TYPE_STRING, false,
                                 std::nullopt, string_like(pattern));
        ASSERT_TRUE(baseline.has_value()) << baseline.error();
        ASSERT_EQ(baseline->rows_read, expected) << "uncompacted baseline, like " << pattern;
        auto baseline_values = sorted_strings(*baseline);
        for (bool use_index : {false, true}) {
            auto result = query_b1({compacted}, FieldType::OLAP_FIELD_TYPE_STRING, use_index,
                                   std::nullopt, string_like(pattern));
            ASSERT_TRUE(result.has_value()) << result.error();
            EXPECT_EQ(result->rows_read, expected)
                    << "compacted carrier, like " << pattern << " use_index=" << use_index;
            EXPECT_EQ(sorted_strings(*result), baseline_values) << "like " << pattern;
        }
    }
}

} // namespace doris::index_storage_test
