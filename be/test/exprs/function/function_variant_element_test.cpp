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

#include "exprs/function/function_variant_element.cpp"

#include <gtest/gtest.h>

#include <array>
#include <utility>

#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/jsonb_value.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr_context.h"
#include "runtime/runtime_state.h"

namespace doris {
namespace {

ColumnVariantV2::MutablePtr nested_shredded_variant_v2() {
    constexpr std::array<std::string_view, 6> RESIDUAL {"{}", R"({"a":9})", R"({"a":{"other":2}})",
                                                        "{}", "{}",         R"({"a":null})"};
    JsonStringToVariantEncoder encoder;
    for (std::string_view json : RESIDUAL) {
        encoder.add_json({json.data(), json.size()});
    }
    auto residual = ColumnVariantV2::create();
    VariantBatchBuilder residual_batch = encoder.finish_batch();
    residual->insert_encoded_batch(residual_batch);

    auto values = ColumnInt64::create();
    auto child_nulls = ColumnUInt8::create();
    for (const auto [value, is_null] : std::array<std::pair<int64_t, uint8_t>, 6> {
                 {{7, 0}, {0, 1}, {8, 0}, {0, 1}, {0, 1}, {0, 1}}}) {
        values->insert_value(value);
        child_nulls->insert_value(is_null);
    }
    auto child = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(values), std::move(child_nulls)),
            std::make_shared<DataTypeInt64>());
    auto presence = ColumnUInt8::create();
    for (uint8_t present : {1, 0, 1, 0, 0, 0}) {
        presence->insert_value(present);
    }
    ColumnVariantV2::ShreddedFields fields;
    fields.emplace_back(PathInData(std::vector<std::string> {"a", "b"}), std::move(child),
                        std::move(presence));
    return ColumnVariantV2::create_shredded(std::move(residual), std::move(fields));
}

ColumnPtr execute_variant_v2_element(ColumnPtr source, const DataTypePtr& source_type,
                                     std::string_view key) {
    auto index_values = ColumnString::create();
    index_values->insert_data(key.data(), key.size());
    ColumnPtr index = ColumnConst::create(std::move(index_values), source->size());
    const auto index_type = std::make_shared<DataTypeString>();
    const DataTypePtr result_type = make_nullable(remove_nullable(source_type));
    Block block {{std::move(source), source_type, "source"},
                 {std::move(index), index_type, "index"},
                 {result_type->create_column(), result_type, "result"}};
    const Status status =
            FunctionVariantElement::create()->execute_impl(nullptr, block, {0, 1}, 2, block.rows());
    EXPECT_TRUE(status.ok()) << status;
    return block.get_by_position(2).column;
}

class CountingColumnExpr final : public VExpr {
public:
    CountingColumnExpr(ColumnPtr column, DataTypePtr type) : _column(std::move(column)) {
        _data_type = std::move(type);
    }

    const std::string& expr_name() const override {
        static const std::string name = "counting_variant_source";
        return name;
    }

    bool is_constant() const override { return false; }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr& result_column) const override {
        ++_executions;
        result_column = _column;
        return Status::OK();
    }

    size_t executions() const { return _executions; }

private:
    ColumnPtr _column;
    mutable size_t _executions = 0;
};

class CountingLiteral final : public VLiteral {
public:
    CountingLiteral(ColumnPtr column, DataTypePtr type) {
        _column_ptr = std::move(column);
        _data_type = std::move(type);
        _expr_name = "counting_literal";
        _node_type = TExprNodeType::LITERAL;
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        ++_executions;
        return VLiteral::execute_column_impl(context, block, selector, count, result_column);
    }

    size_t executions() const { return _executions; }

private:
    mutable size_t _executions = 0;
};

class CountingElementAtExpr final : public VectorizedFnCall {
public:
    void initialize(const VExprSPtr& source, const VExprSPtr& selector) {
        _function_name = "element_at";
        _expr_name = "element_at";
        _data_type = make_nullable(remove_nullable(source->data_type()));
        _function = std::make_shared<DefaultFunction>(
                FunctionVariantElement::create(),
                DataTypes {source->data_type(), selector->data_type()}, _data_type);
        set_children({source, selector});
        const Status status = prepare_variant_element_path_fusion_for_test();
        DORIS_CHECK(status.ok()) << status;
    }

    void bind_function_context(int index) { _fn_context_index = index; }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        ++_executions;
        return VectorizedFnCall::execute_column_impl(context, block, selector, count,
                                                     result_column);
    }

    size_t executions() const { return _executions; }

private:
    mutable size_t _executions = 0;
};

std::shared_ptr<CountingLiteral> string_literal(std::string_view value) {
    auto data = ColumnString::create();
    data->insert_data(value.data(), value.size());
    return std::make_shared<CountingLiteral>(ColumnConst::create(std::move(data), 1),
                                             std::make_shared<DataTypeString>());
}

std::shared_ptr<CountingLiteral> integer_literal(int64_t value) {
    auto data = ColumnInt64::create();
    data->insert_value(value);
    return std::make_shared<CountingLiteral>(ColumnConst::create(std::move(data), 1),
                                             std::make_shared<DataTypeInt64>());
}

std::shared_ptr<CountingLiteral> null_string_literal() {
    auto data = ColumnString::create();
    data->insert_default();
    auto nulls = ColumnUInt8::create();
    nulls->insert_value(1);
    const DataTypePtr type = make_nullable(std::make_shared<DataTypeString>());
    return std::make_shared<CountingLiteral>(
            ColumnConst::create(ColumnNullable::create(std::move(data), std::move(nulls)), 1),
            type);
}

std::shared_ptr<CountingElementAtExpr> element_at_expr(const VExprSPtr& source,
                                                       const VExprSPtr& selector) {
    auto expression = std::make_shared<CountingElementAtExpr>();
    expression->initialize(source, selector);
    return expression;
}

struct ElementAtChain {
    std::shared_ptr<CountingElementAtExpr> root;
    std::vector<std::shared_ptr<CountingElementAtExpr>> calls;
};

ElementAtChain element_at_chain(const VExprSPtr& source,
                                const std::vector<std::shared_ptr<CountingLiteral>>& selectors) {
    VExprSPtr current = source;
    ElementAtChain chain;
    chain.calls.reserve(selectors.size());
    for (const auto& selector : selectors) {
        auto call = element_at_expr(current, selector);
        chain.calls.push_back(call);
        current = call;
    }
    chain.root = chain.calls.back();
    return chain;
}

ColumnPtr execute_chain(const ElementAtChain& chain, size_t rows) {
    RuntimeState state;
    VExprContext context(chain.root);
    const int context_index =
            context.register_function_context(&state, chain.root->data_type(), {});
    for (const auto& call : chain.calls) {
        call->bind_function_context(context_index);
    }
    ColumnPtr result;
    const Status status = chain.root->execute_column(&context, nullptr, nullptr, rows, result);
    EXPECT_TRUE(status.ok()) << status;
    return result;
}

ColumnVariantV2::MutablePtr encoded_variant_v2(std::span<const std::string_view> rows) {
    JsonStringToVariantEncoder encoder;
    for (std::string_view row : rows) {
        encoder.add_json({row.data(), row.size()});
    }
    auto result = ColumnVariantV2::create();
    VariantBatchBuilder batch = encoder.finish_batch();
    result->insert_encoded_batch(batch);
    return result;
}

ColumnVariant::MutablePtr legacy_nested_variant() {
    auto result = ColumnVariant::create(1, false);
    ColumnVariant::Subcolumn leaf(0, true, false);
    leaf.insert(Field::create_field<TYPE_INT>(7));
    auto [paths, values] = result->get_sparse_data_paths_and_values();
    leaf.serialize_to_binary_column(paths, "a.b", values, 0);
    result->serialized_sparse_column_offsets().push_back(paths->size());
    result->get_subcolumn({})->insert_default();
    result->set_num_rows(1);
    result->get_doc_value_column_mutable().resize(1);
    return result;
}

const ColumnNullable& nullable_column(const ColumnPtr& column) {
    return assert_cast<const ColumnNullable&>(*column);
}

const ColumnVariantV2& variant_v2_column(const ColumnPtr& column) {
    return assert_cast<const ColumnVariantV2&>(nullable_column(column).get_nested_column());
}

} // namespace

TEST(function_variant_element_test, extract_from_sparse_column) {
    auto variant_column = ColumnVariant::create(1 /*max_subcolumns_count*/, false);
    auto* variant_ptr = variant_column.get();

    ColumnVariant::Subcolumn subcolumn(0, true, false);
    Field field = Field::create_field<TYPE_STRING>("John");
    subcolumn.insert(field);

    auto [sparse_column_keys, sparse_column_values] =
            variant_ptr->get_sparse_data_paths_and_values();
    auto& sparse_column_offsets = variant_ptr->serialized_sparse_column_offsets();
    subcolumn.serialize_to_binary_column(sparse_column_keys, "profile.age", sparse_column_values,
                                         0);
    subcolumn.serialize_to_binary_column(sparse_column_keys, "profile.name", sparse_column_values,
                                         0);
    subcolumn.serialize_to_binary_column(sparse_column_keys, "profile_id", sparse_column_values, 0);
    sparse_column_offsets.push_back(sparse_column_keys->size());
    variant_ptr->get_subcolumn({})->insert_default();
    variant_ptr->set_num_rows(1);
    variant_ptr->get_doc_value_column_mutable().resize(1);

    ColumnPtr result;
    ColumnPtr index_column_ptr = ColumnString::create();
    auto* index_column_ptr_mutable =
            assert_cast<ColumnString*>(index_column_ptr->assert_mutable().get());
    index_column_ptr_mutable->insert_data("profile", 7);
    ColumnPtr index_column = ColumnConst::create(index_column_ptr, 1);
    auto status =
            FunctionVariantElement::get_element_column(*variant_column, index_column, &result);
    EXPECT_TRUE(status.ok());

    DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;
    auto result_ptr = assert_cast<const ColumnVariant&>(*result.get());
    std::string result_string;
    result_ptr.serialize_one_row_to_string(0, &result_string, options);
    EXPECT_EQ(result_string, "{\"age\":\"John\",\"name\":\"John\"}");
}

// CIR-20498: extracting a string property from a scalar-string-root variant
// (the shape produced by `cast(text as variant)`) must return the raw string,
// not its JSON token with surrounding double quotes.
TEST(function_variant_element_test, extract_string_from_scalar_root) {
    auto variant_column = ColumnVariant::create(0 /*max_subcolumns_count*/, false);
    auto root_column = ColumnString::create();
    std::string doc = R"({"wsn":"SRFSPXFDVY","uploadTimeValue":"2026-05-20 18:40:02","n":49.98})";
    root_column->insert_data(doc.data(), doc.size());
    variant_column->create_root(std::make_shared<DataTypeString>(), std::move(root_column));
    variant_column->set_num_rows(1);
    ASSERT_TRUE(variant_column->is_scalar_variant());

    DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;

    auto extract = [&](const std::string& key) {
        ColumnPtr index_inner = ColumnString::create();
        assert_cast<ColumnString*>(index_inner->assert_mutable().get())
                ->insert_data(key.data(), key.size());
        ColumnPtr index_column = ColumnConst::create(index_inner, 1);
        ColumnPtr result;
        auto status =
                FunctionVariantElement::get_element_column(*variant_column, index_column, &result);
        EXPECT_TRUE(status.ok());
        std::string out;
        assert_cast<const ColumnVariant&>(*result.get())
                .serialize_one_row_to_string(0, &out, options);
        return out;
    };

    // string values: no surrounding quotes
    EXPECT_EQ(extract("wsn"), "SRFSPXFDVY");
    EXPECT_EQ(extract("uploadTimeValue"), "2026-05-20 18:40:02");
    // non-string scalars keep their JSON representation
    EXPECT_EQ(extract("n"), "49.98");
}

TEST(function_variant_element_test, exact_storage_json_null_remains_variant_null) {
    Slice null_json("null", 4);
    JsonBinaryValue null_value;
    ASSERT_TRUE(null_value.from_json_string(null_json.data, null_json.size).ok());
    ColumnVariant::Subcolumn null_subcolumn(0, true, false);
    null_subcolumn.insert(
            Field::create_field<TYPE_JSONB>(JsonbField(null_value.value(), null_value.size())));

    for (const bool use_doc_value : {false, true}) {
        SCOPED_TRACE(use_doc_value ? "doc" : "sparse");
        auto source = ColumnVariant::create(1, use_doc_value);
        auto [paths, values] = use_doc_value ? source->get_doc_value_data_paths_and_values()
                                             : source->get_sparse_data_paths_and_values();
        auto& offsets = use_doc_value ? source->serialized_doc_value_column_offsets()
                                      : source->serialized_sparse_column_offsets();
        null_subcolumn.serialize_to_binary_column(paths, "a", values, 0);
        offsets.push_back(paths->size());
        source->get_subcolumn({})->insert_default();
        source->set_num_rows(1);
        if (use_doc_value) {
            source->get_sparse_column_mutable().resize(1);
        } else {
            source->get_doc_value_column_mutable().resize(1);
        }

        ColumnPtr index_data = ColumnString::create();
        assert_cast<ColumnString*>(index_data->assert_mutable().get())->insert_data("a", 1);
        ColumnPtr index = ColumnConst::create(index_data, 1);
        const auto variant_type = std::make_shared<DataTypeVariant>();
        const auto index_type = std::make_shared<DataTypeString>();
        const auto result_type = make_nullable(variant_type);
        Block block {{std::move(source), variant_type, "source"},
                     {std::move(index), index_type, "index"},
                     {result_type->create_column(), result_type, "result"}};
        ASSERT_TRUE(
                FunctionVariantElement::create()->execute_impl(nullptr, block, {0, 1}, 2, 1).ok());

        const auto& nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        ASSERT_EQ(nullable.size(), 1);
        EXPECT_FALSE(nullable.is_null_at(0));

        DataTypeSerDe::FormatOptions options;
        auto timezone = cctz::utc_time_zone();
        options.timezone = &timezone;
        std::string logical_value;
        assert_cast<const ColumnVariant&>(nullable.get_nested_column())
                .serialize_one_row_to_string(0, &logical_value, options);
        EXPECT_EQ(logical_value, "null");
    }
}

TEST(function_variant_element_test, v2_nested_constant_path_preserves_shredded_intermediate) {
    auto source_values = nested_shredded_variant_v2();
    const ColumnVariantV2* source_identity = source_values.get();
    auto source_nulls = ColumnUInt8::create();
    for (uint8_t is_null : {0, 0, 0, 0, 1, 0}) {
        source_nulls->insert_value(is_null);
    }
    const DataTypePtr source_type = make_nullable(std::make_shared<DataTypeVariantV2>());
    ColumnPtr source = ColumnNullable::create(std::move(source_values), std::move(source_nulls));

    ColumnPtr ancestor = execute_variant_v2_element(source, source_type, "a");

    const auto& ancestor_nullable = assert_cast<const ColumnNullable&>(*ancestor);
    EXPECT_EQ(ancestor_nullable.get_null_map_data(), (NullMap {0, 0, 0, 1, 1, 0}));
    const auto& ancestor_values =
            assert_cast<const ColumnVariantV2&>(ancestor_nullable.get_nested_column());
    ASSERT_TRUE(ancestor_values.is_shredded());
    ASSERT_EQ(ancestor_values.shredded_field_count(), 1);
    EXPECT_EQ(ancestor_values.shredded_field_path(0).get_parts(),
              PathInData(std::vector<std::string> {"b"}).get_parts());
    EXPECT_TRUE(source_identity->is_shredded());
    auto materialized_ancestor =
            ancestor_values.materialize_encoded_range(0, ancestor_values.size());
    EXPECT_EQ(materialized_ancestor->get_value_ref(1).get_int(), 9);
    VariantRef row2 = materialized_ancestor->get_value_ref(2);
    ASSERT_EQ(row2.basic_type(), VariantBasicType::OBJECT);
    VariantRef row2_other;
    ASSERT_TRUE(row2.object_find(StringRef("other"), &row2_other));
    EXPECT_EQ(row2_other.get_int(), 2);
    EXPECT_TRUE(materialized_ancestor->get_value_ref(5).is_null());

    const ColumnVariantV2* ancestor_identity = &ancestor_values;
    ColumnPtr leaf = execute_variant_v2_element(ancestor, source_type, "b");

    const auto& leaf_nullable = assert_cast<const ColumnNullable&>(*leaf);
    EXPECT_EQ(leaf_nullable.get_null_map_data(), (NullMap {0, 1, 0, 1, 1, 1}));
    const auto& leaf_values =
            assert_cast<const ColumnVariantV2&>(leaf_nullable.get_nested_column());
    ASSERT_TRUE(leaf_values.is_typed());
    EXPECT_EQ(&leaf_values, &ancestor_values.shredded_field_values(0));
    const auto& typed = assert_cast<const ColumnNullable&>(leaf_values.typed_column());
    EXPECT_EQ(typed.get_null_map_data(), (NullMap {0, 1, 0, 1, 1, 1}));
    const auto& typed_values = assert_cast<const ColumnInt64&>(typed.get_nested_column());
    EXPECT_EQ(typed_values.get_element(0), 7);
    EXPECT_EQ(typed_values.get_element(2), 8);
    EXPECT_TRUE(source_identity->is_shredded());
    EXPECT_TRUE(ancestor_identity->is_shredded());
}

TEST(function_variant_element_test, v2_path_fusion_keeps_literal_tokens_separate) {
    constexpr std::array<std::string_view, 2> JSON_ROWS {R"({"a.b":{"c":11},"a":{"b":{"c":22}}})",
                                                         R"({"a.b":{"c":33},"a":{"b":{"c":44}}})"};
    auto values = encoded_variant_v2(JSON_ROWS);
    auto outer_nulls = ColumnUInt8::create();
    outer_nulls->insert_value(0);
    outer_nulls->insert_value(1);
    const DataTypePtr source_type = make_nullable(std::make_shared<DataTypeVariantV2>());
    ColumnPtr source_column = ColumnNullable::create(std::move(values), std::move(outer_nulls));

    auto dotted_source = std::make_shared<CountingColumnExpr>(source_column, source_type);
    auto dotted_key = string_literal("a.b");
    auto dotted_leaf = string_literal("c");
    ElementAtChain dotted = element_at_chain(dotted_source, {dotted_key, dotted_leaf});
    ColumnPtr dotted_result = execute_chain(dotted, 2);
    ASSERT_TRUE(static_cast<bool>(dotted_result));
    EXPECT_EQ(dotted_source->executions(), 1);
    EXPECT_EQ(dotted.calls.front()->executions(), 0);
    EXPECT_EQ(dotted.root->executions(), 1);
    EXPECT_EQ(dotted_key->executions(), 0);
    EXPECT_EQ(dotted_leaf->executions(), 0);
    EXPECT_EQ(nullable_column(dotted_result).get_null_map_data(), (NullMap {0, 1}));
    EXPECT_EQ(variant_v2_column(dotted_result).get_value_ref(0).get_int(), 11);

    auto segmented_source = std::make_shared<CountingColumnExpr>(source_column, source_type);
    auto a = string_literal("a");
    auto b = string_literal("b");
    auto c = string_literal("c");
    ElementAtChain segmented = element_at_chain(segmented_source, {a, b, c});
    ColumnPtr segmented_result = execute_chain(segmented, 2);
    ASSERT_TRUE(static_cast<bool>(segmented_result));
    EXPECT_EQ(segmented_source->executions(), 1);
    EXPECT_EQ(segmented.calls[0]->executions(), 0);
    EXPECT_EQ(segmented.calls[1]->executions(), 0);
    EXPECT_EQ(segmented.root->executions(), 1);
    EXPECT_EQ(nullable_column(segmented_result).get_null_map_data(), (NullMap {0, 1}));
    EXPECT_EQ(variant_v2_column(segmented_result).get_value_ref(0).get_int(), 22);
}

TEST(function_variant_element_test, v2_path_fusion_reuses_prepared_token_plan) {
    constexpr std::array<std::string_view, 2> JSON_ROWS {R"({"a":{"b":7}})", R"({"a":{"b":8}})"};
    const DataTypePtr source_type = std::make_shared<DataTypeVariantV2>();
    auto source = std::make_shared<CountingColumnExpr>(encoded_variant_v2(JSON_ROWS), source_type);
    ElementAtChain chain = element_at_chain(source, {string_literal("a"), string_literal("b")});
    ASSERT_EQ(chain.root->variant_element_path_plan_builds_for_test(), 1);

    ColumnPtr first = execute_chain(chain, JSON_ROWS.size());
    ColumnPtr second = execute_chain(chain, JSON_ROWS.size());
    ASSERT_TRUE(static_cast<bool>(first));
    ASSERT_TRUE(static_cast<bool>(second));
    EXPECT_EQ(chain.root->variant_element_path_plan_builds_for_test(), 1);
    EXPECT_EQ(source->executions(), 2);
    EXPECT_EQ(chain.calls.front()->executions(), 0);
    EXPECT_EQ(chain.root->executions(), 2);
    EXPECT_EQ(variant_v2_column(first).get_value_ref(0).get_int(), 7);
    EXPECT_EQ(variant_v2_column(second).get_value_ref(1).get_int(), 8);
}

TEST(function_variant_element_test, cloned_node_does_not_retain_prepared_path_plan) {
    constexpr std::array<std::string_view, 1> JSON_ROWS {R"({"a":{"b":7}})"};
    const DataTypePtr source_type = std::make_shared<DataTypeVariantV2>();
    auto source = std::make_shared<CountingColumnExpr>(encoded_variant_v2(JSON_ROWS), source_type);
    ElementAtChain chain = element_at_chain(source, {string_literal("a"), string_literal("b")});
    ASSERT_TRUE(chain.root->has_variant_element_path_plan_for_test());

    VExprSPtr cloned_expression;
    ASSERT_TRUE(chain.root->clone_node(&cloned_expression).ok());
    auto cloned = std::dynamic_pointer_cast<VectorizedFnCall>(cloned_expression);
    ASSERT_NE(cloned, nullptr);
    EXPECT_FALSE(cloned->has_variant_element_path_plan_for_test());
    EXPECT_EQ(cloned->variant_element_path_plan_builds_for_test(), 0);
}

TEST(function_variant_element_test, v2_all_null_result_uses_single_physical_row) {
    constexpr std::array<std::string_view, 3> JSON_ROWS {R"({"items":[1]})", R"({"items":[2]})",
                                                         R"({"items":[3]})"};
    const DataTypePtr source_type = std::make_shared<DataTypeVariantV2>();
    auto source = std::make_shared<CountingColumnExpr>(encoded_variant_v2(JSON_ROWS), source_type);
    auto items = string_literal("items");
    auto zero = integer_literal(0);
    ElementAtChain chain = element_at_chain(source, {items, zero});
    ASSERT_TRUE(chain.root->has_variant_element_path_plan_for_test());

    ColumnPtr result = execute_chain(chain, JSON_ROWS.size());
    ASSERT_TRUE(static_cast<bool>(result));
    EXPECT_EQ(source->executions(), 1);
    EXPECT_EQ(chain.calls.front()->executions(), 0);
    EXPECT_EQ(items->executions(), 0);
    EXPECT_EQ(zero->executions(), 0);
    const auto* constant = check_and_get_column<ColumnConst>(result.get());
    ASSERT_NE(constant, nullptr);
    EXPECT_EQ(constant->size(), JSON_ROWS.size());
    const auto& physical = assert_cast<const ColumnNullable&>(constant->get_data_column());
    EXPECT_EQ(physical.size(), 1);
    EXPECT_TRUE(physical.is_null_at(0));
    ColumnPtr materialized = result->convert_to_full_column_if_const();
    EXPECT_EQ(nullable_column(materialized).get_null_map_data(), (NullMap {1, 1, 1}));
}

TEST(function_variant_element_test, v2_all_null_result_keeps_zero_and_one_row_non_const) {
    constexpr std::array<std::string_view, 1> JSON_ROWS {R"([1])"};
    const DataTypePtr selector_type = std::make_shared<DataTypeInt64>();
    for (size_t rows : {0, 1}) {
        ColumnPtr source = encoded_variant_v2(std::span(JSON_ROWS).first(rows));
        auto selector_values = ColumnInt64::create();
        selector_values->insert_value(0);
        ColumnsWithTypeAndName selectors {
                {ColumnConst::create(std::move(selector_values), rows), selector_type, "index"}};

        ColumnPtr result;
        bool applied = false;
        const Status status =
                try_extract_variant_element_v2_path(source, selectors, &result, &applied);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_TRUE(applied);
        ASSERT_TRUE(static_cast<bool>(result));
        EXPECT_EQ(check_and_get_column<ColumnConst>(result.get()), nullptr);
        const auto& nullable = nullable_column(result);
        EXPECT_EQ(nullable.size(), rows);
        if (rows == 1) {
            EXPECT_TRUE(nullable.is_null_at(0));
        }
    }
}

TEST(function_variant_element_test, v2_path_fusion_handles_const_array_and_null_selectors) {
    constexpr std::array<std::string_view, 1> JSON_ROWS {R"({"items":[10,20]})"};
    ColumnPtr source_column = ColumnConst::create(encoded_variant_v2(JSON_ROWS), 3);
    const DataTypePtr source_type = std::make_shared<DataTypeVariantV2>();

    auto negative_source = std::make_shared<CountingColumnExpr>(source_column, source_type);
    auto items = string_literal("items");
    auto negative_one = integer_literal(-1);
    ElementAtChain negative = element_at_chain(negative_source, {items, negative_one});
    ColumnPtr negative_result = execute_chain(negative, 3);
    ASSERT_TRUE(static_cast<bool>(negative_result));
    EXPECT_EQ(negative_source->executions(), 1);
    EXPECT_EQ(negative.calls.front()->executions(), 0);
    EXPECT_EQ(negative.root->executions(), 1);
    ASSERT_NE(check_and_get_column<ColumnConst>(negative_result.get()), nullptr);
    ColumnPtr materialized_negative = negative_result->convert_to_full_column_if_const();
    const auto& negative_nullable = nullable_column(materialized_negative);
    EXPECT_EQ(negative_nullable.get_null_map_data(), (NullMap {0, 0, 0}));
    const auto& negative_values = variant_v2_column(materialized_negative);
    for (size_t row = 0; row < negative_values.size(); ++row) {
        EXPECT_EQ(negative_values.get_value_ref(row).get_int(), 20);
    }

    auto zero_source = std::make_shared<CountingColumnExpr>(source_column, source_type);
    ElementAtChain zero =
            element_at_chain(zero_source, {string_literal("items"), integer_literal(0)});
    ColumnPtr zero_result = execute_chain(zero, 3);
    ASSERT_TRUE(static_cast<bool>(zero_result));
    EXPECT_EQ(zero_source->executions(), 1);
    EXPECT_EQ(zero.calls.front()->executions(), 0);
    ASSERT_NE(check_and_get_column<ColumnConst>(zero_result.get()), nullptr);
    ColumnPtr materialized_zero = zero_result->convert_to_full_column_if_const();
    EXPECT_EQ(nullable_column(materialized_zero).get_null_map_data(), (NullMap {1, 1, 1}));

    auto null_source = std::make_shared<CountingColumnExpr>(source_column, source_type);
    ElementAtChain null =
            element_at_chain(null_source, {string_literal("items"), null_string_literal()});
    ColumnPtr null_result = execute_chain(null, 3);
    ASSERT_TRUE(static_cast<bool>(null_result));
    EXPECT_EQ(null_source->executions(), 1);
    EXPECT_EQ(null.calls.front()->executions(), 0);
    ASSERT_NE(check_and_get_column<ColumnConst>(null_result.get()), nullptr);
    ColumnPtr materialized_null = null_result->convert_to_full_column_if_const();
    EXPECT_EQ(nullable_column(materialized_null).get_null_map_data(), (NullMap {1, 1, 1}));
}

TEST(function_variant_element_test, v1_path_uses_original_nested_execution) {
    const DataTypePtr source_type = std::make_shared<DataTypeVariant>();
    auto source = std::make_shared<CountingColumnExpr>(legacy_nested_variant(), source_type);
    ElementAtChain chain = element_at_chain(source, {string_literal("a"), string_literal("b")});

    ColumnPtr result = execute_chain(chain, 1);
    ASSERT_TRUE(static_cast<bool>(result));
    EXPECT_EQ(source->executions(), 1);
    EXPECT_EQ(chain.calls.front()->executions(), 1);
    EXPECT_EQ(chain.root->executions(), 1);

    const auto& nullable = nullable_column(result);
    ASSERT_FALSE(nullable.is_null_at(0));
    const auto& values = assert_cast<const ColumnVariant&>(nullable.get_nested_column());
    DataTypeSerDe::FormatOptions options;
    auto timezone = cctz::utc_time_zone();
    options.timezone = &timezone;
    std::string logical_value;
    values.serialize_one_row_to_string(0, &logical_value, options);
    EXPECT_EQ(logical_value, "7");
}

TEST(function_variant_element_test, dynamic_selector_uses_original_nested_execution) {
    constexpr std::array<std::string_view, 1> JSON_ROWS {R"({"a":{"b":7}})"};
    const DataTypePtr source_type = std::make_shared<DataTypeVariantV2>();
    auto source = std::make_shared<CountingColumnExpr>(encoded_variant_v2(JSON_ROWS), source_type);
    auto inner_key = string_literal("a");
    auto inner = element_at_expr(source, inner_key);
    auto dynamic_data = ColumnString::create();
    dynamic_data->insert_data("b", 1);
    auto dynamic_selector = std::make_shared<CountingColumnExpr>(
            ColumnConst::create(std::move(dynamic_data), 1), std::make_shared<DataTypeString>());
    auto outer = element_at_expr(inner, dynamic_selector);
    ElementAtChain chain {.root = outer, .calls = {inner, outer}};

    ColumnPtr result = execute_chain(chain, 1);
    ASSERT_TRUE(static_cast<bool>(result));
    EXPECT_EQ(source->executions(), 1);
    EXPECT_EQ(inner->executions(), 1);
    EXPECT_EQ(outer->executions(), 1);
    EXPECT_EQ(inner_key->executions(), 1);
    EXPECT_EQ(dynamic_selector->executions(), 1);
    EXPECT_EQ(variant_v2_column(result).get_value_ref(0).get_int(), 7);
}

TEST(function_variant_element_test, runtime_filter_execution_does_not_fuse_path) {
    constexpr std::array<std::string_view, 1> JSON_ROWS {R"({"a":{"b":7}})"};
    const DataTypePtr source_type = std::make_shared<DataTypeVariantV2>();
    auto source = std::make_shared<CountingColumnExpr>(encoded_variant_v2(JSON_ROWS), source_type);
    auto first_key = string_literal("a");
    auto second_key = string_literal("b");
    ElementAtChain chain = element_at_chain(source, {first_key, second_key});
    RuntimeState state;
    VExprContext context(chain.root);
    const int context_index =
            context.register_function_context(&state, chain.root->data_type(), {});
    for (const auto& call : chain.calls) {
        call->bind_function_context(context_index);
    }

    ColumnPtr result;
    ColumnPtr first_argument;
    const Status status = chain.root->execute_runtime_filter(&context, nullptr, nullptr, 1, result,
                                                             &first_argument);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(static_cast<bool>(result));
    ASSERT_TRUE(static_cast<bool>(first_argument));
    EXPECT_EQ(source->executions(), 1);
    EXPECT_EQ(chain.calls.front()->executions(), 1);
    EXPECT_EQ(first_key->executions(), 1);
    EXPECT_EQ(second_key->executions(), 1);
    EXPECT_EQ(variant_v2_column(result).get_value_ref(0).get_int(), 7);
}

} // namespace doris
