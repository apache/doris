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

#include "exprs/variant_element_path_fusion.h"

#include <algorithm>
#include <optional>
#include <vector>

#include "common/check.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/typeid_cast.h"
#include "exprs/function/function_variant_element_v2.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vliteral.h"

namespace doris {

namespace {

struct ConsecutiveVariantElementPath {
    VExprSPtr root;
    std::vector<const VLiteral*> selectors;
};

const VLiteral* supported_selector(const VExpr& selector) {
    const auto* literal = dynamic_cast<const VLiteral*>(&selector);
    if (literal == nullptr) {
        return nullptr;
    }
    const DataTypePtr& type = selector.data_type();
    if (type->is_null_literal()) {
        return literal;
    }
    const PrimitiveType primitive = remove_nullable(type)->get_primitive_type();
    return is_string_type(primitive) || is_int_or_bool(primitive) ? literal : nullptr;
}

std::optional<ConsecutiveVariantElementPath> collect_path(const VectorizedFnCall& expression) {
    const VectorizedFnCall* current = &expression;
    ConsecutiveVariantElementPath path;
    while (current->function_name() == "element_at") {
        const VExprSPtrs& children = current->children();
        const VLiteral* selector =
                children.size() == 2 ? supported_selector(*children[1]) : nullptr;
        if (children.size() != 2 ||
            remove_nullable(children[0]->data_type())->get_primitive_type() != TYPE_VARIANT ||
            selector == nullptr) {
            return std::nullopt;
        }
        path.selectors.push_back(selector);

        const auto* inner = dynamic_cast<const VectorizedFnCall*>(children[0].get());
        if (inner == nullptr || inner->function_name() != "element_at") {
            path.root = children[0];
            break;
        }
        current = inner;
    }

    if (!path.root || path.selectors.size() < 2 ||
        typeid_cast<const DataTypeVariantV2*>(remove_nullable(path.root->data_type()).get()) ==
                nullptr) {
        return std::nullopt;
    }
    std::ranges::reverse(path.selectors);
    return path;
}

} // namespace

class VariantElementPathFusionPlan final {
public:
    VariantElementPathFusionPlan(VExprSPtr root,
                                 std::shared_ptr<const VariantElementV2PathPlan> path)
            : root(std::move(root)), path(std::move(path)) {}

    const VExprSPtr root;
    const std::shared_ptr<const VariantElementV2PathPlan> path;
};

Status build_variant_element_path_fusion_plan(
        const VectorizedFnCall& expression,
        // The plan is published only after the root and every owned path token are validated.
        // NOLINTNEXTLINE(readability-non-const-parameter)
        std::shared_ptr<const VariantElementPathFusionPlan>* output) {
    if (output == nullptr) {
        return Status::InvalidArgument("Variant element fusion plan output is null");
    }
    output->reset();
    std::optional<ConsecutiveVariantElementPath> path = collect_path(expression);
    if (!path.has_value()) {
        return Status::OK();
    }

    ColumnsWithTypeAndName selector_columns;
    selector_columns.reserve(path->selectors.size());
    for (const VLiteral* selector_expression : path->selectors) {
        selector_columns.emplace_back(selector_expression->get_column_ptr(),
                                      selector_expression->data_type(),
                                      selector_expression->expr_name());
    }
    std::shared_ptr<const VariantElementV2PathPlan> token_plan;
    RETURN_IF_ERROR(build_variant_element_v2_path_plan(selector_columns, &token_plan));
    output->reset(new VariantElementPathFusionPlan(std::move(path->root), std::move(token_plan)));
    return Status::OK();
}

Status try_execute_variant_element_path_fusion(
        const std::shared_ptr<const VariantElementPathFusionPlan>& plan, VExprContext* context,
        const Block* block, const Selector* selector, size_t count,
        // Outputs are published only after their pointers have been validated.
        // NOLINTNEXTLINE(readability-non-const-parameter)
        ColumnPtr* output, bool* executed) {
    if (output == nullptr || executed == nullptr) {
        return Status::InvalidArgument("Variant element path fusion output is null");
    }
    *executed = false;
    if (!plan) {
        return Status::OK();
    }

    ColumnPtr root_column;
    RETURN_IF_ERROR(plan->root->execute_column(context, block, selector, count, root_column));

    ColumnPtr candidate;
    bool applied = false;
    RETURN_IF_ERROR(
            try_extract_variant_element_v2_path(root_column, *plan->path, &candidate, &applied));
    DORIS_CHECK(applied)
            << "DataTypeVariantV2 expression produced a non-ColumnVariantV2 physical column";
    output->swap(candidate);
    *executed = true;
    return Status::OK();
}

} // namespace doris
