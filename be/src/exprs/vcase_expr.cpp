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

#include "exprs/vcase_expr.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>

#include <algorithm>
#include <ostream>

#include "common/status.h"
#include "core/block/block.h"
#include "core/call_on_type_index.h"
#include "core/column/column.h"
#include "core/column/column_execute_util.h"
#include "core/custom_allocator.h"
#include "exec/common/hash_table/phmap_fwd_decl.h"
#include "exec/common/util.hpp"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "runtime/runtime_state.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
} // namespace doris

namespace doris {

namespace {

template <PrimitiveType PType>
constexpr bool supports_simple_case = is_number(PType) || PType == TYPE_STRING;

template <PrimitiveType PType>
constexpr size_t simple_case_lookup_threshold() {
    static_assert(supports_simple_case<PType>);
    if constexpr (sizeof(typename ColumnElementView<PType>::ElementType) <= sizeof(uint32_t)) {
        return 64;
    } else if constexpr (sizeof(typename ColumnElementView<PType>::ElementType) <=
                         sizeof(uint64_t)) {
        return 32;
    } else {
        return 8;
    }
}

template <PrimitiveType PType>
struct SimpleCaseLookupState {
    using Key = typename ColumnElementView<PType>::ElementType;
    using Hash = std::conditional_t<PType == TYPE_STRING, StringRefHash, phmap::Hash<Key>>;

    // StringRef keys borrow storage from the expression's VLiteral children.
    doris::flat_hash_map<Key, uint16_t, Hash> table;
    DorisVector<uint16_t> first_branch;
};

template <PrimitiveType PType>
bool match_simple_case_key(const ColumnView<PType>& input_view,
                           typename ColumnElementView<PType>::ElementType key,
                           uint16_t branch_index, DorisVector<uint16_t>& selected) {
    if (input_view.is_const) {
        const bool matches = input_view.value_at(0) == key;
        if (matches && selected[0] == 0) {
            std::ranges::fill(selected, branch_index);
        }
        return matches;
    }
    bool has_match = false;
    for (size_t row = 0; row < selected.size(); ++row) {
        // Compare before masking NULL rows to keep this loop branch-free and
        // friendly to automatic vectorization.
        bool matches = input_view.value_at(row) == key;
        matches &= !input_view.is_null_at(row);
        has_match |= matches;
        selected[row] = matches && selected[row] == 0 ? branch_index : selected[row];
    }
    return has_match;
}

template <PrimitiveType PType>
void lookup_simple_case_keys(const SimpleCaseLookupState<PType>& state,
                             const ColumnView<PType>& input_view, DorisVector<uint16_t>& selected,
                             DorisVector<uint8_t>& matched) {
    for (size_t row = 0; row < selected.size(); ++row) {
        if (input_view.is_null_at(row)) {
            continue;
        }
        const auto entry = state.table.find(input_view.value_at(row));
        if (entry != state.table.end()) {
            selected[row] = entry->second;
            matched[entry->second] = 1;
        }
    }
}

} // namespace

VCaseExpr::VCaseExpr(const TExprNode& node)
        : VExpr(node),
          _has_case_expr(node.case_expr.has_case_expr),
          _has_else_expr(node.case_expr.has_else_expr) {}

Status VCaseExpr::prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

    if (_has_case_expr) {
        DORIS_CHECK(_children.size() >= 3U + _has_else_expr);
        DORIS_CHECK((_children.size() - 1 - _has_else_expr) % 2 == 0);
        DORIS_CHECK((_children.size() - 1 - _has_else_expr) / 2 < UINT16_MAX);
        const auto input_type = remove_nullable(_children[0]->data_type());
        for (size_t i = 1; i < _children.size() - _has_else_expr; i += 2) {
            DORIS_CHECK(_children[i]->is_literal());
            DORIS_CHECK(!_children[i]->data_type()->is_nullable());
            const auto key_type = _children[i]->data_type();
            if (input_type->get_primitive_type() == TYPE_STRING ||
                input_type->get_primitive_type() == TYPE_VARCHAR) {
                DORIS_CHECK(key_type->get_primitive_type() == TYPE_STRING ||
                            key_type->get_primitive_type() == TYPE_VARCHAR);
            } else {
                DORIS_CHECK(input_type->equals(*key_type));
            }
        }
    }

    VExpr::register_function_context(state, context);
    _prepare_finished = true;
    return Status::OK();
}

Status VCaseExpr::open(RuntimeState* state, VExprContext* context,
                       FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    for (auto& i : _children) {
        RETURN_IF_ERROR(i->open(state, context, scope));
    }
    if (scope == FunctionContext::FunctionStateScope::FRAGMENT_LOCAL) {
        if (_has_case_expr) {
            _open_simple_case_lookup(context);
        }
        RETURN_IF_ERROR(VExpr::get_const_col(context, nullptr));
    }
    _open_finished = true;
    return Status::OK();
}

void VCaseExpr::_open_simple_case_lookup(VExprContext* context) {
    const size_t branch_count = (_children.size() - 1 - _has_else_expr) / 2;
    const auto initialize = [&]<typename Type>(Type) {
        constexpr auto PType = Type::PType;
        if constexpr (supports_simple_case<PType>) {
            if (branch_count < simple_case_lookup_threshold<PType>()) {
                return false;
            }
            auto state = std::make_shared<SimpleCaseLookupState<PType>>();
            state->table.reserve(branch_count);
            state->first_branch.resize(branch_count);
            for (size_t branch = 0; branch < branch_count; ++branch) {
                const auto& column =
                        assert_cast<const VLiteral&>(*_children[1 + branch * 2]).get_column_ptr();
                const auto key = ColumnView<PType>::create(column).value_at(0);
                const auto [entry, inserted] =
                        state->table.emplace(key, static_cast<uint16_t>(branch + 1));
                state->first_branch[branch] =
                        inserted ? static_cast<uint16_t>(branch + 1) : entry->second;
            }
            context->fn_context(_fn_context_index)
                    ->set_function_state(FunctionContext::FunctionStateScope::FRAGMENT_LOCAL,
                                         std::move(state));
            return true;
        }
        return false;
    };
    dispatch_switch_all(remove_nullable(_children[0]->data_type())->get_primitive_type(),
                        initialize);
}

void VCaseExpr::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    VExpr::close(context, scope);
}

Status VCaseExpr::execute_column_impl(VExprContext* context, const Block* block,
                                      const Selector* selector, size_t count,
                                      ColumnPtr& result_column) const {
    if (is_const_and_have_executed()) { // const have execute in open function
        result_column = get_result_from_const(count);
        return Status::OK();
    }
    DCHECK(_open_finished || block == nullptr);

    if (_has_case_expr) {
        if (count == 0) {
            result_column = data_type()->create_column();
            return Status::OK();
        }
        Status status;
        const auto execute = [&]<typename Type>(Type) {
            constexpr auto PType = Type::PType;
            if constexpr (supports_simple_case<PType>) {
                status =
                        _execute_simple_case<PType>(context, block, selector, count, result_column);
                return true;
            }
            return false;
        };
        const auto input_type = remove_nullable(_children[0]->data_type())->get_primitive_type();
        bool dispatched = dispatch_switch_number(input_type, execute);
        if (!dispatched && input_type != TYPE_CHAR) {
            dispatched = dispatch_switch_all(input_type, execute);
        }
        if (!dispatched) {
            return Status::NotSupported("simple case input type {} is not supported",
                                        _children[0]->data_type()->get_name());
        }
        return status;
    }

    size_t rows_count = count;
    std::vector<ColumnPtr> when_columns;
    std::vector<ColumnPtr> then_columns;

    if (_has_else_expr) {
        ColumnPtr else_column_ptr;
        RETURN_IF_ERROR(
                _children.back()->execute_column(context, block, selector, count, else_column_ptr));
        then_columns.emplace_back(else_column_ptr);
    } else {
        then_columns.emplace_back(nullptr);
    }

    for (int i = 0; i < _children.size() - _has_else_expr; i += 2) {
        ColumnPtr when_column_ptr;
        RETURN_IF_ERROR(
                _children[i]->execute_column(context, block, selector, count, when_column_ptr));
        if (calculate_false_number(when_column_ptr) == rows_count) {
            continue;
        }
        when_columns.emplace_back(when_column_ptr);
        ColumnPtr then_column_ptr;
        RETURN_IF_ERROR(
                _children[i + 1]->execute_column(context, block, selector, count, then_column_ptr));
        then_columns.emplace_back(then_column_ptr);
    }

    if (then_columns.size() > UINT16_MAX) {
        return Status::NotSupported(
                "case when do not support more than UINT16_MAX then conditions");
    } else if (then_columns.size() > UINT8_MAX) {
        result_column = _execute_impl<uint16_t>(when_columns, then_columns, rows_count);
    } else {
        result_column = _execute_impl<uint8_t>(when_columns, then_columns, rows_count);
    }
    if (result_column->size() != count) {
        return Status::InternalError("case when result column size {} not equal input count {}",
                                     result_column->size(), count);
    }
    return Status::OK();
}

template <PrimitiveType PType>
Status VCaseExpr::_execute_simple_case_lookup(VExprContext* context, const Block* block,
                                              const Selector* selector, size_t count,
                                              const ColumnPtr& input,
                                              ColumnPtr& result_column) const {
    const auto* state = static_cast<const SimpleCaseLookupState<PType>*>(
            context->fn_context(_fn_context_index)
                    ->get_function_state(FunctionContext::FunctionStateScope::FRAGMENT_LOCAL));
    DCHECK(state);
    const auto input_view = ColumnView<PType>::create(input);
    DorisVector<uint16_t> selected(count, 0);
    const size_t branch_count = (_children.size() - 1 - _has_else_expr) / 2;
    DorisVector<uint8_t> matched(branch_count + 1, 0);
    lookup_simple_case_keys(*state, input_view, selected, matched);
    std::vector<ColumnPtr> then_columns(branch_count + 1);
    if (_has_else_expr) {
        RETURN_IF_ERROR(
                _children.back()->execute_column(context, block, selector, count, then_columns[0]));
    }
    for (size_t branch = 0; branch < branch_count; ++branch) {
        if (matched[state->first_branch[branch]]) {
            RETURN_IF_ERROR(_children[2 + branch * 2]->execute_column(
                    context, block, selector, count, then_columns[branch + 1]));
        }
    }
    result_column = _execute_update_result<uint16_t>(selected.data(), then_columns, count);
    return Status::OK();
}

template <PrimitiveType PType>
Status VCaseExpr::_execute_simple_case(VExprContext* context, const Block* block,
                                       const Selector* selector, size_t count,
                                       ColumnPtr& result_column) const {
    const size_t branch_count = (_children.size() - 1 - _has_else_expr) / 2;
    ColumnPtr input;
    RETURN_IF_ERROR(_children[0]->execute_column(context, block, selector, count, input));
    if (input->only_null()) {
        if (!_has_else_expr) {
            result_column = data_type()->create_column_const(count, Field());
            return Status::OK();
        }
        return _children.back()->execute_column(context, block, selector, count, result_column);
    }
    const auto input_view = ColumnView<PType>::create(input);
    if (!input_view.is_const && branch_count >= simple_case_lookup_threshold<PType>()) {
        return _execute_simple_case_lookup<PType>(context, block, selector, count, input,
                                                  result_column);
    }
    DorisVector<uint16_t> selected(count, 0);
    std::vector<ColumnPtr> then_columns(branch_count + 1);
    if (_has_else_expr) {
        RETURN_IF_ERROR(
                _children.back()->execute_column(context, block, selector, count, then_columns[0]));
    }
    for (size_t branch = 0; branch < branch_count; ++branch) {
        ColumnPtr when_column;
        RETURN_IF_ERROR(_children[1 + branch * 2]->execute_column(context, block, selector, count,
                                                                  when_column));
        const auto key_view = ColumnView<PType>::create(when_column);
        DCHECK(key_view.is_const);
        const auto key = key_view.value_at(0);
        const auto branch_index = static_cast<uint16_t>(branch + 1);
        // Keep searched CASE's batch skipping, including shadowed duplicate keys.
        // A matching THEN still evaluates the entire batch, not just selected rows.
        if (match_simple_case_key(input_view, key, branch_index, selected)) {
            RETURN_IF_ERROR(_children[2 + branch * 2]->execute_column(
                    context, block, selector, count, then_columns[branch + 1]));
        }
    }
    result_column = _execute_update_result<uint16_t>(selected.data(), then_columns, count);
    return Status::OK();
}

const std::string& VCaseExpr::expr_name() const {
    return EXPR_NAME;
}

std::string VCaseExpr::debug_string() const {
    std::stringstream out;
    out << "CaseExpr(has_case_expr=" << _has_case_expr << " has_else_expr=" << _has_else_expr
        << " function=" << FUNCTION_NAME << "){";
    bool first = true;
    for (const auto& input_expr : children()) {
        if (first) {
            first = false;
        } else {
            out << ",";
        }
        out << input_expr->debug_string();
    }
    out << "}";
    return out.str();
}

} // namespace doris
