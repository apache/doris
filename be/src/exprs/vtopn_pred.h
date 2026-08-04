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

#pragma once

#include <gen_cpp/types.pb.h>

#include <utility>

#include "core/block/column_numbers.h"
#include "core/data_type/data_type.h"
#include "exec/common/util.hpp"
#include "exprs/function/simple_function_factory.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr.h"
#include "exprs/vslot_ref.h"
#include "runtime/query_context.h"
#include "runtime/runtime_predicate.h"
#include "runtime/runtime_state.h"

namespace doris {

// only used for dynamic topn filter
class VTopNPred : public VExpr {
    ENABLE_FACTORY_CREATOR(VTopNPred);

public:
    VTopNPred(const TExprNode& node, int source_node_id, VExprContextSPtr target_ctx)
            : VExpr(node),
              _source_node_id(source_node_id),
              _expr_name(fmt::format("VTopNPred(source_node_id={})", _source_node_id)),
              _target_ctx(std::move(target_ctx)) {}
    bool is_topn_filter() const override { return true; }

    static Status create_vtopn_pred(const TExpr& target_expr, int source_node_id, VExprSPtr& expr) {
        VExprContextSPtr target_ctx;
        RETURN_IF_ERROR(VExpr::create_expr_tree(target_expr, target_ctx));

        TExprNode node;
        node.__set_node_type(TExprNodeType::FUNCTION_CALL);
        node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        node.__set_is_nullable(target_ctx->root()->is_nullable());
        expr = VTopNPred::create_shared(node, source_node_id, target_ctx);

        DCHECK(target_ctx->root() != nullptr);
        expr->add_child(target_ctx->root());

        return Status::OK();
    }

    int source_node_id() const { return _source_node_id; }
    Status clone_node(VExprSPtr* cloned_expr) const override {
        DORIS_CHECK(cloned_expr != nullptr);
        *cloned_expr = VTopNPred::create_shared(clone_texpr_node(), _source_node_id, nullptr);
        return Status::OK();
    }

    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override {
        _predicate = &state->get_query_ctx()->get_runtime_predicate(_source_node_id);
        RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

        ColumnsWithTypeAndName argument_template;
        argument_template.emplace_back(nullptr, _children[0]->data_type(),
                                       _children[0]->expr_name());
        argument_template.emplace_back(nullptr, _children[0]->data_type(), "topn value");

        _function = SimpleFunctionFactory::instance().get_function(
                _predicate->is_asc() ? "le" : "ge", argument_template, _data_type, {},
                state->be_exec_version());
        if (!_function) {
            return Status::InternalError("get function failed");
        }
        return Status::OK();
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        if (!_predicate->has_value()) {
            result_column = create_always_true_column(count, _data_type->is_nullable());
            return Status::OK();
        }

        Block temp_block;

        // slot
        ColumnPtr slot_column;
        RETURN_IF_ERROR(_children[0]->execute_column(context, block, selector, count, slot_column));
        auto slot_type = _children[0]->execute_type(block);
        temp_block.insert({slot_column, slot_type, _children[0]->expr_name()});
        int slot_id = 0;

        // topn value
        Field field = _predicate->get_value();
        auto column_ptr = _children[0]->data_type()->create_column_const(1, field);
        int topn_value_id = VExpr::insert_param(&temp_block,
                                                {column_ptr, _children[0]->data_type(), _expr_name},
                                                std::max(count, column_ptr->size()));

        // if error(slot_id == -1), will return.
        ColumnNumbers arguments = {static_cast<uint32_t>(slot_id),
                                   static_cast<uint32_t>(topn_value_id)};

        uint32_t num_columns_without_result = temp_block.columns();
        // prepare a column to save result
        temp_block.insert({nullptr, _data_type, _expr_name});

        RETURN_IF_ERROR(_function->execute(nullptr, temp_block, arguments,
                                           num_columns_without_result, temp_block.rows()));
        result_column = std::move(temp_block.get_by_position(num_columns_without_result).column);
        result_column = _normalize_filter_result(std::move(result_column));
        DCHECK_EQ(result_column->size(), count);
        return Status::OK();
    }

    // Returns true only for a direct slot binding whose logical fixed-width type exactly matches
    // `data_type`. Eligibility does not depend on whether the dynamic TopN bound has arrived: a
    // reader may cache this answer during initialization and observe the bound in a later batch.
    bool can_execute_on_raw_fixed_values(const DataTypePtr& data_type,
                                         int column_id) const override;

    // Compares the contiguous non-NULL physical values with one snapshot of the current TopN bound
    // and ANDs the result into `matches`. `value_width` must equal the logical Doris value width.
    // When no bound is available yet, this is deliberately an all-pass operation.
    Status execute_on_raw_fixed_values(const uint8_t* values, size_t num_values, size_t value_width,
                                       const DataTypePtr& data_type, int column_id,
                                       uint8_t* matches) const override;

    // Returns true for a directly bound STRING-like or VARBINARY slot. As with the fixed-width
    // capability, a not-yet-published bound does not force the scanner onto a materializing path.
    bool can_execute_on_raw_binary_values(const DataTypePtr& data_type,
                                          int column_id) const override;

    // Compares decoder-owned immutable byte slices with the current TopN bound and ANDs each
    // decision into `matches`; it never copies the slices into a ColumnString/ColumnVarbinary.
    Status execute_on_raw_binary_values(const StringRef* values, size_t num_values,
                                        const DataTypePtr& data_type, int column_id,
                                        uint8_t* matches) const override;

    bool raw_predicate_result_for_null() const override {
        // execute_column() is all-pass until publication; afterwards NULLS FIRST keeps NULL while
        // NULLS LAST rejects it. Sample this state per fragment just like the mutable bound.
        return _predicate != nullptr && (!_predicate->has_value() || _predicate->nulls_first());
    }

    // Dictionary capability is restricted to a direct slot and NULLS-LAST semantics. It stays
    // enabled before the first bound so row-group setup can cache an all-pass bitmap; the scan
    // scheduler retains a per-batch residual and defers dictionary-id filtering while NULL still
    // matches, then safely resumes it after bound publication.
    bool can_evaluate_dictionary_filter() const override;

    // Evaluates every non-NULL entry in the bound slot dictionary against one current-bound
    // snapshot. kNoMatch proves that no entry can pass; kMayMatch includes both a matching entry and
    // a not-yet-published bound; kUnsupported reports an absent or incompatible dictionary slot.
    ZoneMapFilterResult evaluate_dictionary_filter(const DictionaryEvalContext& ctx) const override;

    const std::string& expr_name() const override { return _expr_name; }

    // only used in external table (for min-max filter). get `slot > xxx`, not `function(slot) > xxx`.
    bool get_binary_expr(VExprSPtr& new_root) const {
        if (!get_child(0)->is_slot_ref()) {
            // top rf maybe is `xxx order by abs(column) limit xxx`.
            return false;
        }

        if (!_predicate->has_value()) {
            return false;
        }

        auto* slot_ref = assert_cast<VSlotRef*>(get_child(0).get());
        auto slot_data_type = remove_nullable(slot_ref->data_type());
        {
            TFunction fn;
            TFunctionName fn_name;
            fn_name.__set_db_name("");
            fn_name.__set_function_name(_predicate->is_asc() ? "le" : "ge");
            fn.__set_name(fn_name);
            fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
            std::vector<TTypeDesc> arg_types;
            arg_types.push_back(create_type_desc(slot_data_type->get_primitive_type(),
                                                 slot_data_type->get_precision(),
                                                 slot_data_type->get_scale()));

            arg_types.push_back(create_type_desc(slot_data_type->get_primitive_type(),
                                                 slot_data_type->get_precision(),
                                                 slot_data_type->get_scale()));
            fn.__set_arg_types(arg_types);
            fn.__set_ret_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
            fn.__set_has_var_args(false);

            TExprNode texpr_node;
            texpr_node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
            texpr_node.__set_node_type(TExprNodeType::BINARY_PRED);
            texpr_node.__set_opcode(_predicate->is_asc() ? TExprOpcode::LE : TExprOpcode::GE);
            texpr_node.__set_fn(fn);
            texpr_node.__set_num_children(2);
            texpr_node.__set_is_nullable(is_nullable());
            new_root = VectorizedFnCall::create_shared(texpr_node);
        }

        {
            // add slot
            new_root->add_child(children().at(0));
        }
        // add Literal
        {
            Field field = _predicate->get_value();
            TExprNode node = create_texpr_node_from(field, slot_data_type->get_primitive_type(),
                                                    slot_data_type->get_precision(),
                                                    slot_data_type->get_scale());
            new_root->add_child(VLiteral::create_shared(node));
        }

        // Since the normal greater than or less than relationship does not consider the relationship of null values, the generated `col >=/<= xxx OR col is null.`
        if (_predicate->nulls_first()) {
            VExprSPtr col_is_null_node;
            {
                TFunction fn;
                TFunctionName fn_name;
                fn_name.__set_db_name("");
                fn_name.__set_function_name("is_null_pred");
                fn.__set_name(fn_name);
                fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
                std::vector<TTypeDesc> arg_types;
                arg_types.push_back(create_type_desc(slot_data_type->get_primitive_type(),
                                                     slot_data_type->get_precision(),
                                                     slot_data_type->get_scale()));
                fn.__set_arg_types(arg_types);
                fn.__set_ret_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
                fn.__set_has_var_args(false);

                TExprNode texpr_node;
                texpr_node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
                texpr_node.__set_node_type(TExprNodeType::FUNCTION_CALL);
                texpr_node.__set_fn(fn);
                texpr_node.__set_num_children(1);
                col_is_null_node = VectorizedFnCall::create_shared(texpr_node);

                // add slot.
                col_is_null_node->add_child(children().at(0));
            }

            VExprSPtr or_node;
            {
                TExprNode texpr_node;
                texpr_node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
                texpr_node.__set_node_type(TExprNodeType::COMPOUND_PRED);
                texpr_node.__set_opcode(TExprOpcode::COMPOUND_OR);
                texpr_node.__set_num_children(2);
                or_node = VectorizedFnCall::create_shared(texpr_node);
            }

            or_node->add_child(col_is_null_node);
            or_node->add_child(new_root);
            new_root = or_node;
        }

        return true;
    }

private:
    ColumnPtr _normalize_filter_result(ColumnPtr column) const {
        const size_t rows = column->size();
        if (const auto* constant = check_and_get_column<ColumnConst>(*column)) {
            auto nested = _normalize_filter_result(constant->get_data_column_ptr());
            return ColumnConst::create(std::move(nested), rows);
        }

        auto mutable_column = IColumn::mutate(std::move(column));
        if (auto* nullable = check_and_get_column<ColumnNullable>(*mutable_column)) {
            auto& values = assert_cast<ColumnUInt8&>(*nullable->get_nested_column_ptr()).get_data();
            const auto& null_map = nullable->get_null_map_data();
            // Collapse SQL NULL to the filter decision: NULLS FIRST keeps it, while NULLS LAST
            // rejects it. Return a non-nullable Boolean so every caller observes the same decision.
            for (size_t row = 0; row < values.size(); ++row) {
                values[row] = null_map[row] ? _predicate->nulls_first() : values[row];
            }
            return nullable->get_nested_column_ptr();
        }
        return mutable_column;
    }

    int _source_node_id;
    std::string _expr_name;
    RuntimePredicate* _predicate = nullptr;
    FunctionBasePtr _function;
    VExprContextSPtr _target_ctx;
};

} // namespace doris
