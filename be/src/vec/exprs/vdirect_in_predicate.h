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

#include "common/status.h"
#include "exprs/hybrid_set.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vin_predicate.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vslot_ref.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class VDirectInPredicate final : public VExpr {
    ENABLE_FACTORY_CREATOR(VDirectInPredicate);

public:
    VDirectInPredicate(const TExprNode& node, const std::shared_ptr<HybridSetBase>& filter)
            : VExpr(node), _filter(filter), _expr_name("direct_in_predicate") {}
    ~VDirectInPredicate() override = default;

#ifdef BE_TEST
    VDirectInPredicate() = default;
#endif

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                   VExprContext* context) override {
        RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, row_desc, context));
        _prepare_finished = true;
        return Status::OK();
    }

    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override {
        DCHECK(_prepare_finished);
        RETURN_IF_ERROR(VExpr::open(state, context, scope));
        _open_finished = true;
        return Status::OK();
    }

    Status execute_column(VExprContext* context, const Block* block, Selector* selector,
                          size_t count, ColumnPtr& result_column) const override {
        return _do_execute(context, block, nullptr, selector, count, result_column, nullptr);
    }

    Status execute_runtime_filter(VExprContext* context, const Block* block,
                                  const uint8_t* __restrict filter, size_t count,
                                  ColumnPtr& result_column, ColumnPtr* arg_column) const override {
        return _do_execute(context, block, filter, nullptr, count, result_column, arg_column);
    }

    const std::string& expr_name() const override { return _expr_name; }

    std::shared_ptr<HybridSetBase> get_set_func() const override { return _filter; }

    bool get_slot_in_expr(VExprSPtr& new_root) const {
        if (!get_child(0)->is_slot_ref()) {
            return false;
        }

        auto* slot_ref = assert_cast<VSlotRef*>(get_child(0).get());
        auto slot_data_type = remove_nullable(slot_ref->data_type());
        {
            TTypeDesc type_desc = create_type_desc(PrimitiveType::TYPE_BOOLEAN);
            TExprNode node;
            node.__set_type(type_desc);
            node.__set_node_type(TExprNodeType::IN_PRED);
            node.in_predicate.__set_is_not_in(false);
            node.__set_opcode(TExprOpcode::FILTER_IN);
            // VdirectInPredicate assume is_nullable = false.
            node.__set_is_nullable(false);
            new_root = VInPredicate::create_shared(node);
        }
        {
            // add slot
            new_root->add_child(children().at(0));
        }
        {
            auto iter = get_set_func()->begin();
            while (iter->has_next()) {
                DCHECK(iter->get_value() != nullptr);
                const void* value = iter->get_value();

                TExprNode node = create_texpr_node_from(value, slot_data_type->get_primitive_type(),
                                                        slot_data_type->get_precision(),
                                                        slot_data_type->get_scale());
                new_root->add_child(VLiteral::create_shared(node));
                iter->next();
            }
        }
        return true;
    }

    uint64_t get_digest(uint64_t seed) const override {
        seed = _children[0]->get_digest(seed);
        if (seed) {
            return _filter->get_digest(seed);
        }
        return seed;
    }

private:
    Status _do_execute(VExprContext* context, const Block* block, const uint8_t* __restrict filter,
                       Selector* selector, size_t count, ColumnPtr& result_column,
                       ColumnPtr* arg_column) const {
        DCHECK(_open_finished || block == nullptr);
        DCHECK(!(filter != nullptr && selector != nullptr))
                << "filter and selector can not be both set";
        ColumnPtr argument_column;
        RETURN_IF_ERROR(
                _children[0]->execute_column(context, block, selector, count, argument_column));
        argument_column = argument_column->convert_to_full_column_if_const();

        if (arg_column != nullptr) {
            *arg_column = argument_column;
        }

        size_t sz = argument_column->size();
        auto res_data_column = ColumnUInt8::create(sz);
        res_data_column->resize(sz);

        if (argument_column->is_nullable()) {
            auto column_nested = static_cast<const ColumnNullable*>(argument_column.get())
                                         ->get_nested_column_ptr();
            const auto& null_map =
                    static_cast<const ColumnNullable*>(argument_column.get())->get_null_map_data();
            _filter->find_batch_nullable(*column_nested, sz, null_map, res_data_column->get_data(),
                                         filter);
        } else {
            _filter->find_batch(*argument_column, sz, res_data_column->get_data(), filter);
        }

        DCHECK(!_data_type->is_nullable());
        result_column = std::move(res_data_column);
        return Status::OK();
    }

    std::shared_ptr<HybridSetBase> _filter;
    std::string _expr_name;
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
