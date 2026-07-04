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

#include <utility>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "core/field.h"
#include "core/types.h"
#include "exprs/expr_zonemap_filter.h"
#include "exprs/hybrid_set.h"
#include "exprs/vexpr.h"
#include "exprs/vin_predicate.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"

namespace doris {

class VDirectInPredicate final : public VExpr {
    ENABLE_FACTORY_CREATOR(VDirectInPredicate);

public:
    // `hybrid_set_values_match_child_type` tells whether values in `filter` can be interpreted with
    // the child expression type. Parquet/ORC dictionary-filter rewrites evaluate the original
    // logical predicate against dictionary entries and then rewrite it to matched physical
    // dictionary codes, for example `col IN ('a', 'b')` becomes `dict_code IN (0, 1)`. In that
    // shape the HybridSet stores TYPE_INT dictionary codes while the child slot still has the
    // original logical type such as STRING. Callers must pass false to disable zonemap
    // materialization and slot-IN rewrite that would otherwise rebuild child-typed literals from
    // dictionary codes.
    VDirectInPredicate(const TExprNode& node, const std::shared_ptr<HybridSetBase>& filter,
                       bool hybrid_set_values_match_child_type = true)
            : VExpr(node),
              _filter(filter),
              _hybrid_set_values_match_child_type(hybrid_set_values_match_child_type),
              _expr_name("direct_in_predicate") {}
    ~VDirectInPredicate() override = default;

#ifdef BE_TEST
    VDirectInPredicate() = default;
#endif

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                   VExprContext* context) override {
        RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, row_desc, context));
        RETURN_IF_ERROR(_materialize_for_zonemap_filter());
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

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
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

    ZoneMapFilterResult evaluate_zonemap_filter(const ZoneMapEvalContext& ctx) const override {
        return expr_zonemap::eval_in_zonemap(ctx, get_child(0), false, _seg_filter_values,
                                             _seg_filter_min, _seg_filter_max);
    }

    bool can_evaluate_zonemap_filter() const override {
        return _zonemap_materialized &&
               std::dynamic_pointer_cast<VSlotRef>(get_child(0)) != nullptr;
    }

    Status clone_node(VExprSPtr* cloned_expr) const override {
        DORIS_CHECK(cloned_expr != nullptr);
        *cloned_expr = VDirectInPredicate::create_shared(clone_texpr_node(), _filter,
                                                         _hybrid_set_values_match_child_type);
        return Status::OK();
    }

    bool get_slot_in_expr(VExprSPtr& new_root) const {
        if (!_hybrid_set_values_match_child_type) {
            return false;
        }
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

                TExprNode node = expr_zonemap::create_texpr_node_from_hybrid_set_value(
                        value, slot_data_type->get_primitive_type(),
                        slot_data_type->get_precision(), slot_data_type->get_scale());
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
                       const Selector* selector, size_t count, ColumnPtr& result_column,
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

        if (const auto* nullable = check_and_get_column<ColumnNullable>(argument_column.get())) {
            auto column_nested = nullable->get_nested_column_ptr();
            const auto& null_map = nullable->get_null_map_data();
            _filter->find_batch_nullable(*column_nested, sz, null_map, res_data_column->get_data(),
                                         filter);
        } else {
            _filter->find_batch(*argument_column, sz, res_data_column->get_data(), filter);
        }

        DCHECK(!_data_type->is_nullable());
        result_column = std::move(res_data_column);
        return Status::OK();
    }

    Status _materialize_for_zonemap_filter() {
        if (!_hybrid_set_values_match_child_type) {
            _zonemap_materialized = false;
            return Status::OK();
        }
        DORIS_CHECK(_filter != nullptr);
        auto& filter = *_filter;
        const auto& data_type = remove_nullable(get_child(0)->data_type());
        expr_zonemap::InZonemapMaterializedSet materialized;
        RETURN_IF_ERROR(expr_zonemap::materialize_hybrid_set_for_zonemap_filter(filter, data_type,
                                                                                &materialized));
        _seg_filter_values = std::move(materialized.values);
        _seg_filter_min = std::move(materialized.min_value);
        _seg_filter_max = std::move(materialized.max_value);
        _zonemap_materialized = true;
        return Status::OK();
    }

    std::shared_ptr<HybridSetBase> _filter;
    // Dictionary-filter rewrites may store physical dictionary codes in the HybridSet while the
    // child slot keeps the original logical type. Such values must not be materialized as child-type
    // literals for zonemap pruning or slot-IN rewrite.
    bool _hybrid_set_values_match_child_type = true;
    std::string _expr_name;
    bool _zonemap_materialized = false;
    std::vector<Field> _seg_filter_values;
    Field _seg_filter_min;
    Field _seg_filter_max;
};

} // namespace doris
