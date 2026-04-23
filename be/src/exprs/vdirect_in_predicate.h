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

#include <mutex>
#include <optional>

#include "common/status.h"
#include "exprs/hybrid_set.h"
#include "exprs/vexpr.h"
#include "exprs/vin_predicate.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "storage/index/zone_map/zone_map_eval_context.h"

namespace doris {

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

    ZoneMapEvalResult evaluate_zone_map(const ZoneMapEvalContext& ctx) const override {
        if (_children.empty() || !_children[0]->is_slot_ref()) {
            return ZoneMapEvalResult::kUnsupported;
        }

        int slot_index = static_cast<VSlotRef*>(_children[0].get())->column_id();
        auto zone_map_it = ctx.slot_index_to_zone_map.find(slot_index);
        if (zone_map_it == ctx.slot_index_to_zone_map.end()) {
            return ZoneMapEvalResult::kUnsupported;
        }
        const auto& zone_map = *zone_map_it->second;
        if (zone_map.pass_all) {
            return ZoneMapEvalResult::kMayMatch;
        }
        if (!zone_map.has_not_null) {
            return ZoneMapEvalResult::kNoMatch;
        }

        auto type_it = ctx.slot_index_to_data_type.find(slot_index);
        if (type_it != ctx.slot_index_to_data_type.end()) {
            auto zone_map_type = remove_nullable(type_it->second);
            auto in_set_type = remove_nullable(get_child(0)->data_type());
            if (!zone_map_type->equals(*in_set_type)) {
                ctx.type_mismatch_count++;
                return ZoneMapEvalResult::kUnsupported;
            }
        }

        auto set_func = get_set_func();
        if (!set_func || set_func->size() == 0) {
            return ZoneMapEvalResult::kUnsupported;
        }

        std::call_once(_cached_in_set_min_max_once,
                       [&]() { _cached_in_set_min_max = _compute_in_set_min_max(set_func.get()); });
        if (!_cached_in_set_min_max.has_value() || !_cached_in_set_min_max->valid) {
            return ZoneMapEvalResult::kUnsupported;
        }

        const auto& in_min_max = *_cached_in_set_min_max;
        if (in_min_max.max_value < zone_map.min_value ||
            in_min_max.min_value > zone_map.max_value) {
            return ZoneMapEvalResult::kNoMatch;
        }
        return ZoneMapEvalResult::kMayMatch;
    }

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
    struct InSetMinMax {
        Field min_value;
        Field max_value;
        bool valid = false;
    };

    InSetMinMax _compute_in_set_min_max(HybridSetBase* set_func) const {
        InSetMinMax result;
        auto slot_data_type = remove_nullable(get_child(0)->data_type());
        auto primitive_type = slot_data_type->get_primitive_type();
        int precision = slot_data_type->get_precision();
        int scale = slot_data_type->get_scale();

        auto* iter = set_func->begin();
        bool first = true;
        while (iter->has_next()) {
            const void* value = iter->get_value();
            if (value == nullptr) {
                iter->next();
                continue;
            }
            Field field_value = _make_field_from_value(value, primitive_type, precision, scale);
            if (field_value.is_null()) {
                iter->next();
                continue;
            }
            if (first) {
                result.min_value = field_value;
                result.max_value = field_value;
                first = false;
            } else {
                if (field_value < result.min_value) {
                    result.min_value = field_value;
                }
                if (field_value > result.max_value) {
                    result.max_value = field_value;
                }
            }
            iter->next();
        }
        result.valid = !first;
        return result;
    }

    Field _make_field_from_value(const void* value, PrimitiveType primitive_type, int precision,
                                 int scale) const {
        TExprNode literal_node = create_texpr_node_from(value, primitive_type, precision, scale);
        auto literal = VLiteral::create_shared(literal_node);
        Field field;
        literal->get_column_ptr()->get(0, field);
        return field;
    }

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
    mutable std::once_flag _cached_in_set_min_max_once;
    mutable std::optional<InSetMinMax> _cached_in_set_min_max;
};

} // namespace doris
