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

#include "exprs/runtime_filter_expr.h"

#include <fmt/format.h>

#include <cstddef>

#include "common/logging.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/data_type/data_type.h"
#include "core/types.h"
#include "exec/common/util.hpp"
#include "exprs/vslot_ref.h"
#include "runtime/runtime_profile.h"
#include "storage/index/zone_map/zonemap_eval_context.h"

namespace doris {

class RowDescriptor;
class RuntimeState;
class TExprNode;
// branch-4.1 still builds VRuntimeFilterWrapper, which owns these shared threshold helpers.
} // namespace doris

namespace doris {

class VExprContext;

RuntimeFilterExpr::RuntimeFilterExpr(const TExprNode& node, VExprSPtr impl, double ignore_thredhold,
                                     bool null_aware, int filter_id, int sampling_frequency)
        : VExpr(node),
          _impl(std::move(impl)),
          _ignore_thredhold(ignore_thredhold),
          _null_aware(null_aware),
          _filter_id(filter_id),
          _sampling_frequency(sampling_frequency) {
    DORIS_CHECK(_impl != nullptr);
}

Status RuntimeFilterExpr::clone_node(VExprSPtr* cloned_expr) const {
    DORIS_CHECK(cloned_expr != nullptr);
    DORIS_CHECK(_impl != nullptr);
    VExprSPtr cloned_impl;
    RETURN_IF_ERROR(_impl->deep_clone(&cloned_impl));
    auto cloned_runtime_filter = RuntimeFilterExpr::create_shared(
            clone_texpr_node(), std::move(cloned_impl), _ignore_thredhold, _null_aware, _filter_id,
            _sampling_frequency);
    cloned_runtime_filter->attach_profile_counter(_rf_input_rows, _rf_filter_rows,
                                                  _always_true_filter_rows);
    *cloned_expr = std::move(cloned_runtime_filter);
    return Status::OK();
}

Status RuntimeFilterExpr::prepare(RuntimeState* state, const RowDescriptor& desc,
                                  VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(_impl->prepare(state, desc, context));
    _expr_name = fmt::format("RuntimeFilterExpr({})", _impl->expr_name());
    _prepare_finished = true;
    return Status::OK();
}

Status RuntimeFilterExpr::open(RuntimeState* state, VExprContext* context,
                               FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    RETURN_IF_ERROR(_impl->open(state, context, scope));
    context->get_runtime_filter_selectivity().set_sampling_frequency(_sampling_frequency);
    _open_finished = true;
    return Status::OK();
}

void RuntimeFilterExpr::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    _impl->close(context, scope);
}

Status RuntimeFilterExpr::execute_column_impl(VExprContext* context, const Block* block,
                                              const Selector* selector, size_t count,
                                              ColumnPtr& result_column) const {
    // branch-4.1's VExpr API predates const selectors; RuntimeFilterExpr only forwards the
    // selector and does not mutate it.
    return _impl->execute_column(context, block, const_cast<Selector*>(selector), count,
                                 result_column);
}

const std::string& RuntimeFilterExpr::expr_name() const {
    return _expr_name;
}

Status RuntimeFilterExpr::execute_filter(VExprContext* context, const Block* block,
                                         uint8_t* __restrict result_filter_data, size_t rows,
                                         bool accept_null, bool* can_filter_all) const {
    DCHECK(_open_finished);
    if (accept_null) {
        return Status::InternalError(
                "Runtime filter does not support accept_null in execute_filter");
    }

    auto& rf_selectivity = context->get_runtime_filter_selectivity();
    Defer auto_update_judge_counter = [&]() { rf_selectivity.update_judge_counter(); };

    // if always true, skip evaluate runtime filter
    if (rf_selectivity.maybe_always_true_can_ignore()) {
        COUNTER_UPDATE(_always_true_filter_rows, rows);
        return Status::OK();
    }

    ColumnPtr filter_column;
    ColumnPtr arg_column = nullptr;

    RETURN_IF_ERROR(_impl->execute_runtime_filter(context, block, result_filter_data, rows,
                                                  filter_column, &arg_column));

    // bloom filter will handle null aware inside itself
    if (_null_aware && TExprNodeType::BLOOM_PRED != node_type()) {
        DCHECK(arg_column);
        change_null_to_true(filter_column->assert_mutable(), arg_column);
    }

    if (const auto* const_column = check_and_get_column<ColumnConst>(*filter_column)) {
        // const(nullable) or const(bool)
        if (!const_column->get_bool(0)) {
            // filter all
            COUNTER_UPDATE(_rf_filter_rows, rows);
            COUNTER_UPDATE(_rf_input_rows, rows);
            rf_selectivity.update_judge_selectivity(_filter_id, rows, rows, _ignore_thredhold);
            *can_filter_all = true;
            memset(result_filter_data, 0, rows);
            return Status::OK();
        } else {
            // filter none
            COUNTER_UPDATE(_rf_input_rows, rows);
            rf_selectivity.update_judge_selectivity(_filter_id, 0, rows, _ignore_thredhold);
            return Status::OK();
        }
    } else if (const auto* nullable_column = check_and_get_column<ColumnNullable>(*filter_column)) {
        // nullable(bool)
        const ColumnPtr& nested_column = nullable_column->get_nested_column_ptr();
        const IColumn::Filter& filter = assert_cast<const ColumnUInt8&>(*nested_column).get_data();
        const auto* __restrict filter_data = filter.data();
        const auto* __restrict null_map_data = nullable_column->get_null_map_data().data();

        const size_t input_rows = rows - simd::count_zero_num((int8_t*)result_filter_data, rows);

        for (size_t i = 0; i < rows; ++i) {
            result_filter_data[i] &= (!null_map_data[i]) & filter_data[i];
        }

        const size_t output_rows = rows - simd::count_zero_num((int8_t*)result_filter_data, rows);

        COUNTER_UPDATE(_rf_filter_rows, input_rows - output_rows);
        COUNTER_UPDATE(_rf_input_rows, input_rows);
        rf_selectivity.update_judge_selectivity(_filter_id, input_rows - output_rows, input_rows,
                                                _ignore_thredhold);

        if (output_rows == 0) {
            *can_filter_all = true;
            return Status::OK();
        }
    } else {
        // bool
        const IColumn::Filter& filter = assert_cast<const ColumnUInt8&>(*filter_column).get_data();
        const auto* __restrict filter_data = filter.data();

        const size_t input_rows = rows - simd::count_zero_num((int8_t*)result_filter_data, rows);

        for (size_t i = 0; i < rows; ++i) {
            result_filter_data[i] &= filter_data[i];
        }

        const size_t output_rows = rows - simd::count_zero_num((int8_t*)result_filter_data, rows);

        COUNTER_UPDATE(_rf_filter_rows, input_rows - output_rows);
        COUNTER_UPDATE(_rf_input_rows, input_rows);
        rf_selectivity.update_judge_selectivity(_filter_id, input_rows - output_rows, input_rows,
                                                _ignore_thredhold);

        if (output_rows == 0) {
            *can_filter_all = true;
            return Status::OK();
        }
    }
    return Status::OK();
}

ZoneMapFilterResult RuntimeFilterExpr::evaluate_zonemap_filter(
        const ZoneMapEvalContext& ctx) const {
    if (_null_aware) {
        const auto child = _impl->get_child(0);
        if (auto slot_ref = std::dynamic_pointer_cast<VSlotRef>(child); slot_ref != nullptr) {
            auto zone_map = ctx.zone_map(slot_ref->column_id());
            if (zone_map == nullptr) {
                return unsupported_zonemap_filter(ctx);
            }

            if (zone_map->has_null) {
                return ZoneMapFilterResult::kMayMatch;
            }
        } else {
            // RuntimeFilterExpr implementations generated by RuntimeFilterConsumer put the probe
            // slot as child 0. If a custom RF expression violates this shape, keep the null-aware
            // evaluation conservative because we cannot decide whether the target zone has NULLs.
            return unsupported_zonemap_filter(ctx);
        }
    }
    return _impl->evaluate_zonemap_filter(ctx);
}

bool RuntimeFilterExpr::can_evaluate_zonemap_filter() const {
    return _impl->can_evaluate_zonemap_filter();
}

void RuntimeFilterExpr::collect_slot_column_ids(std::set<int>& column_ids) const {
    _impl->collect_slot_column_ids(column_ids);
}

} // namespace doris
