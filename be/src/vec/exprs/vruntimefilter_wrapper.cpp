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

#include "vec/exprs/vruntimefilter_wrapper.h"

#include <fmt/format.h>

#include <cstddef>

#include "util/runtime_profile.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/utils/util.hpp"

namespace doris {
#include "common/compile_check_begin.h"

class RowDescriptor;
class RuntimeState;
class TExprNode;

double get_in_list_ignore_thredhold(size_t list_size) {
    return std::log2(list_size + 1) / 64;
}

double get_comparison_ignore_thredhold() {
    return 0.1;
}

double get_bloom_filter_ignore_thredhold() {
    return 0.4;
}
} // namespace doris

namespace doris::vectorized {

class VExprContext;

VRuntimeFilterWrapper::VRuntimeFilterWrapper(const TExprNode& node, VExprSPtr impl,
                                             double ignore_thredhold, bool null_aware,
                                             int filter_id)
        : VExpr(node),
          _impl(std::move(impl)),
          _ignore_thredhold(ignore_thredhold),
          _null_aware(null_aware),
          _filter_id(filter_id) {}

Status VRuntimeFilterWrapper::prepare(RuntimeState* state, const RowDescriptor& desc,
                                      VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(_impl->prepare(state, desc, context));
    _expr_name = fmt::format("VRuntimeFilterWrapper({})", _impl->expr_name());
    _prepare_finished = true;
    return Status::OK();
}

Status VRuntimeFilterWrapper::open(RuntimeState* state, VExprContext* context,
                                   FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    RETURN_IF_ERROR(_impl->open(state, context, scope));
    _open_finished = true;
    return Status::OK();
}

void VRuntimeFilterWrapper::close(VExprContext* context,
                                  FunctionContext::FunctionStateScope scope) {
    _impl->close(context, scope);
}

Status VRuntimeFilterWrapper::execute_column(VExprContext* context, const Block* block,
                                             Selector* selector, size_t count,
                                             ColumnPtr& result_column) const {
    return Status::InternalError("Not implement VRuntimeFilterWrapper::execute_column");
}

const std::string& VRuntimeFilterWrapper::expr_name() const {
    return _expr_name;
}

Status VRuntimeFilterWrapper::execute_filter(VExprContext* context, const Block* block,
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
        change_null_to_true(filter_column->assume_mutable(), arg_column);
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

#include "common/compile_check_end.h"
} // namespace doris::vectorized
