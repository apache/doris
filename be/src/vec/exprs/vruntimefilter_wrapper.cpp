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
#include <stddef.h>

#include <cstdint>
#include <memory>
#include <utility>

#include "util/defer_op.h"
#include "util/runtime_profile.h"
#include "util/simd/bits.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/utils/util.hpp"

namespace doris {
class RowDescriptor;
class RuntimeState;
class TExprNode;

namespace vectorized {
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

VRuntimeFilterWrapper::VRuntimeFilterWrapper(const TExprNode& node, const VExprSPtr& impl,
                                             bool null_aware)
        : VExpr(node),
          _impl(impl),
          _always_true(false),
          _filtered_rows(0),
          _scan_rows(0),
          _null_aware(null_aware) {}

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

Status VRuntimeFilterWrapper::execute(VExprContext* context, Block* block, int* result_column_id) {
    DCHECK(_open_finished || _getting_const_col);
    if (_always_true) {
        block->insert({create_always_true_column(block->rows(), _data_type->is_nullable()),
                       _data_type, expr_name()});
        *result_column_id = block->columns() - 1;
        return Status::OK();
    } else {
        int64_t input_rows = 0, filter_rows = 0;
        Defer statistic_filter_info {[&]() {
            if (_expr_filtered_rows_counter) {
                COUNTER_UPDATE(_expr_filtered_rows_counter, filter_rows);
            }
            if (_expr_input_rows_counter) {
                COUNTER_UPDATE(_expr_input_rows_counter, input_rows);
            }
            if (_always_true_counter) {
                COUNTER_SET(_always_true_counter, (int64_t)_always_true);
            }
        }};
        input_rows += block->rows();

        if (_getting_const_col) {
            _impl->set_getting_const_col(true);
        }
        std::vector<size_t> args;
        RETURN_IF_ERROR(_impl->execute_runtime_fitler(context, block, result_column_id, args));
        if (_getting_const_col) {
            _impl->set_getting_const_col(false);
        }

        const auto rows = block->rows();
        ColumnWithTypeAndName& result_column = block->get_by_position(*result_column_id);

        if (_null_aware) {
            change_null_to_true(result_column.column, block->get_by_position(args[0]).column);
        }

        filter_rows = rows - calculate_false_number(result_column.column);
        _filtered_rows += filter_rows;
        _scan_rows += input_rows;
        calculate_filter(VRuntimeFilterWrapper::EXPECTED_FILTER_RATE, _filtered_rows, _scan_rows,
                         _has_calculate_filter, _always_true);
        return Status::OK();
    }
}

const std::string& VRuntimeFilterWrapper::expr_name() const {
    return _expr_name;
}

} // namespace doris::vectorized