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
                                             double ignore_thredhold, bool null_aware)
        : VExpr(node),
          _impl(std::move(impl)),
          _ignore_thredhold(ignore_thredhold),
          _null_aware(null_aware) {
    reset_judge_selectivity();
}

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
    DCHECK(_expr_filtered_rows_counter && _expr_input_rows_counter && _always_true_counter)
            << "rf counter must be initialized";
    if (_judge_counter.fetch_sub(1) == 0) {
        reset_judge_selectivity();
    }
    if (_always_true) {
        size_t size = block->rows();
        block->insert({create_always_true_column(size, _data_type->is_nullable()), _data_type,
                       expr_name()});
        *result_column_id = block->columns() - 1;
        if (_always_true_counter) {
            COUNTER_UPDATE(_always_true_counter, size);
        }
        return Status::OK();
    } else {
        if (_getting_const_col) {
            _impl->set_getting_const_col(true);
        }
        ColumnNumbers args;
        RETURN_IF_ERROR(_impl->execute_runtime_fitler(context, block, result_column_id, args));
        if (_getting_const_col) {
            _impl->set_getting_const_col(false);
        }

        ColumnWithTypeAndName& result_column = block->get_by_position(*result_column_id);

        if (_null_aware) {
            change_null_to_true(result_column.column, block->get_by_position(args[0]).column);
        }

        return Status::OK();
    }
}

const std::string& VRuntimeFilterWrapper::expr_name() const {
    return _expr_name;
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
