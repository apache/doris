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

#include "vec/exprs/vbloom_predicate.h"

#include <stddef.h>

#include <utility>
#include <vector>

#include "common/status.h"
#include "exprs/bloom_filter_func.h"
#include "gutil/integral_types.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris {
class RowDescriptor;
class TExprNode;

namespace vectorized {
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

VBloomPredicate::VBloomPredicate(const TExprNode& node)
        : VExpr(node), _filter(nullptr), _expr_name("bloom_predicate") {}

Status VBloomPredicate::prepare(RuntimeState* state, const RowDescriptor& desc,
                                VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

    if (_children.size() != 1) {
        return Status::InternalError("Invalid argument for VBloomPredicate.");
    }

    _be_exec_version = state->be_exec_version();
    _prepare_finished = true;
    return Status::OK();
}

Status VBloomPredicate::open(RuntimeState* state, VExprContext* context,
                             FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    RETURN_IF_ERROR(VExpr::open(state, context, scope));
    _open_finished = true;
    return Status::OK();
}

void VBloomPredicate::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    VExpr::close(context, scope);
}

Status VBloomPredicate::execute(VExprContext* context, Block* block, int* result_column_id) {
    DCHECK(_open_finished || _getting_const_col);
    doris::vectorized::ColumnNumbers arguments(_children.size());
    for (int i = 0; i < _children.size(); ++i) {
        int column_id = -1;
        RETURN_IF_ERROR(_children[i]->execute(context, block, &column_id));
        arguments[i] = column_id;
    }
    // call function
    size_t num_columns_without_result = block->columns();
    auto res_data_column = ColumnVector<UInt8>::create(block->rows());

    ColumnPtr argument_column =
            block->get_by_position(arguments[0]).column->convert_to_full_column_if_const();
    size_t sz = argument_column->size();
    res_data_column->resize(sz);
    auto* ptr = ((ColumnVector<UInt8>*)res_data_column.get())->get_data().data();
    _filter->find_fixed_len(argument_column, ptr);

    if (_data_type->is_nullable()) {
        auto null_map = ColumnVector<UInt8>::create(block->rows(), 0);
        block->insert({ColumnNullable::create(std::move(res_data_column), std::move(null_map)),
                       _data_type, _expr_name});
    } else {
        block->insert({std::move(res_data_column), _data_type, _expr_name});
    }
    *result_column_id = num_columns_without_result;
    return Status::OK();
}

const std::string& VBloomPredicate::expr_name() const {
    return _expr_name;
}
void VBloomPredicate::set_filter(std::shared_ptr<BloomFilterFuncBase>& filter) {
    _filter = filter;
}
} // namespace doris::vectorized