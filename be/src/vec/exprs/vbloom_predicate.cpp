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

#include <cstddef>
#include <utility>

#include "common/status.h"
#include "exprs/bloom_filter_func.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris {
class RowDescriptor;
class TExprNode;

} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class VExprContext;

VBloomPredicate::VBloomPredicate(const TExprNode& node) : VExpr(node), _filter(nullptr) {}

Status VBloomPredicate::prepare(RuntimeState* state, const RowDescriptor& desc,
                                VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

    if (_children.size() != 1) {
        return Status::InternalError("Invalid argument for VBloomPredicate.");
    }

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

Status VBloomPredicate::execute_column(VExprContext* context, const Block* block, size_t count,
                                       ColumnPtr& result_column) const {
    DCHECK(_open_finished || _getting_const_col);
    DCHECK_EQ(_children.size(), 1);

    ColumnPtr argument_column;
    RETURN_IF_ERROR(_children[0]->execute_column(context, block, count, argument_column));
    argument_column = argument_column->convert_to_full_column_if_const();

    size_t sz = argument_column->size();
    auto res_data_column = ColumnUInt8::create(sz);

    res_data_column->resize(sz);
    auto* ptr = ((ColumnUInt8*)res_data_column.get())->get_data().data();

    _filter->find_fixed_len(argument_column, ptr);

    result_column = std::move(res_data_column);
    DCHECK_EQ(result_column->size(), count);
    return Status::OK();
}

const std::string& VBloomPredicate::expr_name() const {
    return EXPR_NAME;
}

void VBloomPredicate::set_filter(std::shared_ptr<BloomFilterFuncBase> filter) {
    _filter = filter;
}

uint64_t VBloomPredicate::get_digest(uint64_t seed) const {
    seed = _children[0]->get_digest(seed);
    if (seed) {
        char* data;
        int len;
        _filter->get_data(&data, &len);
        return HashUtil::hash64(data, len, seed);
    }
    return 0;
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized