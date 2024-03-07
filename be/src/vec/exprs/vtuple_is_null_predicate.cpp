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

#include "vec/exprs/vtuple_is_null_predicate.h"

#include <gen_cpp/Exprs_types.h>
#include <glog/logging.h>

#include <ostream>
#include <vector>

#include "runtime/descriptors.h"

namespace doris {
class RuntimeState;
namespace vectorized {
class Block;
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

VTupleIsNullPredicate::VTupleIsNullPredicate(const TExprNode& node)
        : VExpr(node), _expr_name(function_name) {
    DCHECK(node.tuple_is_null_pred.__isset.null_side);
    _is_left_null_side = node.tuple_is_null_pred.null_side == TNullSide::LEFT;
    _column_to_check = 0;
}

Status VTupleIsNullPredicate::prepare(RuntimeState* state, const RowDescriptor& desc,
                                      VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));
    DCHECK_EQ(0, _children.size());
    _column_to_check =
            _is_left_null_side ? desc.num_materialized_slots() : desc.num_materialized_slots() + 1;
    _prepare_finished = true;
    return Status::OK();
}

Status VTupleIsNullPredicate::open(RuntimeState* state, VExprContext* context,
                                   FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    RETURN_IF_ERROR(VExpr::open(state, context, scope));
    _open_finished = true;
    return Status::OK();
}

Status VTupleIsNullPredicate::execute(VExprContext* context, Block* block, int* result_column_id) {
    DCHECK(_open_finished || _getting_const_col);
    *result_column_id = _column_to_check;
    return Status::OK();
}

const std::string& VTupleIsNullPredicate::expr_name() const {
    return _expr_name;
}

std::string VTupleIsNullPredicate::debug_string() const {
    std::stringstream out;
    out << "TupleIsNullPredicate(_column_to_check=[";
    out << _column_to_check;
    out << "])";
    return out.str();
}

} // namespace doris::vectorized