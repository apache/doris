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

#include "exprs/tuple_is_null_predicate.h"

#include <sstream>

#include "gen_cpp/Exprs_types.h"

namespace doris {

TupleIsNullPredicate::TupleIsNullPredicate(const TExprNode& node)
        : Predicate(node),
          _tuple_ids(node.tuple_is_null_pred.tuple_ids.begin(),
                     node.tuple_is_null_pred.tuple_ids.end()) {}

Status TupleIsNullPredicate::prepare(RuntimeState* state, const RowDescriptor& row_desc,
                                     ExprContext* ctx) {
    RETURN_IF_ERROR(Expr::prepare(state, row_desc, ctx));
    DCHECK_EQ(0, _children.size());

    // Resolve tuple ids to tuple indexes.
    for (int i = 0; i < _tuple_ids.size(); ++i) {
        int32_t tuple_idx = row_desc.get_tuple_idx(_tuple_ids[i]);
        if (row_desc.tuple_is_nullable(tuple_idx)) {
            _tuple_idxs.push_back(tuple_idx);
        }
    }

    return Status::OK();
}

BooleanVal TupleIsNullPredicate::get_boolean_val(ExprContext* ctx, TupleRow* row) {
    int count = 0;
    for (int i = 0; i < _tuple_idxs.size(); ++i) {
        count += row->get_tuple(_tuple_idxs[i]) == nullptr;
    }
    return BooleanVal(!_tuple_idxs.empty() && count == _tuple_idxs.size());
}

std::string TupleIsNullPredicate::debug_string() const {
    std::stringstream out;
    out << "TupleIsNullPredicate(tupleids=[";

    for (int i = 0; i < _tuple_ids.size(); ++i) {
        out << (i == 0 ? "" : " ") << _tuple_ids[i];
    }

    out << "])";
    return out.str();
}

} // namespace doris
