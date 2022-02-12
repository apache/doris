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

#include "exprs/compound_predicate.h"

#include <sstream>

#include "runtime/runtime_state.h"
#include "util/debug_util.h"

namespace doris {

CompoundPredicate::CompoundPredicate(const TExprNode& node) : Predicate(node) {}

void CompoundPredicate::init() {}

BooleanVal CompoundPredicate::compound_not(FunctionContext* context, const BooleanVal& v) {
    if (v.is_null) {
        return BooleanVal::null();
    }
    return BooleanVal(!v.val);
}

BooleanVal AndPredicate::get_boolean_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_children.size(), 2);
    BooleanVal val1 = _children[0]->get_boolean_val(context, row);
    if (!val1.is_null && !val1.val) {
        return BooleanVal(false);
    }
    BooleanVal val2 = _children[1]->get_boolean_val(context, row);
    if (!val2.is_null && !val2.val) {
        return BooleanVal(false);
    }
    if (val1.is_null || val2.is_null) {
        return BooleanVal::null();
    }
    return BooleanVal(true);
}

BooleanVal OrPredicate::get_boolean_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_children.size(), 2);
    BooleanVal val1 = _children[0]->get_boolean_val(context, row);
    if (!val1.is_null && val1.val) {
        return BooleanVal(true);
    }
    BooleanVal val2 = _children[1]->get_boolean_val(context, row);
    if (!val2.is_null && val2.val) {
        return BooleanVal(true);
    }
    if (val1.is_null || val2.is_null) {
        return BooleanVal::null();
    }
    return BooleanVal(false);
}

BooleanVal NotPredicate::get_boolean_val(ExprContext* context, TupleRow* row) {
    BooleanVal val = _children[0]->get_boolean_val(context, row);
    if (val.is_null) {
        return BooleanVal::null();
    }
    return BooleanVal(!val.val);
}

std::string CompoundPredicate::debug_string() const {
    std::stringstream out;
    out << "CompoundPredicate(" << Expr::debug_string() << ")";
    return out.str();
}

} // namespace doris
