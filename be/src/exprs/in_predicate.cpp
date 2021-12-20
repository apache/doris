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

#include "exprs/in_predicate.h"

#include <sstream>

#include "exprs/anyval_util.h"
#include "exprs/expr_context.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.hpp"

namespace doris {

InPredicate::InPredicate(const TExprNode& node)
        : Predicate(node),
          _is_not_in(node.in_predicate.is_not_in),
          _is_prepare(false),
          _null_in_set(false),
          _hybrid_set() {}

InPredicate::~InPredicate() {}

Status InPredicate::prepare(RuntimeState* state, HybridSetBase* hset) {
    if (_is_prepare) {
        return Status::OK();
    }
    _hybrid_set.reset(hset);
    if (nullptr == _hybrid_set) {
        return Status::InternalError("Unknown column type.");
    }
    _is_prepare = true;

    return Status::OK();
}

Status InPredicate::open(RuntimeState* state, ExprContext* context,
                         FunctionContext::FunctionStateScope scope) {
    Expr::open(state, context, scope);

    for (int i = 1; i < _children.size(); ++i) {
        if (_children[0]->type().is_string_type()) {
            if (!_children[i]->type().is_string_type()) {
                return Status::InternalError("InPredicate type not same");
            }
        } else {
            if (_children[i]->type().type != _children[0]->type().type) {
                return Status::InternalError("InPredicate type not same");
            }
        }

        void* value = context->get_value(_children[i], nullptr);
        if (value == nullptr) {
            _null_in_set = true;
            continue;
        }
        _hybrid_set->insert(value);
    }
    return Status::OK();
}

Status InPredicate::prepare(RuntimeState* state, const RowDescriptor& row_desc,
                            ExprContext* context) {
    for (int i = 0; i < _children.size(); ++i) {
        RETURN_IF_ERROR(_children[i]->prepare(state, row_desc, context));
    }
    if (_is_prepare) {
        return Status::OK();
    }
    if (_children.size() < 1) {
        return Status::InternalError("no Function operator in.");
    }

    _hybrid_set.reset(create_set(_children[0]->type().type));
    if (nullptr == _hybrid_set.get()) {
        return Status::InternalError("Unknown column type.");
    }

    _is_prepare = true;

    return Status::OK();
}

void InPredicate::insert(void* value) {
    if (nullptr == value) {
        _null_in_set = true;
    } else {
        _hybrid_set->insert(value);
    }
}

std::string InPredicate::debug_string() const {
    std::stringstream out;
    out << "InPredicate(" << get_child(0)->debug_string() << " " << _is_not_in << ",[";
    int num_children = get_num_children();

    for (int i = 1; i < num_children; ++i) {
        out << (i == 1 ? "" : " ") << get_child(i)->debug_string();
    }

    out << "])";
    return out.str();
}

// this in predicate profile for case "a IN (1, 2, 3)"
// not for "a IN (b, 2, 3)"
// a, b is a column or a expr that contain slot
BooleanVal InPredicate::get_boolean_val(ExprContext* ctx, TupleRow* row) {
    void* lhs_slot = ctx->get_value(_children[0], row);
    if (lhs_slot == nullptr) {
        return BooleanVal::null();
    }
    // if find in const set, return true
    if (_hybrid_set->find(lhs_slot)) {
        return BooleanVal(!_is_not_in);
    }
    if (_null_in_set) {
        return BooleanVal::null();
    }
    return BooleanVal(_is_not_in);
}

} // namespace doris
