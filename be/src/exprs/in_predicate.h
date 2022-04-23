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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/in-predicate.h
// and modified by Doris

#ifndef DORIS_BE_SRC_QUERY_EXPRS_IN_PREDICATE_H
#define DORIS_BE_SRC_QUERY_EXPRS_IN_PREDICATE_H

#include <string>
#include <unordered_set>

#include "exprs/create_predicate_function.h"
#include "exprs/hybrid_set.h"
#include "exprs/predicate.h"
#include "runtime/raw_value.h"

namespace doris {

// has two method:
// 1. construct from TExprNode
// 2. construct by new one, and push child.
class InPredicate : public Predicate {
public:
    virtual ~InPredicate();
    virtual Expr* clone(ObjectPool* pool) const override {
        return pool->add(new InPredicate(*this));
    }

    Status prepare(RuntimeState* state, HybridSetBase* hset);
    Status open(RuntimeState* state, ExprContext* context,
                FunctionContext::FunctionStateScope scope) override;
    virtual Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                           ExprContext* context) override;

    virtual BooleanVal get_boolean_val(ExprContext* context, TupleRow* row) override;

    // this function add one item in hashset, not add to children.
    // if add to children, when List is long, copy is a expensive op.
    void insert(void* value);

    HybridSetBase* hybrid_set() const { return _hybrid_set.get(); }

    bool is_not_in() const { return _is_not_in; }

protected:
    friend class Expr;
    friend class HashJoinNode;
    friend class RuntimePredicateWrapper;

    InPredicate(const TExprNode& node);

    // virtual Status prepare(RuntimeState* state, const RowDescriptor& desc);
    virtual std::string debug_string() const override;

private:
    const bool _is_not_in;
    bool _is_prepare;
    bool _null_in_set;
    std::shared_ptr<HybridSetBase> _hybrid_set;
};

} // namespace doris

#endif
