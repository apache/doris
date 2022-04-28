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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/tuple-is-null-predicate.h
// and modified by Doris

#ifndef DORIS_BE_SRC_QUERY_EXPRS_TUPLE_IS_NULL_PREDICATE_H
#define DORIS_BE_SRC_QUERY_EXPRS_TUPLE_IS_NULL_PREDICATE_H

#include "common/object_pool.h"
#include "exprs/predicate.h"

namespace doris {

class TExprNode;

class TupleIsNullPredicate : public Predicate {
public:
    virtual Expr* clone(ObjectPool* pool) const override {
        return pool->add(new TupleIsNullPredicate(*this));
    }

    bool is_constant() const override { return false; }

protected:
    friend class Expr;

    TupleIsNullPredicate(const TExprNode& node);

    virtual Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                           ExprContext* ctx) override;

    virtual BooleanVal get_boolean_val(ExprContext* ctx, TupleRow* row) override;
    virtual std::string debug_string() const override;

private:
    std::vector<TupleId> _tuple_ids;
    std::vector<int32_t> _tuple_idxs;
};

} // namespace doris

#endif
