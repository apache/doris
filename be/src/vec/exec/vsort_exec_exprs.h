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

#pragma once

#include "vec/exprs/vexpr.h"
#include "runtime/runtime_state.h"

namespace doris {

class MemTracker;

// Helper class to Prepare() , Open() and Close() the ordering expressions used to perform
// comparisons in a sort. Used by TopNNode, SortNode.  When two
// rows are compared, the ordering expressions are evaluated once for each side.
// TopN and Sort materialize input rows into a single tuple before sorting.
// If _materialize_tuple is true, SortExecExprs also stores the slot expressions used to
// materialize the sort tuples.
namespace vectorized {

class VSortExecExprs {
public:
    // Initialize the expressions from a TSortInfo using the specified pool.
    Status init(const TSortInfo &sort_info, ObjectPool *pool);

    // Initialize the ordering and (optionally) materialization expressions from the thrift
    // TExprs into the specified pool. sort_tuple_slot_exprs is NULL if the tuple is not
    // materialized.
    Status init(const std::vector<TExpr> &ordering_exprs,
                const std::vector<TExpr> *sort_tuple_slot_exprs, ObjectPool *pool);

    // prepare all expressions used for sorting and tuple materialization.
    Status prepare(RuntimeState *state, const RowDescriptor &child_row_desc,
                   const RowDescriptor &output_row_desc,
                   const std::shared_ptr<MemTracker> &mem_tracker);

    // open all expressions used for sorting and tuple materialization.
    Status open(RuntimeState *state);

    // close all expressions used for sorting and tuple materialization.
    void close(RuntimeState *state);

    const std::vector<VExprContext *> &sort_tuple_slot_expr_ctxs() const {
        return _sort_tuple_slot_expr_ctxs;
    }

    // Can only be used after calling prepare()
    const std::vector<VExprContext *> &lhs_ordering_expr_ctxs() const {
        return _lhs_ordering_expr_ctxs;
    }

    // Can only be used after calling open()
    const std::vector<VExprContext *> &rhs_ordering_expr_ctxs() const {
        return _rhs_ordering_expr_ctxs;
    }

    bool need_materialize_tuple() const {
        return _materialize_tuple;
    }

private:
    // Create two VExprContexts for evaluating over the TupleRows.
    std::vector<VExprContext *> _lhs_ordering_expr_ctxs;
    std::vector<VExprContext *> _rhs_ordering_expr_ctxs;

    // If true, the tuples to be sorted are materialized by
    // _sort_tuple_slot_exprs before the actual sort is performed.
    bool _materialize_tuple;

    // Expressions used to materialize slots in the tuples to be sorted.
    // One expr per slot in the materialized tuple. Valid only if
    // _materialize_tuple is true.
    std::vector<VExprContext *> _sort_tuple_slot_expr_ctxs;

    // Initialize directly from already-created VExprContexts. Callers should manually call
    // Prepare(), Open(), and Close() on input VExprContexts (instead of calling the
    // analogous functions in this class). Used for testing.
    Status init(const std::vector<VExprContext *> &lhs_ordering_expr_ctxs,
                const std::vector<VExprContext *> &rhs_ordering_expr_ctxs);
};

} // namepace vectorized
} // namespace doris

