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
// https://github.com/cloudera/Impala/blob/v0.7refresh/be/src/exec/merge-node.h
// and modified by Doris

#ifndef DORIS_BE_SRC_QUERY_EXEC_MERGE_NODE_H
#define DORIS_BE_SRC_QUERY_EXEC_MERGE_NODE_H

#include "exec/exec_node.h"
#include "runtime/mem_pool.h"

namespace doris {

class Tuple;
class TupleRow;

// Node that merges the results of its children by materializing their
// evaluated expressions into row batches. The MergeNode pulls row batches sequentially
// from its children sequentially, i.e., it exhausts one child completely before moving
// on to the next one.
class MergeNode : public ExecNode {
public:
    MergeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    virtual ~MergeNode() {}

    // Create const exprs, child exprs and conjuncts from corresponding thrift exprs.
    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status close(RuntimeState* state);

private:
    const static int INVALID_CHILD_IDX = -1;

    // Tuple id resolved in prepare() to set _tuple_desc;
    int _tuple_id;

    // Descriptor for tuples this merge node constructs.
    const TupleDescriptor* _tuple_desc;

    // those tuple_desc_->slots() which are materialized, in the same order
    std::vector<SlotDescriptor*> _materialized_slots;

    // Const exprs materialized by this node. These exprs don't refer to any children.
    std::vector<std::vector<ExprContext*>> _const_result_expr_ctx_lists;

    // Exprs materialized by this node. The i-th result expr list refers to the i-th child.
    std::vector<std::vector<ExprContext*>> _result_expr_ctx_lists;

    // Index of current const result expr list.
    int _const_result_expr_idx;

    // Index of current child.
    int _child_idx;

    // Current row batch of current child. We reset the pointer to a new RowBatch
    // when switching to a different child.
    std::unique_ptr<RowBatch> _child_row_batch;

    // Saved from the last to get_next() on the current child.
    bool _child_eos;

    // Index of current row in _child_row_batch.
    int _child_row_idx;

    // Evaluates exprs on all rows in _child_row_batch starting from _child_row_idx,
    // and materializes their results into *tuple.
    // Adds *tuple into row_batch, and increments *tuple.
    // If const_exprs is true, then the exprs are evaluated exactly once without
    // fetching rows from _child_row_batch.
    // Only commits tuples to row_batch if they are not filtered by conjuncts.
    // Returns true if row_batch should be returned to caller or limit has been
    // reached, false otherwise.
    bool eval_and_materialize_exprs(const std::vector<ExprContext*>& exprs, bool const_exprs,
                                    Tuple** tuple, RowBatch* row_batch);
};

} // namespace doris

#endif
