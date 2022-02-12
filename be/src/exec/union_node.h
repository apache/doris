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

#include "exec/exec_node.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"

namespace doris {

class DescriptorTbl;
class ExprContext;
class Tuple;
class TupleRow;
class TPlanNode;

/// Node that merges the results of its children by either materializing their
/// evaluated expressions into row batches or passing through (forwarding) the
/// batches if the input tuple layout is identical to the output tuple layout
/// and expressions don't need to be evaluated. The children should be ordered
/// such that all passthrough children come before the children that need
/// materialization. The union node pulls from its children sequentially, i.e.
/// it exhausts one child completely before moving on to the next one.
class UnionNode : public ExecNode {
public:
    UnionNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    // virtual Status reset(RuntimeState* state);
    virtual Status close(RuntimeState* state);

protected:
    void debug_string(int indentation_level, std::stringstream* out) const;

private:
    /// Tuple id resolved in Prepare() to set tuple_desc_;
    const int _tuple_id;

    /// Descriptor for tuples this union node constructs.
    const TupleDescriptor* _tuple_desc;

    /// Index of the first non-passthrough child; i.e. a child that needs materialization.
    /// 0 when all children are materialized, '_children.size()' when no children are
    /// materialized.
    const int _first_materialized_child_idx;

    /// Const exprs materialized by this node. These exprs don't refer to any children.
    /// Only materialized by the first fragment instance to avoid duplication.
    std::vector<std::vector<ExprContext*>> _const_expr_lists;

    /// Exprs materialized by this node. The i-th result expr list refers to the i-th child.
    std::vector<std::vector<ExprContext*>> _child_expr_lists;

    /////////////////////////////////////////
    /// BEGIN: Members that must be Reset()

    /// Index of current child.
    int _child_idx;

    /// Current row batch of current child. We reset the pointer to a new RowBatch
    /// when switching to a different child.
    std::unique_ptr<RowBatch> _child_batch;

    /// Index of current row in child_row_batch_.
    int _child_row_idx;

    typedef void (*UnionMaterializeBatchFn)(UnionNode*, RowBatch*, uint8_t**);
    /// Vector of pointers to codegen'ed materialize_batch functions. The vector contains one
    /// function for each child. The size of the vector should be equal to the number of
    /// children. If a child is passthrough, there should be a nullptr for that child. If
    /// Codegen is disabled, there should be a nullptr for every child.
    std::vector<UnionMaterializeBatchFn> _codegend_union_materialize_batch_fns;

    /// Saved from the last to GetNext() on the current child.
    bool _child_eos;

    /// Index of current const result expr list.
    int _const_expr_list_idx;

    /// Index of the child that needs to be closed on the next GetNext() call. Should be set
    /// to -1 if no child needs to be closed.
    int _to_close_child_idx;

    // Time spent to evaluates exprs and materializes the results
    RuntimeProfile::Counter* _materialize_exprs_evaluate_timer = nullptr;

    /// END: Members that must be Reset()
    /////////////////////////////////////////

    /// The following GetNext* functions don't apply the limit. It must be enforced by the
    /// caller.

    /// GetNext() for the passthrough case. We pass 'row_batch' directly into the GetNext()
    /// call on the child.
    Status get_next_pass_through(RuntimeState* state, RowBatch* row_batch);

    /// GetNext() for the materialized case. Materializes and evaluates rows from each
    /// non-passthrough child.
    Status get_next_materialized(RuntimeState* state, RowBatch* row_batch);

    /// GetNext() for the constant expression case.
    Status get_next_const(RuntimeState* state, RowBatch* row_batch);

    /// Evaluates exprs for the current child and materializes the results into 'tuple_buf',
    /// which is attached to 'dst_batch'. Runs until 'dst_batch' is at capacity, or all rows
    /// have been consumed from the current child batch. Updates '_child_row_idx'.
    void materialize_batch(RowBatch* dst_batch, uint8_t** tuple_buf);

    /// Evaluates 'exprs' over 'row', materializes the results in 'tuple_buf'.
    /// and appends the new tuple to 'dst_batch'. Increments '_num_rows_returned'.
    void materialize_exprs(const std::vector<ExprContext*>& exprs, TupleRow* row,
                           uint8_t* tuple_buf, RowBatch* dst_batch);

    Status get_error_msg(const std::vector<ExprContext*>& exprs);

    /// Returns true if the child at 'child_idx' can be passed through.
    bool is_child_passthrough(int child_idx) const {
        DCHECK_LT(child_idx, _children.size());
        return child_idx < _first_materialized_child_idx;
    }

    /// Returns true if there are still rows to be returned from passthrough children.
    bool has_more_passthrough() const { return _child_idx < _first_materialized_child_idx; }

    /// Returns true if there are still rows to be returned from children that need
    /// materialization.
    bool has_more_materialized() const {
        return _first_materialized_child_idx != _children.size() && _child_idx < _children.size();
    }

    /// Returns true if there are still rows to be returned from constant expressions.
    bool has_more_const(const RuntimeState* state) const {
        return state->per_fragment_instance_idx() == 0 &&
               _const_expr_list_idx < _const_expr_lists.size();
    }
};

} // namespace doris
