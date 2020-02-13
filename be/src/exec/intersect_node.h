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


#ifndef  DORIS_BE_SRC_QUERY_EXEC_INTERSECT_NODE_H
#define  DORIS_BE_SRC_QUERY_EXEC_INTERSECT_NODE_H

#include "exec/exec_node.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"

namespace doris {

// Node that calulate the intersect results of its children by either materializing their
// evaluated expressions into row batches or passing through (forwarding) the
// batches if the input tuple layout is identical to the output tuple layout
// and expressions don't need to be evaluated. The children should be ordered
// such that all passthrough children come before the children that need
// materialization. The interscet node pulls from its children sequentially, i.e.
// it exhausts one child completely before moving on to the next one.
class IntersectNode : public ExecNode {
public:
    IntersectNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual void codegen(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    // virtual Status reset(RuntimeState* state);
    virtual Status close(RuntimeState* state);

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

    typedef void (*IntersectMaterializeBatchFn)(IntersectNode*, RowBatch*, uint8_t**);
    /// Vector of pointers to codegen'ed materialize_batch functions. The vector contains one
    /// function for each child. The size of the vector should be equal to the number of
    /// children. If a child is passthrough, there should be a NULL for that child. If
    /// Codegen is disabled, there should be a NULL for every child.
    std::vector<IntersectMaterializeBatchFn> _codegend_except_materialize_batch_fns;

    /// Saved from the last to GetNext() on the current child.
    bool _child_eos;

    /// Index of current const result expr list.
    int _const_expr_list_idx;

    /// Index of the child that needs to be closed on the next GetNext() call. Should be set
    /// to -1 if no child needs to be closed.
    int _to_close_child_idx;

};

}; // namespace doris

#endif // DORIS_BE_SRC_QUERY_EXEC_INTERSECT_NODE_H