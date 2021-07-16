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

#include "vec/exec/vset_operation_node.h"

namespace doris {
namespace vectorized {

class VUnionNode : public VSetOperationNode {
public:
    VUnionNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos);
    virtual Status close(RuntimeState* state);

private:
    /// Index of the first non-passthrough child; i.e. a child that needs materialization.
    /// 0 when all children are materialized, '_children.size()' when no children are
    /// materialized.
    const int _first_materialized_child_idx;

    /// GetNext() for the passthrough case. We pass 'block' directly into the GetNext()
    /// call on the child.
    Status get_next_pass_through(RuntimeState* state, Block* block);

    /// GetNext() for the materialized case. Materializes and evaluates rows from each
    /// non-passthrough child.
    Status get_next_materialized(RuntimeState* state, Block* block);

    /// GetNext() for the constant expression case.
    Status get_next_const(RuntimeState* state, Block* block);

    /// Evaluates exprs for the current child and materializes the results into 'tuple_buf',
    /// which is attached to 'dst_block'. Runs until 'dst_block' is at capacity, or all rows
    /// have been consumed from the current child block. Updates '_child_row_idx'.
    Block materialize_block(Block* dst_block);

    Status get_error_msg(const std::vector<VExprContext*>& exprs);

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

    virtual void debug_string(int indentation_level, std::stringstream* out) const;
};

} // namespace vectorized
} // namespace doris
