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

#include "codegen/doris_ir.h"
#include "exec/exec_node.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"

namespace doris {

namespace vectorized {

class VSetOperationNode : public ExecNode {
public:
    VSetOperationNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
        return Status::NotSupported("Not Implemented get RowBatch in vecorized execution.");
    }
    virtual Status close(RuntimeState* state);
    virtual void debug_string(int indentation_level, std::stringstream* out) const {};

protected:
    /// Const exprs materialized by this node. These exprs don't refer to any children.
    /// Only materialized by the first fragment instance to avoid duplication.
    std::vector<std::vector<VExprContext*>> _const_expr_lists;

    /// Exprs materialized by this node. The i-th result expr list refers to the i-th child.
    std::vector<std::vector<VExprContext*>> _child_expr_lists;

    /// Index of current const result expr list.
    int _const_expr_list_idx;

    /// Index of current child.
    int _child_idx;

    /// Index of current row in child_row_block_.
    int _child_row_idx;

    /// Saved from the last to GetNext() on the current child.
    bool _child_eos;

    /// Index of the child that needs to be closed on the next GetNext() call. Should be set
    /// to -1 if no child needs to be closed.
    int _to_close_child_idx;

    // Time spent to evaluates exprs and materializes the results
    RuntimeProfile::Counter* _materialize_exprs_evaluate_timer = nullptr;
};

} // namespace vectorized
} // namespace doris
