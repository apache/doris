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

#include "common/status.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"

namespace doris::vectorized {
class VExpr;

class VExprContext {
public:
    VExprContext(VExpr* expr);
    ~VExprContext();
    Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                   const std::shared_ptr<MemTracker>& tracker);
    Status open(RuntimeState* state);
    void close(RuntimeState* state);
    Status clone(RuntimeState* state, VExprContext** new_ctx);
    Status execute(Block* block, int* result_column_id);

    VExpr* root() { return _root; }
    void set_root(VExpr* expr) { _root = expr; }

    /// Creates a FunctionContext, and returns the index that's passed to fn_context() to
    /// retrieve the created context. Exprs that need a FunctionContext should call this in
    /// Prepare() and save the returned index. 'varargs_buffer_size', if specified, is the
    /// size of the varargs buffer in the created FunctionContext (see udf-internal.h).
    int register_func(RuntimeState* state, const FunctionContext::TypeDesc& return_type,
                      const std::vector<FunctionContext::TypeDesc>& arg_types,
                      int varargs_buffer_size);

    /// Retrieves a registered FunctionContext. 'i' is the index returned by the call to
    /// register_func(). This should only be called by VExprs.
    FunctionContext* fn_context(int i) {
        DCHECK_GE(i, 0);
        DCHECK_LT(i, _fn_contexts.size());
        return _fn_contexts[i];
    }

    static Status filter_block(VExprContext* vexpr_ctx, Block* block, int column_to_keep);
    static Status filter_block(const std::unique_ptr<VExprContext*>& vexpr_ctx_ptr, Block* block,
                               int column_to_keep);

    static Block get_output_block_after_execute_exprs(const std::vector<vectorized::VExprContext*>&,
                                                      const Block&, Status&);

    int get_last_result_column_id() {
        DCHECK(_last_result_column_id != -1);
        return _last_result_column_id;
    }

    FunctionContext::FunctionStateScope get_function_state_scope() const {
        return _is_clone ? FunctionContext::THREAD_LOCAL : FunctionContext::FRAGMENT_LOCAL;
    }

private:
    friend class VExpr;

    /// The expr tree this context is for.
    VExpr* _root;

    /// True if this context came from a Clone() call. Used to manage FunctionStateScope.
    bool _is_clone;

    /// Variables keeping track of current state.
    bool _prepared;
    bool _opened;
    bool _closed;

    /// FunctionContexts for each registered expression. The FunctionContexts are created
    /// and owned by this VExprContext.
    std::vector<FunctionContext*> _fn_contexts;

    /// Pool backing fn_contexts_. Counts against the runtime state's UDF mem tracker.
    std::unique_ptr<MemPool> _pool;

    int _last_result_column_id;
};
} // namespace doris::vectorized
