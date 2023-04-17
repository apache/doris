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
    [[nodiscard]] Status prepare(RuntimeState* state, const RowDescriptor& row_desc);
    [[nodiscard]] Status open(RuntimeState* state);
    void close(RuntimeState* state);
    [[nodiscard]] Status clone(RuntimeState* state, VExprContext** new_ctx);
    [[nodiscard]] Status execute(Block* block, int* result_column_id);

    VExpr* root() { return _root; }
    void set_root(VExpr* expr) { _root = expr; }

    /// Creates a FunctionContext, and returns the index that's passed to fn_context() to
    /// retrieve the created context. Exprs that need a FunctionContext should call this in
    /// Prepare() and save the returned index. 'varargs_buffer_size', if specified, is the
    /// size of the varargs buffer in the created FunctionContext (see udf-internal.h).
    int register_function_context(RuntimeState* state, const doris::TypeDescriptor& return_type,
                                  const std::vector<doris::TypeDescriptor>& arg_types);

    /// Retrieves a registered FunctionContext. 'i' is the index returned by the call to
    /// register_function_context(). This should only be called by VExprs.
    FunctionContext* fn_context(int i) {
        DCHECK_GE(i, 0);
        DCHECK_LT(i, _fn_contexts.size());
        return _fn_contexts[i].get();
    }

    [[nodiscard]] static Status filter_block(VExprContext* vexpr_ctx, Block* block,
                                             int column_to_keep);
    [[nodiscard]] static Status filter_block(const std::unique_ptr<VExprContext*>& vexpr_ctx_ptr,
                                             Block* block, int column_to_keep);

    static Block get_output_block_after_execute_exprs(const std::vector<vectorized::VExprContext*>&,
                                                      const Block&, Status&);

    int get_last_result_column_id() const {
        DCHECK(_last_result_column_id != -1);
        return _last_result_column_id;
    }

    FunctionContext::FunctionStateScope get_function_state_scope() const {
        return _is_clone ? FunctionContext::THREAD_LOCAL : FunctionContext::FRAGMENT_LOCAL;
    }

    void clone_fn_contexts(VExprContext* other);

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
    std::vector<std::unique_ptr<FunctionContext>> _fn_contexts;

    int _last_result_column_id;

    /// The depth of expression-tree.
    int _depth_num = 0;
};
} // namespace doris::vectorized
