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

#include "vec/exprs/vexpr_context.h"

#include "runtime/thread_context.h"
#include "udf/udf_internal.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {
VExprContext::VExprContext(VExpr* expr)
        : _root(expr),
          _is_clone(false),
          _prepared(false),
          _opened(false),
          _closed(false),
          _last_result_column_id(-1) {}

doris::Status VExprContext::execute(doris::vectorized::Block* block, int* result_column_id) {
    Status st = _root->execute(this, block, result_column_id);
    _last_result_column_id = *result_column_id;
    return st;
}

doris::Status VExprContext::prepare(doris::RuntimeState* state,
                                    const doris::RowDescriptor& row_desc,
                                    const std::shared_ptr<doris::MemTracker>& tracker) {
    _prepared = true;
    _mem_tracker = tracker;
    SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER(_mem_tracker);
    _pool.reset(new MemPool(_mem_tracker.get()));
    return _root->prepare(state, row_desc, this);
}

doris::Status VExprContext::open(doris::RuntimeState* state) {
    DCHECK(_prepared);
    if (_opened) {
        return Status::OK();
    }
    SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER(_mem_tracker);
    _opened = true;
    // Fragment-local state is only initialized for original contexts. Clones inherit the
    // original's fragment state and only need to have thread-local state initialized.
    FunctionContext::FunctionStateScope scope =
            _is_clone ? FunctionContext::THREAD_LOCAL : FunctionContext::FRAGMENT_LOCAL;
    return _root->open(state, this, scope);
}

void VExprContext::close(doris::RuntimeState* state) {
    DCHECK(!_closed);
    FunctionContext::FunctionStateScope scope =
            _is_clone ? FunctionContext::THREAD_LOCAL : FunctionContext::FRAGMENT_LOCAL;
    _root->close(state, this, scope);

    for (int i = 0; i < _fn_contexts.size(); ++i) {
        _fn_contexts[i]->impl()->close();
        delete _fn_contexts[i];
    }
    // _pool can be NULL if Prepare() was never called
    if (_pool != NULL) {
        _pool->free_all();
    }
    _closed = true;
}

doris::Status VExprContext::clone(RuntimeState* state, VExprContext** new_ctx) {
    DCHECK(_prepared);
    DCHECK(_opened);
    DCHECK(*new_ctx == nullptr);
    SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER(_mem_tracker);

    *new_ctx = state->obj_pool()->add(new VExprContext(_root));
    (*new_ctx)->_pool.reset(new MemPool(_pool->mem_tracker()));
    for (auto& _fn_context : _fn_contexts) {
        (*new_ctx)->_fn_contexts.push_back(_fn_context->impl()->clone((*new_ctx)->_pool.get()));
    }

    (*new_ctx)->_is_clone = true;
    (*new_ctx)->_prepared = true;
    (*new_ctx)->_opened = true;
    (*new_ctx)->_mem_tracker = _mem_tracker;

    return _root->open(state, *new_ctx, FunctionContext::THREAD_LOCAL);
}

int VExprContext::register_func(RuntimeState* state, const FunctionContext::TypeDesc& return_type,
                                const std::vector<FunctionContext::TypeDesc>& arg_types,
                                int varargs_buffer_size) {
    _fn_contexts.push_back(FunctionContextImpl::create_context(
            state, _pool.get(), return_type, arg_types, varargs_buffer_size, false));
    return _fn_contexts.size() - 1;
}

Status VExprContext::filter_block(VExprContext* vexpr_ctx, Block* block, int column_to_keep) {
    if (vexpr_ctx == nullptr || block->rows() == 0) {
        return Status::OK();
    }
    int result_column_id = -1;
    vexpr_ctx->execute(block, &result_column_id);
    return Block::filter_block(block, result_column_id, column_to_keep);
}

Status VExprContext::filter_block(const std::unique_ptr<VExprContext*>& vexpr_ctx_ptr, Block* block,
                                  int column_to_keep) {
    if (vexpr_ctx_ptr == nullptr || block->rows() == 0) {
        return Status::OK();
    }
    DCHECK((*vexpr_ctx_ptr) != nullptr);
    int result_column_id = -1;
    (*vexpr_ctx_ptr)->execute(block, &result_column_id);
    return Block::filter_block(block, result_column_id, column_to_keep);
}

Block VExprContext::get_output_block_after_execute_exprs(
        const std::vector<vectorized::VExprContext*>& output_vexpr_ctxs, const Block& input_block,
        Status& status) {
    vectorized::Block tmp_block(input_block.get_columns_with_type_and_name());
    vectorized::ColumnsWithTypeAndName result_columns;
    for (auto vexpr_ctx : output_vexpr_ctxs) {
        int result_column_id = -1;
        status = vexpr_ctx->execute(&tmp_block, &result_column_id);
        if (UNLIKELY(!status.ok())) return {};
        DCHECK(result_column_id != -1);
        result_columns.emplace_back(tmp_block.get_by_position(result_column_id));
    }

    return {result_columns};
}

} // namespace doris::vectorized
