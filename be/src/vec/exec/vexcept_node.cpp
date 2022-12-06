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

#include "vec/exec/vexcept_node.h"

#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/exec/vset_operation_node.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
namespace doris {
namespace vectorized {

VExceptNode::VExceptNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : VSetOperationNode(pool, tnode, descs) {}

Status VExceptNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(VSetOperationNode::init(tnode, state));
    DCHECK(tnode.__isset.except_node);
    return Status::OK();
}

Status VExceptNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(VSetOperationNode::prepare(state));
    return Status::OK();
}

Status VExceptNode::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VExceptNode::open");
    RETURN_IF_ERROR(VSetOperationNode::open(state));
    bool eos = false;
    Status st = Status::OK();
    for (int i = 1; i < _children.size(); ++i) {
        if (i > 1) {
            refresh_hash_table<false>();
        }

        RETURN_IF_ERROR(child(i)->open(state));
        eos = false;
        int probe_expr_ctxs_sz = _child_expr_lists[i].size();
        _probe_columns.resize(probe_expr_ctxs_sz);

        while (!eos) {
            RETURN_IF_ERROR(process_probe_block(state, i, &eos));
            if (_probe_rows == 0) continue;

            std::visit(
                    [&](auto&& arg) {
                        using HashTableCtxType = std::decay_t<decltype(arg)>;
                        if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                            HashTableProbe<HashTableCtxType, false> process_hashtable_ctx(
                                    this, state->batch_size(), _probe_rows);
                            st = process_hashtable_ctx.mark_data_in_hashtable(arg);

                        } else {
                            LOG(FATAL) << "FATAL: uninited hash table";
                        }
                    },
                    *_hash_table_variants);
        }
    }
    return st;
}

Status VExceptNode::get_next(RuntimeState* state, Block* output_block, bool* eos) {
    INIT_AND_SCOPE_GET_NEXT_SPAN(state->get_tracer(), _get_next_span, "VExceptNode::get_next");
    SCOPED_TIMER(_probe_timer);
    Status st;
    create_mutable_cols(output_block);

    std::visit(
            [&](auto&& arg) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    HashTableProbe<HashTableCtxType, false> process_hashtable_ctx(
                            this, state->batch_size(), _probe_rows);
                    st = process_hashtable_ctx.get_data_in_hashtable(arg, _mutable_cols,
                                                                     output_block, eos);
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            *_hash_table_variants);

    RETURN_IF_ERROR(
            VExprContext::filter_block(_vconjunct_ctx_ptr, output_block, output_block->columns()));
    reached_limit(output_block, eos);

    return st;
}

Status VExceptNode::close(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VExceptNode::close");
    return VSetOperationNode::close(state);
}

} // namespace vectorized
} // namespace doris
