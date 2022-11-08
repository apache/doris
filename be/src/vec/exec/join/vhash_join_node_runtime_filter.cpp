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

#include "gen_cpp/PlanNodes_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_filter_mgr.h"
#include "util/defer_op.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exec/join/vhash_join_node.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/template_helpers.hpp"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

template <class HashTableContext>
struct ProcessRuntimeFilterBuild {
    ProcessRuntimeFilterBuild(HashJoinNode* join_node) : _join_node(join_node) {}

    Status operator()(RuntimeState* state, HashTableContext& hash_table_ctx) {
        if (_join_node->_runtime_filter_descs.empty()) {
            return Status::OK();
        }
        VRuntimeFilterSlots runtime_filter_slots(_join_node->_probe_expr_ctxs,
                                                 _join_node->_build_expr_ctxs,
                                                 _join_node->_runtime_filter_descs);

        RETURN_IF_ERROR(runtime_filter_slots.init(state, hash_table_ctx.hash_table.get_size()));

        if (!runtime_filter_slots.empty() && !_join_node->_inserted_rows.empty()) {
            {
                SCOPED_TIMER(_join_node->_push_compute_timer);
                runtime_filter_slots.insert(_join_node->_inserted_rows);
            }
        }
        {
            SCOPED_TIMER(_join_node->_push_down_timer);
            runtime_filter_slots.publish();
        }

        return Status::OK();
    }

private:
    HashJoinNode* _join_node;
};

Status HashJoinNode::_runtime_filter_build_process_variants(RuntimeState* state) {
    return std::visit(
            [&](auto&& arg) -> Status {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    ProcessRuntimeFilterBuild<HashTableCtxType> runtime_filter_build_process(this);
                    return runtime_filter_build_process(state, arg);
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            _hash_table_variants);
}

}