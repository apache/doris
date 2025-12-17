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

#include <gen_cpp/internal_service.pb.h>

#include "common/status.h"
#include "operator.h"
#include "pipeline/exec/union_sink_operator.h"
#include "pipeline/rec_cte_shared_state.h"
#include "util/brpc_client_cache.h"
#include "util/uid_util.h"
#include "vec/core/block.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized

namespace pipeline {
class DataQueue;

class RecCTESourceOperatorX;
class RecCTESourceLocalState final : public PipelineXLocalState<RecCTESharedState> {
public:
    ENABLE_FACTORY_CREATOR(RecCTESourceLocalState);
    using Base = PipelineXLocalState<RecCTESharedState>;
    using Parent = RecCTESourceOperatorX;
    RecCTESourceLocalState(RuntimeState* state, OperatorXBase* parent);

    Status open(RuntimeState* state) override;
    Status init(RuntimeState* state, LocalStateInfo& info) override;

    bool is_blockable() const override { return true; }

    std::vector<Dependency*> dependencies() const override {
        return std::vector<Dependency*> {_dependency, _anchor_dependency.get()};
    }

private:
    friend class RecCTESourceOperatorX;
    friend class OperatorX<RecCTESourceLocalState>;

    std::shared_ptr<Dependency> _anchor_dependency = nullptr;
};

class RecCTESourceOperatorX : public OperatorX<RecCTESourceLocalState> {
public:
    using Base = OperatorX<RecCTESourceLocalState>;
    RecCTESourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                          const DescriptorTbl& descs)
            : Base(pool, tnode, operator_id, descs),
              _is_union_all(tnode.rec_cte_node.is_union_all),
              _targets(tnode.rec_cte_node.targets),
              _fragments_to_reset(tnode.rec_cte_node.fragments_to_reset),
              _global_rf_ids(tnode.rec_cte_node.rec_side_runtime_filter_ids),
              _is_used_by_other_rec_cte(tnode.rec_cte_node.is_used_by_other_rec_cte) {
        DCHECK(tnode.__isset.rec_cte_node);
    }
    ~RecCTESourceOperatorX() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;

    Status terminate(RuntimeState* state) override {
        RETURN_IF_ERROR(_send_close(state));
        return Base::terminate(state);
    }

    Status close(RuntimeState* state) override {
        RETURN_IF_ERROR(_send_close(state));
        return Base::close(state);
    }

    Status set_child(OperatorPtr child) override {
        Base::_child = child;
        return Status::OK();
    }

    bool is_serial_operator() const override { return true; }

    DataDistribution required_data_distribution(RuntimeState* /*state*/) const override {
        return {ExchangeType::NOOP};
    }

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override {
        auto& local_state = get_local_state(state);
        auto& ctx = local_state._shared_state;
        ctx->update_ready_to_return();

        if (!ctx->ready_to_return) {
            if (ctx->current_round + 1 > _max_recursion_depth) {
                return Status::Aborted("reach cte_max_recursion_depth {}", _max_recursion_depth);
            }

            ctx->source_dep->block();
            // ctx->blocks.size() may be changed after _recursive_process
            int current_blocks_size = int(ctx->blocks.size());
            RETURN_IF_ERROR(_recursive_process(state, ctx->last_round_offset));
            ctx->current_round++;
            ctx->last_round_offset = current_blocks_size;
        } else {
            if (ctx->blocks.empty()) {
                *eos = true;
            } else {
                block->swap(ctx->blocks.back());
                RETURN_IF_ERROR(
                        local_state.filter_block(local_state.conjuncts(), block, block->columns()));
                ctx->blocks.pop_back();
            }
        }
        return Status::OK();
    }

    bool is_source() const override { return true; }

private:
    Status _send_close(RuntimeState* state) {
        if (!_already_send_close && !_is_used_by_other_rec_cte) {
            _already_send_close = true;
            auto* round_counter = ADD_COUNTER(get_local_state(state).Base::custom_profile(),
                                              "RecursiveRound", TUnit::UNIT);
            round_counter->set(int64_t(get_local_state(state)._shared_state->current_round));

            RETURN_IF_ERROR(_send_rerun_fragments(state, PRerunFragmentParams::close));
        }
        return Status::OK();
    }

    Status _recursive_process(RuntimeState* state, size_t last_round_offset) const {
        RETURN_IF_ERROR(_send_rerun_fragments(state, PRerunFragmentParams::wait));
        RETURN_IF_ERROR(_send_reset_global_rf(state));
        RETURN_IF_ERROR(_send_rerun_fragments(state, PRerunFragmentParams::release));
        RETURN_IF_ERROR(_send_rerun_fragments(state, PRerunFragmentParams::rebuild));
        RETURN_IF_ERROR(_send_rerun_fragments(state, PRerunFragmentParams::submit));
        RETURN_IF_ERROR(get_local_state(state)._shared_state->send_data_to_targets(
                state, last_round_offset));
        return Status::OK();
    }

    Status _send_reset_global_rf(RuntimeState* state) const {
        TNetworkAddress addr;
        RETURN_IF_ERROR(state->global_runtime_filter_mgr()->get_merge_addr(&addr));
        auto stub =
                state->get_query_ctx()->exec_env()->brpc_internal_client_cache()->get_client(addr);
        PResetGlobalRfParams request;
        request.mutable_query_id()->CopyFrom(UniqueId(state->query_id()).to_proto());
        for (auto filter_id : _global_rf_ids) {
            request.add_filter_ids(filter_id);
        }

        PResetGlobalRfResult result;
        brpc::Controller controller;
        controller.set_timeout_ms(
                get_execution_rpc_timeout_ms(state->get_query_ctx()->execution_timeout()));
        stub->reset_global_rf(&controller, &request, &result, brpc::DoNothing());
        brpc::Join(controller.call_id());
        return Status::create(result.status());
    }

    Status _send_rerun_fragments(RuntimeState* state, PRerunFragmentParams_Opcode stage) const {
        for (auto fragment : _fragments_to_reset) {
            auto stub =
                    state->get_query_ctx()->exec_env()->brpc_internal_client_cache()->get_client(
                            fragment.addr);

            PRerunFragmentParams request;
            request.mutable_query_id()->CopyFrom(UniqueId(state->query_id()).to_proto());
            request.set_fragment_id(fragment.fragment_id);
            request.set_stage(stage);

            PRerunFragmentResult result;
            brpc::Controller controller;
            controller.set_timeout_ms(
                    get_execution_rpc_timeout_ms(state->get_query_ctx()->execution_timeout()));
            stub->rerun_fragment(&controller, &request, &result, brpc::DoNothing());
            brpc::Join(controller.call_id());
            if (controller.Failed()) {
                return Status::InternalError(controller.ErrorText());
            }

            auto rpc_st = Status::create(result.status());
            if (!rpc_st.ok()) {
                return rpc_st;
            }
        }
        return Status::OK();
    }

    friend class RecCTESourceLocalState;
    std::vector<vectorized::DataTypePtr> _hash_table_key_types;

    const bool _is_union_all = false;
    std::vector<TRecCTETarget> _targets;
    std::vector<TRecCTEResetInfo> _fragments_to_reset;
    std::vector<int> _global_rf_ids;

    int _max_recursion_depth = 0;

    bool _already_send_close = false;

    bool _is_used_by_other_rec_cte = false;
};

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris