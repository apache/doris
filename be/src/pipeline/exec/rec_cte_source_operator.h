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

private:
    friend class RecCTESourceOperatorX;
    friend class OperatorX<RecCTESourceLocalState>;

    vectorized::VExprContextSPtrs _child_expr;
};

class RecCTESourceOperatorX : public OperatorX<RecCTESourceLocalState> {
public:
    using Base = OperatorX<RecCTESourceLocalState>;
    RecCTESourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                          const DescriptorTbl& descs)
            : Base(pool, tnode, operator_id, descs),
              _is_union_all(tnode.rec_cte_node.is_union_all),
              _targets(tnode.rec_cte_node.targets),
              _fragments_to_reset(tnode.rec_cte_node.fragments_to_reset) {
        DCHECK(tnode.__isset.rec_cte_node);
    }
    ~RecCTESourceOperatorX() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;

    Status set_child(OperatorPtr child) override {
        Base::_child = child;
        return Status::OK();
    }

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override {
        auto& local_state = get_local_state(state);
        auto& ctx = local_state._shared_state;
        LOG(WARNING) << "RecCTESinkOperatorX::sink eos, current_round: "
                     << local_state._shared_state->current_round
                     << " ready_to_return:" << ctx->ready_to_return
                     << " max_recursion_depth:" << _max_recursion_depth << " "
                     << ctx->max_recursion_depth;
        if (!ctx->ready_to_return) {
            ctx->source_dep->block();
            RETURN_IF_ERROR(_recursive_process(state, ctx->last_round_offset));
            ctx->current_round++;
            ctx->last_round_offset = int(ctx->blocks.size());
        } else {
            if (!_aready_send_close) {
                RETURN_IF_ERROR(_send_reset_fragments(state, true));
                _aready_send_close = true;
            }
            if (ctx->blocks.empty()) {
                *eos = true;
            } else {
                block->swap(ctx->blocks.back());
                ctx->blocks.pop_back();
            }
        }
        return Status::OK();
    }

    bool is_source() const override { return true; }

private:
    Status _recursive_process(RuntimeState* state, size_t last_round_offset) const {
        RETURN_IF_ERROR(_send_reset_fragments(state, false));
        RETURN_IF_ERROR(_send_data_to_targets(state, last_round_offset));
        return Status::OK();
    }

    Status _send_reset_fragments(RuntimeState* state, bool close) const {
        for (auto fragment : _fragments_to_reset) {
            if (state->fragment_id() == fragment.fragment_id) {
                return Status::InternalError("Fragment {} contains a recursive CTE node",
                                             fragment.fragment_id);
            }
            auto stub =
                    state->get_query_ctx()->exec_env()->brpc_internal_client_cache()->get_client(
                            fragment.addr);

            PResetFragmentParams request;
            request.mutable_query_id()->CopyFrom(UniqueId(state->query_id()).to_proto());
            request.set_fragment_id(fragment.fragment_id);
            request.set_close(close);

            PResetFragmentResult result;
            brpc::Controller controller;
            controller.set_timeout_ms(
                    get_execution_rpc_timeout_ms(state->get_query_ctx()->execution_timeout()));
            stub->reset_fragment(&controller, &request, &result, brpc::DoNothing());
            brpc::Join(controller.call_id());
            if (controller.Failed()) {
                return Status::InternalError(controller.ErrorText());
            }

            RETURN_IF_ERROR(Status::create(result.status()));
        }
        return Status::OK();
    }

    PTransmitRecCTEBlockParams _build_basic_param(RuntimeState* state,
                                                  const TRecCTETarget& target) const {
        PTransmitRecCTEBlockParams request;
        request.set_node_id(target.node_id);
        request.mutable_query_id()->CopyFrom(UniqueId(state->query_id()).to_proto());
        request.mutable_fragment_instance_id()->CopyFrom(
                UniqueId(target.fragment_instance_id).to_proto());
        return request;
    }

    Status _send_data_to_targets(RuntimeState* state, size_t last_round_offset) const {
        auto& local_state = get_local_state(state);
        auto& ctx = local_state._shared_state;

        int send_multi_blocks_byte_size = state->query_options().exchange_multi_blocks_byte_size;
        int block_number_per_target =
                int(ctx->blocks.size() - last_round_offset + _targets.size() - 1) / _targets.size();
        for (auto target : _targets) {
            auto stub =
                    state->get_query_ctx()->exec_env()->brpc_internal_client_cache()->get_client(
                            target.addr);
            if (!stub) {
                return Status::InternalError(fmt::format("Get rpc stub failed, host={}, port={}",
                                                         target.addr.hostname, target.addr.port));
            }

            // send blocks
            int step = block_number_per_target;
            while (last_round_offset < ctx->blocks.size() && step > 0) {
                PTransmitRecCTEBlockParams request = _build_basic_param(state, target);
                auto current_bytes = 0;
                while (last_round_offset < ctx->blocks.size() && step > 0 &&
                       current_bytes < send_multi_blocks_byte_size) {
                    auto* pblock = request.add_blocks();
                    size_t uncompressed_bytes = 0;
                    size_t compressed_bytes = 0;
                    RETURN_IF_ERROR(ctx->blocks[last_round_offset].serialize(
                            state->be_exec_version(), pblock, &uncompressed_bytes,
                            &compressed_bytes, state->fragement_transmission_compression_type()));
                    last_round_offset++;
                    step--;
                    current_bytes += compressed_bytes;
                }
                request.set_eos(false);

                PTransmitRecCTEBlockResult result;
                brpc::Controller controller;
                controller.set_timeout_ms(
                        get_execution_rpc_timeout_ms(state->get_query_ctx()->execution_timeout()));

                stub->transmit_rec_cte_block(&controller, &request, &result, brpc::DoNothing());
                brpc::Join(controller.call_id());
                RETURN_IF_ERROR(Status::create(result.status()));
            }

            // send eos
            {
                PTransmitRecCTEBlockParams request = _build_basic_param(state, target);
                request.set_eos(true);

                PTransmitRecCTEBlockResult result;
                brpc::Controller controller;
                stub->transmit_rec_cte_block(&controller, &request, &result, brpc::DoNothing());
                brpc::Join(controller.call_id());
                RETURN_IF_ERROR(Status::create(result.status()));
            }
        }
        return Status::OK();
    }

    friend class RecCTESourceLocalState;

    vectorized::VExprContextSPtrs _child_expr;

    const bool _is_union_all = false;
    std::vector<TRecCTETarget> _targets;
    std::vector<TRecCTEResetInfo> _fragments_to_reset;

    int _max_recursion_depth = 0;

    bool _aready_send_close = false;
};

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris