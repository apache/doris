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
#include "util/brpc_client_cache.h"
#include "util/uid_util.h"

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
    RecCTESourceLocalState(RuntimeState* state, OperatorXBase* parent) : Base(state, parent) {};

private:
    friend class RecCTESourceOperatorX;
    friend class OperatorX<RecCTESourceLocalState>;
};

class RecCTESourceOperatorX MOCK_REMOVE(final) : public OperatorX<RecCTESourceLocalState> {
public:
    using Base = OperatorX<RecCTESourceLocalState>;
    RecCTESourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                          const DescriptorTbl& descs)
            : Base(pool, tnode, operator_id, descs) {
        DCHECK(tnode.__isset.rec_cte_node);
        _is_union_all = tnode.rec_cte_node.is_union_all;
        _targets = tnode.rec_cte_node.targets;
        _fragments_to_reset = tnode.rec_cte_node.fragments_to_reset;
    }
    ~RecCTESourceOperatorX() override = default;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override {
        auto& local_state = get_local_state(state);
        while (!_ready_to_return) {
            auto last_round_offset = _blocks.size();
            if (!_current_round) {
                for (auto anchor_block : local_state._shared_state->anchor_side) {
                    _blocks.emplace_back(std::move(anchor_block));
                }
            } else {
                RETURN_IF_ERROR(collect_child_block(state));
            }

            if (last_round_offset == _blocks.size() || _current_round >= max_recursion_depth) {
                _ready_to_return = true;
            } else {
                RETURN_IF_ERROR(_recursive_process(state, last_round_offset));
            }

            _current_round++;
        }

        if (_blocks.empty()) {
            *eos = true;
        } else {
            *eos = false;
            block->swap(_blocks.back());
            _blocks.pop_back();
        }

        return Status::OK();
    }

    bool is_source() const override { return true; }

private:
    Status collect_child_block(RuntimeState* state) {
        bool eos = false;
        vectorized::Block* block = nullptr;
        while (!eos) {
            if (state->is_cancelled()) {
                return Status::Cancelled("RecCTESourceOperatorX is cancelled");
            }
            RETURN_IF_ERROR(child()->get_block(state, block, &eos));
            if (block->rows() > 0) {
                _blocks.emplace_back(std::move(*block));
            }
        }
        return Status::OK();
    }

    Status _recursive_process(RuntimeState* state, size_t last_round_offset) {
        for (auto fragment : _fragments_to_reset) {
            auto stub =
                    state->get_query_ctx()->exec_env()->brpc_internal_client_cache()->get_client(
                            fragment.addr);

            PResetFragmentParams request;
            request.mutable_query_id()->CopyFrom(UniqueId(state->query_id()).to_proto());
            request.set_fragment_id(fragment.fragment_id);

            PResetFragmentResult result;
            brpc::Controller controller;
            stub->reset_fragment(&controller, &request, &result, brpc::DoNothing());
            brpc::Join(controller.call_id());

            RETURN_IF_ERROR(Status::create(result.status()));
        }

        RETURN_IF_ERROR(_send_data_to_targets(state, last_round_offset));
        return Status::OK();
    }

    Status _send_data_to_targets(RuntimeState* state, size_t last_round_offset) {
        auto stub = state->get_query_ctx()->exec_env()->brpc_internal_client_cache()->get_client(
                _targets[0].addr);
        if (!stub) {
            return Status::InternalError(fmt::format("Get rpc stub failed, host={}, port={}",
                                                     _targets[0].addr.hostname,
                                                     _targets[0].addr.port));
        }
        PTransmitRecCTEBlockParams request;
        for (size_t i = last_round_offset; i < _blocks.size(); i++) {
            auto* pblock = request.add_blocks();
            size_t uncompressed_bytes = 0;
            size_t compressed_bytes = 0;
            RETURN_IF_ERROR(_blocks[i].serialize(state->be_exec_version(), pblock,
                                                 &uncompressed_bytes, &compressed_bytes,
                                                 state->fragement_transmission_compression_type()));
        }
        request.set_eos(true);
        request.set_node_id(_targets[0].node_id);
        request.mutable_query_id()->CopyFrom(UniqueId(state->query_id()).to_proto());
        request.mutable_fragment_instance_id()->CopyFrom(
                UniqueId(state->fragment_instance_id()).to_proto());
        request.set_node_id(_targets[0].node_id);

        PTransmitRecCTEBlockResult result;
        brpc::Controller controller;

        stub->transmit_rec_cte_block(&controller, &request, &result, brpc::DoNothing());
        brpc::Join(controller.call_id());
        return Status::create(result.status());
    }

    friend class RecCTESourceLocalState;

    bool _is_union_all = false;
    std::vector<TRecCTETarget> _targets;
    std::vector<TRecCTEResetInfo> _fragments_to_reset;

    std::vector<vectorized::Block> _blocks;
    int _current_round = 0;
    bool _ready_to_return = false;

    const int max_recursion_depth = 10;
};

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris