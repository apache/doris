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
            : Base(pool, tnode, operator_id, descs),
              _is_union_all(tnode.rec_cte_node.is_union_all),
              _targets(tnode.rec_cte_node.targets),
              _fragments_to_reset(tnode.rec_cte_node.fragments_to_reset) {
        DCHECK(tnode.__isset.rec_cte_node);
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
                RETURN_IF_ERROR(_collect_child_block(state));
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
    Status _collect_child_block(RuntimeState* state) {
        bool eos = false;
        vectorized::Block* block = nullptr;
        while (!eos) {
            if (state->is_cancelled()) {
                return Status::Cancelled("RecCTESourceOperatorX is cancelled");
            }
            RETURN_IF_ERROR(child()->get_block(state, block, &eos));
            if (block->rows() > 0) {
                if (!_is_union_all) {
                    // do something
                }
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

    PTransmitRecCTEBlockParams _build_basic_param(RuntimeState* state,
                                                  const TRecCTETarget& target) {
        PTransmitRecCTEBlockParams request;
        request.set_node_id(target.node_id);
        request.mutable_query_id()->CopyFrom(UniqueId(state->query_id()).to_proto());
        request.mutable_fragment_instance_id()->CopyFrom(
                UniqueId(state->fragment_instance_id()).to_proto());
        return request;
    }

    Status _send_data_to_targets(RuntimeState* state, size_t last_round_offset) {
        int send_multi_blocks_byte_size = state->query_options().exchange_multi_blocks_byte_size;
        int block_number_per_target =
                int(_blocks.size() - last_round_offset + _targets.size() - 1) / _targets.size();
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
            while (last_round_offset < _blocks.size() && step > 0) {
                PTransmitRecCTEBlockParams request = _build_basic_param(state, target);
                auto current_bytes = 0;
                while (last_round_offset < _blocks.size() && step > 0 &&
                       current_bytes < send_multi_blocks_byte_size) {
                    auto* pblock = request.add_blocks();
                    size_t uncompressed_bytes = 0;
                    size_t compressed_bytes = 0;
                    RETURN_IF_ERROR(_blocks[last_round_offset].serialize(
                            state->be_exec_version(), pblock, &uncompressed_bytes,
                            &compressed_bytes, state->fragement_transmission_compression_type()));
                    last_round_offset++;
                    step--;
                    current_bytes += compressed_bytes;
                }
                request.set_eos(false);

                PTransmitRecCTEBlockResult result;
                brpc::Controller controller;
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

    const bool _is_union_all = false;
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