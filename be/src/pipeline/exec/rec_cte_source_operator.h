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

#include <cstdint>

#include "common/status.h"
#include "operator.h"
#include "pipeline/common/distinct_agg_utils.h"
#include "pipeline/exec/union_sink_operator.h"
#include "util/brpc_client_cache.h"
#include "util/uid_util.h"
#include "vec/common/arena.h"
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
    Status init(RuntimeState* state, LocalStateInfo& info) override {
        _hash_table_compute_timer = ADD_TIMER(Base::custom_profile(), "HashTableComputeTime");
        _hash_table_emplace_timer = ADD_TIMER(Base::custom_profile(), "HashTableEmplaceTime");
        _hash_table_input_counter =
                ADD_COUNTER(Base::custom_profile(), "HashTableInputCount", TUnit::UNIT);
        return Status::OK();
    }

private:
    friend class RecCTESourceOperatorX;
    friend class OperatorX<RecCTESourceLocalState>;

    vectorized::VExprContextSPtrs _child_expr;
    std::unique_ptr<DistinctDataVariants> _agg_data = nullptr;

    RuntimeProfile::Counter* _hash_table_compute_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_emplace_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_input_counter = nullptr;

    vectorized::Arena _arena;
    vectorized::IColumn::Selector _distinct_row;
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

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override {
        auto& local_state = get_local_state(state);
        while (!_ready_to_return) {
            auto last_round_offset = _blocks.size();
            if (!_current_round) {
                for (auto anchor_block : local_state._shared_state->anchor_side) {
                    RETURN_IF_ERROR(_emplace_block(state, std::move(anchor_block)));
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
        auto& local_state = get_local_state(state);

        bool eos = false;
        vectorized::Block* input_block = nullptr;
        while (!eos) {
            if (state->is_cancelled()) {
                return Status::Cancelled("RecCTESourceOperatorX is cancelled");
            }
            RETURN_IF_ERROR(child()->get_block(state, input_block, &eos));
            if (input_block->rows() == 0) {
                continue;
            }
            vectorized::Block block;
            RETURN_IF_ERROR(materialize_block(local_state._child_expr, input_block, &block));
            RETURN_IF_ERROR(_emplace_block(state, std::move(block)));
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

    Status _emplace_block(RuntimeState* state, vectorized::Block&& block) {
        auto& local_state = get_local_state(state);
        if (!_is_union_all) {
            local_state._distinct_row.clear();
            auto num_rows = uint32_t(block.rows());
            vectorized::ColumnRawPtrs raw_columns;
            std::vector<vectorized::ColumnPtr> columns = block.get_columns_and_convert();
            for (auto& col : columns) {
                raw_columns.push_back(col.get());
            }

            std::visit(vectorized::Overload {
                               [&](std::monostate& arg) -> void {
                                   throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                                          "uninited hash table");
                               },
                               [&](auto& agg_method) -> void {
                                   auto& local_state = get_local_state(state);
                                   SCOPED_TIMER(local_state._hash_table_compute_timer);
                                   using HashMethodType = std::decay_t<decltype(agg_method)>;
                                   using AggState = typename HashMethodType::State;

                                   AggState agg_state(raw_columns);
                                   agg_method.init_serialized_keys(raw_columns, num_rows);
                                   local_state._distinct_row.clear();

                                   size_t row = 0;
                                   auto creator = [&](const auto& ctor, auto& key, auto& origin) {
                                       HashMethodType::try_presis_key(key, origin,
                                                                      local_state._arena);
                                       ctor(key);
                                       local_state._distinct_row.push_back(row);
                                   };
                                   auto creator_for_null_key = [&]() {
                                       local_state._distinct_row.push_back(row);
                                   };

                                   SCOPED_TIMER(local_state._hash_table_emplace_timer);
                                   for (; row < num_rows; ++row) {
                                       agg_method.lazy_emplace(agg_state, row, creator,
                                                               creator_for_null_key);
                                   }
                                   COUNTER_UPDATE(local_state._hash_table_input_counter, num_rows);
                               }},
                       local_state._agg_data->method_variant);

            if (local_state._distinct_row.size() == block.rows()) {
                _blocks.emplace_back(std::move(block));
            } else if (!local_state._distinct_row.empty()) {
                auto distinct_block = vectorized::MutableBlock(block.clone_empty());
                RETURN_IF_ERROR(block.append_to_block_by_selector(&distinct_block,
                                                                  local_state._distinct_row));
                _blocks.emplace_back(distinct_block.to_block());
            }
        } else {
            _blocks.emplace_back(std::move(block));
        }
        return Status::OK();
    }

    friend class RecCTESourceLocalState;

    vectorized::VExprContextSPtrs _child_expr;

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