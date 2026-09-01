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

#include "exec/pipeline/rec_cte_shared_state.h"

#include <gen_cpp/internal_service.pb.h>

#include "exec/common/distinct_agg_utils.h"
#include "exec/common/template_helpers.hpp"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "util/brpc_client_cache.h"
#include "util/uid_util.h"

namespace doris {

RecCTESharedState::RecCTESharedState() = default;

RecCTESharedState::~RecCTESharedState() = default;

Status RecCTESharedState::emplace_block(RuntimeState* state, Block&& block) {
    if (agg_data) {
        auto num_rows = uint32_t(block.rows());
        ColumnRawPtrs raw_columns;
        std::vector<ColumnPtr> columns = block.get_columns_and_convert();
        for (auto& col : columns) {
            raw_columns.push_back(col.get());
        }

        std::visit(Overload {[&](std::monostate& arg) -> void {
                                 throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                                        "uninited hash table");
                             },
                             [&](auto& agg_method) -> void {
                                 SCOPED_TIMER(hash_table_compute_timer);
                                 using HashMethodType = std::decay_t<decltype(agg_method)>;
                                 using AggState = typename HashMethodType::State;

                                 AggState agg_state(raw_columns);
                                 agg_method.init_serialized_keys(raw_columns, num_rows);
                                 distinct_row.clear();

                                 size_t row = 0;
                                 auto creator = [&](const auto& ctor, auto& key, auto& origin) {
                                     HashMethodType::try_presis_key(key, origin, arena);
                                     ctor(key);
                                     distinct_row.push_back(row);
                                 };
                                 auto creator_for_null_key = [&]() { distinct_row.push_back(row); };

                                 SCOPED_TIMER(hash_table_emplace_timer);
                                 lazy_emplace_batch_void(agg_method, agg_state, num_rows, creator,
                                                         creator_for_null_key,
                                                         [&](uint32_t r) { row = r; });
                                 COUNTER_UPDATE(hash_table_input_counter, num_rows);
                             }},
                   agg_data->method_variant);

        if (distinct_row.size() == block.rows()) {
            blocks.emplace_back(std::move(block));
        } else if (!distinct_row.empty()) {
            auto distinct_block = MutableBlock(block.clone_empty());
            RETURN_IF_ERROR(block.append_to_block_by_selector(&distinct_block, distinct_row));
            blocks.emplace_back(distinct_block.to_block());
        }
    } else {
        blocks.emplace_back(std::move(block));
    }
    return Status::OK();
}

PTransmitRecCTEBlockParams RecCTESharedState::build_basic_param(RuntimeState* state,
                                                                const TRecCTETarget& target) const {
    PTransmitRecCTEBlockParams request;
    request.set_node_id(target.node_id);
    request.mutable_query_id()->CopyFrom(UniqueId(state->query_id()).to_proto());
    request.mutable_fragment_instance_id()->CopyFrom(
            UniqueId(target.fragment_instance_id).to_proto());
    return request;
}

Status RecCTESharedState::send_data_to_targets(RuntimeState* state, size_t round_offset) const {
    if (targets.size() == 0) {
        return Status::OK();
    }
    int send_multi_blocks_byte_size = state->query_options().exchange_multi_blocks_byte_size;
    int block_number_per_target =
            int(blocks.size() - round_offset + targets.size() - 1) / targets.size();
    for (auto target : targets) {
        auto stub = state->get_query_ctx()->exec_env()->brpc_internal_client_cache()->get_client(
                target.addr);
        if (!stub) {
            return Status::InternalError(fmt::format("Get rpc stub failed, host={}, port={}",
                                                     target.addr.hostname, target.addr.port));
        }

        // send blocks
        int step = block_number_per_target;
        while (round_offset < blocks.size() && step > 0) {
            PTransmitRecCTEBlockParams request = build_basic_param(state, target);
            auto current_bytes = 0;
            while (round_offset < blocks.size() && step > 0 &&
                   current_bytes < send_multi_blocks_byte_size) {
                auto* pblock = request.add_blocks();
                size_t uncompressed_bytes = 0;
                size_t compressed_bytes = 0;
                int64_t compress_time;
                RETURN_IF_ERROR(blocks[round_offset].serialize(
                        state->be_exec_version(), pblock, &uncompressed_bytes, &compressed_bytes,
                        &compress_time, state->fragement_transmission_compression_type()));
                round_offset++;
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
            PTransmitRecCTEBlockParams request = build_basic_param(state, target);
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

} // namespace doris
