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

#include <stdint.h>

#include "common/status.h"
#include "pipeline/exec/operator.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

namespace pipeline {

class MaterializationOperator;

struct FetchRpcStruct {
    std::shared_ptr<PBackendService_Stub> stub;
    std::unique_ptr<brpc::Controller> cntl;
    PMultiGetRequestV2 request;
    PMultiGetResponseV2 response;
};

struct MaterializationSharedState {
public:
    MaterializationSharedState() = default;

    Status init_multi_requests(const TMaterializationNode& tnode, RuntimeState* state);
    Status create_muiltget_result(const vectorized::Columns& columns, bool eos, bool gc_id_map);

    Status merge_multi_response();
    void get_block(vectorized::Block* block);

private:
    void _update_profile_info(int64_t backend_id, RuntimeProfile* response_profile);

public:
    bool rpc_struct_inited = false;

    bool eos = false;
    // empty materialization sink block not need to merge block
    bool need_merge_block = true;
    vectorized::Block origin_block;
    // The rowid column of the origin block. should be replaced by the column of the result block.
    std::vector<int> rowid_locs;
    std::vector<vectorized::MutableBlock> response_blocks;
    std::map<int64_t, FetchRpcStruct> rpc_struct_map;
    // Register each line in which block to ensure the order of the result.
    // Zero means NULL value.
    std::vector<std::vector<int64_t>> block_order_results;
    // backend id => <rpc profile info string key, rpc profile info string value>.
    std::map<int64_t, std::map<std::string, fmt::memory_buffer>> backend_profile_info_string;
};

class MaterializationLocalState final : public PipelineXLocalState<FakeSharedState> {
public:
    using Parent = MaterializationOperator;
    using Base = PipelineXLocalState<FakeSharedState>;

    ENABLE_FACTORY_CREATOR(MaterializationLocalState);
    MaterializationLocalState(RuntimeState* state, OperatorXBase* parent) : Base(state, parent) {};

    Status init(RuntimeState* state, LocalStateInfo& info) override {
        RETURN_IF_ERROR(Base::init(state, info));
        _max_rpc_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "MaxRpcTime", 2);
        _merge_response_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "MergeResponseTime", 2);
        return Status::OK();
    }

private:
    friend class MaterializationOperator;
    template <typename LocalStateType>
    friend class StatefulOperatorX;

    std::unique_ptr<vectorized::Block> _child_block = vectorized::Block::create_unique();
    bool _child_eos = false;
    MaterializationSharedState _materialization_state;
    RuntimeProfile::Counter* _max_rpc_timer = nullptr;
    RuntimeProfile::Counter* _merge_response_timer = nullptr;
};

class MaterializationOperator final : public StatefulOperatorX<MaterializationLocalState> {
public:
    using Base = StatefulOperatorX<MaterializationLocalState>;
    MaterializationOperator(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                            const DescriptorTbl& descs)
            : Base(pool, tnode, operator_id, descs) {}

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    bool is_blockable(RuntimeState* state) const override { return true; }
    bool need_more_input_data(RuntimeState* state) const override;
    Status pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) const override;
    Status push(RuntimeState* state, vectorized::Block* input_block, bool eos) const override;

private:
    friend class MaterializationLocalState;

    // Materialized slot by this node. The i-th result expr list refers to a slot of RowId
    TMaterializationNode _materialization_node;
    vectorized::VExprContextSPtrs _rowid_exprs;
    bool _gc_id_map = false;
};

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris
