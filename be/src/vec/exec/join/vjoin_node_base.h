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

#include <future>
#include <string>
#include <variant>

#include "exec/exec_node.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"

namespace doris::vectorized {

using JoinOpVariants =
        std::variant<std::integral_constant<TJoinOp::type, TJoinOp::INNER_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::LEFT_SEMI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::LEFT_ANTI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::LEFT_OUTER_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::FULL_OUTER_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::RIGHT_OUTER_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::CROSS_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::RIGHT_SEMI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::RIGHT_ANTI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN>>;

class VJoinNodeBase : public ExecNode {
public:
    VJoinNodeBase(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    virtual Status close(RuntimeState* state) override;

    virtual Status open(RuntimeState* state) override;

    virtual const RowDescriptor& row_desc() const override { return *_output_row_desc; }

    virtual const RowDescriptor& intermediate_row_desc() const override {
        return *_intermediate_row_desc;
    }

    virtual Status alloc_resource(RuntimeState* state) override;
    virtual void release_resource(RuntimeState* state) override;

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;

protected:
    // Construct the intermediate blocks to store the results from join operation.
    void _construct_mutable_join_block();
    // Convert the intermediate blocks to the final result. For example, if the block from probe
    // side is non-nullable and the join op is righter outer join, we need to convert the non-nullable
    // columns from probe side to a nullable column.
    Status _build_output_block(Block* origin_block, Block* output_block);
    // Open probe side asynchronously.
    void _probe_side_open_thread(RuntimeState* state, std::promise<Status>* status);

    // Initialize the join operation.
    void _init_join_op();

    virtual void _add_tuple_is_null_column(Block* block) = 0;

    // reset the tuple is null flag column for the next call
    void _reset_tuple_is_null_column();

    // Materialize build relation. For HashJoin, it will build a hash table while a list of build blocks for NLJoin.
    virtual Status _materialize_build_side(RuntimeState* state) = 0;

    TJoinOp::type _join_op;
    JoinOpVariants _join_op_variants;

    bool _have_other_join_conjunct;
    const bool _match_all_probe; // output all rows coming from the probe input. Full/Left Join
    const bool _match_all_build; // output all rows coming from the build input. Full/Right Join
    bool _build_unique;          // build a hash table without duplicated rows. Left semi/anti Join

    const bool _is_right_semi_anti;
    const bool _is_left_semi_anti;
    const bool _is_outer_join;

    // For null aware left anti join, we apply a short circuit strategy.
    // 1. Set _short_circuit_for_null_in_build_side to true if join operator is null aware left anti join.
    // 2. In build phase, we stop materialize build side when we meet the first null value and set _short_circuit_for_null_in_probe_side to true.
    // 3. In probe phase, if _short_circuit_for_null_in_probe_side is true, join node returns empty block directly. Otherwise, probing will continue as the same as generic left anti join.
    const bool _short_circuit_for_null_in_build_side = false;
    bool _short_circuit_for_null_in_probe_side = false;

    std::unique_ptr<RowDescriptor> _output_row_desc;
    std::unique_ptr<RowDescriptor> _intermediate_row_desc;
    // output expr
    std::vector<VExprContext*> _output_expr_ctxs;

    Block _join_block;

    MutableColumnPtr _tuple_is_null_left_flag_column;
    MutableColumnPtr _tuple_is_null_right_flag_column;

    RuntimeProfile::Counter* _build_timer;
    RuntimeProfile::Counter* _probe_timer;
    RuntimeProfile::Counter* _build_rows_counter;
    RuntimeProfile::Counter* _probe_rows_counter;
    RuntimeProfile::Counter* _push_down_timer;
    RuntimeProfile::Counter* _push_compute_timer;
    RuntimeProfile::Counter* _join_filter_timer;
};

} // namespace doris::vectorized
