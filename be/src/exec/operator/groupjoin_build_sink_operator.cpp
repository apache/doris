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

#include "exec/operator/groupjoin_build_sink_operator.h"

#include "exec/common/template_helpers.hpp"

namespace doris {

GroupJoinBuildSinkOperatorX::GroupJoinBuildSinkOperatorX(ObjectPool* pool, int operator_id,
                                                         int dest_id, const TPlanNode& tnode,
                                                         const DescriptorTbl& descs)
        : HashJoinBuildSinkOperatorX(pool, operator_id, dest_id, tnode, descs) {}

Status GroupJoinBuildSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(HashJoinBuildSinkOperatorX::init(tnode, state));
    if (tnode.__isset.group_join_node && !tnode.group_join_node.aggregate_functions.empty()) {
        for (const auto& agg_expr : tnode.group_join_node.aggregate_functions) {
            AggFnEvaluator* evaluator = nullptr;
            RETURN_IF_ERROR(AggFnEvaluator::create(state->obj_pool(), agg_expr, TSortInfo(), false,
                                                   false, &evaluator));
            _aggregate_evaluators.push_back(evaluator);
        }
    }
    return Status::OK();
}

Status GroupJoinBuildSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(HashJoinBuildSinkOperatorX::prepare(state));
    if (!_aggregate_evaluators.empty()) {
        // Use first slot from build child as dummy descriptor for intermediate/output.
        // The slot descriptor is stored but not used in core function creation.
        const SlotDescriptor* dummy_slot = nullptr;
        if (!_child->row_desc().tuple_descriptors().empty() &&
            !_child->row_desc().tuple_descriptors()[0]->slots().empty()) {
            dummy_slot = _child->row_desc().tuple_descriptors()[0]->slots()[0];
        }
        for (auto* evaluator : _aggregate_evaluators) {
            RETURN_IF_ERROR(evaluator->prepare(state, _child->row_desc(), dummy_slot, dummy_slot));
            RETURN_IF_ERROR(evaluator->open(state));
        }

        // Compute offsets for aggregate states
        size_t offset = 0;
        for (auto* evaluator : _aggregate_evaluators) {
            _offsets_of_aggregate_states.push_back(offset);
            offset += evaluator->function()->size_of_data();
        }
        _total_size_of_aggregate_states = offset;
    }

    // For GroupJoin, keep all original build columns since the probe side needs them.
    std::fill(_should_keep_column_flags.begin(), _should_keep_column_flags.end(), true);

    return Status::OK();
}

Status GroupJoinBuildSinkOperatorX::setup_local_state(RuntimeState* state,
                                                      LocalSinkStateInfo& info) {
    auto local_state = GroupJoinBuildSinkLocalState::create_unique(this, state);
    RETURN_IF_ERROR(local_state->init(state, info));
    state->emplace_sink_local_state(operator_id(), std::move(local_state));
    return Status::OK();
}

Status GroupJoinBuildSinkOperatorX::_on_build_complete(RuntimeState* state,
                                                       HashJoinBuildSinkLocalState& local_state) {
    return static_cast<GroupJoinBuildSinkLocalState&>(local_state)
            ._compute_group_join_aggregates(state);
}

Status GroupJoinBuildSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(HashJoinBuildSinkLocalState::init(state, info));
    auto& p = _parent->cast<GroupJoinBuildSinkOperatorX>();
    _aggregate_evaluators.resize(p._aggregate_evaluators.size());
    for (size_t i = 0; i < _aggregate_evaluators.size(); i++) {
        _aggregate_evaluators[i] = p._aggregate_evaluators[i]->clone(state, state->obj_pool());
    }
    return Status::OK();
}

Status GroupJoinBuildSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (_agg_arena && !_group_states.empty()) {
        auto& p = _parent->cast<GroupJoinBuildSinkOperatorX>();
        for (size_t i = 0; i < _aggregate_evaluators.size(); i++) {
            for (auto* place : _group_states) {
                _aggregate_evaluators[i]->destroy(place + p._offsets_of_aggregate_states[i]);
            }
        }
        _group_states.clear();
        _agg_arena.reset();
    }
    return HashJoinBuildSinkLocalState::close(state, exec_status);
}

Status GroupJoinBuildSinkLocalState::_compute_group_join_aggregates(RuntimeState* state) {
    auto& p = _parent->cast<GroupJoinBuildSinkOperatorX>();
    if (p._aggregate_evaluators.empty()) {
        return Status::OK();
    }

    auto& build_block = *_shared_state->build_block;
    uint32_t build_rows = static_cast<uint32_t>(build_block.rows());

    // Step 1: Traverse hash table to find unique keys, prune duplicate build rows from hash
    // chains, and build row_to_group mapping.
    // The hash table stores 1-based row indices (0 is end-of-chain marker).
    // Row 0 in the block is the mocked row and is not in the hash table.
    std::vector<uint32_t> row_to_group(build_rows, 0);
    std::vector<uint32_t> group_representatives;

    std::visit(
            Overload {[&](std::monostate&) {},
                      [&](auto&& hash_table_ctx) {
                          auto* hash_table = hash_table_ctx.hash_table.get();
                          uint32_t bucket_size = hash_table->get_bucket_size();
                          auto& first = hash_table->get_first();
                          auto& next = hash_table->get_next();
                          const auto* build_keys = hash_table->get_build_keys();
                          using KeyType =
                                  std::remove_const_t<std::remove_pointer_t<decltype(build_keys)>>;

                          for (uint32_t bucket = 0; bucket <= bucket_size; ++bucket) {
                              uint32_t row = first[bucket];
                              uint32_t previous_row = 0;
                              if (row == 0) {
                                  continue;
                              }

                              std::vector<std::pair<KeyType, uint32_t>> bucket_keys;
                              while (row != 0) {
                                  uint32_t next_row = next[row];
                                  const auto& key = build_keys[row];
                                  uint32_t group_id = 0;
                                  for (const auto& [k, gid] : bucket_keys) {
                                      if (k == key) {
                                          group_id = gid;
                                          break;
                                      }
                                  }
                                  if (group_id == 0) {
                                      group_id = static_cast<uint32_t>(
                                              group_representatives.size() + 1);
                                      bucket_keys.emplace_back(key, group_id);
                                      group_representatives.push_back(row);
                                      previous_row = row;
                                  } else if (previous_row == 0) {
                                      first[bucket] = next_row;
                                  } else {
                                      next[previous_row] = next_row;
                                  }
                                  row_to_group[row] = group_id;
                                  row = next_row;
                              }
                          }
                      }},
            _shared_state->hash_table_variant_vector.front()->method_variant);

    int num_groups = static_cast<int>(group_representatives.size());
    if (num_groups == 0) {
        return Status::OK();
    }

    // Step 2: Create aggregate states for each group.
    _agg_arena = std::make_unique<Arena>();
    _group_states.resize(num_groups);
    size_t total_size = p._total_size_of_aggregate_states;
    for (int i = 0; i < num_groups; i++) {
        _group_states[i] = _agg_arena->alloc(total_size);
        for (size_t j = 0; j < p._aggregate_evaluators.size(); j++) {
            p._aggregate_evaluators[j]->create(_group_states[i] +
                                               p._offsets_of_aggregate_states[j]);
        }
    }

    // Step 3: Build places array mapping each row to its group's state.
    std::vector<AggregateDataPtr> places(build_rows);
    AggregateDataPtr dummy_state = _agg_arena->alloc(total_size);
    for (size_t j = 0; j < p._aggregate_evaluators.size(); j++) {
        p._aggregate_evaluators[j]->create(dummy_state + p._offsets_of_aggregate_states[j]);
    }
    places[0] = dummy_state;
    for (uint32_t row = 1; row < build_rows; row++) {
        uint32_t group_id = row_to_group[row];
        places[row] = (group_id > 0) ? _group_states[group_id - 1] : dummy_state;
    }

    // Step 4: Execute batch add for each aggregate evaluator.
    for (size_t i = 0; i < _aggregate_evaluators.size(); i++) {
        RETURN_IF_ERROR(_aggregate_evaluators[i]->execute_batch_add(&build_block, 0, places.data(),
                                                                    *_agg_arena, true));
    }

    // Reset dummy state so unmatched outer-join rows get a clean aggregate result.
    for (size_t j = 0; j < p._aggregate_evaluators.size(); j++) {
        p._aggregate_evaluators[j]->reset(dummy_state + p._offsets_of_aggregate_states[j]);
    }

    // Step 5: Finalize aggregate states to columns and append to build block.
    for (size_t i = 0; i < p._aggregate_evaluators.size(); i++) {
        auto result_type = p._aggregate_evaluators[i]->function()->get_return_type();
        MutableColumnPtr agg_column = result_type->create_column();
        _aggregate_evaluators[i]->insert_result_info_vec(places, p._offsets_of_aggregate_states[i],
                                                         agg_column.get(), build_rows);
        build_block.insert(
                {std::move(agg_column), result_type, p._aggregate_evaluators[i]->debug_string()});
    }

    return Status::OK();
}

} // namespace doris
