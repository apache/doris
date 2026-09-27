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

#include "exec/operator/groupjoin_operator_utils.h"

#include <gen_cpp/Opcodes_types.h>

#include <algorithm>
#include <new>
#include <variant>

#include "common/cast_set.h"
#include "common/check.h"
#include "common/exception.h"
#include "core/arena.h"
#include "core/column/column_nullable.h"
#include "exec/common/agg_utils.h"
#include "exec/common/hash_table/hash_map_context.h"
#include "exec/common/hash_table/hash_map_util.h"
#include "exec/common/template_helpers.hpp"
#include "exec/common/util.hpp"
#include "exec/operator/groupjoin_probe_operator.h"
#include "exec/pipeline/dependency.h"
#include "exprs/vectorized_agg_fn.h"
#include "exprs/vexpr.h"

namespace doris {

GroupJoinSharedState::GroupJoinSharedState()
        : data_variants(std::make_unique<GroupJoinDataVariants>()),
          arena(std::make_shared<Arena>()) {}

void GroupJoinSharedState::update_memory_usage() {
    DCHECK(memory_used_counter != nullptr);
    const auto hash_table_bytes =
            std::visit(Overload {[](std::monostate&) -> size_t { return 0; },
                                 [](auto& method) -> size_t {
                                     return method.hash_table->get_buffer_size_in_bytes();
                                 }},
                       data_variants->method_variant);
    const auto data_container_bytes =
            data_container == nullptr ? 0 : data_container->memory_usage();
    // Persisted string keys and aggregate-state-owned allocations belong to the arena. Keys,
    // entries and inline aggregate state storage belong to the data container.
    COUNTER_SET(memory_used_counter, arena->size() + hash_table_bytes + data_container_bytes);
}

GroupJoinSharedState::~GroupJoinSharedState() {
    groupjoin::destroy_agg_states(this);
}

} // namespace doris

namespace doris::groupjoin {

namespace {

inline void prefetch_group_join_entry(const AggregateDataPtr* places, uint32_t row,
                                      uint32_t num_rows) {
    const auto future_row = row + HASH_MAP_PREFETCH_DIST;
    if (future_row >= num_rows) {
        return;
    }
    auto* future_entry = reinterpret_cast<GroupJoinEntry*>(places[future_row]);
    if (future_entry != nullptr) {
        __builtin_prefetch(future_entry, 1, 1);
    }
}

AggregateDataPtr get_entry_agg_states(GroupJoinEntry* entry,
                                      const GroupJoinSharedState* shared_state) {
    DCHECK(entry != nullptr);
    DCHECK(shared_state->agg_layout_ready);
    DCHECK(!shared_state->aggregate_evaluators.empty());
    return reinterpret_cast<AggregateDataPtr>(entry) + sizeof(GroupJoinEntry);
}

} // namespace

Status validate_group_join_node(const TPlanNode& tnode) {
    if (!tnode.__isset.group_join_node) {
        return Status::InternalError("GroupJoin node is not set");
    }

    const auto& group_join_node = tnode.group_join_node;
    if (group_join_node.join_op != TJoinOp::INNER_JOIN) {
        return Status::InternalError("GroupJoin only supports inner join now");
    }
    if (group_join_node.__isset.dist_type &&
        group_join_node.dist_type == TJoinDistributionType::BROADCAST) {
        return Status::InternalError("GroupJoin does not support broadcast join now");
    }
    if (group_join_node.agg_output_mode != TGroupJoinAggOutputMode::FINAL_RESULT) {
        return Status::InternalError(
                "GroupJoin only supports final-result aggregate output mode now: {}",
                group_join_node.agg_output_mode);
    }
    if (group_join_node.aggregate_functions.empty()) {
        return Status::InternalError("GroupJoin requires at least one aggregate function");
    }
    for (const auto& eq_join_conjunct : group_join_node.eq_join_conjuncts) {
        if (eq_join_conjunct.__isset.opcode &&
            eq_join_conjunct.opcode == TExprOpcode::EQ_FOR_NULL) {
            return Status::InternalError("GroupJoin does not support null-safe equal join now");
        }
    }
    return Status::OK();
}

Status do_evaluate(const Block& block, VExprContextSPtrs& exprs,
                   std::vector<ColumnPtr>& key_columns_holder) {
    key_columns_holder.resize(exprs.size());
    for (size_t i = 0; i < exprs.size(); ++i) {
        RETURN_IF_ERROR(exprs[i]->execute(&block, key_columns_holder[i]));
        key_columns_holder[i] = key_columns_holder[i]->convert_to_full_column_if_const();
        auto mutable_col = IColumn::mutate(std::move(key_columns_holder[i]));
        mutable_col->replace_float_special_values();
        key_columns_holder[i] = std::move(mutable_col);
    }
    return Status::OK();
}

Status extract_key_columns(size_t rows, const std::vector<ColumnPtr>& key_columns_holder,
                           ColumnRawPtrs& key_not_nullable_columns,
                           ColumnUInt8::MutablePtr& null_map_column) {
    key_not_nullable_columns.resize(key_columns_holder.size());
    null_map_column.reset();

    for (size_t i = 0; i < key_columns_holder.size(); ++i) {
        const auto* column = key_columns_holder[i].get();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(*column); nullable) {
            const auto& col_nested = nullable->get_nested_column();
            const auto& col_nullmap = nullable->get_null_map_data();
            if (!null_map_column) {
                null_map_column = ColumnUInt8::create();
                null_map_column->get_data().assign(rows, uint8_t {0});
            }
            VectorizedUtils::update_null_map(null_map_column->get_data(), col_nullmap);
            key_not_nullable_columns[i] = &col_nested;
        } else {
            key_not_nullable_columns[i] = column;
        }
    }
    return Status::OK();
}

Status register_agg_state_layout(GroupJoinSharedState* shared_state,
                                 const std::vector<TGroupJoinAggSide::type>& aggregate_sides,
                                 const Sizes& sizes_of_aggregate_states,
                                 const Sizes& aligns_of_aggregate_states,
                                 const std::vector<int>& aggregate_indices,
                                 const std::vector<AggFnEvaluator*>& aggregate_evaluators) {
    if (shared_state->aggregate_sides.empty()) {
        shared_state->aggregate_sides = aggregate_sides;
        shared_state->has_build_side_aggregate =
                std::find(aggregate_sides.begin(), aggregate_sides.end(),
                          TGroupJoinAggSide::BUILD) != aggregate_sides.end();
        shared_state->sizes_of_aggregate_states.assign(aggregate_sides.size(), 0);
        shared_state->aligns_of_aggregate_states.assign(aggregate_sides.size(), 1);
        shared_state->offsets_of_aggregate_states.assign(aggregate_sides.size(), 0);
        shared_state->aggregate_evaluators.assign(aggregate_sides.size(), nullptr);
    } else if (shared_state->aggregate_sides != aggregate_sides) {
        return Status::InternalError("GroupJoin aggregate sides are inconsistent");
    }

    if (sizes_of_aggregate_states.size() != shared_state->aggregate_sides.size() ||
        aligns_of_aggregate_states.size() != shared_state->aggregate_sides.size()) {
        return Status::InternalError("GroupJoin aggregate state layout size is inconsistent");
    }
    if (aggregate_indices.size() != aggregate_evaluators.size()) {
        return Status::InternalError("GroupJoin aggregate evaluator index size is inconsistent");
    }

    for (size_t i = 0; i < sizes_of_aggregate_states.size(); ++i) {
        if (sizes_of_aggregate_states[i] == 0) {
            continue;
        }
        if (shared_state->sizes_of_aggregate_states[i] != 0 &&
            shared_state->sizes_of_aggregate_states[i] != sizes_of_aggregate_states[i]) {
            return Status::InternalError("GroupJoin aggregate state size is inconsistent");
        }
        if (shared_state->aligns_of_aggregate_states[i] != 1 &&
            shared_state->aligns_of_aggregate_states[i] != aligns_of_aggregate_states[i]) {
            return Status::InternalError("GroupJoin aggregate state align is inconsistent");
        }
        shared_state->sizes_of_aggregate_states[i] = sizes_of_aggregate_states[i];
        shared_state->aligns_of_aggregate_states[i] = aligns_of_aggregate_states[i];
    }
    for (size_t i = 0; i < aggregate_indices.size(); ++i) {
        const auto agg_idx = aggregate_indices[i];
        DCHECK_GE(agg_idx, 0);
        DCHECK_LT(agg_idx, shared_state->aggregate_evaluators.size());
        if (shared_state->aggregate_evaluators[agg_idx] != nullptr &&
            shared_state->aggregate_evaluators[agg_idx] != aggregate_evaluators[i]) {
            return Status::InternalError("GroupJoin aggregate evaluator is inconsistent");
        }
        shared_state->aggregate_evaluators[agg_idx] = aggregate_evaluators[i];
    }

    bool ready = true;
    for (size_t size : shared_state->sizes_of_aggregate_states) {
        ready &= size != 0;
    }
    for (auto* evaluator : shared_state->aggregate_evaluators) {
        ready &= evaluator != nullptr;
    }
    if (!ready || shared_state->agg_layout_ready) {
        return Status::OK();
    }

    shared_state->total_size_of_aggregate_states = 0;
    shared_state->align_aggregate_states = 1;
    for (size_t i = 0; i < shared_state->sizes_of_aggregate_states.size(); ++i) {
        shared_state->offsets_of_aggregate_states[i] = shared_state->total_size_of_aggregate_states;
        const auto align = shared_state->aligns_of_aggregate_states[i];
        if ((align & (align - 1)) != 0) {
            return Status::RuntimeError("Logical error: GroupJoin align_of_data is not 2^N");
        }
        shared_state->align_aggregate_states =
                std::max(shared_state->align_aggregate_states, align);
        shared_state->total_size_of_aggregate_states += shared_state->sizes_of_aggregate_states[i];
        if (i + 1 < shared_state->sizes_of_aggregate_states.size()) {
            const auto next_align = shared_state->aligns_of_aggregate_states[i + 1];
            shared_state->total_size_of_aggregate_states =
                    (shared_state->total_size_of_aggregate_states + next_align - 1) / next_align *
                    next_align;
        }
    }
    shared_state->agg_layout_ready = true;
    return Status::OK();
}

void init_data_container(GroupJoinSharedState* shared_state) {
    DORIS_CHECK(shared_state->agg_layout_ready);
    DORIS_CHECK(shared_state->data_container == nullptr);

    // AggregateDataContainer uses a fixed stride. Round the trailing aggregate-state block
    // exactly as regular aggregation does so states in every slot retain their alignment.
    const auto aligned_states_size = (shared_state->total_size_of_aggregate_states +
                                      shared_state->align_aggregate_states - 1) /
                                     shared_state->align_aggregate_states *
                                     shared_state->align_aggregate_states;
    const auto size_of_entry = sizeof(GroupJoinEntry) + aligned_states_size;

    std::visit(
            Overload {[&](std::monostate&) {
                          throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                      },
                      [&](auto& hash_method) {
                          using HashMethodType = std::decay_t<decltype(hash_method)>;
                          using KeyType = typename HashMethodType::Key;
                          shared_state->data_container = std::make_unique<AggregateDataContainer>(
                                  sizeof(KeyType), size_of_entry);
                      }},
            shared_state->data_variants->method_variant);
}

void create_all_agg_states(GroupJoinSharedState* shared_state, AggregateDataPtr data) {
    for (size_t i = 0; i < shared_state->aggregate_evaluators.size(); ++i) {
        try {
            shared_state->aggregate_evaluators[i]->create(
                    data + shared_state->offsets_of_aggregate_states[i]);
        } catch (...) {
            for (size_t j = 0; j < i; ++j) {
                shared_state->aggregate_evaluators[j]->destroy(
                        data + shared_state->offsets_of_aggregate_states[j]);
            }
            throw;
        }
    }
}

void destroy_all_agg_states(GroupJoinSharedState* shared_state, AggregateDataPtr data) {
    if (data == nullptr) {
        return;
    }
    for (size_t i = 0; i < shared_state->aggregate_evaluators.size(); ++i) {
        shared_state->aggregate_evaluators[i]->destroy(
                data + shared_state->offsets_of_aggregate_states[i]);
    }
}

void create_entry_agg_states(GroupJoinEntry* entry, GroupJoinSharedState* shared_state) {
    create_all_agg_states(shared_state, get_entry_agg_states(entry, shared_state));
}

Status add_build_counts_by_key(GroupJoinSharedState* shared_state, Arena& arena,
                               ColumnRawPtrs& key_not_nullable_columns, uint32_t num_rows,
                               const uint8_t* null_map, const std::vector<int>& aggregate_indices,
                               AggregateDataPtr* places) {
    if (null_map != nullptr) {
        std::fill(places, places + num_rows, nullptr);
    }
    return std::visit(
            Overload {[&](std::monostate&) -> Status {
                          throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                      },
                      [&](auto& hash_method) -> Status {
                          using HashMethodType = std::decay_t<decltype(hash_method)>;
                          using State = typename HashMethodType::State;
                          State state(key_not_nullable_columns);
                          hash_method.init_serialized_keys(key_not_nullable_columns, num_rows,
                                                           null_map);

                          auto creator = [&](const auto& ctor, auto& key, auto& origin) {
                              HashMethodType::try_presis_key_and_origin(key, origin, arena);
                              auto* mapped = new (shared_state->data_container->append_data(origin))
                                      GroupJoinEntry();
                              ctor(key, mapped);
                          };
                          auto creator_for_null_key = [](auto&) {
                              throw doris::Exception(
                                      ErrorCode::INTERNAL_ERROR,
                                      "GroupJoin key columns should not contain nullable columns");
                          };

                          auto result_handler = [&](uint32_t row, auto& mapped) {
                              places[row] = reinterpret_cast<AggregateDataPtr>(mapped);
                          };
                          if (null_map == nullptr) {
                              lazy_emplace_batch(hash_method, state, num_rows, creator,
                                                 creator_for_null_key, result_handler);
                          } else {
                              for (uint32_t row = 0; row < num_rows; ++row) {
                                  // For normal inner equal join, any row with a NULL join key can
                                  // never match the probe side. Skip it before lazy_emplace so the
                                  // build hash map does not contain NULL-key entries.
                                  if (null_map[row]) {
                                      continue;
                                  }
                                  auto& mapped = *hash_method.lazy_emplace(state, row, creator,
                                                                           creator_for_null_key);
                                  result_handler(row, mapped);
                              }
                          }
                          for (uint32_t row = 0; row < num_rows; ++row) {
                              auto* mapped = reinterpret_cast<GroupJoinEntry*>(places[row]);
                              if (mapped == nullptr) {
                                  continue;
                              }
                              prefetch_group_join_entry(places, row, num_rows);
                              // Only create aggregate states when the build side has aggregate
                              // functions to update. The count advances only after state creation
                              // succeeds, so zero also identifies an uninitialized state during
                              // exception cleanup.
                              if (!aggregate_indices.empty()) {
                                  if (mapped->build_count == 0) {
                                      create_entry_agg_states(mapped, shared_state);
                                  }
                                  places[row] = get_entry_agg_states(mapped, shared_state);
                              } else {
                                  places[row] = nullptr;
                              }
                              ++mapped->build_count;
                          }
                          return Status::OK();
                      }},
            shared_state->data_variants->method_variant);
}

void build_repeat_vectors(const std::vector<GroupJoinEntry*>& entries, uint32_t num_rows,
                          bool needs_build_counts, bool needs_probe_counts,
                          std::vector<uint64_t>& build_counts,
                          std::vector<uint64_t>& probe_counts) {
    DCHECK_GE(entries.size(), num_rows);
    if (needs_build_counts) {
        build_counts.resize(num_rows);
        for (uint32_t row = 0; row < num_rows; ++row) {
            build_counts[row] = entries[row]->build_count;
        }
    }
    if (needs_probe_counts) {
        probe_counts.resize(num_rows);
        for (uint32_t row = 0; row < num_rows; ++row) {
            probe_counts[row] = entries[row]->probe_count;
        }
    }
}

Status update_probe_counts(GroupJoinSharedState* shared_state,
                           ColumnRawPtrs& key_not_nullable_columns, uint32_t num_rows,
                           const uint8_t* null_map, const std::vector<int>& aggregate_indices,
                           AggregateDataPtr* places, int64_t& matched_rows,
                           uint32_t& matched_probe_rows) {
    std::fill(places, places + num_rows, nullptr);
    matched_rows = 0;
    matched_probe_rows = 0;
    return std::visit(
            Overload {[&](std::monostate&) -> Status {
                          throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                      },
                      [&](auto& hash_method) -> Status {
                          using HashMethodType = std::decay_t<decltype(hash_method)>;
                          using State = typename HashMethodType::State;
                          State state(key_not_nullable_columns);
                          hash_method.init_serialized_keys(key_not_nullable_columns, num_rows,
                                                           null_map);
                          find_batch(hash_method, state, num_rows, [&](uint32_t row, auto& result) {
                              // For normal inner equal join, any row with a NULL join key
                              // can never match the build side.
                              if ((null_map != nullptr && null_map[row]) || !result.is_found()) {
                                  return;
                              }
                              places[row] = reinterpret_cast<AggregateDataPtr>(result.get_mapped());
                          });
                          for (uint32_t row = 0; row < num_rows; ++row) {
                              auto* mapped = reinterpret_cast<GroupJoinEntry*>(places[row]);
                              if (mapped == nullptr) {
                                  continue;
                              }
                              prefetch_group_join_entry(places, row, num_rows);
                              // Only create aggregate states when the probe side has aggregate
                              // functions and the build side did not already initialize them.
                              if (!aggregate_indices.empty()) {
                                  if (!shared_state->has_build_side_aggregate &&
                                      mapped->probe_count == 0) {
                                      create_entry_agg_states(mapped, shared_state);
                                  }
                                  places[row] = get_entry_agg_states(mapped, shared_state);
                              } else {
                                  places[row] = nullptr;
                              }
                              ++mapped->probe_count;
                              ++matched_probe_rows;
                              matched_rows += cast_set<int64_t>(mapped->build_count);
                          }
                          return Status::OK();
                      }},
            shared_state->data_variants->method_variant);
}

Status drain_groupjoin_result(GroupJoinSharedState* shared_state, size_t batch_size,
                              GroupJoinProbeLocalState& local_state, MutableColumns& key_columns,
                              MutableColumns& value_columns, bool& output_eos) {
    local_state._output_arena.clear();
    const size_t agg_size = shared_state->aggregate_evaluators.size();
    return std::visit(
            Overload {[&](std::monostate&) -> Status {
                          throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                      },
                      [&](auto& hash_method) -> Status {
                          using HashMethodType = std::decay_t<decltype(hash_method)>;
                          using KeyType = typename HashMethodType::Key;
                          std::vector<KeyType> keys(batch_size);
                          if (agg_size != 0) {
                              if (local_state._values.size() < batch_size) {
                                  local_state._values.resize(batch_size);
                              }
                              if (local_state._entries.size() < batch_size) {
                                  local_state._entries.resize(batch_size);
                              }
                          }

                          uint32_t num_rows = 0;
                          shared_state->data_container->init_once();
                          auto& iter = shared_state->data_container->iterator;
                          while (iter != shared_state->data_container->end() &&
                                 num_rows < batch_size) {
                              auto* entry =
                                      reinterpret_cast<GroupJoinEntry*>(iter.get_aggregate_data());
                              // Inner join only outputs groups that have valid rows on both
                              // sides, so probe_count alone is not sufficient.
                              if (entry->build_count > 0 && entry->probe_count > 0) {
                                  keys[num_rows] = iter.template get_key<KeyType>();
                                  if (agg_size != 0) {
                                      local_state._entries[num_rows] = entry;
                                      local_state._values[num_rows] =
                                              get_entry_agg_states(entry, shared_state);
                                  }
                                  ++num_rows;
                              }
                              ++iter;
                          }

                          hash_method.insert_keys_into_columns(keys, key_columns, num_rows);
                          if (agg_size != 0) {
                              const bool needs_build_counts =
                                      std::find(shared_state->aggregate_sides.begin(),
                                                shared_state->aggregate_sides.end(),
                                                TGroupJoinAggSide::PROBE) !=
                                      shared_state->aggregate_sides.end();
                              const bool needs_probe_counts =
                                      std::find(shared_state->aggregate_sides.begin(),
                                                shared_state->aggregate_sides.end(),
                                                TGroupJoinAggSide::BUILD) !=
                                      shared_state->aggregate_sides.end();
                              build_repeat_vectors(local_state._entries, num_rows,
                                                   needs_build_counts, needs_probe_counts,
                                                   local_state._build_counts,
                                                   local_state._probe_counts);
                          }
                          for (size_t i = 0; i < agg_size; ++i) {
                              const auto& repeats =
                                      shared_state->aggregate_sides[i] == TGroupJoinAggSide::BUILD
                                              ? local_state._probe_counts
                                              : local_state._build_counts;
                              shared_state->aggregate_evaluators[i]->insert_result_info_repeat_vec(
                                      local_state._values,
                                      shared_state->offsets_of_aggregate_states[i], repeats,
                                      value_columns[i].get(), num_rows, local_state._output_arena);
                          }

                          output_eos = iter == shared_state->data_container->end();
                          return Status::OK();
                      }},
            shared_state->data_variants->method_variant);
}

void destroy_entry_agg_states(GroupJoinSharedState* shared_state, GroupJoinEntry* entry) {
    if (entry == nullptr || (shared_state->has_build_side_aggregate ? entry->build_count == 0
                                                                    : entry->probe_count == 0)) {
        return;
    }
    destroy_all_agg_states(shared_state, get_entry_agg_states(entry, shared_state));
}

void destroy_agg_states(GroupJoinSharedState* shared_state) {
    if (shared_state == nullptr || shared_state->data_variants == nullptr) {
        return;
    }
    // Pure GROUP BY entries have no aggregate states to destroy.
    if (shared_state->aggregate_evaluators.empty() || shared_state->data_container == nullptr) {
        return;
    }
    auto iter = shared_state->data_container->begin();
    while (iter != shared_state->data_container->end()) {
        auto* entry = reinterpret_cast<GroupJoinEntry*>(iter.get_aggregate_data());
        destroy_entry_agg_states(shared_state, entry);
        ++iter;
    }
}

} // namespace doris::groupjoin
