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

#include "exec/operator/groupjoin_build_sink.h"

#include <algorithm>
#include <limits>

#include "core/column/column_nullable.h"
#include "exec/common/hash_table/hash_map_util.h"
#include "exec/common/hash_table/join_hash_table_utils.h"
#include "exec/common/join_utils.h"
#include "exec/common/template_helpers.hpp"
#include "exec/common/util.hpp"
#include "exec/operator/groupjoin_operator_utils.h"
#include "exec/runtime_filter/runtime_filter_producer_helper.h"
#include "exprs/vexpr.h"
#include "exprs/vslot_ref.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace doris {
GroupJoinSharedState::GroupJoinSharedState()
        : build_context(std::make_shared<JoinDataVariants>()) {}
GroupJoinSharedState::~GroupJoinSharedState() = default;

Status GroupJoinBuildSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    auto& parent = _parent->cast<GroupJoinBuildSinkOperatorX>();
    for (const auto& expr : parent._key_exprs) {
        VExprContextSPtr clone;
        RETURN_IF_ERROR(expr->clone(state, clone));
        _key_exprs.push_back(std::move(clone));
    }
    if (!parent._runtime_filter_descs.empty()) {
        _filter_helper = std::make_shared<RuntimeFilterProducerHelper>(true, false);
        RETURN_IF_ERROR(_filter_helper->init(state, _key_exprs, parent._runtime_filter_descs,
                                             parent._child->operator_row_desc_after_projection()));
    }
    _build_table_timer = ADD_TIMER(custom_profile(), "BuildHashTableTime");
    _merge_timer = ADD_TIMER(custom_profile(), "MergeBuildBlockTime");
    _dependency->set_ready();
    return Status::OK();
}

GroupJoinBuildSinkOperatorX::GroupJoinBuildSinkOperatorX(ObjectPool* pool, int operator_id,
                                                         int dest_id, const TPlanNode& node,
                                                         const DescriptorTbl& descs)
        : Base(operator_id, node, dest_id),
          _runtime_filter_descs(node.runtime_filters),
          _distribution(node.group_join_node.dist_type),
          _partition_exprs(node.__isset.distribute_expr_lists ? node.distribute_expr_lists[1]
                                                              : std::vector<TExpr> {}) {
    _name = "GROUP_JOIN_BUILD_SINK_OPERATOR";
}

Status GroupJoinBuildSinkOperatorX::init(const TPlanNode& node, RuntimeState* state) {
    RETURN_IF_ERROR(Base::init(node, state));
    RETURN_IF_ERROR(groupjoin::validate_group_join_node(node));
    for (const auto& conjunct : node.group_join_node.eq_join_conjuncts) {
        VExprContextSPtr expr;
        RETURN_IF_ERROR(VExpr::create_expr_tree(conjunct.right, expr));
        _key_exprs.push_back(std::move(expr));
    }
    return Status::OK();
}

Status GroupJoinBuildSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));
    for (const auto& expr : _key_exprs) {
        RETURN_IF_ERROR(expr->prepare(state, _child->operator_row_desc_after_projection()));
        RETURN_IF_ERROR(expr->open(state));
    }
    return Status::OK();
}

Status GroupJoinBuildSinkLocalState::terminate(RuntimeState* state) {
    if (_terminated) {
        return Status::OK();
    }
    if (_filter_helper) {
        RETURN_IF_ERROR(_filter_helper->skip_process(state));
    }
    return Base::terminate(state);
}

Status GroupJoinBuildSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    if (_filter_helper) {
        if (!_terminated && !state->is_cancelled() && exec_status.ok() && _filter_size_sent &&
            _shared_state->build_block.rows() > 1) {
            // Decoupled RF expressions may append columns: never mutate the published build Block.
            Block filter_block = _shared_state->build_block;
            std::map<int, std::shared_ptr<RuntimeFilterWrapper>> unused_shared_filters;
            RETURN_IF_ERROR(
                    _filter_helper->build(state, &filter_block, false, unused_shared_filters));
            RETURN_IF_ERROR(_filter_helper->publish(state));
        } else {
            RETURN_IF_ERROR(_filter_helper->skip_process(state));
        }
        _filter_helper->collect_realtime_profile(custom_profile());
    }
    return Base::close(state, exec_status);
}

size_t GroupJoinBuildSinkOperatorX::get_reserve_mem_size(RuntimeState* state, bool eos,
                                                         const Block* block) {
    auto& local = get_local_state(state);
    // Follow HashJoinBuildSinkLocalState::get_reserve_mem_size() in hashjoin_build_sink.cpp:
    // reserve for merge growth, hash-table arrays, key evaluation and method-specific buffers.
    // Existing build columns are already tracked; to_block() moves them without copying.
    size_t reserve = 0;
    if (block->rows() > 0) {
        reserve += block->allocated_bytes();
        if (local._build_rows.bytes() + block->bytes() >= local._build_rows.allocated_bytes()) {
            reserve += local._build_rows.allocated_bytes();
        }
    }
    if (!eos) {
        return reserve;
    }

    const auto build_rows = local._build_rows.rows();
    const auto rows = build_rows + block->rows() + (build_rows == 0 ? 1 : 0);
    if (rows == 1) {
        return reserve; // Empty builds skip key evaluation and hash-table allocation.
    }
    if (rows > std::numeric_limits<uint32_t>::max()) {
        throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                        "GroupJoin build exceeds uint32 row indexes");
    }

    // HashJoin has already evaluated its keys when estimating memory. GroupJoin evaluates
    // them at build EOS, so estimate expressions without executing them here. Direct mapping
    // is also selected later; use the ordinary bucket count as an upper bound.
    std::vector<DataTypePtr> key_types;
    // Only the serialized method reads these columns; include both accumulated and input rows.
    ColumnRawPtrs serialization_columns;
    size_t expression_bytes = 0;
    size_t extra_serialized_bytes = 0;
    bool has_nullable_key = false;
    for (const auto& expr : local._key_exprs) {
        auto root = expr->root();
        key_types.push_back(remove_nullable(root->data_type()));
        has_nullable_key |= root->data_type()->is_nullable();
        const auto evaluated_bytes = root->estimate_memory(rows);
        expression_bytes += evaluated_bytes;
        if (root->is_slot_ref()) {
            const auto column_id = assert_cast<VSlotRef*>(root.get())->column_id();
            if (build_rows > 0) {
                serialization_columns.push_back(
                        local._build_rows.get_column_by_position(column_id).get());
            }
            if (block->rows() > 0) {
                const auto* column = block->get_by_position(column_id).column.get();
                serialization_columns.push_back(column);
                if (is_column_const(*column)) {
                    // merge() materializes constants before key serialization.
                    extra_serialized_bytes += column->byte_size() * (block->rows() - 1);
                }
            }
        } else {
            // Computed key columns do not exist yet. Use the expression's estimate
            // for their serialized payload rather than evaluating them a second time.
            extra_serialized_bytes += evaluated_bytes;
        }
    }
    JoinDataVariants context;
    THROW_IF_ERROR(init_hash_method<JoinDataVariants>(&context, key_types, true));
    const auto buckets = hash_join_table_calc_bucket_size(rows + 1);
    reserve += (buckets + rows + 1) * sizeof(uint32_t);
    reserve += expression_bytes;
    if (has_nullable_key) {
        reserve += rows * sizeof(uint8_t); // Combined build-key NULL map.
    }
    std::visit(Overload {[](std::monostate&) { DORIS_CHECK(false); },
                         [&](auto& method) {
                             using Method = std::decay_t<decltype(method)>;
                             if constexpr (requires { method.key_sizes; }) {
                                 // MethodKeysFixed::estimated_size() assumes StringRef-sized
                                 // keys. Account for the actual packed key width instead.
                                 using Key = typename Method::Key;
                                 reserve += rows * (sizeof(Key) + sizeof(uint32_t));
                             } else {
                                 reserve += method.estimated_size(serialization_columns,
                                                                  static_cast<uint32_t>(rows), true,
                                                                  true, buckets);
                             }
                             if constexpr (std::is_same_v<Method, SerializedHashTableContext>) {
                                 reserve += extra_serialized_bytes;
                             }
                         }},
               context.method_variant);
    return reserve;
}

Status GroupJoinBuildSinkOperatorX::sink_impl(RuntimeState* state, Block* block, bool eos) {
    auto& local = get_local_state(state);
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(local.exec_time_counter());
    COUNTER_UPDATE(local.rows_input_counter(), static_cast<int64_t>(block->rows()));
    if (local._build_rows.empty() && (block->rows() > 0 || eos)) {
        auto schema = VectorizedUtils::create_empty_columnswithtypename(
                _child->operator_row_desc_after_projection());
        // Row zero is reserved by JoinHashTable. Copy a real row so evaluating BUILD
        // arguments does not introduce errors from synthetic default values.
        // An entirely empty build keeps a default sentinel and skips expression evaluation.
        auto sentinel = schema.create_same_struct_block(block->rows() > 0 ? 0 : 1, false);
        local._build_rows = MutableBlock::build_mutable_block(std::move(*sentinel));
        if (block->rows() > 0) {
            for (size_t i = 0; i < local._build_rows.columns(); ++i) {
                auto source = block->get_by_position(i).column->convert_to_full_column_if_const();
                local._build_rows.get_column_by_position(i)->insert_from(*source, 0);
            }
        }
    }
    if (block->rows() > 0) {
        if (local._build_rows.rows() + block->rows() > std::numeric_limits<uint32_t>::max()) {
            return Status::NotSupported("GroupJoin build exceeds uint32 row indexes");
        }
        SCOPED_TIMER(local._merge_timer);
        // TODO: Support ColumnString64 in build key and aggregate argument expressions before
        // allowing string offset overflow. Use checked merge to reject oversized string columns.
        RETURN_IF_ERROR(local._build_rows.merge(std::move(*block)));
        COUNTER_SET(local._memory_used_counter, local._build_rows.allocated_bytes());
    }
    if (eos) {
        auto* shared = local._shared_state;
        shared->build_block = local._build_rows.to_block();
        RETURN_IF_ERROR(_build_hash_table(state, local));
        if (local._filter_helper && shared->build_block.rows() > 1) {
            RETURN_IF_ERROR(local._filter_helper->send_filter_size(
                    state, shared->build_block.rows() - 1, local._finish_dependency));
            local._filter_size_sent = true;
        }
        local._dependency->set_ready_to_read();
    }
    return Status::OK();
}

Status GroupJoinBuildSinkOperatorX::_build_hash_table(RuntimeState* state,
                                                      GroupJoinBuildSinkLocalState& local) const {
    auto* shared = local._shared_state;
    if (shared->build_block.rows() == 1) {
        // Do not evaluate join keys on a synthetic row when the actual build is empty.
        shared->short_circuit_for_probe = true;
        return Status::OK();
    }
    RETURN_IF_ERROR(
            groupjoin::do_evaluate(shared->build_block, local._key_exprs, shared->build_keys));
    ColumnRawPtrs raw_keys;
    ColumnUInt8::MutablePtr null_map;
    RETURN_IF_ERROR(groupjoin::extract_key_columns(shared->build_block.rows(), shared->build_keys,
                                                   raw_keys, null_map));
    const auto* nulls = null_map ? null_map->get_data().data() : nullptr;
    std::vector<DataTypePtr> key_types;
    ColumnRawPtrs nullable_keys;
    for (size_t i = 0; i < local._key_exprs.size(); ++i) {
        key_types.push_back(remove_nullable(local._key_exprs[i]->root()->data_type()));
        nullable_keys.push_back(shared->build_keys[i].get());
    }
    RETURN_IF_ERROR(
            init_hash_method<JoinDataVariants>(shared->build_context.get(), key_types, true));
    // Use the ordinary Join conversion, retaining the NULL map during min/max selection.
    std::visit(
            [&](auto& method) {
                try_convert_to_direct_mapping(&method, nullable_keys, {shared->build_context});
            },
            shared->build_context->method_variant);
    shared->short_circuit_for_probe =
            null_map && std::all_of(null_map->get_data().begin() + 1, null_map->get_data().end(),
                                    [](uint8_t value) { return value != 0; });
    {
        SCOPED_TIMER(local._build_table_timer);
        const auto rows = static_cast<uint32_t>(shared->build_block.rows());
        std::visit(Overload {[](std::monostate&) { DORIS_CHECK(false); },
                             [&](auto& method) {
                                 build_join_hash_table<TJoinOp::INNER_JOIN>(
                                         method, raw_keys, rows, state->batch_size(), nulls,
                                         nulls && simd::contain_one(nulls + 1, rows - 1), false);
                                 COUNTER_SET(local._memory_used_counter,
                                             shared->build_block.allocated_bytes() +
                                                     method.hash_table->get_byte_size() +
                                                     method.serialized_keys_size(true));
                                 local.custom_profile()->add_info_string(
                                         "IndexLayout", method.direct_mapping_range() != 0
                                                                ? "DirectMapping"
                                                                : "BucketChain");
                             }},
                   shared->build_context->method_variant);
    }
    return Status::OK();
}

DataDistribution GroupJoinBuildSinkOperatorX::required_data_distribution(RuntimeState*) const {
    return groupjoin::distribution(_distribution, _partition_exprs);
}
bool GroupJoinBuildSinkOperatorX::is_shuffled_operator() const {
    return _distribution != TJoinDistributionType::NONE;
}
bool GroupJoinBuildSinkOperatorX::is_colocated_operator() const {
    return _distribution == TJoinDistributionType::BUCKET_SHUFFLE ||
           _distribution == TJoinDistributionType::COLOCATE;
}
bool GroupJoinBuildSinkOperatorX::followed_by_shuffled_operator() const {
    return (is_shuffled_operator() && !is_colocated_operator()) || _followed_by_shuffled_operator;
}

} // namespace doris
