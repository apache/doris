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

#include "partition_sort_sink_operator.h"

#include "common/status.h"

namespace doris {

namespace pipeline {

OperatorPtr PartitionSortSinkOperatorBuilder::build_operator() {
    return std::make_shared<PartitionSortSinkOperator>(this, _node);
}

Status PartitionSortSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSinkLocalState<PartitionSortDependency>::init(state, info));
    SCOPED_TIMER(profile()->total_time_counter());
    auto& p = _parent->cast<PartitionSortSinkOperatorX>();
    RETURN_IF_ERROR(p._vsort_exec_exprs.clone(state, _vsort_exec_exprs));
    _partition_expr_ctxs.resize(p._partition_expr_ctxs.size());
    _partition_columns.resize(p._partition_expr_ctxs.size());
    for (size_t i = 0; i < p._partition_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._partition_expr_ctxs[i]->clone(state, _partition_expr_ctxs[i]));
    }
    _partition_exprs_num = p._partition_exprs_num;
    _partitioned_data = std::make_unique<vectorized::PartitionedHashMapVariants>();
    _agg_arena_pool = std::make_unique<vectorized::Arena>();
    _hash_table_size_counter = ADD_COUNTER(_profile, "HashTableSize", TUnit::UNIT);
    _build_timer = ADD_TIMER(_profile, "HashTableBuildTime");
    _partition_sort_timer = ADD_TIMER(_profile, "PartitionSortTime");
    _get_sorted_timer = ADD_TIMER(_profile, "GetSortedTime");
    _selector_block_timer = ADD_TIMER(_profile, "SelectorBlockTime");
    _emplace_key_timer = ADD_TIMER(_profile, "EmplaceKeyTime");
    _init_hash_method();
    return Status::OK();
}

PartitionSortSinkOperatorX::PartitionSortSinkOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                                       const DescriptorTbl& descs)
        : DataSinkOperatorX(tnode.node_id),
          _pool(pool),
          _row_descriptor(descs, tnode.row_tuples, tnode.nullable_tuples),
          _limit(tnode.limit) {}

Status PartitionSortSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX::init(tnode, state));

    //order by key
    if (tnode.partition_sort_node.__isset.sort_info) {
        RETURN_IF_ERROR(_vsort_exec_exprs.init(tnode.partition_sort_node.sort_info, _pool));
        _is_asc_order = tnode.partition_sort_node.sort_info.is_asc_order;
        _nulls_first = tnode.partition_sort_node.sort_info.nulls_first;
    }
    //partition by key
    if (tnode.partition_sort_node.__isset.partition_exprs) {
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(
                tnode.partition_sort_node.partition_exprs, _partition_expr_ctxs));
        _partition_exprs_num = _partition_expr_ctxs.size();
    }

    _has_global_limit = tnode.partition_sort_node.has_global_limit;
    _top_n_algorithm = tnode.partition_sort_node.top_n_algorithm;
    _partition_inner_limit = tnode.partition_sort_node.partition_inner_limit;
    return Status::OK();
}

Status PartitionSortSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(_vsort_exec_exprs.prepare(state, _child_x->row_desc(), _row_descriptor));
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_partition_expr_ctxs, state, _child_x->row_desc()));
    return Status::OK();
}

Status PartitionSortSinkOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(_vsort_exec_exprs.open(state));
    RETURN_IF_ERROR(vectorized::VExpr::open(_partition_expr_ctxs, state));
    return Status::OK();
}

Status PartitionSortSinkOperatorX::sink(RuntimeState* state, vectorized::Block* input_block,
                                        SourceState source_state) {
    CREATE_SINK_LOCAL_STATE_RETURN_IF_ERROR(local_state);
    auto current_rows = input_block->rows();
    SCOPED_TIMER(local_state.profile()->total_time_counter());
    if (current_rows > 0) {
        local_state.child_input_rows = local_state.child_input_rows + current_rows;
        if (UNLIKELY(_partition_exprs_num == 0)) {
            if (UNLIKELY(local_state._value_places.empty())) {
                local_state._value_places.push_back(_pool->add(new vectorized::PartitionBlocks()));
            }
            //no partition key
            local_state._value_places[0]->append_whole_block(input_block, _child_x->row_desc());
        } else {
            //just simply use partition num to check
            if (local_state._num_partition > config::partition_topn_partition_threshold &&
                local_state.child_input_rows < 10000 * local_state._num_partition) {
                {
                    std::lock_guard<std::mutex> lock(local_state._shared_state->buffer_mutex);
                    local_state._shared_state->blocks_buffer.push(std::move(*input_block));
                    // buffer have data, source could read this.
                    local_state._dependency->set_ready_for_read();
                }
            } else {
                RETURN_IF_ERROR(
                        _split_block_by_partition(input_block, state->batch_size(), local_state));
                RETURN_IF_CANCELLED(state);
                RETURN_IF_ERROR(
                        state->check_query_state("VPartitionSortNode, while split input block."));
                input_block->clear_column_data();
            }
        }
    }

    if (source_state == SourceState::FINISHED) {
        //seems could free for hashtable
        local_state._agg_arena_pool.reset(nullptr);
        local_state._partitioned_data.reset(nullptr);
        for (int i = 0; i < local_state._value_places.size(); ++i) {
            auto sorter = vectorized::PartitionSorter::create_unique(
                    _vsort_exec_exprs, _limit, 0, _pool, _is_asc_order, _nulls_first,
                    _child_x->row_desc(), state, i == 0 ? local_state._profile : nullptr,
                    _has_global_limit, _partition_inner_limit, _top_n_algorithm,
                    local_state._shared_state->previous_row.get());

            DCHECK(_child_x->row_desc().num_materialized_slots() ==
                   local_state._value_places[i]->blocks.back()->columns());
            //get blocks from every partition, and sorter get those data.
            for (const auto& block : local_state._value_places[i]->blocks) {
                RETURN_IF_ERROR(sorter->append_block(block.get()));
            }
            sorter->init_profile(local_state._profile);
            RETURN_IF_ERROR(sorter->prepare_for_read());
            local_state._shared_state->partition_sorts.push_back(std::move(sorter));
        }

        COUNTER_SET(local_state._hash_table_size_counter, int64_t(local_state._num_partition));
        //so all data from child have sink completed
        local_state._dependency->set_ready_for_read();
    }

    return Status::OK();
}

Status PartitionSortSinkOperatorX::_split_block_by_partition(
        vectorized::Block* input_block, int batch_size, PartitionSortSinkLocalState& local_state) {
    for (int i = 0; i < _partition_exprs_num; ++i) {
        int result_column_id = -1;
        RETURN_IF_ERROR(_partition_expr_ctxs[i]->execute(input_block, &result_column_id));
        DCHECK(result_column_id != -1);
        local_state._partition_columns[i] =
                input_block->get_by_position(result_column_id).column.get();
    }
    _emplace_into_hash_table(local_state._partition_columns, input_block, batch_size, local_state);
    return Status::OK();
}

void PartitionSortSinkOperatorX::_emplace_into_hash_table(
        const vectorized::ColumnRawPtrs& key_columns, const vectorized::Block* input_block,
        int batch_size, PartitionSortSinkLocalState& local_state) {
    std::visit(
            [&](auto&& agg_method) -> void {
                SCOPED_TIMER(local_state._build_timer);
                using HashMethodType = std::decay_t<decltype(agg_method)>;
                using HashTableType = std::decay_t<decltype(agg_method.data)>;
                using AggState = typename HashMethodType::State;

                AggState state(key_columns, local_state._partition_key_sz, nullptr);
                size_t num_rows = input_block->rows();
                _pre_serialize_key_if_need(state, agg_method, key_columns, num_rows);

                //PHHashMap
                const auto& keys = state.get_keys();
                if constexpr (HashTableTraits<HashTableType>::is_phmap) {
                    local_state._hash_values.resize(num_rows);

                    for (size_t i = 0; i < num_rows; ++i) {
                        local_state._hash_values[i] = agg_method.data.hash(keys[i]);
                    }
                }

                for (size_t row = 0; row < num_rows; ++row) {
                    SCOPED_TIMER(local_state._emplace_key_timer);
                    vectorized::PartitionDataPtr aggregate_data = nullptr;
                    auto emplace_result = [&]() {
                        if constexpr (HashTableTraits<HashTableType>::is_phmap) {
                            if (LIKELY(row + HASH_MAP_PREFETCH_DIST < num_rows)) {
                                agg_method.data.prefetch_by_hash(
                                        local_state._hash_values[row + HASH_MAP_PREFETCH_DIST]);
                            }
                            return state.emplace_with_key(agg_method.data, keys[row],
                                                          local_state._hash_values[row], row);
                        } else {
                            return state.emplace_with_key(agg_method.data, keys[row], row);
                        }
                    }();

                    /// If a new key is inserted, initialize the states of the aggregate functions, and possibly something related to the key.
                    if (emplace_result.is_inserted()) {
                        /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
                        emplace_result.set_mapped(nullptr);
                        aggregate_data = _pool->add(new vectorized::PartitionBlocks());
                        emplace_result.set_mapped(aggregate_data);
                        local_state._value_places.push_back(aggregate_data);
                        local_state._num_partition++;
                    } else {
                        aggregate_data = emplace_result.get_mapped();
                    }
                    assert(aggregate_data != nullptr);
                    aggregate_data->add_row_idx(row);
                }
                for (auto place : local_state._value_places) {
                    SCOPED_TIMER(local_state._selector_block_timer);
                    place->append_block_by_selector(input_block, _child_x->row_desc(),
                                                    _has_global_limit, _partition_inner_limit,
                                                    batch_size);
                }
            },
            local_state._partitioned_data->method_variant);
}

void PartitionSortSinkLocalState::_init_hash_method() {
    if (_partition_exprs_num == 0) {
        return;
    } else if (_partition_exprs_num == 1) {
        auto is_nullable = _partition_expr_ctxs[0]->root()->is_nullable();
        switch (_partition_expr_ctxs[0]->root()->result_type()) {
        case TYPE_TINYINT:
        case TYPE_BOOLEAN:
            _partitioned_data->init(vectorized::PartitionedHashMapVariants::Type::int8_key,
                                    is_nullable);
            return;
        case TYPE_SMALLINT:
            _partitioned_data->init(vectorized::PartitionedHashMapVariants::Type::int16_key,
                                    is_nullable);
            return;
        case TYPE_INT:
        case TYPE_FLOAT:
        case TYPE_DATEV2:
            _partitioned_data->init(vectorized::PartitionedHashMapVariants::Type::int32_key,
                                    is_nullable);
            return;
        case TYPE_BIGINT:
        case TYPE_DOUBLE:
        case TYPE_DATE:
        case TYPE_DATETIME:
        case TYPE_DATETIMEV2:
            _partitioned_data->init(vectorized::PartitionedHashMapVariants::Type::int64_key,
                                    is_nullable);
            return;
        case TYPE_LARGEINT: {
            _partitioned_data->init(vectorized::PartitionedHashMapVariants::Type::int128_key,
                                    is_nullable);
            return;
        }
        case TYPE_DECIMALV2:
        case TYPE_DECIMAL32:
        case TYPE_DECIMAL64:
        case TYPE_DECIMAL128I: {
            vectorized::DataTypePtr& type_ptr = _partition_expr_ctxs[0]->root()->data_type();
            vectorized::TypeIndex idx =
                    is_nullable ? assert_cast<const vectorized::DataTypeNullable&>(*type_ptr)
                                          .get_nested_type()
                                          ->get_type_id()
                                : type_ptr->get_type_id();
            vectorized::WhichDataType which(idx);
            if (which.is_decimal32()) {
                _partitioned_data->init(vectorized::PartitionedHashMapVariants::Type::int32_key,
                                        is_nullable);
            } else if (which.is_decimal64()) {
                _partitioned_data->init(vectorized::PartitionedHashMapVariants::Type::int64_key,
                                        is_nullable);
            } else {
                _partitioned_data->init(vectorized::PartitionedHashMapVariants::Type::int128_key,
                                        is_nullable);
            }
            return;
        }
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_STRING: {
            _partitioned_data->init(vectorized::PartitionedHashMapVariants::Type::string_key,
                                    is_nullable);
            break;
        }
        default:
            _partitioned_data->init(vectorized::PartitionedHashMapVariants::Type::serialized);
        }
    } else {
        bool use_fixed_key = true;
        bool has_null = false;
        size_t key_byte_size = 0;
        size_t bitmap_size = vectorized::get_bitmap_size(_partition_exprs_num);

        _partition_key_sz.resize(_partition_exprs_num);
        for (int i = 0; i < _partition_exprs_num; ++i) {
            const auto& data_type = _partition_expr_ctxs[i]->root()->data_type();

            if (!data_type->have_maximum_size_of_value()) {
                use_fixed_key = false;
                break;
            }

            auto is_null = data_type->is_nullable();
            has_null |= is_null;
            _partition_key_sz[i] =
                    data_type->get_maximum_size_of_value_in_memory() - (is_null ? 1 : 0);
            key_byte_size += _partition_key_sz[i];
        }

        if (bitmap_size + key_byte_size > sizeof(vectorized::UInt256)) {
            use_fixed_key = false;
        }

        if (use_fixed_key) {
            if (has_null) {
                if (bitmap_size + key_byte_size <= sizeof(vectorized::UInt64)) {
                    _partitioned_data->init(
                            vectorized::PartitionedHashMapVariants::Type::int64_keys, has_null);
                } else if (bitmap_size + key_byte_size <= sizeof(vectorized::UInt128)) {
                    _partitioned_data->init(
                            vectorized::PartitionedHashMapVariants::Type::int128_keys, has_null);
                } else {
                    _partitioned_data->init(
                            vectorized::PartitionedHashMapVariants::Type::int256_keys, has_null);
                }
            } else {
                if (key_byte_size <= sizeof(vectorized::UInt64)) {
                    _partitioned_data->init(
                            vectorized::PartitionedHashMapVariants::Type::int64_keys, has_null);
                } else if (key_byte_size <= sizeof(vectorized::UInt128)) {
                    _partitioned_data->init(
                            vectorized::PartitionedHashMapVariants::Type::int128_keys, has_null);
                } else {
                    _partitioned_data->init(
                            vectorized::PartitionedHashMapVariants::Type::int256_keys, has_null);
                }
            }
        } else {
            _partitioned_data->init(vectorized::PartitionedHashMapVariants::Type::serialized);
        }
    }
}

} // namespace pipeline
} // namespace doris
