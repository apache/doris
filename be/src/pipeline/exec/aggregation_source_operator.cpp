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

#include "aggregation_source_operator.h"

#include <memory>
#include <string>

#include "common/exception.h"
#include "pipeline/exec/operator.h"
#include "vec/exprs/vectorized_agg_fn.h"

namespace doris::pipeline {

AggLocalState::AggLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent),
          _get_results_timer(nullptr),
          _serialize_result_timer(nullptr),
          _hash_table_iterate_timer(nullptr),
          _insert_keys_to_column_timer(nullptr),
          _serialize_data_timer(nullptr) {}

Status AggLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    _get_results_timer = ADD_TIMER(profile(), "GetResultsTime");
    _serialize_result_timer = ADD_TIMER(profile(), "SerializeResultTime");
    _hash_table_iterate_timer = ADD_TIMER(profile(), "HashTableIterateTime");
    _insert_keys_to_column_timer = ADD_TIMER(profile(), "InsertKeysToColumnTime");
    _serialize_data_timer = ADD_TIMER(profile(), "SerializeDataTime");

    _merge_timer = ADD_TIMER(Base::profile(), "MergeTime");
    _deserialize_data_timer = ADD_TIMER(Base::profile(), "DeserializeAndMergeTime");
    _hash_table_compute_timer = ADD_TIMER(Base::profile(), "HashTableComputeTime");
    _hash_table_emplace_timer = ADD_TIMER(Base::profile(), "HashTableEmplaceTime");
    _hash_table_input_counter = ADD_COUNTER(Base::profile(), "HashTableInputCount", TUnit::UNIT);

    auto& p = _parent->template cast<AggSourceOperatorX>();
    if (p._without_key) {
        if (p._needs_finalize) {
            _executor.get_result = std::bind<Status>(&AggLocalState::_get_without_key_result, this,
                                                     std::placeholders::_1, std::placeholders::_2,
                                                     std::placeholders::_3);
        } else {
            _executor.get_result = std::bind<Status>(&AggLocalState::_serialize_without_key, this,
                                                     std::placeholders::_1, std::placeholders::_2,
                                                     std::placeholders::_3);
        }
    } else {
        if (p._needs_finalize) {
            _executor.get_result = std::bind<Status>(
                    &AggLocalState::_get_with_serialized_key_result, this, std::placeholders::_1,
                    std::placeholders::_2, std::placeholders::_3);
        } else {
            _executor.get_result = std::bind<Status>(
                    &AggLocalState::_serialize_with_serialized_key_result, this,
                    std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
        }
    }

    return Status::OK();
}

Status AggLocalState::_create_agg_status(vectorized::AggregateDataPtr data) {
    auto& shared_state = *Base::_shared_state;
    for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
        try {
            shared_state.aggregate_evaluators[i]->create(
                    data + shared_state.offsets_of_aggregate_states[i]);
        } catch (...) {
            for (int j = 0; j < i; ++j) {
                shared_state.aggregate_evaluators[j]->destroy(
                        data + shared_state.offsets_of_aggregate_states[j]);
            }
            throw;
        }
    }
    return Status::OK();
}

Status AggLocalState::_destroy_agg_status(vectorized::AggregateDataPtr data) {
    auto& shared_state = *Base::_shared_state;
    for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
        shared_state.aggregate_evaluators[i]->function()->destroy(
                data + shared_state.offsets_of_aggregate_states[i]);
    }
    return Status::OK();
}

Status AggLocalState::_serialize_with_serialized_key_result(RuntimeState* state,
                                                            vectorized::Block* block, bool* eos) {
    SCOPED_TIMER(_serialize_result_timer);
    auto& shared_state = *_shared_state;
    int key_size = _shared_state->probe_expr_ctxs.size();
    int agg_size = _shared_state->aggregate_evaluators.size();
    vectorized::MutableColumns value_columns(agg_size);
    vectorized::DataTypes value_data_types(agg_size);

    // non-nullable column(id in `_make_nullable_keys`) will be converted to nullable.
    bool mem_reuse = shared_state.make_nullable_keys.empty() && block->mem_reuse();

    vectorized::MutableColumns key_columns;
    for (int i = 0; i < key_size; ++i) {
        if (mem_reuse) {
            key_columns.emplace_back(std::move(*block->get_by_position(i).column).mutate());
        } else {
            key_columns.emplace_back(
                    shared_state.probe_expr_ctxs[i]->root()->data_type()->create_column());
        }
    }

    SCOPED_TIMER(_get_results_timer);
    std::visit(
            vectorized::Overload {
                    [&](std::monostate& arg) -> void {
                        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                    },
                    [&](auto& agg_method) -> void {
                        agg_method.init_iterator();
                        auto& data = *agg_method.hash_table;
                        const auto size = std::min(data.size(), size_t(state->batch_size()));
                        using KeyType = std::decay_t<decltype(agg_method.iterator->get_first())>;
                        std::vector<KeyType> keys(size);
                        if (shared_state.values.size() < size + 1) {
                            shared_state.values.resize(size + 1);
                        }

                        size_t num_rows = 0;
                        shared_state.aggregate_data_container->init_once();
                        auto& iter = shared_state.aggregate_data_container->iterator;

                        {
                            SCOPED_TIMER(_hash_table_iterate_timer);
                            while (iter != shared_state.aggregate_data_container->end() &&
                                   num_rows < state->batch_size()) {
                                keys[num_rows] = iter.template get_key<KeyType>();
                                shared_state.values[num_rows] = iter.get_aggregate_data();
                                ++iter;
                                ++num_rows;
                            }
                        }

                        {
                            SCOPED_TIMER(_insert_keys_to_column_timer);
                            agg_method.insert_keys_into_columns(keys, key_columns, num_rows);
                        }

                        if (iter == shared_state.aggregate_data_container->end()) {
                            if (agg_method.hash_table->has_null_key_data()) {
                                // only one key of group by support wrap null key
                                // here need additional processing logic on the null key / value
                                DCHECK(key_columns.size() == 1);
                                DCHECK(key_columns[0]->is_nullable());
                                if (agg_method.hash_table->has_null_key_data()) {
                                    key_columns[0]->insert_data(nullptr, 0);
                                    shared_state.values[num_rows] =
                                            agg_method.hash_table->template get_null_key_data<
                                                    vectorized::AggregateDataPtr>();
                                    ++num_rows;
                                    *eos = true;
                                }
                            } else {
                                *eos = true;
                            }
                        }

                        {
                            SCOPED_TIMER(_serialize_data_timer);
                            for (size_t i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
                                value_data_types[i] = shared_state.aggregate_evaluators[i]
                                                              ->function()
                                                              ->get_serialized_type();
                                if (mem_reuse) {
                                    value_columns[i] =
                                            std::move(*block->get_by_position(i + key_size).column)
                                                    .mutate();
                                } else {
                                    value_columns[i] = shared_state.aggregate_evaluators[i]
                                                               ->function()
                                                               ->create_serialize_column();
                                }
                                shared_state.aggregate_evaluators[i]
                                        ->function()
                                        ->serialize_to_column(
                                                shared_state.values,
                                                shared_state.offsets_of_aggregate_states[i],
                                                value_columns[i], num_rows);
                            }
                        }
                    }},
            shared_state.agg_data->method_variant);

    if (!mem_reuse) {
        vectorized::ColumnsWithTypeAndName columns_with_schema;
        for (int i = 0; i < key_size; ++i) {
            columns_with_schema.emplace_back(std::move(key_columns[i]),
                                             shared_state.probe_expr_ctxs[i]->root()->data_type(),
                                             shared_state.probe_expr_ctxs[i]->root()->expr_name());
        }
        for (int i = 0; i < agg_size; ++i) {
            columns_with_schema.emplace_back(std::move(value_columns[i]), value_data_types[i], "");
        }
        *block = vectorized::Block(columns_with_schema);
    }

    return Status::OK();
}

Status AggLocalState::_get_with_serialized_key_result(RuntimeState* state, vectorized::Block* block,
                                                      bool* eos) {
    auto& shared_state = *_shared_state;
    // non-nullable column(id in `_make_nullable_keys`) will be converted to nullable.
    bool mem_reuse = shared_state.make_nullable_keys.empty() && block->mem_reuse();

    auto columns_with_schema = vectorized::VectorizedUtils::create_columns_with_type_and_name(
            _parent->cast<AggSourceOperatorX>()._row_descriptor);
    int key_size = shared_state.probe_expr_ctxs.size();

    vectorized::MutableColumns key_columns;
    for (int i = 0; i < key_size; ++i) {
        if (!mem_reuse) {
            key_columns.emplace_back(columns_with_schema[i].type->create_column());
        } else {
            key_columns.emplace_back(std::move(*block->get_by_position(i).column).mutate());
        }
    }
    vectorized::MutableColumns value_columns;
    for (int i = key_size; i < columns_with_schema.size(); ++i) {
        if (!mem_reuse) {
            value_columns.emplace_back(columns_with_schema[i].type->create_column());
        } else {
            value_columns.emplace_back(std::move(*block->get_by_position(i).column).mutate());
        }
    }

    SCOPED_TIMER(_get_results_timer);
    std::visit(
            vectorized::Overload {
                    [&](std::monostate& arg) -> void {
                        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                    },
                    [&](auto& agg_method) -> void {
                        auto& data = *agg_method.hash_table;
                        agg_method.init_iterator();
                        const auto size = std::min(data.size(), size_t(state->batch_size()));
                        using KeyType = std::decay_t<decltype(agg_method.iterator->get_first())>;
                        std::vector<KeyType> keys(size);
                        if (shared_state.values.size() < size) {
                            shared_state.values.resize(size);
                        }

                        size_t num_rows = 0;
                        shared_state.aggregate_data_container->init_once();
                        auto& iter = shared_state.aggregate_data_container->iterator;

                        {
                            SCOPED_TIMER(_hash_table_iterate_timer);
                            while (iter != shared_state.aggregate_data_container->end() &&
                                   num_rows < state->batch_size()) {
                                keys[num_rows] = iter.template get_key<KeyType>();
                                shared_state.values[num_rows] = iter.get_aggregate_data();
                                ++iter;
                                ++num_rows;
                            }
                        }

                        {
                            SCOPED_TIMER(_insert_keys_to_column_timer);
                            agg_method.insert_keys_into_columns(keys, key_columns, num_rows);
                        }

                        for (size_t i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
                            shared_state.aggregate_evaluators[i]->insert_result_info_vec(
                                    shared_state.values,
                                    shared_state.offsets_of_aggregate_states[i],
                                    value_columns[i].get(), num_rows);
                        }

                        if (iter == shared_state.aggregate_data_container->end()) {
                            if (agg_method.hash_table->has_null_key_data()) {
                                // only one key of group by support wrap null key
                                // here need additional processing logic on the null key / value
                                DCHECK(key_columns.size() == 1);
                                DCHECK(key_columns[0]->is_nullable());
                                if (key_columns[0]->size() < state->batch_size()) {
                                    key_columns[0]->insert_data(nullptr, 0);
                                    auto mapped = agg_method.hash_table->template get_null_key_data<
                                            vectorized::AggregateDataPtr>();
                                    for (size_t i = 0; i < shared_state.aggregate_evaluators.size();
                                         ++i)
                                        shared_state.aggregate_evaluators[i]->insert_result_info(
                                                mapped +
                                                        shared_state.offsets_of_aggregate_states[i],
                                                value_columns[i].get());
                                    *eos = true;
                                }
                            } else {
                                *eos = true;
                            }
                        }
                    }},
            shared_state.agg_data->method_variant);

    if (!mem_reuse) {
        *block = columns_with_schema;
        vectorized::MutableColumns columns(block->columns());
        for (int i = 0; i < block->columns(); ++i) {
            if (i < key_size) {
                columns[i] = std::move(key_columns[i]);
            } else {
                columns[i] = std::move(value_columns[i - key_size]);
            }
        }
        block->set_columns(std::move(columns));
    }

    return Status::OK();
}

Status AggLocalState::_serialize_without_key(RuntimeState* state, vectorized::Block* block,
                                             bool* eos) {
    auto& shared_state = *_shared_state;
    // 1. `child(0)->rows_returned() == 0` mean not data from child
    // in level two aggregation node should return NULL result
    //    level one aggregation node set `eos = true` return directly
    SCOPED_TIMER(_serialize_result_timer);
    if (UNLIKELY(_shared_state->input_num_rows == 0)) {
        *eos = true;
        return Status::OK();
    }
    block->clear();

    DCHECK(shared_state.agg_data->without_key != nullptr);
    int agg_size = shared_state.aggregate_evaluators.size();

    vectorized::MutableColumns value_columns(agg_size);
    std::vector<vectorized::DataTypePtr> data_types(agg_size);
    // will serialize data to string column
    for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
        data_types[i] = shared_state.aggregate_evaluators[i]->function()->get_serialized_type();
        value_columns[i] =
                shared_state.aggregate_evaluators[i]->function()->create_serialize_column();
    }

    for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
        shared_state.aggregate_evaluators[i]->function()->serialize_without_key_to_column(
                shared_state.agg_data->without_key + shared_state.offsets_of_aggregate_states[i],
                *value_columns[i]);
    }

    {
        vectorized::ColumnsWithTypeAndName data_with_schema;
        for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
            vectorized::ColumnWithTypeAndName column_with_schema = {nullptr, data_types[i], ""};
            data_with_schema.push_back(std::move(column_with_schema));
        }
        *block = vectorized::Block(data_with_schema);
    }

    block->set_columns(std::move(value_columns));
    *eos = true;
    return Status::OK();
}

Status AggLocalState::_get_without_key_result(RuntimeState* state, vectorized::Block* block,
                                              bool* eos) {
    auto& shared_state = *_shared_state;
    DCHECK(_shared_state->agg_data->without_key != nullptr);
    block->clear();

    auto& p = _parent->cast<AggSourceOperatorX>();
    *block = vectorized::VectorizedUtils::create_empty_columnswithtypename(p._row_descriptor);
    int agg_size = shared_state.aggregate_evaluators.size();

    vectorized::MutableColumns columns(agg_size);
    std::vector<vectorized::DataTypePtr> data_types(agg_size);
    for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
        data_types[i] = shared_state.aggregate_evaluators[i]->function()->get_return_type();
        columns[i] = data_types[i]->create_column();
    }

    for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
        auto column = columns[i].get();
        shared_state.aggregate_evaluators[i]->insert_result_info(
                shared_state.agg_data->without_key + shared_state.offsets_of_aggregate_states[i],
                column);
    }

    const auto& block_schema = block->get_columns_with_type_and_name();
    DCHECK_EQ(block_schema.size(), columns.size());
    for (int i = 0; i < block_schema.size(); ++i) {
        const auto column_type = block_schema[i].type;
        if (!column_type->equals(*data_types[i])) {
            if (!vectorized::is_array(remove_nullable(column_type))) {
                if (!column_type->is_nullable() || data_types[i]->is_nullable() ||
                    !remove_nullable(column_type)->equals(*data_types[i])) {
                    return Status::InternalError(
                            "node id = {}, column_type not match data_types, column_type={}, "
                            "data_types={}",
                            _parent->node_id(), column_type->get_name(), data_types[i]->get_name());
                }
            }

            if (column_type->is_nullable() && !data_types[i]->is_nullable()) {
                vectorized::ColumnPtr ptr = std::move(columns[i]);
                // unless `count`, other aggregate function dispose empty set should be null
                // so here check the children row return
                ptr = make_nullable(ptr, shared_state.input_num_rows == 0);
                columns[i] = ptr->assume_mutable();
            }
        }
    }

    block->set_columns(std::move(columns));
    *eos = true;
    return Status::OK();
}

AggSourceOperatorX::AggSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                       const DescriptorTbl& descs)
        : Base(pool, tnode, operator_id, descs),
          _needs_finalize(tnode.agg_node.need_finalize),
          _without_key(tnode.agg_node.grouping_exprs.empty()) {}

Status AggSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block, bool* eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    RETURN_IF_ERROR(local_state._executor.get_result(state, block, eos));
    local_state.make_nullable_output_key(block);
    // dispose the having clause, should not be execute in prestreaming agg
    RETURN_IF_ERROR(vectorized::VExprContext::filter_block(_conjuncts, block, block->columns()));
    local_state.do_agg_limit(block, eos);
    return Status::OK();
}

void AggLocalState::do_agg_limit(vectorized::Block* block, bool* eos) {
    if (_shared_state->reach_limit) {
        if (_shared_state->do_sort_limit && _shared_state->do_limit_filter(block, block->rows())) {
            vectorized::Block::filter_block_internal(block, _shared_state->need_computes);
            if (auto rows = block->rows()) {
                _num_rows_returned += rows;
                COUNTER_UPDATE(_blocks_returned_counter, 1);
                COUNTER_SET(_rows_returned_counter, _num_rows_returned);
            }
        } else {
            reached_limit(block, eos);
        }
    } else {
        if (auto rows = block->rows()) {
            _num_rows_returned += rows;
            COUNTER_UPDATE(_blocks_returned_counter, 1);
            COUNTER_SET(_rows_returned_counter, _num_rows_returned);
        }
    }
}

void AggLocalState::make_nullable_output_key(vectorized::Block* block) {
    if (block->rows() != 0) {
        for (auto cid : _shared_state->make_nullable_keys) {
            block->get_by_position(cid).column = make_nullable(block->get_by_position(cid).column);
            block->get_by_position(cid).type = make_nullable(block->get_by_position(cid).type);
        }
    }
}

template <bool limit>
Status AggLocalState::merge_with_serialized_key_helper(vectorized::Block* block) {
    SCOPED_TIMER(_merge_timer);

    size_t key_size = Base::_shared_state->probe_expr_ctxs.size();
    vectorized::ColumnRawPtrs key_columns(key_size);

    for (size_t i = 0; i < key_size; ++i) {
        key_columns[i] = block->get_by_position(i).column.get();
    }

    int rows = block->rows();
    if (_places.size() < rows) {
        _places.resize(rows);
    }

    if constexpr (limit) {
        _find_in_hash_table(_places.data(), key_columns, rows);

        for (int i = 0; i < Base::_shared_state->aggregate_evaluators.size(); ++i) {
            if (Base::_shared_state->aggregate_evaluators[i]->is_merge()) {
                int col_id = AggSharedState::get_slot_column_id(
                        Base::_shared_state->aggregate_evaluators[i]);
                auto column = block->get_by_position(col_id).column;
                if (column->is_nullable()) {
                    column = ((vectorized::ColumnNullable*)column.get())->get_nested_column_ptr();
                }

                size_t buffer_size =
                        Base::_shared_state->aggregate_evaluators[i]->function()->size_of_data() *
                        rows;
                if (_deserialize_buffer.size() < buffer_size) {
                    _deserialize_buffer.resize(buffer_size);
                }

                {
                    SCOPED_TIMER(_deserialize_data_timer);
                    Base::_shared_state->aggregate_evaluators[i]
                            ->function()
                            ->deserialize_and_merge_vec_selected(
                                    _places.data(), _shared_state->offsets_of_aggregate_states[i],
                                    _deserialize_buffer.data(), column.get(),
                                    _shared_state->agg_arena_pool.get(), rows);
                }
            } else {
                RETURN_IF_ERROR(
                        Base::_shared_state->aggregate_evaluators[i]->execute_batch_add_selected(
                                block, _shared_state->offsets_of_aggregate_states[i],
                                _places.data(), _shared_state->agg_arena_pool.get()));
            }
        }
    } else {
        _emplace_into_hash_table(_places.data(), key_columns, rows);

        for (int i = 0; i < Base::_shared_state->aggregate_evaluators.size(); ++i) {
            int col_id = 0;
            col_id = Base::_shared_state->probe_expr_ctxs.size() + i;
            auto column = block->get_by_position(col_id).column;
            if (column->is_nullable()) {
                column = ((vectorized::ColumnNullable*)column.get())->get_nested_column_ptr();
            }

            size_t buffer_size =
                    Base::_shared_state->aggregate_evaluators[i]->function()->size_of_data() * rows;
            if (_deserialize_buffer.size() < buffer_size) {
                _deserialize_buffer.resize(buffer_size);
            }

            {
                SCOPED_TIMER(_deserialize_data_timer);
                Base::_shared_state->aggregate_evaluators[i]->function()->deserialize_and_merge_vec(
                        _places.data(), _shared_state->offsets_of_aggregate_states[i],
                        _deserialize_buffer.data(), column.get(),
                        _shared_state->agg_arena_pool.get(), rows);
            }
        }
    }

    return Status::OK();
}
template <bool limit>
Status AggSourceOperatorX::merge_with_serialized_key_helper(RuntimeState* state,
                                                            vectorized::Block* block) {
    auto& local_state = get_local_state(state);
    return local_state.merge_with_serialized_key_helper<limit>(block);
}
template Status AggSourceOperatorX::merge_with_serialized_key_helper<true>(
        RuntimeState* state, vectorized::Block* block);
template Status AggSourceOperatorX::merge_with_serialized_key_helper<false>(
        RuntimeState* state, vectorized::Block* block);

size_t AggLocalState::_get_hash_table_size() {
    return std::visit(
            vectorized::Overload {[&](std::monostate& arg) -> size_t {
                                      throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                                             "uninited hash table");
                                      return 0;
                                  },
                                  [&](auto& agg_method) { return agg_method.hash_table->size(); }},
            _shared_state->agg_data->method_variant);
}

void AggLocalState::_emplace_into_hash_table(vectorized::AggregateDataPtr* places,
                                             vectorized::ColumnRawPtrs& key_columns,
                                             size_t num_rows) {
    std::visit(vectorized::Overload {
                       [&](std::monostate& arg) -> void {
                           throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                       },
                       [&](auto& agg_method) -> void {
                           SCOPED_TIMER(_hash_table_compute_timer);
                           using HashMethodType = std::decay_t<decltype(agg_method)>;
                           using AggState = typename HashMethodType::State;
                           AggState state(key_columns);
                           agg_method.init_serialized_keys(key_columns, num_rows);

                           auto creator = [this](const auto& ctor, auto& key, auto& origin) {
                               HashMethodType::try_presis_key_and_origin(
                                       key, origin, *_shared_state->agg_arena_pool);
                               auto mapped =
                                       Base::_shared_state->aggregate_data_container->append_data(
                                               origin);
                               auto st = _create_agg_status(mapped);
                               if (!st) {
                                   throw Exception(st.code(), st.to_string());
                               }
                               ctor(key, mapped);
                           };

                           auto creator_for_null_key = [&](auto& mapped) {
                               mapped = _shared_state->agg_arena_pool->aligned_alloc(
                                       _shared_state->total_size_of_aggregate_states,
                                       _shared_state->align_aggregate_states);
                               auto st = _create_agg_status(mapped);
                               if (!st) {
                                   throw Exception(st.code(), st.to_string());
                               }
                           };

                           SCOPED_TIMER(_hash_table_emplace_timer);
                           for (size_t i = 0; i < num_rows; ++i) {
                               places[i] = agg_method.lazy_emplace(state, i, creator,
                                                                   creator_for_null_key);
                           }

                           COUNTER_UPDATE(_hash_table_input_counter, num_rows);
                       }},
               _shared_state->agg_data->method_variant);
}

void AggLocalState::_find_in_hash_table(vectorized::AggregateDataPtr* places,
                                        vectorized::ColumnRawPtrs& key_columns, size_t num_rows) {
    std::visit(vectorized::Overload {[&](std::monostate& arg) -> void {
                                         throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                                                "uninited hash table");
                                     },
                                     [&](auto& agg_method) -> void {
                                         using HashMethodType = std::decay_t<decltype(agg_method)>;
                                         using AggState = typename HashMethodType::State;
                                         AggState state(key_columns);
                                         agg_method.init_serialized_keys(key_columns, num_rows);

                                         /// For all rows.
                                         for (size_t i = 0; i < num_rows; ++i) {
                                             auto find_result = agg_method.find(state, i);

                                             if (find_result.is_found()) {
                                                 places[i] = find_result.get_mapped();
                                             } else {
                                                 places[i] = nullptr;
                                             }
                                         }
                                     }},
               _shared_state->agg_data->method_variant);
}

Status AggLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }

    vectorized::PODArray<vectorized::AggregateDataPtr> tmp_places;
    _places.swap(tmp_places);

    std::vector<char> tmp_deserialize_buffer;
    _deserialize_buffer.swap(tmp_deserialize_buffer);
    return Base::close(state);
}

} // namespace doris::pipeline
