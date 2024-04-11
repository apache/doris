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
#include "operator.h"
#include "pipeline/exec/aggregation_operator_helper.h"
#include "pipeline/pipeline_x/operator.h"
#include "vec/exec/vaggregation_node.h"
namespace doris {

namespace pipeline {
template <typename Derived, typename OperatorX>
class AggSourceLocalStateHelper : public AggLocalStateHelper<Derived, OperatorX> {
public:
    ~AggSourceLocalStateHelper() = default;
    AggSourceLocalStateHelper(Derived* derived)
            : AggLocalStateHelper<Derived, OperatorX>(derived) {}
    using AggLocalStateHelper<Derived, OperatorX>::_get_hash_table_size;
    using AggLocalStateHelper<Derived, OperatorX>::_find_in_hash_table;
    using AggLocalStateHelper<Derived, OperatorX>::_emplace_into_hash_table;
    using AggLocalStateHelper<Derived, OperatorX>::_shared_state;
    using AggLocalStateHelper<Derived, OperatorX>::_derived;
    using AggLocalStateHelper<Derived, OperatorX>::_operator;
    using AggLocalStateHelper<Derived, OperatorX>::_create_agg_status;
    using AggLocalStateHelper<Derived, OperatorX>::_destroy_agg_status;
    using AggLocalStateHelper<Derived, OperatorX>::_init_hash_method;

    Status _get_without_key_result(RuntimeState* state, vectorized::Block* block, bool* eos) {
        auto& agg_data = _shared_state()->agg_data;
        auto& aggregate_evaluators = _shared_state()->aggregate_evaluators;
        DCHECK(agg_data->without_key != nullptr);
        block->clear();

        auto& p = _operator();
        *block = vectorized::VectorizedUtils::create_empty_columnswithtypename(p.row_descriptor());
        int agg_size = aggregate_evaluators.size();

        vectorized::MutableColumns columns(agg_size);
        std::vector<vectorized::DataTypePtr> data_types(agg_size);
        for (int i = 0; i < aggregate_evaluators.size(); ++i) {
            data_types[i] = aggregate_evaluators[i]->function()->get_return_type();
            columns[i] = data_types[i]->create_column();
        }

        for (int i = 0; i < aggregate_evaluators.size(); ++i) {
            auto* column = columns[i].get();
            aggregate_evaluators[i]->insert_result_info(
                    agg_data->without_key + _shared_state()->offsets_of_aggregate_states[i],
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
                                p.node_id(), column_type->get_name(), data_types[i]->get_name());
                    }
                }

                if (column_type->is_nullable() && !data_types[i]->is_nullable()) {
                    vectorized::ColumnPtr ptr = std::move(columns[i]);
                    // unless `count`, other aggregate function dispose empty set should be null
                    // so here check the children row return
                    ptr = make_nullable(ptr, _shared_state()->input_num_rows == 0);
                    columns[i] = ptr->assume_mutable();
                }
            }
        }

        block->set_columns(std::move(columns));
        *eos = true;
        return Status::OK();
    }

    Status _get_with_serialized_key_result(RuntimeState* state, vectorized::Block* block,
                                           bool* eos) {
        auto& p = _operator();
        auto& _values = _derived()->_values;
        auto& aggregate_data_container = _shared_state()->aggregate_data_container;
        auto& aggregate_evaluators = _shared_state()->aggregate_evaluators;
        auto& agg_data = _shared_state()->agg_data;
        // non-nullable column(id in `_make_nullable_keys`) will be converted to nullable.
        bool mem_reuse = p._make_nullable_keys.empty() && block->mem_reuse();

        auto columns_with_schema =
                vectorized::VectorizedUtils::create_columns_with_type_and_name(p.row_descriptor());
        int key_size = _shared_state()->probe_expr_ctxs.size();

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

        SCOPED_TIMER(_derived()->_get_results_timer);
        std::visit(
                [&](auto&& agg_method) -> void {
                    auto& data = *agg_method.hash_table;
                    agg_method.init_iterator();
                    const auto size = std::min(data.size(), size_t(state->batch_size()));
                    using KeyType = std::decay_t<decltype(agg_method.iterator->get_first())>;
                    std::vector<KeyType> keys(size);
                    if (_values.size() < size) {
                        _values.resize(size);
                    }

                    size_t num_rows = 0;
                    aggregate_data_container->init_once();
                    auto& iter = aggregate_data_container->iterator;

                    {
                        SCOPED_TIMER(_derived()->_hash_table_iterate_timer);
                        while (iter != aggregate_data_container->end() &&
                               num_rows < state->batch_size()) {
                            keys[num_rows] = iter.template get_key<KeyType>();
                            _values[num_rows] = iter.get_aggregate_data();
                            ++iter;
                            ++num_rows;
                        }
                    }

                    {
                        SCOPED_TIMER(_derived()->_insert_keys_to_column_timer);
                        agg_method.insert_keys_into_columns(keys, key_columns, num_rows);
                    }

                    for (size_t i = 0; i < aggregate_evaluators.size(); ++i) {
                        aggregate_evaluators[i]->insert_result_info_vec(
                                _values, _shared_state()->offsets_of_aggregate_states[i],
                                value_columns[i].get(), num_rows);
                    }

                    if (iter == aggregate_data_container->end()) {
                        if (agg_method.hash_table->has_null_key_data()) {
                            // only one key of group by support wrap null key
                            // here need additional processing logic on the null key / value
                            DCHECK(key_columns.size() == 1);
                            DCHECK(key_columns[0]->is_nullable());
                            if (key_columns[0]->size() < state->batch_size()) {
                                key_columns[0]->insert_data(nullptr, 0);
                                auto mapped = agg_method.hash_table->template get_null_key_data<
                                        vectorized::AggregateDataPtr>();
                                for (size_t i = 0; i < aggregate_evaluators.size(); ++i) {
                                    aggregate_evaluators[i]->insert_result_info(
                                            mapped +
                                                    _shared_state()->offsets_of_aggregate_states[i],
                                            value_columns[i].get());
                                }
                                *eos = true;
                            }
                        } else {
                            *eos = true;
                        }
                    }
                },
                agg_data->method_variant);

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

    Status _serialize_without_key(RuntimeState* state, vectorized::Block* block, bool* eos) {
        // 1. `child(0)->rows_returned() == 0` mean not data from child
        // in level two aggregation node should return NULL result
        //    level one aggregation node set `eos = true` return directly
        auto& agg_data = _shared_state()->agg_data;
        auto& aggregate_evaluators = _shared_state()->aggregate_evaluators;
        SCOPED_TIMER(_derived()->_serialize_result_timer);
        if (UNLIKELY(_shared_state()->input_num_rows == 0)) {
            *eos = true;
            return Status::OK();
        }
        block->clear();

        DCHECK(agg_data->without_key != nullptr);
        int agg_size = aggregate_evaluators.size();

        vectorized::MutableColumns value_columns(agg_size);
        std::vector<vectorized::DataTypePtr> data_types(agg_size);
        // will serialize data to string column
        for (int i = 0; i < aggregate_evaluators.size(); ++i) {
            data_types[i] = aggregate_evaluators[i]->function()->get_serialized_type();
            value_columns[i] = aggregate_evaluators[i]->function()->create_serialize_column();
        }

        for (int i = 0; i < aggregate_evaluators.size(); ++i) {
            aggregate_evaluators[i]->function()->serialize_without_key_to_column(
                    agg_data->without_key + _shared_state()->offsets_of_aggregate_states[i],
                    *value_columns[i]);
        }

        {
            vectorized::ColumnsWithTypeAndName data_with_schema;
            for (int i = 0; i < aggregate_evaluators.size(); ++i) {
                vectorized::ColumnWithTypeAndName column_with_schema = {nullptr, data_types[i], ""};
                data_with_schema.push_back(std::move(column_with_schema));
            }
            *block = vectorized::Block(data_with_schema);
        }

        block->set_columns(std::move(value_columns));
        *eos = true;
        return Status::OK();
    }

    Status _serialize_with_serialized_key_result(RuntimeState* state, vectorized::Block* block,
                                                 bool* eos) {
        SCOPED_TIMER(_derived()->_serialize_result_timer);
        auto& p = _operator();
        auto& probe_expr_ctxs = _shared_state()->probe_expr_ctxs;
        auto& aggregate_data_container = _shared_state()->aggregate_data_container;
        auto& aggregate_evaluators = _shared_state()->aggregate_evaluators;
        auto& agg_data = _shared_state()->agg_data;
        auto& _values = _derived()->_values;

        int key_size = probe_expr_ctxs.size();
        int agg_size = aggregate_evaluators.size();
        vectorized::MutableColumns value_columns(agg_size);
        vectorized::DataTypes value_data_types(agg_size);

        // non-nullable column(id in `_make_nullable_keys`) will be converted to nullable.
        bool mem_reuse = p._make_nullable_keys.empty() && block->mem_reuse();

        vectorized::MutableColumns key_columns;
        for (int i = 0; i < key_size; ++i) {
            if (mem_reuse) {
                key_columns.emplace_back(std::move(*block->get_by_position(i).column).mutate());
            } else {
                key_columns.emplace_back(probe_expr_ctxs[i]->root()->data_type()->create_column());
            }
        }

        SCOPED_TIMER(_derived()->_get_results_timer);
        std::visit(
                [&](auto&& agg_method) -> void {
                    agg_method.init_iterator();
                    auto& data = *agg_method.hash_table;
                    const auto size = std::min(data.size(), size_t(state->batch_size()));
                    using KeyType = std::decay_t<decltype(agg_method.iterator->get_first())>;
                    std::vector<KeyType> keys(size);
                    if (_values.size() < size + 1) {
                        _values.resize(size + 1);
                    }

                    size_t num_rows = 0;
                    aggregate_data_container->init_once();
                    auto& iter = aggregate_data_container->iterator;

                    {
                        SCOPED_TIMER(_derived()->_hash_table_iterate_timer);
                        while (iter != aggregate_data_container->end() &&
                               num_rows < state->batch_size()) {
                            keys[num_rows] = iter.template get_key<KeyType>();
                            _values[num_rows] = iter.get_aggregate_data();
                            ++iter;
                            ++num_rows;
                        }
                    }

                    {
                        SCOPED_TIMER(_derived()->_insert_keys_to_column_timer);
                        agg_method.insert_keys_into_columns(keys, key_columns, num_rows);
                    }

                    if (iter == aggregate_data_container->end()) {
                        if (agg_method.hash_table->has_null_key_data()) {
                            // only one key of group by support wrap null key
                            // here need additional processing logic on the null key / value
                            DCHECK(key_columns.size() == 1);
                            DCHECK(key_columns[0]->is_nullable());
                            if (agg_method.hash_table->has_null_key_data()) {
                                key_columns[0]->insert_data(nullptr, 0);
                                _values[num_rows] =
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
                        SCOPED_TIMER(_derived()->_serialize_data_timer);
                        for (size_t i = 0; i < aggregate_evaluators.size(); ++i) {
                            value_data_types[i] =
                                    aggregate_evaluators[i]->function()->get_serialized_type();
                            if (mem_reuse) {
                                value_columns[i] =
                                        std::move(*block->get_by_position(i + key_size).column)
                                                .mutate();
                            } else {
                                value_columns[i] = aggregate_evaluators[i]
                                                           ->function()
                                                           ->create_serialize_column();
                            }
                            aggregate_evaluators[i]->function()->serialize_to_column(
                                    _values, _shared_state()->offsets_of_aggregate_states[i],
                                    value_columns[i], num_rows);
                        }
                    }
                },
                agg_data->method_variant);

        if (!mem_reuse) {
            vectorized::ColumnsWithTypeAndName columns_with_schema;
            for (int i = 0; i < key_size; ++i) {
                columns_with_schema.emplace_back(std::move(key_columns[i]),
                                                 probe_expr_ctxs[i]->root()->data_type(),
                                                 probe_expr_ctxs[i]->root()->expr_name());
            }
            for (int i = 0; i < agg_size; ++i) {
                columns_with_schema.emplace_back(std::move(value_columns[i]), value_data_types[i],
                                                 "");
            }
            *block = vectorized::Block(columns_with_schema);
        }

        return Status::OK();
    }
};
}; // namespace pipeline

}; // namespace doris
