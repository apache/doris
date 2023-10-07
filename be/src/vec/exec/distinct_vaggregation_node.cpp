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

#include "vec/exec/distinct_vaggregation_node.h"

#include "runtime/runtime_state.h"
#include "vec/aggregate_functions/aggregate_function_uniq.h"
#include "vec/exec/vaggregation_node.h"

namespace doris {
class ObjectPool;
} // namespace doris

namespace doris::vectorized {

DistinctAggregationNode::DistinctAggregationNode(ObjectPool* pool, const TPlanNode& tnode,
                                                 const DescriptorTbl& descs)
        : AggregationNode(pool, tnode, descs) {
    dummy_mapped_data = pool->add(new char('A'));
}

Status DistinctAggregationNode::_distinct_pre_agg_with_serialized_key(
        doris::vectorized::Block* in_block, doris::vectorized::Block* out_block) {
    SCOPED_TIMER(_build_timer);
    DCHECK(!_probe_expr_ctxs.empty());

    size_t key_size = _probe_expr_ctxs.size();
    ColumnRawPtrs key_columns(key_size);
    {
        SCOPED_TIMER(_expr_timer);
        for (size_t i = 0; i < key_size; ++i) {
            int result_column_id = -1;
            RETURN_IF_ERROR(_probe_expr_ctxs[i]->execute(in_block, &result_column_id));
            in_block->get_by_position(result_column_id).column =
                    in_block->get_by_position(result_column_id)
                            .column->convert_to_full_column_if_const();
            key_columns[i] = in_block->get_by_position(result_column_id).column.get();
        }
    }

    int rows = in_block->rows();
    _distinct_row.clear();
    _distinct_row.reserve(rows);

    RETURN_IF_CATCH_EXCEPTION(
            _emplace_into_hash_table_to_distinct(_distinct_row, key_columns, rows));

    bool mem_reuse = _make_nullable_keys.empty() && out_block->mem_reuse();
    if (mem_reuse) {
        for (int i = 0; i < key_size; ++i) {
            auto dst = out_block->get_by_position(i).column->assume_mutable();
            key_columns[i]->append_data_by_selector(dst, _distinct_row);
        }
    } else {
        ColumnsWithTypeAndName columns_with_schema;
        for (int i = 0; i < key_size; ++i) {
            auto distinct_column = key_columns[i]->clone_empty();
            key_columns[i]->append_data_by_selector(distinct_column, _distinct_row);
            columns_with_schema.emplace_back(std::move(distinct_column),
                                             _probe_expr_ctxs[i]->root()->data_type(),
                                             _probe_expr_ctxs[i]->root()->expr_name());
        }
        out_block->swap(Block(columns_with_schema));
    }
    return Status::OK();
}

void DistinctAggregationNode::_emplace_into_hash_table_to_distinct(IColumn::Selector& distinct_row,
                                                                   ColumnRawPtrs& key_columns,
                                                                   const size_t num_rows) {
    std::visit(
            [&](auto&& agg_method) -> void {
                SCOPED_TIMER(_hash_table_compute_timer);
                using HashMethodType = std::decay_t<decltype(agg_method)>;
                using HashTableType = std::decay_t<decltype(agg_method.data)>;
                using AggState = typename HashMethodType::State;
                AggState state(key_columns, _probe_key_sz, nullptr);
                _pre_serialize_key_if_need(state, agg_method, key_columns, num_rows);

                if constexpr (HashTableTraits<HashTableType>::is_phmap) {
                    const auto& keys = state.get_keys();
                    if (_hash_values.size() < num_rows) {
                        _hash_values.resize(num_rows);
                    }

                    for (size_t i = 0; i < num_rows; ++i) {
                        _hash_values[i] = agg_method.data.hash(keys[i]);
                    }
                    SCOPED_TIMER(_hash_table_emplace_timer);
                    for (size_t i = 0; i < num_rows; ++i) {
                        if (LIKELY(i + HASH_MAP_PREFETCH_DIST < num_rows)) {
                            agg_method.data.prefetch_by_hash(
                                    _hash_values[i + HASH_MAP_PREFETCH_DIST]);
                        }
                        auto result = state.emplace_with_key(
                                agg_method.data, state.pack_key_holder(keys[i], *_agg_arena_pool),
                                _hash_values[i], i);
                        if (result.is_inserted()) {
                            distinct_row.push_back(i);
                        }
                    }
                } else {
                    SCOPED_TIMER(_hash_table_emplace_timer);
                    for (size_t i = 0; i < num_rows; ++i) {
                        auto result = state.emplace_key(agg_method.data, i, *_agg_arena_pool);
                        if (result.is_inserted()) {
                            result.set_mapped(dummy_mapped_data);
                            distinct_row.push_back(i);
                        }
                    }
                }
                COUNTER_UPDATE(_hash_table_input_counter, num_rows);
            },
            _agg_data->method_variant);
}

} // namespace doris::vectorized
