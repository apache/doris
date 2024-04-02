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

#include <glog/logging.h>

#include "runtime/runtime_state.h"
#include "vec/aggregate_functions/aggregate_function_uniq.h"
#include "vec/columns/column.h"
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
    DCHECK(!_probe_expr_ctxs.empty());

    size_t key_size = _probe_expr_ctxs.size();
    ColumnRawPtrs key_columns(key_size);
    std::vector<int> result_idxs(key_size);
    {
        SCOPED_TIMER(_expr_timer);
        for (size_t i = 0; i < key_size; ++i) {
            int result_column_id = -1;
            RETURN_IF_ERROR(_probe_expr_ctxs[i]->execute(in_block, &result_column_id));
            in_block->get_by_position(result_column_id).column =
                    in_block->get_by_position(result_column_id)
                            .column->convert_to_full_column_if_const();
            key_columns[i] = in_block->get_by_position(result_column_id).column.get();
            result_idxs[i] = result_column_id;
        }
    }

    int rows = in_block->rows();
    _distinct_row.clear();
    _distinct_row.reserve(rows);

    RETURN_IF_CATCH_EXCEPTION(
            _emplace_into_hash_table_to_distinct(_distinct_row, key_columns, rows));
    // if get stop_emplace_flag = true, means have no need to emplace value into hash table
    // so return block directly and notice the column ref is 2, need deal with.
    SCOPED_TIMER(_insert_keys_to_column_timer);
    bool mem_reuse = _make_nullable_keys.empty() && out_block->mem_reuse();
    if (mem_reuse) {
        if (_stop_emplace_flag && !out_block->empty()) {
            // when out_block row >= batch_size, push it to data_queue, so when _stop_emplace_flag = true, maybe have some data in block
            // need output those data firstly
            for (int i = 0; i < rows; ++i) {
                _distinct_row.push_back(i);
            }
        }
        for (int i = 0; i < key_size; ++i) {
            auto output_column = out_block->get_by_position(i).column;
            if (_stop_emplace_flag && _distinct_row.empty()) {
                // means it's streaming and out_block have no data.
                // swap the column directly, so not insert data again. and solve Check failed: d.column->use_count() == 1 (2 vs. 1)
                out_block->replace_by_position(i, key_columns[i]->assume_mutable());
                in_block->replace_by_position(result_idxs[i], output_column);
            } else {
                auto dst = output_column->assume_mutable();
                key_columns[i]->append_data_by_selector(dst, _distinct_row);
            }
        }
    } else {
        ColumnsWithTypeAndName columns_with_schema;
        for (int i = 0; i < key_size; ++i) {
            if (_stop_emplace_flag) {
                columns_with_schema.emplace_back(key_columns[i]->assume_mutable(),
                                                 _probe_expr_ctxs[i]->root()->data_type(),
                                                 _probe_expr_ctxs[i]->root()->expr_name());
            } else {
                auto distinct_column = key_columns[i]->clone_empty();
                key_columns[i]->append_data_by_selector(distinct_column, _distinct_row);
                columns_with_schema.emplace_back(std::move(distinct_column),
                                                 _probe_expr_ctxs[i]->root()->data_type(),
                                                 _probe_expr_ctxs[i]->root()->expr_name());
            }
        }
        out_block->swap(Block(columns_with_schema));
        if (_stop_emplace_flag) {
            in_block->clear(); // clear the column ref with stop_emplace_flag = true
        }
    }
    return Status::OK();
}

void DistinctAggregationNode::_emplace_into_hash_table_to_distinct(IColumn::Selector& distinct_row,
                                                                   ColumnRawPtrs& key_columns,
                                                                   const size_t num_rows) {
    SCOPED_TIMER(_exec_timer);
    std::visit(
            [&](auto&& agg_method) -> void {
                SCOPED_TIMER(_hash_table_compute_timer);
                using HashMethodType = std::decay_t<decltype(agg_method)>;
                using AggState = typename HashMethodType::State;
                auto& hash_tbl = *agg_method.hash_table;
                if (is_streaming_preagg() && hash_tbl.add_elem_size_overflow(num_rows)) {
                    if (!_should_expand_preagg_hash_tables()) {
                        _stop_emplace_flag = true;
                        return;
                    }
                }
                AggState state(key_columns);
                agg_method.init_serialized_keys(key_columns, num_rows);

                size_t row = 0;
                auto creator = [&](const auto& ctor, auto& key, auto& origin) {
                    HashMethodType::try_presis_key(key, origin, *_agg_arena_pool);
                    ctor(key, dummy_mapped_data);
                    distinct_row.push_back_without_reserve(row);
                };

                auto creator_for_null_key = [&](auto& mapped) {
                    mapped = dummy_mapped_data;
                    distinct_row.push_back_without_reserve(row);
                };

                SCOPED_TIMER(_hash_table_emplace_timer);
                for (; row < num_rows; ++row) {
                    agg_method.lazy_emplace(state, row, creator, creator_for_null_key);
                }

                COUNTER_UPDATE(_hash_table_input_counter, num_rows);
            },
            _agg_data->method_variant);
}

} // namespace doris::vectorized
