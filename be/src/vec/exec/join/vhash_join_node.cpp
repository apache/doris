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

#include "vec/exec/join/vhash_join_node.h"

#include "gen_cpp/PlanNodes_types.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"
#include "vec/core/materialize_block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

std::variant<std::false_type, std::true_type>
static inline make_bool_variant(bool condition) {
    if (condition) {
        return std::true_type{};
    } else {
        return std::false_type{};
    }
}

using ProfileCounter = RuntimeProfile::Counter;
template <class HashTableContext, bool ignore_null, bool build_unique>
struct ProcessHashTableBuild {
    ProcessHashTableBuild(int rows, Block& acquired_block, ColumnRawPtrs& build_raw_ptrs,
                          HashJoinNode* join_node, int batch_size, uint8_t offset)
            : _rows(rows),
              _skip_rows(0),
              _acquired_block(acquired_block),
              _build_raw_ptrs(build_raw_ptrs),
              _join_node(join_node),
              _batch_size(batch_size),
              _offset(offset) {}

    Status operator()(HashTableContext& hash_table_ctx, ConstNullMapPtr null_map,
                      bool has_runtime_filter) {
        using KeyGetter = typename HashTableContext::State;
        using Mapped = typename HashTableContext::Mapped;
        int64_t old_bucket_bytes = hash_table_ctx.hash_table.get_buffer_size_in_bytes();

        Defer defer {[&]() {
            int64_t bucket_size = hash_table_ctx.hash_table.get_buffer_size_in_cells();
            int64_t bucket_bytes = hash_table_ctx.hash_table.get_buffer_size_in_bytes();
            _join_node->_hash_table_mem_tracker->consume(bucket_bytes - old_bucket_bytes);
            _join_node->_mem_used += bucket_bytes - old_bucket_bytes;
            COUNTER_SET(_join_node->_build_buckets_counter, bucket_size);
        }};

        KeyGetter key_getter(_build_raw_ptrs, _join_node->_build_key_sz, nullptr);

        SCOPED_TIMER(_join_node->_build_table_insert_timer);
        hash_table_ctx.hash_table.reset_resize_timer();

        vector<int>& inserted_rows = _join_node->_inserted_rows[&_acquired_block];
        if (has_runtime_filter) {
            inserted_rows.reserve(_batch_size);
        }

        for (size_t k = 0; k < _rows; ++k) {
            if constexpr (ignore_null) {
                if ((*null_map)[k]) {
                    continue;
                }
            }

            auto emplace_result =
                    key_getter.emplace_key(hash_table_ctx.hash_table, k, _join_node->_arena);
            if (k + 1 < _rows) {
                key_getter.prefetch(hash_table_ctx.hash_table, k + 1, _join_node->_arena);
            }

            if (emplace_result.is_inserted()) {
                new (&emplace_result.get_mapped()) Mapped({k, _offset});
                if (has_runtime_filter) {
                    inserted_rows.push_back(k);
                }
            } else {
                if constexpr (!build_unique) {
                    /// The first element of the list is stored in the value of the hash table, the rest in the pool.
                    emplace_result.get_mapped().insert({k, _offset}, _join_node->_arena);
                    if (has_runtime_filter) {
                        inserted_rows.push_back(k);
                    }
                } else {
                    _skip_rows++;
                }
            }
        }

        COUNTER_UPDATE(_join_node->_build_table_expanse_timer,
                       hash_table_ctx.hash_table.get_resize_timer_value());

        return Status::OK();
    }

private:
    const int _rows;
    int _skip_rows;
    Block& _acquired_block;
    ColumnRawPtrs& _build_raw_ptrs;
    HashJoinNode* _join_node;
    int _batch_size;
    uint8_t _offset;
};

template <class HashTableContext>
struct ProcessRuntimeFilterBuild {
    ProcessRuntimeFilterBuild(HashJoinNode* join_node) : _join_node(join_node) {}

    Status operator()(RuntimeState* state, HashTableContext& hash_table_ctx) {
        if (_join_node->_runtime_filter_descs.empty()) {
            return Status::OK();
        }
        VRuntimeFilterSlots* runtime_filter_slots =
                new VRuntimeFilterSlots(_join_node->_probe_expr_ctxs, _join_node->_build_expr_ctxs,
                                        _join_node->_runtime_filter_descs);

        RETURN_IF_ERROR(runtime_filter_slots->init(state, hash_table_ctx.hash_table.get_size()));

        if (!runtime_filter_slots->empty() && !_join_node->_inserted_rows.empty()) {
            {
                SCOPED_TIMER(_join_node->_push_compute_timer);
                runtime_filter_slots->insert(_join_node->_inserted_rows);
            }
        }
        {
            SCOPED_TIMER(_join_node->_push_down_timer);
            runtime_filter_slots->publish();
        }

        return Status::OK();
    }

private:
    HashJoinNode* _join_node;
};

template <class HashTableContext, class JoinOpType, bool ignore_null>
struct ProcessHashTableProbe {
    ProcessHashTableProbe(HashJoinNode* join_node, int batch_size, int probe_rows)
            : _join_node(join_node),
              _batch_size(batch_size),
              _probe_rows(probe_rows),
              _build_blocks(join_node->_build_blocks),
              _probe_block(join_node->_probe_block),
              _probe_index(join_node->_probe_index),
              _probe_raw_ptrs(join_node->_probe_columns),
              _items_counts(join_node->_items_counts),
              _build_block_offsets(join_node->_build_block_offsets),
              _build_block_rows(join_node->_build_block_rows),
              _rows_returned_counter(join_node->_rows_returned_counter),
              _search_hashtable_timer(join_node->_search_hashtable_timer),
              _build_side_output_timer(join_node->_build_side_output_timer),
              _probe_side_output_timer(join_node->_probe_side_output_timer) {}

    // output build side result column
    void build_side_output_column(MutableColumns& mcol, int column_offset, int column_length, int size) {
        constexpr auto is_semi_anti_join = JoinOpType::value == TJoinOp::RIGHT_ANTI_JOIN ||
                                           JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN ||
                                           JoinOpType::value == TJoinOp::LEFT_ANTI_JOIN ||
                                           JoinOpType::value == TJoinOp::LEFT_SEMI_JOIN;

        constexpr auto probe_all = JoinOpType::value == TJoinOp::LEFT_OUTER_JOIN ||
                                   JoinOpType::value == TJoinOp::FULL_OUTER_JOIN;

        if constexpr (!is_semi_anti_join) {
            if (_build_blocks.size() == 1) {
                for (int i = 0; i < column_length; i++) {
                    auto& column = *_build_blocks[0].get_by_position(i).column;
                    mcol[i + column_offset]->insert_indices_from(column,
                            _build_block_rows.data(), _build_block_rows.data() + size);
                }
            } else {
                for (int i = 0; i < column_length; i++) {
                    for (int j = 0; j < size; j++) {
                        if constexpr (probe_all) {
                            if (_build_block_offsets[j] == -1) {
                                DCHECK(mcol[i + column_offset]->is_nullable());
                                assert_cast<ColumnNullable *>(mcol[i + column_offset].get())->insert_join_null_data();
                            } else {
                                auto& column = *_build_blocks[_build_block_offsets[j]].get_by_position(i).column;
                                mcol[i + column_offset]->insert_from(column, _build_block_rows[j]);
                            }
                        } else {
                            auto& column = *_build_blocks[_build_block_offsets[j]].get_by_position(i).column;
                            mcol[i + column_offset]->insert_from(column, _build_block_rows[j]);
                        }
                    }
                }
            }
        }
    }

    // output probe side result column
    void probe_side_output_column(MutableColumns& mcol, int column_length, int size) {
        for (int i = 0; i < column_length; ++i) {
            auto& column = _probe_block.get_by_position(i).column;
            column->replicate(&_items_counts[0], size, *mcol[i]);
        }
    }
    // Only process the join with no other join conjunt, because of no other join conjunt
    // the output block struct is same with mutable block. we can do more opt on it and simplify
    // the logic of probe
    // TODO: opt the visited here to reduce the size of hash table
    Status do_process(HashTableContext& hash_table_ctx, ConstNullMapPtr null_map,
                      MutableBlock& mutable_block, Block* output_block) {
        using KeyGetter = typename HashTableContext::State;
        using Mapped = typename HashTableContext::Mapped;

        int right_col_idx = _join_node->_is_right_semi_anti ? 0 :
                _join_node->_left_table_data_types.size();
        int right_col_len = _join_node->_right_table_data_types.size();

        KeyGetter key_getter(_probe_raw_ptrs, _join_node->_probe_key_sz, nullptr);
        auto& mcol = mutable_block.mutable_columns();
        int current_offset = 0;

        _items_counts.resize(_probe_rows);
        _build_block_offsets.resize(_batch_size);
        _build_block_rows.resize(_batch_size);
        memset(_items_counts.data(), 0, sizeof(uint32_t) * _probe_rows);

        constexpr auto need_to_set_visited = JoinOpType::value == TJoinOp::RIGHT_ANTI_JOIN ||
                                       JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN ||
                                       JoinOpType::value == TJoinOp::RIGHT_OUTER_JOIN ||
                                       JoinOpType::value == TJoinOp::FULL_OUTER_JOIN;

        constexpr auto is_right_semi_anti_join = JoinOpType::value == TJoinOp::RIGHT_ANTI_JOIN ||
                                            JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN;

        constexpr auto probe_all = JoinOpType::value == TJoinOp::LEFT_OUTER_JOIN ||
                                     JoinOpType::value == TJoinOp::FULL_OUTER_JOIN;

        {
            SCOPED_TIMER(_search_hashtable_timer);
            for (; _probe_index < _probe_rows;) {
                if constexpr (ignore_null) {
                    if ((*null_map)[_probe_index]) {
                        _items_counts[_probe_index++] = (uint32_t)0;
                        continue;
                    }
                }
                int last_offset = current_offset;
                auto find_result = (*null_map)[_probe_index]
                                    ? decltype(key_getter.find_key(hash_table_ctx.hash_table, _probe_index,
                                                                    _arena)) {nullptr, false}
                                    : key_getter.find_key(hash_table_ctx.hash_table, _probe_index, _arena);

                if constexpr (JoinOpType::value == TJoinOp::LEFT_ANTI_JOIN) {
                    if (!find_result.is_found()) {
                        ++current_offset;
                    }
                } else if constexpr (JoinOpType::value == TJoinOp::LEFT_SEMI_JOIN) {
                    if (find_result.is_found()) {
                        ++current_offset;
                    }
                } else {
                    if (find_result.is_found()) {
                        auto& mapped = find_result.get_mapped();
                        // TODO: Iterators are currently considered to be a heavy operation and have a certain impact on performance.
                        // We should rethink whether to use this iterator mode in the future. Now just opt the one row case
                        if (mapped.get_row_count() == 1) {
                            if constexpr (need_to_set_visited)
                                mapped.visited = true;

                            if constexpr (!is_right_semi_anti_join) {
                                _build_block_offsets[current_offset] = mapped.block_offset;
                                _build_block_rows[current_offset] = mapped.row_num;
                                ++current_offset;
                            }
                        } else {
                            // prefetch is more useful while matching to multiple rows
                            if (_probe_index + 2 < _probe_rows)
                                key_getter.prefetch(hash_table_ctx.hash_table, _probe_index + 2, _arena);

                            for (auto it = mapped.begin(); it.ok(); ++it) {
                                if constexpr (!is_right_semi_anti_join) {
                                    if (current_offset < _batch_size) {
                                        _build_block_offsets[current_offset] = it->block_offset;
                                        _build_block_rows[current_offset] = it->row_num;
                                    } else {
                                        _build_block_offsets.emplace_back(it->block_offset);
                                        _build_block_rows.emplace_back(it->row_num);
                                    }
                                    ++current_offset;
                                }
                                if constexpr (need_to_set_visited)
                                    it->visited = true;
                            }
                        }
                    } else {
                        if constexpr (probe_all) {
                            // only full outer / left outer need insert the data of right table
                            _build_block_offsets[current_offset] = -1;
                            _build_block_rows[current_offset] = -1;
                            ++current_offset;
                        }
                    }
                }

                _items_counts[_probe_index++] = (uint32_t)(current_offset - last_offset);
                if (current_offset >= _batch_size) {
                    break;
                }
            }
        }

        {
            SCOPED_TIMER(_build_side_output_timer);
            build_side_output_column(mcol, right_col_idx, right_col_len, current_offset);
        }

        {
            SCOPED_TIMER(_probe_side_output_timer);
            probe_side_output_column(mcol, right_col_idx, current_offset);
        }

        output_block->swap(mutable_block.to_block());

        return Status::OK();
    }
    // In the presence of other join conjunt, the process of join become more complicated.
    // each matching join column need to be processed by other join conjunt. so the sturct of mutable block
    // and output block may be different
    // The output result is determined by the other join conjunt result and same_to_prev struct
    Status do_process_with_other_join_conjunts(HashTableContext& hash_table_ctx,
                                               ConstNullMapPtr null_map,
                                               MutableBlock& mutable_block, Block* output_block) {
        using KeyGetter = typename HashTableContext::State;
        using Mapped = typename HashTableContext::Mapped;
        KeyGetter key_getter(_probe_raw_ptrs, _join_node->_probe_key_sz, nullptr);

        int right_col_idx = _join_node->_left_table_data_types.size();
        int right_col_len = _join_node->_right_table_data_types.size();

        IColumn::Offsets offset_data;
        auto& mcol = mutable_block.mutable_columns();
        offset_data.assign(_probe_rows, (uint32_t)0);

        // use in right join to change visited state after
        // exec the vother join conjunt
        std::vector<bool*> visited_map;
        visited_map.reserve(1.2 * _batch_size);

        std::vector<bool> same_to_prev;
        same_to_prev.reserve(1.2 * _batch_size);

        int current_offset = 0;

        for (; _probe_index < _probe_rows;) {
            // ignore null rows
            if constexpr (ignore_null) {
                if ((*null_map)[_probe_index]) {
                    offset_data[_probe_index++] = current_offset;
                    continue;
                }
            }
            auto find_result =
                    (*null_map)[_probe_index]
                            ? decltype(key_getter.find_key(hash_table_ctx.hash_table, _probe_index,
                                                           _arena)) {nullptr, false}
                            : key_getter.find_key(hash_table_ctx.hash_table, _probe_index, _arena);

            if (find_result.is_found()) {
                auto& mapped = find_result.get_mapped();
                auto origin_offset = current_offset;

                for (auto it = mapped.begin(); it.ok(); ++it) {
                    ++current_offset;
                    const Block& cur_blk = _build_blocks[it->block_offset];
                    for (size_t j = 0; j < right_col_len; ++j) {
                        auto& column = *cur_blk.get_by_position(j).column;
                        mcol[j + right_col_idx]->insert_from(column, it->row_num);
                    }
                    visited_map.emplace_back(&it->visited);
                }
                same_to_prev.emplace_back(false);
                for (int i = 0; i < current_offset - origin_offset - 1; ++i) {
                    same_to_prev.emplace_back(true);
                }
            } else if constexpr (JoinOpType::value == TJoinOp::LEFT_OUTER_JOIN ||
                                 JoinOpType::value == TJoinOp::FULL_OUTER_JOIN ||
                                 JoinOpType::value == TJoinOp::LEFT_ANTI_JOIN) {
                ++current_offset;
                same_to_prev.emplace_back(false);
                visited_map.emplace_back(nullptr);
                // only full outer / left outer need insert the data of right table
                if constexpr (JoinOpType::value == TJoinOp::LEFT_OUTER_JOIN ||
                              JoinOpType::value == TJoinOp::FULL_OUTER_JOIN) {
                    for (size_t j = 0; j < right_col_len; ++j) {
                        DCHECK(mcol[j + right_col_idx]->is_nullable());
                        assert_cast<ColumnNullable *>(mcol[j + right_col_idx].get())->insert_join_null_data();
                    }
                } else {
                    for (size_t j = 0; j < right_col_len; ++j) {
                        mcol[j + right_col_idx]->insert_default();
                    }
                }
            } else {
                // other join, no nothing
            }

            offset_data[_probe_index++] = current_offset;

            if (current_offset >= _batch_size) {
                break;
            }
        }

        for (int i = _probe_index; i < _probe_rows; ++i) {
            offset_data[i] = current_offset;
        }

        output_block->swap(mutable_block.to_block());
        for (int i = 0; i < right_col_idx; ++i) {
            auto& column = _probe_block.get_by_position(i).column;
            output_block->get_by_position(i).column = column->replicate(offset_data);
        }

        if (_join_node->_vother_join_conjunct_ptr) {
            int result_column_id = -1;
            int orig_columns = output_block->columns();
            (*_join_node->_vother_join_conjunct_ptr)->execute(output_block, &result_column_id);

            auto column = output_block->get_by_position(result_column_id).column;
            if constexpr (JoinOpType::value == TJoinOp::LEFT_OUTER_JOIN ||
                          JoinOpType::value == TJoinOp::FULL_OUTER_JOIN) {
                auto new_filter_column = ColumnVector<UInt8>::create();
                auto& filter_map = new_filter_column->get_data();

                for (int i = 0; i < column->size(); ++i) {
                    auto join_hit = visited_map[i] != nullptr;
                    auto other_hit = column->get_bool(i);

                    if (!other_hit) {
                        for (size_t j = 0; j < right_col_len; ++j) {
                            typeid_cast<ColumnNullable*>(
                                    std::move(*output_block->get_by_position(j + right_col_idx)
                                                       .column)
                                            .mutate()
                                            .get())
                                    ->get_null_map_data()[i] = true;
                        }
                    }

                    if (join_hit) {
                        *visited_map[i] |= other_hit;
                        filter_map.push_back(other_hit || !same_to_prev[i] ||
                                             (!column->get_bool(i - 1) && filter_map.back()));
                        // Here to keep only hit join conjunt and other join conjunt is true need to be output.
                        // if not, only some key must keep one row will output will null right table column
                        if (same_to_prev[i] && filter_map.back() && !column->get_bool(i - 1))
                            filter_map[i - 1] = false;
                    } else {
                        filter_map.push_back(true);
                    }
                }
                output_block->get_by_position(result_column_id).column =
                        std::move(new_filter_column);
            } else if constexpr (JoinOpType::value == TJoinOp::RIGHT_OUTER_JOIN) {
                for (int i = 0; i < column->size(); ++i) {
                    DCHECK(visited_map[i]);
                    *visited_map[i] |= column->get_bool(i);
                }
            } else if constexpr (JoinOpType::value == TJoinOp::LEFT_SEMI_JOIN) {
                auto new_filter_column = ColumnVector<UInt8>::create();
                auto& filter_map = new_filter_column->get_data();

                if (!column->empty()) filter_map.emplace_back(column->get_bool(0));
                for (int i = 1; i < column->size(); ++i) {
                    if (column->get_bool(i) || (same_to_prev[i] && filter_map[i - 1])) {
                        // Only last same element is true, output last one
                        filter_map.push_back(true);
                        filter_map[i - 1] = !same_to_prev[i] && filter_map[i - 1];
                    } else {
                        filter_map.push_back(false);
                    }
                }

                output_block->get_by_position(result_column_id).column =
                        std::move(new_filter_column);
            } else if constexpr (JoinOpType::value == TJoinOp::LEFT_ANTI_JOIN) {
                auto new_filter_column = ColumnVector<UInt8>::create();
                auto& filter_map = new_filter_column->get_data();

                if (!column->empty()) filter_map.emplace_back(column->get_bool(0) && visited_map[0]);
                for (int i = 1; i < column->size(); ++i) {
                    if ((visited_map[i] && column->get_bool(i)) || (same_to_prev[i] && filter_map[i - 1])) {
                        filter_map.push_back(true);
                        filter_map[i - 1] = !same_to_prev[i] && filter_map[i - 1];
                    } else {
                        filter_map.push_back(false);
                    }
                }

                // Same to the semi join, but change the last value to opposite value
                for (int i = 1; i < same_to_prev.size(); ++i) {
                    if (!same_to_prev[i]) filter_map[i - 1] = !filter_map[i - 1];
                }
                filter_map[same_to_prev.size() - 1] = !filter_map[same_to_prev.size() - 1];

                output_block->get_by_position(result_column_id).column =
                        std::move(new_filter_column);
            } else if constexpr (JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN ||
                                 JoinOpType::value == TJoinOp::RIGHT_ANTI_JOIN) {
                for (int i = 0; i < column->size(); ++i) {
                    DCHECK(visited_map[i]);
                    *visited_map[i] |= column->get_bool(i);
                }
            } else {
                // inner join do nothing
            }

            if constexpr (JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN ||
                          JoinOpType::value == TJoinOp::RIGHT_ANTI_JOIN) {
                output_block->clear();
            } else {
                if constexpr (JoinOpType::value == TJoinOp::LEFT_SEMI_JOIN ||
                          JoinOpType::value == TJoinOp::LEFT_ANTI_JOIN) orig_columns = right_col_idx;
                Block::filter_block(output_block, result_column_id, orig_columns);
            }
        }

        return Status::OK();
    }

    // Process full outer join/ right join / right semi/anti join to output the join result
    // in hash table
    Status process_data_in_hashtable(HashTableContext& hash_table_ctx, MutableBlock& mutable_block,
                                     Block* output_block, bool* eos) {
        hash_table_ctx.init_once();
        auto& mcol = mutable_block.mutable_columns();

        int right_col_idx = _join_node->_is_right_semi_anti ? 0 :
                _join_node->_left_table_data_types.size();
        int right_col_len = _join_node->_right_table_data_types.size();

        auto& iter = hash_table_ctx.iter;
        auto block_size = 0;

        auto insert_from_hash_table = [&](uint8_t offset, uint32_t row_num) {
            block_size++;
            for (size_t j = 0; j < right_col_len; ++j) {
                auto& column = *_build_blocks[offset].get_by_position(j).column;
                mcol[j + right_col_idx]->insert_from(column, row_num);
            }
        };

        for (; iter != hash_table_ctx.hash_table.end() && block_size < _batch_size; ++iter) {
            auto& mapped = iter->get_second();
            for (auto it = mapped.begin(); it.ok(); ++it) {
                if constexpr (JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN) {
                    if (it->visited)
                        insert_from_hash_table(it->block_offset, it->row_num);
                } else {
                    if (!it->visited)
                        insert_from_hash_table(it->block_offset, it->row_num);
                }
            }
        }

        // right outer join / full join need insert data of left table
        if constexpr (JoinOpType::value == TJoinOp::LEFT_OUTER_JOIN ||
                      JoinOpType::value == TJoinOp::RIGHT_OUTER_JOIN ||
                      JoinOpType::value == TJoinOp::FULL_OUTER_JOIN) {
            for (int i = 0; i < right_col_idx; ++i) {
                for (int j = 0; j < block_size; ++j) {
                    assert_cast<ColumnNullable *>(mcol[i].get())->insert_join_null_data();
                }
            }
        }
        *eos = iter == hash_table_ctx.hash_table.end();

        output_block->swap(mutable_block.to_block());
        return Status::OK();
    }

private:
    HashJoinNode* _join_node;
    const int _batch_size;
    const size_t _probe_rows;
    const std::vector<Block>& _build_blocks;
    const Block& _probe_block;
    int& _probe_index;
    ColumnRawPtrs& _probe_raw_ptrs;
    Arena _arena;

    std::vector<uint32_t>& _items_counts;
    std::vector<int8_t>& _build_block_offsets;
    std::vector<int>& _build_block_rows;

    ProfileCounter* _rows_returned_counter;
    ProfileCounter* _search_hashtable_timer;
    ProfileCounter* _build_side_output_timer;
    ProfileCounter* _probe_side_output_timer;
};

// now we only support inner join
HashJoinNode::HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _join_op(tnode.hash_join_node.join_op),
          _hash_table_rows(0),
          _mem_used(0),
          _match_all_probe(_join_op == TJoinOp::LEFT_OUTER_JOIN ||
                           _join_op == TJoinOp::FULL_OUTER_JOIN),
          _match_one_build(_join_op == TJoinOp::LEFT_SEMI_JOIN),
          _match_all_build(_join_op == TJoinOp::RIGHT_OUTER_JOIN ||
                           _join_op == TJoinOp::FULL_OUTER_JOIN),
          _build_unique(_join_op == TJoinOp::LEFT_ANTI_JOIN || _join_op == TJoinOp::LEFT_SEMI_JOIN),
          _is_right_semi_anti(_join_op == TJoinOp::RIGHT_ANTI_JOIN ||
                              _join_op == TJoinOp::RIGHT_SEMI_JOIN),
          _is_outer_join(_match_all_build || _match_all_probe) {
    _runtime_filter_descs = tnode.runtime_filters;
    init_join_op();

    // avoid vector expand change block address.
    // one block can store 4g data, _build_blocks can store 128*4g data.
    // if probe data bigger than 512g, runtime filter maybe will core dump when insert data.
    _build_blocks.reserve(128);
}

HashJoinNode::~HashJoinNode() = default;

void HashJoinNode::init_join_op() {
    switch (_join_op) {
#define M(NAME)                                                                                      \
        case TJoinOp::NAME:                                                                          \
            _join_op_variants.emplace<std::integral_constant<TJoinOp::type, TJoinOp::NAME>>();       \
            break;
        APPLY_FOR_JOINOP_VARIANTS(M);
#undef M
        default:
            //do nothing
            break;
    }
    return;
}
Status HashJoinNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK(tnode.__isset.hash_join_node);
    if (tnode.hash_join_node.join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
        return Status::InternalError("Do not support null aware left anti join");
    }
    _row_desc_for_other_join_conjunt = RowDescriptor(child(0)->row_desc(), child(1)->row_desc());

    const bool build_stores_null = _join_op == TJoinOp::RIGHT_OUTER_JOIN ||
                                   _join_op == TJoinOp::FULL_OUTER_JOIN ||
                                   _join_op == TJoinOp::RIGHT_ANTI_JOIN;
    const bool probe_dispose_null =
            _match_all_probe || _build_unique || _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN;

    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;
    for (int i = 0; i < eq_join_conjuncts.size(); ++i) {
        VExprContext* ctx = nullptr;
        RETURN_IF_ERROR(VExpr::create_expr_tree(_pool, eq_join_conjuncts[i].left, &ctx));
        _probe_expr_ctxs.push_back(ctx);
        RETURN_IF_ERROR(VExpr::create_expr_tree(_pool, eq_join_conjuncts[i].right, &ctx));
        _build_expr_ctxs.push_back(ctx);

        bool null_aware = eq_join_conjuncts[i].__isset.opcode &&
                          eq_join_conjuncts[i].opcode == TExprOpcode::EQ_FOR_NULL;
        _is_null_safe_eq_join.push_back(null_aware);

        // if is null aware, build join column and probe join column both need dispose null value
        _build_not_ignore_null.emplace_back(
                null_aware ||
                (_build_expr_ctxs.back()->root()->is_nullable() && build_stores_null));
        _probe_not_ignore_null.emplace_back(
                null_aware ||
                (_probe_expr_ctxs.back()->root()->is_nullable() && probe_dispose_null));
    }
    _probe_column_disguise_null.reserve(eq_join_conjuncts.size());

    if (tnode.hash_join_node.__isset.vother_join_conjunct) {
        _vother_join_conjunct_ptr.reset(new doris::vectorized::VExprContext*);
        RETURN_IF_ERROR(doris::vectorized::VExpr::create_expr_tree(
                _pool, tnode.hash_join_node.vother_join_conjunct, _vother_join_conjunct_ptr.get()));

        // If LEFT SEMI JOIN/LEFT ANTI JOIN with not equal predicate,
        // build table should not be deduplicated.
        _build_unique = false;
        _have_other_join_conjunct = true;
    }

    for (const auto& filter_desc : _runtime_filter_descs) {
        RETURN_IF_ERROR(state->runtime_filter_mgr()->regist_filter(RuntimeFilterRole::PRODUCER,
                                                                   filter_desc, state->query_options()));
    }

    return Status::OK();
}

Status HashJoinNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    _hash_table_mem_tracker = MemTracker::create_virtual_tracker(-1, "VSetOperationNode:HashTable");

    // Build phase
    auto build_phase_profile = runtime_profile()->create_child("BuildPhase", true, true);
    runtime_profile()->add_child(build_phase_profile, false, nullptr);
    _build_timer = ADD_TIMER(build_phase_profile, "BuildTime");
    _build_table_timer = ADD_TIMER(build_phase_profile, "BuildTableTime");
    _build_table_insert_timer = ADD_TIMER(build_phase_profile, "BuildTableInsertTime");
    _build_expr_call_timer = ADD_TIMER(build_phase_profile, "BuildExprCallTime");
    _build_table_expanse_timer = ADD_TIMER(build_phase_profile, "BuildTableExpanseTime");
    _build_rows_counter = ADD_COUNTER(build_phase_profile, "BuildRows", TUnit::UNIT);

    // Probe phase
    auto probe_phase_profile = runtime_profile()->create_child("ProbePhase", true, true);
    _probe_timer = ADD_TIMER(probe_phase_profile, "ProbeTime");
    _probe_next_timer = ADD_TIMER(probe_phase_profile, "ProbeFindNextTime");
    _probe_expr_call_timer = ADD_TIMER(probe_phase_profile, "ProbeExprCallTime");
    _probe_rows_counter = ADD_COUNTER(probe_phase_profile, "ProbeRows", TUnit::UNIT);
    _search_hashtable_timer = ADD_TIMER(probe_phase_profile, "ProbeWhenSearchHashTableTime");
    _build_side_output_timer = ADD_TIMER(probe_phase_profile, "ProbeWhenBuildSideOutputTime");
    _probe_side_output_timer = ADD_TIMER(probe_phase_profile, "ProbeWhenProbeSideOutputTime");

    _push_down_timer = ADD_TIMER(runtime_profile(), "PushDownTime");
    _push_compute_timer = ADD_TIMER(runtime_profile(), "PushDownComputeTime");
    _build_buckets_counter = ADD_COUNTER(runtime_profile(), "BuildBuckets", TUnit::UNIT);

    RETURN_IF_ERROR(
            VExpr::prepare(_build_expr_ctxs, state, child(1)->row_desc(), expr_mem_tracker()));
    RETURN_IF_ERROR(
            VExpr::prepare(_probe_expr_ctxs, state, child(0)->row_desc(), expr_mem_tracker()));

    // _vother_join_conjuncts are evaluated in the context of the rows produced by this node
    if (_vother_join_conjunct_ptr) {
        RETURN_IF_ERROR(
                (*_vother_join_conjunct_ptr)
                        ->prepare(state, _row_desc_for_other_join_conjunt, expr_mem_tracker()));
    }
    // right table data types
    _right_table_data_types = VectorizedUtils::get_data_types(child(1)->row_desc());
    _left_table_data_types = VectorizedUtils::get_data_types(child(0)->row_desc());

    // Hash Table Init
    _hash_table_init();

    _build_block_offsets.resize(state->batch_size());
    _build_block_rows.resize(state->batch_size());
    return Status::OK();
}

Status HashJoinNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }

    if (_vother_join_conjunct_ptr) (*_vother_join_conjunct_ptr)->close(state);

    _hash_table_mem_tracker->release(_mem_used);

    return ExecNode::close(state);
}

Status HashJoinNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("Not Implemented HashJoin Node::get_next scalar");
}

Status HashJoinNode::get_next(RuntimeState* state, Block* output_block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_TIMER(_probe_timer);

    size_t probe_rows = _probe_block.rows();
    if ((probe_rows == 0 || _probe_index == probe_rows) && !_probe_eos) {
        _probe_index = 0;
        // clear_column_data of _probe_block
        {
            if (!_probe_column_disguise_null.empty()) {
                for (int i = 0; i < _probe_column_disguise_null.size(); ++i) {
                    auto column_to_erase = _probe_column_disguise_null[i];
                    _probe_block.erase(column_to_erase - i);
                }
                _probe_column_disguise_null.clear();
            }
            release_block_memory(_probe_block);
        }

        do {
            SCOPED_TIMER(_probe_next_timer);
            RETURN_IF_ERROR(child(0)->get_next(state, &_probe_block, &_probe_eos));
        } while (_probe_block.rows() == 0 && !_probe_eos);

        probe_rows = _probe_block.rows();
        if (probe_rows != 0) {
            COUNTER_UPDATE(_probe_rows_counter, probe_rows);

            int probe_expr_ctxs_sz = _probe_expr_ctxs.size();
            _probe_columns.resize(probe_expr_ctxs_sz);
            if (_null_map_column == nullptr) {
                _null_map_column = ColumnUInt8::create();
            }
            _null_map_column->get_data().assign(probe_rows, (uint8_t)0);

            Status st = std::visit(
                    [&](auto&& arg) -> Status {
                        using HashTableCtxType = std::decay_t<decltype(arg)>;
                        if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                            auto& null_map_val = _null_map_column->get_data();
                            return extract_probe_join_column(_probe_block, null_map_val,
                                                             _probe_columns, _probe_ignore_null,
                                                             *_probe_expr_call_timer);
                        } else {
                            LOG(FATAL) << "FATAL: uninited hash table";
                        }
                        __builtin_unreachable();
                    },
                    _hash_table_variants);

            RETURN_IF_ERROR(st);
        }
    }

    Status st;

    if (_probe_index < _probe_block.rows()) {
        std::visit(
            [&](auto&& arg, auto&& join_op_variants, auto have_other_join_conjunct, auto probe_ignore_null) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                using JoinOpType = std::decay_t<decltype(join_op_variants)>;
                if constexpr (have_other_join_conjunct) {
                    MutableBlock mutable_block(VectorizedUtils::create_empty_columnswithtypename(
                        _row_desc_for_other_join_conjunt));

                    if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                        ProcessHashTableProbe<HashTableCtxType, JoinOpType, probe_ignore_null> process_hashtable_ctx(
                                this, state->batch_size(), probe_rows);
                        st = process_hashtable_ctx.do_process_with_other_join_conjunts(
                                    arg, &_null_map_column->get_data(),
                                    mutable_block, output_block);
                    } else {
                        LOG(FATAL) << "FATAL: uninited hash table";
                    }
                } else {
                    MutableBlock mutable_block = output_block->mem_reuse() ? MutableBlock(output_block) : 
                                                MutableBlock(VectorizedUtils::create_empty_columnswithtypename(row_desc()));

                    if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                        ProcessHashTableProbe<HashTableCtxType, JoinOpType, probe_ignore_null> process_hashtable_ctx(
                                this, state->batch_size(), probe_rows);
                        st = process_hashtable_ctx.do_process(
                                arg, &_null_map_column->get_data(),
                                mutable_block, output_block);
                    } else {
                        LOG(FATAL) << "FATAL: uninited hash table";
                    }
                }
            }, _hash_table_variants,
               _join_op_variants,
                make_bool_variant(_have_other_join_conjunct),
                make_bool_variant(_probe_ignore_null));
    } else if (_probe_eos) {
        if (_is_right_semi_anti || (_is_outer_join && _join_op != TJoinOp::LEFT_OUTER_JOIN)) {
            MutableBlock mutable_block(
                    VectorizedUtils::create_empty_columnswithtypename(row_desc()));
            std::visit(
                    [&](auto&& arg, auto&& join_op_variants) {
                        using JoinOpType = std::decay_t<decltype(join_op_variants)>;
                        using HashTableCtxType = std::decay_t<decltype(arg)>;
                        if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                            ProcessHashTableProbe<HashTableCtxType, JoinOpType, false> process_hashtable_ctx(
                                    this, state->batch_size(), probe_rows);
                            st = process_hashtable_ctx.process_data_in_hashtable(arg, mutable_block,
                                                                                 output_block, eos);
                        } else {
                            LOG(FATAL) << "FATAL: uninited hash table";
                        }
                    },
                    _hash_table_variants,
                    _join_op_variants);
        } else {
            *eos = true;
            return Status::OK();
        }
    } else {
        return Status::OK();
    }

    RETURN_IF_ERROR(
            VExprContext::filter_block(_vconjunct_ctx_ptr, output_block, output_block->columns()));
    reached_limit(output_block, eos);

    return st;
}

Status HashJoinNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);

    RETURN_IF_ERROR(VExpr::open(_build_expr_ctxs, state));
    RETURN_IF_ERROR(VExpr::open(_probe_expr_ctxs, state));
    if (_vother_join_conjunct_ptr) {
        RETURN_IF_ERROR((*_vother_join_conjunct_ptr)->open(state));
    }

    RETURN_IF_ERROR(_hash_table_build(state));
    RETURN_IF_ERROR(child(0)->open(state));

    return Status::OK();
}

Status HashJoinNode::_hash_table_build(RuntimeState* state) {
    RETURN_IF_ERROR(child(1)->open(state));
    SCOPED_SWITCH_THREAD_LOCAL_MEM_TRACKER_ERR_CB("Hash join, while constructing the hash table.");
    SCOPED_TIMER(_build_timer);
    MutableBlock mutable_block(child(1)->row_desc().tuple_descriptors());

    uint8_t index = 0;
    int64_t last_mem_used = 0;
    bool eos = false;

    Block block;
    while (!eos) {
        block.clear_column_data();
        RETURN_IF_CANCELLED(state);

        RETURN_IF_ERROR(child(1)->get_next(state, &block, &eos));
        _hash_table_mem_tracker->consume(block.allocated_bytes());
        _mem_used += block.allocated_bytes();

        if (block.rows() != 0) { mutable_block.merge(block); }

        // make one block for each 4 gigabytes
        constexpr static auto BUILD_BLOCK_MAX_SIZE =  4 * 1024UL * 1024UL * 1024UL;
        if (_mem_used - last_mem_used > BUILD_BLOCK_MAX_SIZE) {
            _build_blocks.emplace_back(mutable_block.to_block());
            // TODO:: Rethink may we should do the proess after we recevie all build blocks ?
            // which is better.
            RETURN_IF_ERROR(_process_build_block(state, _build_blocks[index], index));

            mutable_block = MutableBlock();
            ++index;
            last_mem_used = _mem_used;
        }
    }

    _build_blocks.emplace_back(mutable_block.to_block());
    RETURN_IF_ERROR(_process_build_block(state, _build_blocks[index], index));

    return std::visit(
            [&](auto&& arg) -> Status {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    ProcessRuntimeFilterBuild<HashTableCtxType> runtime_filter_build_process(this);
                    return runtime_filter_build_process(state, arg);
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            _hash_table_variants);
}

// TODO:: unify the code of extract probe join column
Status HashJoinNode::extract_build_join_column(Block& block, NullMap& null_map,
                                               ColumnRawPtrs& raw_ptrs, bool& ignore_null,
                                               RuntimeProfile::Counter& expr_call_timer) {
    for (size_t i = 0; i < _build_expr_ctxs.size(); ++i) {
        int result_col_id = -1;
        // execute build column
        {
            SCOPED_TIMER(&expr_call_timer);
            RETURN_IF_ERROR(_build_expr_ctxs[i]->execute(&block, &result_col_id));
        }

        // TODO: opt the column is const
        block.get_by_position(result_col_id).column =
                block.get_by_position(result_col_id).column->convert_to_full_column_if_const();
        
        if (_is_null_safe_eq_join[i]) {
            raw_ptrs[i] = block.get_by_position(result_col_id).column.get();
        } else {
            auto column = block.get_by_position(result_col_id).column.get();
            if (auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
                auto& col_nested = nullable->get_nested_column();
                auto& col_nullmap = nullable->get_null_map_data();

                ignore_null |= !_build_not_ignore_null[i];
                if (_build_not_ignore_null[i]) {
                    raw_ptrs[i] = nullable;
                } else {
                    VectorizedUtils::update_null_map(null_map, col_nullmap);
                    raw_ptrs[i] = &col_nested;
                }
            } else {
                raw_ptrs[i] = column;
            }
        }
    }
    return Status::OK();
}

Status HashJoinNode::extract_probe_join_column(Block& block, NullMap& null_map,
                                               ColumnRawPtrs& raw_ptrs, bool& ignore_null,
                                               RuntimeProfile::Counter& expr_call_timer) {
    for (size_t i = 0; i < _probe_expr_ctxs.size(); ++i) {
        int result_col_id = -1;
        // execute build column
        {
            SCOPED_TIMER(&expr_call_timer);
            RETURN_IF_ERROR(_probe_expr_ctxs[i]->execute(&block, &result_col_id));
        }

        // TODO: opt the column is const
        block.get_by_position(result_col_id).column =
                block.get_by_position(result_col_id).column->convert_to_full_column_if_const();

        if (_is_null_safe_eq_join[i]) {
            raw_ptrs[i] = block.get_by_position(result_col_id).column.get();
        } else {
            auto column = block.get_by_position(result_col_id).column.get();
            if (auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
                auto& col_nested = nullable->get_nested_column();
                auto& col_nullmap = nullable->get_null_map_data();

                ignore_null |= !_probe_not_ignore_null[i];
                if (_build_not_ignore_null[i]) {
                    raw_ptrs[i] = nullable;
                } else {
                    VectorizedUtils::update_null_map(null_map, col_nullmap);
                    raw_ptrs[i] = &col_nested;
                }
            } else {
                if (_build_not_ignore_null[i]) {
                    auto column_ptr =
                            make_nullable(block.get_by_position(result_col_id).column, false);
                    _probe_column_disguise_null.emplace_back(block.columns());
                    block.insert({column_ptr,
                                  make_nullable(block.get_by_position(result_col_id).type), ""});
                    column = column_ptr.get();
                }
                raw_ptrs[i] = column;
            }
        }
    }
    return Status::OK();
}

Status HashJoinNode::_process_build_block(RuntimeState* state, Block& block, uint8_t offset) {
    SCOPED_TIMER(_build_table_timer);
    size_t rows = block.rows();
    if (rows == 0) {
        return Status::OK();
    }
    COUNTER_UPDATE(_build_rows_counter, rows);

    ColumnRawPtrs raw_ptrs(_build_expr_ctxs.size());

    NullMap null_map_val(rows);
    null_map_val.assign(rows, (uint8_t)0);
    bool has_null = false;

    // Get the key column that needs to be built
    Status st = std::visit(
            [&](auto&& arg) -> Status {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    return extract_build_join_column(block, null_map_val, raw_ptrs,
                                                     has_null, *_build_expr_call_timer);
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
                __builtin_unreachable();
            },
            _hash_table_variants);

    bool has_runtime_filter = !_runtime_filter_descs.empty();

    std::visit(
            [&](auto&& arg) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
#define CALL_BUILD_FUNCTION(HAS_NULL, BUILD_UNIQUE)                                           \
    ProcessHashTableBuild<HashTableCtxType, HAS_NULL, BUILD_UNIQUE> hash_table_build_process( \
            rows, block, raw_ptrs, this, state->batch_size(), offset);                       \
    st = hash_table_build_process(arg, &null_map_val, has_runtime_filter);
                    if (std::pair {has_null, _build_unique} == std::pair {true, true}) {
                        CALL_BUILD_FUNCTION(true, true);
                    } else if (std::pair {has_null, _build_unique} == std::pair {true, false}) {
                        CALL_BUILD_FUNCTION(true, false);
                    } else if (std::pair {has_null, _build_unique} == std::pair {false, true}) {
                        CALL_BUILD_FUNCTION(false, true);
                    } else {
                        CALL_BUILD_FUNCTION(false, false);
                    }
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            _hash_table_variants);

    return st;
}

void HashJoinNode::_hash_table_init() {
    if (_build_expr_ctxs.size() == 1 && !_build_not_ignore_null[0]) {
        // Single column optimization
        switch (_build_expr_ctxs[0]->root()->result_type()) {
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
            _hash_table_variants.emplace<I8HashTableContext>();
            break;
        case TYPE_SMALLINT:
            _hash_table_variants.emplace<I16HashTableContext>();
            break;
        case TYPE_INT:
        case TYPE_FLOAT:
            _hash_table_variants.emplace<I32HashTableContext>();
            break;
        case TYPE_BIGINT:
        case TYPE_DOUBLE:
        case TYPE_DATETIME:
        case TYPE_DATE:
            _hash_table_variants.emplace<I64HashTableContext>();
            break;
        case TYPE_LARGEINT:
        case TYPE_DECIMALV2:
            _hash_table_variants.emplace<I128HashTableContext>();
            break;
        default:
            _hash_table_variants.emplace<SerializedHashTableContext>();
        }
        return;
    }

    bool use_fixed_key = true;
    bool has_null = false;
    int key_byte_size = 0;

    _probe_key_sz.resize(_probe_expr_ctxs.size());
    _build_key_sz.resize(_build_expr_ctxs.size());

    for (int i = 0; i < _build_expr_ctxs.size(); ++i) {
        const auto vexpr = _build_expr_ctxs[i]->root();
        const auto& data_type = vexpr->data_type();

        if (!data_type->have_maximum_size_of_value()) {
            use_fixed_key = false;
            break;
        }

        auto is_null = data_type->is_nullable();
        has_null |= is_null;
        _build_key_sz[i] = data_type->get_maximum_size_of_value_in_memory() - (is_null ? 1 : 0);
        _probe_key_sz[i] = _build_key_sz[i];
        key_byte_size += _probe_key_sz[i];
    }

    if (std::tuple_size<KeysNullMap<UInt256>>::value + key_byte_size > sizeof(UInt256)) {
        use_fixed_key = false;
    }

    if (use_fixed_key) {
        // TODO: may we should support uint256 in the future
        if (has_null) {
            if (std::tuple_size<KeysNullMap<UInt64>>::value + key_byte_size <= sizeof(UInt64)) {
                _hash_table_variants.emplace<I64FixedKeyHashTableContext<true>>();
            } else if (std::tuple_size<KeysNullMap<UInt128>>::value + key_byte_size <=
                       sizeof(UInt128)) {
                _hash_table_variants.emplace<I128FixedKeyHashTableContext<true>>();
            } else {
                _hash_table_variants.emplace<I256FixedKeyHashTableContext<true>>();
            }
        } else {
            if (key_byte_size <= sizeof(UInt64)) {
                _hash_table_variants.emplace<I64FixedKeyHashTableContext<false>>();
            } else if (key_byte_size <= sizeof(UInt128)) {
                _hash_table_variants.emplace<I128FixedKeyHashTableContext<false>>();
            } else {
                _hash_table_variants.emplace<I256FixedKeyHashTableContext<false>>();
            }
        }
    } else {
        _hash_table_variants.emplace<SerializedHashTableContext>();
    }
}

} // namespace doris::vectorized
