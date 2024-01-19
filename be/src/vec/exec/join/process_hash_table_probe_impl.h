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

#include "common/status.h"
#include "pipeline/exec/hashjoin_probe_operator.h"
#include "process_hash_table_probe.h"
#include "runtime/thread_context.h" // IWYU pragma: keep
#include "util/simd/bits.h"
#include "vec/columns/column_filter_helper.h"
#include "vec/exprs/vexpr_context.h"
#include "vhash_join_node.h"

namespace doris::vectorized {

template <int JoinOpType, typename Parent>
ProcessHashTableProbe<JoinOpType, Parent>::ProcessHashTableProbe(Parent* parent, int batch_size)
        : _parent(parent),
          _batch_size(batch_size),
          _build_block(parent->build_block()),
          _tuple_is_null_left_flags(parent->is_outer_join()
                                            ? &(reinterpret_cast<ColumnUInt8&>(
                                                        *parent->_tuple_is_null_left_flag_column)
                                                        .get_data())
                                            : nullptr),
          _tuple_is_null_right_flags(parent->is_outer_join()
                                             ? &(reinterpret_cast<ColumnUInt8&>(
                                                         *parent->_tuple_is_null_right_flag_column)
                                                         .get_data())
                                             : nullptr),
          _have_other_join_conjunct(parent->have_other_join_conjunct()),
          _is_right_semi_anti(parent->is_right_semi_anti()),
          _left_output_slot_flags(parent->left_output_slot_flags()),
          _right_output_slot_flags(parent->right_output_slot_flags()),
          _has_null_in_build_side(parent->has_null_in_build_side()),
          _rows_returned_counter(parent->_rows_returned_counter),
          _search_hashtable_timer(parent->_search_hashtable_timer),
          _init_probe_side_timer(parent->_init_probe_side_timer),
          _build_side_output_timer(parent->_build_side_output_timer),
          _probe_side_output_timer(parent->_probe_side_output_timer),
          _probe_process_hashtable_timer(parent->_probe_process_hashtable_timer),
          _right_col_idx((_is_right_semi_anti && !_have_other_join_conjunct)
                                 ? 0
                                 : _parent->left_table_data_types().size()),
          _right_col_len(_parent->right_table_data_types().size()) {}

template <int JoinOpType, typename Parent>
void ProcessHashTableProbe<JoinOpType, Parent>::build_side_output_column(
        MutableColumns& mcol, const std::vector<bool>& output_slot_flags, int size,
        bool have_other_join_conjunct) {
    SCOPED_TIMER(_build_side_output_timer);
    constexpr auto is_semi_anti_join = JoinOpType == TJoinOp::RIGHT_ANTI_JOIN ||
                                       JoinOpType == TJoinOp::RIGHT_SEMI_JOIN ||
                                       JoinOpType == TJoinOp::LEFT_ANTI_JOIN ||
                                       JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
                                       JoinOpType == TJoinOp::LEFT_SEMI_JOIN;

    constexpr auto probe_all =
            JoinOpType == TJoinOp::LEFT_OUTER_JOIN || JoinOpType == TJoinOp::FULL_OUTER_JOIN;

    if ((!is_semi_anti_join || have_other_join_conjunct) && size) {
        for (int i = 0; i < _right_col_len; i++) {
            const auto& column = *_build_block->safe_get_by_position(i).column;
            if (output_slot_flags[i]) {
                mcol[i + _right_col_idx]->insert_indices_from(column, _build_indexs.data(),
                                                              _build_indexs.data() + size);
            } else {
                mcol[i + _right_col_idx]->insert_many_defaults(size);
            }
        }
    }

    // Dispose right tuple is null flags columns
    if (probe_all && !have_other_join_conjunct) {
        _tuple_is_null_right_flags->resize(size);
        auto* __restrict null_data = _tuple_is_null_right_flags->data();
        for (int i = 0; i < size; ++i) {
            null_data[i] = _build_indexs[i] == 0;
        }
    }
}

template <int JoinOpType, typename Parent>
void ProcessHashTableProbe<JoinOpType, Parent>::probe_side_output_column(
        MutableColumns& mcol, const std::vector<bool>& output_slot_flags, int size,
        int last_probe_index, bool all_match_one, bool have_other_join_conjunct) {
    SCOPED_TIMER(_probe_side_output_timer);
    auto& probe_block = _parent->_probe_block;
    for (int i = 0; i < output_slot_flags.size(); ++i) {
        if (output_slot_flags[i]) {
            auto& column = probe_block.get_by_position(i).column;
            if (all_match_one) {
                mcol[i]->insert_range_from(*column, last_probe_index, size);
            } else {
                column->replicate(_probe_indexs.data(), size, *mcol[i]);
            }
        } else {
            mcol[i]->insert_many_defaults(size);
        }
    }

    if constexpr (JoinOpType == TJoinOp::RIGHT_OUTER_JOIN) {
        if (!have_other_join_conjunct) {
            _tuple_is_null_left_flags->resize_fill(size, 0);
        }
    }
}

template <int JoinOpType, typename Parent>
template <typename HashTableType>
typename HashTableType::State ProcessHashTableProbe<JoinOpType, Parent>::_init_probe_side(
        HashTableType& hash_table_ctx, size_t probe_rows, bool with_other_join_conjuncts,
        const uint8_t* null_map, bool need_judge_null) {
    // may over batch size 1 for some outer join case
    _probe_indexs.resize(_batch_size + 1);
    _build_indexs.resize(_batch_size + 1);

    if (!_parent->_ready_probe) {
        _parent->_ready_probe = true;
        hash_table_ctx.reset();
        hash_table_ctx.init_serialized_keys(_parent->_probe_columns, probe_rows, null_map, true,
                                            false, hash_table_ctx.hash_table->get_bucket_size());
        hash_table_ctx.hash_table->pre_build_idxs(hash_table_ctx.bucket_nums,
                                                  need_judge_null ? null_map : nullptr);
    }
    return typename HashTableType::State(_parent->_probe_columns);
}

template <int JoinOpType, typename Parent>
template <bool need_null_map_for_probe, bool ignore_null, typename HashTableType,
          bool with_other_conjuncts, bool is_mark_join>
Status ProcessHashTableProbe<JoinOpType, Parent>::do_process(HashTableType& hash_table_ctx,
                                                             ConstNullMapPtr null_map,
                                                             MutableBlock& mutable_block,
                                                             Block* output_block,
                                                             size_t probe_rows) {
    if (_right_col_len && !_build_block) {
        return Status::InternalError("build block is nullptr");
    }

    auto& probe_index = _parent->_probe_index;
    auto& build_index = _parent->_build_index;
    auto last_probe_index = probe_index;

    {
        SCOPED_TIMER(_init_probe_side_timer);
        _init_probe_side<HashTableType>(
                hash_table_ctx, probe_rows, with_other_conjuncts,
                need_null_map_for_probe ? null_map->data() : nullptr,
                need_null_map_for_probe && ignore_null &&
                        (JoinOpType == doris::TJoinOp::LEFT_ANTI_JOIN ||
                         JoinOpType == doris::TJoinOp::LEFT_SEMI_JOIN ||
                         JoinOpType == doris::TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN || is_mark_join));
    }

    auto& mcol = mutable_block.mutable_columns();

    int current_offset = 0;

    std::unique_ptr<ColumnFilterHelper> mark_column;
    if (is_mark_join) {
        mark_column = std::make_unique<ColumnFilterHelper>(*mcol[mcol.size() - 1]);
    }

    {
        SCOPED_TIMER(_search_hashtable_timer);
        auto [new_probe_idx, new_build_idx,
              new_current_offset] = hash_table_ctx.hash_table->template find_batch < JoinOpType,
              with_other_conjuncts, is_mark_join,
              need_null_map_for_probe &&
                      ignore_null > (hash_table_ctx.keys, hash_table_ctx.bucket_nums.data(),
                                     probe_index, build_index, probe_rows, _probe_indexs.data(),
                                     _probe_visited, _build_indexs.data(), mark_column.get());
        probe_index = new_probe_idx;
        build_index = new_build_idx;
        current_offset = new_current_offset;
    }

    build_side_output_column(mcol, *_right_output_slot_flags, current_offset, with_other_conjuncts);

    if constexpr (with_other_conjuncts || (JoinOpType != TJoinOp::RIGHT_SEMI_JOIN &&
                                           JoinOpType != TJoinOp::RIGHT_ANTI_JOIN)) {
        auto check_all_match_one = [](const std::vector<uint32_t>& vecs, uint32_t probe_idx,
                                      int size) {
            if (size < 1 || vecs[0] != probe_idx) return false;
            for (int i = 1; i < size; i++) {
                if (vecs[i] - vecs[i - 1] != 1) {
                    return false;
                }
            }
            return true;
        };

        RETURN_IF_CATCH_EXCEPTION(probe_side_output_column(
                mcol, *_left_output_slot_flags, current_offset, last_probe_index,
                check_all_match_one(_probe_indexs, last_probe_index, current_offset),
                with_other_conjuncts));
    }

    output_block->swap(mutable_block.to_block());

    if constexpr (with_other_conjuncts) {
        return do_other_join_conjuncts(output_block, is_mark_join,
                                       hash_table_ctx.hash_table->get_visited(),
                                       hash_table_ctx.hash_table->has_null_key());
    }

    return Status::OK();
}

template <int JoinOpType, typename Parent>
Status ProcessHashTableProbe<JoinOpType, Parent>::do_other_join_conjuncts(
        Block* output_block, bool is_mark_join, std::vector<uint8_t>& visited,
        bool has_null_in_build_side) {
    // dispose the other join conjunct exec
    auto row_count = output_block->rows();
    if (!row_count) {
        return Status::OK();
    }

    SCOPED_TIMER(_parent->_process_other_join_conjunct_timer);
    int orig_columns = output_block->columns();
    IColumn::Filter other_conjunct_filter(row_count, 1);
    {
        bool can_be_filter_all = false;
        RETURN_IF_ERROR(VExprContext::execute_conjuncts(_parent->_other_join_conjuncts, nullptr,
                                                        output_block, &other_conjunct_filter,
                                                        &can_be_filter_all));
    }

    auto filter_column = ColumnUInt8::create();
    filter_column->get_data() = std::move(other_conjunct_filter);
    auto result_column_id = output_block->columns();
    output_block->insert({std::move(filter_column), std::make_shared<DataTypeUInt8>(), ""});
    uint8_t* __restrict filter_column_ptr =
            assert_cast<ColumnUInt8&>(
                    output_block->get_by_position(result_column_id).column->assume_mutable_ref())
                    .get_data()
                    .data();

    if constexpr (JoinOpType == TJoinOp::LEFT_OUTER_JOIN ||
                  JoinOpType == TJoinOp::FULL_OUTER_JOIN) {
        auto new_filter_column = ColumnUInt8::create(row_count);
        auto* __restrict filter_map = new_filter_column->get_data().data();

        // process equal-conjuncts-matched tuples that are newly generated
        // in this run if there are any.
        for (int i = 0; i < row_count; ++i) {
            bool join_hit = _build_indexs[i];
            bool other_hit = filter_column_ptr[i];

            if (!join_hit) {
                filter_map[i] = _parent->_last_probe_match != _probe_indexs[i];
            } else {
                filter_map[i] = other_hit;
            }
            if (filter_map[i]) {
                _parent->_last_probe_match = _probe_indexs[i];
            }
        }

        for (size_t i = 0; i < row_count; ++i) {
            if (filter_map[i]) {
                _tuple_is_null_right_flags->emplace_back(!_build_indexs[i] ||
                                                         !filter_column_ptr[i]);
                if constexpr (JoinOpType == TJoinOp::FULL_OUTER_JOIN) {
                    visited[_build_indexs[i]] = 1;
                }
            }
        }
        output_block->get_by_position(result_column_id).column = std::move(new_filter_column);
    } else if constexpr (JoinOpType == TJoinOp::LEFT_ANTI_JOIN ||
                         JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
                         JoinOpType == TJoinOp::LEFT_SEMI_JOIN) {
        auto new_filter_column = ColumnUInt8::create(row_count);
        auto* __restrict filter_map = new_filter_column->get_data().data();

        for (size_t i = 0; i < row_count; ++i) {
            bool not_matched_before = _parent->_last_probe_match != _probe_indexs[i];

            // _build_indexs[i] == 0 means the end of this probe index
            // if a probe row not matched with any build row, we need output a false value into mark column
            if constexpr (JoinOpType == TJoinOp::LEFT_SEMI_JOIN) {
                if (_build_indexs[i] == 0) {
                    filter_map[i] = is_mark_join && not_matched_before;
                    filter_column_ptr[i] = false;
                } else {
                    if (filter_column_ptr[i]) {
                        filter_map[i] = not_matched_before;
                        _parent->_last_probe_match = _probe_indexs[i];
                    } else {
                        filter_map[i] = false;
                    }
                }
            } else {
                if (_build_indexs[i] == 0) {
                    if (not_matched_before) {
                        filter_map[i] = true;
                    } else if (is_mark_join) {
                        filter_map[i] = true;
                        filter_column_ptr[i] = false;
                    } else {
                        filter_map[i] = false;
                    }
                } else {
                    filter_map[i] = false;
                    if (filter_column_ptr[i]) {
                        _parent->_last_probe_match = _probe_indexs[i];
                    }
                }
            }
        }

        if (is_mark_join) {
            auto mark_column =
                    output_block->get_by_position(orig_columns - 1).column->assume_mutable();
            ColumnFilterHelper helper(*mark_column);
            for (size_t i = 0; i < row_count; ++i) {
                bool mathced = filter_column_ptr[i] &&
                               (_build_indexs[i] != 0) == (JoinOpType == TJoinOp::LEFT_SEMI_JOIN);
                if (has_null_in_build_side && !mathced) {
                    helper.insert_null();
                } else {
                    helper.insert_value(mathced);
                }
            }
        }

        output_block->get_by_position(result_column_id).column = std::move(new_filter_column);
    } else if constexpr (JoinOpType == TJoinOp::RIGHT_SEMI_JOIN ||
                         JoinOpType == TJoinOp::RIGHT_ANTI_JOIN) {
        for (int i = 0; i < row_count; ++i) {
            visited[_build_indexs[i]] |= filter_column_ptr[i];
        }
    } else if constexpr (JoinOpType == TJoinOp::RIGHT_OUTER_JOIN) {
        auto filter_size = 0;
        for (int i = 0; i < row_count; ++i) {
            visited[_build_indexs[i]] |= filter_column_ptr[i];
            filter_size += filter_column_ptr[i];
        }
        _tuple_is_null_left_flags->resize_fill(filter_size, 0);
    }

    if constexpr (JoinOpType == TJoinOp::RIGHT_SEMI_JOIN ||
                  JoinOpType == TJoinOp::RIGHT_ANTI_JOIN) {
        output_block->clear();
    } else {
        if constexpr (JoinOpType == TJoinOp::LEFT_SEMI_JOIN ||
                      JoinOpType == TJoinOp::LEFT_ANTI_JOIN ||
                      JoinOpType == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            orig_columns = _right_col_idx;
        }
        RETURN_IF_ERROR(Block::filter_block(output_block, result_column_id,
                                            is_mark_join ? output_block->columns() : orig_columns));
    }

    return Status::OK();
}

template <int JoinOpType, typename Parent>
template <typename HashTableType>
Status ProcessHashTableProbe<JoinOpType, Parent>::process_data_in_hashtable(
        HashTableType& hash_table_ctx, MutableBlock& mutable_block, Block* output_block,
        bool* eos) {
    SCOPED_TIMER(_probe_process_hashtable_timer);
    auto& mcol = mutable_block.mutable_columns();
    *eos = hash_table_ctx.hash_table->template iterate_map<JoinOpType>(_build_indexs);
    auto block_size = _build_indexs.size();

    if (block_size) {
        if (mcol.size() < _right_col_len + _right_col_idx) {
            return Status::InternalError(
                    "output block invalid, mcol.size()={}, _right_col_len={}, _right_col_idx={}",
                    mcol.size(), _right_col_len, _right_col_idx);
        }
        for (size_t j = 0; j < _right_col_len; ++j) {
            const auto& column = *_build_block->safe_get_by_position(j).column;
            mcol[j + _right_col_idx]->insert_indices_from(column, _build_indexs.data(),
                                                          _build_indexs.data() + block_size);
        }

        // just resize the left table column in case with other conjunct to make block size is not zero
        if (_is_right_semi_anti && _have_other_join_conjunct) {
            for (int i = 0; i < _right_col_idx; ++i) {
                mcol[i]->resize(block_size);
            }
        }

        // right outer join / full join need insert data of left table
        if constexpr (JoinOpType == TJoinOp::RIGHT_OUTER_JOIN ||
                      JoinOpType == TJoinOp::FULL_OUTER_JOIN) {
            for (int i = 0; i < _right_col_idx; ++i) {
                assert_cast<ColumnNullable*>(mcol[i].get())->insert_many_defaults(block_size);
            }
            _tuple_is_null_left_flags->resize_fill(block_size, 1);
        }
        output_block->swap(mutable_block.to_block(0));
        DCHECK(block_size <= _batch_size);
    }
    return Status::OK();
}

template <int JoinOpType, typename Parent>
template <bool need_null_map_for_probe, bool ignore_null, typename HashTableType>
Status ProcessHashTableProbe<JoinOpType, Parent>::process(
        HashTableType& hash_table_ctx, ConstNullMapPtr null_map, MutableBlock& mutable_block,
        Block* output_block, size_t probe_rows, bool is_mark_join, bool have_other_join_conjunct) {
    Status res;
    std::visit(
            [&](auto is_mark_join, auto have_other_join_conjunct) {
                res = do_process<need_null_map_for_probe, ignore_null, HashTableType,
                                 have_other_join_conjunct, is_mark_join>(
                        hash_table_ctx, null_map, mutable_block, output_block, probe_rows);
            },
            make_bool_variant(is_mark_join), make_bool_variant(have_other_join_conjunct));
    return res;
}

template <typename T>
struct ExtractType;

template <typename T, typename U>
struct ExtractType<T(U)> {
    using Type = U;
};

#define INSTANTIATION(JoinOpType, Parent, T)                                                      \
    template Status                                                                               \
    ProcessHashTableProbe<JoinOpType, Parent>::process<false, false, ExtractType<void(T)>::Type>( \
            ExtractType<void(T)>::Type & hash_table_ctx, ConstNullMapPtr null_map,                \
            MutableBlock & mutable_block, Block * output_block, size_t probe_rows,                \
            bool is_mark_join, bool have_other_join_conjunct);                                    \
    template Status                                                                               \
    ProcessHashTableProbe<JoinOpType, Parent>::process<false, true, ExtractType<void(T)>::Type>(  \
            ExtractType<void(T)>::Type & hash_table_ctx, ConstNullMapPtr null_map,                \
            MutableBlock & mutable_block, Block * output_block, size_t probe_rows,                \
            bool is_mark_join, bool have_other_join_conjunct);                                    \
    template Status                                                                               \
    ProcessHashTableProbe<JoinOpType, Parent>::process<true, false, ExtractType<void(T)>::Type>(  \
            ExtractType<void(T)>::Type & hash_table_ctx, ConstNullMapPtr null_map,                \
            MutableBlock & mutable_block, Block * output_block, size_t probe_rows,                \
            bool is_mark_join, bool have_other_join_conjunct);                                    \
    template Status                                                                               \
    ProcessHashTableProbe<JoinOpType, Parent>::process<true, true, ExtractType<void(T)>::Type>(   \
            ExtractType<void(T)>::Type & hash_table_ctx, ConstNullMapPtr null_map,                \
            MutableBlock & mutable_block, Block * output_block, size_t probe_rows,                \
            bool is_mark_join, bool have_other_join_conjunct);                                    \
                                                                                                  \
    template Status ProcessHashTableProbe<JoinOpType, Parent>::process_data_in_hashtable<         \
            ExtractType<void(T)>::Type>(ExtractType<void(T)>::Type & hash_table_ctx,              \
                                        MutableBlock & mutable_block, Block * output_block,       \
                                        bool* eos)

#define INSTANTIATION_FOR1(JoinOpType, Parent)                                            \
    template struct ProcessHashTableProbe<JoinOpType, Parent>;                            \
                                                                                          \
    INSTANTIATION(JoinOpType, Parent, (SerializedHashTableContext<RowRefList>));          \
    INSTANTIATION(JoinOpType, Parent, (I8HashTableContext<RowRefList>));                  \
    INSTANTIATION(JoinOpType, Parent, (I16HashTableContext<RowRefList>));                 \
    INSTANTIATION(JoinOpType, Parent, (I32HashTableContext<RowRefList>));                 \
    INSTANTIATION(JoinOpType, Parent, (I64HashTableContext<RowRefList>));                 \
    INSTANTIATION(JoinOpType, Parent, (I128HashTableContext<RowRefList>));                \
    INSTANTIATION(JoinOpType, Parent, (I256HashTableContext<RowRefList>));                \
    INSTANTIATION(JoinOpType, Parent, (I64FixedKeyHashTableContext<true, RowRefList>));   \
    INSTANTIATION(JoinOpType, Parent, (I64FixedKeyHashTableContext<false, RowRefList>));  \
    INSTANTIATION(JoinOpType, Parent, (I128FixedKeyHashTableContext<true, RowRefList>));  \
    INSTANTIATION(JoinOpType, Parent, (I128FixedKeyHashTableContext<false, RowRefList>)); \
    INSTANTIATION(JoinOpType, Parent, (I256FixedKeyHashTableContext<true, RowRefList>));  \
    INSTANTIATION(JoinOpType, Parent, (I256FixedKeyHashTableContext<false, RowRefList>)); \
    INSTANTIATION(JoinOpType, Parent, (I136FixedKeyHashTableContext<true, RowRefList>));  \
    INSTANTIATION(JoinOpType, Parent, (I136FixedKeyHashTableContext<false, RowRefList>));

#define INSTANTIATION_FOR(JoinOpType)             \
    INSTANTIATION_FOR1(JoinOpType, HashJoinNode); \
    INSTANTIATION_FOR1(JoinOpType, pipeline::HashJoinProbeLocalState)

} // namespace doris::vectorized
