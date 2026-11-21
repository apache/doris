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

#include "load/memtable/memtable_unique_keys.h"

#include <gen_cpp/olap_file.pb.h>
#include <pdqsort.h>

#include <iterator>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/config.h"
#include "core/assert_cast.h"
#include "core/column/column_complex.h"
#include "core/column/column_vector.h"
#include "core/value/bitmap_value.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "load/memtable/memtable_sort.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/resource_context.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"
#include "util/stopwatch.hpp"

namespace doris {

UniqueKeysMemTable::UniqueKeysMemTable(int64_t tablet_id,
                                       std::shared_ptr<TabletSchema> tablet_schema,
                                       const std::vector<SlotDescriptor*>* slot_descs,
                                       TupleDescriptor* tuple_desc, bool enable_unique_key_mow,
                                       PartialUpdateInfo* partial_update_info,
                                       const std::shared_ptr<ResourceContext>& resource_ctx,
                                       bool need_lsn)
        : AggregatingMemTable(tablet_id, std::move(tablet_schema), slot_descs, tuple_desc,
                              partial_update_info, resource_ctx, need_lsn),
          _enable_unique_key_mow(enable_unique_key_mow) {
    if (partial_update_info != nullptr) {
        _partial_update_mode = partial_update_info->update_mode();
    }
}

void UniqueKeysMemTable::_init_aggregation_columns() {
    if (_tablet_schema->has_sequence_col()) {
        if (_partial_update_mode == UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS) {
            // for unique key fixed partial update, sequence column index in block
            // may be different with the index in `_tablet_schema`
            const auto& names = _input_mutable_block.get_names();
            for (int32_t i = 0; i < _input_mutable_block.columns(); i++) {
                if (names[i] == SEQUENCE_COL) {
                    _seq_col_idx_in_block = i;
                    break;
                }
            }
        } else {
            _seq_col_idx_in_block = _tablet_schema->sequence_col_idx();
        }
    }
    if (_partial_update_mode == UniqueKeyUpdateModePB::UPDATE_FLEXIBLE_COLUMNS &&
        _tablet_schema->has_skip_bitmap_col()) {
        // init of _skip_bitmap_col_idx and _delete_sign_col_idx must be before _init_agg_functions()
        _skip_bitmap_col_idx = _tablet_schema->skip_bitmap_col_idx();
        _delete_sign_col_idx = _tablet_schema->delete_sign_idx();
        _delete_sign_col_unique_id = _tablet_schema->column(_delete_sign_col_idx).unique_id();
        if (_seq_col_idx_in_block != -1) {
            _seq_col_unique_id = _tablet_schema->column(_seq_col_idx_in_block).unique_id();
        }
    }
}

AggregateFunctionPtr UniqueKeysMemTable::_make_agg_function(int cid) {
    if (!_enable_unique_key_mow) {
        return AggregatingMemTable::_make_agg_function(cid);
    }
    // In such table, non-key column's aggregation type is NONE, so we need to construct
    // the aggregate function manually.
    const auto& data_type = _input_mutable_block.get_datatype_by_position(cid);
    if (_skip_bitmap_col_idx != cid) {
        return AggregateFunctionSimpleFactory::instance().get(
                "replace_load", {data_type}, data_type, data_type->is_nullable(),
                BeExecVersionManager::get_newest_version());
    }
    return AggregateFunctionSimpleFactory::instance().get(
            "bitmap_intersect", {data_type}, data_type, false,
            BeExecVersionManager::get_newest_version());
}

int64_t UniqueKeysMemTable::_flush_buffer_size() const {
    auto max_size = _adaptive_write_buffer_size();
    if (_partial_update_mode == UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS) {
        auto update_columns_size = _num_columns;
        auto min_buffer_size = config::min_write_buffer_size_for_partial_update;
        max_size = max_size * update_columns_size / _tablet_schema->num_columns();
        max_size = max_size > min_buffer_size ? max_size : min_buffer_size;
    }
    return max_size;
}

Status UniqueKeysMemTable::_after_aggregate() {
    if (!_enable_unique_key_mow || _tablet_schema->cluster_key_uids().empty()) {
        return Status::OK();
    }
    if (_partial_update_mode != UniqueKeyUpdateModePB::UPSERT) {
        return Status::InternalError("Partial update for mow with cluster keys is not supported");
    }
    return _sort_by_cluster_keys();
}

Status UniqueKeysMemTable::_sort_by_cluster_keys() {
    SCOPED_RAW_TIMER(&_stat.sort_ns);
    _stat.sort_times++;
    // sort all rows
    Block in_block = _output_mutable_block.to_block();
    auto clone_block = in_block.clone_without_columns();
    MutableBlock mutable_block = MutableBlock::build_mutable_block(std::move(in_block));
    _output_mutable_block = MutableBlock::build_mutable_block(std::move(clone_block));

    DorisVector<std::shared_ptr<RowInBlock>> row_in_blocks;
    row_in_blocks.reserve(mutable_block.rows());
    if (_need_lsn) {
        DCHECK_EQ(_output_allocated_lsns->size(), mutable_block.rows());
    }
    auto rows = make_row_batch(mutable_block.rows(), [&](size_t i) {
        return RowInBlock(i, _need_lsn ? (*_output_allocated_lsns)[i] : 0);
    });
    for (size_t i = 0; i < mutable_block.rows(); ++i) {
        row_in_blocks.emplace_back(rows, &rows[i]);
    }
    if (_need_lsn) {
        _output_allocated_lsns = std::make_shared<std::vector<int64_t>>();
        _output_allocated_lsns->reserve(mutable_block.rows());
    }
    Tie tie = Tie(0, mutable_block.rows());

    for (auto cid : _tablet_schema->cluster_key_uids()) {
        auto index = _tablet_schema->field_index(cid);
        if (index == -1) {
            return Status::InternalError("could not find cluster key column with unique_id=" +
                                         std::to_string(cid) + " in tablet schema");
        }
        auto cmp = [&](const std::shared_ptr<RowInBlock>& lhs,
                       const std::shared_ptr<RowInBlock>& rhs) -> int {
            return mutable_block.compare_one_column(lhs->_row_pos, rhs->_row_pos, index, -1);
        };
        memtable_detail::sort_one_column(row_in_blocks, tie, cmp);
    }

    // sort extra round by _row_pos to make the sort stable
    auto iter = tie.iter();
    while (iter.next()) {
        pdqsort(std::next(row_in_blocks.begin(), iter.left()),
                std::next(row_in_blocks.begin(), iter.right()),
                [](const std::shared_ptr<RowInBlock>& lhs, const std::shared_ptr<RowInBlock>& rhs)
                        -> bool { return lhs->_row_pos < rhs->_row_pos; });
    }

    in_block = mutable_block.to_block();
    SCOPED_RAW_TIMER(&_stat.put_into_output_ns);
    DorisVector<uint32_t> row_pos_vec;
    DCHECK(in_block.rows() <= std::numeric_limits<int>::max());
    row_pos_vec.reserve(in_block.rows());
    for (int i = 0; i < row_in_blocks.size(); i++) {
        row_pos_vec.emplace_back(row_in_blocks[i]->_row_pos);
        _append_output_allocated_lsn(row_in_blocks[i].get());
    }
    std::vector<int> column_offset;
    for (int i = 0; i < _column_offset.size(); ++i) {
        column_offset.emplace_back(i);
    }
    return _output_mutable_block.add_rows(&in_block, row_pos_vec.data(),
                                          row_pos_vec.data() + in_block.rows(), &column_offset);
}

void UniqueKeysMemTable::_aggregate_flexible_partial_update(
        bool is_final, MutableBlock& mutable_block,
        DorisVector<std::shared_ptr<RowInBlock>>& temp_row_in_blocks) {
    DCHECK(_delete_sign_col_idx != -1);
    if (_seq_col_idx_in_block == -1) {
        if (is_final) {
            _aggregate_for_flexible_partial_update_without_seq_col<true>(mutable_block,
                                                                         temp_row_in_blocks);
        } else {
            _aggregate_for_flexible_partial_update_without_seq_col<false>(mutable_block,
                                                                          temp_row_in_blocks);
        }
    } else {
        if (is_final) {
            _aggregate_for_flexible_partial_update_with_seq_col<true>(mutable_block,
                                                                      temp_row_in_blocks);
        } else {
            _aggregate_for_flexible_partial_update_with_seq_col<false>(mutable_block,
                                                                       temp_row_in_blocks);
        }
    }
}

template <bool is_final>
void UniqueKeysMemTable::_aggregate_for_flexible_partial_update_without_seq_col(
        MutableBlock& mutable_block, DorisVector<std::shared_ptr<RowInBlock>>& temp_row_in_blocks) {
    std::shared_ptr<RowInBlock> prev_row {nullptr};
    int row_pos = -1;
    auto& skip_bitmaps =
            assert_cast<ColumnBitmap*>(mutable_block.mutable_columns()[_skip_bitmap_col_idx].get())
                    ->get_data();
    auto& delete_signs =
            assert_cast<ColumnInt8*>(mutable_block.mutable_columns()[_delete_sign_col_idx].get())
                    ->get_data();
    std::shared_ptr<RowInBlock> row_with_delete_sign {nullptr};
    std::shared_ptr<RowInBlock> row_without_delete_sign {nullptr};

    auto finalize_rows = [&]() {
        if (row_with_delete_sign != nullptr) {
            temp_row_in_blocks.push_back(row_with_delete_sign);
            _finalize_one_row<is_final>(row_with_delete_sign.get(), mutable_block, ++row_pos);
            row_with_delete_sign = nullptr;
        }
        if (row_without_delete_sign != nullptr) {
            temp_row_in_blocks.push_back(row_without_delete_sign);
            _finalize_one_row<is_final>(row_without_delete_sign.get(), mutable_block, ++row_pos);
            row_without_delete_sign = nullptr;
        }
        // _arena.clear();
    };

    auto add_row = [&](std::shared_ptr<RowInBlock> row, bool with_delete_sign) {
        if (with_delete_sign) {
            row_with_delete_sign = std::move(row);
        } else {
            row_without_delete_sign = std::move(row);
        }
    };
    for (const auto& cur_row_ptr : *_row_in_blocks) {
        RowInBlock* cur_row = cur_row_ptr.get();
        const BitmapValue& skip_bitmap = skip_bitmaps[cur_row->_row_pos];
        bool cur_row_has_delete_sign = (!skip_bitmap.contains(_delete_sign_col_unique_id) &&
                                        delete_signs[cur_row->_row_pos] != 0);
        prev_row =
                (row_with_delete_sign == nullptr) ? row_without_delete_sign : row_with_delete_sign;
        // compare keys, the keys of row_with_delete_sign and row_without_delete_sign is the same,
        // choose any of them if it's valid
        if (prev_row != nullptr &&
            _vec_row_comparator(mutable_block, prev_row.get(), cur_row) == 0) {
            if (cur_row_has_delete_sign) {
                if (row_without_delete_sign != nullptr) {
                    // if there exits row without delete sign, remove it first
                    _merge_allocated_lsn(row_without_delete_sign.get(), cur_row);
                    _clear_row_agg(row_without_delete_sign.get());
                    _stat.merged_rows++;
                    row_without_delete_sign = nullptr;
                }
                // and then unconditionally replace the previous row
                prev_row = row_with_delete_sign;
            } else {
                prev_row = row_without_delete_sign;
            }

            if (prev_row == nullptr) {
                add_row(cur_row_ptr, cur_row_has_delete_sign);
            } else {
                if (!prev_row->has_init_agg()) {
                    _init_row_for_agg(prev_row.get(), mutable_block);
                }
                _stat.merged_rows++;
                _aggregate_two_row_in_block<true>(mutable_block, cur_row, prev_row.get());
            }
        } else {
            finalize_rows();
            add_row(cur_row_ptr, cur_row_has_delete_sign);
        }
    }
    // finalize the last lows
    finalize_rows();
}

template <bool is_final>
void UniqueKeysMemTable::_aggregate_for_flexible_partial_update_with_seq_col(
        MutableBlock& mutable_block, DorisVector<std::shared_ptr<RowInBlock>>& temp_row_in_blocks) {
    // For flexible partial update, when table has sequence column, we don't do any aggregation
    // in memtable. These duplicate rows will be aggregated by the BlockAggregator
    // in the flush transform chain
    int row_pos = -1;
    for (const auto& row_ptr : *_row_in_blocks) {
        RowInBlock* row = row_ptr.get();
        temp_row_in_blocks.push_back(row_ptr);
        _finalize_one_row<is_final>(row, mutable_block, ++row_pos);
    }
}

} // namespace doris
