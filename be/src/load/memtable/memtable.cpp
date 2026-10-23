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

#include "load/memtable/memtable.h"

#include <fmt/format.h>
#include <gen_cpp/olap_file.pb.h>
#include <pdqsort.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "bvar/bvar.h"
#include "common/config.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "exec/common/arithmetic_overflow.h"
#include "exprs/aggregate/aggregate_function_reader.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "load/delta_writer/delta_writer_context.h"
#include "load/memtable/memtable_memory_limiter.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_profile.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/resource_context.h"
#include "storage/olap_define.h"
#include "storage/sparse_flush_output.h"
#include "storage/tablet/tablet_schema.h"
#include "util/debug_points.h"
#include "util/stopwatch.hpp"

namespace doris {

bvar::Adder<int64_t> g_memtable_cnt("memtable_cnt");
bvar::Adder<uint64_t> g_flush_cuz_memtable_full("flush_cuz_memtable_full");
bvar::Adder<uint64_t> g_sparse_memtable_encoded_columns("sparse_memtable_encoded_columns");
bvar::Adder<uint64_t> g_sparse_memtable_fallback_columns("sparse_memtable_fallback_columns");
bvar::Adder<uint64_t> g_sparse_memtable_flush_fallbacks("sparse_memtable_flush_fallbacks");

using namespace ErrorCode;

namespace {

// Share one tracked allocation across a batch. Allocating each row separately also
// retains an address record and a stack trace per row when memory diagnostics are enabled.
template <typename RowFactory>
std::shared_ptr<RowInBlock[]> make_row_batch(size_t num_rows, RowFactory&& make_row) {
    auto rows = std::allocate_shared<RowInBlock[]>(CustomStdAllocator<RowInBlock>(), num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        rows[i] = make_row(i);
    }
    return rows;
}

} // namespace

MemTable::MemTable(int64_t tablet_id, std::shared_ptr<TabletSchema> tablet_schema,
                   const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
                   bool enable_unique_key_mow, PartialUpdateInfo* partial_update_info,
                   const std::shared_ptr<ResourceContext>& resource_ctx, bool need_lsn)
        : _mem_type(MemType::ACTIVE),
          _tablet_id(tablet_id),
          _enable_unique_key_mow(enable_unique_key_mow),
          _keys_type(tablet_schema->keys_type()),
          _tablet_schema(tablet_schema),
          _resource_ctx(resource_ctx),
          _need_lsn(need_lsn),
          _is_first_insertion(true),
          _agg_functions(tablet_schema->num_columns()),
          _offsets_of_aggregate_states(tablet_schema->num_columns()),
          _total_size_of_aggregate_states(0),
          _enable_sparse_snapshot(config::enable_sparse_memtable),
          _sparse_options {
                  .sample_rows = kSparseMemtableSampleRows,
                  .max_non_null_ratio = kSparseMemtableMaxNonNullRatio,
                  .min_saving_ratio = kSparseMemtableMinSavingRatio,
                  .materialize_limit_bytes =
                          static_cast<size_t>(config::sparse_memtable_materialize_limit_bytes),
                  .max_rows = static_cast<uint64_t>(config::sparse_memtable_max_rows),
          } {
    g_memtable_cnt << 1;
    _mem_tracker = std::make_shared<MemTracker>();
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _resource_ctx->memory_context()->mem_tracker()->write_tracker());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    _vec_row_comparator = std::make_shared<RowInBlockComparator>(_tablet_schema);
    if (partial_update_info != nullptr) {
        _partial_update_mode = partial_update_info->update_mode();
        if (_partial_update_mode == UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS) {
            if (partial_update_info->is_schema_contains_auto_inc_column &&
                !partial_update_info->is_input_columns_contains_auto_inc_column) {
                _is_partial_update_and_auto_inc = true;
            }
        }
    }
    _init_columns_offset_by_slot_descs(slot_descs, tuple_desc);
    _row_lsn_col_pos = tablet_schema->row_lsn_col_idx();
    // TODO: Support ZOrderComparator in the future
    _row_in_blocks = std::make_unique<DorisVector<std::shared_ptr<RowInBlock>>>();
    _load_mem_limit = MemInfo::mem_limit() * config::load_process_max_memory_limit_percent / 100;
}

void MemTable::_init_columns_offset_by_slot_descs(const std::vector<SlotDescriptor*>* slot_descs,
                                                  const TupleDescriptor* tuple_desc) {
    for (auto slot_desc : *slot_descs) {
        const auto& slots = tuple_desc->slots();
        for (int j = 0; j < slots.size(); ++j) {
            if (slot_desc->id() == slots[j]->id()) {
                _column_offset.emplace_back(j);
                int schema_idx = -1;
                if (slot_desc->col_unique_id() >= 0) {
                    schema_idx = _tablet_schema->field_index(slot_desc->col_unique_id());
                }
                if (schema_idx < 0) {
                    schema_idx = _tablet_schema->field_index(slot_desc->col_name());
                }
                _schema_column_index.emplace_back(schema_idx);
                break;
            }
        }
    }
    if (_is_partial_update_and_auto_inc) {
        _column_offset.emplace_back(_column_offset.size());
        int schema_idx = -1;
        for (int i = 0; i < _tablet_schema->num_columns(); ++i) {
            if (_tablet_schema->column(i).is_auto_increment()) {
                schema_idx = i;
                break;
            }
        }
        _schema_column_index.emplace_back(schema_idx);
    }
    DCHECK_EQ(_column_offset.size(), _schema_column_index.size());
    _num_columns = _column_offset.size();
}

void MemTable::_init_agg_functions(const Block* block) {
    if (_num_columns > _column_offset.size()) [[unlikely]] {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "num_columns {} is greater than block columns {}", _num_columns,
                               _column_offset.size());
    }
    for (auto cid = _tablet_schema->num_key_columns(); cid < _num_columns; ++cid) {
        AggregateFunctionPtr function;
        if (_keys_type == KeysType::UNIQUE_KEYS && _enable_unique_key_mow) {
            // In such table, non-key column's aggregation type is NONE, so we need to construct
            // the aggregate function manually.
            if (_skip_bitmap_col_idx != cid) {
                function = AggregateFunctionSimpleFactory::instance().get(
                        "replace_load", {block->get_data_type(cid)}, block->get_data_type(cid),
                        block->get_data_type(cid)->is_nullable(),
                        BeExecVersionManager::get_newest_version());
            } else {
                function = AggregateFunctionSimpleFactory::instance().get(
                        "bitmap_intersect", {block->get_data_type(cid)}, block->get_data_type(cid),
                        false, BeExecVersionManager::get_newest_version());
            }
        } else {
            function = _tablet_schema->column(cid).get_aggregate_function(
                    AGG_LOAD_SUFFIX, _tablet_schema->column(cid).get_be_exec_version(),
                    block->get_data_type(cid));
            if (function == nullptr) {
                LOG(WARNING) << "column get aggregate function failed, column="
                             << _tablet_schema->column(cid).name();
            }
        }

        DCHECK(function != nullptr);
        const auto* input_column = _input_mutable_block.get_column_by_position(cid).get();
        const IColumn* columns[] = {input_column};
        function->check_input_columns_type(columns);
        function->check_result_column_type(*_output_mutable_block.get_column_by_position(cid));
        _agg_functions[cid] = function;
    }

    for (auto cid = _tablet_schema->num_key_columns(); cid < _num_columns; ++cid) {
        _offsets_of_aggregate_states[cid] = _total_size_of_aggregate_states;
        _total_size_of_aggregate_states += _agg_functions[cid]->size_of_data();

        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        if (cid + 1 < _num_columns) {
            size_t alignment_of_next_state = _agg_functions[cid + 1]->align_of_data();

            /// Extend total_size to next alignment requirement
            /// Add padding by rounding up 'total_size_of_aggregate_states' to be a multiplier of alignment_of_next_state.
            _total_size_of_aggregate_states =
                    (_total_size_of_aggregate_states + alignment_of_next_state - 1) /
                    alignment_of_next_state * alignment_of_next_state;
        }
    }
}

MemTable::~MemTable() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _resource_ctx->memory_context()->mem_tracker()->write_tracker());
    {
        SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
        g_memtable_cnt << -1;
        if (_keys_type != KeysType::DUP_KEYS) {
            for (auto it = _row_in_blocks->begin(); it != _row_in_blocks->end(); it++) {
                if (!(*it)->has_init_agg()) {
                    continue;
                }
                // We should release agg_places here, because they are not released when a
                // load is canceled.
                for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
                    auto function = _agg_functions[i];
                    DCHECK(function != nullptr);
                    function->destroy((*it)->agg_places(i));
                }
            }
        }

        _arena.clear(true);
        _vec_row_comparator.reset();
        _row_in_blocks.reset();
        _agg_functions.clear();
        _sparse_columns.clear();
        _sparse_columns.shrink_to_fit();
        _input_mutable_block.clear();
        _output_mutable_block.clear();
        // Reset the LSN sidecar to release its capacity tracked by the memtable tracker.
        _output_allocated_lsns = std::make_shared<std::vector<int64_t>>();
    }
    if (_is_flush_success) {
        // If the memtable is flush success, then its memtracker's consumption should be 0
        if (_mem_tracker->consumption() != 0 && config::crash_in_memory_tracker_inaccurate) {
            LOG(FATAL) << "memtable flush success but cosumption is not 0, it is "
                       << _mem_tracker->consumption();
        }
    }
}

int RowInBlockComparator::operator()(const RowInBlock* left, const RowInBlock* right) const {
    return _pblock->compare_at(left->_row_pos, right->_row_pos, _tablet_schema->num_key_columns(),
                               *_pblock, -1);
}

Status MemTable::insert(const Block* input_block, const TabletAddRowsPayload& rows) {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _resource_ctx->memory_context()->mem_tracker()->write_tracker());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    const auto& row_idxs = rows.row_idxs;
    const auto& allocated_lsns = rows.allocated_lsns;

    if (_need_lsn) {
        if (allocated_lsns.empty()) {
            return Status::InternalError(
                    "allocated lsn is missing for memtable insert, "
                    "tablet_id={}",
                    _tablet_id);
        }
        DCHECK_EQ(allocated_lsns.size(), row_idxs.size());
    } else if (!allocated_lsns.empty()) {
        return Status::InternalError(
                "allocated lsn is unexpectedly provided for memtable insert, tablet_id={}",
                _tablet_id);
    }

    if (_is_first_insertion) {
        _is_first_insertion = false;
        auto clone_block = input_block->clone_without_columns(&_column_offset);
        _input_mutable_block = MutableBlock::build_mutable_block(std::move(clone_block));
        _vec_row_comparator->set_block(&_input_mutable_block);
        clone_block = input_block->clone_without_columns(&_column_offset);
        _output_mutable_block = MutableBlock::build_mutable_block(std::move(clone_block));
        if (_tablet_schema->has_sequence_col()) {
            if (_partial_update_mode == UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS) {
                // for unique key fixed partial update, sequence column index in block
                // may be different with the index in `_tablet_schema`
                for (int32_t i = 0; i < clone_block.columns(); i++) {
                    if (clone_block.get_by_position(i).name == SEQUENCE_COL) {
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
        if (_keys_type != KeysType::DUP_KEYS) {
            // there may be additional intermediate columns in input_block
            // we only need columns indicated by column offset in the output
            RETURN_IF_CATCH_EXCEPTION(_init_agg_functions(&clone_block));
        }
        // Keep the disabled path identical to the normal dense MemTable initialization. In
        // particular, do not scan the schema or allocate sparse sidecars when the POC is off.
        if (UNLIKELY(_enable_sparse_snapshot)) {
            RETURN_IF_CATCH_EXCEPTION(_init_sparse_columns(clone_block));
        }
    }

    auto num_rows = row_idxs.size();
    size_t cursor_in_mutableblock = _row_in_blocks->size();
    if (UNLIKELY(_enable_sparse)) {
        RETURN_IF_ERROR(_append_rows_by_sparse_state(input_block, row_idxs));
    } else {
        RETURN_IF_ERROR(_input_mutable_block.add_rows(input_block, row_idxs.data(),
                                                      row_idxs.data() + num_rows, &_column_offset));
    }
    if (_need_lsn && _row_lsn_col_pos >= 0) {
        auto lsn_column = ColumnInt64::create();
        lsn_column->get_data().assign(allocated_lsns.begin(), allocated_lsns.end());
        _input_mutable_block.get_column_by_position(_row_lsn_col_pos)
                ->replace_column_data_range(*lsn_column, 0, num_rows, cursor_in_mutableblock);
    }
    DBUG_EXECUTE_IF("MemTable.insert.row_batch_allocation", DBUG_RUN_CALLBACK(this));
    auto row_batch = make_row_batch(num_rows, [&](size_t i) {
        return RowInBlock(cursor_in_mutableblock + i, _need_lsn ? allocated_lsns[i] : 0);
    });
    for (size_t i = 0; i < num_rows; ++i) {
        _row_in_blocks->emplace_back(row_batch, &row_batch[i]);
    }

    _stat.raw_rows += num_rows;
    if (UNLIKELY(_enable_sparse)) {
        RETURN_IF_CATCH_EXCEPTION(_make_sparse_column_decisions());
#ifndef NDEBUG
        _check_sparse_invariants();
#endif
    }
    return Status::OK();
}

void MemTable::_merge_allocated_lsn(RowInBlock* src_row, RowInBlock* dst_row) {
    if (_need_lsn) {
        dst_row->_allocated_lsn = std::max(dst_row->_allocated_lsn, src_row->_allocated_lsn);
    }
}

void MemTable::_append_output_allocated_lsn(RowInBlock* row) {
    if (_need_lsn) {
        _output_allocated_lsns->emplace_back(row->_allocated_lsn);
    }
}

void MemTable::_aggregate_two_row_with_sequence_map(MutableBlock& mutable_block,
                                                    RowInBlock* src_row, RowInBlock* dst_row) {
    _merge_allocated_lsn(src_row, dst_row);
    // for each mapping replace value columns according to the sequence column compare result
    // for example: a b c d s1 s2  (key:a , s1=>[b,c], s2=>[d])
    // src row:     1 4 5 6  8 9
    // dst row:     1 2 3 4  7 10
    // after aggregate
    // dst row:     1 4 5 4  8 10  (b,c,s1 will be replaced, d,s2)
    const auto& seq_map = _tablet_schema->seq_col_idx_to_value_cols_idx();
    for (const auto& it : seq_map) {
        auto sequence = it.first;
        auto* sequence_col_ptr = mutable_block.mutable_columns()[sequence].get();
        auto res = sequence_col_ptr->compare_at(dst_row->_row_pos, src_row->_row_pos,
                                                *sequence_col_ptr, -1);
        if (res > 0) {
            continue;
        }
        for (auto cid : it.second) {
            if (cid < _num_columns) {
                auto* col_ptr = mutable_block.mutable_columns()[cid].get();
                _agg_functions[cid]->add(dst_row->agg_places(cid),
                                         const_cast<const doris::IColumn**>(&col_ptr),
                                         src_row->_row_pos, _arena);
            }
        }
        if (sequence < _num_columns) {
            _agg_functions[sequence]->add(dst_row->agg_places(sequence),
                                          const_cast<const doris::IColumn**>(&sequence_col_ptr),
                                          src_row->_row_pos, _arena);
            // must use replace column instead of update row_pos
            // because one row may have multi sequence column
            // and agg function add method won't change the real column value
            sequence_col_ptr->replace_column_data(*sequence_col_ptr, src_row->_row_pos,
                                                  dst_row->_row_pos);
        }
    }
}

bool MemTable::_is_sparse_supported_type(FieldType type) const {
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_BOOL:
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
    case FieldType::OLAP_FIELD_TYPE_INT:
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
    case FieldType::OLAP_FIELD_TYPE_FLOAT:
    case FieldType::OLAP_FIELD_TYPE_DOUBLE:
    case FieldType::OLAP_FIELD_TYPE_DATE:
    case FieldType::OLAP_FIELD_TYPE_DATEV2:
    case FieldType::OLAP_FIELD_TYPE_DATETIME:
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
    case FieldType::OLAP_FIELD_TYPE_CHAR:
    case FieldType::OLAP_FIELD_TYPE_VARCHAR:
    case FieldType::OLAP_FIELD_TYPE_STRING:
        return true;
    default:
        return false;
    }
}

void MemTable::_init_sparse_columns(const Block& clone_block) {
    _enable_sparse = _enable_sparse_snapshot &&
                     (_keys_type == KeysType::DUP_KEYS || _keys_type == KeysType::UNIQUE_KEYS) &&
                     _partial_update_mode == UniqueKeyUpdateModePB::UPSERT &&
                     _tablet_schema->num_key_columns() > 0 &&
                     _tablet_schema->cluster_key_uids().empty() &&
                     !_tablet_schema->has_row_store_for_all_columns() &&
                     _tablet_schema->row_columns_uids().empty();
    if (!_enable_sparse) {
        return;
    }

    _sparse_columns.resize(_num_columns);
    bool has_candidate = false;
    for (size_t cid = 0; cid < _num_columns; ++cid) {
        const auto& block_col = clone_block.get_by_position(cid);
        const int32_t schema_idx = _schema_column_index[cid];
        if (schema_idx < 0) {
            continue;
        }
        const auto& tablet_col = _tablet_schema->column(schema_idx);
        if (!tablet_col.is_nullable() || tablet_col.is_key() || tablet_col.is_row_store_column() ||
            schema_idx == _tablet_schema->sequence_col_idx() ||
            schema_idx == _tablet_schema->delete_sign_idx() ||
            schema_idx == _tablet_schema->skip_bitmap_col_idx() ||
            schema_idx == _tablet_schema->version_col_idx() ||
            schema_idx == _tablet_schema->row_lsn_col_idx() ||
            _tablet_schema->seq_col_idx_to_value_cols_idx().contains(schema_idx) ||
            !_is_sparse_supported_type(tablet_col.type()) || !block_col.type->is_nullable()) {
            continue;
        }
        _sparse_columns[cid].state = SparseState::COLLECTING_SAMPLE;
        has_candidate = true;
        ++_stat.sparse_candidate_columns;
    }
    if (!has_candidate) {
        _enable_sparse = false;
        _sparse_columns.clear();
        _sparse_columns.shrink_to_fit();
    }
}

void MemTable::_append_sparse_column_data(SparseNullableSidecar& sidecar, const IColumn& src,
                                          const DorisVector<uint32_t>& row_idxs) {
    const auto& nullable = assert_cast<const ColumnNullable&>(src);
    const auto& null_map = nullable.get_null_map_data();
    const auto& nested = nullable.get_nested_column();
    for (uint32_t row_idx : row_idxs) {
        if (!null_map[row_idx]) {
            sidecar.data.rowids.push_back(static_cast<uint32_t>(sidecar.data.row_count));
            sidecar.data.values->insert_from(nested, row_idx);
            ++sidecar.non_null_count;
        }
        ++sidecar.data.row_count;
    }
}

Status MemTable::_append_rows_by_sparse_state(const Block* input_block,
                                              const DorisVector<uint32_t>& row_idxs) {
    if (!_enable_sparse) {
        return _input_mutable_block.add_rows(input_block, row_idxs.data(),
                                             row_idxs.data() + row_idxs.size(), &_column_offset);
    }
    RETURN_IF_CATCH_EXCEPTION({
        const auto& block_data = input_block->get_columns_with_type_and_name();
        for (size_t cid = 0; cid < _num_columns; ++cid) {
            auto& sidecar = _sparse_columns[cid];
            const auto& src = *block_data[_column_offset[cid]].column;
            constexpr uint64_t max_sparse_rows =
                    static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1;
            if (sidecar.encoded() && (sidecar.data.row_count > max_sparse_rows ||
                                      row_idxs.size() > max_sparse_rows - sidecar.data.row_count)) {
                RETURN_IF_ERROR(_materialize_sparse_column(cid, true));
            }
            if (!sidecar.encoded()) {
                _input_mutable_block.get_column_by_position(cid)->insert_indices_from(
                        src, row_idxs.data(), row_idxs.data() + row_idxs.size());
                continue;
            }

            _append_sparse_column_data(sidecar, src, row_idxs);
            if (sidecar.non_null_count != 0) {
                sidecar.state = SparseState::SPARSE;
            }
            const double ratio = sidecar.data.row_count == 0
                                         ? 0
                                         : static_cast<double>(sidecar.non_null_count) /
                                                   static_cast<double>(sidecar.data.row_count);
            if (ratio > _sparse_options.max_non_null_ratio) {
                RETURN_IF_ERROR(_materialize_sparse_column(cid, true));
            }
        }
    });
    return Status::OK();
}

void MemTable::_make_sparse_column_decisions() {
    if (!_enable_sparse || std::cmp_less(_stat.raw_rows.load(), _sparse_options.sample_rows)) {
        return;
    }
    for (size_t cid = 0; cid < _sparse_columns.size(); ++cid) {
        auto& sidecar = _sparse_columns[cid];
        if (sidecar.state != SparseState::COLLECTING_SAMPLE) {
            continue;
        }
        auto& dense_ptr = _input_mutable_block.get_column_by_position(cid);
        const auto& nullable = assert_cast<const ColumnNullable&>(*dense_ptr);
        constexpr uint64_t max_sparse_rows =
                static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1;
        if (nullable.size() > max_sparse_rows) {
            sidecar.state = SparseState::PERMANENT_DENSE;
            ++_stat.sparse_fallback_columns;
            g_sparse_memtable_fallback_columns << 1;
            continue;
        }
        const auto& null_map = nullable.get_null_map_data();
        const auto& nested = nullable.get_nested_column();
        sidecar.data.row_count = nullable.size();
        sidecar.data.values = nested.clone_empty();
        for (size_t row = 0; row < nullable.size(); ++row) {
            if (!null_map[row]) {
                sidecar.data.rowids.push_back(static_cast<uint32_t>(row));
                sidecar.data.values->insert_from(nested, row);
            }
        }
        sidecar.non_null_count = sidecar.data.rowids.size();
        const double ratio = sidecar.data.row_count == 0
                                     ? 0
                                     : static_cast<double>(sidecar.non_null_count) /
                                               static_cast<double>(sidecar.data.row_count);
        const size_t dense_bytes = dense_ptr->allocated_bytes();
        size_t rowid_bytes = 0;
        if (common::mul_overflow(sidecar.data.rowids.capacity(), sizeof(uint32_t), rowid_bytes)) {
            rowid_bytes = std::numeric_limits<size_t>::max();
        }
        size_t sparse_bytes = 0;
        if (common::add_overflow(rowid_bytes, sidecar.data.values->allocated_bytes(),
                                 sparse_bytes)) {
            sparse_bytes = std::numeric_limits<size_t>::max();
        }
        const bool saves_enough =
                static_cast<long double>(sparse_bytes) <=
                static_cast<long double>(dense_bytes) * (1.0 - _sparse_options.min_saving_ratio);
        if (ratio > _sparse_options.max_non_null_ratio || !saves_enough) {
            sidecar = SparseNullableSidecar {};
            sidecar.state = SparseState::PERMANENT_DENSE;
            ++_stat.sparse_fallback_columns;
            g_sparse_memtable_fallback_columns << 1;
            continue;
        }
        sidecar.dense_bytes_per_row =
                std::max<size_t>(1, dense_bytes / sidecar.data.row_count +
                                            (dense_bytes % sidecar.data.row_count != 0));
        sidecar.state = sidecar.non_null_count == 0 ? SparseState::ALL_NULL : SparseState::SPARSE;
        dense_ptr = dense_ptr->clone_empty();
        ++_stat.sparse_encoded_columns;
        if (sidecar.state == SparseState::ALL_NULL) {
            ++_stat.sparse_all_null_columns;
        }
        g_sparse_memtable_encoded_columns << 1;
    }
}

Status MemTable::_materialize_sparse_column(size_t column_idx, bool fallback) {
    auto& sidecar = _sparse_columns[column_idx];
    if (!sidecar.encoded()) {
        return Status::OK();
    }
    auto dense = _input_mutable_block.get_column_by_position(column_idx)->clone_empty();
    auto& nullable = assert_cast<ColumnNullable&>(*dense);
    nullable.reserve(sidecar.data.row_count);
    size_t value_idx = 0;
    for (uint64_t row = 0; row < sidecar.data.row_count; ++row) {
        if (value_idx < sidecar.data.rowids.size() && sidecar.data.rowids[value_idx] == row) {
            nullable.get_nested_column().insert_from(*sidecar.data.values, value_idx++);
            nullable.get_null_map_data().push_back(0);
        } else {
            nullable.insert_default();
        }
    }
    _stat.sparse_materialize_bytes += dense->allocated_bytes();
    _input_mutable_block.get_column_by_position(column_idx) = std::move(dense);
    sidecar.data.rowids.clear();
    sidecar.data.rowids.shrink_to_fit();
    sidecar.data.values.reset();
    sidecar.state = fallback ? SparseState::PERMANENT_DENSE : SparseState::DISABLED;
    if (fallback) {
        ++_stat.sparse_fallback_columns;
        g_sparse_memtable_fallback_columns << 1;
    }
    return Status::OK();
}

Status MemTable::_materialize_sparse_columns() {
    if (!_enable_sparse) {
        return Status::OK();
    }
    MonotonicStopWatch watch;
    watch.start();
    for (size_t cid = 0; cid < _sparse_columns.size(); ++cid) {
        RETURN_IF_ERROR(_materialize_sparse_column(cid, false));
    }
    watch.stop();
    _stat.sparse_materialize_ns += watch.elapsed_time();
    return Status::OK();
}

size_t MemTable::_dense_equivalent_bytes() const {
    size_t bytes = _input_mutable_block.allocated_bytes();
    for (const auto& sidecar : _sparse_columns) {
        if (sidecar.encoded()) {
            size_t projected_dense_bytes = 0;
            if (common::mul_overflow(sidecar.dense_bytes_per_row,
                                     static_cast<size_t>(sidecar.data.row_count),
                                     projected_dense_bytes)) {
                projected_dense_bytes = std::numeric_limits<size_t>::max();
            }
            const size_t allocated_value_bytes =
                    sidecar.data.values ? sidecar.data.values->allocated_bytes() : 0;
            if (common::add_overflow(bytes, std::max(projected_dense_bytes, allocated_value_bytes),
                                     bytes)) {
                bytes = std::numeric_limits<size_t>::max();
            }
        }
    }
    return bytes;
}

size_t MemTable::_sparse_sidecar_bytes() const {
    size_t bytes = 0;
    for (const auto& sidecar : _sparse_columns) {
        if (!sidecar.encoded()) {
            continue;
        }
        size_t rowid_bytes = 0;
        if (common::mul_overflow(sidecar.data.rowids.capacity(), sizeof(uint32_t), rowid_bytes)) {
            rowid_bytes = std::numeric_limits<size_t>::max();
        }
        if (common::add_overflow(bytes, rowid_bytes, bytes)) {
            bytes = std::numeric_limits<size_t>::max();
        }
        if (sidecar.data.values) {
            if (common::add_overflow(bytes, sidecar.data.values->allocated_bytes(), bytes)) {
                bytes = std::numeric_limits<size_t>::max();
            }
        }
    }
    return bytes;
}

#ifndef NDEBUG
void MemTable::_check_sparse_invariants() const {
    const size_t rows = _stat.raw_rows;
    for (size_t cid = 0; cid < _sparse_columns.size(); ++cid) {
        const auto& sidecar = _sparse_columns[cid];
        if (!sidecar.encoded()) {
            DCHECK_EQ(_input_mutable_block.get_column_by_position(cid)->size(), rows);
            continue;
        }
        DCHECK_EQ(sidecar.data.row_count, rows);
        DCHECK(sidecar.data.values);
        DCHECK_EQ(sidecar.data.values->size(), sidecar.data.rowids.size());
        DCHECK(std::ranges::is_sorted(sidecar.data.rowids));
        if (!sidecar.data.rowids.empty()) {
            DCHECK_LT(sidecar.data.rowids.back(), sidecar.data.row_count);
        }
    }
}
#endif

template <bool has_skip_bitmap_col>
void MemTable::_aggregate_two_row_in_block(MutableBlock& mutable_block, RowInBlock* src_row,
                                           RowInBlock* dst_row) {
    _merge_allocated_lsn(src_row, dst_row);
    // for flexible partial update, the caller must guarantees that either src_row and dst_row
    // both specify the sequence column, or src_row and dst_row both don't specify the
    // sequence column
    if (_tablet_schema->has_sequence_col() && _seq_col_idx_in_block >= 0) {
        DCHECK_LT(_seq_col_idx_in_block, mutable_block.columns());
        auto col_ptr = mutable_block.mutable_columns()[_seq_col_idx_in_block].get();
        auto res = col_ptr->compare_at(dst_row->_row_pos, src_row->_row_pos, *col_ptr, -1);
        // dst sequence column larger than src, don't need to update
        if (res > 0) {
            return;
        }
        // need to update the row pos in dst row to the src row pos when has
        // sequence column
        dst_row->_row_pos = src_row->_row_pos;
    }
    // dst is non-sequence row, or dst sequence is smaller
    if constexpr (!has_skip_bitmap_col) {
        DCHECK(_skip_bitmap_col_idx == -1);
        for (size_t cid = _tablet_schema->num_key_columns(); cid < _num_columns; ++cid) {
            auto* col_ptr = mutable_block.mutable_columns()[cid].get();
            _agg_functions[cid]->add(dst_row->agg_places(cid),
                                     const_cast<const doris::IColumn**>(&col_ptr),
                                     src_row->_row_pos, _arena);
        }
    } else {
        DCHECK(_skip_bitmap_col_idx != -1);
        DCHECK_LT(_skip_bitmap_col_idx, mutable_block.columns());
        const BitmapValue& skip_bitmap =
                assert_cast<ColumnBitmap*, TypeCheckOnRelease::DISABLE>(
                        mutable_block.mutable_columns()[_skip_bitmap_col_idx].get())
                        ->get_data()[src_row->_row_pos];
        for (size_t cid = _tablet_schema->num_key_columns(); cid < _num_columns; ++cid) {
            const auto& col = _tablet_schema->column(cid);
            if (cid != _skip_bitmap_col_idx && skip_bitmap.contains(col.unique_id())) {
                continue;
            }
            auto* col_ptr = mutable_block.mutable_columns()[cid].get();
            _agg_functions[cid]->add(dst_row->agg_places(cid),
                                     const_cast<const doris::IColumn**>(&col_ptr),
                                     src_row->_row_pos, _arena);
        }
    }
}
Status MemTable::_put_into_output(Block& in_block) {
    SCOPED_RAW_TIMER(&_stat.put_into_output_ns);
    DorisVector<uint32_t> row_pos_vec;
    DCHECK(in_block.rows() <= std::numeric_limits<int>::max());
    row_pos_vec.reserve(in_block.rows());
    if (_need_lsn) {
        _output_allocated_lsns->reserve(_output_allocated_lsns->size() + in_block.rows());
    }
    for (int i = 0; i < _row_in_blocks->size(); i++) {
        row_pos_vec.emplace_back((*_row_in_blocks)[i]->_row_pos);
        _append_output_allocated_lsn((*_row_in_blocks)[i].get());
    }
    return _output_mutable_block.add_rows(&in_block, row_pos_vec.data(),
                                          row_pos_vec.data() + in_block.rows());
}

SparseFlushCapability MemTable::_sparse_flush_static_capability() const {
    if (!_enable_sparse_snapshot) {
        return {SparseFlushCapabilityReason::CONFIG_DISABLED};
    }
    if (_keys_type != KeysType::DUP_KEYS && _keys_type != KeysType::UNIQUE_KEYS) {
        return {SparseFlushCapabilityReason::UNSUPPORTED_KEYS};
    }
    if (_partial_update_mode != UniqueKeyUpdateModePB::UPSERT) {
        return {SparseFlushCapabilityReason::PARTIAL_UPDATE};
    }
    if (_tablet_schema->num_key_columns() == 0) {
        return {SparseFlushCapabilityReason::NO_KEY_COLUMNS};
    }
    if (!_tablet_schema->cluster_key_uids().empty()) {
        return {SparseFlushCapabilityReason::CLUSTER_KEY};
    }
    if (_tablet_schema->has_row_store_for_all_columns() ||
        !_tablet_schema->row_columns_uids().empty()) {
        return {SparseFlushCapabilityReason::ROW_STORE};
    }
    if (_tablet_schema->num_variant_columns() > 0) {
        return {SparseFlushCapabilityReason::VARIANT};
    }
    return {};
}

SparseFlushCapability MemTable::_sparse_flush_capability(size_t duplicate_keys) const {
    auto result = _sparse_flush_static_capability();
    if (!result.eligible()) {
        return result;
    }
    if (!_enable_sparse || !std::ranges::any_of(_sparse_columns, [](const auto& sidecar) {
            return sidecar.encoded();
        })) {
        return {SparseFlushCapabilityReason::NO_SPARSE_COLUMNS};
    }
    if (_keys_type == KeysType::UNIQUE_KEYS && duplicate_keys != 0) {
        return {SparseFlushCapabilityReason::UNIQUE_DUPLICATES};
    }
    return {};
}

SparseFlushCapability MemTable::sparse_flush_capability_for_test(size_t duplicate_keys) const {
    return _sparse_flush_capability(duplicate_keys);
}

void MemTable::_record_sparse_flush_fallback(SparseFlushCapabilityReason reason) const {
    if (reason == SparseFlushCapabilityReason::ELIGIBLE ||
        reason == SparseFlushCapabilityReason::NO_SPARSE_COLUMNS) {
        return;
    }
    g_sparse_memtable_flush_fallbacks << 1;
    VLOG_DEBUG << "sparse flush fallback tablet=" << _tablet_id
               << " reason=" << static_cast<int>(reason);
}

Status MemTable::_put_into_sparse_output(Block& in_block,
                                         std::shared_ptr<const SparseFlushOutput>* sparse_output) {
    if (sparse_output == nullptr) {
        return Status::InvalidArgument("sparse flush output has no consumer");
    }
    SCOPED_RAW_TIMER(&_stat.put_into_output_ns);
    SparseFlushOutput::Columns columns(_num_columns);
    DorisVector<uint32_t> permutation;
    permutation.reserve(_row_in_blocks->size());
    for (const auto& row : *_row_in_blocks) {
        permutation.emplace_back(row->_row_pos);
        _append_output_allocated_lsn(row.get());
    }

    DorisVector<uint8_t> seen(permutation.size(), 0);
    DorisVector<uint32_t> inverse(permutation.size());
    for (size_t output_row = 0; output_row < permutation.size(); ++output_row) {
        uint32_t input_row = permutation[output_row];
        if (input_row >= permutation.size() || seen[input_row] != 0) {
            return Status::InternalError("invalid sparse permutation tablet={}, row={}", _tablet_id,
                                         input_row);
        }
        seen[input_row] = 1;
        inverse[input_row] = static_cast<uint32_t>(output_row);
    }

    const auto& block_data = in_block.get_columns_with_type_and_name();
    for (size_t cid = 0; cid < _num_columns; ++cid) {
        auto& sidecar = _sparse_columns[cid];
        if (!sidecar.encoded()) {
            auto& dst = _output_mutable_block.get_column_by_position(cid);
            dst->insert_indices_from(*block_data[cid].column, permutation.data(),
                                     permutation.data() + permutation.size());
            continue;
        }
        if (sidecar.data.row_count != permutation.size() ||
            sidecar.data.rowids.size() != sidecar.data.values->size()) {
            return Status::InternalError(
                    "invalid sparse output tablet={}, cid={}, rows={}, permutation={}, rowids={}, "
                    "values={}",
                    _tablet_id, cid, sidecar.data.row_count, permutation.size(),
                    sidecar.data.rowids.size(), sidecar.data.values->size());
        }

        DorisVector<std::pair<uint32_t, uint32_t>> positions;
        positions.reserve(sidecar.data.rowids.size());
        for (uint32_t value_idx = 0; value_idx < sidecar.data.rowids.size(); ++value_idx) {
            if (sidecar.data.rowids[value_idx] >= inverse.size()) {
                return Status::InternalError("invalid sparse rowid tablet={}, cid={}, row={}",
                                             _tablet_id, cid, sidecar.data.rowids[value_idx]);
            }
            positions.emplace_back(inverse[sidecar.data.rowids[value_idx]], value_idx);
        }
        std::ranges::sort(positions);

        auto sparse_column = std::make_unique<SparseFlushOutput::Column>();
        sparse_column->row_count = permutation.size();
        auto reordered_values = sidecar.data.values->clone_empty();
        reordered_values->reserve(positions.size());
        sparse_column->rowids.reserve(positions.size());
        for (const auto& [output_row, value_idx] : positions) {
            sparse_column->rowids.emplace_back(output_row);
            reordered_values->insert_from(*sidecar.data.values, value_idx);
        }
        sparse_column->values = std::move(reordered_values);
        columns[cid] = std::move(sparse_column);
    }
    RETURN_IF_ERROR(SparseFlushOutput::create(std::move(columns), sparse_output));
    // Publish the complete attachment before releasing MemTable ownership. On every error path the
    // sidecars remain intact, so the caller can destroy the MemTable normally without observing a
    // partially transferred sparse representation.
    for (auto& sidecar : _sparse_columns) {
        if (sidecar.encoded()) {
            sidecar.data.rowids.clear();
            sidecar.data.rowids.shrink_to_fit();
            sidecar.data.values.reset();
            sidecar.state = SparseState::DISABLED;
        }
    }
    return Status::OK();
}

size_t MemTable::_sort() {
    SCOPED_RAW_TIMER(&_stat.sort_ns);
    _stat.sort_times++;
    size_t same_keys_num = 0;
    // sort new rows
    Tie tie = Tie(_last_sorted_pos, _row_in_blocks->size());
    for (size_t i = 0; i < _tablet_schema->num_key_columns(); i++) {
        auto cmp = [&](RowInBlock* lhs, RowInBlock* rhs) -> int {
            return _input_mutable_block.compare_one_column(lhs->_row_pos, rhs->_row_pos, i, -1);
        };
        _sort_one_column(*_row_in_blocks, tie, cmp);
    }
    bool is_dup = (_keys_type == KeysType::DUP_KEYS);
    // sort extra round by _row_pos to make the sort stable
    auto iter = tie.iter();
    while (iter.next()) {
        pdqsort(std::next(_row_in_blocks->begin(), iter.left()),
                std::next(_row_in_blocks->begin(), iter.right()),
                [&is_dup](const std::shared_ptr<RowInBlock>& lhs,
                          const std::shared_ptr<RowInBlock>& rhs) -> bool {
                    return is_dup ? lhs->_row_pos > rhs->_row_pos : lhs->_row_pos < rhs->_row_pos;
                });
        same_keys_num += iter.right() - iter.left();
    }
    // merge new rows and old rows
    _vec_row_comparator->set_block(&_input_mutable_block);
    auto cmp_func = [this, is_dup, &same_keys_num](const std::shared_ptr<RowInBlock>& l,
                                                   const std::shared_ptr<RowInBlock>& r) -> bool {
        auto value = (*(this->_vec_row_comparator))(l.get(), r.get());
        if (value == 0) {
            same_keys_num++;
            return is_dup ? l->_row_pos > r->_row_pos : l->_row_pos < r->_row_pos;
        } else {
            return value < 0;
        }
    };
    auto new_row_it = std::next(_row_in_blocks->begin(), _last_sorted_pos);
    std::inplace_merge(_row_in_blocks->begin(), new_row_it, _row_in_blocks->end(), cmp_func);
    _last_sorted_pos = _row_in_blocks->size();
    return same_keys_num;
}

Status MemTable::_sort_by_cluster_keys() {
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
        auto cmp = [&](const RowInBlock* lhs, const RowInBlock* rhs) -> int {
            return mutable_block.compare_one_column(lhs->_row_pos, rhs->_row_pos, index, -1);
        };
        _sort_one_column(row_in_blocks, tie, cmp);
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

void MemTable::_sort_one_column(DorisVector<std::shared_ptr<RowInBlock>>& row_in_blocks, Tie& tie,
                                std::function<int(RowInBlock*, RowInBlock*)> cmp) {
    auto iter = tie.iter();
    while (iter.next()) {
        pdqsort(std::next(row_in_blocks.begin(), static_cast<int>(iter.left())),
                std::next(row_in_blocks.begin(), static_cast<int>(iter.right())),
                [&cmp](auto lhs, auto rhs) -> bool { return cmp(lhs.get(), rhs.get()) < 0; });
        tie[iter.left()] = 0;
        for (auto i = iter.left() + 1; i < iter.right(); i++) {
            tie[i] = (cmp(row_in_blocks[i - 1].get(), row_in_blocks[i].get()) == 0);
        }
    }
}

template <bool is_final>
void MemTable::_finalize_one_row(RowInBlock* row, MutableBlock& mutable_block, int row_pos) {
    // move key columns
    for (size_t i = 0; i < _tablet_schema->num_key_columns(); ++i) {
        _output_mutable_block.get_column_by_position(i)->insert_from(
                *mutable_block.get_column_by_position(i), row->_row_pos);
    }
    if (row->has_init_agg()) {
        // get value columns from agg_places
        for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
            auto function = _agg_functions[i];
            auto* agg_place = row->agg_places(i);
            auto* col_ptr = _output_mutable_block.get_column_by_position(i).get();
            function->insert_result_into(agg_place, *col_ptr);

            if constexpr (is_final) {
                function->destroy(agg_place);
            } else {
                function->reset(agg_place);
            }
        }

        if constexpr (is_final) {
            row->remove_init_agg();
        } else {
            for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
                auto function = _agg_functions[i];
                auto* agg_place = row->agg_places(i);
                auto* col_ptr = _output_mutable_block.get_column_by_position(i).get();
                function->add(agg_place, const_cast<const doris::IColumn**>(&col_ptr), row_pos,
                              _arena);
            }
        }
    } else {
        // move columns for rows do not need agg
        for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
            _output_mutable_block.get_column_by_position(i)->insert_from(
                    *mutable_block.get_column_by_position(i), row->_row_pos);
        }
    }
    _append_output_allocated_lsn(row);
    if constexpr (!is_final) {
        row->_row_pos = row_pos;
    }
}

void MemTable::_init_row_for_agg(RowInBlock* row, MutableBlock& mutable_block) {
    auto* agg_mem = _arena.aligned_alloc(_total_size_of_aggregate_states, 16);
    auto first_value_column = _tablet_schema->num_key_columns();
    auto cid = first_value_column;
    try {
        for (; cid < _num_columns; ++cid) {
            _agg_functions[cid]->create(agg_mem + _offsets_of_aggregate_states[cid]);
        }
    } catch (...) {
        // A failed constructor leaves only the completed prefix available for destruction.
        while (cid > first_value_column) {
            --cid;
            _agg_functions[cid]->destroy(agg_mem + _offsets_of_aggregate_states[cid]);
        }
        throw;
    }
    // add() can allocate. Publish the row only after all states can be safely destroyed.
    row->init_agg_places(agg_mem, _offsets_of_aggregate_states.data());
    for (cid = first_value_column; cid < _num_columns; ++cid) {
        auto* col_ptr = mutable_block.mutable_columns()[cid].get();
        auto* data = row->agg_places(cid);
        _agg_functions[cid]->add(data, const_cast<const doris::IColumn**>(&col_ptr), row->_row_pos,
                                 _arena);
    }
}
void MemTable::_clear_row_agg(RowInBlock* row) {
    if (row->has_init_agg()) {
        for (size_t i = _tablet_schema->num_key_columns(); i < _num_columns; ++i) {
            auto function = _agg_functions[i];
            auto* agg_place = row->agg_places(i);
            function->destroy(agg_place);
        }
        row->remove_init_agg();
    }
}
// only in `to_block` the `is_final` flag will be true, in other cases, it will be false
template <bool is_final, bool has_skip_bitmap_col>
void MemTable::_aggregate() {
    SCOPED_RAW_TIMER(&_stat.agg_ns);
    _stat.agg_times++;
    Block in_block = _input_mutable_block.to_block();
    std::unique_ptr<Block> empty_input_block = in_block.create_same_struct_block(0);
    MutableBlock mutable_block = MutableBlock::build_mutable_block(std::move(in_block));
    _vec_row_comparator->set_block(&mutable_block);
    DorisVector<std::shared_ptr<RowInBlock>> temp_row_in_blocks;
    temp_row_in_blocks.reserve(_last_sorted_pos);
    //only init agg if needed

    if constexpr (!has_skip_bitmap_col) {
        RowInBlock* prev_row = nullptr;
        int row_pos = -1;
        for (const auto& cur_row_ptr : *_row_in_blocks) {
            RowInBlock* cur_row = cur_row_ptr.get();
            if (!temp_row_in_blocks.empty() && (*_vec_row_comparator)(prev_row, cur_row) == 0) {
                if (!prev_row->has_init_agg()) {
                    _init_row_for_agg(prev_row, mutable_block);
                }
                _stat.merged_rows++;
                if (_tablet_schema->has_seq_map()) {
                    _aggregate_two_row_with_sequence_map(mutable_block, cur_row, prev_row);
                } else {
                    _aggregate_two_row_in_block<has_skip_bitmap_col>(mutable_block, cur_row,
                                                                     prev_row);
                }

                // Clean up aggregation state of the merged row to avoid memory leak
                if (cur_row) {
                    _clear_row_agg(cur_row);
                }
            } else {
                prev_row = cur_row;
                if (!temp_row_in_blocks.empty()) {
                    // The rows from the previous batch of _row_in_blocks have been merged into temp_row_in_blocks,
                    // now call finalize to write the aggregation results into _output_mutable_block.
                    _finalize_one_row<is_final>(temp_row_in_blocks.back().get(), mutable_block,
                                                row_pos);
                }
                temp_row_in_blocks.push_back(cur_row_ptr);
                row_pos++;
            }
        }
        if (!temp_row_in_blocks.empty()) {
            // finalize the last low
            _finalize_one_row<is_final>(temp_row_in_blocks.back().get(), mutable_block, row_pos);
        }
    } else {
        DCHECK(_delete_sign_col_idx != -1);
        if (_seq_col_idx_in_block == -1) {
            _aggregate_for_flexible_partial_update_without_seq_col<is_final>(mutable_block,
                                                                             temp_row_in_blocks);
        } else {
            _aggregate_for_flexible_partial_update_with_seq_col<is_final>(mutable_block,
                                                                          temp_row_in_blocks);
        }
    }
    if constexpr (!is_final) {
        // if is not final, we collect the agg results to input_block and then continue to insert
        _input_mutable_block.swap(_output_mutable_block);
        //TODO(weixang):opt here.
        _output_mutable_block = MutableBlock::build_mutable_block(std::move(*empty_input_block));
        _output_mutable_block.clear_column_data();
        _output_allocated_lsns = std::make_shared<std::vector<int64_t>>();
        // Repack surviving rows so they do not retain batches of merged-away rows.
        // Aggregation states belong to the memtable; preserve their pointers and flags.
        DBUG_EXECUTE_IF("MemTable.aggregate.row_batch_allocation", DBUG_RUN_CALLBACK(this));
        auto rows = make_row_batch(temp_row_in_blocks.size(),
                                   [&](size_t i) { return *temp_row_in_blocks[i]; });
        for (size_t i = 0; i < temp_row_in_blocks.size(); ++i) {
            temp_row_in_blocks[i] = {rows, &rows[i]};
        }
        *_row_in_blocks = temp_row_in_blocks;
        _last_sorted_pos = _row_in_blocks->size();
    }
}

template <bool is_final>
void MemTable::_aggregate_for_flexible_partial_update_without_seq_col(
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
        if (prev_row != nullptr && (*_vec_row_comparator)(prev_row.get(), cur_row) == 0) {
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
void MemTable::_aggregate_for_flexible_partial_update_with_seq_col(
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

void MemTable::shrink_memtable_by_agg() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(
            _resource_ctx->memory_context()->mem_tracker()->write_tracker());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    if (_keys_type == KeysType::DUP_KEYS) {
        return;
    }
    size_t same_keys_num = _sort();
    if (same_keys_num != 0) {
        (_skip_bitmap_col_idx == -1) ? _aggregate<false, false>() : _aggregate<false, true>();
    }
    _last_agg_pos = memory_usage();
}

bool MemTable::need_flush() const {
    DBUG_EXECUTE_IF("MemTable.need_flush", { return true; });
    const auto max_size = _effective_write_buffer_size();
    const auto dense_bytes = _dense_equivalent_bytes();
    if (_enable_sparse &&
        (dense_bytes >= _sparse_options.materialize_limit_bytes ||
         std::cmp_greater_equal(_stat.raw_rows.load(), _sparse_options.max_rows))) {
        g_flush_cuz_memtable_full << 1;
        return true;
    }
    if (memory_usage() >= max_size) {
        g_flush_cuz_memtable_full << 1;
        return true;
    }
    return false;
}

size_t MemTable::_effective_write_buffer_size() const {
    auto max_size = _adaptive_write_buffer_size();
    if (_partial_update_mode == UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS) {
        auto update_columns_size = _num_columns;
        auto min_buffer_size = config::min_write_buffer_size_for_partial_update;
        max_size = max_size * update_columns_size / _tablet_schema->num_columns();
        max_size = max_size > min_buffer_size ? max_size : min_buffer_size;
    }

    return max_size;
}

int64_t MemTable::_adaptive_write_buffer_size() const {
    if (!config::enable_adaptive_write_buffer_size) [[unlikely]] {
        return config::write_buffer_size;
    }
    auto* memtable_limiter = ExecEnv::GetInstance()->memtable_memory_limiter();
    int64_t memtable_mem =
            (memtable_limiter != nullptr && memtable_limiter->mem_tracker() != nullptr)
                    ? memtable_limiter->mem_tracker()->consumption()
                    : 0;
    int64_t factor = 4;
    // Memory usage intervals:
    // (80 %, 100 %] → 1× buffer
    // (50 %, 80 %]  → 2× buffer
    // [0 %, 50 %]   → 4× buffer
    if (memtable_mem > (_load_mem_limit * 4) / 5) { // > 80 %
        factor = 1;
    } else if (memtable_mem > _load_mem_limit / 2) { // > 50 %
        factor = 2;
    }
    return config::write_buffer_size * factor;
}

bool MemTable::need_agg() const {
    if (_keys_type == KeysType::AGG_KEYS) {
        auto max_size = _last_agg_pos + config::write_buffer_size_for_agg;
        return memory_usage() >= max_size;
    }
    return false;
}

size_t MemTable::get_flush_reserve_memory_size() const {
    if (!_enable_sparse && _keys_type == KeysType::DUP_KEYS &&
        _tablet_schema->num_key_columns() == 0) {
        return 0; // no need to reserve
    }
    const long double reserve = static_cast<long double>(_dense_equivalent_bytes()) * 1.2L;
    return reserve >= std::numeric_limits<size_t>::max() ? std::numeric_limits<size_t>::max()
                                                         : static_cast<size_t>(reserve);
}

Status MemTable::_put_into_flush_output(size_t same_keys_num, bool use_sparse_output,
                                        MemTableFlushOutput* output) {
    if (_keys_type != KeysType::DUP_KEYS && same_keys_num != 0) {
        (_skip_bitmap_col_idx == -1) ? _aggregate<true, false>() : _aggregate<true, true>();
        return Status::OK();
    }
    if (_keys_type == KeysType::DUP_KEYS && _tablet_schema->num_key_columns() == 0) {
        _output_mutable_block.swap(_input_mutable_block);
        if (_need_lsn) {
            _output_allocated_lsns->reserve(_row_in_blocks->size());
            for (const auto& row : *_row_in_blocks) {
                _append_output_allocated_lsn(row.get());
            }
        }
        return Status::OK();
    }
    Block in_block = _input_mutable_block.to_block();
    if (use_sparse_output) {
        return _put_into_sparse_output(in_block, &output->sparse_attachment);
    }
    return _put_into_output(in_block);
}

Status MemTable::_to_flush_output(MemTableFlushOutput* output) {
    if (output == nullptr) {
        return Status::InvalidArgument("memtable flush output is null");
    }
    *output = {};
    MemTableFlushOutput pending;
    _stat.sparse_actual_bytes_at_flush = static_cast<int64_t>(
            std::min(memory_usage(), static_cast<size_t>(std::numeric_limits<int64_t>::max())));
    _stat.sparse_dense_equivalent_bytes = static_cast<int64_t>(std::min(
            _dense_equivalent_bytes(), static_cast<size_t>(std::numeric_limits<int64_t>::max())));
    _stat.sparse_sidecar_bytes_at_flush = static_cast<int64_t>(std::min(
            _sparse_sidecar_bytes(), static_cast<size_t>(std::numeric_limits<int64_t>::max())));
    const auto static_capability = _sparse_flush_capability(0);
    const bool sparse_output_candidate = static_capability.eligible();
    if (!sparse_output_candidate) {
        _record_sparse_flush_fallback(static_capability.reason);
        RETURN_IF_ERROR(_materialize_sparse_columns());
    }
    _output_allocated_lsns = std::make_shared<std::vector<int64_t>>();
    size_t same_keys_num = _sort();
    const auto final_capability = _sparse_flush_capability(same_keys_num);
    const bool use_sparse_output = final_capability.eligible();
    if (sparse_output_candidate && !use_sparse_output) {
        _record_sparse_flush_fallback(final_capability.reason);
        // Duplicate UNIQUE keys still use the established whole-block aggregation path.
        RETURN_IF_ERROR(_materialize_sparse_columns());
    }
    RETURN_IF_ERROR(_put_into_flush_output(same_keys_num, use_sparse_output, &pending));
    if (_keys_type == KeysType::UNIQUE_KEYS && _enable_unique_key_mow &&
        !_tablet_schema->cluster_key_uids().empty()) {
        if (_partial_update_mode != UniqueKeyUpdateModePB::UPSERT) {
            return Status::InternalError(
                    "Partial update for mow with cluster keys is not supported");
        }
        RETURN_IF_ERROR(_sort_by_cluster_keys());
    }
    if (_need_lsn) {
        DCHECK_EQ(_output_allocated_lsns->size(), _output_mutable_block.rows());
    }
    _input_mutable_block.clear();
    pending.block = Block::create_unique(_output_mutable_block.to_block());
    *output = std::move(pending);
    return Status::OK();
}

Status MemTable::to_block(std::unique_ptr<Block>* res) {
    // Changelog sub-writers share a complete Block before deriving their own schemas.
    // Materialize here so neither sub-writer can observe an empty sparse value column.
    RETURN_IF_ERROR_OR_CATCH_EXCEPTION(_materialize_sparse_columns());
    MemTableFlushOutput output;
    RETURN_IF_ERROR(to_flush_output(&output));
    *res = std::move(output.block);
    return Status::OK();
}

Status MemTable::to_flush_output(MemTableFlushOutput* output) {
    RETURN_IF_ERROR_OR_CATCH_EXCEPTION(_to_flush_output(output));
    return Status::OK();
}

} // namespace doris
