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

#include "olap/memtable.h"

#include "common/logging.h"
#include "olap/row.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/schema.h"
#include "runtime/tuple.h"
#include "util/doris_metrics.h"
#include "vec/aggregate_functions/aggregate_function_reader.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/core/field.h"

namespace doris {

MemTable::MemTable(int64_t tablet_id, Schema* schema, const TabletSchema* tablet_schema,
                   const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
                   KeysType keys_type, RowsetWriter* rowset_writer, bool support_vec)
        : _tablet_id(tablet_id),
          _schema(schema),
          _tablet_schema(tablet_schema),
          _slot_descs(slot_descs),
          _keys_type(keys_type),
          _mem_tracker(std::make_unique<MemTracker>(
                  fmt::format("MemTable:tabletId={}", std::to_string(tablet_id)))),
          _buffer_mem_pool(new MemPool(_mem_tracker.get())),
          _table_mem_pool(new MemPool(_mem_tracker.get())),
          _schema_size(_schema->schema_size()),
          _rowset_writer(rowset_writer),
          _is_first_insertion(true),
          _agg_functions(schema->num_columns()),
          _offsets_of_aggregate_states(schema->num_columns()),
          _total_size_of_aggregate_states(0),
          _mem_usage(0) {
    if (support_vec) {
        _skip_list = nullptr;
        _vec_row_comparator = std::make_shared<RowInBlockComparator>(_schema);
        // TODO: Support ZOrderComparator in the future
        _vec_skip_list = std::make_unique<VecTable>(
                _vec_row_comparator.get(), _table_mem_pool.get(), _keys_type == KeysType::DUP_KEYS);
        _init_columns_offset_by_slot_descs(slot_descs, tuple_desc);
    } else {
        _vec_skip_list = nullptr;
        if (_keys_type == KeysType::DUP_KEYS) {
            _insert_fn = &MemTable::_insert_dup;
        } else {
            _insert_fn = &MemTable::_insert_agg;
        }
        if (_tablet_schema->has_sequence_col()) {
            _aggregate_two_row_fn = &MemTable::_aggregate_two_row_with_sequence;
        } else {
            _aggregate_two_row_fn = &MemTable::_aggregate_two_row;
        }
        if (tablet_schema->sort_type() == SortType::ZORDER) {
            _row_comparator = std::make_shared<TupleRowZOrderComparator>(
                    _schema, tablet_schema->sort_col_num());
        } else {
            _row_comparator = std::make_shared<RowCursorComparator>(_schema);
        }
        _skip_list = std::make_unique<Table>(_row_comparator.get(), _table_mem_pool.get(),
                                             _keys_type == KeysType::DUP_KEYS);
    }
}
void MemTable::_init_columns_offset_by_slot_descs(const std::vector<SlotDescriptor*>* slot_descs,
                                                  const TupleDescriptor* tuple_desc) {
    for (auto slot_desc : *slot_descs) {
        const auto& slots = tuple_desc->slots();
        for (int j = 0; j < slots.size(); ++j) {
            if (slot_desc->id() == slots[j]->id()) {
                _column_offset.emplace_back(j);
                break;
            }
        }
    }
}

void MemTable::_init_agg_functions(const vectorized::Block* block) {
    for (uint32_t cid = _schema->num_key_columns(); cid < _schema->num_columns(); ++cid) {
        vectorized::AggregateFunctionPtr function =
                _tablet_schema->column(cid).get_aggregate_function({block->get_data_type(cid)},
                                                                   vectorized::AGG_LOAD_SUFFIX);

        DCHECK(function != nullptr);
        _agg_functions[cid] = function;
    }

    for (uint32_t cid = _schema->num_key_columns(); cid < _schema->num_columns(); ++cid) {
        _offsets_of_aggregate_states[cid] = _total_size_of_aggregate_states;
        _total_size_of_aggregate_states += _agg_functions[cid]->size_of_data();

        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        if (cid + 1 < _agg_functions.size()) {
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
    if (_vec_skip_list != nullptr && _keys_type != KeysType::DUP_KEYS) {
        VecTable::Iterator it(_vec_skip_list.get());
        for (it.SeekToFirst(); it.Valid(); it.Next()) {
            // We should release agg_places here, because they are not relesed when a
            // load is canceled.
            for (size_t i = _schema->num_key_columns(); i < _schema->num_columns(); ++i) {
                auto function = _agg_functions[i];
                DCHECK(function != nullptr);
                DCHECK(it.key()->agg_places(i) != nullptr);
                function->destroy(it.key()->agg_places(i));
            }
        }
    }
    std::for_each(_row_in_blocks.begin(), _row_in_blocks.end(), std::default_delete<RowInBlock>());
    _mem_tracker->release(_mem_usage);
    _buffer_mem_pool->free_all();
    _table_mem_pool->free_all();
    _mem_tracker->memory_leak_check();
}

MemTable::RowCursorComparator::RowCursorComparator(const Schema* schema) : _schema(schema) {}

int MemTable::RowCursorComparator::operator()(const char* left, const char* right) const {
    ContiguousRow lhs_row(_schema, left);
    ContiguousRow rhs_row(_schema, right);
    return compare_row(lhs_row, rhs_row);
}

int MemTable::RowInBlockComparator::operator()(const RowInBlock* left,
                                               const RowInBlock* right) const {
    return _pblock->compare_at(left->_row_pos, right->_row_pos, _schema->num_key_columns(),
                               *_pblock, -1);
}

void MemTable::insert(const vectorized::Block* input_block, const std::vector<int>& row_idxs) {
    auto target_block = input_block->copy_block(_column_offset);
    if (_is_first_insertion) {
        _is_first_insertion = false;
        auto cloneBlock = target_block.clone_without_columns();
        _input_mutable_block = vectorized::MutableBlock::build_mutable_block(&cloneBlock);
        _vec_row_comparator->set_block(&_input_mutable_block);
        _output_mutable_block = vectorized::MutableBlock::build_mutable_block(&cloneBlock);
        if (_keys_type != KeysType::DUP_KEYS) {
            _init_agg_functions(&target_block);
        }
    }
    auto num_rows = row_idxs.size();
    size_t cursor_in_mutableblock = _input_mutable_block.rows();
    _input_mutable_block.add_rows(&target_block, row_idxs.data(), row_idxs.data() + num_rows);
    size_t input_size = target_block.allocated_bytes() * num_rows / target_block.rows();
    _mem_usage += input_size;
    _mem_tracker->consume(input_size);

    for (int i = 0; i < num_rows; i++) {
        _row_in_blocks.emplace_back(new RowInBlock {cursor_in_mutableblock + i});
        _insert_one_row_from_block(_row_in_blocks.back());
    }
}

void MemTable::_insert_one_row_from_block(RowInBlock* row_in_block) {
    _rows++;
    bool overwritten = false;
    if (_keys_type == KeysType::DUP_KEYS) {
        // TODO: dup keys only need sort opertaion. Rethink skiplist is the beat way to sort columns?
        _vec_skip_list->Insert(row_in_block, &overwritten);
        DCHECK(!overwritten) << "Duplicate key model meet overwrite in SkipList";
        return;
    }

    bool is_exist = _vec_skip_list->Find(row_in_block, &_vec_hint);
    if (is_exist) {
        _aggregate_two_row_in_block(row_in_block, _vec_hint.curr->key);
    } else {
        row_in_block->init_agg_places(
                (char*)_table_mem_pool->allocate_aligned(_total_size_of_aggregate_states, 16),
                _offsets_of_aggregate_states.data());
        for (auto cid = _schema->num_key_columns(); cid < _schema->num_columns(); cid++) {
            auto col_ptr = _input_mutable_block.mutable_columns()[cid].get();
            auto data = row_in_block->agg_places(cid);
            _agg_functions[cid]->create(data);
            _agg_functions[cid]->add(data, const_cast<const doris::vectorized::IColumn**>(&col_ptr),
                                     row_in_block->_row_pos, nullptr);
        }

        _vec_skip_list->InsertWithHint(row_in_block, is_exist, &_vec_hint);
    }
}

// For non-DUP models, for the data rows passed from the upper layer, when copying the data,
// we first allocate from _buffer_mem_pool, and then check whether it already exists in
// _skiplist.  If it exists, we aggregate the new row into the row in skiplist.
// otherwise, we need to copy it into _table_mem_pool before we can insert it.
void MemTable::_insert_agg(const Tuple* tuple) {
    _rows++;
    uint8_t* tuple_buf = _buffer_mem_pool->allocate(_schema_size);
    ContiguousRow src_row(_schema, tuple_buf);
    _tuple_to_row(tuple, &src_row, _buffer_mem_pool.get());

    bool is_exist = _skip_list->Find((TableKey)tuple_buf, &_hint);
    if (is_exist) {
        (this->*_aggregate_two_row_fn)(src_row, _hint.curr->key);
    } else {
        tuple_buf = _table_mem_pool->allocate(_schema_size);
        ContiguousRow dst_row(_schema, tuple_buf);
        _agg_object_pool.acquire_data(&_agg_buffer_pool);
        copy_row_in_memtable(&dst_row, src_row, _table_mem_pool.get());
        _skip_list->InsertWithHint((TableKey)tuple_buf, is_exist, &_hint);
    }

    // Make MemPool to be reusable, but does not free its memory
    _buffer_mem_pool->clear();
    _agg_buffer_pool.clear();
}

void MemTable::_insert_dup(const Tuple* tuple) {
    _rows++;
    bool overwritten = false;
    uint8_t* tuple_buf = _table_mem_pool->allocate(_schema_size);
    ContiguousRow row(_schema, tuple_buf);
    _tuple_to_row(tuple, &row, _table_mem_pool.get());
    _skip_list->Insert((TableKey)tuple_buf, &overwritten);
    DCHECK(!overwritten) << "Duplicate key model meet overwrite in SkipList";
}

void MemTable::_tuple_to_row(const Tuple* tuple, ContiguousRow* row, MemPool* mem_pool) {
    for (size_t i = 0; i < _slot_descs->size(); ++i) {
        auto cell = row->cell(i);
        const SlotDescriptor* slot = (*_slot_descs)[i];

        bool is_null = tuple->is_null(slot->null_indicator_offset());
        const auto* value = (const char*)tuple->get_slot(slot->tuple_offset());
        _schema->column(i)->consume(&cell, value, is_null, mem_pool, &_agg_buffer_pool);
    }
}

void MemTable::_aggregate_two_row(const ContiguousRow& src_row, TableKey row_in_skiplist) {
    ContiguousRow dst_row(_schema, row_in_skiplist);
    agg_update_row(&dst_row, src_row, _table_mem_pool.get());
}

void MemTable::_aggregate_two_row_with_sequence(const ContiguousRow& src_row,
                                                TableKey row_in_skiplist) {
    ContiguousRow dst_row(_schema, row_in_skiplist);
    agg_update_row_with_sequence(&dst_row, src_row, _tablet_schema->sequence_col_idx(),
                                 _table_mem_pool.get());
}

void MemTable::_aggregate_two_row_in_block(RowInBlock* new_row, RowInBlock* row_in_skiplist) {
    if (_tablet_schema->has_sequence_col()) {
        auto sequence_idx = _tablet_schema->sequence_col_idx();
        auto res = _input_mutable_block.compare_at(row_in_skiplist->_row_pos, new_row->_row_pos,
                                                   sequence_idx, _input_mutable_block, -1);
        // dst sequence column larger than src, don't need to update
        if (res > 0) {
            return;
        }
    }
    // dst is non-sequence row, or dst sequence is smaller
    for (uint32_t cid = _schema->num_key_columns(); cid < _schema->num_columns(); ++cid) {
        auto col_ptr = _input_mutable_block.mutable_columns()[cid].get();
        _agg_functions[cid]->add(row_in_skiplist->agg_places(cid),
                                 const_cast<const doris::vectorized::IColumn**>(&col_ptr),
                                 new_row->_row_pos, nullptr);
    }
}
template <bool is_final>
void MemTable::_collect_vskiplist_results() {
    VecTable::Iterator it(_vec_skip_list.get());
    vectorized::Block in_block = _input_mutable_block.to_block();
    if (_keys_type == KeysType::DUP_KEYS) {
        std::vector<int> row_pos_vec;
        DCHECK(in_block.rows() <= std::numeric_limits<int>::max());
        row_pos_vec.reserve(in_block.rows());
        for (it.SeekToFirst(); it.Valid(); it.Next()) {
            row_pos_vec.emplace_back(it.key()->_row_pos);
        }
        _output_mutable_block.add_rows(&in_block, row_pos_vec.data(),
                                       row_pos_vec.data() + in_block.rows());
    } else {
        size_t idx = 0;
        for (it.SeekToFirst(); it.Valid(); it.Next()) {
            auto& block_data = in_block.get_columns_with_type_and_name();
            // move key columns
            for (size_t i = 0; i < _schema->num_key_columns(); ++i) {
                _output_mutable_block.get_column_by_position(i)->insert_from(
                        *block_data[i].column.get(), it.key()->_row_pos);
            }
            // get value columns from agg_places
            for (size_t i = _schema->num_key_columns(); i < _schema->num_columns(); ++i) {
                auto function = _agg_functions[i];
                auto agg_place = it.key()->agg_places(i);
                auto col_ptr = _output_mutable_block.get_column_by_position(i).get();
                function->insert_result_into(agg_place, *col_ptr);
                if constexpr (is_final) {
                    function->destroy(agg_place);
                } else {
                    function->reset(agg_place);
                    function->add(agg_place,
                                  const_cast<const doris::vectorized::IColumn**>(&col_ptr), idx,
                                  nullptr);
                }
            }
            if constexpr (!is_final) {
                // re-index the row_pos in VSkipList
                it.key()->_row_pos = idx;
                idx++;
            }
        }
        if constexpr (!is_final) {
            // if is not final, we collect the agg results to input_block and then continue to insert
            size_t shrunked_after_agg = _output_mutable_block.allocated_bytes();
            _mem_tracker->consume(shrunked_after_agg - _mem_usage);
            _mem_usage = shrunked_after_agg;
            _input_mutable_block.swap(_output_mutable_block);
            //TODO(weixang):opt here.
            std::unique_ptr<vectorized::Block> empty_input_block =
                    in_block.create_same_struct_block(0);
            _output_mutable_block =
                    vectorized::MutableBlock::build_mutable_block(empty_input_block.get());
            _output_mutable_block.clear_column_data();
        }
    }

    if (is_final) {
        _vec_skip_list.reset();
    }
}

void MemTable::shrink_memtable_by_agg() {
    if (_keys_type == KeysType::DUP_KEYS) {
        return;
    }
    _collect_vskiplist_results<false>();
}

bool MemTable::is_flush() const {
    return memory_usage() >= config::write_buffer_size;
}

bool MemTable::need_to_agg() {
    return _keys_type == KeysType::DUP_KEYS ? is_flush()
                                            : memory_usage() >= config::memtable_max_buffer_size;
}

Status MemTable::flush() {
    VLOG_CRITICAL << "begin to flush memtable for tablet: " << _tablet_id
                  << ", memsize: " << memory_usage() << ", rows: " << _rows;
    int64_t duration_ns = 0;
    RETURN_NOT_OK(_do_flush(duration_ns));
    DorisMetrics::instance()->memtable_flush_total->increment(1);
    DorisMetrics::instance()->memtable_flush_duration_us->increment(duration_ns / 1000);
    VLOG_CRITICAL << "after flush memtable for tablet: " << _tablet_id
                  << ", flushsize: " << _flush_size;
    return Status::OK();
}

Status MemTable::_do_flush(int64_t& duration_ns) {
    SCOPED_RAW_TIMER(&duration_ns);
    if (_skip_list) {
        Status st = _rowset_writer->flush_single_memtable(this, &_flush_size);
        if (st.precise_code() == OLAP_ERR_FUNC_NOT_IMPLEMENTED) {
            // For alpha rowset, we do not implement "flush_single_memtable".
            // Flush the memtable like the old way.
            Table::Iterator it(_skip_list.get());
            for (it.SeekToFirst(); it.Valid(); it.Next()) {
                char* row = (char*)it.key();
                ContiguousRow dst_row(_schema, row);
                agg_finalize_row(&dst_row, _table_mem_pool.get());
                RETURN_NOT_OK(_rowset_writer->add_row(dst_row));
            }
            RETURN_NOT_OK(_rowset_writer->flush());
        } else {
            RETURN_NOT_OK(st);
        }
    } else {
        _collect_vskiplist_results<true>();
        vectorized::Block block = _output_mutable_block.to_block();
        RETURN_NOT_OK(_rowset_writer->flush_single_memtable(&block));
        _flush_size = block.allocated_bytes();
    }
    return Status::OK();
}

Status MemTable::close() {
    return flush();
}

MemTable::Iterator::Iterator(MemTable* memtable)
        : _mem_table(memtable), _it(memtable->_skip_list.get()) {}

void MemTable::Iterator::seek_to_first() {
    _it.SeekToFirst();
}

bool MemTable::Iterator::valid() {
    return _it.Valid();
}

void MemTable::Iterator::next() {
    _it.Next();
}

ContiguousRow MemTable::Iterator::get_current_row() {
    char* row = (char*)_it.key();
    ContiguousRow dst_row(_mem_table->_schema, row);
    agg_finalize_row(&dst_row, _mem_table->_table_mem_pool.get());
    return dst_row;
}

} // namespace doris
