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
#include "olap/row_cursor.h"
#include "olap/rowset/column_data_writer.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/schema.h"
#include "runtime/tuple.h"
#include "util/debug_util.h"
#include "util/doris_metrics.h"

namespace doris {
MemTable::MemTable(int64_t tablet_id, Schema* schema, const TabletSchema* tablet_schema,
                   const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
                   KeysType keys_type, RowsetWriter* rowset_writer,
                   bool support_vec)
        : _tablet_id(tablet_id),
          _schema(schema),
          _tablet_schema(tablet_schema),
          _slot_descs(slot_descs),
          _keys_type(keys_type),
          _mem_tracker(MemTracker::create_tracker(-1, "MemTable")),
          _buffer_mem_pool(new MemPool(_mem_tracker.get())),
          _table_mem_pool(new MemPool(_mem_tracker.get())),
          _schema_size(_schema->schema_size()),
          _rowset_writer(rowset_writer),
          _is_first_insertion(true) {
    if (support_vec){
        _vec_row_comparator = std::make_shared<RowInBlockComparator>(_schema);
        _vec_skip_list = new VecTable(_vec_row_comparator.get(), _table_mem_pool.get(),
                                _keys_type == KeysType::DUP_KEYS);
    }else{
        if (tablet_schema->sort_type() == SortType::ZORDER) {
            _row_comparator =
                    std::make_shared<TupleRowZOrderComparator>(_schema, tablet_schema->sort_col_num());
        } else {
            _row_comparator = std::make_shared<RowCursorComparator>(_schema);
        }
        _skip_list = new Table(_row_comparator.get(), _table_mem_pool.get(),
                            _keys_type == KeysType::DUP_KEYS);
    }
}

MemTable::~MemTable() {
    delete _skip_list;
}

MemTable::RowCursorComparator::RowCursorComparator(const Schema* schema) : _schema(schema) {}

int MemTable::RowCursorComparator::operator()(const char* left, const char* right) const {
    ContiguousRow lhs_row(_schema, left);
    ContiguousRow rhs_row(_schema, right);
    return compare_row(lhs_row, rhs_row);
}

int MemTable::RowInBlockComparator::operator()(const RowInBlock left, const RowInBlock right) const{
    return _pblock->compare_at(left._row_pos, right._row_pos, 
                            _schema->num_key_columns(), 
                            *_pblock, -1); 
}

void MemTable::insert(const vectorized::Block* block, size_t row_pos, size_t num_rows)
{
    if (_is_first_insertion)
    {
        _is_first_insertion = false;
        auto cloneBlock = block->clone_without_columns();
        _input_mutable_block = vectorized::MutableBlock::build_mutable_block(&cloneBlock);
        _output_mutable_block = vectorized::MutableBlock::build_mutable_block(&cloneBlock);
    }
    size_t cursor_in_mutableblock = _input_mutable_block.rows();
    size_t oldsize = block->allocated_bytes();
    _input_mutable_block.add_rows(block, row_pos, row_pos + num_rows);
    size_t newsize = block->allocated_bytes();
    _mem_tracker->Consume(newsize - oldsize);

    for(int i = 0; i < num_rows; i++){       
        insert_one_row_from_block(RowInBlock(cursor_in_mutableblock + i));
    }   
}

void MemTable::insert_one_row_from_block(struct RowInBlock row_in_block)
{
    _rows++;
    bool overwritten = false;
    if (_keys_type == KeysType::DUP_KEYS)
    {
        _vec_skip_list->Insert(row_in_block, &overwritten);
        DCHECK(!overwritten) << "Duplicate key model meet overwrite in SkipList";
        return;
    }
    bool is_exist = _vec_skip_list->Find(row_in_block, &_vec_hint);
    if (is_exist){
        _aggregate_two_rowInBlock(row_in_block, _vec_hint.curr->key);
    }else{
        _vec_skip_list->InsertWithHint(row_in_block, is_exist, &_vec_hint);
    }
}

void MemTable::insert(const Tuple* tuple) {
    _rows++;
    bool overwritten = false;
    uint8_t* _tuple_buf = nullptr;
    if (_keys_type == KeysType::DUP_KEYS) {
        // Will insert directly, so use memory from _table_mem_pool
        _tuple_buf = _table_mem_pool->allocate(_schema_size);
        ContiguousRow row(_schema, _tuple_buf);
        _tuple_to_row(tuple, &row, _table_mem_pool.get());
        _skip_list->Insert((TableKey)_tuple_buf, &overwritten);
        DCHECK(!overwritten) << "Duplicate key model meet overwrite in SkipList";
        return;
    }

    // For non-DUP models, for the data rows passed from the upper layer, when copying the data,
    // we first allocate from _buffer_mem_pool, and then check whether it already exists in
    // _skiplist.  If it exists, we aggregate the new row into the row in skiplist.
    // otherwise, we need to copy it into _table_mem_pool before we can insert it.
    _tuple_buf = _buffer_mem_pool->allocate(_schema_size);
    ContiguousRow src_row(_schema, _tuple_buf);
    _tuple_to_row(tuple, &src_row, _buffer_mem_pool.get());

    bool is_exist = _skip_list->Find((TableKey)_tuple_buf, &_hint);
    if (is_exist) {
        _aggregate_two_row(src_row, _hint.curr->key);
    } else {
        _tuple_buf = _table_mem_pool->allocate(_schema_size);
        ContiguousRow dst_row(_schema, _tuple_buf);
        _agg_object_pool.acquire_data(&_agg_buffer_pool);
        copy_row_in_memtable(&dst_row, src_row, _table_mem_pool.get());
        _skip_list->InsertWithHint((TableKey)_tuple_buf, is_exist, &_hint);
    }

    // Make MemPool to be reusable, but does not free its memory
    _buffer_mem_pool->clear();
    _agg_buffer_pool.clear();
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
    if (_tablet_schema->has_sequence_col()) {
        agg_update_row_with_sequence(&dst_row, src_row, _tablet_schema->sequence_col_idx(),
                                     _table_mem_pool.get());
    } else {
        agg_update_row(&dst_row, src_row, _table_mem_pool.get());
    }
}

void MemTable::_aggregate_two_rowInBlock(RowInBlock new_row, RowInBlock row_in_skiplist){
    if (_tablet_schema->has_sequence_col()) {
        auto sequence_idx = _tablet_schema->sequence_col_idx();
        auto seq_dst_cell = row_in_skiplist.cell(&_input_mutable_block, sequence_idx);
        auto seq_src_cell = new_row.cell(&_input_mutable_block, sequence_idx);
        auto res = _schema->column(sequence_idx)->compare_cell(seq_dst_cell, seq_src_cell);
        // dst sequence column larger than src, don't need to update
        if (res > 0) {
            return;
        }
    }                       
    //dst is non-sequence row, or dst sequence is smaller
    for (uint32_t cid = _schema->num_key_columns(); 
                    cid < _schema->num_columns();
                    ++cid) {
        auto dst_cell = row_in_skiplist.cell(&_input_mutable_block, cid);
        auto src_cell = new_row.cell(&_input_mutable_block, cid);
        _schema->column(cid)->agg_update(&dst_cell, &src_cell, _table_mem_pool.get());
    }
    
}
vectorized::Block MemTable::collect_skiplist_results()
{
    VecTable::Iterator it(_vec_skip_list);
    vectorized::Block in_block = _input_mutable_block.to_block();
    for (it.SeekToFirst(); it.Valid(); it.Next()) {
        _output_mutable_block.add_row(&in_block, it.key()._row_pos);
    }
    return _output_mutable_block.to_block();
}

OLAPStatus MemTable::vflush(){
    VLOG_CRITICAL << "begin to flush memtable for tablet: " << _tablet_id
                  << ", memsize: " << memory_usage() << ", rows: " << _rows;
    size_t _flush_size = 0;
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        vectorized::Block block = collect_skiplist_results();
        OLAPStatus st = _rowset_writer->add_block(&block);
        RETURN_NOT_OK(st);
        _flush_size = block.allocated_bytes();
    }
    DorisMetrics::instance()->memtable_flush_total->increment(1);
    DorisMetrics::instance()->memtable_flush_duration_us->increment(duration_ns / 1000);
    VLOG_CRITICAL << "after flush memtable for tablet: " << _tablet_id
                  << ", flushsize: " << _flush_size;
    return OLAP_SUCCESS;
}


vectorized::Block MemTable::flush_to_block(){

    return collect_skiplist_results();
    
}


OLAPStatus MemTable::vclose() {
    return vflush();
}

OLAPStatus MemTable::flush() {
    VLOG_CRITICAL << "begin to flush memtable for tablet: " << _tablet_id
                  << ", memsize: " << memory_usage() << ", rows: " << _rows;
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        OLAPStatus st = _rowset_writer->flush_single_memtable(this, &_flush_size);
        if (st == OLAP_ERR_FUNC_NOT_IMPLEMENTED) {
            // For alpha rowset, we do not implement "flush_single_memtable".
            // Flush the memtable like the old way.
            Table::Iterator it(_skip_list);
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
    }
    DorisMetrics::instance()->memtable_flush_total->increment(1);
    DorisMetrics::instance()->memtable_flush_duration_us->increment(duration_ns / 1000);
    VLOG_CRITICAL << "after flush memtable for tablet: " << _tablet_id
                  << ", flushsize: " << _flush_size;
    return OLAP_SUCCESS;
}

OLAPStatus MemTable::close() {
    return flush();
}

MemTable::Iterator::Iterator(MemTable* memtable)
        : _mem_table(memtable), _it(memtable->_skip_list) {}

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
