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
#include "olap/rowset/column_data_writer.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/schema.h"
#include "runtime/tuple.h"
#include "util/doris_metrics.h"
#include "vec/core/field.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/aggregate_function_reader.h"

namespace doris {

MemTable::MemTable(int64_t tablet_id, Schema* schema, const TabletSchema* tablet_schema,
                   const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
                   KeysType keys_type, RowsetWriter* rowset_writer,
                   const std::shared_ptr<MemTracker>& parent_tracker, bool support_vec)
        : _tablet_id(tablet_id),
          _schema(schema),
          _tablet_schema(tablet_schema),
          _slot_descs(slot_descs),
          _keys_type(keys_type),
          _mem_tracker(MemTracker::create_tracker(-1, "MemTable", parent_tracker)),
          _buffer_mem_pool(new MemPool(_mem_tracker.get())),
          _table_mem_pool(new MemPool(_mem_tracker.get())),
          _schema_size(_schema->schema_size()),
          _rowset_writer(rowset_writer),
          _is_first_insertion(true),
          _agg_functions(schema->num_columns()),
          _mem_usage(0) {
    if (support_vec) {
        _skip_list = nullptr;
        _vec_row_comparator = std::make_shared<RowInBlockComparator>(_schema);
        // TODO: Support ZOrderComparator in the future
        _vec_skip_list = new VecTable(_vec_row_comparator.get(), _table_mem_pool.get(),
                                      _keys_type == KeysType::DUP_KEYS);
    } else {
        _vec_skip_list = nullptr;
        if (tablet_schema->sort_type() == SortType::ZORDER) {
            _row_comparator = std::make_shared<TupleRowZOrderComparator>(
                    _schema, tablet_schema->sort_col_num());
        } else {
            _row_comparator = std::make_shared<RowCursorComparator>(_schema);
        }
        _skip_list = new Table(_row_comparator.get(), _table_mem_pool.get(),
                               _keys_type == KeysType::DUP_KEYS);
    }
}

void MemTable::_init_agg_functions(const vectorized::Block* block) {
    for (uint32_t cid = _schema->num_key_columns(); cid < _schema->num_columns(); ++cid) {
        FieldAggregationMethod agg_method = _tablet_schema->column(cid).aggregation();
        std::string agg_name = TabletColumn::get_string_by_aggregation_type(agg_method) +
                               vectorized::AGG_LOAD_SUFFIX;
        std::transform(agg_name.begin(), agg_name.end(), agg_name.begin(),
                       [](unsigned char c) { return std::tolower(c); });

        // create aggregate function
        vectorized::DataTypes argument_types {block->get_data_type(cid)};
        vectorized::AggregateFunctionPtr function =
                vectorized::AggregateFunctionSimpleFactory::instance().get(
                        agg_name, argument_types, {}, argument_types.back()->is_nullable());

        DCHECK(function != nullptr);
        _agg_functions[cid] = function;
    }
}

MemTable::~MemTable() {
    delete _skip_list;
    delete _vec_skip_list;

    std::for_each(_row_in_blocks.begin(), _row_in_blocks.end(), std::default_delete<RowInBlock>());
    _mem_tracker->release(_mem_usage);
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

void MemTable::insert(const vectorized::Block* block, size_t row_pos, size_t num_rows) {
    if (_is_first_insertion) {
        _is_first_insertion = false;
        auto cloneBlock = block->clone_without_columns();
        _input_mutable_block = vectorized::MutableBlock::build_mutable_block(&cloneBlock);
        _vec_row_comparator->set_block(&_input_mutable_block);
        _output_mutable_block = vectorized::MutableBlock::build_mutable_block(&cloneBlock);
        if (_keys_type != KeysType::DUP_KEYS) {
            _init_agg_functions(block);
        }
    }
    size_t cursor_in_mutableblock = _input_mutable_block.rows();
    size_t oldsize = _input_mutable_block.allocated_bytes();
    _input_mutable_block.add_rows(block, row_pos, num_rows);
    size_t newsize = _input_mutable_block.allocated_bytes();
    _mem_usage += newsize - oldsize;
    _mem_tracker->consume(newsize - oldsize);

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
        row_in_block->init_agg_places(_agg_functions, _schema->num_key_columns());
        for (auto cid = _schema->num_key_columns(); cid < _schema->num_columns(); cid++) {
            auto col_ptr = _input_mutable_block.mutable_columns()[cid].get();
            auto place = row_in_block->_agg_places[cid];
            _agg_functions[cid]->add(place,
                                     const_cast<const doris::vectorized::IColumn**>(&col_ptr),
                                     row_in_block->_row_pos, nullptr);
        }

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
        auto place = row_in_skiplist->_agg_places[cid];
        auto col_ptr = _input_mutable_block.mutable_columns()[cid].get();
        _agg_functions[cid]->add(place, const_cast<const doris::vectorized::IColumn**>(&col_ptr),
                                 new_row->_row_pos, nullptr);
    }
}
vectorized::Block MemTable::_collect_vskiplist_results() {
    VecTable::Iterator it(_vec_skip_list);
    vectorized::Block in_block = _input_mutable_block.to_block();
    // TODO: should try to insert data by column, not by row. to opt the the code
    if (_keys_type == KeysType::DUP_KEYS) {
        for (it.SeekToFirst(); it.Valid(); it.Next()) {
            _output_mutable_block.add_row(&in_block, it.key()->_row_pos);
        }
    } else {
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
                function->insert_result_into(it.key()->_agg_places[i],
                                             *(_output_mutable_block.get_column_by_position(i)));
                function->destroy(it.key()->_agg_places[i]);
            }
        }
    }
    return _output_mutable_block.to_block();
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
        if (st == Status::OLAPInternalError(OLAP_ERR_FUNC_NOT_IMPLEMENTED)) {
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
    } else {
        vectorized::Block block = _collect_vskiplist_results();
        RETURN_NOT_OK(_rowset_writer->add_block(&block));
        _flush_size = block.allocated_bytes();
        RETURN_NOT_OK(_rowset_writer->flush());
    }
    return Status::OK();
}

Status MemTable::close() {
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
