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
#include "olap/rowset/column_data_writer.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/row_cursor.h"
#include "olap/row.h"
#include "olap/schema.h"
#include "runtime/tuple.h"
#include "util/debug_util.h"

namespace doris {

MemTable::MemTable(int64_t tablet_id, Schema* schema, const TabletSchema* tablet_schema,
                   const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
                   KeysType keys_type, RowsetWriter* rowset_writer, MemTracker* mem_tracker)
    : _tablet_id(tablet_id),
      _schema(schema),
      _tablet_schema(tablet_schema),
      _tuple_desc(tuple_desc),
      _slot_descs(slot_descs),
      _keys_type(keys_type),
      _row_comparator(_schema),
      _rowset_writer(rowset_writer) {

    _schema_size = _schema->schema_size();
    _mem_tracker.reset(new MemTracker(-1, "memtable", mem_tracker));
    _buffer_mem_pool.reset(new MemPool(_mem_tracker.get()));
    _table_mem_pool.reset(new MemPool(_mem_tracker.get()));
    _skip_list = new Table(_row_comparator, _table_mem_pool.get(), _keys_type == KeysType::DUP_KEYS);
}

MemTable::~MemTable() {
    delete _skip_list;
}

MemTable::RowCursorComparator::RowCursorComparator(const Schema* schema)
    : _schema(schema) {}

int MemTable::RowCursorComparator::operator()(const char* left, const char* right) const {
    ContiguousRow lhs_row(_schema, left);
    ContiguousRow rhs_row(_schema, right);
    return compare_row(lhs_row, rhs_row);
}

void MemTable::insert(const Tuple* tuple) {
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
        copy_row(&dst_row, src_row, _table_mem_pool.get());
        _skip_list->InsertWithHint((TableKey)_tuple_buf, is_exist, &_hint);
    }

    // Make MemPool to be reusable, but does not free its memory
    _buffer_mem_pool->clear();
}

void MemTable::_tuple_to_row(const Tuple* tuple, ContiguousRow* row, MemPool* mem_pool) {
    for (size_t i = 0; i < _slot_descs->size(); ++i) {
        auto cell = row->cell(i);
        const SlotDescriptor* slot = (*_slot_descs)[i];

        bool is_null = tuple->is_null(slot->null_indicator_offset());
        const void* value = tuple->get_slot(slot->tuple_offset());
        _schema->column(i)->consume(
                &cell, (const char*)value, is_null, mem_pool, &_agg_object_pool);
    }
}

void MemTable::_aggregate_two_row(const ContiguousRow& src_row, TableKey row_in_skiplist) {
    ContiguousRow dst_row(_schema, row_in_skiplist);
    agg_update_row(&dst_row, src_row, _table_mem_pool.get());
}

OLAPStatus MemTable::flush() {
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        Table::Iterator it(_skip_list);
        for (it.SeekToFirst(); it.Valid(); it.Next()) {
            char* row = (char*)it.key();
            ContiguousRow dst_row(_schema, row);
            agg_finalize_row(&dst_row, _table_mem_pool.get());
            RETURN_NOT_OK(_rowset_writer->add_row(dst_row));
        }
        RETURN_NOT_OK(_rowset_writer->flush());
    }
    DorisMetrics::memtable_flush_total.increment(1);
    DorisMetrics::memtable_flush_duration_us.increment(duration_ns / 1000);
    return OLAP_SUCCESS;
}

OLAPStatus MemTable::close() {
    return flush();
}

} // namespace doris
