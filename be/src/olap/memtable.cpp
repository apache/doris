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

#include "olap/hll.h"
#include "olap/rowset/column_data_writer.h"
#include "olap/row_cursor.h"
#include "olap/row.h"
#include "util/runtime_profile.h"
#include "util/debug_util.h"

namespace doris {

MemTable::MemTable(Schema* schema, const TabletSchema* tablet_schema,
                   std::vector<uint32_t>* col_ids, TupleDescriptor* tuple_desc,
                   KeysType keys_type)
    : _schema(schema),
      _tablet_schema(tablet_schema),
      _tuple_desc(tuple_desc),
      _col_ids(col_ids),
      _keys_type(keys_type),
      _row_comparator(_schema) {
    _schema_size = _schema->schema_size();
    _tuple_buf = _arena.Allocate(_schema_size);
    _skip_list = new Table(_row_comparator, &_arena);
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

size_t MemTable::memory_usage() {
    return _arena.MemoryUsage();
}

void MemTable::insert(Tuple* tuple) {
    const std::vector<SlotDescriptor*>& slots = _tuple_desc->slots();
    ContiguousRow row(_schema, _tuple_buf);
    for (size_t i = 0; i < _col_ids->size(); ++i) {
        auto cell = row.cell(i);
        const SlotDescriptor* slot = slots[(*_col_ids)[i]];

        // todo(kks): currently, HLL implementation don't have a merge method
        // we should refactor HLL implementation and remove this special case handle
        if (slot->type() == TYPE_HLL && _skip_list->Contains(_tuple_buf)) {
            cell.set_not_null();
            const StringValue* src = tuple->get_string_slot(slot->tuple_offset());
            auto* dest = (Slice*)(cell.cell_ptr());
            dest->size = src->len;
            dest->data = _arena.Allocate(dest->size);
            memcpy(dest->data, src->ptr, dest->size);
        } else {
            bool is_null = tuple->is_null(slot->null_indicator_offset());
            void* value = tuple->get_slot(slot->tuple_offset());
            _schema->column(i)->consume(&cell, (const char *)value, is_null, _skip_list->arena());
        }
    }

    bool overwritten = false;
    _skip_list->Insert(_tuple_buf, &overwritten, _keys_type);
    if (!overwritten) {
        _tuple_buf = _arena.Allocate(_schema_size);
    }
}

OLAPStatus MemTable::flush(RowsetWriterSharedPtr rowset_writer) {
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        Table::Iterator it(_skip_list);
        for (it.SeekToFirst(); it.Valid(); it.Next()) {
            char* row = (char*)it.key();
            ContiguousRow dst_row(_schema, row);
            agg_finalize_row(&dst_row, _skip_list->arena());
            RETURN_NOT_OK(rowset_writer->add_row(dst_row));
        }
        RETURN_NOT_OK(rowset_writer->flush());
    }
    DorisMetrics::memtable_flush_total.increment(1); 
    DorisMetrics::memtable_flush_duration_us.increment(duration_ns / 1000);
    return OLAP_SUCCESS;
}

OLAPStatus MemTable::close(RowsetWriterSharedPtr rowset_writer) {
    return flush(rowset_writer);
}

} // namespace doris
