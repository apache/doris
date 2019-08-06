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
        
        if (tuple->is_null(slot->null_indicator_offset())) {
            cell.set_null();
            continue;
        }
        cell.set_not_null();
        TypeDescriptor type = slot->type();
        switch (type.type) {
            case TYPE_CHAR: {
                const StringValue* src = tuple->get_string_slot(slot->tuple_offset());
                Slice* dest = (Slice*)(cell.cell_ptr());
                dest->size = _tablet_schema->column(i).length();
                dest->data = _arena.Allocate(dest->size);
                memcpy(dest->data, src->ptr, src->len);
                memset(dest->data + src->len, 0, dest->size - src->len);
                break;
            }
            case TYPE_VARCHAR: {
                const StringValue* src = tuple->get_string_slot(slot->tuple_offset());
                Slice* dest = (Slice*)(cell.cell_ptr());
                dest->size = src->len;
                dest->data = _arena.Allocate(dest->size);
                memcpy(dest->data, src->ptr, dest->size);
                break;
            }
            case TYPE_HLL: {
                const StringValue* src = tuple->get_string_slot(slot->tuple_offset());
                Slice* dest = (Slice*)(cell.cell_ptr());
                dest->size = src->len;
                bool exist = _skip_list->Contains(_tuple_buf);
                if (exist) {
                    dest->data = _arena.Allocate(dest->size);
                    memcpy(dest->data, src->ptr, dest->size);
                } else {
                    dest->data = src->ptr;
                    char* mem = _arena.Allocate(sizeof(HllContext));
                    HllContext* context = new (mem) HllContext;
                    HllSetHelper::init_context(context);
                    HllSetHelper::fill_set(reinterpret_cast<char*>(dest), context);
                    context->has_value = true;
                    char* variable_ptr = _arena.Allocate(sizeof(HllContext*) + HLL_COLUMN_DEFAULT_LEN);
                    *(size_t*)(variable_ptr) = (size_t)(context);
                    variable_ptr += sizeof(HllContext*);
                    dest->data = variable_ptr;
                    dest->size = HLL_COLUMN_DEFAULT_LEN;
                }
                break;
            }
            case TYPE_DECIMAL: {
                DecimalValue* decimal_value = tuple->get_decimal_slot(slot->tuple_offset());
                decimal12_t* storage_decimal_value = reinterpret_cast<decimal12_t*>(cell.mutable_cell_ptr());
                storage_decimal_value->integer = decimal_value->int_value();
                storage_decimal_value->fraction = decimal_value->frac_value();
                break;
            }
            case TYPE_DECIMALV2: {
                DecimalV2Value* decimal_value = tuple->get_decimalv2_slot(slot->tuple_offset());
                decimal12_t* storage_decimal_value = reinterpret_cast<decimal12_t*>(cell.mutable_cell_ptr());
                storage_decimal_value->integer = decimal_value->int_value();
                storage_decimal_value->fraction = decimal_value->frac_value();
                break;
            }
            case TYPE_DATETIME: {
                DateTimeValue* datetime_value = tuple->get_datetime_slot(slot->tuple_offset());
                uint64_t* storage_datetime_value = reinterpret_cast<uint64_t*>(cell.mutable_cell_ptr());
                *storage_datetime_value = datetime_value->to_olap_datetime();
                break;
            }
            case TYPE_DATE: {
                DateTimeValue* date_value = tuple->get_datetime_slot(slot->tuple_offset());
                uint24_t* storage_date_value = reinterpret_cast<uint24_t*>(cell.mutable_cell_ptr());
                *storage_date_value = static_cast<int64_t>(date_value->to_olap_date());
                break;
            }
            default: {
                memcpy(cell.mutable_cell_ptr(), tuple->get_slot(slot->tuple_offset()), _schema->column_size(i));
                break;
            }
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
