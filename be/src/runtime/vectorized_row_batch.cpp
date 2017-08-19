// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/vectorized_row_batch.h"
#include "common/logging.h"

namespace palo {

//VectorizedRowBatch::VectorizedRowBatch(const TupleDescriptor& tuple_desc, int capacity)
VectorizedRowBatch::VectorizedRowBatch(
        const std::vector<FieldInfo>& schema, int capacity, MemTracker* mem_tracker)
    : _schema(schema), _capacity(capacity), _num_cols(schema.size()), _mem_pool(mem_tracker) {
    _selected_in_use = false;
    _size = 0;
    _row_iter = 0;
    _has_backup = false;
    _selected = reinterpret_cast<int*>(_mem_pool.allocate(sizeof(int) * _capacity));

    for (int i = 0; i < _num_cols; ++i) {
        boost::shared_ptr<ColumnVector> col_vec(new ColumnVector(_capacity));
        _columns.push_back(col_vec);
    }
}

bool VectorizedRowBatch::get_next_tuple(Tuple* tuple, const TupleDescriptor& tuple_desc) {
    if (_row_iter < _size) {
        std::vector<SlotDescriptor*> slots = tuple_desc.slots();
        if (_selected_in_use) {
            for (int i = 0; i < slots.size(); ++i) {
                void* slot = tuple->get_slot(slots[i]->tuple_offset());
                memory_copy(slot,
                        reinterpret_cast<uint8_t*>(_columns[i]->col_data())
                        + get_slot_size(slots[i]->type().type) * _selected[_row_iter],
                        get_slot_size(slots[i]->type().type));
            }
        } else {
            for (int i = 0; i < slots.size(); ++i) {
                void* slot = tuple->get_slot(slots[i]->tuple_offset());
                memory_copy(slot,
                        reinterpret_cast<uint8_t*>(_columns[i]->col_data())
                        + get_slot_size(slots[i]->type().type) * _row_iter,
                        get_slot_size(slots[i]->type().type));
            }
        }
        ++_row_iter;
        return true;
    } else {
        return false;
    }
}

void VectorizedRowBatch::to_row_batch(RowBatch* row_batch, const TupleDescriptor& tuple_desc) {
    const std::vector<SlotDescriptor*> slots = tuple_desc.slots();
    int row_remain = row_batch->capacity() - row_batch->num_rows();
    int size = std::min(row_remain, _size - _row_iter);

    if (size <= 0) {
        return;
    }

    int row_index = row_batch->add_rows(size);
    DCHECK(row_index != RowBatch::INVALID_ROW_INDEX);
    uint8_t* tuple_buf = row_batch->tuple_data_pool()->allocate(
                             size * tuple_desc.byte_size());
    bzero(tuple_buf, size * tuple_desc.byte_size());
    Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buf);

    if (_selected_in_use) {
        for (int i = _row_iter; i < _row_iter + size; ++i) {
            for (int j = 0; j < slots.size(); ++j) {
                // TODO(hujie01) bad code need optimize
                if (slots[j]->type().is_string_type()) {
                    StringValue* src = reinterpret_cast<StringValue*>(
                                           reinterpret_cast<uint8_t*>(_columns[j]->col_data())
                                           + slots[j]->type().get_slot_size() * _selected[i]);
                    uint8_t* v = row_batch->tuple_data_pool()->allocate(src->len);
                    memory_copy(v, src->ptr, src->len);
                    // if src->len == 0 then dst->ptr = NULL
                    StringValue* slot = tuple->get_string_slot(slots[j]->tuple_offset());
                    slot->ptr = reinterpret_cast<char*>(v);
                    slot->len = src->len;
                } else {
                    void* slot = tuple->get_slot(slots[j]->tuple_offset());
                    memory_copy(slot,
                                reinterpret_cast<uint8_t*>(_columns[j]->col_data())
                                + slots[j]->type().get_slot_size() * _selected[i],
                                slots[j]->type().get_slot_size());
                }
            }

            TupleRow* row = row_batch->get_row(row_index++);
            row->set_tuple(0, tuple);
            tuple = reinterpret_cast<Tuple*>(reinterpret_cast<uint8_t*>(tuple) +
                                             tuple_desc.byte_size());
        }
    } else {
        for (int i = _row_iter; i < _row_iter + size; ++i) {
            for (int j = 0; j < slots.size(); ++j) {
                // TODO(hujie01) bad code need optimize
                if (slots[j]->type().is_string_type()) {
                    StringValue* slot = tuple->get_string_slot(slots[j]->tuple_offset());
                    StringValue* src = reinterpret_cast<StringValue*>(
                                           reinterpret_cast<uint8_t*>(_columns[j]->col_data())
                                           + slots[j]->type().get_slot_size() * i);
                    uint8_t* v = row_batch->tuple_data_pool()->allocate(src->len);
                    memory_copy(v, src->ptr, src->len);
                    // if src->len == 0 then dst->ptr = NULL
                    slot->ptr = reinterpret_cast<char*>(v);
                    slot->len = src->len;
                } else {
                    void* slot = tuple->get_slot(slots[j]->tuple_offset());
                    memory_copy(slot,
                                reinterpret_cast<uint8_t*>(_columns[j]->col_data())
                                + slots[j]->type().get_slot_size() * i,
                                slots[j]->type().get_slot_size());
                }
            }

            TupleRow* row = row_batch->get_row(row_index++);
            row->set_tuple(0, tuple);
            tuple = reinterpret_cast<Tuple*>(reinterpret_cast<uint8_t*>(tuple) +
                                             tuple_desc.byte_size());
        }
    }

    _row_iter += size;
    row_batch->commit_rows(size);
}

#if 0
void VectorizedRowBatch::reorganized_from_pax(
        RowBlock
        const std::vector<FieldInfo>& schema) {
    for (int i = 0, j = 0; i < _num_cols && j < schema.size(); ++i) {
        if (_schema[i].unique_id != schema[j].unique_id
                || column(i)->col_data() != NULL) {
            continue;
        }
        ++j;

        switch (_schema[i].type) {
        case OLAP_FIELD_TYPE_STRING: {
            StringValue* value = reinterpret_cast<StringValue*>(
                                     _mem_pool.allocate(get_slot_size(TYPE_VARCHAR) * _size));
            int len = column(i)->byte_size() / _size;
            char* raw = reinterpret_cast<char*>(column(i)->col_data());

            for (int j = 0; j < _size; ++j) {
                value[j].ptr = raw + len * j;
                value[j].len = strnlen(value[j].ptr, len);
            }

            column(i)->set_col_data(value);
            break;
        }
        case OLAP_FIELD_TYPE_VARCHAR:
             OLAP_FIELD_TYPE_HLL :{
            typedef uint32_t OffsetValueType;
            typedef uint16_t LengthValueType;
            DCHECK_EQ(sizeof(OffsetValueType) * _size, column(i)->byte_size());
            StringValue* value = reinterpret_cast<StringValue*>(
                                     _mem_pool.allocate(get_slot_size(TYPE_VARCHAR) * _size));
            OffsetValueType* offsets = reinterpret_cast<OffsetValueType*>(
                                           column(i)->col_data());
            char* raw = reinterpret_cast<char*>(column(i)->col_string_data());

            for (int j = 0; j < _size; ++j) {
                value[j].len = *reinterpret_cast<LengthValueType*>(raw + offsets[j]);
                value[j].ptr = raw + offsets[j] + sizeof(LengthValueType);
            }

            column(i)->set_col_data(value);
            break;
        }
        case OLAP_FIELD_TYPE_DATE: {
            uint8_t* value = reinterpret_cast<uint8_t*>(
                                 _mem_pool.allocate(get_slot_size(TYPE_DATE) * _size));
            uint8_t* raw = reinterpret_cast<uint8_t*>(column(i)->col_data());

            for (int j = 0; j < _size; ++j) {
                new(value + j * get_slot_size(TYPE_DATE))
                TimestampValue(raw + j * 3, OLAP_DATETIME);
            }

            column(i)->set_col_data(value);
            break;
        }
        case OLAP_FIELD_TYPE_DATETIME: {
            uint8_t* value = reinterpret_cast<uint8_t*>(
                                 _mem_pool.allocate(get_slot_size(TYPE_DATETIME) * _size));
            uint8_t* raw = reinterpret_cast<uint8_t*>(column(i)->col_data());

            for (int j = 0; j < _size; ++j) {
                new(value + j * get_slot_size(TYPE_DATETIME))
                TimestampValue(raw + j * 8, OLAP_DATE);
            }

            column(i)->set_col_data(value);
            break;
        }
        default:
            break;
        }
    }
}
#endif

//void VectorizedRowBatch::reorganized_from_dsm() {

//}
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
