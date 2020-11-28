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

#include "runtime/buffered_tuple_stream2.inline.h"

// #include "runtime/collection-value.h"
#include "runtime/descriptors.h"
#include "runtime/tuple_row.h"

using std::vector;

namespace doris {

bool BufferedTupleStream2::deep_copy(TupleRow* row) {
    if (_nullable_tuple) {
        return deep_copy_internal<true>(row);
    } else {
        return deep_copy_internal<false>(row);
    }
}

// TODO: this really needs codegen
// TODO: in case of duplicate tuples, this can redundantly serialize data.
template <bool HasNullableTuple>
bool BufferedTupleStream2::deep_copy_internal(TupleRow* row) {
    if (UNLIKELY(_write_block == NULL)) {
        return false;
    }
    DCHECK_GE(_null_indicators_write_block, 0);
    DCHECK(_write_block->is_pinned()) << debug_string() << std::endl
                                      << _write_block->debug_string();

    const uint64_t tuples_per_row = _desc.tuple_descriptors().size();
    if (UNLIKELY((_write_block->bytes_remaining() < _fixed_tuple_row_size) ||
                 (HasNullableTuple &&
                  (_write_tuple_idx + tuples_per_row > _null_indicators_write_block * 8)))) {
        return false;
    }
    // Allocate the maximum possible buffer for the fixed portion of the tuple.
    uint8_t* tuple_buf = _write_block->allocate<uint8_t>(_fixed_tuple_row_size);
    // Total bytes allocated in _write_block for this row. Saved so we can roll back
    // if this row doesn't fit.
    int bytes_allocated = _fixed_tuple_row_size;

    // Copy the not NULL fixed len tuples. For the NULL tuples just update the NULL tuple
    // indicator.
    if (HasNullableTuple) {
        DCHECK_GT(_null_indicators_write_block, 0);
        uint8_t* null_word = NULL;
        uint32_t null_pos = 0;
        // Calculate how much space it should return.
        int to_return = 0;
        for (int i = 0; i < tuples_per_row; ++i) {
            null_word = _write_block->buffer() + (_write_tuple_idx >> 3); // / 8
            null_pos = _write_tuple_idx & 7;
            ++_write_tuple_idx;
            const int tuple_size = _desc.tuple_descriptors()[i]->byte_size();
            Tuple* t = row->get_tuple(i);
            const uint8_t mask = 1 << (7 - null_pos);
            if (t != NULL) {
                *null_word &= ~mask;
                memcpy(tuple_buf, t, tuple_size);
                tuple_buf += tuple_size;
            } else {
                *null_word |= mask;
                to_return += tuple_size;
            }
        }
        DCHECK_LE(_write_tuple_idx - 1, _null_indicators_write_block * 8);
        _write_block->return_allocation(to_return);
        bytes_allocated -= to_return;
    } else {
        // If we know that there are no nullable tuples no need to set the nullability flags.
        DCHECK_EQ(_null_indicators_write_block, 0);
        for (int i = 0; i < tuples_per_row; ++i) {
            const int tuple_size = _desc.tuple_descriptors()[i]->byte_size();
            Tuple* t = row->get_tuple(i);
            // TODO: Once IMPALA-1306 (Avoid passing empty tuples of non-materialized slots)
            // is delivered, the check below should become DCHECK(t != NULL).
            DCHECK(t != NULL || tuple_size == 0);
            memcpy(tuple_buf, t, tuple_size);
            tuple_buf += tuple_size;
        }
    }

    // Copy string slots. Note: we do not need to convert the string ptrs to offsets
    // on the write path, only on the read. The tuple data is immediately followed
    // by the string data so only the len information is necessary.
    for (int i = 0; i < _string_slots.size(); ++i) {
        Tuple* tuple = row->get_tuple(_string_slots[i].first);
        if (HasNullableTuple && tuple == NULL) {
            continue;
        }
        if (UNLIKELY(!copy_strings(tuple, _string_slots[i].second, &bytes_allocated))) {
            _write_block->return_allocation(bytes_allocated);
            return false;
        }
    }

    // Copy collection slots. We copy collection data in a well-defined order so we do not
    // need to convert pointers to offsets on the write path.
    // for (int i = 0; i < _collection_slots.size(); ++i) {
    //     Tuple* tuple = row->get_tuple(_collection_slots[i].first);
    //     if (HasNullableTuple && tuple == NULL) continue;
    //     if (UNLIKELY(!copy_collections(tuple, _collection_slots[i].second,
    //                     &bytes_allocated))) {
    //         _write_block->return_allocation(bytes_allocated);
    //         return false;
    //     }
    // }

    _write_block->add_row();
    ++_num_rows;
    return true;
}

bool BufferedTupleStream2::copy_strings(const Tuple* tuple,
                                        const vector<SlotDescriptor*>& string_slots,
                                        int* bytes_allocated) {
    for (int i = 0; i < string_slots.size(); ++i) {
        const SlotDescriptor* slot_desc = string_slots[i];
        if (tuple->is_null(slot_desc->null_indicator_offset())) {
            continue;
        }
        const StringValue* sv = tuple->get_string_slot(slot_desc->tuple_offset());
        if (LIKELY(sv->len > 0)) {
            if (UNLIKELY(_write_block->bytes_remaining() < sv->len)) {
                return false;
            }
            uint8_t* buf = _write_block->allocate<uint8_t>(sv->len);
            (*bytes_allocated) += sv->len;
            memcpy(buf, sv->ptr, sv->len);
        }
    }
    return true;
}

#if 0
bool BufferedTupleStream2::copy_collections(const Tuple* tuple,
    const vector<SlotDescriptor*>& collection_slots, int* bytes_allocated) {
  for (int i = 0; i < collection_slots.size(); ++i) {
    const SlotDescriptor* slot_desc = collection_slots[i];
    if (tuple->IsNull(slot_desc->null_indicator_offset())) continue;
    const CollectionValue* cv = tuple->GetCollectionSlot(slot_desc->tuple_offset());
    const TupleDescriptor& item_desc = *slot_desc->collection_item_descriptor();
    if (LIKELY(cv->num_tuples > 0)) {
      int coll_byte_size = cv->num_tuples * item_desc.byte_size();
      if (UNLIKELY(_write_block->BytesRemaining() < coll_byte_size)) {
        return false;
      }
      uint8_t* coll_data = _write_block->allocate<uint8_t>(coll_byte_size);
      (*bytes_allocated) += coll_byte_size;
      memcpy(coll_data, cv->ptr, coll_byte_size);
      if (!item_desc.HasVarlenSlots()) continue;
      // Copy variable length data when present in collection items.
      for (int j = 0; j < cv->num_tuples; ++j) {
        Tuple* item = reinterpret_cast<Tuple*>(coll_data);
        if (UNLIKELY(!copy_strings(item, item_desc.string_slots(), bytes_allocated))) {
          return false;
        }
        if (UNLIKELY(!copy_collections(item, item_desc.collection_slots(),
            bytes_allocated))) {
          return false;
        }
        coll_data += item_desc.byte_size();
      }
    }
  }
  return true;
}
#endif

} // end namespace doris
