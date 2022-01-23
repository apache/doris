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

#ifndef DORIS_BE_SRC_RUNTIME_BUFFERED_TUPLE_STREAM2_INLINE_H
#define DORIS_BE_SRC_RUNTIME_BUFFERED_TUPLE_STREAM2_INLINE_H

#include "runtime/buffered_tuple_stream2.h"
#include "runtime/descriptors.h"
#include "runtime/tuple_row.h"

namespace doris {

inline bool BufferedTupleStream2::add_row(TupleRow* row, Status* status) {
    DCHECK(!_closed);
    if (LIKELY(deep_copy(row))) {
        return true;
    }
    bool got_block;
    int64_t row_size = compute_row_size(row);
    *status = new_block_for_write(row_size, &got_block);
    if (!status->ok() || !got_block) {
        return false;
    }
    return deep_copy(row);
}

inline uint8_t* BufferedTupleStream2::allocate_row(int size, Status* status) {
    DCHECK(!_closed);
    if (UNLIKELY(_write_block == nullptr || _write_block->bytes_remaining() < size)) {
        bool got_block;
        *status = new_block_for_write(size, &got_block);
        if (!status->ok() || !got_block) {
            return nullptr;
        }
    }
    DCHECK(_write_block != nullptr);
    DCHECK(_write_block->is_pinned());
    DCHECK_GE(_write_block->bytes_remaining(), size);
    ++_num_rows;
    _write_block->add_row();
    return _write_block->allocate<uint8_t>(size);
}

inline void BufferedTupleStream2::get_tuple_row(const RowIdx& idx, TupleRow* row) const {
    DCHECK(row != nullptr);
    DCHECK(!_closed);
    DCHECK(is_pinned());
    DCHECK(!_delete_on_read);
    DCHECK_EQ(_blocks.size(), _block_start_idx.size());
    DCHECK_LT(idx.block(), _blocks.size());

    uint8_t* data = _block_start_idx[idx.block()] + idx.offset();
    if (_nullable_tuple) {
        // Stitch together the tuples from the block and the nullptr ones.
        const int tuples_per_row = _desc.tuple_descriptors().size();
        uint32_t tuple_idx = idx.idx() * tuples_per_row;
        for (int i = 0; i < tuples_per_row; ++i) {
            const uint8_t* null_word = _block_start_idx[idx.block()] + (tuple_idx >> 3);
            const uint32_t null_pos = tuple_idx & 7;
            const bool is_not_null = ((*null_word & (1 << (7 - null_pos))) == 0);
            row->set_tuple(
                    i, reinterpret_cast<Tuple*>(reinterpret_cast<uint64_t>(data) * is_not_null));
            data += _desc.tuple_descriptors()[i]->byte_size() * is_not_null;
            ++tuple_idx;
        }
    } else {
        for (int i = 0; i < _desc.tuple_descriptors().size(); ++i) {
            row->set_tuple(i, reinterpret_cast<Tuple*>(data));
            data += _desc.tuple_descriptors()[i]->byte_size();
        }
    }
}

} // namespace doris

#endif // DORIS_BE_SRC_RUNTIME_BUFFERED_TUPLE_STREAM2_INLINE_H
