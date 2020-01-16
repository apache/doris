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

#include "runtime/buffered_tuple_stream.h"

#include <boost/bind.hpp>
//#include <gutil/strings/substitute.h>

#include "runtime/descriptors.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "util/bit_util.h"
#include "util/debug_util.h"
#include "util/pretty_printer.h"
#include "common/status.h"

namespace doris {
//using namespace strings;

// The first NUM_SMALL_BLOCKS of the tuple stream are made of blocks less than the
// io size. These blocks never spill.
static const int64_t INITIAL_BLOCK_SIZES[] =
{ 64 * 1024, 512 * 1024 };
static const int NUM_SMALL_BLOCKS = sizeof(INITIAL_BLOCK_SIZES) / sizeof(int64_t);

std::string BufferedTupleStream::RowIdx::debug_string() const {
    std::stringstream ss;
    ss << "RowIdx block=" << block() << " offset=" << offset() << " idx=" << idx();
    return ss.str();
}

BufferedTupleStream::BufferedTupleStream(RuntimeState* state,
        const RowDescriptor& row_desc, BufferedBlockMgr* block_mgr)
    : _use_small_buffers(false),
      _delete_on_read(false),
      _read_write(true),
      _state(state),
      _desc(row_desc),
      _nullable_tuple(row_desc.is_any_tuple_nullable()),
      _block_mgr(block_mgr),
      // block_mgr_client_(client),
      _total_byte_size(0),
      _read_ptr(NULL),
      _read_tuple_idx(0),
      _read_bytes(0),
      _rows_returned(0),
      _read_block_idx(-1),
      _write_block(NULL),
      _num_small_blocks(0),
      _closed(false),
      _num_rows(0),
      // pinned_(true),
      _pin_timer(NULL),
      _unpin_timer(NULL),
      _get_new_block_timer(NULL) {
    _null_indicators_read_block = _null_indicators_write_block = -1;
    _read_block = _blocks.end();
    _fixed_tuple_row_size = 0;

    for (int i = 0; i < _desc.tuple_descriptors().size(); ++i) {
        const TupleDescriptor* tuple_desc = _desc.tuple_descriptors()[i];
        const int tuple_byte_size = tuple_desc->byte_size();
        _fixed_tuple_row_size += tuple_byte_size;

        if (tuple_desc->string_slots().empty()) {
            continue;
        }

        _string_slots.push_back(make_pair(i, tuple_desc->string_slots()));
    }
}

std::string BufferedTupleStream::debug_string() const {
    std::stringstream ss;
    ss << "BufferedTupleStream num_rows=" << _num_rows << " rows_returned="
       << _rows_returned << " delete_on_read=" << (_delete_on_read ? "true" : "false")
       << " closed=" << (_closed ? "true" : "false")
       << " write_block=" << _write_block << " _read_block=";

    if (_read_block == _blocks.end()) {
        ss << "<end>";
    } else {
        ss << *_read_block;
    }

    ss << " blocks=[\n";

    for (std::list<BufferedBlockMgr::Block*>::const_iterator it = _blocks.begin();
            it != _blocks.end(); ++it) {
        ss << "{" << (*it)->debug_string() << "}";

        if (*it != _blocks.back()) {
            ss << ",\n";
        }
    }

    ss << "]";
    return ss.str();
}

Status BufferedTupleStream::init(RuntimeProfile* profile) {
    if (profile != NULL) {
        _get_new_block_timer = ADD_TIMER(profile, "GetNewBlockTime");
    }

    if (_block_mgr->max_block_size() < INITIAL_BLOCK_SIZES[0]) {
        _use_small_buffers = false;
    }

    bool got_block = false;
    RETURN_IF_ERROR(new_block_for_write(_fixed_tuple_row_size, &got_block));

    if (!got_block) {
        return Status::InternalError("Allocate memory failed.");
    }

    DCHECK(_write_block != NULL);

    if (_read_write) {
        RETURN_IF_ERROR(prepare_for_read());
    };

    return Status::OK();
}

void BufferedTupleStream::close() {
    for (std::list<BufferedBlockMgr::Block*>::iterator it = _blocks.begin();
            it != _blocks.end(); ++it) {
        (*it)->delete_block();
    }

    _blocks.clear();
    _closed = true;
}

Status BufferedTupleStream::new_block_for_write(int min_size, bool* got_block) {
    DCHECK(!_closed);

    if (min_size > _block_mgr->max_block_size()) {
        std::stringstream err_msg;
        err_msg <<  "Cannot process row that is bigger than the IO size "
                << "(row_size=" << PrettyPrinter::print(min_size, TUnit::BYTES)
                << ". To run this query, increase the io size";
        return Status::InternalError(err_msg.str());
    }

    int64_t block_len = _block_mgr->max_block_size();
    BufferedBlockMgr::Block* new_block = NULL;
    {
        SCOPED_TIMER(_get_new_block_timer);
        RETURN_IF_ERROR(_block_mgr->get_new_block(&new_block, block_len));
    }

    // Compute and allocate the block header with the null indicators
    _null_indicators_write_block = compute_num_null_indicator_bytes(block_len);
    new_block->allocate<uint8_t>(_null_indicators_write_block);
    _write_tuple_idx = 0;

    _blocks.push_back(new_block);
    _block_start_idx.push_back(new_block->buffer());
    _write_block = new_block;
    DCHECK_EQ(_write_block->num_rows(), 0);
    _total_byte_size += block_len;

    *got_block = (new_block != NULL);
    return Status::OK();
}

Status BufferedTupleStream::next_block_for_read() {
    DCHECK(!_closed);
    DCHECK(_read_block != _blocks.end());

    ++_read_block;
    ++_read_block_idx;

    _read_ptr = NULL;
    _read_tuple_idx = 0;
    _read_bytes = 0;

    if (_read_block != _blocks.end()) {
        _null_indicators_read_block =
            compute_num_null_indicator_bytes((*_read_block)->buffer_len());
        _read_ptr = (*_read_block)->buffer() + _null_indicators_read_block;
    }

    return Status::OK();
}

Status BufferedTupleStream::prepare_for_read(bool* got_buffer) {
    DCHECK(!_closed);

    if (_blocks.empty()) {
        return Status::OK();
    }

    _read_block = _blocks.begin();
    DCHECK(_read_block != _blocks.end());
    _null_indicators_read_block =
        compute_num_null_indicator_bytes((*_read_block)->buffer_len());
    _read_ptr = (*_read_block)->buffer() + _null_indicators_read_block;
    _read_tuple_idx = 0;
    _read_bytes = 0;
    _rows_returned = 0;
    _read_block_idx = 0;

    if (got_buffer != NULL) {
        *got_buffer = true;
    }

    return Status::OK();
}

int BufferedTupleStream::compute_num_null_indicator_bytes(int block_size) const {
    if (_nullable_tuple) {
        // We assume that all rows will use their max size, so we may be underutilizing the
        // space, i.e. we may have some unused space in case of rows with NULL tuples.
        const uint32_t tuples_per_row = _desc.tuple_descriptors().size();
        const uint32_t min_row_size_in_bits = 8 * _fixed_tuple_row_size + tuples_per_row;
        const uint32_t block_size_in_bits = 8 * block_size;
        const uint32_t max_num_rows = block_size_in_bits / min_row_size_in_bits;
        return
            BitUtil::round_up_numi64(max_num_rows * tuples_per_row) * 8;
    } else {
        // If there are no nullable tuples then no need to waste space for null indicators.
        return 0;
    }
}

Status BufferedTupleStream::get_next(RowBatch* batch, bool* eos,
        std::vector<RowIdx>* indices) {
    if (_nullable_tuple) {
        return get_next_internal<true>(batch, eos, indices);
    } else {
        return get_next_internal<false>(batch, eos, indices);
    }
}

template <bool HasNullableTuple>
Status BufferedTupleStream::get_next_internal(RowBatch* batch, bool* eos,
        std::vector<RowIdx>* indices) {
    DCHECK(!_closed);
    DCHECK(batch->row_desc().equals(_desc));
    *eos = (_rows_returned == _num_rows);

    if (*eos) {
        return Status::OK();
    }

    DCHECK_GE(_null_indicators_read_block, 0);

    const uint64_t tuples_per_row = _desc.tuple_descriptors().size();
    DCHECK_LE(_read_tuple_idx / tuples_per_row, (*_read_block)->num_rows());
    DCHECK_EQ(_read_tuple_idx % tuples_per_row, 0);
    int rows_returned_curr_block = _read_tuple_idx / tuples_per_row;

    int64_t data_len = (*_read_block)->valid_data_len() - _null_indicators_read_block;

    if (UNLIKELY(rows_returned_curr_block == (*_read_block)->num_rows())) {
        // Get the next block in the stream. We need to do this at the beginning of
        // the GetNext() call to ensure the buffer management semantics. NextBlockForRead()
        // will recycle the memory for the rows returned from the *previous* call to
        // GetNext().
        RETURN_IF_ERROR(next_block_for_read());
        DCHECK(_read_block != _blocks.end()) << debug_string();
        DCHECK_GE(_null_indicators_read_block, 0);
        data_len = (*_read_block)->valid_data_len() - _null_indicators_read_block;
        rows_returned_curr_block = 0;
    }

    DCHECK(_read_block != _blocks.end());
    //DCHECK((*_read_block)->is_pinned()) << DebugString();
    DCHECK(_read_ptr != NULL);

    int64_t rows_left = _num_rows - _rows_returned;
    int rows_to_fill = std::min(
                           static_cast<int64_t>(batch->capacity() - batch->num_rows()), rows_left);
    DCHECK_GE(rows_to_fill, 1);
    batch->add_rows(rows_to_fill);
    uint8_t* tuple_row_mem = reinterpret_cast<uint8_t*>(batch->get_row(batch->num_rows()));


    // Produce tuple rows from the current block and the corresponding position on the
    // null tuple indicator.
    std::vector<RowIdx> local_indices;

    if (indices == NULL) {
        // A hack so that we do not need to check whether 'indices' is not null in the
        // tight loop.
        indices = &local_indices;
    } else {
        DCHECK(!_delete_on_read);
        DCHECK_EQ(batch->num_rows(), 0);
        indices->clear();
    }

    indices->reserve(rows_to_fill);

    int i = 0;
    uint8_t* null_word = NULL;
    uint32_t null_pos = 0;
    // Start reading from position _read_tuple_idx in the block.
    uint64_t last_read_ptr = 0;
    uint64_t last_read_row = _read_tuple_idx / tuples_per_row;

    while (i < rows_to_fill) {
        // Check if current block is done.
        if (UNLIKELY(rows_returned_curr_block + i == (*_read_block)->num_rows())) {
            break;
        }

        // Copy the row into the output batch.
        TupleRow* row = reinterpret_cast<TupleRow*>(tuple_row_mem);
        last_read_ptr = reinterpret_cast<uint64_t>(_read_ptr);
        indices->push_back(RowIdx());
        DCHECK_EQ(indices->size(), i + 1);
        (*indices)[i].set(_read_block_idx, _read_bytes + _null_indicators_read_block,
                          last_read_row);

        if (HasNullableTuple) {
            for (int j = 0; j < tuples_per_row; ++j) {
                // Stitch together the tuples from the block and the NULL ones.
                null_word = (*_read_block)->buffer() + (_read_tuple_idx >> 3);
                null_pos = _read_tuple_idx & 7;
                ++_read_tuple_idx;
                const bool is_not_null = ((*null_word & (1 << (7 - null_pos))) == 0);
                // Copy tuple and advance _read_ptr. If it it is a NULL tuple, it calls SetTuple
                // with Tuple* being 0x0. To do that we multiply the current _read_ptr with
                // false (0x0).
                row->set_tuple(j, reinterpret_cast<Tuple*>(
                                  reinterpret_cast<uint64_t>(_read_ptr) * is_not_null));
                _read_ptr += _desc.tuple_descriptors()[j]->byte_size() * is_not_null;
            }

            const uint64_t row_read_bytes =
                reinterpret_cast<uint64_t>(_read_ptr) - last_read_ptr;
            DCHECK_GE(_fixed_tuple_row_size, row_read_bytes);
            _read_bytes += row_read_bytes;
            last_read_ptr = reinterpret_cast<uint64_t>(_read_ptr);
        } else {
            // When we know that there are no nullable tuples we can safely copy them without
            // checking for nullability.
            for (int j = 0; j < tuples_per_row; ++j) {
                row->set_tuple(j, reinterpret_cast<Tuple*>(_read_ptr));
                _read_ptr += _desc.tuple_descriptors()[j]->byte_size();
            }

            _read_bytes += _fixed_tuple_row_size;
            _read_tuple_idx += tuples_per_row;
        }

        tuple_row_mem += sizeof(Tuple*) * tuples_per_row;

        // Update string slot ptrs.
        for (int j = 0; j < _string_slots.size(); ++j) {
            Tuple* tuple = row->get_tuple(_string_slots[j].first);

            if (HasNullableTuple && tuple == NULL) {
                continue;
            }

            DCHECK(tuple != nullptr);

            for (int k = 0; k < _string_slots[j].second.size(); ++k) {
                const SlotDescriptor* slot_desc = _string_slots[j].second[k];

                if (tuple->is_null(slot_desc->null_indicator_offset())) {
                    continue;
                }

                StringValue* sv = tuple->get_string_slot(slot_desc->tuple_offset());
                DCHECK_LE(sv->len, data_len - _read_bytes);
                sv->ptr = reinterpret_cast<char*>(_read_ptr);
                _read_ptr += sv->len;
                _read_bytes += sv->len;
            }
        }

        ++last_read_row;
        ++i;
    }

    batch->commit_rows(i);
    _rows_returned += i;
    *eos = (_rows_returned == _num_rows);

    if (rows_returned_curr_block + i == (*_read_block)->num_rows()) {
        // No more data in this block. Mark this batch as needing to return so
        // the caller can pass the rows up the operator tree.
        batch->mark_need_to_return();
    }

    DCHECK_EQ(indices->size(), i);
    return Status::OK();
}

// TODO: Move this somewhere in general. We don't want this function inlined
// for the buffered tuple stream case though.
// TODO: In case of null-able tuples we ignore the space we could have saved from the
// null tuples of this row.
int BufferedTupleStream::compute_row_size(TupleRow* row) const {
    int size = _fixed_tuple_row_size;

    for (int i = 0; i < _string_slots.size(); ++i) {
        Tuple* tuple = row->get_tuple(_string_slots[i].first);

        if (_nullable_tuple && tuple == NULL) {
            continue;
        }

        DCHECK(tuple != nullptr);

        for (int j = 0; j < _string_slots[i].second.size(); ++j) {
            const SlotDescriptor* slot_desc = _string_slots[i].second[j];

            if (tuple->is_null(slot_desc->null_indicator_offset())) {
                continue;
            }

            StringValue* sv = tuple->get_string_slot(slot_desc->tuple_offset());
            size += sv->len;
        }
    }

    return size;
}

inline uint8_t* BufferedTupleStream::allocate_row(int size) {
    DCHECK(!_closed);

    if (UNLIKELY(_write_block == NULL || _write_block->bytes_remaining() < size)) {
        bool got_block = false;
        _status = new_block_for_write(size, &got_block);

        if (!_status.ok() || !got_block) {
            return NULL;
        }
    }

    DCHECK(_write_block != NULL);
    //  DCHECK(_write_block->is_pinned());
    DCHECK_GE(_write_block->bytes_remaining(), size);
    ++_num_rows;
    _write_block->add_row();
    return _write_block->allocate<uint8_t>(size);
}

inline void BufferedTupleStream::get_tuple_row(const RowIdx& idx, TupleRow* row) const {
    DCHECK(!_closed);
    //DCHECK(is_pinned());
    DCHECK(!_delete_on_read);
    DCHECK_EQ(_blocks.size(), _block_start_idx.size());
    DCHECK_LT(idx.block(), _blocks.size());

    uint8_t* data = _block_start_idx[idx.block()] + idx.offset();

    if (_nullable_tuple) {
        // Stitch together the tuples from the block and the NULL ones.
        const int tuples_per_row = _desc.tuple_descriptors().size();
        uint32_t tuple_idx = idx.idx() * tuples_per_row;

        for (int i = 0; i < tuples_per_row; ++i) {
            const uint8_t* null_word = _block_start_idx[idx.block()] + (tuple_idx >> 3);
            const uint32_t null_pos = tuple_idx & 7;
            const bool is_not_null = ((*null_word & (1 << (7 - null_pos))) == 0);
            row->set_tuple(i, reinterpret_cast<Tuple*>(
                              reinterpret_cast<uint64_t>(data) * is_not_null));
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

}
