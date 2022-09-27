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

#include "runtime/buffered_tuple_stream2.h"

#include "runtime/descriptors.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "util/bit_util.h"
#include "util/debug_util.h"
#include "util/pretty_printer.h"

using std::stringstream;
using std::string;
using std::vector;
using std::list;

using std::unique_ptr;

namespace doris {

// The first NUM_SMALL_BLOCKS of the tuple stream are made of blocks less than the
// IO size. These blocks never spill.
// TODO: Consider adding a 4MB in-memory buffer that would split the gap between the
// 512KB in-memory buffer and the 8MB (IO-sized) spillable buffer.
static const int64_t INITIAL_BLOCK_SIZES[] = {64 * 1024, 512 * 1024};
static const int NUM_SMALL_BLOCKS = sizeof(INITIAL_BLOCK_SIZES) / sizeof(int64_t);

string BufferedTupleStream2::RowIdx::debug_string() const {
    stringstream ss;
    ss << "RowIdx block=" << block() << " offset=" << offset() << " idx=" << idx();
    return ss.str();
}

BufferedTupleStream2::BufferedTupleStream2(RuntimeState* state, const RowDescriptor& row_desc,
                                           BufferedBlockMgr2* block_mgr,
                                           BufferedBlockMgr2::Client* client,
                                           bool use_initial_small_buffers, bool read_write)
        : _use_small_buffers(use_initial_small_buffers),
          _delete_on_read(false),
          _read_write(read_write),
          _state(state),
          _desc(row_desc),
          _nullable_tuple(row_desc.is_any_tuple_nullable()),
          _block_mgr(block_mgr),
          _block_mgr_client(client),
          _total_byte_size(0),
          _read_ptr(nullptr),
          _read_tuple_idx(0),
          _read_bytes(0),
          _rows_returned(0),
          _read_block_idx(-1),
          _write_block(nullptr),
          _num_pinned(0),
          _num_small_blocks(0),
          _closed(false),
          _num_rows(0),
          _pinned(true),
          _pin_timer(nullptr),
          _unpin_timer(nullptr),
          _get_new_block_timer(nullptr) {
    _null_indicators_read_block = _null_indicators_write_block = -1;
    _read_block = _blocks.end();
    _fixed_tuple_row_size = 0;
    for (int i = 0; i < _desc.tuple_descriptors().size(); ++i) {
        const TupleDescriptor* tuple_desc = _desc.tuple_descriptors()[i];
        const int tuple_byte_size = tuple_desc->byte_size();
        _fixed_tuple_row_size += tuple_byte_size;
        if (!tuple_desc->string_slots().empty()) {
            _string_slots.push_back(make_pair(i, tuple_desc->string_slots()));
        }
        // if (!tuple_desc->collection_slots().empty()) {
        //     _collection_slots.push_back(make_pair(i, tuple_desc->collection_slots()));
        // }
    }
}

// Returns the number of pinned blocks in the list.
// Only called in DCHECKs to validate _num_pinned.
int num_pinned(const list<BufferedBlockMgr2::Block*>& blocks) {
    int num_pinned = 0;
    for (list<BufferedBlockMgr2::Block*>::const_iterator it = blocks.begin(); it != blocks.end();
         ++it) {
        if ((*it)->is_pinned() && (*it)->is_max_size()) {
            ++num_pinned;
        }
    }
    return num_pinned;
}

string BufferedTupleStream2::debug_string() const {
    stringstream ss;
    ss << "BufferedTupleStream2 num_rows=" << _num_rows << " rows_returned=" << _rows_returned
       << " pinned=" << (_pinned ? "true" : "false")
       << " delete_on_read=" << (_delete_on_read ? "true" : "false")
       << " closed=" << (_closed ? "true" : "false") << " num_pinned=" << _num_pinned
       << " write_block=" << _write_block << " _read_block=";
    if (_read_block == _blocks.end()) {
        ss << "<end>";
    } else {
        ss << *_read_block;
    }
    ss << " blocks=[\n";
    for (list<BufferedBlockMgr2::Block*>::const_iterator it = _blocks.begin(); it != _blocks.end();
         ++it) {
        ss << "{" << (*it)->debug_string() << "}";
        if (*it != _blocks.back()) {
            ss << ",\n";
        }
    }
    ss << "]";
    return ss.str();
}

Status BufferedTupleStream2::init(int node_id, RuntimeProfile* profile, bool pinned) {
    if (profile != nullptr) {
        _pin_timer = ADD_TIMER(profile, "PinTime");
        _unpin_timer = ADD_TIMER(profile, "UnpinTime");
        _get_new_block_timer = ADD_TIMER(profile, "GetNewBlockTime");
    }

    if (_block_mgr->max_block_size() < INITIAL_BLOCK_SIZES[0]) {
        _use_small_buffers = false;
    }

    bool got_block = false;
    RETURN_IF_ERROR(new_block_for_write(_fixed_tuple_row_size, &got_block));
    if (!got_block) {
        return _block_mgr->mem_limit_too_low_error(_block_mgr_client, node_id);
    }
    DCHECK(_write_block != nullptr);
    if (!pinned) {
        RETURN_IF_ERROR(unpin_stream());
    }
    return Status::OK();
}

Status BufferedTupleStream2::switch_to_io_buffers(bool* got_buffer) {
    if (!_use_small_buffers) {
        *got_buffer = (_write_block != nullptr);
        return Status::OK();
    }
    _use_small_buffers = false;
    Status status = new_block_for_write(_block_mgr->max_block_size(), got_buffer);
    // IMPALA-2330: Set the flag using small buffers back to false in case it failed to
    // got a buffer.
    DCHECK(status.ok() || !*got_buffer) << status.ok() << " " << *got_buffer;
    _use_small_buffers = !*got_buffer;
    return status;
}

void BufferedTupleStream2::close() {
    for (list<BufferedBlockMgr2::Block*>::iterator it = _blocks.begin(); it != _blocks.end();
         ++it) {
        (*it)->del();
    }
    _blocks.clear();
    _num_pinned = 0;
    DCHECK_EQ(_num_pinned, num_pinned(_blocks));
    _closed = true;
}

int64_t BufferedTupleStream2::bytes_in_mem(bool ignore_current) const {
    int64_t result = 0;
    for (list<BufferedBlockMgr2::Block*>::const_iterator it = _blocks.begin(); it != _blocks.end();
         ++it) {
        if (!(*it)->is_pinned()) {
            continue;
        }
        if (!(*it)->is_max_size()) {
            continue;
        }
        if (*it == _write_block && ignore_current) {
            continue;
        }
        result += (*it)->buffer_len();
    }
    return result;
}

Status BufferedTupleStream2::unpin_block(BufferedBlockMgr2::Block* block) {
    SCOPED_TIMER(_unpin_timer);
    DCHECK(block->is_pinned());
    if (!block->is_max_size()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(block->unpin());
    --_num_pinned;
    DCHECK_EQ(_num_pinned, num_pinned(_blocks));
    return Status::OK();
}

Status BufferedTupleStream2::new_block_for_write(int64_t min_size, bool* got_block) {
    DCHECK(!_closed);
    *got_block = false;
    if (min_size > _block_mgr->max_block_size()) {
        std::stringstream error_msg;
        error_msg << "Cannot process row that is bigger than the IO size (row_size="
                  << PrettyPrinter::print(min_size, TUnit::BYTES)
                  << "). To run this query, increase the IO size (--read_size option).";
        return Status::InternalError(error_msg.str());
    }

    BufferedBlockMgr2::Block* unpin_block = _write_block;
    if (_write_block != nullptr) {
        DCHECK(_write_block->is_pinned());
        if (_pinned || _write_block == *_read_block || !_write_block->is_max_size()) {
            // In these cases, don't unpin the current write block.
            unpin_block = nullptr;
        }
    }

    int64_t block_len = _block_mgr->max_block_size();
    if (_use_small_buffers) {
        if (_blocks.size() < NUM_SMALL_BLOCKS) {
            block_len = std::min(block_len, INITIAL_BLOCK_SIZES[_blocks.size()]);
            if (block_len < min_size) {
                block_len = _block_mgr->max_block_size();
            }
        }
        if (block_len == _block_mgr->max_block_size()) {
            // Do not switch to IO-buffers automatically. Do not get a buffer.
            *got_block = false;
            return Status::OK();
        }
    }

    BufferedBlockMgr2::Block* new_block = nullptr;
    {
        SCOPED_TIMER(_get_new_block_timer);
        RETURN_IF_ERROR(
                _block_mgr->get_new_block(_block_mgr_client, unpin_block, &new_block, block_len));
    }
    *got_block = (new_block != nullptr);

    if (!*got_block) {
        DCHECK(unpin_block == nullptr);
        return Status::OK();
    }

    if (unpin_block != nullptr) {
        DCHECK(unpin_block == _write_block);
        DCHECK(!_write_block->is_pinned());
        --_num_pinned;
        DCHECK_EQ(_num_pinned, num_pinned(_blocks));
    }

    // Compute and allocate the block header with the null indicators
    _null_indicators_write_block = compute_num_null_indicator_bytes(block_len);
    new_block->allocate<uint8_t>(_null_indicators_write_block);
    _write_tuple_idx = 0;

    _blocks.push_back(new_block);
    _block_start_idx.push_back(new_block->buffer());
    _write_block = new_block;
    DCHECK(_write_block->is_pinned());
    DCHECK_EQ(_write_block->num_rows(), 0);
    if (_write_block->is_max_size()) {
        ++_num_pinned;
        DCHECK_EQ(_num_pinned, num_pinned(_blocks));
    } else {
        ++_num_small_blocks;
    }
    _total_byte_size += block_len;
    return Status::OK();
}

Status BufferedTupleStream2::next_block_for_read() {
    DCHECK(!_closed);
    DCHECK(_read_block != _blocks.end());
    DCHECK_EQ(_num_pinned, num_pinned(_blocks)) << _pinned;

    // If non-nullptr, this will be the current block if we are going to free it while
    // grabbing the next block. This will stay nullptr if we don't want to free the
    // current block.
    BufferedBlockMgr2::Block* block_to_free =
            (!_pinned || _delete_on_read) ? *_read_block : nullptr;
    if (_delete_on_read) {
        // TODO: this is weird. We are deleting even if it is pinned. The analytic
        // eval node needs this.
        DCHECK(_read_block == _blocks.begin());
        DCHECK(*_read_block != _write_block);
        _blocks.pop_front();
        _read_block = _blocks.begin();
        _read_block_idx = 0;
        if (block_to_free != nullptr && !block_to_free->is_max_size()) {
            block_to_free->del();
            block_to_free = nullptr;
            DCHECK_EQ(_num_pinned, num_pinned(_blocks)) << debug_string();
        }
    } else {
        ++_read_block;
        ++_read_block_idx;
        if (block_to_free != nullptr && !block_to_free->is_max_size()) {
            block_to_free = nullptr;
        }
    }

    _read_ptr = nullptr;
    _read_tuple_idx = 0;
    _read_bytes = 0;

    bool pinned = false;
    if (_read_block == _blocks.end() || (*_read_block)->is_pinned()) {
        // End of the blocks or already pinned, just handle block_to_free
        if (block_to_free != nullptr) {
            SCOPED_TIMER(_unpin_timer);
            if (_delete_on_read) {
                block_to_free->del();
                --_num_pinned;
            } else {
                RETURN_IF_ERROR(unpin_block(block_to_free));
            }
        }
    } else {
        // Call into the block mgr to atomically unpin/delete the old block and pin the
        // new block.
        SCOPED_TIMER(_pin_timer);
        RETURN_IF_ERROR((*_read_block)->pin(&pinned, block_to_free, !_delete_on_read));
        if (!pinned) {
            DCHECK(block_to_free == nullptr) << "Should have been able to pin." << std::endl
                                             << _block_mgr->debug_string(_block_mgr_client);
        }
        if (block_to_free == nullptr && pinned) {
            ++_num_pinned;
        }
    }

    if (_read_block != _blocks.end() && (*_read_block)->is_pinned()) {
        _null_indicators_read_block =
                compute_num_null_indicator_bytes((*_read_block)->buffer_len());
        _read_ptr = (*_read_block)->buffer() + _null_indicators_read_block;
    }
    DCHECK_EQ(_num_pinned, num_pinned(_blocks)) << debug_string();
    return Status::OK();
}

Status BufferedTupleStream2::prepare_for_read(bool delete_on_read, bool* got_buffer) {
    DCHECK(!_closed);
    if (_blocks.empty()) {
        return Status::OK();
    }

    if (!_read_write && _write_block != nullptr) {
        DCHECK(_write_block->is_pinned());
        if (!_pinned && _write_block != _blocks.front()) {
            RETURN_IF_ERROR(unpin_block(_write_block));
        }
        _write_block = nullptr;
    }

    // Walk the blocks and pin the first non-io sized block.
    // (small buffers always being pinned, no need to pin again)
    for (list<BufferedBlockMgr2::Block*>::iterator it = _blocks.begin(); it != _blocks.end();
         ++it) {
        if (!(*it)->is_pinned()) {
            SCOPED_TIMER(_pin_timer);
            bool current_pinned = false;
            RETURN_IF_ERROR((*it)->pin(&current_pinned));
            if (!current_pinned) {
                DCHECK(got_buffer != nullptr) << "Should have reserved enough blocks";
                *got_buffer = false;
                return Status::OK();
            }
            ++_num_pinned;
            DCHECK_EQ(_num_pinned, num_pinned(_blocks));
        }
        if ((*it)->is_max_size()) {
            break;
        }
    }

    _read_block = _blocks.begin();
    DCHECK(_read_block != _blocks.end());
    _null_indicators_read_block = compute_num_null_indicator_bytes((*_read_block)->buffer_len());
    _read_ptr = (*_read_block)->buffer() + _null_indicators_read_block;
    _read_tuple_idx = 0;
    _read_bytes = 0;
    _rows_returned = 0;
    _read_block_idx = 0;
    _delete_on_read = delete_on_read;
    if (got_buffer != nullptr) {
        *got_buffer = true;
    }
    return Status::OK();
}

Status BufferedTupleStream2::pin_stream(bool already_reserved, bool* pinned) {
    DCHECK(!_closed);
    DCHECK(pinned != nullptr);
    if (!already_reserved) {
        // If we can't get all the blocks, don't try at all.
        if (!_block_mgr->try_acquire_tmp_reservation(_block_mgr_client, blocks_unpinned())) {
            *pinned = false;
            return Status::OK();
        }
    }

    for (list<BufferedBlockMgr2::Block*>::iterator it = _blocks.begin(); it != _blocks.end();
         ++it) {
        if ((*it)->is_pinned()) {
            continue;
        }
        {
            SCOPED_TIMER(_pin_timer);
            RETURN_IF_ERROR((*it)->pin(pinned));
        }
        if (!*pinned) {
            VLOG_QUERY << "Should have been reserved." << std::endl
                       << _block_mgr->debug_string(_block_mgr_client);
            return Status::OK();
        }
        ++_num_pinned;
        DCHECK_EQ(_num_pinned, num_pinned(_blocks));
    }

    if (!_delete_on_read) {
        // Populate _block_start_idx on pin.
        DCHECK_EQ(_block_start_idx.size(), _blocks.size());
        _block_start_idx.clear();
        for (list<BufferedBlockMgr2::Block*>::iterator it = _blocks.begin(); it != _blocks.end();
             ++it) {
            _block_start_idx.push_back((*it)->buffer());
        }
    }
    *pinned = true;
    _pinned = true;
    return Status::OK();
}

Status BufferedTupleStream2::unpin_stream(bool all) {
    DCHECK(!_closed);
    SCOPED_TIMER(_unpin_timer);

    for (BufferedBlockMgr2::Block* block : _blocks) {
        if (!block->is_pinned()) {
            continue;
        }
        if (!all && (block == _write_block || (_read_write && block == *_read_block))) {
            continue;
        }
        RETURN_IF_ERROR(unpin_block(block));
    }
    if (all) {
        _read_block = _blocks.end();
        _write_block = nullptr;
    }
    _pinned = false;
    return Status::OK();
}

int BufferedTupleStream2::compute_num_null_indicator_bytes(int block_size) const {
    if (_nullable_tuple) {
        // We assume that all rows will use their max size, so we may be underutilizing the
        // space, i.e. we may have some unused space in case of rows with nullptr tuples.
        const uint32_t tuples_per_row = _desc.tuple_descriptors().size();
        const uint32_t min_row_size_in_bits = 8 * _fixed_tuple_row_size + tuples_per_row;
        const uint32_t block_size_in_bits = 8 * block_size;
        const uint32_t max_num_rows = block_size_in_bits / min_row_size_in_bits;
        return BitUtil::round_up_numi64(max_num_rows * tuples_per_row) * 8;
    } else {
        // If there are no nullable tuples then no need to waste space for null indicators.
        return 0;
    }
}

Status BufferedTupleStream2::get_rows(unique_ptr<RowBatch>* batch, bool* got_rows) {
    RETURN_IF_ERROR(pin_stream(false, got_rows));
    if (!*got_rows) {
        return Status::OK();
    }
    RETURN_IF_ERROR(prepare_for_read(false));
    batch->reset(new RowBatch(_desc, num_rows(), _block_mgr->get_tracker(_block_mgr_client).get()));
    bool eos = false;
    // Loop until get_next fills the entire batch. Each call can stop at block
    // boundaries. We generally want it to stop, so that blocks can be freed
    // as we read. It is safe in this case because we pin the entire stream.
    while (!eos) {
        RETURN_IF_ERROR(get_next(batch->get(), &eos));
    }
    return Status::OK();
}

Status BufferedTupleStream2::get_next(RowBatch* batch, bool* eos, vector<RowIdx>* indices) {
    if (_nullable_tuple) {
        return get_next_internal<true>(batch, eos, indices);
    } else {
        return get_next_internal<false>(batch, eos, indices);
    }
}

template <bool HasNullableTuple>
Status BufferedTupleStream2::get_next_internal(RowBatch* batch, bool* eos,
                                               vector<RowIdx>* indices) {
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
        // the get_next() call to ensure the buffer management semantics. next_block_for_read()
        // will recycle the memory for the rows returned from the *previous* call to
        // get_next().
        RETURN_IF_ERROR(next_block_for_read());
        DCHECK(_read_block != _blocks.end()) << debug_string();
        DCHECK_GE(_null_indicators_read_block, 0);
        data_len = (*_read_block)->valid_data_len() - _null_indicators_read_block;
        rows_returned_curr_block = 0;
    }

    DCHECK(_read_block != _blocks.end());
    DCHECK((*_read_block)->is_pinned()) << debug_string();
    DCHECK(_read_ptr != nullptr);

    int64_t rows_left = _num_rows - _rows_returned;
    int rows_to_fill =
            std::min(static_cast<int64_t>(batch->capacity() - batch->num_rows()), rows_left);
    DCHECK_GE(rows_to_fill, 1);
    batch->add_rows(rows_to_fill);
    uint8_t* tuple_row_mem = reinterpret_cast<uint8_t*>(batch->get_row(batch->num_rows()));

    // Produce tuple rows from the current block and the corresponding position on the
    // null tuple indicator.
    vector<RowIdx> local_indices;
    if (indices == nullptr) {
        // A hack so that we do not need to check whether 'indices' is not null in the
        // tight loop.
        indices = &local_indices;
    } else {
        DCHECK(is_pinned());
        DCHECK(!_delete_on_read);
        DCHECK_EQ(batch->num_rows(), 0);
        indices->clear();
    }
    indices->reserve(rows_to_fill);

    int i = 0;
    uint8_t* null_word = nullptr;
    uint32_t null_pos = 0;
    // Start reading from position _read_tuple_idx in the block.
    uint64_t last_read_ptr = 0;
    // IMPALA-2256: Special case if there are no materialized slots.
    bool increment_row = has_tuple_footprint();
    uint64_t last_read_row = increment_row * (_read_tuple_idx / tuples_per_row);
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
                // Stitch together the tuples from the block and the nullptr ones.
                null_word = (*_read_block)->buffer() + (_read_tuple_idx >> 3);
                null_pos = _read_tuple_idx & 7;
                ++_read_tuple_idx;
                const bool is_not_null = ((*null_word & (1 << (7 - null_pos))) == 0);
                // Copy tuple and advance _read_ptr. If it is a nullptr tuple, it calls set_tuple
                // with Tuple* being 0x0. To do that we multiply the current _read_ptr with
                // false (0x0).
                row->set_tuple(j, reinterpret_cast<Tuple*>(reinterpret_cast<uint64_t>(_read_ptr) *
                                                           is_not_null));
                _read_ptr += _desc.tuple_descriptors()[j]->byte_size() * is_not_null;
            }
            const uint64_t row_read_bytes = reinterpret_cast<uint64_t>(_read_ptr) - last_read_ptr;
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
            if (HasNullableTuple && tuple == nullptr) {
                continue;
            }
            read_strings(_string_slots[j].second, data_len, tuple);
        }

        // Update collection slot ptrs. We traverse the collection structure in the same order
        // as it was written to the stream, allowing us to infer the data layout based on the
        // length of collections and strings.
        // for (int j = 0; j < _collection_slots.size(); ++j) {
        //     Tuple* tuple = row->get_tuple(_collection_slots[j].first);
        //     if (HasNullableTuple && tuple == nullptr) {
        //         continue;
        //     }
        //     ReadCollections(_collection_slots[j].second, data_len, tuple);
        // }
        last_read_row += increment_row;
        ++i;
    }

    batch->commit_rows(i);
    _rows_returned += i;
    *eos = (_rows_returned == _num_rows);
    if ((!_pinned || _delete_on_read) &&
        rows_returned_curr_block + i == (*_read_block)->num_rows()) {
        // No more data in this block. Mark this batch as needing to return so
        // the caller can pass the rows up the operator tree.
        batch->mark_need_to_return();
    }
    DCHECK_EQ(indices->size(), i);
    return Status::OK();
}

void BufferedTupleStream2::read_strings(const vector<SlotDescriptor*>& string_slots, int data_len,
                                        Tuple* tuple) {
    DCHECK(tuple != nullptr);
    for (int i = 0; i < string_slots.size(); ++i) {
        const SlotDescriptor* slot_desc = string_slots[i];
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

int64_t BufferedTupleStream2::compute_row_size(TupleRow* row) const {
    int64_t size = 0;
    for (int i = 0; i < _desc.tuple_descriptors().size(); ++i) {
        const TupleDescriptor* tuple_desc = _desc.tuple_descriptors()[i];
        Tuple* tuple = row->get_tuple(i);
        DCHECK(_nullable_tuple || tuple_desc->byte_size() == 0 || tuple != nullptr);
        if (tuple == nullptr) {
            continue;
        }
        size += tuple->total_byte_size(*tuple_desc);
    }
    return size;
}

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
    if (UNLIKELY(_write_block == nullptr)) {
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

    // Copy the not nullptr fixed len tuples. For the nullptr tuples just update the nullptr tuple
    // indicator.
    if (HasNullableTuple) {
        DCHECK_GT(_null_indicators_write_block, 0);
        uint8_t* null_word = nullptr;
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
            if (t != nullptr) {
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
            // is delivered, the check below should become DCHECK(t != nullptr).
            DCHECK(t != nullptr || tuple_size == 0);
            memcpy(tuple_buf, t, tuple_size);
            tuple_buf += tuple_size;
        }
    }

    // Copy string slots. Note: we do not need to convert the string ptrs to offsets
    // on the write path, only on the read. The tuple data is immediately followed
    // by the string data so only the len information is necessary.
    for (int i = 0; i < _string_slots.size(); ++i) {
        Tuple* tuple = row->get_tuple(_string_slots[i].first);
        if (HasNullableTuple && tuple == nullptr) {
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
    //     if (HasNullableTuple && tuple == nullptr) continue;
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
} // end namespace doris
