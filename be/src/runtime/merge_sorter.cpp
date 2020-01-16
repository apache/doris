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

#include "runtime/merge_sorter.h"
#include "runtime/buffered_block_mgr.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include <string>
#include <boost/foreach.hpp>
#include "runtime/mem_tracker.h"

namespace doris {
// A run is a sequence of blocks containing tuples that are or will eventually be in
// sorted order.
// A run may maintain two sequences of blocks - one containing the tuples themselves,
// (i.e. fixed-len slots and ptrs to var-len data), and the other for the var-length
// column data pointed to by those tuples.
// Tuples in a run may be sorted in place (in-memory) and merged using a merger.
class MergeSorter::Run {
public:
    // materialize_slots is true for runs constructed from input rows. The input rows are
    // materialized into single sort tuples using the expressions in
    // _sort_tuple_slot_expr_ctxs. For intermediate merges, the tuples are already
    // materialized so materialize_slots is false.
    Run(MergeSorter* parent, TupleDescriptor* sort_tuple_desc, bool materialize_slots);

    ~Run() {}
    // Initialize the run for input rows by allocating the minimum number of required
    // blocks - one block for fixed-len data added to _fixed_len_blocks, one for the
    // initially unsorted var-len data added to _var_len_blocks, and one to copy sorted
    // var-len data into (var_len_copy_block_).
    Status init();

    // Add a batch of input rows to the current run. Returns the number
    // of rows actually added in num_processed. If the run is full (no more blocks can
    // be allocated), num_processed may be less than the number of rows in the batch.
    // If _materialize_slots is true, materializes the input rows using the expressions
    // in _sorter->_sort_tuple_slot_expr_ctxs, else just copies the input rows.
    template <bool has_var_len_data>
    Status add_batch(RowBatch* batch, int start_index, int* num_processed);

    // Interface for merger - get the next batch of rows from this run. The callee (Run)
    // still owns the returned batch. Calls get_next(RowBatch*, bool*).
    Status get_next_batch(RowBatch** sorted_batch);

private:
    friend class MergeSorter;
    friend class TupleSorter;

    template <bool convert_offset_to_ptr>
    Status get_next(RowBatch* output_batch, bool* eos);

    // Check if the current run can be extended by a block. Add the newly allocated block
    // to block_sequence, or set added to false if the run could not be extended.
    // If the run is sorted (produced by an intermediate merge), unpin the last block in
    // block_sequence before allocating and adding a new block - the run can always be
    // extended in this case. If the run is unsorted, check max_blocks_in_unsorted_run_
    // to see if a block can be added to the run. Also updates the sort bytes counter.
    Status try_add_block(std::vector<BufferedBlockMgr::Block*>* block_sequence, bool* added);

    // Prepare to read a sorted run. Pins the first block(s) in the run if the run was
    // previously unpinned.
    Status prepare_read();

    // Copy the StringValue data in var_values to dest in order and update the StringValue
    // ptrs to point to the copied data.
    void copy_var_len_data(char* dest, const std::vector<StringValue*>& var_values);

    // Parent sorter object.
    const MergeSorter* _sorter;

    // Materialized sort tuple. Input rows are materialized into 1 tuple (with descriptor
    // _sort_tuple_desc) before sorting.
    const TupleDescriptor* _sort_tuple_desc;

    // Sizes of sort tuple and block.
    const int _sort_tuple_size;
    const int _block_size;

    const bool _has_var_len_slots;

    // True if the sort tuple must be materialized from the input batch in add_batch().
    // _materialize_slots is true for runs being constructed from input batches, and
    // is false for runs being constructed from intermediate merges.
    const bool _materialize_slots;

    // True if the run is sorted. Set to true after an in-memory sort, and initialized to
    // true for runs resulting from merges.
    bool _is_sorted;

    // Sequence of blocks in this run containing the fixed-length portion of the
    // sort tuples comprising this run. The data pointed to by the var-len slots are in
    // _var_len_blocks. If _is_sorted is true, the tuples in _fixed_len_blocks will be in
    // sorted order.
    // _fixed_len_blocks[i] is NULL iff it has been deleted.
    std::vector<BufferedBlockMgr::Block*> _fixed_len_blocks;

    // Sequence of blocks in this run containing the var-length data corresponding to the
    // var-length column data from _fixed_len_blocks. These are reconstructed to be in
    // sorted order in UnpinAllBlocks().
    // _var_len_blocks[i] is NULL iff it has been deleted.
    std::vector<BufferedBlockMgr::Block*> _var_len_blocks;

    // Number of tuples so far in this run.
    int64_t _num_tuples;

    // Number of tuples returned via get_next(), maintained for debug purposes.
    int64_t _num_tuples_returned;


    // Members used when a run is read in get_next()
    // The index into the fixed_ and _var_len_blocks vectors of the current blocks being
    // processed in get_next().
    int _fixed_len_blocks_index;

    // Offset into the current fixed length data block being processed.
    int _fixed_len_block_offset;
}; // class MergeSorter::Run

// Sorts a sequence of tuples from a run in place using a provided tuple comparator.
// Quick sort is used for sequences of tuples larger that 16 elements, and
// insertion sort is used for smaller sequences.
// The TupleSorter is initialized with a RuntimeState instance to check for
// cancellation during an in-memory sort.
class MergeSorter::TupleSorter {
public:
    TupleSorter(const TupleRowComparator& less_than_comp, int64_t block_size,
                int tuple_size, RuntimeState* state);

    ~TupleSorter() {
        delete[] _temp_tuple_buffer;
        delete[] _swap_buffer;
    }

    // Performs a quicksort for tuples in 'run' followed by an insertion sort to
    // finish smaller blocks.
    // Returns early if stste_->is_cancelled() is true. No status
    // is returned - the caller must check for cancellation.

    void sort(Run* run) {
        _run = run;
        sort_helper(TupleIterator(this, 0), TupleIterator(this, _run->_num_tuples));
        run->_is_sorted = true;
    }

private:
    static const int INSERTION_THRESHOLD = 16;
    // static const int INSERTION_THRESHOLD = FLAGS_insertion_threadhold;

    // Helper class used to iterate over tuples in a run during quick sort and insertion
    // sort.
    class TupleIterator {
    public:
        TupleIterator(TupleSorter* parent, int64_t index)
            : _parent(parent),
              _index(index) {
            DCHECK_GE(index, 0);
            DCHECK_LE(index, _parent->_run->_num_tuples);
            // If the run is empty, only _index is initialized.
            if (_parent->_run->_num_tuples == 0) {
                return;
            }
            // If the iterator is initialized to past the end, set up _buffer_start and
            // _block_index as if it pointing to the last tuple. Add _tuple_size bytes to
            // _current_tuple, so everything is correct when prev() is invoked.
            int past_end_bytes = 0;
            if (UNLIKELY(index >= _parent->_run->_num_tuples)) {
                past_end_bytes = parent->_tuple_size;
                _index = _parent->_run->_num_tuples;
                index = _index - 1;
            }
            _block_index = index / parent->_block_capacity;
            _buffer_start = parent->_run->_fixed_len_blocks[_block_index]->buffer();
            int block_offset = (index % parent->_block_capacity) * parent->_tuple_size;
            _current_tuple = _buffer_start + block_offset + past_end_bytes;
        }
        ~TupleIterator() {}
        // Sets _current_tuple to point to the next tuple in the run. Increments
        // block_index and resets buffer if the next tuple is in the next block.
        void next() {
            _current_tuple += _parent->_tuple_size;
            ++_index;
            if (UNLIKELY(_current_tuple > _buffer_start + _parent->_last_tuple_block_offset &&
                         _index < _parent->_run->_num_tuples)) {
                // Don't increment block index, etc. past the end.
                ++_block_index;
                DCHECK_LT(_block_index, _parent->_run->_fixed_len_blocks.size());
                _buffer_start = _parent->_run->_fixed_len_blocks[_block_index]->buffer();
                _current_tuple = _buffer_start;
            }
        }

        // Sets current_tuple to point to the previous tuple in the run. Decrements
        // block_index and resets buffer if the new tuple is in the previous block.
        void prev() {
            _current_tuple -= _parent->_tuple_size;
            --_index;
            if (UNLIKELY(_current_tuple < _buffer_start && _index >= 0)) {
                --_block_index;
                DCHECK_GE(_block_index, 0);
                _buffer_start = _parent->_run->_fixed_len_blocks[_block_index]->buffer();
                _current_tuple = _buffer_start + _parent->_last_tuple_block_offset;
            }
        }

    private:
        friend class TupleSorter;

        // Pointer to the tuple sorter.
        TupleSorter* _parent;

        // Index of the current tuple in the run.
        int64_t _index;

        // Pointer to the current tuple.
        uint8_t* _current_tuple = nullptr;

        // Start of the buffer containing current tuple.
        uint8_t* _buffer_start = nullptr;

        // Index into _run._fixed_len_blocks of the block containing the current tuple.
        int _block_index;
    };

    // Size of the tuples in memory.
    const int _tuple_size;

    // Number of tuples per block in a run.
    const int _block_capacity;

    // Offset in bytes of the last tuple in a block, calculated from block and tuple sizes.
    const int _last_tuple_block_offset;

    // Tuple comparator that returns true if lhs < rhs.
    const TupleRowComparator _less_than_comp;

    // Runtime state instance to check for cancellation. Not owned.
    RuntimeState* const _state;

    // The run to be sorted.
    Run* _run;

    // Temporarily allocated space to copy and swap tuples (Both are used in partition()).
    // temp_tuple_ points to _temp_tuple_buffer. Owned by this TupleSorter instance.
    TupleRow* _temp_tuple_row;
    uint8_t* _temp_tuple_buffer;
    uint8_t* _swap_buffer;

    // Perform an insertion sort for rows in the range [first, last) in a run.
    void insertion_sort(const TupleIterator& first, const TupleIterator& last);

    // Partitions the sequence of tuples in the range [first, last) in a run into two
    // groups around the pivot tuple - i.e. tuples in first group are <= the pivot, and
    // tuples in the second group are >= pivot. Tuples are swapped in place to create the
    // groups and the index to the first element in the second group is returned.
    // Checks _state->is_cancelled() and returns early with an invalid result if true.
    TupleIterator partition(TupleIterator first, TupleIterator last, Tuple* pivot);

    // Performs a quicksort of rows in the range [first, last).
    // followed by insertion sort for smaller groups of elements.
    // Checks _state->is_cancelled() and returns early if true.
    void sort_helper(TupleIterator first, TupleIterator last);

    // Swaps tuples pointed to by left and right using the swap buffer.
    void swap(uint8_t* left, uint8_t* right);
}; // class TupleSorter

// MergeSorter::Run methods
MergeSorter::Run::Run(MergeSorter* parent, TupleDescriptor* sort_tuple_desc,
                 bool materialize_slots)
    : _sorter(parent),
      _sort_tuple_desc(sort_tuple_desc),
      _sort_tuple_size(sort_tuple_desc->byte_size()),
      _block_size(parent->_block_mgr->max_block_size()),
      _has_var_len_slots(sort_tuple_desc->string_slots().size() > 0),
      _materialize_slots(materialize_slots),
      _is_sorted(!materialize_slots),
      _num_tuples(0) {
}

Status MergeSorter::Run::init() {
    BufferedBlockMgr::Block* block = NULL;
    RETURN_IF_ERROR(
        _sorter->_block_mgr->get_new_block(&block));
    DCHECK(block != NULL);
    _fixed_len_blocks.push_back(block);

    if (_has_var_len_slots) {
        RETURN_IF_ERROR(
            _sorter->_block_mgr->get_new_block(&block));
        DCHECK(block != NULL);
        _var_len_blocks.push_back(block);
    }

    if (!_is_sorted) {
        _sorter->_initial_runs_counter->update(1);
    }
    return Status::OK();
}

template <bool has_var_len_data>
Status MergeSorter::Run::add_batch(RowBatch* batch, int start_index, int* num_processed) {
    *num_processed = 0;
    BufferedBlockMgr::Block* cur_fixed_len_block = _fixed_len_blocks.back();

    DCHECK_EQ(_materialize_slots, !_is_sorted);
    DCHECK_EQ(_materialize_slots, true);

    // Input rows are copied/materialized into tuples allocated in _fixed_len_blocks.
    // The variable length column data are copied into blocks stored in _var_len_blocks.
    // Input row processing is split into two loops.
    // The inner loop processes as many input rows as will fit in cur_fixed_len_block.
    // The outer loop allocates a new block for fixed-len data if the input batch is
    // not exhausted.

    // cur_input_index is the index into the input 'batch' of the current
    // input row being processed.
    int cur_input_index = start_index;
    std::vector<StringValue*> var_values;
    var_values.reserve(_sort_tuple_desc->string_slots().size());
    while (cur_input_index < batch->num_rows()) {
        // tuples_remaining is the number of tuples to copy/materialize into
        // cur_fixed_len_block.
        int tuples_remaining = cur_fixed_len_block->bytes_remaining() / _sort_tuple_size;
        tuples_remaining = std::min(batch->num_rows() - cur_input_index, tuples_remaining);

        for (int i = 0; i < tuples_remaining; ++i) {
            int total_var_len = 0;
            TupleRow* input_row = batch->get_row(cur_input_index);
            Tuple* new_tuple = cur_fixed_len_block->allocate<Tuple>(_sort_tuple_size);
            if (_materialize_slots) {
                new_tuple->materialize_exprs<has_var_len_data>(input_row, *_sort_tuple_desc,
                        _sorter->_sort_tuple_slot_expr_ctxs, NULL, &var_values, &total_var_len);
                if (total_var_len > _sorter->_block_mgr->max_block_size()) {
                    std::stringstream ss;
                    ss << "Variable length data in a single tuple larger than block size ";
                    ss << total_var_len;
                    ss << " > " << _sorter->_block_mgr->max_block_size();
                    return Status::InternalError(ss.str());
                }
            }

            if (has_var_len_data) {
                BufferedBlockMgr::Block* cur_var_len_block = _var_len_blocks.back();
                if (cur_var_len_block->bytes_remaining() < total_var_len) {
                    bool added;
                    RETURN_IF_ERROR(try_add_block(&_var_len_blocks, &added));
                    if (added) {
                        cur_var_len_block = _var_len_blocks.back();
                    } else {
                        // There wasn't enough space in the last var-len block for this tuple, and
                        // the run could not be extended. Return the fixed-len allocation and exit.
                        // dhc: we can't get here, because we can get the new block. If we can't get new block,
                        // we will exit in tryAddBlock(MemTracker exceed).
                        cur_fixed_len_block->return_allocation(_sort_tuple_size);
                        return Status::OK();
                    }
                }

                char* var_data_ptr = cur_var_len_block->allocate<char>(total_var_len);
                if (_materialize_slots) {
                    copy_var_len_data(var_data_ptr, var_values);
                }
            }

            ++_num_tuples;
            ++*num_processed;
            ++cur_input_index;
        }

        // If there are still rows left to process, get a new block for the fixed-length
        // tuples. If the  run is already too long, return.
        if (cur_input_index < batch->num_rows()) {
            bool added;
            RETURN_IF_ERROR(try_add_block(&_fixed_len_blocks, &added));
            if (added) {
                cur_fixed_len_block = _fixed_len_blocks.back();
            } else {
                return Status::OK();
            }
        }
    }

    return Status::OK();
}

Status MergeSorter::Run::prepare_read() {
    _fixed_len_blocks_index = 0;
    _fixed_len_block_offset = 0;
    //var_len_blocks_index_ = 0;
    _num_tuples_returned = 0;

    return Status::OK();
}

template <bool convert_offset_to_ptr>
Status MergeSorter::Run::get_next(RowBatch* output_batch, bool* eos) {
    if (_fixed_len_blocks_index == _fixed_len_blocks.size()) {
        *eos = true;
        DCHECK_EQ(_num_tuples_returned, _num_tuples);
        return Status::OK();
    } else {
        *eos = false;
    }

    BufferedBlockMgr::Block* fixed_len_block = _fixed_len_blocks[_fixed_len_blocks_index];

    // get_next fills rows into the output batch until a block boundary is reached.
    while (!output_batch->is_full() &&
            _fixed_len_block_offset < fixed_len_block->valid_data_len()) {
        Tuple* input_tuple = reinterpret_cast<Tuple*>(
                                 fixed_len_block->buffer() + _fixed_len_block_offset);

        int output_row_idx = output_batch->add_row();
        output_batch->get_row(output_row_idx)->set_tuple(0, input_tuple);
        output_batch->commit_last_row();
        _fixed_len_block_offset += _sort_tuple_size;
        ++_num_tuples_returned;
    }

    if (_fixed_len_block_offset >= fixed_len_block->valid_data_len()) {
        ++_fixed_len_blocks_index;
        _fixed_len_block_offset = 0;
    }

    return Status::OK();
}

Status MergeSorter::Run::try_add_block(std::vector<BufferedBlockMgr::Block*>* block_sequence,
                                bool* added) {
    DCHECK(!block_sequence->empty());

    BufferedBlockMgr::Block* last_block = block_sequence->back();
    _sorter->_sorted_data_size->update(last_block->valid_data_len());

    BufferedBlockMgr::Block* new_block;
    RETURN_IF_ERROR(_sorter->_block_mgr->get_new_block(&new_block));
    if (new_block != NULL) {
        *added = true;
        block_sequence->push_back(new_block);
    } else {
        *added = false;
    }
    return Status::OK();
}

void MergeSorter::Run::copy_var_len_data(char* dest, const std::vector<StringValue*>& var_values) {
    BOOST_FOREACH(StringValue* var_val, var_values) {
        memcpy(dest, var_val->ptr, var_val->len);
        var_val->ptr = dest;
        dest += var_val->len;
    }
}


// MergeSorter::TupleSorter methods.
MergeSorter::TupleSorter::TupleSorter(const TupleRowComparator& comp, int64_t block_size,
                                 int tuple_size, RuntimeState* state)
    : _tuple_size(tuple_size),
      _block_capacity(block_size / tuple_size),
      _last_tuple_block_offset(tuple_size * ((block_size / tuple_size) - 1)),
      _less_than_comp(comp),
      _state(state) {
    _temp_tuple_buffer = new uint8_t[tuple_size];
    _temp_tuple_row = reinterpret_cast<TupleRow*>(&_temp_tuple_buffer);
    _swap_buffer = new uint8_t[tuple_size];
}


// Sort the sequence of tuples from [first, last).
// Begin with a sorted sequence of size 1 [first, first+1).
// During each pass of the outermost loop, add the next tuple (at position 'i') to
// the sorted sequence by comparing it to each element of the sorted sequence
// (reverse order) to find its correct place in the sorted sequence, copying tuples
// along the way.
void MergeSorter::TupleSorter::insertion_sort(const TupleIterator& first,
                                        const TupleIterator& last) {
    TupleIterator insert_iter = first;
    insert_iter.next();
    for (; insert_iter._index < last._index; insert_iter.next()) {
        // insert_iter points to the tuple after the currently sorted sequence that must
        // be inserted into the sorted sequence. Copy to _temp_tuple_row since it may be
        // overwritten by the one at position 'insert_iter - 1'
        memcpy(_temp_tuple_buffer, insert_iter._current_tuple, _tuple_size);

        // 'iter' points to the tuple that _temp_tuple_row will be compared to.
        // 'copy_to' is the where iter should be copied to if it is >= _temp_tuple_row.
        // copy_to always to the next row after 'iter'
        TupleIterator iter = insert_iter;
        iter.prev();
        uint8_t* copy_to = insert_iter._current_tuple;
        while (_less_than_comp(_temp_tuple_row,
                               reinterpret_cast<TupleRow*>(&iter._current_tuple))) {
            memcpy(copy_to, iter._current_tuple, _tuple_size);
            copy_to = iter._current_tuple;
            // Break if 'iter' has reached the first row, meaning that _temp_tuple_row
            // will be inserted in position 'first'
            if (iter._index <= first._index) break;
            iter.prev();
        }

        memcpy(copy_to, _temp_tuple_buffer, _tuple_size);
    }
}

MergeSorter::TupleSorter::TupleIterator MergeSorter::TupleSorter::partition(TupleIterator first,
        TupleIterator last, Tuple* pivot) {

    // Copy pivot into temp_tuple since it points to a tuple within [first, last).
    memcpy(_temp_tuple_buffer, pivot, _tuple_size);

    last.prev();
    while (true) {
        // Search for the first and last out-of-place elements, and swap them.
        while (_less_than_comp(reinterpret_cast<TupleRow*>(&first._current_tuple),
                               _temp_tuple_row)) {
            first.next();
        }
        while (_less_than_comp(_temp_tuple_row,
                               reinterpret_cast<TupleRow*>(&last._current_tuple))) {
            last.prev();
        }

        if (first._index >= last._index) break;
        // swap first and last tuples.
        swap(first._current_tuple, last._current_tuple);

        first.next();
        last.prev();
    }

    return first;

}

void MergeSorter::TupleSorter::sort_helper(TupleIterator first, TupleIterator last) {
    if (UNLIKELY(_state->is_cancelled())) return;
    // Use insertion sort for smaller sequences.
    while (last._index - first._index > INSERTION_THRESHOLD) {
        TupleIterator iter(this, first._index + (last._index - first._index)/2);
        // Parititon() splits the tuples in [first, last) into two groups (<= pivot
        // and >= pivot) in-place. 'cut' is the index of the first tuple in the second group.
        TupleIterator cut = partition(first, last,
                                      reinterpret_cast<Tuple*>(iter._current_tuple));
        sort_helper(cut, last);
        last = cut;
        if (UNLIKELY(_state->is_cancelled())) return;
    }
    insertion_sort(first, last);
}

inline void MergeSorter::TupleSorter::swap(uint8_t* left, uint8_t* right) {
    memcpy(_swap_buffer, left, _tuple_size);
    memcpy(left, right, _tuple_size);
    memcpy(right, _swap_buffer, _tuple_size);
}

// MergeSorter methods
MergeSorter::MergeSorter(const TupleRowComparator& compare_less_than,
               const std::vector<ExprContext*>& slot_materialize_expr_ctxs,
               RowDescriptor* output_row_desc,
               RuntimeProfile* profile, RuntimeState* state)
    : _state(state),
      _compare_less_than(compare_less_than),
      _block_mgr(state->block_mgr()),
      _output_row_desc(output_row_desc),
      _sort_tuple_slot_expr_ctxs(slot_materialize_expr_ctxs),
      _profile(profile) {
    TupleDescriptor* sort_tuple_desc = output_row_desc->tuple_descriptors()[0];
    _has_var_len_slots = sort_tuple_desc->string_slots().size() > 0;
    _in_mem_tuple_sorter.reset(new TupleSorter(compare_less_than,
                               _block_mgr->max_block_size(), sort_tuple_desc->byte_size(), state));

    _initial_runs_counter = ADD_COUNTER(_profile, "InitialRunsCreated", TUnit::UNIT);
    _num_merges_counter = ADD_COUNTER(_profile, "TotalMergesPerformed", TUnit::UNIT);
    _in_mem_sort_timer = ADD_TIMER(_profile, "InMemorySortTime");
    _sorted_data_size = ADD_COUNTER(_profile, "SortDataSize", TUnit::BYTES);

    _unsorted_run = _obj_pool.add(new Run(this, sort_tuple_desc, true));
    _unsorted_run->init();
}

MergeSorter::~MergeSorter() {
}

Status MergeSorter::add_batch(RowBatch* batch) {
    int num_processed = 0;
    int cur_batch_index = 0;

    while (cur_batch_index < batch->num_rows()) {
        if (_has_var_len_slots) {
            _unsorted_run->add_batch<true>(batch, cur_batch_index, &num_processed);
        } else {
            _unsorted_run->add_batch<false>(batch, cur_batch_index, &num_processed);
        }

        cur_batch_index += num_processed;
        if (cur_batch_index < batch->num_rows()) {
            return Status::InternalError("run is full");
        }
    }
    return Status::OK();
}

Status MergeSorter::input_done() {
// Sort the tuples accumulated so far in the current run.
    RETURN_IF_ERROR(sort_run());

    DCHECK(_sorted_runs.size() == 1);

// The entire input fit in one run. Read sorted rows in get_next() directly
// from the sorted run.
    _sorted_runs.back()->prepare_read();

    return Status::OK();
}

Status MergeSorter::get_next(RowBatch* output_batch, bool* eos) {
    DCHECK(_sorted_runs.size() == 1);

    // In this case, only TupleRows are copied into output_batch. Sorted tuples are left
    // in the pinned blocks in the single sorted run.
    RETURN_IF_ERROR(_sorted_runs.back()->get_next<false>(output_batch, eos));

    return Status::OK();
}

Status MergeSorter::sort_run() {
    BufferedBlockMgr::Block* last_block = _unsorted_run->_fixed_len_blocks.back();
    if (last_block->valid_data_len() > 0) {
        _sorted_data_size->update(last_block->valid_data_len());
    } else {
        // need to delete block?
        _unsorted_run->_fixed_len_blocks.pop_back();
    }
    if (_has_var_len_slots) {
        last_block = _unsorted_run->_var_len_blocks.back();
        if (last_block->valid_data_len() > 0) {
            _sorted_data_size->update(last_block->valid_data_len());
        } else {
            // need to delete block?
            _unsorted_run->_var_len_blocks.pop_back();
        }
    }
    {
        SCOPED_TIMER(_in_mem_sort_timer);
        _in_mem_tuple_sorter->sort(_unsorted_run);
        RETURN_IF_CANCELLED(_state);
    }
    _sorted_runs.push_back(_unsorted_run);
    _unsorted_run = NULL;
    return Status::OK();
}
} // namespace doris
