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

#include "runtime/spill_sorter.h"

#include <sstream>
#include <string>

#include "runtime/buffered_block_mgr2.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/sorted_run_merger.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"

using std::deque;
using std::string;
using std::vector;

using std::bind;
using std::function;
using std::mem_fn;
using std::unique_ptr;

namespace doris {

// Number of pinned blocks required for a merge.
const int BLOCKS_REQUIRED_FOR_MERGE = 3;

// Error message when pinning fixed or variable length blocks failed.
// TODO: Add the node id that initiated the sort
const string PIN_FAILED_ERROR_MSG_1 = "Failed to pin block for ";
const string PIN_FAILED_ERROR_MSG_2 =
        "-length data needed "
        "for sorting. Reducing query concurrency or increasing the memory limit may help "
        "this query to complete successfully.";

const string MEM_ALLOC_FAILED_ERROR_MSG_1 = "Failed to allocate block for $0-length ";
const string MEM_ALLOC_FAILED_ERROR_MSG_2 =
        "-length "
        "data needed for sorting. Reducing query concurrency or increasing the "
        "memory limit may help this query to complete successfully.";

static std::string get_pin_failed_error_msg(const std::string& block_type) {
    std::stringstream error_msg;
    error_msg << PIN_FAILED_ERROR_MSG_1 << block_type << PIN_FAILED_ERROR_MSG_2;
    return error_msg.str();
}

static std::string get_mem_alloc_failed_error_msg(const std::string& block_type) {
    std::stringstream error_msg;
    error_msg << MEM_ALLOC_FAILED_ERROR_MSG_1 << block_type << MEM_ALLOC_FAILED_ERROR_MSG_2;
    return error_msg.str();
}

// A run is a sequence of blocks containing tuples that are or will eventually be in
// sorted order.
// A run may maintain two sequences of blocks - one containing the tuples themselves,
// (i.e. fixed-len slots and ptrs to var-len data), and the other for the var-length
// column data pointed to by those tuples.
// Tuples in a run may be sorted in place (in-memory) and merged using a merger.
class SpillSorter::Run {
public:
    // materialize_slots is true for runs constructed from input rows. The input rows are
    // materialized into single sort tuples using the expressions in
    // _sort_tuple_slot_expr_ctxs. For intermediate merges, the tuples are already
    // materialized so materialize_slots is false.
    Run(SpillSorter* parent, TupleDescriptor* sort_tuple_desc, bool materialize_slots);

    ~Run() { delete_all_blocks(); }

    // Initialize the run for input rows by allocating the minimum number of required
    // blocks - one block for fixed-len data added to _fixed_len_blocks, one for the
    // initially unsorted var-len data added to _var_len_blocks, and one to copy sorted
    // var-len data into (_var_len_copy_block).
    Status init();

    // Add a batch of input rows to the current run. Returns the number
    // of rows actually added in num_processed. If the run is full (no more blocks can
    // be allocated), num_processed may be less than the number of rows in the batch.
    // If _materialize_slots is true, materializes the input rows using the expressions
    // in _sorter->_sort_tuple_slot_expr_ctxs, else just copies the input rows.
    template <bool has_var_len_data>
    Status add_batch(RowBatch* batch, int start_index, int* num_processed);

    // Attaches all fixed-len and var-len blocks to the given row batch.
    void transfer_resources(RowBatch* row_batch);

    // Unpins all the blocks in a sorted run. Var-length column data is copied into new
    // blocks in sorted order. Pointers in the original tuples are converted to offsets
    // from the beginning of the sequence of var-len data blocks.
    Status unpin_all_blocks();

    // Deletes all blocks.
    void delete_all_blocks();

    // Interface for merger - get the next batch of rows from this run. The callee (Run)
    // still owns the returned batch. Calls get_next(RowBatch*, bool*).
    Status get_next_batch(RowBatch** sorted_batch);

private:
    friend class SpillSorter;
    friend class TupleSorter;

    // Fill output_batch with rows from this run. If convert_offset_to_ptr is true, offsets
    // in var-length slots are converted back to pointers. Only row pointers are copied
    // into output_batch.
    // If this run was unpinned, one block (2 if there are var-len slots) is pinned while
    // rows are filled into output_batch. The block is unpinned before the next block is
    // pinned. Atmost 1 (2) block(s) will be pinned at any time.
    // If the run was pinned, the blocks are not unpinned (SpillSorter holds on to the memory).
    // In either case, all rows in output_batch will have their fixed and var-len data from
    // the same block.
    // TODO: If we leave the last run to be merged in memory, the fixed-len blocks can be
    // unpinned as they are consumed.
    template <bool convert_offset_to_ptr>
    Status get_next(RowBatch* output_batch, bool* eos);

    // Check if a run can be extended by allocating additional blocks from the block
    // manager. Always true when building a sorted run in an intermediate merge, because
    // the current block(s) can be unpinned before getting the next free block (so a block
    // is always available)
    bool can_extend_run() const;

    // Collect the non-null var-len (e.g. STRING) slots from 'src' in var_slots and return
    // the total length of all var_len slots in total_var_len.
    void collect_non_null_varslots(Tuple* src, vector<StringValue*>* var_len_values,
                                   int* total_var_len);

    // Check if the current run can be extended by a block. Add the newly allocated block
    // to block_sequence, or set added to false if the run could not be extended.
    // If the run is sorted (produced by an intermediate merge), unpin the last block in
    // block_sequence before allocating and adding a new block - the run can always be
    // extended in this case. If the run is unsorted, check _max_blocks_in_unsorted_run
    // to see if a block can be added to the run. Also updates the sort bytes counter.
    Status try_add_block(vector<BufferedBlockMgr2::Block*>* block_sequence, bool* added);

    // Prepare to read a sorted run. Pins the first block(s) in the run if the run was
    // previously unpinned.
    Status prepare_read();

    // Copy the StringValue data in var_values to dest in order and update the StringValue
    // ptrs to point to the copied data.
    void copy_var_len_data(char* dest, const vector<StringValue*>& var_values);

    // Copy the StringValue in var_values to dest in order. Update the StringValue ptrs to
    // contain an offset to the copied data. Parameter 'offset' is the offset for the first
    // StringValue.
    void copy_var_len_data_convert_offset(char* dest, int64_t offset,
                                          const vector<StringValue*>& var_values);

    // Returns true if we have var-len slots and there are var-len blocks.
    inline bool has_var_len_blocks() const {
        return _has_var_len_slots && !_var_len_blocks.empty();
    }

    // Parent sorter object.
    const SpillSorter* _sorter;

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

    // True if all blocks in the run are pinned.
    bool _is_pinned;

    // Sequence of blocks in this run containing the fixed-length portion of the sort
    // tuples comprising this run. The data pointed to by the var-len slots are in
    // _var_len_blocks.
    // If _is_sorted is true, the tuples in _fixed_len_blocks will be in sorted order.
    // _fixed_len_blocks[i] is nullptr iff it has been deleted.
    vector<BufferedBlockMgr2::Block*> _fixed_len_blocks;

    // Sequence of blocks in this run containing the var-length data corresponding to the
    // var-length columns from _fixed_len_blocks. These are reconstructed to be in sorted
    // order in unpin_all_blocks().
    // _var_len_blocks[i] is nullptr iff it has been deleted.
    vector<BufferedBlockMgr2::Block*> _var_len_blocks;

    // If there are var-len slots, an extra pinned block is used to copy out var-len data
    // into a new sequence of blocks in sorted order. _var_len_copy_block stores this
    // extra allocated block.
    BufferedBlockMgr2::Block* _var_len_copy_block;

    // Number of tuples so far in this run.
    int64_t _num_tuples;

    // Number of tuples returned via get_next(), maintained for debug purposes.
    int64_t _num_tuples_returned;

    // _buffered_batch is used to return TupleRows to the merger when this run is being
    // merged. _buffered_batch is returned in calls to get_next_batch().
    unique_ptr<RowBatch> _buffered_batch;

    // Members used when a run is read in get_next().
    // The index into the _fixed_len_blocks and _var_len_blocks vectors of the current blocks being
    // processed in get_next().
    int _fixed_len_blocks_index;
    int _var_len_blocks_index;

    // If true, pin the next fixed and var-len blocks and delete the previous ones
    // in the next call to get_next(). Set during the previous call to get_next().
    // Not used if a run is already pinned.
    bool _pin_next_fixed_len_block;
    bool _pin_next_var_len_block;

    // Offset into the current fixed length data block being processed.
    int _fixed_len_block_offset;
}; // class SpillSorter::Run

// Sorts a sequence of tuples from a run in place using a provided tuple comparator.
// Quick sort is used for sequences of tuples larger that 16 elements, and insertion sort
// is used for smaller sequences. The TupleSorter is initialized with a RuntimeState
// instance to check for cancellation during an in-memory sort.
class SpillSorter::TupleSorter {
public:
    TupleSorter(const TupleRowComparator& less_than_comp, int64_t block_size, int tuple_size,
                RuntimeState* state);

    ~TupleSorter();

    // Performs a quicksort for tuples in 'run' followed by an insertion sort to
    // finish smaller blocks.
    // Returns early if _state->is_cancelled() is true. No status
    // is returned - the caller must check for cancellation.
    void sort(Run* run);

private:
    static const int INSERTION_THRESHOLD = 16;

    // Helper class used to iterate over tuples in a run during quick sort and insertion sort.
    class TupleIterator {
    public:
        TupleIterator(TupleSorter* parent, int64_t index)
                : _parent(parent), _index(index), _current_tuple(nullptr) {
            DCHECK_GE(index, 0);
            DCHECK_LE(index, _parent->_run->_num_tuples);
            // If the run is empty, only _index and _current_tuple are initialized.
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
        uint8_t* _current_tuple;

        // Start of the buffer containing current tuple.
        uint8_t* _buffer_start;

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
    // _temp_tuple points to _temp_tuple_buffer. Owned by this TupleSorter instance.
    TupleRow* _temp_tuple_row;
    uint8_t* _temp_tuple_buffer;
    uint8_t* _swap_buffer;

    // Perform an insertion sort for rows in the range [first, last) in a run.
    void insertion_sort(const TupleIterator& first, const TupleIterator& last);

    // Partitions the sequence of tuples in the range [first, last) in a run into two
    // groups around the mid._current_tuple - i.e. tuples in first group are <= the mid._current_tuple
    // and tuples in the second group are >= mid._current_tuple. Tuples are swapped in place to create the
    // groups and the index to the first element in the second group is returned.
    // Checks _state->is_cancelled() and returns early with an invalid result if true.
    TupleIterator partition(TupleIterator first, TupleIterator last, TupleIterator& mid);

    // Select the median of three iterator tuples. taking the median tends to help us select better
    // pivots that more evenly split the input range. This method makes selection of
    // bad pivots very infrequent.
    void find_the_median(TupleIterator& first, TupleIterator& last, TupleIterator& mid);

    // Performs a quicksort of rows in the range [first, last) followed by insertion sort
    // for smaller groups of elements.
    // Checks _state->is_cancelled() and returns early if true.
    void sort_helper(TupleIterator first, TupleIterator last);

    // Swaps tuples pointed to by left and right using the swap buffer.
    void swap(uint8_t* left, uint8_t* right);
}; // class TupleSorter

// SpillSorter::Run methods
SpillSorter::Run::Run(SpillSorter* parent, TupleDescriptor* sort_tuple_desc, bool materialize_slots)
        : _sorter(parent),
          _sort_tuple_desc(sort_tuple_desc),
          _sort_tuple_size(sort_tuple_desc->byte_size()),
          _block_size(parent->_block_mgr->max_block_size()),
          _has_var_len_slots(sort_tuple_desc->has_varlen_slots()),
          _materialize_slots(materialize_slots),
          _is_sorted(!materialize_slots),
          _is_pinned(true),
          _var_len_copy_block(nullptr),
          _num_tuples(0) {}

Status SpillSorter::Run::init() {
    BufferedBlockMgr2::Block* block = nullptr;
    RETURN_IF_ERROR(
            _sorter->_block_mgr->get_new_block(_sorter->_block_mgr_client, nullptr, &block));
    if (block == nullptr) {
        return Status::MemoryLimitExceeded(get_mem_alloc_failed_error_msg("fixed"));
    }
    _fixed_len_blocks.push_back(block);

    if (_has_var_len_slots) {
        RETURN_IF_ERROR(
                _sorter->_block_mgr->get_new_block(_sorter->_block_mgr_client, nullptr, &block));
        if (block == nullptr) {
            return Status::MemoryLimitExceeded(get_mem_alloc_failed_error_msg("variable"));
        }
        _var_len_blocks.push_back(block);

        if (!_is_sorted) {
            RETURN_IF_ERROR(_sorter->_block_mgr->get_new_block(_sorter->_block_mgr_client, nullptr,
                                                               &_var_len_copy_block));
            if (_var_len_copy_block == nullptr) {
                return Status::MemoryLimitExceeded(get_mem_alloc_failed_error_msg("variable"));
            }
        }
    }
    if (!_is_sorted) {
        _sorter->_initial_runs_counter->update(1);
    }
    return Status::OK();
}

template <bool has_var_len_data>
Status SpillSorter::Run::add_batch(RowBatch* batch, int start_index, int* num_processed) {
    DCHECK(!_fixed_len_blocks.empty());
    *num_processed = 0;
    BufferedBlockMgr2::Block* cur_fixed_len_block = _fixed_len_blocks.back();

    DCHECK_EQ(_materialize_slots, !_is_sorted);
    if (!_materialize_slots) {
        // If materialize slots is false the run is being constructed for an
        // intermediate merge and the sort tuples have already been materialized.
        // The input row should have the same schema as the sort tuples.
        DCHECK_EQ(batch->row_desc().tuple_descriptors().size(), 1);
        DCHECK_EQ(batch->row_desc().tuple_descriptors()[0], _sort_tuple_desc);
    }

    // Input rows are copied/materialized into tuples allocated in _fixed_len_blocks.
    // The variable length column data are copied into blocks stored in _var_len_blocks.
    // Input row processing is split into two loops.
    // The inner loop processes as many input rows as will fit in cur_fixed_len_block.
    // The outer loop allocates a new block for fixed-len data if the input batch is
    // not exhausted.

    // cur_input_index is the index into the input 'batch' of the current input row being
    // processed.
    int cur_input_index = start_index;
    vector<StringValue*> string_values;
    string_values.reserve(_sort_tuple_desc->string_slots().size());
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
                new_tuple->materialize_exprs<has_var_len_data>(
                        input_row, *_sort_tuple_desc, _sorter->_sort_tuple_slot_expr_ctxs, nullptr,
                        &string_values, &total_var_len);
                if (total_var_len > _sorter->_block_mgr->max_block_size()) {
                    std::stringstream error_msg;
                    error_msg << "Variable length data in a single tuple larger than block size "
                              << total_var_len << " > " << _sorter->_block_mgr->max_block_size();
                    return Status::InternalError(error_msg.str());
                }
            } else {
                memcpy(new_tuple, input_row->get_tuple(0), _sort_tuple_size);
                if (has_var_len_data) {
                    collect_non_null_varslots(new_tuple, &string_values, &total_var_len);
                }
            }

            if (has_var_len_data) {
                DCHECK_GT(_var_len_blocks.size(), 0);
                BufferedBlockMgr2::Block* cur_var_len_block = _var_len_blocks.back();
                if (cur_var_len_block->bytes_remaining() < total_var_len) {
                    bool added = false;
                    RETURN_IF_ERROR(try_add_block(&_var_len_blocks, &added));
                    if (added) {
                        cur_var_len_block = _var_len_blocks.back();
                    } else {
                        // There was not enough space in the last var-len block for this tuple, and
                        // the run could not be extended. Return the fixed-len allocation and exit.
                        cur_fixed_len_block->return_allocation(_sort_tuple_size);
                        return Status::OK();
                    }
                }

                // Sorting of tuples containing array values is not implemented. The planner
                // combined with projection should guarantee that none are in each tuple.
                // for(const SlotDescriptor* collection_slot :
                //         _sort_tuple_desc->collection_slots()) {
                //     DCHECK(new_tuple->is_null(collection_slot->null_indicator_offset()));
                // }

                char* var_data_ptr = cur_var_len_block->allocate<char>(total_var_len);
                if (_materialize_slots) {
                    copy_var_len_data(var_data_ptr, string_values);
                } else {
                    int64_t offset = (_var_len_blocks.size() - 1) * _block_size;
                    offset += var_data_ptr - reinterpret_cast<char*>(cur_var_len_block->buffer());
                    copy_var_len_data_convert_offset(var_data_ptr, offset, string_values);
                }
            }
            ++_num_tuples;
            ++*num_processed;
            ++cur_input_index;
        }
        // There we already copy the tuple data to Block, So we need to release the mem
        // in expr mempool to prevent memory leak
        ExprContext::free_local_allocations(_sorter->_sort_tuple_slot_expr_ctxs);

        // If there are still rows left to process, get a new block for the fixed-length
        // tuples. If the run is already too long, return.
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

void SpillSorter::Run::transfer_resources(RowBatch* row_batch) {
    DCHECK(row_batch != nullptr);
    for (BufferedBlockMgr2::Block* block : _fixed_len_blocks) {
        if (block != nullptr) {
            row_batch->add_block(block);
        }
    }
    _fixed_len_blocks.clear();
    for (BufferedBlockMgr2::Block* block : _var_len_blocks) {
        if (block != nullptr) {
            row_batch->add_block(block);
        }
    }
    _var_len_blocks.clear();
    if (_var_len_copy_block != nullptr) {
        row_batch->add_block(_var_len_copy_block);
        _var_len_copy_block = nullptr;
    }
}

void SpillSorter::Run::delete_all_blocks() {
    for (BufferedBlockMgr2::Block* block : _fixed_len_blocks) {
        if (block != nullptr) {
            block->del();
        }
    }
    _fixed_len_blocks.clear();
    for (BufferedBlockMgr2::Block* block : _var_len_blocks) {
        if (block != nullptr) {
            block->del();
        }
    }
    _var_len_blocks.clear();
    if (_var_len_copy_block != nullptr) {
        _var_len_copy_block->del();
        _var_len_copy_block = nullptr;
    }
}

Status SpillSorter::Run::unpin_all_blocks() {
    vector<BufferedBlockMgr2::Block*> sorted_var_len_blocks;
    sorted_var_len_blocks.reserve(_var_len_blocks.size());
    vector<StringValue*> string_values;
    int64_t var_data_offset = 0;
    int total_var_len = 0;
    string_values.reserve(_sort_tuple_desc->string_slots().size());
    BufferedBlockMgr2::Block* cur_sorted_var_len_block = nullptr;
    if (has_var_len_blocks()) {
        DCHECK(_var_len_copy_block != nullptr);
        sorted_var_len_blocks.push_back(_var_len_copy_block);
        cur_sorted_var_len_block = _var_len_copy_block;
    } else {
        DCHECK(_var_len_copy_block == nullptr);
    }

    for (int i = 0; i < _fixed_len_blocks.size(); ++i) {
        BufferedBlockMgr2::Block* cur_fixed_block = _fixed_len_blocks[i];
        if (has_var_len_blocks()) {
            for (int block_offset = 0; block_offset < cur_fixed_block->valid_data_len();
                 block_offset += _sort_tuple_size) {
                Tuple* cur_tuple =
                        reinterpret_cast<Tuple*>(cur_fixed_block->buffer() + block_offset);
                collect_non_null_varslots(cur_tuple, &string_values, &total_var_len);
                DCHECK(cur_sorted_var_len_block != nullptr);
                if (cur_sorted_var_len_block->bytes_remaining() < total_var_len) {
                    bool added = false;
                    RETURN_IF_ERROR(try_add_block(&sorted_var_len_blocks, &added));
                    DCHECK(added);
                    cur_sorted_var_len_block = sorted_var_len_blocks.back();
                }
                char* var_data_ptr = cur_sorted_var_len_block->allocate<char>(total_var_len);
                var_data_offset = _block_size * (sorted_var_len_blocks.size() - 1) +
                                  (var_data_ptr -
                                   reinterpret_cast<char*>(cur_sorted_var_len_block->buffer()));
                copy_var_len_data_convert_offset(var_data_ptr, var_data_offset, string_values);
            }
        }
        RETURN_IF_ERROR(cur_fixed_block->unpin());
    }

    if (_has_var_len_slots && _var_len_blocks.size() > 0) {
        DCHECK_GT(sorted_var_len_blocks.back()->valid_data_len(), 0);
        RETURN_IF_ERROR(sorted_var_len_blocks.back()->unpin());
    }

    // Clear _var_len_blocks and replace with it with the contents of sorted_var_len_blocks
    for (BufferedBlockMgr2::Block* var_block : _var_len_blocks) {
        var_block->del();
    }
    _var_len_blocks.clear();
    sorted_var_len_blocks.swap(_var_len_blocks);
    // Set _var_len_copy_block to nullptr since it's now in _var_len_blocks and is no longer
    // needed.
    _var_len_copy_block = nullptr;
    _is_pinned = false;
    return Status::OK();
}

Status SpillSorter::Run::prepare_read() {
    _fixed_len_blocks_index = 0;
    _fixed_len_block_offset = 0;
    _var_len_blocks_index = 0;
    _pin_next_fixed_len_block = _pin_next_var_len_block = false;
    _num_tuples_returned = 0;

    _buffered_batch.reset(new RowBatch(*_sorter->_output_row_desc, _sorter->_state->batch_size()));

    // If the run is pinned, merge is not invoked, so _buffered_batch is not needed
    // and the individual blocks do not need to be pinned.
    if (_is_pinned) {
        return Status::OK();
    }

    // Attempt to pin the first fixed and var-length blocks. In either case, pinning may
    // fail if the number of reserved blocks is oversubscribed, see IMPALA-1590.
    if (_fixed_len_blocks.size() > 0) {
        bool pinned = false;
        RETURN_IF_ERROR(_fixed_len_blocks[0]->pin(&pinned));
        // Temporary work-around for IMPALA-1868. Fail the query with OOM rather than
        // DCHECK in case block pin fails.
        if (!pinned) {
            return Status::MemoryLimitExceeded(get_pin_failed_error_msg("fixed"));
        }
    }

    if (_has_var_len_slots && _var_len_blocks.size() > 0) {
        bool pinned = false;
        RETURN_IF_ERROR(_var_len_blocks[0]->pin(&pinned));
        // Temporary work-around for IMPALA-1590. Fail the query with OOM rather than
        // DCHECK in case block pin fails.
        if (!pinned) {
            return Status::MemoryLimitExceeded(get_pin_failed_error_msg("variable"));
        }
    }
    return Status::OK();
}

Status SpillSorter::Run::get_next_batch(RowBatch** output_batch) {
    if (_buffered_batch.get() != nullptr) {
        _buffered_batch->reset();
        // Fill more rows into _buffered_batch.
        bool eos = false;
        if (_has_var_len_slots && !_is_pinned) {
            RETURN_IF_ERROR(get_next<true>(_buffered_batch.get(), &eos));
            if (_buffered_batch->num_rows() == 0 && !eos) {
                // No rows were filled because get_next() had to read the next var-len block
                // Call get_next() again.
                RETURN_IF_ERROR(get_next<true>(_buffered_batch.get(), &eos));
            }
        } else {
            RETURN_IF_ERROR(get_next<false>(_buffered_batch.get(), &eos));
        }
        DCHECK(eos || _buffered_batch->num_rows() > 0);
        if (eos) {
            // No rows are filled in get_next() on eos, so this is safe.
            DCHECK_EQ(_buffered_batch->num_rows(), 0);
            _buffered_batch.reset();
            // The merge is complete. Delete the last blocks in the run.
            _fixed_len_blocks.back()->del();
            _fixed_len_blocks[_fixed_len_blocks.size() - 1] = nullptr;
            if (has_var_len_blocks()) {
                _var_len_blocks.back()->del();
                _var_len_blocks[_var_len_blocks.size() - 1] = nullptr;
            }
        }
    }

    // *output_batch == nullptr indicates eos.
    *output_batch = _buffered_batch.get();
    return Status::OK();
}

template <bool convert_offset_to_ptr>
Status SpillSorter::Run::get_next(RowBatch* output_batch, bool* eos) {
    if (_fixed_len_blocks_index == _fixed_len_blocks.size()) {
        *eos = true;
        DCHECK_EQ(_num_tuples_returned, _num_tuples);
        return Status::OK();
    } else {
        *eos = false;
    }

    BufferedBlockMgr2::Block* fixed_len_block = _fixed_len_blocks[_fixed_len_blocks_index];

    if (!_is_pinned) {
        // Pin the next block and delete the previous if set in the previous call to
        // get_next().
        if (_pin_next_fixed_len_block) {
            _fixed_len_blocks[_fixed_len_blocks_index - 1]->del();
            _fixed_len_blocks[_fixed_len_blocks_index - 1] = nullptr;
            bool pinned;
            RETURN_IF_ERROR(fixed_len_block->pin(&pinned));
            // Temporary work-around for IMPALA-2344. Fail the query with OOM rather than
            // DCHECK in case block pin fails.
            if (!pinned) {
                return Status::MemoryLimitExceeded(get_pin_failed_error_msg("fixed"));
            }
            _pin_next_fixed_len_block = false;
        }
        if (_pin_next_var_len_block) {
            _var_len_blocks[_var_len_blocks_index - 1]->del();
            _var_len_blocks[_var_len_blocks_index - 1] = nullptr;
            bool pinned;
            RETURN_IF_ERROR(_var_len_blocks[_var_len_blocks_index]->pin(&pinned));
            // Temporary work-around for IMPALA-2344. Fail the query with OOM rather than
            // DCHECK in case block pin fails.
            if (!pinned) {
                return Status::MemoryLimitExceeded(get_pin_failed_error_msg("variable"));
            }
            _pin_next_var_len_block = false;
        }
    }

    // get_next fills rows into the output batch until a block boundary is reached.
    DCHECK(fixed_len_block != nullptr);
    while (!output_batch->at_capacity() &&
           _fixed_len_block_offset < fixed_len_block->valid_data_len()) {
        DCHECK(fixed_len_block != nullptr);
        Tuple* input_tuple =
                reinterpret_cast<Tuple*>(fixed_len_block->buffer() + _fixed_len_block_offset);

        if (convert_offset_to_ptr) {
            // Convert the offsets in the var-len slots in input_tuple back to pointers.
            const vector<SlotDescriptor*>& string_slots = _sort_tuple_desc->string_slots();
            for (int i = 0; i < string_slots.size(); ++i) {
                SlotDescriptor* slot_desc = string_slots[i];
                if (input_tuple->is_null(slot_desc->null_indicator_offset())) {
                    continue;
                }

                DCHECK(slot_desc->type().is_string_type());
                StringValue* value = reinterpret_cast<StringValue*>(
                        input_tuple->get_slot(slot_desc->tuple_offset()));
                int64_t data_offset = reinterpret_cast<int64_t>(value->ptr);

                // data_offset is an offset in bytes from the beginning of the first block
                // in _var_len_blocks. Convert it into an index into _var_len_blocks and an
                // offset within that block.
                int block_index = data_offset / _block_size;
                int block_offset = data_offset % _block_size;

                if (block_index > _var_len_blocks_index) {
                    // We've reached the block boundary for the current var-len block.
                    // This tuple will be returned in the next call to get_next().
                    DCHECK_EQ(block_index, _var_len_blocks_index + 1);
                    DCHECK_EQ(block_offset, 0);
                    DCHECK_EQ(i, 0);
                    _var_len_blocks_index = block_index;
                    _pin_next_var_len_block = true;
                    break;
                } else {
                    DCHECK_EQ(block_index, _var_len_blocks_index) << "block_index: " << block_index;
                    // Calculate the address implied by the offset and assign it.
                    value->ptr = reinterpret_cast<char*>(
                            _var_len_blocks[_var_len_blocks_index]->buffer() + block_offset);
                } // if (block_index > _var_len_blocks_index)
            }     // for (int i = 0; i < string_slots.size(); ++i)

            // The var-len data is in the next block, so end this call to get_next().
            if (_pin_next_var_len_block) {
                break;
            }
        } // if (convert_offset_to_ptr)

        int output_row_idx = output_batch->add_row();
        output_batch->get_row(output_row_idx)->set_tuple(0, input_tuple);
        output_batch->commit_last_row();
        _fixed_len_block_offset += _sort_tuple_size;
        ++_num_tuples_returned;
    }

    // Reached the block boundary, need to move to the next block.
    if (_fixed_len_block_offset >= fixed_len_block->valid_data_len()) {
        _pin_next_fixed_len_block = true;
        ++_fixed_len_blocks_index;
        _fixed_len_block_offset = 0;
    }
    return Status::OK();
}

void SpillSorter::Run::collect_non_null_varslots(Tuple* src, vector<StringValue*>* string_values,
                                                 int* total_var_len) {
    string_values->clear();
    *total_var_len = 0;
    for (const SlotDescriptor* string_slot : _sort_tuple_desc->string_slots()) {
        if (!src->is_null(string_slot->null_indicator_offset())) {
            StringValue* string_val =
                    reinterpret_cast<StringValue*>(src->get_slot(string_slot->tuple_offset()));
            string_values->push_back(string_val);
            *total_var_len += string_val->len;
        }
    }
}

Status SpillSorter::Run::try_add_block(vector<BufferedBlockMgr2::Block*>* block_sequence,
                                       bool* added) {
    DCHECK(!block_sequence->empty());
    BufferedBlockMgr2::Block* last_block = block_sequence->back();
    if (!_is_sorted) {
        _sorter->_sorted_data_size->update(last_block->valid_data_len());
        last_block = nullptr;
    } else {
        // If the run is sorted, we will unpin the last block and extend the run.
    }

    BufferedBlockMgr2::Block* new_block;
    RETURN_IF_ERROR(
            _sorter->_block_mgr->get_new_block(_sorter->_block_mgr_client, last_block, &new_block));
    if (new_block != nullptr) {
        *added = true;
        block_sequence->push_back(new_block);
    } else {
        *added = false;
    }
    return Status::OK();
}

void SpillSorter::Run::copy_var_len_data(char* dest, const vector<StringValue*>& string_values) {
    for (StringValue* string_val : string_values) {
        memcpy(dest, string_val->ptr, string_val->len);
        string_val->ptr = dest;
        dest += string_val->len;
    }
}

void SpillSorter::Run::copy_var_len_data_convert_offset(char* dest, int64_t offset,
                                                        const vector<StringValue*>& string_values) {
    for (StringValue* string_val : string_values) {
        memcpy(dest, string_val->ptr, string_val->len);
        string_val->ptr = reinterpret_cast<char*>(offset);
        dest += string_val->len;
        offset += string_val->len;
    }
}

// SpillSorter::TupleSorter methods.
SpillSorter::TupleSorter::TupleSorter(const TupleRowComparator& comp, int64_t block_size,
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

SpillSorter::TupleSorter::~TupleSorter() {
    delete[] _temp_tuple_buffer;
    delete[] _swap_buffer;
}

void SpillSorter::TupleSorter::sort(Run* run) {
    _run = run;
    sort_helper(TupleIterator(this, 0), TupleIterator(this, _run->_num_tuples));
    run->_is_sorted = true;
}

// Sort the sequence of tuples from [first, last).
// Begin with a sorted sequence of size 1 [first, first+1).
// During each pass of the outermost loop, add the next tuple (at position 'i') to
// the sorted sequence by comparing it to each element of the sorted sequence
// (reverse order) to find its correct place in the sorted sequence, copying tuples
// along the way.
void SpillSorter::TupleSorter::insertion_sort(const TupleIterator& first,
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
            if (iter._index <= first._index) {
                break;
            }
            iter.prev();
        }

        memcpy(copy_to, _temp_tuple_buffer, _tuple_size);
    }
}

void SpillSorter::TupleSorter::find_the_median(TupleSorter::TupleIterator& first,
                                               TupleSorter::TupleIterator& last,
                                               TupleSorter::TupleIterator& mid) {
    last.prev();
    auto f_com_result = _less_than_comp.compare(reinterpret_cast<TupleRow*>(&first._current_tuple),
                                                reinterpret_cast<TupleRow*>(&mid._current_tuple));
    auto l_com_result = _less_than_comp.compare(reinterpret_cast<TupleRow*>(&last._current_tuple),
                                                reinterpret_cast<TupleRow*>(&mid._current_tuple));
    if (f_com_result == -1 && l_com_result == -1) {
        if (_less_than_comp(reinterpret_cast<TupleRow*>(&first._current_tuple),
                            reinterpret_cast<TupleRow*>(&last._current_tuple))) {
            swap(mid._current_tuple, last._current_tuple);
        } else {
            swap(mid._current_tuple, first._current_tuple);
        }
    }
    if (f_com_result == 1 && l_com_result == 1) {
        if (_less_than_comp(reinterpret_cast<TupleRow*>(&first._current_tuple),
                            reinterpret_cast<TupleRow*>(&last._current_tuple))) {
            swap(mid._current_tuple, first._current_tuple);
        } else {
            swap(mid._current_tuple, last._current_tuple);
        }
    }
}

SpillSorter::TupleSorter::TupleIterator SpillSorter::TupleSorter::partition(TupleIterator first,
                                                                            TupleIterator last,
                                                                            TupleIterator& mid) {
    find_the_median(first, last, mid);

    // Copy &mid._current_tuple into temp_tuple since it points to a tuple within [first, last).
    memcpy(_temp_tuple_buffer, mid._current_tuple, _tuple_size);
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

        if (first._index >= last._index) {
            break;
        }
        // Swap first and last tuples.
        swap(first._current_tuple, last._current_tuple);

        first.next();
        last.prev();
    }

    return first;
}

void SpillSorter::TupleSorter::sort_helper(TupleIterator first, TupleIterator last) {
    if (UNLIKELY(_state->is_cancelled())) {
        return;
    }
    // Use insertion sort for smaller sequences.
    while (last._index - first._index > INSERTION_THRESHOLD) {
        TupleIterator mid(this, first._index + (last._index - first._index) / 2);

        DCHECK(mid._current_tuple != nullptr);
        // partition() splits the tuples in [first, last) into two groups (<=  mid iter
        // and >= mid iter) in-place. 'cut' is the index of the first tuple in the second group.
        TupleIterator cut = partition(first, last, mid);

        // Recurse on the smaller partition. This limits stack size to log(n) stack frames.
        if (last._index - cut._index < cut._index - first._index) {
            sort_helper(cut, last);
            last = cut;
        } else {
            sort_helper(first, cut);
            first = cut;
        }

        if (UNLIKELY(_state->is_cancelled())) {
            return;
        }
    }

    insertion_sort(first, last);
}

inline void SpillSorter::TupleSorter::swap(uint8_t* left, uint8_t* right) {
    memcpy(_swap_buffer, left, _tuple_size);
    memcpy(left, right, _tuple_size);
    memcpy(right, _swap_buffer, _tuple_size);
}

// SpillSorter methods
SpillSorter::SpillSorter(const TupleRowComparator& compare_less_than,
                         const vector<ExprContext*>& slot_materialize_expr_ctxs,
                         RowDescriptor* output_row_desc,
                         const std::shared_ptr<MemTracker>& mem_tracker, RuntimeProfile* profile,
                         RuntimeState* state)
        : _state(state),
          _compare_less_than(compare_less_than),
          _in_mem_tuple_sorter(nullptr),
          _block_mgr(state->block_mgr2()),
          _block_mgr_client(nullptr),
          _has_var_len_slots(false),
          _sort_tuple_slot_expr_ctxs(slot_materialize_expr_ctxs),
          _mem_tracker(mem_tracker),
          _output_row_desc(output_row_desc),
          _unsorted_run(nullptr),
          _profile(profile),
          _initial_runs_counter(nullptr),
          _num_merges_counter(nullptr),
          _in_mem_sort_timer(nullptr),
          _sorted_data_size(nullptr),
          _spilled(false) {}

SpillSorter::~SpillSorter() {
    // Delete blocks from the block mgr.
    for (deque<Run*>::iterator it = _sorted_runs.begin(); it != _sorted_runs.end(); ++it) {
        (*it)->delete_all_blocks();
    }
    for (deque<Run*>::iterator it = _merging_runs.begin(); it != _merging_runs.end(); ++it) {
        (*it)->delete_all_blocks();
    }
    if (_unsorted_run != nullptr) {
        _unsorted_run->delete_all_blocks();
    }
    _block_mgr->clear_reservations(_block_mgr_client);
}

Status SpillSorter::init() {
    DCHECK(_unsorted_run == nullptr) << "Already initialized";
    TupleDescriptor* sort_tuple_desc = _output_row_desc->tuple_descriptors()[0];
    _has_var_len_slots = sort_tuple_desc->has_varlen_slots();
    _in_mem_tuple_sorter.reset(new TupleSorter(_compare_less_than, _block_mgr->max_block_size(),
                                               sort_tuple_desc->byte_size(), _state));
    _unsorted_run = _obj_pool.add(new Run(this, sort_tuple_desc, true));

    _initial_runs_counter = ADD_COUNTER(_profile, "InitialRunsCreated", TUnit::UNIT);
    _num_merges_counter = ADD_COUNTER(_profile, "TotalMergesPerformed", TUnit::UNIT);
    _in_mem_sort_timer = ADD_TIMER(_profile, "InMemorySortTime");
    _sorted_data_size = ADD_COUNTER(_profile, "SortDataSize", TUnit::BYTES);

    int min_blocks_required = BLOCKS_REQUIRED_FOR_MERGE;
    // Fixed and var-length blocks are separate, so we need BLOCKS_REQUIRED_FOR_MERGE
    // blocks for both if there is var-length data.
    if (_output_row_desc->tuple_descriptors()[0]->has_varlen_slots()) {
        min_blocks_required *= 2;
    }
    RETURN_IF_ERROR(_block_mgr->register_client(min_blocks_required, _mem_tracker, _state,
                                                &_block_mgr_client));

    DCHECK(_unsorted_run != nullptr);
    RETURN_IF_ERROR(_unsorted_run->init());
    return Status::OK();
}

Status SpillSorter::add_batch(RowBatch* batch) {
    DCHECK(_unsorted_run != nullptr);
    DCHECK(batch != nullptr);
    int num_processed = 0;
    int cur_batch_index = 0;
    while (cur_batch_index < batch->num_rows()) {
        if (_has_var_len_slots) {
            RETURN_IF_ERROR(_unsorted_run->add_batch<true>(batch, cur_batch_index, &num_processed));
        } else {
            RETURN_IF_ERROR(
                    _unsorted_run->add_batch<false>(batch, cur_batch_index, &num_processed));
        }
        cur_batch_index += num_processed;
        if (cur_batch_index < batch->num_rows()) {
            // The current run is full. Sort it and begin the next one.
            RETURN_IF_ERROR(sort_run());
            RETURN_IF_ERROR(_sorted_runs.back()->unpin_all_blocks());
            _spilled = true;
            _unsorted_run =
                    _obj_pool.add(new Run(this, _output_row_desc->tuple_descriptors()[0], true));
            RETURN_IF_ERROR(_unsorted_run->init());
        }
    }
    return Status::OK();
}

Status SpillSorter::input_done() {
    // Sort the tuples accumulated so far in the current run.
    RETURN_IF_ERROR(sort_run());

    if (_sorted_runs.size() == 1) {
        // The entire input fit in one run. Read sorted rows in get_next() directly
        // from the sorted run.
        RETURN_IF_ERROR(_sorted_runs.back()->prepare_read());
    } else {
        // At least one merge is necessary.
        int blocks_per_run = _has_var_len_slots ? 2 : 1;
        int min_buffers_for_merge = _sorted_runs.size() * blocks_per_run;
        // Check if the final run needs to be unpinned.
        bool unpinned_final = false;
        if (_block_mgr->num_free_buffers() < min_buffers_for_merge - blocks_per_run) {
            // Number of available buffers is less than the size of the final run and
            // the buffers needed to read the remainder of the runs in memory.
            // Unpin the final run.
            RETURN_IF_ERROR(_sorted_runs.back()->unpin_all_blocks());
            unpinned_final = true;
        } else {
            // No need to unpin the current run. There is enough memory to stream the
            // other runs.
            // TODO: revisit. It might be better to unpin some from this run if it means
            // we can get double buffering in the other runs.
        }

        // For an intermediate merge, intermediate_merge_batch contains deep-copied rows from
        // the input runs. If (_unmerged_sorted_runs.size() > max_runs_per_final_merge),
        // one or more intermediate merges are required.
        // TODO: Attempt to allocate more memory before doing intermediate merges. This may
        // be possible if other operators have relinquished memory after the sort has built
        // its runs.
        if (min_buffers_for_merge > _block_mgr->available_allocated_buffers()) {
            DCHECK(unpinned_final);
            RETURN_IF_ERROR(merge_intermediate_runs());
        }

        // Create the final merger.
        RETURN_IF_ERROR(create_merger(_sorted_runs.size()));
    }
    return Status::OK();
}

Status SpillSorter::get_next(RowBatch* output_batch, bool* eos) {
    if (_sorted_runs.size() == 1) {
        DCHECK(_sorted_runs.back()->_is_pinned);
        // In this case, only TupleRows are copied into output_batch. Sorted tuples are left
        // in the pinned blocks in the single sorted run.
        RETURN_IF_ERROR(_sorted_runs.back()->get_next<false>(output_batch, eos));
        if (*eos) {
            _sorted_runs.back()->transfer_resources(output_batch);
        }
    } else {
        // In this case, rows are deep copied into output_batch.
        RETURN_IF_ERROR(_merger->get_next(output_batch, eos));
    }
    return Status::OK();
}

Status SpillSorter::reset() {
    _merger.reset();
    _merging_runs.clear();
    _sorted_runs.clear();
    _obj_pool.clear();
    DCHECK(_unsorted_run == nullptr);
    _unsorted_run = _obj_pool.add(new Run(this, _output_row_desc->tuple_descriptors()[0], true));
    RETURN_IF_ERROR(_unsorted_run->init());
    return Status::OK();
}

Status SpillSorter::sort_run() {
    BufferedBlockMgr2::Block* last_block = _unsorted_run->_fixed_len_blocks.back();
    if (last_block->valid_data_len() > 0) {
        _sorted_data_size->update(last_block->valid_data_len());
    } else {
        last_block->del();
        _unsorted_run->_fixed_len_blocks.pop_back();
    }
    if (_has_var_len_slots) {
        DCHECK(_unsorted_run->_var_len_copy_block != nullptr);
        last_block = _unsorted_run->_var_len_blocks.back();
        if (last_block->valid_data_len() > 0) {
            _sorted_data_size->update(last_block->valid_data_len());
        } else {
            last_block->del();
            _unsorted_run->_var_len_blocks.pop_back();
            if (_unsorted_run->_var_len_blocks.size() == 0) {
                _unsorted_run->_var_len_copy_block->del();
                _unsorted_run->_var_len_copy_block = nullptr;
            }
        }
    }
    {
        SCOPED_TIMER(_in_mem_sort_timer);
        _in_mem_tuple_sorter->sort(_unsorted_run);
        RETURN_IF_CANCELLED(_state);
    }
    _sorted_runs.push_back(_unsorted_run);
    _unsorted_run = nullptr;
    return Status::OK();
}

uint64_t SpillSorter::estimate_merge_mem(uint64_t available_blocks, RowDescriptor* row_desc,
                                         int merge_batch_size) {
    bool has_var_len_slots = row_desc->tuple_descriptors()[0]->has_varlen_slots();
    int blocks_per_run = has_var_len_slots ? 2 : 1;
    int max_input_runs_per_merge = (available_blocks / blocks_per_run) - 1;
    // During a merge, the batches corresponding to the input runs contain only TupleRows.
    // (The data itself is in pinned blocks held by the run)
    uint64_t input_batch_mem = merge_batch_size * sizeof(Tuple*) * max_input_runs_per_merge;
    // Since rows are deep copied into the output batch for the merger, use a pessimistic
    // estimate of the memory required.
    uint64_t output_batch_mem = RowBatch::AT_CAPACITY_MEM_USAGE;

    return input_batch_mem + output_batch_mem;
}

Status SpillSorter::merge_intermediate_runs() {
    int blocks_per_run = _has_var_len_slots ? 2 : 1;
    int max_runs_per_final_merge = _block_mgr->available_allocated_buffers() / blocks_per_run;

    // During an intermediate merge, blocks from the output sorted run will have to be pinned.
    int max_runs_per_intermediate_merge = max_runs_per_final_merge - 1;
    DCHECK_GT(max_runs_per_intermediate_merge, 1);
    // For an intermediate merge, intermediate_merge_batch contains deep-copied rows from
    // the input runs. If (_sorted_runs.size() > max_runs_per_final_merge),
    // one or more intermediate merges are required.
    unique_ptr<RowBatch> intermediate_merge_batch;
    while (_sorted_runs.size() > max_runs_per_final_merge) {
        // An intermediate merge adds one merge to _unmerged_sorted_runs.
        // Merging 'runs - (_max_runs_final - 1)' number of runs is sufficient to guarantee
        // that the final merge can be performed.
        int num_runs_to_merge =
                std::min<int>(max_runs_per_intermediate_merge,
                              _sorted_runs.size() - max_runs_per_intermediate_merge);
        RETURN_IF_ERROR(create_merger(num_runs_to_merge));
        RowBatch intermediate_merge_batch(*_output_row_desc, _state->batch_size());
        // merged_run is the new sorted run that is produced by the intermediate merge.
        Run* merged_run =
                _obj_pool.add(new Run(this, _output_row_desc->tuple_descriptors()[0], false));
        RETURN_IF_ERROR(merged_run->init());
        bool eos = false;
        while (!eos) {
            // Copy rows into the new run until done.
            int num_copied = 0;
            RETURN_IF_CANCELLED(_state);
            RETURN_IF_ERROR(_merger->get_next(&intermediate_merge_batch, &eos));
            Status ret_status;
            if (_has_var_len_slots) {
                ret_status = merged_run->add_batch<true>(&intermediate_merge_batch, 0, &num_copied);
            } else {
                ret_status =
                        merged_run->add_batch<false>(&intermediate_merge_batch, 0, &num_copied);
            }
            if (!ret_status.ok()) return ret_status;

            DCHECK_EQ(num_copied, intermediate_merge_batch.num_rows());
            intermediate_merge_batch.reset();
        }

        BufferedBlockMgr2::Block* last_block = merged_run->_fixed_len_blocks.back();
        if (last_block->valid_data_len() > 0) {
            RETURN_IF_ERROR(last_block->unpin());
        } else {
            last_block->del();
            merged_run->_fixed_len_blocks.pop_back();
        }
        if (_has_var_len_slots) {
            last_block = merged_run->_var_len_blocks.back();
            if (last_block->valid_data_len() > 0) {
                RETURN_IF_ERROR(last_block->unpin());
            } else {
                last_block->del();
                merged_run->_var_len_blocks.pop_back();
            }
        }
        merged_run->_is_pinned = false;
        _sorted_runs.push_back(merged_run);
    }

    return Status::OK();
}

Status SpillSorter::create_merger(int num_runs) {
    DCHECK_GT(num_runs, 1);

    // Clean up the runs from the previous merge.
    for (deque<Run*>::iterator it = _merging_runs.begin(); it != _merging_runs.end(); ++it) {
        (*it)->delete_all_blocks();
    }
    _merging_runs.clear();
    _merger.reset(new SortedRunMerger(_compare_less_than, _output_row_desc, _profile, true));

    vector<function<Status(RowBatch**)>> merge_runs;
    merge_runs.reserve(num_runs);
    for (int i = 0; i < num_runs; ++i) {
        Run* run = _sorted_runs.front();
        RETURN_IF_ERROR(run->prepare_read());
        // Run::get_next_batch() is used by the merger to retrieve a batch of rows to merge
        // from this run.
        merge_runs.push_back(
                bind<Status>(mem_fn(&Run::get_next_batch), run, std::placeholders::_1));
        _sorted_runs.pop_front();
        _merging_runs.push_back(run);
    }
    RETURN_IF_ERROR(_merger->prepare(merge_runs));

    _num_merges_counter->update(1);
    return Status::OK();
}

} // namespace doris
