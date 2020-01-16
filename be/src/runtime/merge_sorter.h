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

#ifndef DORIS_BE_SRC_QUERY_RUNTIME_SORTER_H
#define DORIS_BE_SRC_QUERY_RUNTIME_SORTER_H

#include "runtime/buffered_block_mgr.h"
#include "util/tuple_row_compare.h"
#include "common/object_pool.h"
#include "util/runtime_profile.h"

namespace doris {
class RuntimeProfile;
class RowBatch;
struct BufferDescriptor;

// Sorter contains the external sort implementation. Its purpose is to sort arbitrarily
// large input data sets with a fixed memory budget by spilling data to disk if
// necessary. BufferedBlockMgr is used to allocate and manage blocks of data to be
// sorted.
//
// The client API for Sorter is as follows:
// AddBatch() is used to add input rows to be sorted. Multiple tuples in an input row are
// materialized into a row with a single tuple (the sort tuple) using the materialization
// exprs in sort_tuple_slot_expr_ctxs_. The sort tuples are sorted according to the sort
// parameters and output by the sorter.
// AddBatch() can be called multiple times.
//
// InputDone() is called to indicate the end of input. If multiple sorted runs were
// created, it triggers intermediate merge steps (if necessary) and creates the final
// merger that returns results via GetNext().
//
// GetNext() is used to retrieve sorted rows. It can be called multiple times.
// AddBatch(), InputDone() and GetNext() must be called in that order.
//
// Batches of input rows are collected into a sequence of pinned BufferedBlockMgr blocks
// called a run. The maximum size of a run is determined by the maximum available buffers
// in the block manager. After the run is full, it is sorted in memory, unpinned and the
// next run is collected.  The variable-length column data (e.g. string slots) in the
// materialized sort tuples are stored in separate sequence of blocks from the tuples
// themselves.
// When the blocks containing tuples in a run are unpinned, the var-len slot pointers are
// converted to offsets from the start of the first var-len data block. When a block is
// read back, these offsets are converted back to pointers.
// The in-memory sorter sorts the fixed-length tuples in-place. The output rows have the
// same schema as the materialized sort tuples.
//
// After the input is consumed, the sorter is left with one or more sorted runs. The
// client calls GetNext(output_batch) to retrieve batches of sorted rows. If there are
// multiple runs, the runs are merged using SortedRunMerger to produce a stream of sorted
// tuples. At least one block per run (two if there are var-length slots) must be pinned
// in memory during a merge, so multiple merges may be necessary if the number of runs is
// too large. During a merge, rows from multiple sorted input runs are compared and copied
// into a single larger run. One input batch is created to hold tuple rows for each
// input run, and one batch is created to hold deep copied rows (i.e. ptrs + data) from
// the output of the merge.
//
// If there is a single sorted run (i.e. no merge required), only tuple rows are
// copied into the output batch supplied by GetNext, and the data itself is left in
// pinned blocks held by the sorter.
//
// During a merge, one row batch is created for each input run, and one batch is created
// for the output of the merge (if is not the final merge). It is assumed that the memory
// for these batches have already been accounted for in the memory budget for the sort.
// That is, the memory for these batches does not come out of the block buffer manager.
//
// TODO: Not necessary to actually copy var-len data - instead take ownership of the
// var-length data in the input batch. Copying can be deferred until a run is unpinned.
// TODO: When the first run is constructed, create a sequence of pointers to materialized
// tuples. If the input fits in memory, the pointers can be sorted instead of sorting the
// tuples in place.
class MergeSorter {
public:
    // sort_tuple_slot_exprs are the slot exprs used to materialize the tuple to be sorted.
    // compare_less_than is a comparator for the sort tuples (returns true if lhs < rhs).
    // merge_batch_size_ is the size of the batches created to provide rows to the merger
    // and retrieve rows from an intermediate merger.
    MergeSorter(const TupleRowComparator& compare_less_than,
           const std::vector<ExprContext*>& sort_tuple_slot_expr_ctxs,
           RowDescriptor* output_row_desc,
           RuntimeProfile* profile, RuntimeState* state);

    ~MergeSorter();

    // Adds a batch of input rows to the current unsorted run.
    Status add_batch(RowBatch* batch);

    // Called to indicate there is no more input. Triggers the creation of merger(s) if
    // necessary.
    Status input_done();

    // Get the next batch of sorted output rows from the sorter.
    Status get_next(RowBatch* batch, bool* eos);

private:
    class Run;
    class TupleSorter;

    // Runtime state instance used to check for cancellation. Not owned.
    RuntimeState* const _state;

    // Sorts _unsorted_run and appends it to the list of sorted runs. Deletes any empty
    // blocks at the end of the run. Updates the sort bytes counter if necessary.
    Status sort_run();


    // In memory sorter and less-than comparator.
    TupleRowComparator _compare_less_than;
    boost::scoped_ptr<TupleSorter> _in_mem_tuple_sorter;

    // Block manager object used to allocate, pin and release runs. Not owned by Sorter.
    BufferedBlockMgr* _block_mgr;

    // Handle to block mgr to make allocations from.
    //BufferedBlockMgr::Client* block_mgr_client_;

    // True if the tuples to be sorted have var-length slots.
    bool _has_var_len_slots;

    // The current unsorted run that is being collected. Is sorted and added to
    // _sorted_runs after it is full (i.e. number of blocks allocated == max available
    // buffers) or after the input is complete. Owned and placed in _obj_pool.
    // When it is added to _sorted_runs, it is set to NULL.
    Run* _unsorted_run;

    // List of sorted runs that have been produced but not merged. _unsorted_run is added
    // to this list after an in-memory sort. Sorted runs produced by intermediate merges
    // are also added to this list. Runs are added to the object pool.
    std::list<Run*> _sorted_runs;

    // Descriptor for the sort tuple. Input rows are materialized into 1 tuple before
    // sorting. Not owned by the Sorter.
    RowDescriptor* _output_row_desc;

    // Expressions used to materialize the sort tuple. Contains one expr per slot in the
    // tuple.
    std::vector<ExprContext*> _sort_tuple_slot_expr_ctxs;

    // Pool of owned Run objects.
    ObjectPool _obj_pool;

    // Runtime profile and counters for this sorter instance.
    RuntimeProfile* _profile;
    RuntimeProfile::Counter* _initial_runs_counter;
    RuntimeProfile::Counter* _num_merges_counter;
    RuntimeProfile::Counter* _in_mem_sort_timer;
    RuntimeProfile::Counter* _sorted_data_size;
};

} // namespace doris

#endif
