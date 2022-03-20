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

#ifndef DORIS_BE_SRC_RUNTIME_SORTED_RUN_MERGER_H
#define DORIS_BE_SRC_RUNTIME_SORTED_RUN_MERGER_H

#include <mutex>

#include "common/object_pool.h"
#include "util/tuple_row_compare.h"

namespace doris {

class RowBatch;
class RowDescriptor;
class RuntimeProfile;

// SortedRunMerger is used to merge multiple sorted runs of tuples. A run is a sorted
// sequence of row batches, which are fetched from a RunBatchSupplier function object.
// Merging is implemented using a binary min-heap that maintains the run with the next
// tuple in sorted order at the top of the heap.
//
// Merged batches of rows are retrieved from SortedRunMerger via calls to get_next().
// The merger is constructed with a boolean flag deep_copy_input.
// If true, sorted output rows are deep copied into the data pool of the output batch.
// If false, get_next() only copies tuple pointers (TupleRows) into the output batch,
// and transfers resource ownership from the input batches to the output batch when
// an input batch is processed.
class SortedRunMerger {
public:
    // Function that returns the next batch of rows from an input sorted run. The batch
    // is owned by the supplier (i.e. not SortedRunMerger). eos is indicated by an nullptr
    // batch being returned.
    typedef std::function<Status(RowBatch**)> RunBatchSupplier;

    SortedRunMerger(const TupleRowComparator& compare_less_than, RowDescriptor* row_desc,
                    RuntimeProfile* profile, bool deep_copy_input);

    virtual ~SortedRunMerger() = default;

    // Prepare this merger to merge and return rows from the sorted runs in 'input_runs'.
    // Retrieves the first batch from each run and sets up the binary heap implementing
    // the priority queue.
    Status prepare(const std::vector<RunBatchSupplier>& input_runs, bool parallel = false);

    // Return the next batch of sorted rows from this merger.
    Status get_next(RowBatch* output_batch, bool* eos);

    // Only Child class implement this Method, Return the next batch of sorted rows from this merger.
    virtual Status get_batch(RowBatch** output_batch) {
        return Status::InternalError("no support method get_batch(RowBatch** output_batch)");
    }

    // Called to finalize a merge when deep_copy is false. Transfers resources from
    // all input batches to the specified output batch.
    void transfer_all_resources(RowBatch* transfer_resource_batch);

protected:
    class BatchedRowSupplier;
    class ParallelBatchedRowSupplier;

    // Assuming the element at parent_index is the only out of place element in the heap,
    // restore the heap property (i.e. swap elements so parent <= children).
    void heapify(int parent_index);

    // The binary min-heap used to merge rows from the sorted input runs. Since the heap is
    // stored in a 0-indexed array, the 0-th element is the minimum element in the heap,
    // and the children of the element at index i are 2*i+1 and 2*i+2. The heap property is
    // that row of the parent element is <= the rows of the child elements according to the
    // comparator _compare_less_than.
    // The BatchedRowSupplier objects used in the _min_heap are owned by this
    // SortedRunMerger instance.
    std::vector<BatchedRowSupplier*> _min_heap;

    // Row comparator. Returns true if lhs < rhs.
    TupleRowComparator _compare_less_than;

    // Descriptor for the rows provided by the input runs. Owned by the exec-node through
    // which this merger was created.
    RowDescriptor* _input_row_desc;

    // True if rows must be deep copied into the output batch.
    bool _deep_copy_input;

    // Pool of BatchedRowSupplier instances.
    ObjectPool _pool;

    // Times calls to get_next().
    RuntimeProfile::Counter* _get_next_timer;

    // Times calls to get the next batch of rows from the input run.
    RuntimeProfile::Counter* _get_next_batch_timer;
};

class ChildSortedRunMerger : public SortedRunMerger {
public:
    ChildSortedRunMerger(const TupleRowComparator& compare_less_than, RowDescriptor* row_desc,
                         RuntimeProfile* profile, uint32_t row_batch_size, bool deep_copy_input);

    Status get_batch(RowBatch** output_batch) override;

private:
    // Ptr to prevent mem leak for api get_batch(Rowbatch**)
    std::unique_ptr<RowBatch> _current_row_batch;

    // The data in merger is exhaust
    bool _eos = false;

    uint32_t _row_batch_size;
};

} // namespace doris

#endif // DORIS_BE_SRC_RUNTIME_SORTED_RUN_MERGER_H
