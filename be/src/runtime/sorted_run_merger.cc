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

#include "runtime/sorted_run_merger.h"

#include <vector>

#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/row_batch.h"
#include "runtime/sorter.h"
#include "runtime/tuple_row.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"

using std::vector;

namespace doris {

// BatchedRowSupplier returns individual rows in a batch obtained from a sorted input
// run (a RunBatchSupplier). Used as the heap element in the min heap maintained by the
// merger.
// next() advances the row supplier to the next row in the input batch and retrieves
// the next batch from the input if the current input batch is exhausted. Transfers
// ownership from the current input batch to an output batch if requested.
class SortedRunMerger::BatchedRowSupplier {
public:
    // Construct an instance from a sorted input run.
    BatchedRowSupplier(SortedRunMerger* parent, const RunBatchSupplier& sorted_run)
            : _sorted_run(sorted_run),
              _input_row_batch(NULL),
              _input_row_batch_index(-1),
              _parent(parent) {}

    ~BatchedRowSupplier() {}

    // Retrieves the first batch of sorted rows from the run.
    Status init(bool* done) {
        *done = false;
        RETURN_IF_ERROR(_sorted_run(&_input_row_batch));
        if (_input_row_batch == NULL) {
            *done = true;
            return Status::OK();
        }
        RETURN_IF_ERROR(next(NULL, done));
        return Status::OK();
    }

    // Increment the current row index. If the current input batch is exhausted fetch the
    // next one from the sorted run. Transfer ownership to transfer_batch if not NULL.
    Status next(RowBatch* transfer_batch, bool* done) {
        DCHECK(_input_row_batch != NULL);
        ++_input_row_batch_index;
        if (_input_row_batch_index < _input_row_batch->num_rows()) {
            *done = false;
        } else {
            ScopedTimer<MonotonicStopWatch> timer(_parent->_get_next_batch_timer);
            if (transfer_batch != NULL) {
                _input_row_batch->transfer_resource_ownership(transfer_batch);
            }

            RETURN_IF_ERROR(_sorted_run(&_input_row_batch));
            DCHECK(_input_row_batch == NULL || _input_row_batch->num_rows() > 0);
            *done = _input_row_batch == NULL;
            _input_row_batch_index = 0;
        }
        return Status::OK();
    }

    TupleRow* current_row() const { return _input_row_batch->get_row(_input_row_batch_index); }

private:
    friend class SortedRunMerger;

    // The run from which this object supplies rows.
    RunBatchSupplier _sorted_run;

    // The current input batch being processed.
    RowBatch* _input_row_batch;

    // Index into _input_row_batch of the current row being processed.
    int _input_row_batch_index;

    // The parent merger instance.
    SortedRunMerger* _parent;
};

void SortedRunMerger::heapify(int parent_index) {
    int left_index = 2 * parent_index + 1;
    int right_index = left_index + 1;
    if (left_index >= _min_heap.size()) {
        return;
    }
    int least_child = 0;
    // Find the least child of parent.
    if (right_index >= _min_heap.size() ||
        _compare_less_than(_min_heap[left_index]->current_row(),
                           _min_heap[right_index]->current_row())) {
        least_child = left_index;
    } else {
        least_child = right_index;
    }

    // If the parent is out of place, swap it with the least child and invoke
    // heapify recursively.
    if (_compare_less_than(_min_heap[least_child]->current_row(),
                           _min_heap[parent_index]->current_row())) {
        iter_swap(_min_heap.begin() + least_child, _min_heap.begin() + parent_index);
        heapify(least_child);
    }
}

SortedRunMerger::SortedRunMerger(const TupleRowComparator& compare_less_than,
                                 RowDescriptor* row_desc, RuntimeProfile* profile,
                                 bool deep_copy_input)
        : _compare_less_than(compare_less_than),
          _input_row_desc(row_desc),
          _deep_copy_input(deep_copy_input) {
    _get_next_timer = ADD_TIMER(profile, "MergeGetNext");
    _get_next_batch_timer = ADD_TIMER(profile, "MergeGetNextBatch");
}

Status SortedRunMerger::prepare(const vector<RunBatchSupplier>& input_runs) {
    DCHECK_EQ(_min_heap.size(), 0);
    _min_heap.reserve(input_runs.size());
    BOOST_FOREACH (const RunBatchSupplier& input_run, input_runs) {
        BatchedRowSupplier* new_elem = _pool.add(new BatchedRowSupplier(this, input_run));
        DCHECK(new_elem != NULL);
        bool empty = false;
        RETURN_IF_ERROR(new_elem->init(&empty));
        if (!empty) {
            _min_heap.push_back(new_elem);
        }
    }

    // Construct the min heap from the sorted runs.
    const int last_parent = (_min_heap.size() / 2) - 1;
    for (int i = last_parent; i >= 0; --i) {
        heapify(i);
    }
    return Status::OK();
}

Status SortedRunMerger::get_next(RowBatch* output_batch, bool* eos) {
    ScopedTimer<MonotonicStopWatch> timer(_get_next_timer);
    if (_min_heap.empty()) {
        *eos = true;
        return Status::OK();
    }

    while (!output_batch->at_capacity()) {
        BatchedRowSupplier* min = _min_heap[0];
        int output_row_index = output_batch->add_row();
        TupleRow* output_row = output_batch->get_row(output_row_index);
        if (_deep_copy_input) {
            min->current_row()->deep_copy(output_row, _input_row_desc->tuple_descriptors(),
                                          output_batch->tuple_data_pool(), false);
        } else {
            // Simply copy tuple pointers if deep_copy is false.
            memcpy(output_row, min->current_row(),
                   _input_row_desc->tuple_descriptors().size() * sizeof(Tuple*));
        }

        output_batch->commit_last_row();

        bool min_run_complete = false;
        // Advance to the next element in min. output_batch is supplied to transfer
        // resource ownership if the input batch in min is exhausted.
        RETURN_IF_ERROR(min->next(_deep_copy_input ? NULL : output_batch, &min_run_complete));
        if (min_run_complete) {
            // Remove the element from the heap.
            iter_swap(_min_heap.begin(), _min_heap.end() - 1);
            _min_heap.pop_back();
            if (_min_heap.empty()) break;
        }

        heapify(0);
    }

    *eos = _min_heap.empty();
    return Status::OK();
}

} // namespace doris
