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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/sorted-run-merger.cc
// and modified by Doris

#include "runtime/sorted_run_merger.h"

#include <condition_variable>
#include <vector>

#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/row_batch.h"
#include "runtime/sorter.h"
#include "runtime/thread_context.h"
#include "runtime/tuple_row.h"
#include "util/debug_util.h"
#include "util/defer_op.h"
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
              _input_row_batch(nullptr),
              _input_row_batch_index(-1),
              _parent(parent) {}

    virtual ~BatchedRowSupplier() = default;

    // Retrieves the first batch of sorted rows from the run.
    virtual Status init(bool* done) {
        *done = false;
        RETURN_IF_ERROR(_sorted_run(&_input_row_batch));
        if (_input_row_batch == nullptr) {
            *done = true;
            return Status::OK();
        }
        RETURN_IF_ERROR(next(nullptr, done));
        return Status::OK();
    }

    // Increment the current row index. If the current input batch is exhausted fetch the
    // next one from the sorted run. Transfer ownership to transfer_batch if not nullptr.
    virtual Status next(RowBatch* transfer_batch, bool* done) {
        DCHECK(_input_row_batch != nullptr);
        ++_input_row_batch_index;
        if (_input_row_batch_index < _input_row_batch->num_rows()) {
            *done = false;
        } else {
            ScopedTimer<MonotonicStopWatch> timer(_parent->_get_next_batch_timer);
            if (transfer_batch != nullptr) {
                _input_row_batch->transfer_resource_ownership(transfer_batch);
            }

            RETURN_IF_ERROR(_sorted_run(&_input_row_batch));
            DCHECK(_input_row_batch == nullptr || _input_row_batch->num_rows() > 0);
            *done = _input_row_batch == nullptr;
            _input_row_batch_index = 0;
        }
        return Status::OK();
    }

    TupleRow* current_row() const { return _input_row_batch->get_row(_input_row_batch_index); }

    RowBatch* get_row_batch() const { return _input_row_batch; }

protected:
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

class SortedRunMerger::ParallelBatchedRowSupplier : public SortedRunMerger::BatchedRowSupplier {
public:
    // Construct an instance from a sorted input run.
    ParallelBatchedRowSupplier(SortedRunMerger* parent, const RunBatchSupplier& sorted_run)
            : BatchedRowSupplier(parent, sorted_run), _input_row_batch_backup(nullptr) {}

    ~ParallelBatchedRowSupplier() {
        // when have the limit clause need to wait the _pull_task_thread join terminate
        _cancel = true;
        _backup_ready = false;
        _batch_prepared_cv.notify_one();
        _pull_task_thread.join();

        delete _input_row_batch;
        delete _input_row_batch_backup;
    }

    // Retrieves the first batch of sorted rows from the run.
    Status init(bool* done) override {
        *done = false;
        _pull_task_thread =
                std::thread(&SortedRunMerger::ParallelBatchedRowSupplier::process_sorted_run_task,
                            this, tls_ctx()->_thread_mem_tracker_mgr->mem_tracker());

        RETURN_IF_ERROR(next(nullptr, done));
        return Status::OK();
    }

    // Increment the current row index. If the current input batch is exhausted fetch the
    // next one from the sorted run. Transfer ownership to transfer_batch if not nullptr.
    Status next(RowBatch* transfer_batch, bool* done) override {
        ++_input_row_batch_index;
        if (_input_row_batch && _input_row_batch_index < _input_row_batch->num_rows()) {
            *done = false;
        } else {
            ScopedTimer<MonotonicStopWatch> timer(_parent->_get_next_batch_timer);
            if (_input_row_batch && transfer_batch != nullptr) {
                _input_row_batch->transfer_resource_ownership(transfer_batch);
            }
            // release the mem of child merge
            delete _input_row_batch;

            std::unique_lock<std::mutex> lock(_mutex);
            _batch_prepared_cv.wait(lock, [this]() { return _backup_ready.load(); });

            // switch input_row_batch_backup to _input_row_batch
            _input_row_batch = _input_row_batch_backup;
            _input_row_batch_index = 0;
            _input_row_batch_backup = nullptr;
            _backup_ready = false;
            DCHECK(_input_row_batch == nullptr || _input_row_batch->num_rows() > 0);

            *done = _input_row_batch == nullptr;
            _batch_prepared_cv.notify_one();
        }
        return Status::OK();
    }

private:
    // The backup row batch input be backup batch from _sort_run.
    RowBatch* _input_row_batch_backup;

    std::atomic_bool _backup_ready {false};

    std::atomic_bool _cancel {false};

    std::thread _pull_task_thread;

    Status _status_backup;

    std::mutex _mutex;

    // signal of new batch or the eos/cancelled condition
    std::condition_variable _batch_prepared_cv;

    void process_sorted_run_task(const std::shared_ptr<MemTracker>& mem_tracker) {
        SCOPED_ATTACH_TASK_THREAD(ThreadContext::TaskType::QUERY, mem_tracker);
        std::unique_lock<std::mutex> lock(_mutex);
        while (true) {
            _batch_prepared_cv.wait(lock, [this]() { return !_backup_ready.load(); });
            if (_cancel) {
                break;
            }

            // do merge from sender queue data
            _status_backup = _sorted_run(&_input_row_batch_backup);
            _backup_ready = true;
            Defer defer_op {[this]() { _batch_prepared_cv.notify_one(); }};

            if (!_status_backup.ok() || _input_row_batch_backup == nullptr || _cancel) {
                if (!_status_backup.ok()) _input_row_batch_backup = nullptr;
                break;
            }
        }
    }
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

Status SortedRunMerger::prepare(const vector<RunBatchSupplier>& input_runs, bool parallel) {
    DCHECK_EQ(_min_heap.size(), 0);
    _min_heap.reserve(input_runs.size());
    for (const RunBatchSupplier& input_run : input_runs) {
        BatchedRowSupplier* new_elem =
                _pool.add(parallel ? new ParallelBatchedRowSupplier(this, input_run)
                                   : new BatchedRowSupplier(this, input_run));
        DCHECK(new_elem != nullptr);
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

void SortedRunMerger::transfer_all_resources(class doris::RowBatch* transfer_resource_batch) {
    for (BatchedRowSupplier* batched_row_supplier : _min_heap) {
        auto row_batch = batched_row_supplier->get_row_batch();
        if (row_batch != nullptr) {
            row_batch->transfer_resource_ownership(transfer_resource_batch);
        }
    }
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
        RETURN_IF_ERROR(min->next(_deep_copy_input ? nullptr : output_batch, &min_run_complete));
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

ChildSortedRunMerger::ChildSortedRunMerger(const TupleRowComparator& compare_less_than,
                                           RowDescriptor* row_desc, RuntimeProfile* profile,
                                           uint32_t row_batch_size, bool deep_copy_input)
        : SortedRunMerger(compare_less_than, row_desc, profile, deep_copy_input),
          _eos(false),
          _row_batch_size(row_batch_size) {
    _get_next_timer = ADD_TIMER(profile, "ChildMergeGetNext");
    _get_next_batch_timer = ADD_TIMER(profile, "ChildMergeGetNextBatch");
}

Status ChildSortedRunMerger::get_batch(RowBatch** output_batch) {
    *output_batch = nullptr;
    if (_eos) {
        return Status::OK();
    }

    _current_row_batch.reset(new RowBatch(*_input_row_desc, _row_batch_size));

    bool eos = false;
    RETURN_IF_ERROR(get_next(_current_row_batch.get(), &eos));
    *output_batch =
            UNLIKELY(_current_row_batch->num_rows() == 0) ? nullptr : _current_row_batch.release();
    _eos = eos;

    return Status::OK();
}

} // namespace doris
