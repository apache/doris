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

#include "vec/exec/vbroker_scan_node.h"

#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"
#include "runtime/mem_tracker.h"
#include "util/runtime_profile.h"
#include "util/types.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

VBrokerScanNode::VBrokerScanNode(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
        : BrokerScanNode(pool, tnode, descs) {
    _vectorized = true;
}

Status VBrokerScanNode::start_scanners() {
    {
        std::unique_lock<std::mutex> l(_batch_queue_lock);
        _num_running_scanners = 1;
    }
    _scanner_threads.emplace_back(&VBrokerScanNode::scanner_worker, this, 0, _scan_ranges.size());
    return Status::OK();
}

Status VBrokerScanNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    // check if CANCELLED.
    if (state->is_cancelled()) {
        std::unique_lock<std::mutex> l(_batch_queue_lock);
        if (update_status(Status::Cancelled("Cancelled"))) {
            // Notify all scanners
            _queue_writer_cond.notify_all();
        }
    }

    if (_scan_finished.load()) {
        *eos = true;
        return Status::OK();
    }

    std::shared_ptr<vectorized::Block> scanner_block;
    {
        std::unique_lock<std::mutex> l(_batch_queue_lock);
        while (_process_status.ok() && !_runtime_state->is_cancelled() &&
               _num_running_scanners > 0 && _block_queue.empty()) {
            SCOPED_TIMER(_wait_scanner_timer);
            _queue_reader_cond.wait_for(l, std::chrono::seconds(1));
        }
        if (!_process_status.ok()) {
            // Some scanner process failed.
            return _process_status;
        }
        if (_runtime_state->is_cancelled()) {
            if (update_status(Status::Cancelled("Cancelled"))) {
                _queue_writer_cond.notify_all();
            }
            return _process_status;
        }
        if (!_block_queue.empty()) {
            scanner_block = _block_queue.front();
            _block_queue.pop_front();
        }
    }

    // All scanner has been finished, and all cached batch has been read
    if (scanner_block == nullptr) {
        _scan_finished.store(true);
        *eos = true;
        return Status::OK();
    }

    // notify one scanner
    _queue_writer_cond.notify_one();

    reached_limit(scanner_block.get(), eos);
    *block = *scanner_block;

    if (*eos) {
        _scan_finished.store(true);
        _queue_writer_cond.notify_all();
        LOG(INFO) << "VBrokerScanNode ReachedLimit.";
    } else {
        *eos = false;
    }

    return Status::OK();
}

Status VBrokerScanNode::close(RuntimeState* state) {
    auto status = BrokerScanNode::close(state);
    _block_queue.clear();
    return status;
}

Status VBrokerScanNode::scanner_scan(const TBrokerScanRange& scan_range, ScannerCounter* counter) {
    //create scanner object and open
    std::unique_ptr<BaseScanner> scanner = create_scanner(scan_range, counter);
    RETURN_IF_ERROR(scanner->open());
    bool scanner_eof = false;

    const int batch_size = _runtime_state->batch_size();
    size_t slot_num = _tuple_desc->slots().size();

    while (!scanner_eof) {
        std::shared_ptr<vectorized::Block> block(new vectorized::Block());
        std::vector<vectorized::MutableColumnPtr> columns(slot_num);
        for (int i = 0; i < slot_num; i++) {
            columns[i] = _tuple_desc->slots()[i]->get_empty_mutable_column();
        }

        while (columns[0]->size() < batch_size && !scanner_eof) {
            RETURN_IF_CANCELLED(_runtime_state);
            // If we have finished all works
            if (_scan_finished.load()) {
                return Status::OK();
            }

            RETURN_IF_ERROR(scanner->get_next(columns, &scanner_eof));
            if (scanner_eof) {
                break;
            }
        }

        if (!columns[0]->empty()) {
            auto n_columns = 0;
            for (const auto slot_desc : _tuple_desc->slots()) {
                block->insert(ColumnWithTypeAndName(std::move(columns[n_columns++]),
                                                    slot_desc->get_data_type_ptr(),
                                                    slot_desc->col_name()));
            }

            auto old_rows = block->rows();

            RETURN_IF_ERROR(VExprContext::filter_block(_vconjunct_ctx_ptr, block.get(),
                                                       _tuple_desc->slots().size()));

            counter->num_rows_unselected += old_rows - block->rows();

            std::unique_lock<std::mutex> l(_batch_queue_lock);
            while (_process_status.ok() && !_scan_finished.load() &&
                   !_runtime_state->is_cancelled() &&
                   // stop pushing more batch if
                   // 1. too many batches in queue, or
                   // 2. at least one batch in queue and memory exceed limit.
                   (_block_queue.size() >= _max_buffered_batches ||
                    (mem_tracker()->any_limit_exceeded() && !_block_queue.empty()))) {
                _queue_writer_cond.wait_for(l, std::chrono::seconds(1));
            }
            // Process already set failed, so we just return OK
            if (!_process_status.ok()) {
                return Status::OK();
            }
            // Scan already finished, just return
            if (_scan_finished.load()) {
                return Status::OK();
            }
            // Runtime state is canceled, just return cancel
            if (_runtime_state->is_cancelled()) {
                return Status::Cancelled("Cancelled");
            }
            // Queue size Must be smaller than _max_buffered_batches
            _block_queue.push_back(block);

            // Notify reader to
            _queue_reader_cond.notify_one();
        }
    }

    return Status::OK();
}

void VBrokerScanNode::scanner_worker(int start_idx, int length) {
    Status status = Status::OK();
    ScannerCounter counter;
    for (int i = 0; i < length && status.ok(); ++i) {
        const TBrokerScanRange& scan_range =
                _scan_ranges[start_idx + i].scan_range.broker_scan_range;
        status = scanner_scan(scan_range, &counter);
        if (!status.ok()) {
            LOG(WARNING) << "Scanner[" << start_idx + i
                         << "] process failed. status=" << status.get_error_msg();
        }
    }

    // Update stats
    _runtime_state->update_num_rows_load_filtered(counter.num_rows_filtered);
    _runtime_state->update_num_rows_load_unselected(counter.num_rows_unselected);

    // scanner is going to finish
    {
        std::lock_guard<std::mutex> l(_batch_queue_lock);
        if (!status.ok()) {
            update_status(status);
        }
        // This scanner will finish
        _num_running_scanners--;
    }
    _queue_reader_cond.notify_all();
    // If one scanner failed, others don't need scan any more
    if (!status.ok()) {
        _queue_writer_cond.notify_all();
    }
}

} // namespace doris::vectorized