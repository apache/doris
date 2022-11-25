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
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"
#include "util/thread.h"
#include "util/types.h"
#include "vec/exec/vbroker_scanner.h"
#include "vec/exec/vjson_scanner.h"
#include "vec/exec/vorc_scanner.h"
#include "vec/exec/vparquet_scanner.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

VBrokerScanNode::VBrokerScanNode(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _tuple_id(tnode.broker_scan_node.tuple_id),
          _runtime_state(nullptr),
          _tuple_desc(nullptr),
          _num_running_scanners(0),
          _scan_finished(false),
          _max_buffered_batches(32),
          _wait_scanner_timer(nullptr) {}

Status VBrokerScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ScanNode::init(tnode, state));
    auto& broker_scan_node = tnode.broker_scan_node;

    if (broker_scan_node.__isset.pre_filter_exprs) {
        _pre_filter_texprs = broker_scan_node.pre_filter_exprs;
    }

    return Status::OK();
}

Status VBrokerScanNode::prepare(RuntimeState* state) {
    VLOG_QUERY << "VBrokerScanNode prepare";
    RETURN_IF_ERROR(ScanNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    // get tuple desc
    _runtime_state = state;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    if (_tuple_desc == nullptr) {
        return Status::InternalError("Failed to get tuple descriptor, _tuple_id={}", _tuple_id);
    }

    // Initialize slots map
    for (auto slot : _tuple_desc->slots()) {
        auto pair = _slots_map.emplace(slot->col_name(), slot);
        if (!pair.second) {
            return Status::InternalError("Failed to insert slot, col_name={}", slot->col_name());
        }
    }

    // Profile
    _wait_scanner_timer = ADD_TIMER(runtime_profile(), "WaitScannerTime");

    return Status::OK();
}

Status VBrokerScanNode::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VBrokerScanNode::open");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    RETURN_IF_CANCELLED(state);

    RETURN_IF_ERROR(start_scanners());

    return Status::OK();
}

Status VBrokerScanNode::start_scanners() {
    {
        std::unique_lock<std::mutex> l(_batch_queue_lock);
        _num_running_scanners = 1;
    }
    _scanner_threads.emplace_back([this, size = 0, length = _scan_ranges.size(),
                                   parent_span = opentelemetry::trace::Tracer::GetCurrentSpan()] {
        OpentelemetryScope scope {parent_span};
        this->scanner_worker(size, length);
    });
    return Status::OK();
}

Status VBrokerScanNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    INIT_AND_SCOPE_GET_NEXT_SPAN(state->get_tracer(), _get_next_span, "VBrokerScanNode::get_next");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
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

    const int batch_size = _runtime_state->batch_size();
    while (true) {
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
        if (!scanner_block) {
            if (_mutable_block && !_mutable_block->empty()) {
                *block = _mutable_block->to_block();
                reached_limit(block, eos);
                LOG_IF(INFO, *eos) << "VBrokerScanNode ReachedLimit.";
            }
            _scan_finished.store(true);
            *eos = true;
            return Status::OK();
        }
        // notify one scanner
        _queue_writer_cond.notify_one();

        if (UNLIKELY(!_mutable_block)) {
            _mutable_block.reset(new MutableBlock(scanner_block->clone_empty()));
        }

        if (_mutable_block->rows() + scanner_block->rows() < batch_size) {
            // merge scanner_block into _mutable_block
            _mutable_block->add_rows(scanner_block.get(), 0, scanner_block->rows());
            continue;
        } else {
            if (_mutable_block->empty()) {
                // directly use scanner_block
                *block = *scanner_block;
            } else {
                // copy _mutable_block firstly, then merge scanner_block into _mutable_block for next.
                *block = _mutable_block->to_block();
                _mutable_block->set_muatable_columns(scanner_block->clone_empty_columns());
                _mutable_block->add_rows(scanner_block.get(), 0, scanner_block->rows());
            }
            break;
        }
    }

    reached_limit(block, eos);
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
    if (is_closed()) {
        return Status::OK();
    }
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VBrokerScanNode::close");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    _scan_finished.store(true);
    _queue_writer_cond.notify_all();
    _queue_reader_cond.notify_all();
    for (int i = 0; i < _scanner_threads.size(); ++i) {
        _scanner_threads[i].join();
    }

    // Close
    _batch_queue.clear();
    return ExecNode::close(state);
}

Status VBrokerScanNode::scanner_scan(const TBrokerScanRange& scan_range, ScannerCounter* counter) {
    //create scanner object and open
    std::unique_ptr<BaseScanner> scanner = create_scanner(scan_range, counter);
    RETURN_IF_ERROR(scanner->open());
    bool scanner_eof = false;
    while (!scanner_eof) {
        RETURN_IF_CANCELLED(_runtime_state);
        // If we have finished all works
        if (_scan_finished.load() || !_process_status.ok()) {
            return Status::OK();
        }

        std::shared_ptr<vectorized::Block> block(new vectorized::Block());
        RETURN_IF_ERROR(scanner->get_next(block.get(), &scanner_eof));
        if (block->rows() == 0) {
            continue;
        }
        auto old_rows = block->rows();
        RETURN_IF_ERROR(VExprContext::filter_block(_vconjunct_ctx_ptr, block.get(),
                                                   _tuple_desc->slots().size()));
        counter->num_rows_unselected += old_rows - block->rows();
        if (block->rows() == 0) {
            continue;
        }

        std::unique_lock<std::mutex> l(_batch_queue_lock);
        while (_process_status.ok() && !_scan_finished.load() && !_runtime_state->is_cancelled() &&
               // stop pushing more batch if
               // 1. too many batches in queue, or
               // 2. at least one batch in queue and memory exceed limit.
               (_block_queue.size() >= _max_buffered_batches || !_block_queue.empty())) {
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
        _block_queue.push_back(std::move(block));

        // Notify reader to process
        _queue_reader_cond.notify_one();
    }
    return Status::OK();
}

void VBrokerScanNode::scanner_worker(int start_idx, int length) {
    START_AND_SCOPE_SPAN(_runtime_state->get_tracer(), span, "VBrokerScanNode::scanner_worker");
    SCOPED_ATTACH_TASK(_runtime_state);
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker_shared());
    Thread::set_self_name("vbroker_scanner");
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

std::unique_ptr<BaseScanner> VBrokerScanNode::create_scanner(const TBrokerScanRange& scan_range,
                                                             ScannerCounter* counter) {
    BaseScanner* scan = nullptr;
    switch (scan_range.ranges[0].format_type) {
    case TFileFormatType::FORMAT_PARQUET:
        scan = new vectorized::VParquetScanner(_runtime_state, runtime_profile(), scan_range.params,
                                               scan_range.ranges, scan_range.broker_addresses,
                                               _pre_filter_texprs, counter);
        break;
    case TFileFormatType::FORMAT_ORC:
        scan = new vectorized::VORCScanner(_runtime_state, runtime_profile(), scan_range.params,
                                           scan_range.ranges, scan_range.broker_addresses,
                                           _pre_filter_texprs, counter);
        break;
    case TFileFormatType::FORMAT_JSON:
        if (config::enable_simdjson_reader) {
            scan = new vectorized::VJsonScanner<vectorized::VSIMDJsonReader>(
                    _runtime_state, runtime_profile(), scan_range.params, scan_range.ranges,
                    scan_range.broker_addresses, _pre_filter_texprs, counter);
        } else {
            scan = new vectorized::VJsonScanner<vectorized::VJsonReader>(
                    _runtime_state, runtime_profile(), scan_range.params, scan_range.ranges,
                    scan_range.broker_addresses, _pre_filter_texprs, counter);
        }
        break;
    default:
        scan = new vectorized::VBrokerScanner(_runtime_state, runtime_profile(), scan_range.params,
                                              scan_range.ranges, scan_range.broker_addresses,
                                              _pre_filter_texprs, counter);
    }
    scan->reg_conjunct_ctxs(_tuple_id, _conjunct_ctxs);
    std::unique_ptr<BaseScanner> scanner(scan);
    return scanner;
}

// This function is called after plan node has been prepared.
Status VBrokerScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    _scan_ranges = scan_ranges;
    return Status::OK();
}

void VBrokerScanNode::debug_string(int ident_level, std::stringstream* out) const {
    (*out) << "VBrokerScanNode";
}

} // namespace doris::vectorized
