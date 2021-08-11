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

#include "exec/broker_scan_node.h"

#include <chrono>
#include <sstream>

#include "common/object_pool.h"
#include "exec/broker_scanner.h"
#include "exec/json_scanner.h"
#include "exec/orc_scanner.h"
#include "exec/parquet_scanner.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/dpp_sink_internal.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace doris {

BrokerScanNode::BrokerScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _tuple_id(tnode.broker_scan_node.tuple_id),
          _runtime_state(nullptr),
          _tuple_desc(nullptr),
          _num_running_scanners(0),
          _scan_finished(false),
          _max_buffered_batches(32),
          _wait_scanner_timer(nullptr) {}

BrokerScanNode::~BrokerScanNode() {}

Status BrokerScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ScanNode::init(tnode, state));
    auto& broker_scan_node = tnode.broker_scan_node;

    if (broker_scan_node.__isset.pre_filter_exprs) {
        RETURN_IF_ERROR(Expr::create_expr_trees(_pool, broker_scan_node.pre_filter_exprs,
                                                &_pre_filter_ctxs));
    }

    return Status::OK();
}

Status BrokerScanNode::prepare(RuntimeState* state) {
    VLOG_QUERY << "BrokerScanNode prepare";
    RETURN_IF_ERROR(ScanNode::prepare(state));
    // get tuple desc
    _runtime_state = state;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    if (_tuple_desc == nullptr) {
        std::stringstream ss;
        ss << "Failed to get tuple descriptor, _tuple_id=" << _tuple_id;
        return Status::InternalError(ss.str());
    }

    // Initialize slots map
    for (auto slot : _tuple_desc->slots()) {
        auto pair = _slots_map.emplace(slot->col_name(), slot);
        if (!pair.second) {
            std::stringstream ss;
            ss << "Failed to insert slot, col_name=" << slot->col_name();
            return Status::InternalError(ss.str());
        }
    }

    if (_pre_filter_ctxs.size() > 0) {
        RETURN_IF_ERROR(Expr::prepare(_pre_filter_ctxs, state, row_desc(), expr_mem_tracker()));
    }

    // Profile
    _wait_scanner_timer = ADD_TIMER(runtime_profile(), "WaitScannerTime");

    return Status::OK();
}

Status BrokerScanNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    RETURN_IF_CANCELLED(state);

    if (_pre_filter_ctxs.size() > 0) {
        RETURN_IF_ERROR(Expr::open(_pre_filter_ctxs, state));
    }

    RETURN_IF_ERROR(start_scanners());

    return Status::OK();
}

Status BrokerScanNode::start_scanners() {
    {
        std::unique_lock<std::mutex> l(_batch_queue_lock);
        _num_running_scanners = 1;
    }
    _scanner_threads.emplace_back(&BrokerScanNode::scanner_worker, this, 0, _scan_ranges.size());
    return Status::OK();
}

Status BrokerScanNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
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

    std::shared_ptr<RowBatch> scanner_batch;
    {
        std::unique_lock<std::mutex> l(_batch_queue_lock);
        while (_process_status.ok() && !_runtime_state->is_cancelled() &&
               _num_running_scanners > 0 && _batch_queue.empty()) {
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
        if (!_batch_queue.empty()) {
            scanner_batch = _batch_queue.front();
            _batch_queue.pop_front();
        }
    }

    // All scanner has been finished, and all cached batch has been read
    if (scanner_batch == nullptr) {
        _scan_finished.store(true);
        *eos = true;
        return Status::OK();
    }

    // notify one scanner
    _queue_writer_cond.notify_one();

    // get scanner's batch memory
    row_batch->acquire_state(scanner_batch.get());
    _num_rows_returned += row_batch->num_rows();
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);

    // This is first time reach limit.
    // Only valid when query 'select * from table1 limit 20'
    if (reached_limit()) {
        int num_rows_over = _num_rows_returned - _limit;
        row_batch->set_num_rows(row_batch->num_rows() - num_rows_over);
        _num_rows_returned -= num_rows_over;
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);

        _scan_finished.store(true);
        _queue_writer_cond.notify_all();
        *eos = true;
    } else {
        *eos = false;
    }

    if (VLOG_ROW_IS_ON) {
        for (int i = 0; i < row_batch->num_rows(); ++i) {
            TupleRow* row = row_batch->get_row(i);
            VLOG_ROW << "BrokerScanNode output row: "
                     << Tuple::to_string(row->get_tuple(0), *_tuple_desc);
        }
    }

    return Status::OK();
}

Status BrokerScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::CLOSE));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    _scan_finished.store(true);
    _queue_writer_cond.notify_all();
    _queue_reader_cond.notify_all();
    for (int i = 0; i < _scanner_threads.size(); ++i) {
        _scanner_threads[i].join();
    }

    if (_pre_filter_ctxs.size() > 0) {
        Expr::close(_pre_filter_ctxs, state);
    }

    // Close
    _batch_queue.clear();

    return ExecNode::close(state);
}

// This function is called after plan node has been prepared.
Status BrokerScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    _scan_ranges = scan_ranges;
    return Status::OK();
}

void BrokerScanNode::debug_string(int ident_level, std::stringstream* out) const {
    (*out) << "BrokerScanNode";
}

std::unique_ptr<BaseScanner> BrokerScanNode::create_scanner(const TBrokerScanRange& scan_range,
                                                            const std::vector<ExprContext*>& pre_filter_ctxs,
                                                            ScannerCounter* counter) {
    BaseScanner* scan = nullptr;
    switch (scan_range.ranges[0].format_type) {
    case TFileFormatType::FORMAT_PARQUET:
        scan = new ParquetScanner(_runtime_state, runtime_profile(), scan_range.params,
                                  scan_range.ranges, scan_range.broker_addresses,
                                  pre_filter_ctxs, counter);
        break;
    case TFileFormatType::FORMAT_ORC:
        scan = new ORCScanner(_runtime_state, runtime_profile(), scan_range.params,
                              scan_range.ranges, scan_range.broker_addresses,
                              pre_filter_ctxs, counter);
        break;
    case TFileFormatType::FORMAT_JSON:
        scan = new JsonScanner(_runtime_state, runtime_profile(), scan_range.params,
                               scan_range.ranges, scan_range.broker_addresses,
                               pre_filter_ctxs, counter);
        break;
    default:
        scan = new BrokerScanner(_runtime_state, runtime_profile(), scan_range.params,
                                 scan_range.ranges, scan_range.broker_addresses,
                                 pre_filter_ctxs, counter);
    }
    std::unique_ptr<BaseScanner> scanner(scan);
    return scanner;
}

Status BrokerScanNode::scanner_scan(const TBrokerScanRange& scan_range,
                                    const std::vector<ExprContext*>& pre_filter_ctxs,
                                    const std::vector<ExprContext*>& conjunct_ctxs,
                                    ScannerCounter* counter) {
    //create scanner object and open
    std::unique_ptr<BaseScanner> scanner = create_scanner(scan_range, pre_filter_ctxs, counter);
    RETURN_IF_ERROR(scanner->open());
    bool scanner_eof = false;

    while (!scanner_eof) {
        // Fill one row batch
        std::shared_ptr<RowBatch> row_batch(
                new RowBatch(row_desc(), _runtime_state->batch_size(), mem_tracker().get()));

        // create new tuple buffer for row_batch
        MemPool* tuple_pool = row_batch->tuple_data_pool();
        int tuple_buffer_size = row_batch->capacity() * _tuple_desc->byte_size();
        void* tuple_buffer = tuple_pool->allocate(tuple_buffer_size);
        if (tuple_buffer == nullptr) {
            return Status::InternalError("Allocate memory for row batch failed.");
        }

        Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buffer);
        while (!scanner_eof) {
            RETURN_IF_CANCELLED(_runtime_state);
            // If we have finished all works
            if (_scan_finished.load()) {
                return Status::OK();
            }

            // This row batch has been filled up, and break this
            if (row_batch->is_full()) {
                break;
            }

            int row_idx = row_batch->add_row();
            TupleRow* row = row_batch->get_row(row_idx);
            // scan node is the first tuple of tuple row
            row->set_tuple(0, tuple);
            memset(tuple, 0, _tuple_desc->num_null_bytes());

            // Get from scanner
            RETURN_IF_ERROR(scanner->get_next(tuple, tuple_pool, &scanner_eof));
            if (scanner_eof) {
                continue;
            }

            // eval conjuncts of this row.
            if (eval_conjuncts(&conjunct_ctxs[0], conjunct_ctxs.size(), row)) {
                row_batch->commit_last_row();
                char* new_tuple = reinterpret_cast<char*>(tuple);
                new_tuple += _tuple_desc->byte_size();
                tuple = reinterpret_cast<Tuple*>(new_tuple);
                // counter->num_rows_returned++;
            } else {
                counter->num_rows_unselected++;
            }
        }

        // Row batch has been filled, push this to the queue
        if (row_batch->num_rows() > 0) {
            std::unique_lock<std::mutex> l(_batch_queue_lock);
            while (_process_status.ok() && !_scan_finished.load() &&
                   !_runtime_state->is_cancelled() &&
                   // stop pushing more batch if
                   // 1. too many batches in queue, or
                   // 2. at least one batch in queue and memory exceed limit.
                   (_batch_queue.size() >= _max_buffered_batches ||
                    (mem_tracker()->AnyLimitExceeded(MemLimit::HARD) && !_batch_queue.empty()))) {
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
            _batch_queue.push_back(row_batch);

            // Notify reader to
            _queue_reader_cond.notify_one();
        }
    }

    return Status::OK();
}

void BrokerScanNode::scanner_worker(int start_idx, int length) {
    // Clone expr context
    std::vector<ExprContext*> scanner_expr_ctxs;
    auto status = Expr::clone_if_not_exists(_conjunct_ctxs, _runtime_state, &scanner_expr_ctxs);
    if (!status.ok()) {
        LOG(WARNING) << "Clone conjuncts failed.";
    }

    std::vector<ExprContext*> pre_filter_ctxs;
    if (status.ok()) {
        status = Expr::clone_if_not_exists(_pre_filter_ctxs, _runtime_state, &pre_filter_ctxs);
    }

    ScannerCounter counter;
    for (int i = 0; i < length && status.ok(); ++i) {
        const TBrokerScanRange& scan_range =
                _scan_ranges[start_idx + i].scan_range.broker_scan_range;
        status = scanner_scan(scan_range, pre_filter_ctxs, scanner_expr_ctxs, &counter);
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
    Expr::close(scanner_expr_ctxs, _runtime_state);
}

} // namespace doris
